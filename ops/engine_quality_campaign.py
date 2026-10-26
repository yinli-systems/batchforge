"""Exclusive, finite engine QA with a separate, sole restoration owner.

Trusted operator CLI only. Prepare by default. No model/engine/cloud action.
The restore role verifies its frozen private config before taking both locks;
the inference role cannot proceed until its exact identity is durably recorded.
"""
from __future__ import annotations
import argparse,asyncio,base64,fcntl,hashlib,json,math,os,re,select,signal,subprocess,sys,threading,time
from datetime import datetime,timezone
from pathlib import Path
import httpx
from ops.manage import process_identity
from ops.maintenance_guard import RestorationGuard
from ops.ha.retune_full_gpu import file_env
from ops.ha.set_scheduler_policy import PolicyManager,atomic_bytes
from ops.product_campaign import managed_identity
from ops.recover_latched_worker import require_effective_configuration
from ops.supervise import private_read
from ops.worker_latch_guard import Pins,ROOT,STOP,METRICS
from worker.kv_admission import engine_quiescent

LOCKS=('product-campaign.lock','worker-kv-recovery.lock')
DATASET=ROOT/'.runtime/public-data/dev-v1.1.json'


def sha(raw):return hashlib.sha256(raw).hexdigest()
def now():return datetime.now(timezone.utc).isoformat()
def save(path,value):atomic_bytes(path,(json.dumps(value,indent=2,allow_nan=False)+'\n').encode())
def read(path,limit=262144):return json.loads(private_read(path,limit))


def gated(raw):
    """Change one key only; preserve all other bytes, reject ambiguous files."""
    rows=raw.decode().splitlines(keepends=True);count=0;result=[]
    for row in rows:
        key,sep,_=row.strip().removeprefix('export ').strip().partition('=')
        if sep and key.strip()=='BATCH_SUBMISSION_ENABLED':
            count+=1;result.append('BATCH_SUBMISSION_ENABLED=false\n')
        else:result.append(row)
    if count!=1:raise ValueError('Exactly one explicit admission assignment required')
    return ''.join(result).encode()


def owned_run(path):
    if (path.parent!=ROOT.parent or path.resolve()!=path or not re.fullmatch(r'soak480-engine-quality-[a-zA-Z0-9-]+',path.name)):
        raise ValueError('New owned engine-quality run required')
    return path


def window_end(epoch,seconds):
    if type(seconds) is not int or not 60<=seconds<=3600 or epoch+seconds+900>STOP:
        raise ValueError('Insufficient finite evaluation and restoration margin')
    return epoch+seconds+300


def acquire(seconds=0):
    handles=[];end=time.monotonic()+seconds
    try:
        for name in LOCKS:
            h=(ROOT/'.runtime'/name).open('a');handles.append(h)
            while True:
                try:fcntl.flock(h,fcntl.LOCK_EX|fcntl.LOCK_NB);break
                except BlockingIOError:
                    if time.monotonic()>=end:raise
                    time.sleep(.1)
        return handles
    except BaseException:
        for h in handles:h.close()
        raise


def release(handles):
    for h in handles:h.close()
    handles.clear()


def signal_exact(identity,*,grace=10,kill=False):
    """Never signal a reused PID; refuse a resistant service by default."""
    if process_identity(identity['pid'])!=identity:return
    try:fd=os.pidfd_open(identity['pid'])
    except ProcessLookupError:return
    try:
        if process_identity(identity['pid'])!=identity:raise ValueError('Identity changed during pidfd bind')
        try:signal.pidfd_send_signal(fd,signal.SIGTERM)
        except ProcessLookupError:return
        if not select.select([fd],[],[],grace)[0]:
            if not kill:raise RuntimeError('Owned process did not stop within bound')
            try:signal.pidfd_send_signal(fd,signal.SIGKILL)
            except ProcessLookupError:return
            if not select.select([fd],[],[],3)[0]:raise RuntimeError('Owned process remained live after KILL')
    finally:os.close(fd)


def physical_kv(log,identity):
    # Every request log repeats the APIServer PID. Bind the LAST startup
    # segment, not the last arbitrary line containing that PID.
    starts=list(re.finditer(r'APIServer pid=(\d+)[^\n]*vLLM API server version 0\.10\.2',log))
    if not starts or int(starts[-1][1])!=identity['pid']:raise ValueError('Current engine cache startup segment missing')
    segment=log[starts[-1].end():]
    values=re.findall(r'GPU KV cache size: ([0-9,]+) tokens',segment)
    if not values or min(int(v.replace(',','')) for v in values)!=172912:
        raise ValueError('Fresh qualified physical KV differs')
    return 172912


def capture(base,guard_run,seconds,count):
    if (guard_run.parent!=ROOT.parent or guard_run.resolve()!=guard_run
        or not guard_run.name.startswith('evidence-api-worker-latch-guard-')):raise ValueError('Owned existing guard run required')
    pins=Pins();end=window_end(time.time(),seconds)
    for s in pins.all:
        identity=pins.identities[s['role']+'-'+s['name']]
        require_effective_configuration(file_env(s['env_file']),Path(f'/proc/{identity["pid"]}/environ').read_bytes())
    guard=read(guard_run/'validation.json');launcher=read(guard_run/'launcher.json')
    identity=launcher['identity'];command=launcher['command']
    actual=Path(f'/proc/{identity["pid"]}/cmdline').read_bytes().split(b'\0')[:-1]
    if (guard.get('phase') not in ('armed','waiting_campaign_boundary') or guard.get('recovery_invocations')!=0
        or process_identity(identity['pid'])!=identity or actual!=[x.encode() for x in command]
        or command[1:3]!=['-m','ops.worker_latch_guard'] or command[3:5]!=['--run',str(guard_run)]
        or guard.get('deadline_epoch')!=STOP):raise ValueError('Known zero-action finite guard required')
    from scripts.engine_quality import prepare,plan
    cohort=prepare(DATASET.read_bytes(),count=count,seed=2026102601)
    public_plan=plan(cohort,runtime_fingerprint=pins.probe['runtime_fingerprint'],deadline_seconds=seconds)
    cache=physical_kv((ROOT/'.runtime/engine-qwen480bench.log').read_text(errors='replace'),pins.identities['engine-qwen480bench'])
    config={'protocol':'batchforge.exclusive-quality.v1','base':str(base),'controller':process_identity(os.getpid()),
        'operator_source':str(Path(__file__).resolve().parents[1]),'operator_files':{},'services':pins.all,
        'manifest_path':str(pins.manifest),'manifest_b64':base64.b64encode(pins.original_manifest).decode(),
        'originals':{str(p):base64.b64encode(v).decode() for p,v in pins.configs.items()},
        'managers':{str(p):v for p,v in pins.managers.items()},'identities':pins.identities,
        'worker_source':str(pins.source),'worker_receipt_path':str(pins.receipt_path),'worker_receipt_sha256':sha(pins.receipt_raw),
        'worker_files':pins.receipt['files'],'plan':public_plan,'physical_kv_tokens':cache,'end_epoch':end,
        'guard_run':str(guard_run),'guard_identity':identity,'guard_command':command,'guard_deadline':guard['deadline_epoch']}
    guard_source=Path(identity['cwd']);guard_receipt=read(guard_source.parent/'source-receipt.json')
    if any(sha((guard_source/n).read_bytes())!=v for n,v in guard_receipt['files'].items()):raise ValueError('Qualified guard source drift')
    config['guard_source_files']=guard_receipt['files'];config['guard_source_commit']=guard_receipt['git_commit']
    gpu=subprocess.run(['nvidia-smi','--query-gpu=index,name','--format=csv,noheader,nounits'],capture_output=True,text=True,check=True,timeout=5)
    cards=[line.split(',',1) for line in gpu.stdout.strip().splitlines()]
    if len(cards)!=4 or {x[0].strip() for x in cards}!={'0','1','2','3'} or any('H100' not in x[1] for x in cards):raise ValueError('Exactly four existing H100 required')
    config['gpu_inventory']=gpu.stdout.strip().splitlines()
    receipt=read(Path(config['operator_source']).parent/'source-receipt.json')
    config['operator_files']=receipt['files'];config['operator_commit']=receipt['git_commit']
    return config


def load_config(path,digest):
    base=owned_run(path.parent)
    if path!=base/'restore-private.json':raise ValueError('Fixed private restore configuration required')
    raw=private_read(path,1024**2)
    if sha(raw)!=digest:raise ValueError('Restore config commitment differs')
    c=json.loads(raw)
    if c.get('base')!=str(base) or c.get('protocol')!='batchforge.exclusive-quality.v1':raise ValueError('Invalid frozen window config')
    source=Path(c['operator_source'])
    if source!=Path(__file__).resolve().parents[1]:raise ValueError('Restorer source differs from frozen caller')
    if any(sha((source/n).read_bytes())!=v for n,v in c['operator_files'].items()):raise ValueError('Operator source changed')
    return c


class Platform:
    def __init__(self,c):
        self.c=c;self.base=Path(c['base']);self.manager=PolicyManager(root=ROOT,timeout=30)
        self.services=c['services'];self.apis=sorted((s for s in self.services if s['role']=='api'),key=lambda s:s['name'])
        self.worker=next(s for s in self.services if s['role']=='worker');self.watch=next(s for s in self.services if s['role']=='supervisor')
        self.original={Path(p):base64.b64decode(v) for p,v in c['originals'].items()}
        self.http=httpx.Client(trust_env=False,timeout=5,follow_redirects=False)
        self.key=file_env(ROOT.parent/'access.env')['BATCHFORGE_API_KEY']
        self.database_url=file_env(self.apis[0]['env_file'])['DATABASE_URL']

    def close(self):self.http.close()

    def identity(self,s,optional=False):
        path=ROOT/'.runtime'/f'{s["role"]}-{s["name"]}.json'
        if not path.exists() and optional:
            old=self.c['identities'][s['role']+'-'+s['name']]
            if process_identity(old['pid'])==old:raise ValueError('Live owned process lost its managed record')
            return None
        record=read(path)
        identity={k:record[k] for k in ('pid','start_ticks','cwd')}
        current=process_identity(identity['pid'])
        if current is None and optional:return None
        if current!=identity or current['cwd']!=str(ROOT):raise ValueError('Managed process identity differs')
        require_effective_configuration(file_env(s['env_file']),Path(f'/proc/{current["pid"]}/environ').read_bytes())
        return current

    def check(self,exclusive=False):
        c=self.c
        if private_read(Path(c['manifest_path']))!=base64.b64decode(c['manifest_b64']):raise ValueError('Manifest changed')
        if any(sha(Path(p).read_bytes())!=v for p,v in c['managers'].items()):raise ValueError('Manager changed')
        if sha(private_read(Path(c['worker_receipt_path'])))!=c['worker_receipt_sha256']:raise ValueError('Worker receipt changed')
        source=Path(c['worker_source'])
        # Module bytes and the frozen receipt are checked on every authority
        # checkpoint; full operator/service snapshots are hashed at boundaries.
        from ops.worker_source import PROBED_SOURCES
        if any(sha((source/n).read_bytes())!=c['worker_files'][n] for n in PROBED_SOURCES):raise ValueError('Live worker source changed')
        for s in self.services:
            p=Path(s['env_file']);raw=private_read(p);original=self.original[p]
            allowed=(gated(original),) if exclusive and s['role']=='api' else ((original,gated(original)) if s['role']=='api' else (original,))
            if raw not in allowed:raise ValueError('Foreign configuration change')
            if s['role'] in ('engine','gateway'):
                if self.identity(s)!=c['identities'][s['role']+'-'+s['name']]:raise ValueError('Protected service identity changed')
            elif exclusive:
                actual=self.identity(s,optional=True)
                if s['role'] in ('worker','supervisor') and actual is not None:raise ValueError('Exclusive window lost to a running worker/watchdog')
                if s['role']=='api' and actual is None:raise ValueError('Gated API unavailable')
        return True

    def invoke(self,s,action):
        self.check();self.identity(s,optional=True)
        value,error=self.manager.invoke(s,action)
        if error:raise RuntimeError('Owned '+s['role']+' '+action+' failed')
        if action=='start':
            end=time.monotonic()+50
            while True:
                self.identity(s)
                if s.get('port') is None or self.manager.readiness(s):break
                if time.monotonic()>=end:raise RuntimeError('Service readiness timed out')
                time.sleep(.5)
        elif self.identity(s,optional=True) is not None:raise RuntimeError('Service remained alive after stop')
        return value

    def gates_closed(self):
        for api in self.apis:
            self.identity(api)
            for path,body in (('/v1/batches',{'input_file_id':'file_nonexistent_engine_quality_probe'}),
                ('/v1/batches/batch_nonexistent_engine_quality_probe/retry',{})):
                r=self.http.post(f'http://127.0.0.1:{api["port"]}'+path,json=body,headers={'Authorization':'Bearer '+self.key})
                if r.status_code!=503 or r.json().get('error',{}).get('code')!='admission_paused':raise ValueError('Create/retry admission is not closed')

    def observe(self,worker=True):
        import psycopg
        from psycopg.conninfo import conninfo_to_dict
        started=now();self.check()
        if conninfo_to_dict(self.database_url).get('sslmode')!='verify-full':raise ValueError('Verified TLS database configuration required')
        with psycopg.connect(self.database_url,autocommit=True,connect_timeout=5,
            options='-c default_transaction_read_only=on -c statement_timeout=5000') as db:
            if db.execute('SHOW default_transaction_read_only').fetchone()[0]!='on':raise ValueError('Read-only DB observation required')
            schema=db.execute('SELECT max(version) FROM batchforge_schema_migrations').fetchone()[0]
            role=db.execute('SELECT rolsuper,rolcreatedb,rolcreaterole FROM pg_roles WHERE rolname=current_user').fetchone()
            if schema!=11 or role!=(False,False,False):raise ValueError('Current schema11 least-privilege database required')
            pending,running=db.execute("SELECT count(*) FILTER(WHERE status='pending'),count(*) FILTER(WHERE status='running') FROM tasks WHERE status IN ('pending','running')").fetchone()
        r=self.http.get('http://127.0.0.1:8348/metrics');r.raise_for_status()
        if len(r.content)>2*1024**2:raise ValueError('Metrics exceed bound')
        o={'at':started,'db_pending':pending,'db_running':running,'engine_quiescent':engine_quiescent(r.text),
            'schema_version':schema,'runtime_privileges_verified':True,'configured_tls_verify_full':True}
        if worker:
            self.identity(self.worker);r=self.http.get('http://127.0.0.1:9105/metrics');r.raise_for_status();metrics={}
            for line in r.text.splitlines():
                parts=line.split()
                if len(parts)==2 and parts[0] in METRICS:
                    if parts[0] in metrics:raise ValueError('Duplicate worker gauge')
                    metrics[parts[0]]=float(parts[1])
            o['worker_metrics']=metrics
            o['worker_idle']=all(metrics.get(k)==0 for k in METRICS)
        return o

    def quiet(self,worker=True,seconds=120):
        end=time.monotonic()+seconds;previous=None;rows=[]
        while time.monotonic()<end:
            o=self.observe(worker);rows.append(o)
            idle=o['db_pending']==o['db_running']==0 and o['engine_quiescent'] and (not worker or o['worker_idle'])
            if idle and previous is not None and 1<=time.monotonic()-previous<=10:return rows[-2:]
            previous=time.monotonic() if idle else None;time.sleep(1.1)
        raise RuntimeError('Fresh engine/database/worker quiescence not proven')

    def configure(self,api,closed):
        self.check();self.invoke(api,'stop');p=Path(api['env_file'])
        atomic_bytes(p,gated(self.original[p]) if closed else self.original[p]);self.invoke(api,'start')


def restore_services(platform,*,direct_runner,stop_runner):
    """A direct-engine client must be dead and quiet before reopening Batch.

    Before START there may still be already-accepted Batch work. Do not kill
    its live worker or demand it disappear as a condition of restoring service.
    """
    platform.check();observations=None
    if direct_runner:
        stop_runner();observations=platform.quiet(worker=False,seconds=240)
    for s in (platform.worker,*platform.apis):
        if s['role']=='api':
            if private_read(Path(s['env_file']))!=platform.original[Path(s['env_file'])]:platform.configure(s,False)
            elif platform.identity(s,optional=True) is None:platform.invoke(s,'start')
        elif platform.identity(s,optional=True) is None:platform.invoke(s,'start')
    if platform.identity(platform.watch,optional=True) is None:platform.invoke(platform.watch,'start')
    platform.check()
    if any(private_read(p)!=raw for p,raw in platform.original.items()):raise ValueError('Original configuration bytes not restored')
    return observations


def restore_config(path,digest):
    """Sole restore owner after parent releases both locks or dies."""
    c=load_config(path,digest);base=Path(c['base']);locks=acquire(15);platform=None
    restore_lock=(base/'restore.lock').open('a');fcntl.flock(restore_lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    result={'passed':False,'phase':'restoring','started_at':now(),'config_sha256':digest,
        'engine_actions':False,'cloud_actions':False,'errors':[],'identity':process_identity(os.getpid())}
    report=base/'recovery-validation.json'
    if report.exists():raise ValueError('Restoration is never automatically replayed')
    try:
        # Journal failure must not skip a safe restoration attempt. The final
        # report is still required before anyone can claim verified recovery.
        try:save(report,result)
        except OSError:result['initial_journal_write_failed']=True
        platform=Platform(c);platform.check()
        # A runner cannot receive START until this record was fsynced. A child
        # without a record stays blocked and exits on EOF/parent death.
        direct=(base/'runner.json').exists()
        def stop_runner():signal_exact(read(base/'runner.json')['identity'],grace=5,kill=True)
        result['engine_quiescence']=restore_services(platform,direct_runner=direct,stop_runner=stop_runner)
        result['direct_runner_stopped']=direct
        fresh=Pins();result['restored_identities']=fresh.identities;result['restored_state_sha256']=fresh.check(full=True)
        for s in fresh.services:
            if not platform.manager.readiness(s):raise ValueError('Restored primary not ready')
        # A separate truthful live handler campaign owns startup lock authority;
        # never impersonate a dead original controller's launcher.
        boundary=base.with_name(base.name+'-restore-boundary');boundary.mkdir(mode=0o700,exist_ok=False)
        save(boundary/'launcher.json',{'identity':process_identity(os.getpid())})
        save(boundary/'campaign.json',{'phase':'sustained_running','scope':'Restoration handler owns exclusive locks'})
        old=read(Path(c['guard_run'])/'validation.json')
        if old.get('recovery_invocations')!=0:raise ValueError('Guard lifetime budget changed; refusing allowance reset')
        command=c['guard_command'];trial=command[command.index('--qualified-trial')+1];audit=command[command.index('--qualified-audit')+1]
        run=ROOT.parent/('evidence-api-worker-latch-guard-'+base.name)
        remaining=math.ceil(c['guard_deadline']-time.time())
        if remaining<10:raise ValueError('Finite guard deadline expired')
        guard_source=Path(c['guard_identity']['cwd'])
        if any(sha((guard_source/n).read_bytes())!=v for n,v in c['guard_source_files'].items()):raise ValueError('Qualified guard source changed')
        args=[sys.executable,'-m','ops.worker_latch_guard','--run',str(run),'--qualified-trial',trial,
            '--qualified-audit',audit,'--active-campaign',str(boundary),'--seconds',str(remaining),'--apply']
        with (base/'guard-private.log').open('xb') as log:
            env=os.environ.copy();env.update(PYTHONSAFEPATH='1',PYTHONPATH=str(guard_source))
            child=subprocess.Popen(args,cwd=guard_source,env=env,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        save(run.with_name(run.name+'-launcher.json'),{'identity':process_identity(child.pid),'command':args})
        until=time.monotonic()+35
        while True:
            if child.poll() is not None:raise RuntimeError('Rearmed guard exited during startup')
            if (run/'validation.json').exists():
                g=read(run/'validation.json')
                if g['phase']=='armed' and g['recovery_invocations']==0:break
                if g['phase']=='failed':raise RuntimeError('Rearmed guard qualification refused')
            if time.monotonic()>=until:raise RuntimeError('Rearmed guard startup not verified')
            time.sleep(.5)
        save(run/'launcher.json',{'identity':process_identity(child.pid),'command':args,'source_commit':c['guard_source_commit']})
        result.update(passed=True,phase='complete',guard_run=str(run),guard_identity=process_identity(child.pid),guard_recovery_invocations=0)
        save(boundary/'campaign.json',{'phase':'complete','passed':True})
    except BaseException as exc:
        result.update(phase='manual_recovery_required',error_type=type(exc).__name__);raise
    finally:
        result['finished_at']=now();save(report,result)
        if platform:platform.close()
        release(locks);restore_lock.close()


def runner(config_path,digest):
    c=load_config(config_path,digest);base=Path(c['base']);parent=c['controller']
    if process_identity(parent['pid'])!=parent:raise ValueError('Controller unavailable before runner bind')
    parent_fd=os.pidfd_open(parent['pid'])
    try:
        if process_identity(parent['pid'])!=parent:raise ValueError('Controller identity changed')
        if not select.select([0,parent_fd],[],[],30)[0] or select.select([parent_fd],[],[],0)[0] or os.read(0,32)!=b'START\n':return
        own=read(base/'runner.json')
        if own.get('identity')!=process_identity(os.getpid()) or own.get('config_sha256')!=digest:raise ValueError('Runner identity not durably granted')
        platform=Platform(c)
        from scripts.engine_quality import prepare,Evaluator
        cohort=prepare(DATASET.read_bytes(),count=c['plan']['tasks'],seed=2026102601)
        deadline=time.monotonic()+min(c['plan']['deadline_seconds'],max(0,c['end_epoch']-time.time()))
        def authority():
            return (not select.select([parent_fd,0],[],[],0)[0] and time.monotonic()<deadline
                and time.time()<c['end_epoch'] and read(base/'authority.json').get('active') is True and platform.check(exclusive=True))
        private=base/'private-responses';private.mkdir(mode=0o700)
        output=(base/'run/scores.jsonl').open('x');sampling_stop=threading.Event()
        from ops.gpu_campaign import sample
        sampling=threading.Thread(target=sample,args=('http://127.0.0.1:8348',base/'run/observations.jsonl',sampling_stop),kwargs={'interval':5},daemon=True)
        def emit(event):
            output.write(json.dumps(event,allow_nan=False)+'\n');output.flush();os.fsync(output.fileno())
        def raw(case,content):atomic_bytes(private/(sha(case.encode())+'.json'),content)
        async def execute():
            async with httpx.AsyncClient(base_url='http://127.0.0.1:8348',trust_env=False,follow_redirects=False) as client:
                e=Evaluator(cohort,client,authority=authority,emit=emit,private_response=raw,
                    runtime_fingerprint=c['plan']['runtime_fingerprint'],deadline_seconds=c['plan']['deadline_seconds'])
                return await e.run()
        sampling.start()
        try:save(base/'run/report.json',asyncio.run(execute()))
        finally:sampling_stop.set();sampling.join(timeout=10);output.close();platform.close()
    finally:os.close(parent_fd)


def qualified_canary(c,path):
    if path is None:raise ValueError('Actual controller-loss restoration qualification required before full inference')
    owned_run(path);old=read(path/'restore-private.json',1024**2);r=read(path/'recovery-validation.json');proof=read(path/'dispatch-observation.json')
    if (old.get('operator_commit')!=c['operator_commit'] or old.get('operator_files')!=c['operator_files'] or old['plan']['tasks']!=100
        or r.get('passed') is not True or r.get('config_sha256')!=sha(private_read(path/'restore-private.json',1024**2))
        or proof.get('intentional_controller_exit')!=42 or proof.get('engine_not_quiescent') is not True
        or proof.get('four_h100_sample_observed') is not True or process_identity(old['controller']['pid'])==old['controller']):
        raise ValueError('Closed source-matched real engine restoration qualification required')
    return sha(private_read(path/'recovery-validation.json'))


def execute(base,guard_run,seconds,count,apply,qualify_loss,qualification=None):
    owned_run(base);base.mkdir(mode=0o700,exist_ok=False);locks=acquire();guardian=None;platform=None
    report={'phase':'prepared','passed':False,'started_at':now(),'engine_actions':False,'new_cloud_resources':False,
        'scope':'Engine-only quality, separate from Batch API acceptance/usage and quality cohorts'}
    (base/'run').mkdir(mode=0o700)
    save(base/'launcher.json',{'identity':process_identity(os.getpid())})
    def stage(name):report['maintenance_stage']=name;save(base/'campaign.json',report)
    try:
        c=capture(base,guard_run,seconds,count);platform=Platform(c);platform.check();platform.quiet(seconds=20)
        save(base/'run/plan.json',c['plan']);save(base/'restore-private.json',c);digest=sha(private_read(base/'restore-private.json',1024**2))
        report.update(operator_commit=c['operator_commit'],config_sha256=digest,physical_kv_tokens=c['physical_kv_tokens'],apply_requested=apply)
        stage('prepared')
        if not apply:report['passed']=True;return
        if count==10570:report['qualified_canary_report_sha256']=qualified_canary(c,qualification)
        # Guardian is active BEFORE stopping even the old finite observer.
        guardian=RestorationGuard(c['controller'],max(.1,c['end_epoch']-time.time()),
            [sys.executable,'-m','ops.engine_quality_campaign','--restore-config',str(base/'restore-private.json'),'--config-sha',digest],
            restore_timeout=600,log_path=base/'restore-private.log')
        save(base/'guard-launcher.json',{'identity':process_identity(guardian.child.pid),'config_sha256':digest,
            'kind':'Independent fixed-window sole restoration trigger','maximum_restore_seconds':600})
        guardian.begin();stage('independent_restoration_armed')
        signal_exact(c['guard_identity']);stage('prior_guard_stopped')
        platform.invoke(platform.watch,'stop')
        for api in platform.apis:platform.configure(api,True)
        platform.gates_closed();stage('both_create_retry_gates_closed')
        report['drain_observations']=platform.quiet(seconds=180)
        platform.invoke(platform.worker,'stop');platform.check(exclusive=True)
        report['phase']='sustained_running';stage('exclusive_engine_verified')
        save(base/'authority.json',{'active':True})
        with (base/'runner-private.log').open('xb') as log:
            child=subprocess.Popen([sys.executable,'-m','ops.engine_quality_campaign','--runner-config',str(base/'restore-private.json'),
                '--config-sha',digest],cwd=c['operator_source'],stdin=subprocess.PIPE,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        save(base/'runner.json',{'identity':process_identity(child.pid),'config_sha256':digest})
        child.stdin.write(b'START\n');child.stdin.flush();stage('inference_running')
        # Loss qualification is restricted to100 public cases and requires a
        # fsynced inference intent plus a fresh engine running observation.
        while child.poll() is None:
            if qualify_loss:
                journal=base/'run/scores.jsonl'
                if journal.exists() and b'"event": "inference.intent"' in journal.read_bytes():
                    metrics=platform.http.get('http://127.0.0.1:8348/metrics');metrics.raise_for_status()
                    observations=base/'run/observations.jsonl';samples=[]
                    if observations.exists():
                        data=observations.read_bytes();data=data[:data.rfind(b'\n')+1]
                        samples=[json.loads(v) for v in data.splitlines()]
                    recent=[v for v in samples if time.monotonic()-v.get('monotonic',0)<10 and len(v.get('gpu_csv',[]))==4 and not v.get('gpu_error')]
                    if not engine_quiescent(metrics.text) and recent:
                        save(base/'dispatch-observation.json',{'at':now(),'runner_identity':process_identity(child.pid),
                            'intent_journal_sha256':sha(journal.read_bytes()),'engine_not_quiescent':True,'intentional_controller_exit':42,
                            'four_h100_sample_observed':True,'gpu_sample_sha256':sha(json.dumps(recent[-1],sort_keys=True).encode())})
                        os._exit(42)
            if time.time()>=c['end_epoch']-20:raise RuntimeError('Finite controller window exhausted')
            time.sleep(.2 if qualify_loss else 1)
        if child.returncode:raise RuntimeError('Engine QA runner failed; original outcomes retained')
        result=read(base/'run/report.json');report['evaluation_report_sha256']=sha(private_read(base/'run/report.json'))
        report['evaluation_valid']=result.get('evaluation_valid');report['quality_passed']=result.get('passed')
        report['passed']=result.get('evaluation_valid') is True
    except BaseException as exc:
        report.update(error_type=type(exc).__name__,failed_stage=report.get('maintenance_stage'),passed=False)
    finally:
        if guardian and guardian.active:
            # Revoke all future admission, relinquish both locks, then observe
            # the ONE independent restore handler; never run a parallel rollback.
            try:save(base/'authority.json',{'active':False})
            finally:release(locks)
            try:
                guardian.restore();restored=read(base/'recovery-validation.json')
                if restored.get('passed') is not True:raise RuntimeError('Restoration not verified')
                report['restoration_passed']=True;report['restoration_report_sha256']=sha(private_read(base/'recovery-validation.json'))
            except BaseException as exc:report.update(passed=False,restoration_passed=False,restore_error_type=type(exc).__name__)
            guardian.close_pipes()
        if platform:platform.close()
        release(locks);report['finished_at']=now()
        report['elapsed_seconds']=(datetime.fromisoformat(report['finished_at'])-datetime.fromisoformat(report['started_at'])).total_seconds()
        report['cost']={'operator_whole_pod_usd_per_hour':16.10,'window_allocation_usd':report['elapsed_seconds']*16.10/3600,
            'scope':'Whole controller window; overlaps child engine window, not invoice or marginal cost'}
        report['phase']='complete' if report['passed'] else 'failed'
        stage(report.get('maintenance_stage','prepared'))
    if not report['passed']:raise SystemExit(1)


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run',type=Path);p.add_argument('--guard-run',type=Path)
    p.add_argument('--seconds',type=int,default=3600);p.add_argument('--count',type=int,choices=(100,10570),default=10570)
    p.add_argument('--apply',action='store_true');p.add_argument('--qualify-controller-loss',action='store_true')
    p.add_argument('--qualified-canary',type=Path)
    p.add_argument('--restore-config',type=Path);p.add_argument('--runner-config',type=Path);p.add_argument('--config-sha')
    a=p.parse_args();os.umask(0o077);signal.signal(signal.SIGHUP,signal.SIG_IGN)
    if a.restore_config or a.runner_config:
        if not a.config_sha or not re.fullmatch('[0-9a-f]{64}',a.config_sha):p.error('Frozen config SHA required')
        if a.restore_config and a.runner_config:p.error('A single trusted role required')
        return restore_config(a.restore_config,a.config_sha) if a.restore_config else runner(a.runner_config,a.config_sha)
    if not a.run or not a.guard_run:p.error('New run and existing finite guard required')
    if a.qualify_controller_loss and (a.count!=100 or not a.apply):p.error('Loss injection only on applied100-case qualification')
    execute(a.run,a.guard_run,a.seconds,a.count,a.apply,a.qualify_controller_loss,a.qualified_canary)


if __name__=='__main__':main()
