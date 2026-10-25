"""Finite source-aware live-latch guard; prepare-only without --apply.

No engine/API action, quota change or latch reset. Every actual recovery is the
separately locked native command, bound to this observer's exact pinned state.
"""
from __future__ import annotations
import argparse
from datetime import datetime,timezone
import fcntl,hashlib,json,math,os
from pathlib import Path
import signal,subprocess,sys,time
from ops.ha.retune_full_gpu import file_env
from ops.ha.set_scheduler_policy import atomic_bytes
from ops.product_campaign import MODEL,managed_identity
from ops.recover_latched_worker import (ROOT,EXPECTED,RUNTIME,campaign_holds_open_lock,
    require_effective_configuration,require_engine_binding,require_quiescence,charge_attempt)
from ops.recovery_state import state_document,fingerprint
from ops.supervise import load_manifest,private_read
from ops.worker_source import probe_source,PROBED_SOURCES
from worker.kv_admission import engine_quiescent

STOP=datetime(2026,10,26,9,tzinfo=timezone.utc).timestamp()
METRICS=('batchforge_worker_adaptive_kv_paused','batchforge_worker_inflight',
         'batchforge_worker_reserved_context_tokens','batchforge_worker_adaptive_kv_waiting')


def candidate(observation,now):
    try:require_quiescence(observation,now=now);return True
    except ValueError:return False


def action_allowed(*,now,end,attempts,apply):
    return (apply is True and type(attempts) is int and 0<=attempts<2
        and all(type(x) in (int,float) and math.isfinite(x) for x in (now,end)) and end-now>=600)


def budget_available(document,now):
    # Read-only peek. Exhaustion waits for natural expiry; corrupt/future data
    # remains closed. Only the locked helper may persist a charged intent.
    try:charge_attempt(document['attempts'],now=now);return True
    except (ValueError,KeyError,TypeError):return False


class EvidenceWindow:
    def __init__(self):self.previous=None;self.evidence=[]
    def observe(self,observation,now):
        if not candidate(observation,now):self.previous=None;self.evidence=[];return False
        before=self.previous;self.previous=observation
        if before is None or not candidate(before,now):return False
        gap=(datetime.fromisoformat(observation['captured_at'])-datetime.fromisoformat(before['captured_at'])).total_seconds()
        valid=1<=gap<=10
        self.evidence=[before,observation] if valid else []
        return valid


class Pins:
    def __init__(self,root=ROOT):
        self.root=root;self.manifest,self.services,_=load_manifest(root/'.runtime/services-primary.json',root=root)
        if len(self.services)!=5 or {(s['role'],s['name']) for s in self.services}!=EXPECTED:raise ValueError('Current480 primary required')
        self.worker=next(s for s in self.services if s['role']=='worker');self.engine=next(s for s in self.services if s['role']=='engine')
        self.watch={'role':'supervisor','name':'primary','env_file':str(root.parent/'supervisor.env'),'python':str(root.parent/'venv/bin/python')}
        self.all=[*self.services,self.watch]
        self.original_manifest=private_read(self.manifest)
        self.configs={Path(s['env_file']):private_read(Path(s['env_file'])) for s in self.all}
        self.identities={self.key(s):managed_identity(root,s) for s in self.all}
        self.environment=file_env(self.worker['env_file']);self.source=Path(self.environment.get('PYTHONPATH',''))
        if (self.environment.get('PYTHONSAFEPATH')!='1' or not self.source.is_absolute()
            or self.source.resolve()!=self.source or self.source.name!='source'
            or self.source.parent.parent!=root.parent or not self.source.parent.name.startswith('evidence-api-')
            or os.pathsep in str(self.source)):raise ValueError('Single immutable safe-path source required')
        self.receipt_path=self.source.parent/'source-receipt.json';self.receipt_raw=private_read(self.receipt_path)
        self.receipt=json.loads(self.receipt_raw)
        require_engine_binding(self.environment,engine_port=self.engine['port'],worker_port=self.worker['port'])
        self.probe=probe_source(self.worker['python'],self.source,self.environment,root,self.receipt['files'])
        engine=file_env(self.engine['env_file'])
        if (self.probe['model']!=MODEL or self.probe['slots']!=32 or self.probe['token_budget']!=164256
            or self.probe['max_model_len']!=18440 or self.probe['runtime_fingerprint']!=RUNTIME
            or self.probe['adaptive_kv_enabled'] is not True or engine.get('MODEL_NAME')!=MODEL
            or engine.get('ENGINE_TENSOR_PARALLEL_SIZE')!='4'):raise ValueError('Qualified480 TP4 KV profile required')
        self.managers={p:hashlib.sha256(p.read_bytes()).hexdigest() for p in [root/'ops/manage.py',root/'ops/supervise.py']}
        self.check(full=True)

    @staticmethod
    def key(s):return s['role']+'-'+s['name']

    def check(self,*,full=False,changed_worker=None):
        if (private_read(self.manifest)!=self.original_manifest or private_read(self.receipt_path)!=self.receipt_raw
            or any(private_read(p)!=raw for p,raw in self.configs.items())
            or any(hashlib.sha256(p.read_bytes()).hexdigest()!=sha for p,sha in self.managers.items())):
            raise ValueError('Pinned manifest, configuration, source receipt or manager changed')
        names=self.receipt['files'] if full else PROBED_SOURCES
        if any(hashlib.sha256((self.source/n).read_bytes()).hexdigest()!=self.receipt['files'][n] for n in names):
            raise ValueError('Immutable service source changed')
        current={self.key(s):managed_identity(self.root,s) for s in self.all}
        if changed_worker is None:
            if current!=self.identities:raise ValueError('Pinned managed identity changed')
        else:
            if (current[self.key(self.worker)]!=changed_worker
                or current[self.key(self.worker)]==self.identities[self.key(self.worker)]
                or any(current[k]!=v for k,v in self.identities.items() if k not in (self.key(self.worker),self.key(self.watch)))):
                raise ValueError('Unexpected service identity change during recovery')
        require_effective_configuration(self.environment,Path(f"/proc/{current[self.key(self.worker)]['pid']}/environ").read_bytes())
        if changed_worker is not None:self.identities=current
        return fingerprint(state_document(self.original_manifest,self.configs,self.receipt_raw,self.managers,self.identities))


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run',type=Path,required=True);p.add_argument('--qualified-trial',type=Path,required=True)
    p.add_argument('--qualified-audit',type=Path,required=True);p.add_argument('--active-campaign',type=Path)
    p.add_argument('--seconds',type=int,default=14*3600);p.add_argument('--apply',action='store_true')
    p.add_argument('--stop-after-recovery',action='store_true')
    p.add_argument('--max-observations',type=int,default=0,help='Optional finite qualification sample bound;0 uses the time deadline')
    args=p.parse_args();os.umask(0o077)
    if not 0<=args.max_observations<=17280:p.error('Bounded observation count required')
    if (args.run.parent!=ROOT.parent or not args.run.name.startswith('evidence-api-worker-latch-guard-')
        or args.run.resolve()!=args.run or not 10<=args.seconds<=14*3600):p.error('New owned guard run and finite10s–14h bound required')
    for path,prefix in [(args.qualified_trial,'soak480-latch-qualification-'),(args.qualified_audit,'evidence-api-latch-recovery-audit-')]:
        if path.parent!=ROOT.parent or path.resolve()!=path or not path.name.startswith(prefix):p.error('Owned closed qualification evidence required')
    if args.active_campaign is not None and (args.active_campaign.parent!=ROOT.parent or args.active_campaign.resolve()!=args.active_campaign or not args.active_campaign.name.startswith('soak480-')):
        p.error('Explicit owned fixed campaign required')
    base=args.run;base.mkdir(mode=0o700,exist_ok=False)
    began=time.time();end=min(STOP,began+args.seconds);monotonic_end=time.monotonic()+max(0,end-began)
    report={'phase':'preflight','passed':False,'apply_requested':args.apply,'events':[],
        'started_at':datetime.now(timezone.utc).isoformat(),'deadline_epoch':end,
        'action_cutoff_epoch':end-600,'recovery_invocations':0,'recoveries':[],
        'engine_actions':False,'api_actions':False,'new_cloud_resources':False,
        'controller_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'observations':0}
    last_saved=0;http=None;db=None;stopping=False
    def stop_requested(_signal,_frame):
        nonlocal stopping
        stopping=True;report['stop_requested']=True
    signal.signal(signal.SIGTERM,stop_requested)
    def save(phase=None,force=False):
        nonlocal last_saved
        changed=phase is not None and phase!=report['phase']
        if changed:report['events'].append({'phase':phase,'at':datetime.now(timezone.utc).isoformat()});report['phase']=phase
        if force or changed or time.monotonic()-last_saved>=60:
            atomic_bytes(base/'validation.json',(json.dumps(report,indent=2,allow_nan=False)+'\n').encode());last_saved=time.monotonic()
    def boundary():
        with (ROOT/'.runtime/product-campaign.lock').open('a') as f:
            try:fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB);return True
            except BlockingIOError:
                if args.active_campaign is None:return False
                try:campaign_holds_open_lock(ROOT,args.active_campaign);return True
                except (ValueError,FileNotFoundError):return False
    try:
        lock=(ROOT/'.runtime/worker-latch-guard.lock').open('a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        if end<=began:raise ValueError('Authorized guard deadline passed')
        if not boundary():raise ValueError('Unknown active controller owns startup boundary')
        trial_raw=private_read(args.qualified_trial/'campaign.json');trial=json.loads(trial_raw)
        audit_raw=private_read(args.qualified_audit/'validation.json');audit=json.loads(audit_raw)
        if (trial.get('passed') is not True or trial.get('qualified_recovery') is not True
            or trial.get('original_configuration_restored') is not True or trial.get('restore_errors')!=[]
            or audit.get('passed') is not True or audit.get('trial_report_sha256')!=hashlib.sha256(trial_raw).hexdigest()):
            raise ValueError('Closed real recovery and source-bound storage audit required')
        pins=Pins();report.update(qualification_sha256=hashlib.sha256(trial_raw).hexdigest(),
            qualification_audit_sha256=hashlib.sha256(audit_raw).hexdigest(),worker_source_probe=pins.probe,
            initial_identities=pins.identities.copy(),initial_state_sha256=pins.check())
        budget_path=ROOT/'.runtime/worker-kv-recovery-budget.json'
        report['initial_budget_available']=budget_available(json.loads(private_read(budget_path)),time.time())
        if (trial.get('live_worker_source_probe',{}).get('source_files')!=pins.probe['source_files']
            or audit.get('worker_source_probe',{}).get('source_files')!=pins.probe['source_files']):
            raise ValueError('Current service module bytes differ from real fault qualification')
        import httpx,psycopg
        http=httpx.Client(trust_env=False,timeout=5)
        env=file_env(next(s['env_file'] for s in pins.services if s['role']=='api'))
        db=psycopg.connect(env['DATABASE_URL'],autocommit=True,connect_timeout=5,
            options='-c default_transaction_read_only=on -c statement_timeout=5000')
        if db.execute('SHOW default_transaction_read_only').fetchone()[0]!='on':raise ValueError('Read-only guard database session required')
        db.close();db=None
        def get(port,path):
            r=http.get(f'http://127.0.0.1:{port}'+path);r.raise_for_status()
            if len(r.content)>2*1024**2:raise ValueError('Guard response exceeds bound')
            return r
        def observe():
            started=datetime.now(timezone.utc).isoformat();metrics={}
            for line in get(pins.worker['port'],'/metrics').text.splitlines():
                if line.startswith('batchforge_worker_') and len(line.split())==2:
                    k,v=line.split()
                    if k in METRICS:
                        if k in metrics:raise ValueError('Duplicate worker evidence')
                        number=float(v);metrics[k]=number if math.isfinite(number) else None
            o={'captured_at':started,'worker_ready_status':http.get(f"http://127.0.0.1:{pins.worker['port']}/readyz").status_code,'worker_metrics':metrics}
            if o['worker_ready_status']==503 and metrics.get(METRICS[0])==1 and all(metrics.get(k)==0 for k in METRICS[1:]):
                with psycopg.connect(env['DATABASE_URL'],autocommit=True,connect_timeout=5,
                    options='-c default_transaction_read_only=on -c statement_timeout=5000') as fresh:
                    running,pending=fresh.execute("SELECT count(*) FILTER(WHERE status='running'),count(*) FILTER(WHERE status='pending') FROM tasks WHERE status IN ('running','pending')").fetchone()
                o.update(db_running=running,db_pending=pending,engine_quiescent=engine_quiescent(get(pins.engine['port'],'/metrics').text),
                    model_ids=[x['id'] for x in get(pins.engine['port'],'/v1/models').json()['data']],
                    engine_version=get(pins.engine['port'],'/version').json()['version'])
            return o
        save('prepared',True)
        if not args.apply:
            report['last_observation']=observe();report['observations']=1;report['passed']=True;save('prepared',True);return
        window=EvidenceWindow();errors=0;save('armed',True)
        while not stopping and time.time()<end and time.monotonic()<monotonic_end:
            state=pins.check()
            if not boundary():window.previous=None;save('waiting_campaign_boundary');time.sleep(5);continue
            try:o=observe();errors=0
            except (httpx.HTTPError,psycopg.Error,ValueError,KeyError) as exc:
                window.previous=None;errors+=1;report['telemetry_error_type']=type(exc).__name__;save('telemetry_unavailable')
                if errors>=6:raise RuntimeError('Bounded observation error threshold reached') from None
                time.sleep(5);continue
            report['observations']+=1;report['last_observation']=o
            if window.observe(o,datetime.now(timezone.utc)):
                if not action_allowed(now=time.time(),end=end,attempts=report['recovery_invocations'],apply=args.apply):
                    report['action_refused']='deadline_margin_or_lifetime_limit';save('action_refused',True);break
                report['budget_available']=budget_available(json.loads(private_read(budget_path)),time.time())
                if not report['budget_available']:
                    save('waiting_persistent_budget');time.sleep(5);continue
                state=pins.check(full=True);report['recovery_invocations']+=1
                recovery=ROOT.parent/('evidence-api-worker-recovery-guard-'+base.name.removeprefix('evidence-api-')+'-'+str(report['recovery_invocations']))
                command=[sys.executable,'-m','ops.recover_latched_worker','--run',str(recovery),'--expected-state',state,'--apply']
                if args.active_campaign:command+=['--active-campaign',str(args.active_campaign)]
                entry={'run':str(recovery),'expected_state_sha256':state,'started_at':datetime.now(timezone.utc).isoformat(),
                       'detected_at':window.evidence[0]['captured_at'],'quiescence_observations':window.evidence}
                report['recoveries'].append(entry);save('invoking_locked_recovery',True)
                from ops.manage import process_identity
                with (base/('recovery-'+str(report['recovery_invocations'])+'-private.log')).open('xb') as log:
                    child=subprocess.Popen(command,cwd=Path(__file__).resolve().parents[1],stdin=subprocess.DEVNULL,
                        stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
                    entry['identity']=process_identity(child.pid);save(force=True)
                    # Do not KILL a helper between worker stop and watchdog restore.
                    # An overrun stays an identified independent child for inspection.
                    try:rc=child.wait(timeout=360)
                    except subprocess.TimeoutExpired:raise RuntimeError('Recovery child exceeded guard wait; inspect the same identified child') from None
                result_raw=private_read(recovery/'validation.json');result=json.loads(result_raw)
                entry.update(exit_code=rc,result_sha256=hashlib.sha256(result_raw).hexdigest(),passed=result.get('passed'),finished_at=datetime.now(timezone.utc).isoformat())
                save(force=True)
                if rc or result.get('passed') is not True or result.get('locked_state_sha256')!=state:raise RuntimeError('Locked recovery did not verify')
                pins.check(full=True,changed_worker=result['recovered_worker_identity']);window.previous=None
                save('armed',True)
                if args.stop_after_recovery:break
            else:save('armed')
            if args.max_observations and report['observations']>=args.max_observations:break
            time.sleep(min(5,max(0,end-time.time())))
        report.update(passed=True,final_identities=pins.identities.copy(),final_state_sha256=pins.check(full=True))
        save('complete',True)
    except BaseException as exc:
        report.update(passed=False,error_type=type(exc).__name__,phase_before_failure=report['phase']);save('failed',True)
    finally:
        for connection in (http,db):
            if connection is not None:connection.close()
        print(json.dumps({k:report.get(k) for k in ['phase','passed','apply_requested','recovery_invocations','error_type']}),flush=True)
    if not report['passed']:raise SystemExit(1)


if __name__=='__main__':main()
