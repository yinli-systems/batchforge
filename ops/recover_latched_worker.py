"""Bounded same-source worker recovery; prepare-only without --apply.

This does not clear a limiter, replace source/model, cancel tasks or restart an
engine/API. A finite controller may invoke it after independently detecting a
latched live worker. Deployment and real fault qualification are separate.
"""
from __future__ import annotations
import argparse
from datetime import datetime,timezone
import fcntl,hashlib,json,math,os
import re
from pathlib import Path
import time
from ops.ha.retune_full_gpu import file_env
from ops.ha.set_scheduler_policy import PolicyManager,atomic_bytes
from ops.product_campaign import MODEL,managed_identity,wait_until
from ops.supervise import load_manifest,private_read
from ops.worker_source import probe_source
from worker.kv_admission import engine_quiescent
from ops.recovery_state import state_document,require_expected

ROOT=Path('/root/batchforge-runtime/app')
EXPECTED={('api','a'),('api','b'),('gateway','edge'),('engine','qwen480bench'),('worker','qwen480')}
RUNTIME='sha256:1c16be65ee46d160ffb33e5aee8af00d3d42e918b4253789219602eaf6768f36'


def require_effective_configuration(expected,raw):
    """File contents are not evidence of what the already running worker used."""
    pairs=[part.partition(b'=')[::2] for part in raw.split(b'\0') if b'=' in part]
    for key,value in expected.items():
        found=[v for k,v in pairs if k==key.encode()]
        if found!=[value.encode()]:raise ValueError('Owned worker effective configuration differs from its pinned file')


def require_engine_binding(environment,*,engine_port,worker_port):
    # Quiescence of one endpoint cannot authorize restarting a worker whose
    # uncertain invocation went to a different endpoint or an unobserved proxy.
    raw=environment.get('INFERENCE_BASE_URLS','http://127.0.0.1:8001')
    urls=[value.strip().rstrip('/') for value in raw.split(',') if value.strip()] if isinstance(raw,str) else []
    if (type(engine_port) is not int or not 1<=engine_port<=65535
        or type(worker_port) is not int or not 1<=worker_port<=65535
        or urls!=[f'http://127.0.0.1:{engine_port}']
        or environment.get('WORKER_METRICS_HOST','127.0.0.1')!='127.0.0.1'
        or environment.get('WORKER_METRICS_PORT','9101')!=str(worker_port)):
        raise ValueError('Recovery observations must bind to the worker actual private engine and metrics endpoints')


def require_quiescence(observation,*,now):
    try:age=(now-datetime.fromisoformat(observation['captured_at'])).total_seconds()
    except (ValueError,TypeError,KeyError):raise ValueError('Fresh independent recovery observation required') from None
    if (not -2<=age<=15 or observation.get('worker_ready_status')!=503
        or observation.get('model_ids')!=[MODEL] or observation.get('engine_version')!='0.10.2'
        or observation.get('engine_quiescent') is not True
        or type(observation.get('db_running')) is not int or observation['db_running']!=0
        or type(observation.get('db_pending')) is not int or not 1<=observation['db_pending']<=50000):
        raise ValueError('Live paused worker and independently quiescent engine/database required')
    metrics=observation.get('worker_metrics',{})
    for name,expected in [('batchforge_worker_adaptive_kv_paused',1),('batchforge_worker_inflight',0),
        ('batchforge_worker_reserved_context_tokens',0),('batchforge_worker_adaptive_kv_waiting',0)]:
        value=metrics.get(name)
        if type(value) not in (int,float) or not math.isfinite(value) or value!=expected:
            raise ValueError('Paused worker has active/unknown reservations')


def charge_attempt(previous,*,now,maximum=2,window_seconds=3600):
    """Count the intent before signaling, including unsuccessful recoveries."""
    if (not isinstance(previous,list) or len(previous)>100 or type(now) not in (int,float)
        or not math.isfinite(now) or now<0 or maximum!=2 or window_seconds!=3600
        or any(type(v) not in (int,float) or not math.isfinite(v) or v<0 or v>now for v in previous)):
        raise ValueError('Valid persistent recovery budget required')
    retained=[v for v in previous if now-v<window_seconds]
    if len(retained)>=maximum:raise ValueError('Recovery attempt budget exhausted')
    return [*retained,now]


class RecoveryFailed(RuntimeError):
    def __init__(self,report):
        self.report=report
        super().__init__('Worker recovery failed; inspect phase evidence')


def recovery_sequence(*,validate,charge,watchdog,stop_worker,guard_stopped,start_worker,verify,event):
    report={'passed':False,'events':[],'engine_actions':False,'api_actions':False,
            'source_or_environment_changed':False,'watchdog_recovery_attempted':False}
    stage='preflight';watch_attempted=False
    def perform(name,action):
        nonlocal stage
        stage=name;event(name);report['events'].append(name);action()
    try:
        perform('preflight',validate)
        perform('charging_attempt_intent',charge)
        watch_attempted=True
        perform('watchdog_off',lambda:watchdog(False))
        # Recheck after watchdog shutdown; a previous snapshot is not authority.
        perform('fresh_quiescence_before_stop',validate)
        perform('stopping_paused_worker',stop_worker)
        perform('checking_unchanged_source',guard_stopped)
        perform('starting_same_worker',start_worker)
        perform('verifying_progress_and_preserved_identities',verify)
        report['passed']=True
    except BaseException as exc:
        report.update(passed=False,failed_stage=stage,error_type=type(exc).__name__)
    finally:
        if watch_attempted:
            report['watchdog_recovery_attempted']=True
            # Journal failure must never prevent an otherwise safe restoration.
            try:watchdog(True)
            except BaseException as exc:report.update(passed=False,watchdog_error_type=type(exc).__name__)
            try:event('watchdog_recovery_finished')
            except BaseException as exc:report.update(passed=False,journal_error_type=type(exc).__name__)
    if not report['passed']:raise RecoveryFailed(report) from None
    return report


def campaign_holds_open_lock(root,campaign):
    """A bypass is restricted to the known live fixed-study controller.

    The controller's stable launch identity and lock descriptor must still
    exist. All mutation controllers must also take worker-kv-recovery.lock.
    This never guesses PIDs from command lines or enumerates arbitrary apps.
    """
    if (campaign.parent!=root.parent or not campaign.name.startswith('soak480-')
        or campaign.resolve()!=campaign):raise ValueError('Owned current campaign required')
    state=json.loads(private_read(campaign/'campaign.json'))
    launch=json.loads(private_read(campaign/'launcher.json'))
    identity=launch.get('identity');from ops.manage import process_identity
    if (state.get('phase')!='sustained_running' or not isinstance(identity,dict)
        or type(identity.get('pid')) is not int or process_identity(identity['pid'])!=identity):
        raise ValueError('Fixed campaign controller is not live')
    lock=(root/'.runtime/product-campaign.lock').stat()
    for entry in Path(f"/proc/{identity['pid']}/fd").iterdir():
        try:info=entry.stat()
        except (FileNotFoundError,PermissionError):continue
        if info.st_dev==lock.st_dev and info.st_ino==lock.st_ino:return identity
    raise ValueError('Live fixed controller does not hold an open campaign-lock descriptor')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run',type=Path,required=True)
    parser.add_argument('--active-campaign',type=Path)
    parser.add_argument('--expected-state',help='Optional SHA256 of the observer pinned state; checked after both locks')
    parser.add_argument('--apply',action='store_true')
    args=parser.parse_args();os.umask(0o077)
    if args.expected_state is not None and not re.fullmatch('[a-f0-9]{64}',args.expected_state):parser.error('SHA256 expected state required')
    if os.name!='posix' or not Path('/proc/self').exists():parser.error('Linux owned-process manager required')
    base=args.run
    if (base.parent!=ROOT.parent or not base.name.startswith('evidence-api-worker-recovery-')
        or not base.is_absolute() or base.resolve()!=base):parser.error('New owned absolute recovery run required')
    base.mkdir(mode=0o700,exist_ok=False)
    report={'phase':'preflight','passed':False,'events':[],'apply_requested':args.apply,
        'service_mutation_attempted':False,'worker_restarted':False,'new_cloud_resources':False,
        'engine_actions':False,'api_actions':False,'source_or_environment_changed':False,
        'controller_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    def event(phase):
        report['phase']=phase;report['events'].append({'phase':phase,'at':datetime.now(timezone.utc).isoformat()})
        atomic_bytes(base/'validation.json',(json.dumps(report,indent=2)+'\n').encode())
    event('preflight');db=None;http=None
    try:
        recovery_lock=(ROOT/'.runtime/worker-kv-recovery.lock').open('a');fcntl.flock(recovery_lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        campaign_lock=(ROOT/'.runtime/product-campaign.lock').open('a')
        try:fcntl.flock(campaign_lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:
            if args.active_campaign is None:raise ValueError('Another controller owns campaign boundary')
            report['fixed_campaign_identity']=campaign_holds_open_lock(ROOT,args.active_campaign)
        manifest,services,_=load_manifest(ROOT/'.runtime/services-primary.json',root=ROOT)
        if len(services)!=5 or {(s['role'],s['name']) for s in services}!=EXPECTED:raise ValueError('Expected current480 primary only')
        manifest_raw=private_read(manifest)
        worker=next(s for s in services if s['role']=='worker');engine=next(s for s in services if s['role']=='engine')
        watchdog={'role':'supervisor','name':'primary','env_file':str(ROOT.parent/'supervisor.env'),'python':str(ROOT.parent/'venv/bin/python')}
        def key(s):return s['role']+'-'+s['name']
        before={key(s):managed_identity(ROOT,s) for s in [*services,watchdog]}
        configs={Path(s['env_file']):private_read(Path(s['env_file'])) for s in [*services,watchdog]}
        environment=file_env(worker['env_file']);source=Path(environment.get('PYTHONPATH',''))
        require_engine_binding(environment,engine_port=engine['port'],worker_port=worker['port'])
        if (environment.get('PYTHONSAFEPATH')!='1' or not source.is_absolute() or source.resolve()!=source
            or source.name!='source' or source.parent.parent!=ROOT.parent
            or not source.parent.name.startswith('evidence-api-') or os.pathsep in str(source)):
            raise ValueError('Current safe-path immutable worker source required')
        receipt_raw=private_read(source.parent/'source-receipt.json');receipt=json.loads(receipt_raw)
        if environment.get('MODEL_NAME')!=MODEL or environment.get('WORKER_ADAPTIVE_KV_ENABLED')!='true':raise ValueError('Current adaptive480 worker only')
        probe=probe_source(worker['python'],source,environment,ROOT,receipt['files'])
        if (probe['slots']!=32 or probe['token_budget']!=164256 or probe['max_model_len']!=18440
            or probe['runtime_fingerprint']!=RUNTIME):raise ValueError('Current KV/runtime profile changed')
        engine_env=file_env(engine['env_file'])
        if engine_env.get('MODEL_NAME')!=MODEL or engine_env.get('ENGINE_TENSOR_PARALLEL_SIZE')!='4':raise ValueError('Current TP4 engine only')
        manager_sources={p:hashlib.sha256(p.read_bytes()).hexdigest() for p in [ROOT/'ops/manage.py',ROOT/'ops/supervise.py']}
        report['locked_state_sha256']=require_expected(state_document(manifest_raw,configs,receipt_raw,manager_sources,before),args.expected_state)
        def guard(check_worker=False):
            if private_read(manifest)!=manifest_raw or any(private_read(p)!=raw for p,raw in configs.items()):raise ValueError('Current manifest/environment changed')
            if any(hashlib.sha256((source/n).read_bytes()).hexdigest()!=sha for n,sha in receipt['files'].items()):raise ValueError('Immutable source changed')
            if any(hashlib.sha256(p.read_bytes()).hexdigest()!=sha for p,sha in manager_sources.items()):raise ValueError('Live manager source changed')
            for s in services:
                if s['role']=='worker' and not check_worker:continue
                if managed_identity(ROOT,s)!=before[key(s)]:raise ValueError('Owned service identity changed')
            if check_worker:
                require_effective_configuration(environment,Path(f"/proc/{before[key(worker)]['pid']}/environ").read_bytes())
        import psycopg,httpx
        api=file_env(next(s['env_file'] for s in services if s['role']=='api'))
        db=psycopg.connect(api['DATABASE_URL'],autocommit=True,connect_timeout=5,
            options='-c default_transaction_read_only=on -c statement_timeout=5000')
        http=httpx.Client(trust_env=False,timeout=5)
        manager=PolicyManager(root=ROOT,timeout=30)
        def request(port,path):
            result=http.get(f'http://127.0.0.1:{port}'+path);result.raise_for_status()
            if len(result.content)>2*1024**2:raise ValueError('Recovery HTTP response exceeds bound')
            return result
        def observation():
            guard(True);started=datetime.now(timezone.utc).isoformat()
            rows=db.execute("SELECT count(*) FILTER (WHERE status='running'),count(*) FILTER (WHERE status='pending') FROM tasks WHERE status IN ('running','pending')").fetchone()
            metrics={}
            for line in request(worker['port'],'/metrics').text.splitlines():
                if line.startswith('batchforge_worker_') and len(line.split())==2:
                    name,value=line.split()
                    if name in metrics:raise ValueError('Duplicate recovery metric')
                    metrics[name]=float(value)
            return {'captured_at':started,'db_running':rows[0],'db_pending':rows[1],
                'worker_ready_status':http.get(f"http://127.0.0.1:{worker['port']}/readyz").status_code,
                'worker_metrics':{k:metrics.get(k) for k in ['batchforge_worker_adaptive_kv_paused','batchforge_worker_inflight','batchforge_worker_reserved_context_tokens','batchforge_worker_adaptive_kv_waiting']},
                'model_ids':[x['id'] for x in request(engine['port'],'/v1/models').json()['data']],
                'engine_version':request(engine['port'],'/version').json()['version'],
                'engine_quiescent':engine_quiescent(request(engine['port'],'/metrics').text)}
        def validate():
            first=observation();require_quiescence(first,now=datetime.now(timezone.utc));time.sleep(1)
            second=observation();require_quiescence(second,now=datetime.now(timezone.utc))
            report['quiescence_observations']=[first,second]
        validate()
        pending=[row[0] for row in db.execute("SELECT id FROM tasks WHERE status='pending' ORDER BY id LIMIT 50001").fetchall()]
        if not 1<=len(pending)<=50000:raise ValueError('Bounded durable pending identity set required')
        report.update(original_identities=before,worker_source_probe=probe,source_commit=receipt['git_commit'],
            pending_count=len(pending),pending_identity_sha256=hashlib.sha256('\n'.join(str(x) for x in pending).encode()).hexdigest(),
            worker_environment_sha256=hashlib.sha256(configs[Path(worker['env_file'])]).hexdigest())
        event('prepared')
        if not args.apply:return
        def charge():
            path=ROOT/'.runtime/worker-kv-recovery-budget.json'
            previous=json.loads(private_read(path)) if path.exists() else {'attempts':[]}
            attempts=charge_attempt(previous['attempts'],now=time.time())
            atomic_bytes(path,(json.dumps({'attempts':attempts})+'\n').encode())
            report['attempts_in_window']=len(attempts)
        def invoke(s,action):
            guard(s['role']=='worker' and action=='stop')
            report['service_mutation_attempted']=True
            _,error=manager.invoke(s,action)
            if error:raise RuntimeError('Owned service recovery operation failed')
        def watch(enabled):
            guard()
            if not enabled:
                if managed_identity(ROOT,watchdog)!=before[key(watchdog)]:raise ValueError('Watchdog identity changed')
            invoke(watchdog,'start' if enabled else 'stop')
        def start():
            invoke(worker,'start');wait_until(lambda:manager.readiness(worker),90,'Same-source worker not ready')
            if managed_identity(ROOT,worker)==before[key(worker)]:raise RuntimeError('Worker did not change recovery epoch')
            report['worker_restarted']=True
        def progressed():
            return db.execute("SELECT count(*) FROM tasks WHERE id=ANY(%s) AND status='completed'",(pending,)).fetchone()[0]>0
        def verify():
            wait_until(progressed,180,'Pending work made no observed progress');guard()
            if db.execute('SELECT count(*) FROM tasks WHERE id=ANY(%s)',(pending,)).fetchone()[0]!=len(pending):raise RuntimeError('Pending identities disappeared')
            if not manager.readiness(worker):raise RuntimeError('Recovered worker not ready')
            require_effective_configuration(environment,Path(f"/proc/{managed_identity(ROOT,worker)['pid']}/environ").read_bytes())
            report['recovered_worker_identity']=managed_identity(ROOT,worker)
            report['accepted_pending_identities_retained']=True
        try:
            result=recovery_sequence(validate=validate,charge=charge,watchdog=watch,stop_worker=lambda:invoke(worker,'stop'),
                guard_stopped=guard,start_worker=start,verify=verify,event=event)
            report['sequence']=result;report['passed']=True;event('complete')
        except RecoveryFailed as exc:
            report['sequence']=exc.report;raise
    except BaseException as exc:
        report.update(passed=False,error_type=type(exc).__name__,phase_before_failure=report['phase'])
        try:event('failed')
        except BaseException as logging:report['final_journal_error_type']=type(logging).__name__
        raise SystemExit(1) from None
    finally:
        for connection in (http,db):
            if connection is not None:
                try:connection.close()
                except BaseException:pass
        print(json.dumps({k:report.get(k) for k in ['phase','passed','apply_requested','service_mutation_attempted',
            'worker_restarted','error_type','engine_actions','api_actions']}),flush=True)


if __name__=='__main__':main()
