"""Finite real480B app-transport fault qualification, never a cloud/GPU reset.

Primary submissions stay gated. A private canary admits8 calibration and40 fault
tasks. Only the original worker timeout is temporarily reduced. An independent
pidfd guard must arm before an inference-confirmed engine API-parent STOP.
"""
from __future__ import annotations

import argparse
import copy
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import time

from ops.bounded_pause import BoundedPause
from ops.gpu_campaign import write_json
from ops.ha.retune_full_gpu import file_env
from ops.ha.set_scheduler_policy import PolicyManager, atomic_bytes
from ops.product_campaign import MODEL, managed_identity, wait_until
from ops.recover_latched_worker import EXPECTED, RUNTIME, require_effective_configuration, require_engine_binding
from ops.supervise import load_manifest, private_read
from worker.kv_admission import engine_quiescent

ROOT=Path('/root/batchforge-runtime/app')


def task_rows(template, count, label):
    if count not in (8,40) or template.get('body',{}).get('model')!=MODEL:
        raise ValueError('Fixed public calibration/fault fixture required')
    rows=[]
    for i in range(count):
        row=copy.deepcopy(template);row['custom_id']=f'{label}-{i:03d}'
        row['body'].update(max_tokens=64,min_tokens=64,temperature=0,seed=20261025)
        rows.append(row)
    return ('\n'.join(json.dumps(r,sort_keys=True,separators=(',',':')) for r in rows)+'\n').encode()


def inference_confirmed(worker, engine_metrics, running):
    """A new HTTP observation of actual running work, not readiness or PID alone."""
    active=[]
    for line in engine_metrics.splitlines():
        if line.startswith('vllm:num_requests_running') and not line.startswith('#'):
            try:active.append(float(line.rsplit(None,1)[1]))
            except (ValueError,IndexError):return False
    return (type(running) is int and running>0 and bool(active)
        and all(__import__('math').isfinite(v) and v>=0 for v in active) and sum(active)>0
        and worker.get('batchforge_worker_inflight',0)>0
        and worker.get('batchforge_worker_reserved_context_tokens',0)>0
        and worker.get('batchforge_worker_adaptive_kv_paused')==0)


def strict_receipt(envelope, count):
    from backend.receipts import verify
    r=envelope.get('receipt',{})
    if (not verify(envelope) or r.get('model')!=MODEL or r.get('ledger_state')!='consistent_ledger'
        or r.get('counts')!={'total':count,'pending':0,'running':0,'completed':count,'failed':0,'cancelled':0}
        or not r.get('checks') or any(v is not True for v in r['checks'].values())
        or r.get('accepted_usage',{}).get('completion_tokens')!=count*64
        or r.get('history',{}).get('observed_accepted_attempts')!=count
        or r.get('runtime_fingerprint')!=RUNTIME):
        raise RuntimeError('Strict acceptance, attempt, usage or runtime receipt failed')


def started(manager,root,service,*,identity=managed_identity):
    # A supervisor is a managed daemon, not an HTTP server. None from the
    # manager's HTTP readiness method is not evidence that daemon start failed.
    if service.get('port') is None:
        return identity(root,service).get('cwd')==str(root)
    return manager.readiness(service) is True


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run',type=Path,required=True)
    p.add_argument('--apply',action='store_true');args=p.parse_args();os.umask(0o077)
    base=args.run
    if (base.parent!=ROOT.parent or not base.name.startswith('soak480-latch-qualification-')
            or base.resolve()!=base or base.exists()):raise ValueError('New owned qualification run required')
    base.mkdir(mode=0o700)
    report={'phase':'preflight','passed':False,'events':[],'new_cloud_resources':False,
        'scope':'Application API-parent/transport fault, not a physical GPU fault',
        'planned_tasks':48,'calibration_tasks':8,'fault_tasks':40,
        'temporary_inference_timeout_seconds':30,'maximum_pause_seconds':45,
        'source_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    def event(phase):
        report['phase']=phase;report['events'].append({'phase':phase,'at':datetime.now(timezone.utc).isoformat()})
        write_json(base/'campaign.json',report)
    event('preflight')
    campaign_lock=(ROOT/'.runtime/product-campaign.lock').open('a')
    fcntl.flock(campaign_lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    recovery_lock=(ROOT/'.runtime/worker-kv-recovery.lock').open('a')
    fcntl.flock(recovery_lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    # The exact controller identity is required for the helper's campaign exception.
    from ops.manage import process_identity
    write_json(base/'launcher.json',{'identity':process_identity(os.getpid())})
    import psycopg,httpx
    _,services,_=load_manifest(ROOT/'.runtime/services-primary.json',root=ROOT)
    if len(services)!=5 or {(s['role'],s['name']) for s in services}!=EXPECTED:
        raise RuntimeError('Current primary manifest required')
    apis=[s for s in services if s['role']=='api'];worker=next(s for s in services if s['role']=='worker')
    engine=next(s for s in services if s['role']=='engine')
    watchdog={'role':'supervisor','name':'primary','env_file':str(ROOT.parent/'supervisor.env'),'python':str(ROOT.parent/'venv/bin/python')}
    key=lambda s:s['role']+'-'+s['name']
    before={key(s):managed_identity(ROOT,s) for s in [*services,watchdog]}
    originals={Path(s['env_file']):private_read(Path(s['env_file'])) for s in [*services,watchdog]}
    environments={key(s):file_env(s['env_file']) for s in services}
    env=environments[key(apis[0])];we=environments[key(worker)]
    require_engine_binding(we,engine_port=engine['port'],worker_port=worker['port'])
    require_effective_configuration(we,Path(f"/proc/{before[key(worker)]['pid']}/environ").read_bytes())
    if we.get('INFERENCE_TIMEOUT_SECONDS')!='1200' or we.get('PYTHONSAFEPATH')!='1':
        raise RuntimeError('Original long-timeout safe-path worker required')
    api_key=file_env(ROOT.parent/'access.env')['BATCHFORGE_API_KEY']
    db=psycopg.connect(env['DATABASE_URL'],autocommit=True,connect_timeout=5,
        options='-c default_transaction_read_only=on -c statement_timeout=5000')
    http=httpx.Client(trust_env=False,timeout=10,headers={'Authorization':'Bearer '+api_key})
    manager=PolicyManager(root=ROOT,timeout=30);pause=None;canary=None;touched=False;watch_stopped=False
    accepted=[];all_task_ids=[]
    def request(port,path,method='GET',**kwargs):
        result=http.request(method,f'http://127.0.0.1:{port}'+path,**kwargs);result.raise_for_status();return result
    def metrics():
        result={}
        for line in request(worker['port'],'/metrics').text.splitlines():
            if line.startswith('batchforge_worker_') and len(line.split())==2:
                k,v=line.split();result[k]=float(v)
        return result
    def counts():
        return db.execute("SELECT count(*) FILTER(WHERE status='pending'),count(*) FILTER(WHERE status='running') FROM tasks WHERE status IN ('pending','running')").fetchone()
    def drained():
        if counts()!=(0,0):return False
        m=metrics()
        return (all(m.get(k)==0 for k in ('batchforge_worker_inflight','batchforge_worker_reserved_context_tokens','batchforge_worker_adaptive_kv_waiting'))
            and engine_quiescent(request(engine['port'],'/metrics').text))
    def protected():
        for s in services:
            if s['role'] in ('engine','gateway'):
                if managed_identity(ROOT,s)!=before[key(s)] or private_read(Path(s['env_file']))!=originals[Path(s['env_file'])]:
                    raise RuntimeError('Protected engine/gateway identity or environment changed')
    def invoke(s,action):
        protected()
        if action=='stop':managed_identity(ROOT,s)
        _,error=manager.invoke(s,action)
        if error:raise RuntimeError('Owned service action failed')
        if action=='start':wait_until(lambda:started(manager,ROOT,s),90,'Service not ready')
    def configure(s,e):
        invoke(s,'stop')
        atomic_bytes(Path(s['env_file']),''.join(k+'='+shlex.quote(v)+'\n' for k,v in e.items()).encode())
        invoke(s,'start')
        require_effective_configuration(e,Path(f"/proc/{managed_identity(ROOT,s)['pid']}/environ").read_bytes())
    def stop_watch():
        nonlocal watch_stopped
        invoke(watchdog,'stop');watch_stopped=True
    def start_watch():
        nonlocal watch_stopped
        invoke(watchdog,'start');watch_stopped=False
    def submit(raw,label):
        # No automatic replay of an ambiguous upload. Batch create retries use one key.
        r=request(canary['port'],'/v1/files','POST',files={'file':(label+'.jsonl',raw,'application/jsonl')},data={'purpose':'batch'})
        fid=r.json()['id'];report.setdefault('uploads',[]).append({'file_id':fid,'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()});event('sustained_running')
        idem=base.name+'-'+label
        result=request(canary['port'],'/v1/batches','POST',json={'input_file_id':fid,'metadata':{'job_id':base.name,'cohort':label}},headers={'Idempotency-Key':idem})
        bid=result.json()['id'];accepted.append(bid);report['batches']=accepted.copy();event('sustained_running')
        # Same key, same body must resolve to the same accepted batch.
        replay=request(canary['port'],'/v1/batches','POST',json={'input_file_id':fid,'metadata':{'job_id':base.name,'cohort':label}},headers={'Idempotency-Key':idem})
        if replay.json()['id']!=bid:raise RuntimeError('Idempotent acceptance changed')
        return bid
    def done(bid):
        return db.execute("SELECT count(*) FROM tasks WHERE batch_id=%s AND status NOT IN ('completed','failed','cancelled')",(bid,)).fetchone()[0]==0
    def read_receipt(bid):
        envelope=request(apis[0]['port'],'/v1/batches/'+bid+'/receipt').json()
        strict_receipt(envelope,8 if accepted.index(bid)==0 else 40)
        write_json(base/('receipt-'+str(accepted.index(bid))+'.json'),envelope)
        return envelope
    try:
        from ops.worker_source import probe_source
        live_source=Path(we['PYTHONPATH']);source_receipt=json.loads(private_read(live_source.parent/'source-receipt.json'))
        report['live_worker_source_probe']=probe_source(worker['python'],live_source,we,ROOT,source_receipt['files'])
        probe=report['live_worker_source_probe']
        if (probe['model']!=MODEL or probe['slots']!=32 or probe['token_budget']!=164256
            or probe['max_model_len']!=18440 or probe['runtime_fingerprint']!=RUNTIME
            or environments[key(engine)].get('MODEL_NAME')!=MODEL
            or environments[key(engine)].get('ENGINE_TENSOR_PARALLEL_SIZE')!='4'):
            raise RuntimeError('Current480B TP4 exact-token32 profile required')
        tenant=db.execute('SELECT tenant_id FROM api_keys WHERE key_hash=%s AND NOT revoked',(hashlib.sha256(api_key.encode()).hexdigest(),)).fetchone()[0]
        headroom=db.execute('SELECT max_retained_tasks-retained_tasks FROM tenant_retention_limits JOIN tenant_retention_usage USING(tenant_id) WHERE tenant_id=%s',(tenant,)).fetchone()[0]
        if headroom<48:raise RuntimeError('Existing tenant has insufficient headroom')
        if (datetime(2026,10,26,9,tzinfo=timezone.utc)-datetime.now(timezone.utc)).total_seconds()<2400:
            raise RuntimeError('Insufficient qualification and recovery margin')
        raw=(ROOT.parent/'gpu-takeover-20261025/trial480-v2/workload.jsonl').read_bytes()
        if hashlib.sha256(raw).hexdigest()!='d18a1d60dfb2dd67e1913bc0acfb3bbcc55dbe03988305b3f66a04df22ed9bc2':
            raise RuntimeError('Public fixture changed')
        template=json.loads(raw.splitlines()[6])
        calibration=task_rows(template,8,'calibration');fault=task_rows(template,40,'fault')
        report.update(original_identities=before,retained_headroom=int(headroom),
            fixture_sha256=hashlib.sha256(raw).hexdigest(),
            cohorts=[{'count':8,'sha256':hashlib.sha256(calibration).hexdigest()},
                     {'count':40,'sha256':hashlib.sha256(fault).hexdigest()}])
        (base/'backups').mkdir(mode=0o700)
        for s in [*apis,worker]:(base/'backups'/(key(s)+'.env')).write_bytes(originals[Path(s['env_file'])])
        event('prepared')
        if not args.apply:return
        if not all(manager.readiness(s) for s in services):raise RuntimeError('Primary not ready')
        # A stop may succeed remotely and still lose its acknowledgement.
        # Mark maintenance intent first so finally always attempts watchdog start.
        touched=True;stop_watch()
        for s in apis:
            e=environments[key(s)].copy();e['BATCH_SUBMISSION_ENABLED']='false';configure(s,e)
            probe=http.post(f"http://127.0.0.1:{s['port']}/v1/batches",json={'input_file_id':'maintenance-probe'})
            if probe.status_code!=503 or probe.json()['error']['code']!='admission_paused':raise RuntimeError('Primary gate did not close')
        wait_until(drained,300,'Previous tasks did not drain')
        temporary=we.copy();temporary['INFERENCE_TIMEOUT_SECONDS']='30';configure(worker,temporary)
        canary={'role':'api','name':base.name.replace('soak480-','canary-'),'port':18158,
                'env_file':str(base/'canary.env'),'python':str(ROOT.parent/'venv/bin/python')}
        atomic_bytes(Path(canary['env_file']),originals[Path(apis[0]['env_file'])]);invoke(canary,'start')
        start_watch();event('sustained_running')
        began=time.monotonic();cal=submit(calibration,'calibration')
        wait_until(lambda:done(cal),120,'Calibration did not finish');wait_until(drained,60,'Calibration not quiescent')
        calibration_wall=time.monotonic()-began
        completed=db.execute("SELECT count(*) FROM tasks WHERE batch_id=%s AND status='completed'",(cal,)).fetchone()[0]
        if completed!=8 or calibration_wall>=20:raise RuntimeError('Normal calibration cannot fit temporary timeout safely')
        report['calibration_wall_seconds']=calibration_wall
        pause=BoundedPause(before[key(engine)],45)
        fault_bid=submit(fault,'fault')
        # Only this bounded controller submits to the private canary. Do not
        # spend a service-stop interval before proving dispatched inference.
        end=time.monotonic()+20;proof=None
        while time.monotonic()<end:
            m=metrics();em=request(engine['port'],'/metrics').text
            running=db.execute("SELECT count(*) FROM tasks WHERE batch_id=%s AND status='running'",(fault_bid,)).fetchone()[0]
            if inference_confirmed(m,em,running):
                proof={'captured_at':datetime.now(timezone.utc).isoformat(),'db_running':running,
                    'worker_inflight':m['batchforge_worker_inflight'],'worker_reserved':m['batchforge_worker_reserved_context_tokens'],
                    'engine_metrics_sha256':hashlib.sha256(em.encode()).hexdigest(),'engine_metrics':em}
                break
            time.sleep(.1)
        if proof is None:raise RuntimeError('No actual dispatched inference observed; refusing STOP')
        # Exact cohort identities are retained before any fault is injected.
        all_task_ids=[r[0] for r in db.execute('SELECT id FROM tasks WHERE batch_id=%s ORDER BY id',(fault_bid,)).fetchall()]
        if len(all_task_ids)!=40:raise RuntimeError('Fault cohort identity count differs')
        report['dispatch_proof']={k:v for k,v in proof.items() if k!='engine_metrics'}
        write_json(base/'dispatch-observation.json',proof);protected()
        pause.stop();report['pause_events']=pause.events.copy();event('sustained_running')
        invoke(canary,'stop');canary=None
        # The guardian resumes automatically after45s. Observe without extending it.
        time.sleep(max(0,pause.events[-1]['at_monotonic']+46-time.monotonic()))
        pause.resume();report['pause_events']=pause.events.copy();pause.close();pause=None
        event('sustained_running')
        until=time.monotonic()+120;observations=[]
        while time.monotonic()<until:
            m=metrics();pending,running=counts();em=request(engine['port'],'/metrics').text
            o={'at':datetime.now(timezone.utc).isoformat(),'pending':pending,'running':running,
               'inflight':m.get('batchforge_worker_inflight'),'reserved':m.get('batchforge_worker_reserved_context_tokens'),
               'waiting':m.get('batchforge_worker_adaptive_kv_waiting'),'paused':m.get('batchforge_worker_adaptive_kv_paused'),
               'engine_quiescent':engine_quiescent(em)}
            observations.append(o);write_json(base/'queue-report.json',{'observations':observations})
            if o['paused']==1 and pending>0 and running==0 and o['inflight']==0 and o['reserved']==0 and o['waiting']==0 and o['engine_quiescent']:break
            time.sleep(2)
        else:raise RuntimeError('A safely recoverable real latch was not established')
        report['recovery_detected_at']=datetime.now(timezone.utc).isoformat();event('sustained_running')
        # The helper owns the recovery lock; the parent retains the campaign lock.
        fcntl.flock(recovery_lock,fcntl.LOCK_UN)
        recovery_run=ROOT.parent/('evidence-api-worker-recovery-'+base.name.removeprefix('soak480-'))
        child=subprocess.run([sys.executable,'-m','ops.recover_latched_worker','--run',str(recovery_run),
            '--active-campaign',str(base),'--apply'],stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,timeout=360)
        fcntl.flock(recovery_lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        recovery=json.loads(private_read(recovery_run/'validation.json'));write_json(base/'recovery-validation.json',recovery)
        if child.returncode or not recovery.get('passed'):raise RuntimeError('Guarded same-config worker recovery failed')
        report['recovery_finished_at']=datetime.now(timezone.utc).isoformat();event('sustained_running')
        wait_until(lambda:all(done(bid) for bid in accepted),600,'Accepted cohort did not finish')
        if db.execute('SELECT count(*) FROM tasks WHERE id=ANY(%s)',(all_task_ids,)).fetchone()[0]!=40:
            raise RuntimeError('Fault cohort identities disappeared')
        for bid in accepted:read_receipt(bid)
        totals=db.execute("SELECT count(*),count(*) FILTER(WHERE status='completed'),sum(attempts)::bigint,sum(completion_tokens)::bigint FROM tasks WHERE batch_id=ANY(%s)",(accepted,)).fetchone()
        if totals[0]!=48 or totals[1]!=48 or totals[3]!=48*64:raise RuntimeError('Delivery/fixed-token conservation failed')
        report.update(delivered_tasks=totals[1],attempts=totals[2],output_tokens=totals[3],
            accepted_identities_preserved=True,qualified_recovery=True)
        event('qualification_passed')
    except BaseException as exc:
        report.update(error_type=type(exc).__name__,failed_phase=report['phase']);event('failed')
    finally:
        restore_errors=[]
        try:
            if pause is not None:
                pause.resume();report['pause_events']=pause.events.copy();pause.close()
        except BaseException as exc:restore_errors.append('resume_'+type(exc).__name__)
        try:
            if canary is not None:invoke(canary,'stop')
        except BaseException as exc:restore_errors.append('canary_'+type(exc).__name__)
        if touched:
            try:
                # Never change configuration beneath accepted active work.
                wait_until(drained,900,'Restore refuses undrained accepted work')
                fcntl.flock(recovery_lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
                if not watch_stopped:stop_watch()
                for s in [worker,*apis]:
                    current=file_env(s['env_file']);expected=environments[key(s)].copy()
                    if s['role']=='worker':expected['INFERENCE_TIMEOUT_SECONDS']='30'
                    else:expected['BATCH_SUBMISSION_ENABLED']='false'
                    if current not in (expected,environments[key(s)]):raise RuntimeError('Restore refuses foreign config changes')
                    invoke(s,'stop');atomic_bytes(Path(s['env_file']),originals[Path(s['env_file'])]);invoke(s,'start')
                    require_effective_configuration(environments[key(s)],Path(f"/proc/{managed_identity(ROOT,s)['pid']}/environ").read_bytes())
                report['original_configuration_restored']=True
            except BaseException as exc:restore_errors.append('config_'+type(exc).__name__)
            try:start_watch()
            except BaseException as exc:restore_errors.append('watchdog_'+type(exc).__name__)
        report['restore_errors']=restore_errors
        report['passed']=report.get('qualified_recovery',False) and not restore_errors and 'error_type' not in report
        report['final_identities']={key(s):managed_identity(ROOT,s) for s in [*services,watchdog]}
        event('prepared' if not args.apply and 'error_type' not in report else 'complete' if report['passed'] else 'failed')
        http.close();db.close()
    print(json.dumps({k:report.get(k) for k in ('phase','passed','qualified_recovery','delivered_tasks','restore_errors','error_type')}),flush=True)
    if args.apply and not report['passed']:raise SystemExit(1)


if __name__=='__main__':main()
