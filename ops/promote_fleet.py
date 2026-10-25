"""Prepare/apply an existing-Pod Fleet rollout, without engine/model actions.

A separate post-incident qualification is required. The failed 3600-task parent
is never promoted to a successful endurance result. All service changes use the
existing live manager, drained gates, pinned source and compatible rollback.
"""
from __future__ import annotations
import argparse
from datetime import datetime,timezone
import fcntl,hashlib,json,os
from pathlib import Path
import shlex,time
from ops.fleet_release import release_sequence,ReleaseFailed
from ops.product_campaign import MODEL,managed_identity,wait_until
from ops.supervise import load_manifest,private_read
from ops.ha.set_scheduler_policy import PolicyManager,atomic_bytes
from ops.ha.retune_full_gpu import file_env
from ops.worker_source import source_environment,probe_source
from worker.kv_admission import engine_quiescent
from backend.fleet import inventory_view
from backend.receipts import verify

ROOT=Path('/root/batchforge-runtime/app')
EXPECTED_SERVICES={('api','a'),('api','b'),('gateway','edge'),('engine','qwen480bench'),('worker','qwen480')}


def qualify_recovered_run(state,delivery,recovery,qualification):
    if (state.get('phase')!='failed' or state.get('error_type')!='RuntimeError'
        or state.get('exit_code')!=0 or delivery.get('passed') is not True
        or delivery.get('accepted_tasks')!=3600 or delivery.get('completed_tasks')!=3600
        or delivery.get('unfinished_tasks')!=0 or delivery.get('failed_tasks')!=0
        or not all(delivery.get(k) is True for k in ('accepted_exactly_once','accepted_usage_matches_task_usage','fixed_output_lengths_exact'))
        or recovery.get('passed') is not True or recovery.get('phase')!='complete'
        or recovery.get('quiescence_observed_twice') is not True
        or recovery.get('accepted_pending_identities_retained') is not True
        or recovery.get('engine_actions') is not False or recovery.get('api_actions') is not False
        or recovery.get('source_or_environment_changed') is not False
        or qualification.get('phase')!='complete' or qualification.get('managed_identities_unchanged') is not True
        or qualification.get('calibration',{}).get('delivery_passed') is not True
        or qualification.get('calibration',{}).get('evaluation_valid') is not True
        or qualification.get('calibration_paired',{}).get('tasks')!=100
        or qualification.get('service_actions') is not False
        or qualification.get('cloud_actions') is not False):
        raise ValueError('Closed failure, reconciled recovery and separate real delivery qualification required')
    original=state.get('original_identities',{})
    recovered=recovery.get('recovered_worker_identity')
    qualified=qualification.get('original_identities',{})
    expected={'api-a','api-b','gateway-edge','engine-qwen480bench','worker-qwen480'}
    if (set(original)!=expected or not isinstance(recovered,dict)
        or qualified.get('worker-qwen480')!=recovered
        or any(qualified.get(k)!=v for k,v in original.items() if k!='worker-qwen480')
        or any(recovery.get('original_identities',{}).get(k)!=v for k,v in original.items())):
        raise ValueError('Recovery and post-incident qualification identities do not reconcile')
    return {'uninterrupted_endurance_passed':False,'separate_post_incident_delivery_qualified':True,
        'squad_subset_gate_passed':qualification.get('passed') is True,
        'scope':'Fleet observation release only; original endurance and complete-dev quality failures remain'}


def env_bytes(value):return ''.join(k+'='+shlex.quote(v)+'\n' for k,v in value.items()).encode()


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run',type=Path,required=True);p.add_argument('--source-run',type=Path,required=True)
    p.add_argument('--qualification-run',type=Path,required=True);p.add_argument('--apply',action='store_true')
    args=p.parse_args();os.umask(0o077)
    base=args.run;candidate=args.source_run;qualification=args.qualification_run
    for path,prefix in [(base,'evidence-api-fleet-release-'),(candidate,'evidence-api-fleet-'),(qualification,'quality-squad-contract-')]:
        if not path.is_absolute() or path.parent!=ROOT.parent or not path.name.startswith(prefix) or path.resolve()!=path:
            p.error('Owned absolute nonsymlink run directories required')
    base.mkdir(mode=0o700,exist_ok=False)
    report={'phase':'preflight','passed':False,'events':[],'engine_actions':False,'new_cloud_resources':False,
        'quota_changes':False,'retention_changes':False,'controller_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    def event(phase):
        report['phase']=phase;report['events'].append({'phase':phase,'at':datetime.now(timezone.utc).isoformat()})
        atomic_bytes(base/'release.json',(json.dumps(report,indent=2)+'\n').encode())
    event('preflight');db=None
    try:
        lock=(ROOT/'.runtime/product-campaign.lock').open('a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        recovery_lock=(ROOT/'.runtime/worker-kv-recovery.lock').open('a');fcntl.flock(recovery_lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        old=ROOT.parent/'soak480-reuse-20261025-v1';rec=ROOT.parent/'evidence-api-reuse-recovery-20261025-v2'
        paths={'predecessor':old/'campaign.json','delivery':old/'.runtime/run/report.json','recovery':rec/'validation.json','qualification':qualification/'validation.json'}
        inputs={name:json.loads(path.read_bytes()) for name,path in paths.items()}
        report.update(qualify_recovered_run(inputs['predecessor'],inputs['delivery'],inputs['recovery'],inputs['qualification']))
        report['qualification_sha256']={name:hashlib.sha256(path.read_bytes()).hexdigest() for name,path in paths.items()}
        source=candidate/'source'
        if source.resolve()!=source:raise ValueError('Candidate source cannot be a symlink')
        receipt=json.loads((candidate/'source-receipt.json').read_bytes())
        proof=json.loads((candidate/'validation.json').read_bytes())
        if not all(proof.get(k) is True for k in ('passed','fleet_actual_azure_query_passed','database_read_only_verified','real_owner_authenticated','other_owner_hidden','invalid_key_rejected')):
            raise ValueError('Candidate Azure/Fleet isolation qualification missing')
        for name,sha in receipt['files'].items():
            if hashlib.sha256((source/name).read_bytes()).hexdigest()!=sha:raise ValueError('Pinned candidate changed')
        report['source_commit']=receipt['git_commit']
        manifest,services,_=load_manifest(ROOT/'.runtime/services-primary.json',root=ROOT)
        if len(services)!=5 or {(s['role'],s['name']) for s in services}!=EXPECTED_SERVICES:raise ValueError('Current primary manifest differs')
        manifest_raw=private_read(manifest)
        worker=next(s for s in services if s['role']=='worker');engine=next(s for s in services if s['role']=='engine')
        apis=[s for s in services if s['role']=='api'];gateway=next(s for s in services if s['role']=='gateway')
        watchdog={'role':'supervisor','name':'primary','env_file':str(ROOT.parent/'supervisor.env'),'python':str(ROOT.parent/'venv/bin/python')}
        manager=PolicyManager(root=ROOT,timeout=30)
        def key(s):return s['role']+'-'+s['name']
        identities={key(s):managed_identity(ROOT,s) for s in [*services,watchdog]}
        if not all(manager.readiness(s) for s in services):raise ValueError('Primary unready before release')
        if any(inputs['qualification']['original_identities'].get(key(s))!=identities[key(s)] for s in services):raise ValueError('Post-qualification identity differs')
        original={key(s):private_read(Path(s['env_file'])) for s in services}
        environments={key(s):file_env(s['env_file']) for s in services}
        if any(environments[key(s)].get('MODEL_NAME')!=MODEL for s in [worker,engine,*apis]):raise ValueError('Model identity differs')
        old_worker=environments[key(worker)]
        new_worker=source_environment(old_worker,source,node_id='batchforge-runpod-4x7ej43hrqznjn',gpu_indices=(0,1,2,3))
        probe=probe_source(worker['python'],source,new_worker,ROOT,receipt['files'])
        if (probe['slots']!=32 or probe['token_budget']!=164256 or probe['max_model_len']!=18440
            or probe['runtime_fingerprint']!=inputs['qualification']['runtime_fingerprint']):raise ValueError('Worker resource/runtime profile changed')
        report['worker_source_probe']=probe;report['original_identities']=identities
        (base/'backups').mkdir(mode=0o700)
        for name,raw in original.items():atomic_bytes(base/'backups'/(name+'.env'),raw)
        import psycopg,httpx
        db=psycopg.connect(environments[key(apis[0])]['DATABASE_URL'],autocommit=True,connect_timeout=5,
            options='-c default_transaction_read_only=on -c statement_timeout=5000')
        access=file_env(ROOT.parent/'access.env')['BATCHFORGE_API_KEY']
        http=httpx.Client(headers={'Authorization':'Bearer '+access},trust_env=False,timeout=10)
        def guard():
            if private_read(manifest)!=manifest_raw:raise RuntimeError('Manifest changed')
            for s in (engine,gateway):
                if managed_identity(ROOT,s)!=identities[key(s)] or private_read(Path(s['env_file']))!=original[key(s)]:raise RuntimeError('Protected process/config changed')
        def request(port,path):
            r=http.get('http://127.0.0.1:'+str(port)+path);r.raise_for_status()
            if len(r.content)>2*1024**2:raise RuntimeError('Readback bound exceeded')
            return r
        def gauge(body,name):
            import math
            values=[float(line.split()[-1]) for line in body.splitlines() if line.startswith(name+' ')]
            if len(values)!=1 or not math.isfinite(values[0]):raise ValueError('Missing/ambiguous worker gauge')
            return values[0]
        def quiet():
            guard()
            if db.execute("SELECT count(*) FROM tasks WHERE status IN ('pending','running')").fetchone()[0]:return False
            metrics=request(9105,'/metrics').text
            if any(gauge(metrics,k)!=0 for k in ('batchforge_worker_inflight','batchforge_worker_reserved_context_tokens','batchforge_worker_adaptive_kv_waiting')):return False
            return engine_quiescent(request(8348,'/metrics').text)
        def drain():
            wait_until(quiet,900,'Accepted work did not drain');time.sleep(1)
            if not quiet():raise RuntimeError('Quiescence not stable')
        def invoke(s,action):
            if action=='stop':managed_identity(ROOT,s)
            value,error=manager.invoke(s,action)
            if error:raise RuntimeError('Owned service operation failed')
            return value
        def restart(s):
            invoke(s,'stop');invoke(s,'start');wait_until(lambda:manager.readiness(s),90,'Application readiness failed')
        def configure_apis(paused,install):
            guard()
            for s in apis:
                env=dict(environments[key(s)],BATCH_SUBMISSION_ENABLED='false' if paused else 'true')
                if install:env.update(UVICORN_APP_DIR=str(source),FRONTEND_DIR=str(source/'frontend'),
                    QUALITY_EVIDENCE_CATALOGUE=str(source/'deploy/quality/catalogue.json'))
                atomic_bytes(Path(s['env_file']),env_bytes(env));restart(s)
                if paused:
                    r=http.post('http://127.0.0.1:'+str(s['port'])+'/v1/batches',json={'input_file_id':'maintenance-probe'})
                    if r.status_code!=503 or r.json().get('error',{}).get('code')!='admission_paused':raise RuntimeError('API admission gate not closed')
        def install_worker():
            drain();invoke(worker,'stop');guard();atomic_bytes(Path(worker['env_file']),env_bytes(new_worker))
            invoke(worker,'start');wait_until(lambda:manager.readiness(worker),90,'Candidate worker not ready')
        expected_devices={d['id'] for d in proof['independent_physical_inventory']['devices']}
        def worker_inventory():
            r=db.execute("SELECT metadata->'gpu_inventory' FROM workers WHERE id=%s",(old_worker['WORKER_ID'],)).fetchone()
            if not r or r[0] is None:return None
            inv=inventory_view(r[0]);age=(datetime.now(timezone.utc)-datetime.fromisoformat(inv['observed_at'])).total_seconds()
            return inv if -5<=age<=60 and {d['id'] for d in inv['devices']}==expected_devices else None
        def validate_worker():
            guard();managed_identity(ROOT,worker)
            if not manager.readiness(worker):raise RuntimeError('Worker not ready')
            effective=dict(part.partition(b'=')[::2] for part in Path(f"/proc/{managed_identity(ROOT,worker)['pid']}/environ").read_bytes().split(b'\0') if b'=' in part)
            if effective.get(b'PYTHONSAFEPATH')!=b'1' or effective.get(b'PYTHONPATH')!=str(source).encode():raise RuntimeError('Effective worker source differs')
            wait_until(lambda:worker_inventory() is not None,90,'Fresh physical inventory not persisted')
            report['registered_inventory']=worker_inventory()
        selected=json.loads((qualification/'run/report.json').read_bytes())['batch_ids'][0]
        def validate_apis():
            validate_worker()
            for port in (8082,8083,8080):
                fleet=request(port,'/v1/fleet').json()
                if len(fleet['data'])!=1 or fleet['data'][0]['fresh_device_count']!=4 or fleet['data'][0]['placement_capacity'] is not None or fleet['data'][0]['conflicts']:raise RuntimeError('Primary Fleet view differs')
                if not verify(request(port,'/v1/batches/'+selected+'/receipt').json()):raise RuntimeError('Strict historical receipt failed')
                if hashlib.sha256(request(port,'/app.js').content).hexdigest()!=receipt['files']['frontend/app.js']:raise RuntimeError('Console source differs')
                evaluations=request(port,'/v1/evaluations').json()
                published=evaluations.get('data',[])
                expected_quality=json.loads((source/'deploy/quality/catalogue.json').read_bytes())['evaluations']
                if {e['id'] for e in published}!={e['id'] for e in expected_quality}:raise RuntimeError('Quality catalogue differs')
                if any(e.get('verification_state')!='batch_ledger_reconciled' or e.get('cost',{}).get('kind')!='operator_rate_time_allocation' for e in published):
                    raise RuntimeError('Quality/cost ledger evidence not reconciled')
            report['primary_fleet_views_verified']=True
        def ensure_draining_worker():
            value,error=manager.invoke(worker,'status')
            if error:raise RuntimeError('Worker identity ambiguous during rollback')
            if not value['running']:invoke(worker,'start');wait_until(lambda:manager.readiness(worker),90,'Rollback drain worker not ready')
        def restore_worker():
            drain();invoke(worker,'stop');atomic_bytes(Path(worker['env_file']),original[key(worker)]);invoke(worker,'start')
            wait_until(lambda:manager.readiness(worker),90,'Original worker not ready')
        def validate_original():
            guard()
            if not all(manager.readiness(s) for s in services):raise RuntimeError('Original primary not ready')
            if private_read(Path(worker['env_file']))!=original[key(worker)]:raise RuntimeError('Original worker configuration differs')
        def reopen_original():
            for s in apis:atomic_bytes(Path(s['env_file']),original[key(s)]);restart(s)
        def watch(enabled):
            if not enabled:invoke(watchdog,'stop')
            else:invoke(watchdog,'start')
        # Source/qualification validation above is prepare-only; no service or
        # application config changes happen without the explicit apply flag.
        drain();event('prepared')
        if not args.apply:return
        result=release_sequence(preflight=guard,watchdog=watch,pause=lambda:configure_apis(True,False),drain=drain,
            install_worker=install_worker,validate_worker=validate_worker,install_apis=lambda:configure_apis(True,True),
            validate_apis=validate_apis,reopen=lambda:configure_apis(False,True),
            pause_compatible_apis=lambda:configure_apis(True,False),ensure_draining_worker=ensure_draining_worker,
            restore_worker=restore_worker,validate_original=validate_original,reopen_original=reopen_original,event=event)
        report['sequence']=result;guard();report['final_identities']={key(s):managed_identity(ROOT,s) for s in [*services,watchdog]}
        report['passed']=True;event('complete')
    except BaseException as exc:
        report.update(phase_before_failure=report['phase'],error_type=type(exc).__name__,passed=False)
        if isinstance(exc,ReleaseFailed):report['sequence']=exc.report
        event('failed')
        raise SystemExit(1) from None
    finally:
        if db is not None:db.close()
        print(json.dumps({k:report.get(k) for k in ('phase','passed','error_type','engine_actions','primary_fleet_views_verified')}),flush=True)


if __name__=='__main__':main()
