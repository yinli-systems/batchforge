"""Bounded same-Pod product rollout followed by real sustained GPU load.

No cloud resource operation, model switch, driver reset, quota change or history
deletion. A closed quality run is preserved before application API rollout.
"""
from __future__ import annotations

import argparse
import fcntl
from datetime import datetime,timezone
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import time

from ops.gpu_campaign import write_json

MODEL='QuantTrio/Qwen3-Coder-480B-A35B-Instruct-AWQ'


def rollout_sequence(*,pause,drain,install,validate,reopen,restore,watchdog):
    """A failed rollout restores compatible API config; GPU services stay alive."""
    try:
        watchdog(False)
        try:
            pause();drain();install();validate();reopen();validate()
        except BaseException:
            restore()
            raise
    finally:watchdog(True)


def managed_identity(root,service):
    """Validate the explicit live root, even when importing an immutable snapshot."""
    from ops.manage import process_identity
    from ops.supervise import private_read
    record=json.loads(private_read(root/'.runtime'/f"{service['role']}-{service['name']}.json"))
    if type(record.get('pid')) is not int:raise RuntimeError('invalid_managed_pid')
    expected={k:record.get(k) for k in ('pid','start_ticks','cwd')}
    if expected['cwd']!=str(root) or process_identity(record['pid'])!=expected:
        raise RuntimeError('managed_identity_changed')
    return expected


def wait_until(check,seconds,reason):
    end=time.monotonic()+seconds
    while time.monotonic()<end:
        if check():return
        time.sleep(2)
    raise RuntimeError(reason)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--run',type=Path,required=True);p.add_argument('--source-run',type=Path,required=True)
    p.add_argument('--wait-quality',type=Path,required=True)
    args=p.parse_args();os.umask(0o077)
    root=Path('/root/batchforge-runtime/app');base=args.run.resolve();candidate=args.source_run.resolve()
    quality=args.wait_quality.resolve()
    if (not base.name.startswith('soak480-') or base.parent!=root.parent or base.exists()
        or candidate.parent!=root.parent or not candidate.name.startswith('evidence-api-')
        or quality.parent!=root.parent or not quality.name.startswith('quality-squad-')):
        raise ValueError('Owned new runtime run and existing candidate/quality directories required')
    base.mkdir(mode=0o700)
    lock=(root/'.runtime/product-campaign.lock').open('a+')
    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    report={'phase':'waiting_quality','started_at':datetime.now(timezone.utc).isoformat(),
            'quality_run':str(quality),'candidate':str(candidate),'model':MODEL,
            'cost':None,'new_cloud_resources':False,'model_switch':False,'events':[]}
    def event(phase):
        report['phase']=phase;report['events'].append({'phase':phase,'at':datetime.now(timezone.utc).isoformat()})
        write_json(base/'campaign.json',report)
    event('waiting_quality')
    try:
        # Quality reports are closed by atomic rename, not by PID guesses.
        wait_until(lambda:(quality/'run/report.json').exists(),3900,'quality_report_not_closed')
        q=json.loads((quality/'run/report.json').read_bytes())
        report['quality_report_sha256']=hashlib.sha256((quality/'run/report.json').read_bytes()).hexdigest()
        report['quality_passed']=q['passed'];report['quality_evaluation_valid']=q.get('evaluation_valid',False)
        from ops.supervise import load_manifest,private_read
        from ops.ha.set_scheduler_policy import PolicyManager,atomic_bytes
        owned_identity=managed_identity
        from ops.ha.retune_full_gpu import file_env
        import httpx
        import psycopg
        proof=json.loads((candidate/'deployment.json').read_bytes());source=candidate/'source'
        if not proof['sql_validation_passed'] or not proof['http_validation_passed']:
            raise RuntimeError('candidate_not_qualified')
        for name,digest in proof['files'].items():
            if hashlib.sha256((source/name).read_bytes()).hexdigest()!=digest:
                raise RuntimeError('candidate_source_changed')
        _,services,_=load_manifest(root/'.runtime/services-primary.json',root=root)
        expected={('api','a'),('api','b'),('gateway','edge'),('engine','qwen480bench'),('worker','qwen480')}
        if {(s['role'],s['name']) for s in services}!=expected or len(services)!=5:
            raise RuntimeError('primary_manifest_changed')
        manager=PolicyManager(root=root,timeout=30)
        apis=[s for s in services if s['role']=='api']
        watchdog={'role':'supervisor','name':'primary','env_file':str(root.parent/'supervisor.env'),
                  'python':str(root.parent/'venv/bin/python')}
        identities={s['role']+'-'+s['name']:owned_identity(root,s) for s in [*services,watchdog]}
        if not all(manager.readiness(s) for s in services):raise RuntimeError('primary_not_ready')
        original={s['name']:private_read(Path(s['env_file'])) for s in apis}
        environments={s['name']:file_env(s['env_file']) for s in apis}
        if any(e.get('MODEL_NAME')!=MODEL or e.get('BATCH_SUBMISSION_ENABLED','true')!='true' for e in environments.values()):
            raise RuntimeError('primary_model_or_admission_changed')
        worker=next(s for s in services if s['role']=='worker');worker_env=file_env(worker['env_file'])
        if worker_env.get('MODEL_NAME')!=MODEL or worker_env.get('WORKER_ADAPTIVE_KV_ENABLED')!='true':
            raise RuntimeError('primary_worker_identity_changed')
        key=file_env(root.parent/'access.env')['BATCHFORGE_API_KEY'];api_env=environments[apis[0]['name']]
        db=psycopg.connect(api_env['DATABASE_URL'],autocommit=True,connect_timeout=5,
            options='-c default_transaction_read_only=on -c statement_timeout=10000')
        def drained():
            return db.execute("SELECT coalesce(sum(pending_count+running_count),0) FROM batches WHERE status='in_progress'").fetchone()[0]==0
        wait_until(drained,900,'previous_work_not_drained')
        fixture=root.parent/'gpu-takeover-20261025/trial480-v2/workload.jsonl'
        from scripts.sustained_benchmark import load_fixture,make_plan
        raw,rows=load_fixture(fixture,MODEL)
        if len(rows)!=20:raise RuntimeError('expected_20_cell_fixture')
        config={'tasks':2400,'tasks_per_batch':20,'seed':20261025,'arrival_rate':.5,
                'job_id':base.name,'fixed_output':True}
        plan=make_plan(config,rows);input_bytes=sum(item['input_bytes'] for item in plan)
        tenant=db.execute('SELECT tenant_id FROM api_keys WHERE key_hash=%s AND NOT revoked',
                          (hashlib.sha256(key.encode()).hexdigest(),)).fetchone()[0]
        retention=db.execute('SELECT l.max_retained_tasks,u.retained_tasks FROM tenant_retention_limits l JOIN tenant_retention_usage u USING(tenant_id) WHERE tenant_id=%s',(tenant,)).fetchone()
        blob=db.execute('SELECT stored_bytes,reserved_bytes FROM tenant_blob_usage WHERE tenant_id=%s',(tenant,)).fetchone()
        if retention[1]+2400>retention[0] or sum(blob)+input_bytes+4*1024**2>int(api_env.get('MAX_STORED_INPUT_BYTES_PER_TENANT',256*1024**2)):
            raise RuntimeError('existing_quota_cannot_fit_campaign')
        now=datetime.now(timezone.utc)
        stop=datetime(2026,10,26,9,tzinfo=timezone.utc)
        if (stop-now).total_seconds()<10500:raise RuntimeError('insufficient_time_before_authorized_stop')
        (base/'backups').mkdir(mode=0o700)
        for name,content in original.items():(base/'backups'/('api-'+name+'.env')).write_bytes(content)
        report.update(source_commit=proof['git_commit'],source_files=proof['files'],original_identities=identities,
                      input_fixture_sha256=hashlib.sha256(raw).hexdigest(),planned_tasks=2400,planned_input_bytes=input_bytes,
                      offered_tasks_per_second=.5,max_inflight_tasks=128,admission_seconds=7200,drain_seconds=1800)
        def restart(s):
            owned_identity(root,s)
            _,error=manager.invoke(s,'stop')
            if error:raise RuntimeError('api_stop_failed')
            _,error=manager.invoke(s,'start')
            if error:raise RuntimeError('api_start_failed')
            wait_until(lambda:manager.readiness(s),60,'api_readiness_failed')
        def configure(paused,install=False):
            for s in apis:
                e=environments[s['name']].copy();e['BATCH_SUBMISSION_ENABLED']='false' if paused else 'true'
                if install:e.update(UVICORN_APP_DIR=str(source),FRONTEND_DIR=str(source/'frontend'))
                atomic_bytes(Path(s['env_file']),''.join(k+'='+shlex.quote(v)+'\n' for k,v in e.items()).encode())
                restart(s)
        def pause():
            event('closing_admission');configure(True)
            for s in apis:
                with httpx.Client(base_url='http://127.0.0.1:'+str(s['port']),headers={'Authorization':'Bearer '+key},trust_env=False,timeout=20) as c:
                    r=c.post('/v1/batches',json={'input_file_id':'maintenance-probe'})
                    if r.status_code!=503 or r.json()['error']['code']!='admission_paused':raise RuntimeError('drain_gate_not_closed')
        def drain():event('draining');wait_until(drained,900,'drain_timeout')
        def install():event('installing_api');configure(True,True)
        def validate():
            from backend.receipts import verify
            selected=json.loads((candidate/'sql-validation.json').read_bytes())['batch_id']
            for s in apis:
                with httpx.Client(base_url='http://127.0.0.1:'+str(s['port']),headers={'Authorization':'Bearer '+key},trust_env=False,timeout=20) as c:
                    r=c.get('/v1/batches/'+selected+'/receipt');r.raise_for_status()
                    if not verify(r.json()):raise RuntimeError('primary_receipt_invalid')
                    r=c.get('/v1/capacity');r.raise_for_status()
                    if 'reservation_release' not in r.json():raise RuntimeError('capacity_source_not_current')
                    r=c.get('/app.js');r.raise_for_status()
                    if hashlib.sha256(r.content).hexdigest()!=proof['files']['frontend/app.js']:raise RuntimeError('frontend_source_differs')
        def reopen():event('reopening_admission');configure(False,True)
        def restore():
            event('restoring_api_config');errors=[]
            for s in apis:
                try:atomic_bytes(Path(s['env_file']),original[s['name']]);restart(s)
                except BaseException as exc:errors.append(type(exc).__name__)
            report['restore_errors']=errors
        def watch(enabled):
            if not enabled:owned_identity(root,watchdog)
            _,error=manager.invoke(watchdog,'start' if enabled else 'stop')
            if error:raise RuntimeError('watchdog_control_failed')
        rollout_sequence(pause=pause,drain=drain,install=install,validate=validate,reopen=reopen,restore=restore,watchdog=watch)
        for s in services:
            if s['role'] in ('worker','engine') and owned_identity(root,s)!=identities[s['role']+'-'+s['name']]:
                raise RuntimeError('gpu_service_identity_changed')
        event('api_rollout_passed')
        write_json(base/'rollout.json',report.copy())
        db.close()
        env_file=base/'sustained.env'
        env_file.write_text(''.join(k+'='+shlex.quote(v)+'\n' for k,v in {
            'DATABASE_URL':api_env['DATABASE_URL'],'BATCHFORGE_API_KEY':key,
            'BATCHFORGE_WORKER_TOKEN':api_env['BATCHFORGE_WORKER_TOKEN']}.items()))
        command=[str(root.parent/'venv/bin/python'),'-m','scripts.sustained_benchmark','--job-id',config['job_id'],
            '--env-file',str(env_file),'--input-jsonl',str(fixture),'--model',MODEL,'--state-dir',str(base/'.runtime/run'),
            '--tasks','2400','--tasks-per-batch','20','--arrival-rate','.5','--seed','20261025','--fixed-output',
            '--duration-seconds','7200','--drain-seconds','1800','--max-inflight-tasks','128',
            '--submission-concurrency','1','--sample-seconds','5','--engine-url','http://127.0.0.1:8348',
            '--api','a=http://127.0.0.1:8082','--api','b=http://127.0.0.1:8083']
        report['sustained_command']=command;event('sustained_running')
        result=subprocess.run(command,cwd=source,stdin=subprocess.DEVNULL,timeout=9300)
        report['sustained_exit_code']=result.returncode
        event('complete' if result.returncode==0 else 'sustained_failed')
    except BaseException as exc:
        report['error_type']=type(exc).__name__;event('failed')
        raise SystemExit(1)


if __name__=='__main__':main()
