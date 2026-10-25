"""Release qualified read-only quality features after the fixed GPU run closes.

Only app-owned API processes and their existing watchdog can be restarted.
No cloud action, GPU/model switch, schema change, cancellation or history purge.
"""
from __future__ import annotations

import argparse
from datetime import datetime,timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shlex
import signal
import socket
import time

from ops.product_campaign import MODEL,managed_identity,rollout_sequence,wait_until
from ops.ha.set_scheduler_policy import PolicyManager,atomic_bytes
from ops.ha.retune_full_gpu import file_env
from ops.supervise import load_manifest,private_read
from ops.gpu_campaign import write_json


def require_closed_endurance(state,report):
    count=report.get('configuration',{}).get('tasks')
    if (state.get('phase')!='complete' or report.get('passed') is not True or count!=2400
        or report.get('accepted_tasks')!=count or report.get('completed_tasks')!=count
        or report.get('unfinished_tasks')!=0 or report.get('accepted_exactly_once') is not True
        or report.get('accepted_usage_matches_task_usage') is not True):
        raise RuntimeError('Successful closed endurance evidence required before release')


def release_environment(original,source,*,paused,canary=False):
    env=original.copy()
    env.update(BATCH_SUBMISSION_ENABLED='false' if paused else 'true',UVICORN_APP_DIR=str(source),
               FRONTEND_DIR=str(source/'frontend'),QUALITY_EVIDENCE_CATALOGUE=str(source/'deploy/quality/catalogue.json'))
    if canary:env.update(DB_POOL_MIN='1',DB_POOL_MAX='2',BLOB_GC_ENABLED='false',RESULT_ARCHIVE_ENABLED='false')
    return env


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source-run',type=Path,required=True);p.add_argument('--wait-endurance',type=Path,required=True)
    p.add_argument('--run',type=Path,required=True)
    args=p.parse_args();os.umask(0o077)
    def interrupted(_signal,_frame):raise InterruptedError('Release interrupted')
    signal.signal(signal.SIGTERM,interrupted)
    root=Path('/root/batchforge-runtime/app');base=args.run.resolve();candidate=args.source_run.resolve();endurance=args.wait_endurance.resolve()
    if (base.parent!=root.parent or not base.name.startswith('quality-api-release-') or base.exists()
        or candidate.parent!=root.parent or not candidate.name.startswith('quality-api-review-')
        or endurance.parent!=root.parent or not endurance.name.startswith('soak480-')):
        raise ValueError('Owned new release and existing qualification/endurance paths required')
    base.mkdir(mode=0o700)
    report={'phase':'waiting_endurance','started_at':datetime.now(timezone.utc).isoformat(),
        'source_run':str(candidate),'endurance_run':str(endurance),'events':[],
        'controller_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        'cloud_resources_changed':False,'gpu_services_changed':False,'schema_changed':False}
    def event(phase):
        report['phase']=phase;report['events'].append({'phase':phase,'at':datetime.now(timezone.utc).isoformat()})
        write_json(base/'release.json',report)
    event('waiting_endurance');manager=PolicyManager(root=root,timeout=30);canary=None;canary_started=False
    try:
        source=candidate/'source';manifest=json.loads((candidate/'source-receipt.json').read_bytes())
        qualified=json.loads((candidate/'validation.json').read_bytes())
        if not all(qualified.get(k) is True for k in ['passed','database_read_only_verified','tls_verified_hostname_mode','real_owner_authenticated','other_owner_hidden']):
            raise RuntimeError('Read-only qualified source required')
        def unchanged():
            for name,digest in manifest['files'].items():
                if hashlib.sha256((source/name).read_bytes()).hexdigest()!=digest:raise RuntimeError('Qualified source changed')
        unchanged();report['source_commit']=manifest['git_commit']
        report['qualification_sha256']=hashlib.sha256((candidate/'validation.json').read_bytes()).hexdigest()
        def closed():
            now=datetime.now(timezone.utc)
            if (datetime(2026,10,26,9,tzinfo=timezone.utc)-now).total_seconds()<1800:
                raise RuntimeError('Release cannot fit before authorized stop')
            state=json.loads((endurance/'campaign.json').read_bytes())
            if state.get('phase') in ('failed','sustained_failed'):raise RuntimeError('Resolve failed endurance before release')
            path=endurance/'.runtime/run/report.json'
            if state.get('phase')!='complete' or not path.exists():return False
            require_closed_endurance(state,json.loads(path.read_bytes()))
            report['endurance_report_sha256']=hashlib.sha256(path.read_bytes()).hexdigest();return True
        wait_until(closed,10800,'Endurance did not close')
        lock=(root/'.runtime/product-campaign.lock').open('a+')
        def acquire():
            try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);return True
            except BlockingIOError:return False
        wait_until(acquire,60,'Another controller owns app release');unchanged()
        _,services,_=load_manifest(root/'.runtime/services-primary.json',root=root)
        expected={('api','a'),('api','b'),('gateway','edge'),('engine','qwen480bench'),('worker','qwen480')}
        if {(s['role'],s['name']) for s in services}!=expected or len(services)!=5:raise RuntimeError('Primary manifest changed')
        apis=sorted((s for s in services if s['role']=='api'),key=lambda s:s['name'])
        if [(s['name'],s['port']) for s in apis]!=[('a',8082),('b',8083)]:raise RuntimeError('API ports changed')
        watchdog={'role':'supervisor','name':'primary','env_file':str(root.parent/'supervisor.env'),'python':str(root.parent/'venv/bin/python')}
        identities={s['role']+'-'+s['name']:managed_identity(root,s) for s in [*services,watchdog]}
        known_identities=identities.copy()
        if not all(manager.readiness(s) for s in services):raise RuntimeError('Primary readiness failed')
        original={s['name']:private_read(Path(s['env_file'])) for s in apis};envs={s['name']:file_env(s['env_file']) for s in apis}
        if any(e.get('MODEL_NAME')!=MODEL or e.get('BATCH_SUBMISSION_ENABLED','true')!='true' for e in envs.values()):
            raise RuntimeError('Model or admission changed')
        key=file_env(root.parent/'access.env')['BATCHFORGE_API_KEY']
        import httpx,psycopg
        db=psycopg.connect(envs['a']['DATABASE_URL'],autocommit=True,connect_timeout=5,
            options='-c default_transaction_read_only=on -c statement_timeout=10000')
        def drained():return db.execute("SELECT coalesce(sum(pending_count+running_count),0) FROM batches WHERE status='in_progress'").fetchone()[0]==0
        tag=hashlib.sha256(str(base).encode()).hexdigest()[:12];cfg=root/'.runtime'/('env-quality-'+tag)
        canary={'role':'api','name':'quality-'+tag,'env_file':str(cfg),'python':str(root.parent/'venv/bin/python'),'port':18152}
        with socket.socket() as probe:probe.bind(('127.0.0.1',18152))
        if cfg.exists() or manager.invoke(canary,'status')[0]['running']:raise RuntimeError('Canary identity already exists')
        def env_bytes(e):return ''.join(k+'='+shlex.quote(v)+'\n' for k,v in e.items()).encode()
        atomic_bytes(cfg,env_bytes(release_environment(envs['a'],source,paused=True,canary=True)))
        start,error=manager.invoke(canary,'start');canary_started=True
        if error:raise RuntimeError('Canary start failed')
        wait_until(lambda:manager.readiness(canary),60,'Canary not ready')
        def validate(url):
            from backend.receipts import verify
            with httpx.Client(base_url=url,headers={'Authorization':'Bearer '+key},trust_env=False,timeout=30) as client:
                r=client.get('/v1/evaluations');r.raise_for_status();views=r.json()['data']
                if not views or any(v['verification_state']!='batch_ledger_reconciled' for v in views):raise RuntimeError('Quality cohort not linked')
                for view in views:
                    r=client.get('/v1/evaluations/'+view['id']);r.raise_for_status()
                    if r.headers.get('X-BatchForge-Evidence-SHA256')!=hashlib.sha256(r.content).hexdigest():raise RuntimeError('Quality response digest missing or different')
                bid=client.get('/v1/evaluations/'+views[0]['id']).json()['batch_ids'][0]
                r=client.get('/v1/batches/'+bid+'/receipt');r.raise_for_status();receipt=r.json()
                if not verify(receipt) or not receipt['receipt']['checks'].get('live_task_attempt_counts_match'):
                    raise RuntimeError('Stronger receipt source missing')
                r=client.get('/app.js');r.raise_for_status()
                if hashlib.sha256(r.content).hexdigest()!=manifest['files']['frontend/app.js']:raise RuntimeError('Frontend source differs')
                if client.get('/v1/evaluations',headers={'Authorization':''}).status_code!=401:raise RuntimeError('Quality auth failed')
            return [{'id':v['id'],'quality_state':v['quality_state'],'report_sha256':v['report_sha256']} for v in views]
        report['canary_evidence']=validate('http://127.0.0.1:18152');event('canary_passed')
        (base/'backups').mkdir(mode=0o700)
        for name,raw in original.items():(base/'backups'/('api-'+name+'.env')).write_bytes(raw)
        report['original_identities']=identities
        def restart(s):
            status,error=manager.invoke(s,'status')
            if error:raise RuntimeError('API identity status failed')
            identity_key=s['role']+'-'+s['name']
            if status['running']:
                if managed_identity(root,s)!=known_identities[identity_key]:raise RuntimeError('Unexpected API identity change')
                _,error=manager.invoke(s,'stop')
                if error:raise RuntimeError('API stop failed')
            _,error=manager.invoke(s,'start')
            if error:raise RuntimeError('API start failed')
            wait_until(lambda:manager.readiness(s),60,'API not ready')
            known_identities[identity_key]=managed_identity(root,s)
        def configure(paused,install):
            for s in apis:
                e=(release_environment(envs[s['name']],source,paused=paused) if install else {**envs[s['name']],'BATCH_SUBMISSION_ENABLED':'false' if paused else 'true'})
                atomic_bytes(Path(s['env_file']),env_bytes(e));restart(s)
        def pause():
            event('closing_admission');configure(True,False)
            for s in apis:
                with httpx.Client(base_url='http://127.0.0.1:'+str(s['port']),headers={'Authorization':'Bearer '+key},trust_env=False,timeout=20) as c:
                    for path,body in [('/v1/batches',{'input_file_id':'maintenance-probe'}),('/v1/batches/maintenance-probe/retry',None)]:
                        r=c.post(path,json=body)
                        if r.status_code!=503 or r.json()['error']['code']!='admission_paused':raise RuntimeError('All enqueue paths must be paused')
        def drain():event('draining');wait_until(drained,900,'Accepted work not drained')
        def install():event('installing');unchanged();configure(True,True)
        def verify_apis():
            for s in apis:validate('http://127.0.0.1:'+str(s['port']))
            report['gateway_evidence']=validate('http://127.0.0.1:8080')
        def reopen():event('reopening_admission');configure(False,True)
        def restore():
            event('restoring_api_configuration');errors=[]
            for s in apis:
                try:atomic_bytes(Path(s['env_file']),original[s['name']]);restart(s)
                except BaseException as exc:errors.append(type(exc).__name__)
            report['restore_errors']=errors
        def watch(enabled):
            if not enabled:managed_identity(root,watchdog)
            _,error=manager.invoke(watchdog,'start' if enabled else 'stop')
            if error:raise RuntimeError('Watchdog control failed')
        rollout_sequence(pause=pause,drain=drain,install=install,validate=verify_apis,reopen=reopen,restore=restore,watchdog=watch)
        for s in services:
            if s['role'] in ('worker','engine') and managed_identity(root,s)!=identities[s['role']+'-'+s['name']]:
                raise RuntimeError('GPU identity changed')
        db.close();report['primary_published']=True;report['passed']=True;event('primary_published')
    except BaseException as exc:
        report['phase_before_failure']=report['phase'];report['error_type']=type(exc).__name__;report['passed']=False;event('failed')
    finally:
        if canary_started:
            _,error=manager.invoke(canary,'stop');report['canary_stop_error']=error
            if error:
                report['passed']=False;event('cleanup_failed')
            else:write_json(base/'release.json',report)
        if 'db' in locals():db.close()
    if report.get('passed'):event('complete')
    print(json.dumps({'phase':report['phase'],'passed':report.get('passed',False),'report':str(base/'release.json')}))
    if not report.get('passed'):raise SystemExit(1)


if __name__=='__main__':main()
