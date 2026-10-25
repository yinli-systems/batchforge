"""Finite four-GPU endurance using an existing, independently read-back input.

Waits for the guarded API release. Never changes services, quotas, retention,
models or cloud resources. A failed predecessor blocks all new submissions.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime,timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shlex
import signal
import subprocess
import sys
import time

from ops.gpu_campaign import write_json
from ops.product_campaign import MODEL,managed_identity,wait_until
from ops.ha.retune_full_gpu import file_env
from ops.ha.set_scheduler_policy import PolicyManager
from ops.supervise import load_manifest
from scripts.sustained_benchmark import Database,render

FIXTURE_SHA256='d18a1d60dfb2dd67e1913bc0acfb3bbcc55dbe03988305b3f66a04df22ed9bc2'
RUNTIME='sha256:1c16be65ee46d160ffb33e5aee8af00d3d42e918b4253789219602eaf6768f36'


def verify_public_mixture(source,fixture):
    if hashlib.sha256(fixture).hexdigest()!=FIXTURE_SHA256:
        raise RuntimeError('Public fixture pin differs')
    expected=[json.loads(line) for line in fixture.splitlines() if line.strip()]
    rows=[json.loads(line) for line in source.splitlines() if line.strip()]
    def normalized(row,fixed):
        row={k:v for k,v in row.items() if k!='custom_id'}
        row['body']=dict(row['body'])
        if fixed:row['body']['min_tokens']=row['body'].get('max_tokens',row['body'].get('max_completion_tokens'))
        return render(row)
    if len(rows)!=20 or Counter(normalized(r,False) for r in rows)!=Counter(normalized(r,True) for r in expected):
        raise RuntimeError('Existing input is not the fixed public 20-cell mixture')
    if len({r['custom_id'] for r in rows})!=20:raise RuntimeError('Duplicate fixture custom ID')
    return sum(r['body'].get('max_tokens',r['body'].get('max_completion_tokens')) for r in rows)


def ready_release(report):
    if report.get('phase')=='failed':raise RuntimeError('Fleet release failed')
    if report.get('phase')!='complete':return False
    if report.get('passed') is not True or report.get('primary_fleet_views_verified') is not True or report.get('engine_actions') is not False:
        raise RuntimeError('Fleet release not verified')
    return True


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run',type=Path,required=True);parser.add_argument('--wait-release',type=Path,required=True)
    parser.add_argument('--predecessor',type=Path,required=True)
    args=parser.parse_args();os.umask(0o077)
    def interrupted(_signal,_frame):raise InterruptedError('Campaign interrupted')
    signal.signal(signal.SIGTERM,interrupted)
    root=Path('/root/batchforge-runtime/app');base=args.run.resolve();release=args.wait_release.resolve();old=args.predecessor.resolve()
    if (base.parent!=root.parent or not base.name.startswith('soak480-fleet-') or base.exists()
        or release.parent!=root.parent or not release.name.startswith('evidence-api-fleet-release-')
        or old.parent!=root.parent or not old.name.startswith('soak480-')):raise ValueError('Owned new campaign required')
    base.mkdir(mode=0o700)
    report={'phase':'waiting_release','started_at':datetime.now(timezone.utc).isoformat(),'events':[],
        'cost':None,'service_changes':False,'quota_changes':False,'retention_changes':False,'cloud_changes':False,
        'planned_tasks':800,'planned_batches':40,'arrival_rate':1,'max_inflight_tasks':128,
        'admission_seconds':3600,'drain_seconds':1800,'new_input_uploads':0,
        'comparison':'Independent endurance and artifact reuse; fixed order and offered load differ from prior study, not a controlled speedup'}
    def event(phase):
        report['phase']=phase;report['events'].append({'phase':phase,'at':datetime.now(timezone.utc).isoformat()})
        write_json(base/'campaign.json',report)
    event('waiting_release');db=None
    try:
        def released():
            if (datetime(2026,10,26,9,tzinfo=timezone.utc)-datetime.now(timezone.utc)).total_seconds()<5700:
                raise RuntimeError('Full admission and drain cannot fit before authorized stop')
            f=release/'release.json'
            return f.exists() and ready_release(json.loads(f.read_bytes()))
        wait_until(released,3600,'Qualified API release did not complete')
        lock=(root/'.runtime/product-campaign.lock').open('a+')
        def acquire():
            try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);return True
            except BlockingIOError:return False
        wait_until(acquire,60,'Another controller owns campaign')
        event('preflight')
        source=Path('/root/batchforge-runtime/evidence-api-fleet-20261025-v2/source')
        receipt=json.loads((source.parent/'source-receipt.json').read_bytes())
        for name,digest in receipt['files'].items():
            if hashlib.sha256((source/name).read_bytes()).hexdigest()!=digest:raise RuntimeError('Immutable campaign source differs')
        report['source_commit']=receipt['git_commit']
        report['controller_sha256']=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        report['controller_scope']='Derived frozen operator plan; immutable runtime and model code are unchanged'
        report['hourly_rate_source']='Operator confirmed $16.10 per existing whole-Pod hour on 2026-10-25; not invoice'
        report['automatic_live_latch_recovery']='Not deployed; latched admission requires independent quiescent worker recovery'
        report['source_files']=receipt['files']
        _,services,_=load_manifest(root/'.runtime/services-primary.json',root=root)
        if len(services)!=5 or {(s['role'],s['name']) for s in services}!={('api','a'),('api','b'),('gateway','edge'),('engine','qwen480bench'),('worker','qwen480')}:
            raise RuntimeError('Primary manifest changed')
        manager=PolicyManager(root=root,timeout=30)
        identities={s['role']+'-'+s['name']:managed_identity(root,s) for s in services}
        if not all(manager.readiness(s) for s in services):raise RuntimeError('Primary readiness failed')
        environments=[file_env(s['env_file']) for s in services if s['role']=='api']
        if any(e.get('MODEL_NAME')!=MODEL or e.get('BATCH_SUBMISSION_ENABLED','true')!='true' for e in environments):
            raise RuntimeError('Primary model or admission differs')
        env=environments[0];key=file_env(root.parent/'access.env')['BATCHFORGE_API_KEY']
        db=Database(env['DATABASE_URL'],key,base.name)
        if db.conn.execute("SELECT coalesce(sum(pending_count+running_count),0) AS active FROM batches WHERE status='in_progress'").fetchone()['active']:
            raise RuntimeError('Previous work not drained')
        quota=db.sample();retention=quota['retention']
        if retention['max_retained_tasks']-retention['retained_tasks']<800:raise RuntimeError('Existing task headroom insufficient')
        selected=db.conn.execute("""SELECT input_file_id,runtime_fingerprint FROM batches WHERE tenant_id=%s
            AND metadata->>'sustained_job'=%s AND status='completed' AND total_count=20 AND completed_count=20
            AND model=%s ORDER BY created_at DESC,id DESC LIMIT 1""",(db.tenant,old.name,MODEL)).fetchone()
        if not selected or selected['runtime_fingerprint']!=RUNTIME:raise RuntimeError('No verified predecessor fixture')
        fid=selected['input_file_id']
        import httpx
        chunks=[];size=0
        with httpx.Client(base_url='http://127.0.0.1:8080',headers={'Authorization':'Bearer '+key},trust_env=False,timeout=30,follow_redirects=False) as client:
            with client.stream('GET','/v1/files/'+fid+'/content') as response:
                response.raise_for_status()
                for chunk in response.iter_bytes():
                    size+=len(chunk)
                    if size>2*1024*1024:raise RuntimeError('Input exceeds bound')
                    chunks.append(chunk)
        data=b''.join(chunks);sha=hashlib.sha256(data).hexdigest()
        fixture=root.parent/'gpu-takeover-20261025/trial480-v2/workload.jsonl'
        output_per_batch=verify_public_mixture(data,fixture.read_bytes())
        db.reusable_input(fid,sha,len(data),time.time()+5700)
        report.update(input_file_id=fid,input_sha256=sha,input_bytes=len(data),public_fixture_sha256=FIXTURE_SHA256,
            planned_completion_tokens=40*output_per_batch,preflight_quota=quota,original_identities=identities)
        private=base/'.runtime';private.mkdir(mode=0o700);input_path=private/'input.jsonl';input_path.write_bytes(data)
        env_path=private/'sustained.env';env_path.write_text(''.join(k+'='+shlex.quote(v)+'\n' for k,v in {
            'DATABASE_URL':env['DATABASE_URL'],'BATCHFORGE_API_KEY':key,'BATCHFORGE_WORKER_TOKEN':env['BATCHFORGE_WORKER_TOKEN']}.items()))
        command=[str(root.parent/'venv/bin/python'),'-m','scripts.sustained_benchmark','--job-id',base.name,
            '--env-file',str(env_path),'--input-jsonl',str(input_path),'--reuse-input-file',fid,'--model',MODEL,
            '--state-dir',str(private/'run'),'--tasks','800','--tasks-per-batch','20','--arrival-rate','1',
            '--duration-seconds','3600','--drain-seconds','1800','--max-inflight-tasks','128',
            '--submission-concurrency','1','--fixed-output','--sample-seconds','5','--engine-url','http://127.0.0.1:8348',
            '--hourly-cost','16.10','--api','a=http://127.0.0.1:8082','--api','b=http://127.0.0.1:8083']
        report['command']=command;event('sustained_running');db.close();db=None
        with (base/'controller.log').open('xb') as log:
            result=subprocess.run(command,cwd=source,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,timeout=5700)
        report['exit_code']=result.returncode
        actual=json.loads((private/'run/report.json').read_bytes())
        if result.returncode or actual.get('passed') is not True or actual['input_reuse']['new_upload_requests']!=0:
            raise RuntimeError('Finite reuse endurance failed')
        for s in services:
            if managed_identity(root,s)!=identities[s['role']+'-'+s['name']]:raise RuntimeError('App identity changed')
        report['passed']=True;event('complete')
    except BaseException as exc:
        report['phase_before_failure']=report['phase'];report['error_type']=type(exc).__name__;report['passed']=False;event('failed')
    finally:
        if db is not None:db.close()
    print(json.dumps({'phase':report['phase'],'passed':report.get('passed',False)}))
    if not report.get('passed'):raise SystemExit(1)


if __name__=='__main__':main()
