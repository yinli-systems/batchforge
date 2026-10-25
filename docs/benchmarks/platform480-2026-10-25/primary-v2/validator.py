"""Finite real Batch API validation after the qualified primary 480B rollout.

No service mutation, schema change, provisioning or inference replay. Only public
SQuAD fixture text is submitted. Historical files are read with bounded buffers;
only identities and hashes are retained, never credentials or response text.
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace
import uuid


def require(value,reason):
    if not value:raise RuntimeError(reason)


def execute(base):
    root=Path('/root/batchforge-runtime/app')
    require(base.parent==root.parent and base.name.startswith('promotion480-'),'owned_promotion_required')
    sys.path.insert(0,str(root));sys.path.insert(0,str(base/'source-v1'))
    from ops.platform480 import audit_metering
    from ops.ha.retune_full_gpu import file_env
    from ops.supervise import Manager,load_manifest
    from scripts import benchmark
    import httpx,psycopg
    from psycopg.rows import dict_row
    model='QuantTrio/Qwen3-Coder-480B-A35B-Instruct-AWQ'
    rollout=json.loads((base/'rollout/report.json').read_text())
    require(rollout['phase']=='complete','successful_primary_rollout_required')
    require(not (base/'primary-smoke.json').exists(),'validation_already_exists_do_not_replay')
    work=base/'primary-validation';work.mkdir(mode=0o700,exist_ok=False)
    old=json.loads((base/'legacy-before.json').read_text())
    key=file_env(root.parent/'access.env')['BATCHFORGE_API_KEY']
    _,services,_=load_manifest(root/'.runtime/services-primary.json',root=root)
    api=next(s for s in services if s['role']=='api');e=file_env(api['env_file'])
    with httpx.Client(base_url='http://127.0.0.1:8080',headers={'Authorization':'Bearer '+key},timeout=30,trust_env=False) as client:
        for fid,expected in old['files'].items():
            digest=hashlib.sha256();size=0
            with client.stream('GET','/v1/files/'+fid+'/content') as response:
                response.raise_for_status()
                for chunk in response.iter_bytes():
                    size+=len(chunk);require(size<=2*1024*1024,'legacy_readback_size_bound')
                    digest.update(chunk)
            require({'sha256':digest.hexdigest(),'bytes':size}==expected,'legacy_file_changed_after_cutover')
    (base/'legacy-readback.json').write_text(json.dumps({**old,'verified_after_primary_cutover':True,
        'verified_files':len(old['files']),'observed_at':datetime.now(timezone.utc).isoformat()},indent=2)+'\n')
    rows=[json.loads(line) for line in (root.parent/'gpu-takeover-20261025/trial480-v2/workload.jsonl').read_text().splitlines()]
    require(len(rows)==20 and all(r['body']['model']==model for r in rows),'qualified_workload_required')
    for row in rows:row['body']['min_tokens']=row['body']['max_tokens']
    workload=work/'workload.jsonl';workload.write_text(''.join(json.dumps(r)+'\n' for r in rows))
    args=SimpleNamespace(url='http://127.0.0.1:8080',model=model,tasks=20,batches=1,max_tokens=2048,
        temperature=0,prompt='',engine_label='primary480-'+uuid.uuid4().hex[:12],deadline=1200,
        request_timeout=90,poll=.5,workload_dir=work/'inputs',input_jsonl=workload,
        output_dir=work/'outputs',submission_concurrency=1)
    os.environ['BATCHFORGE_API_KEY']=key
    result=asyncio.run(benchmark.run(args))
    (base/'primary-smoke.json').write_text(json.dumps(result,indent=2)+'\n')
    require(result['passed'] and not result['test_fixture_detected'],'real_primary_batch_validation_failed')
    with psycopg.connect(e['DATABASE_URL'],autocommit=True,row_factory=dict_row,connect_timeout=10,
            options='-c default_transaction_read_only=on -c statement_timeout=10000') as conn:
        tenant=conn.execute('SELECT tenant_id FROM api_keys WHERE key_hash=%s AND NOT revoked',
            (hashlib.sha256(key.encode()).hexdigest(),)).fetchone()['tenant_id']
        params=(result['batch_ids'],tenant)
        tasks=conn.execute('SELECT id,status,prompt_tokens,completion_tokens FROM tasks WHERE batch_id=ANY(%s) AND tenant_id=%s',params).fetchall()
        attempts=conn.execute('SELECT task_id,status,prompt_tokens,completion_tokens,worker_id FROM attempts WHERE batch_id=ANY(%s) AND tenant_id=%s',params).fetchall()
        batches=conn.execute('SELECT prompt_tokens,completion_tokens FROM batches WHERE id=ANY(%s) AND tenant_id=%s',params).fetchall()
        audit=audit_metering(tasks,attempts,batches)
    require(audit['tasks']==20 and audit['prompt_tokens']==109056 and audit['completion_tokens']==13600,
            'primary_metering_work_differs')
    require(all(a['worker_id'].startswith('qwen480-prod-') for a in attempts if a['status']=='completed'),
            'primary_tasks_not_accepted_by_promoted_worker')
    manager=Manager(root=root,timeout=30);health=[]
    for service in services:
        state,error=manager.invoke(service,'status')
        require(error is None and state['running'] and manager.readiness(service),'primary_owned_service_not_ready')
        health.append({k:service.get(k) for k in ['role','name','port']}|{'pid':state['pid'],'ready':True})
    proof={'passed':True,'observed_at':datetime.now(timezone.utc).isoformat(),'services':health,
        'primary_metering':audit,'legacy_files_verified':len(old['files']),
        'benchmark_sha256':hashlib.sha256((base/'primary-smoke.json').read_bytes()).hexdigest(),
        'validator_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        'cost':None,'limitations':['Finite functional smoke; not an additional controlled performance comparison','Private existing ingress, one four-H100 Pod']}
    (base/'post-rollout-health.json').write_text(json.dumps(proof,indent=2)+'\n')
    print(json.dumps(proof),flush=True)


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--base',type=Path,required=True)
    args=parser.parse_args();os.umask(0o077);execute(args.base.resolve())


if __name__=='__main__':main()
