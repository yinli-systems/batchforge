"""Read-only ASGI/real-Postgres qualification without opening an app port.

No application lifespan, background maintenance, admission, inference, DDL,
service control or credential write is performed. Reports contain fixed fields.
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime,timezone
import hashlib
import json
import os
from pathlib import Path
import shlex

import httpx
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from backend import app as app_module,store as store_module,evaluations as evidence_module
from backend.config import Settings
from backend.migrate import verify_schema
from backend.store import APIError,Store
from ops.supervise import private_read


def environment(path):
    result={}
    for line in private_read(path).decode().splitlines():
        line=line.strip()
        if not line or line.startswith('#'):continue
        line=line.removeprefix('export ');key,separator,value=line.partition('=')
        if not separator or not key.replace('_','').isalnum():raise ValueError('Invalid private environment')
        result[key]=' '.join(shlex.split(value,comments=True))
    return result


async def qualify(settings,key):
    root=Path(__file__).resolve().parent.parent
    modules=[app_module,store_module,evidence_module]
    if any(not Path(m.__file__).resolve().is_relative_to(root) for m in modules):
        raise RuntimeError('Backend origins differ from the immutable source')
    options='-c default_transaction_read_only=on -c statement_timeout=10000 -c lock_timeout=1000'
    async with AsyncConnectionPool(settings.database_url,min_size=1,max_size=2,open=False,timeout=5,
        kwargs={'autocommit':True,'row_factory':dict_row,'connect_timeout':5,'options':options}) as pool:
        async with pool.connection() as conn:
            mode=(await (await conn.execute('SHOW default_transaction_read_only')).fetchone())['default_transaction_read_only']
            if mode!='on':raise RuntimeError('Read-only connection required')
            if conn.info.get_parameters().get('sslmode')!='verify-full':raise RuntimeError('Verified TLS hostname mode required')
            tls=await (await conn.execute('SELECT ssl,version FROM pg_stat_ssl WHERE pid=pg_backend_pid()')).fetchone()
            if not tls or not tls['ssl']:raise RuntimeError('TLS connection required')
            schema=await verify_schema(conn,minimum_version=6)
        store=Store(pool,settings,schema_version=schema)
        if not store.quality_evidence or not store.quality_evidence.records:raise RuntimeError('Published evidence required')
        app=app_module.create_app(settings);app.state.store=store
        # ASGITransport deliberately does not enter the production lifespan.
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://quality-review.invalid',
                headers={'Authorization':'Bearer '+key},trust_env=False,timeout=20) as client:
            response=await client.get('/v1/evaluations');response.raise_for_status();views=response.json()['data']
            if not views or len(views)!=len(store.quality_evidence.records):raise RuntimeError('Owner cohort missing')
            if any(v['verification_state']!='batch_ledger_reconciled' for v in views):raise RuntimeError('Batch ledger mismatch')
            checks=[]
            for view in views:
                response=await client.get('/v1/evaluations/'+view['id']);response.raise_for_status()
                detail=response.json();expected=next(r for r in store.quality_evidence.records if r.id==view['id'])
                if set(detail['batch_ids'])!=set(expected.batch_ids):raise RuntimeError('Cohort changed')
                digest=hashlib.sha256(response.content).hexdigest()
                if response.headers.get('X-BatchForge-Evidence-SHA256')!=digest:raise RuntimeError('Response digest differs')
                if response.headers.get('Cache-Control')!='no-store':raise RuntimeError('Evidence must not be cached')
                checks.append({'id':view['id'],'quality_state':view['quality_state'],'tasks':view['tasks'],
                    'batch_count':view['batch_count'],'report_sha256':view['report_sha256'],
                    'response_sha256':digest,'verification_state':view['verification_state']})
            if (await client.get('/v1/evaluations',headers={'Authorization':''})).status_code!=401:
                raise RuntimeError('Unauthenticated list was not rejected')
            if (await client.get('/v1/evaluations',headers={'Authorization':'Bearer invalid-review-key'})).status_code!=401:
                raise RuntimeError('Invalid key was not rejected')
            fleet=await client.get('/v1/fleet');fleet.raise_for_status()
            if fleet.headers.get('Cache-Control')!='no-store':raise RuntimeError('Fleet must not be cached')
            if (await client.get('/v1/fleet',headers={'Authorization':''})).status_code!=401:
                raise RuntimeError('Fleet authentication required')
            response=await client.get('/');response.raise_for_status()
            if 'data-view="quality"' not in response.text:raise RuntimeError('Quality console navigation missing')
            response=await client.get('/app.js');response.raise_for_status()
            if hashlib.sha256(response.content).hexdigest()!=hashlib.sha256((root/'frontend/app.js').read_bytes()).hexdigest():
                raise RuntimeError('Frontend source differs')
        if (await store.list_evaluations('nonexistent-quality-review-tenant'))['data']:
            raise RuntimeError('Another owner saw evidence')
        try:await store.get_evaluation('nonexistent-quality-review-tenant',views[0]['id'])
        except APIError as exc:
            if exc.status!=404:raise
        else:raise RuntimeError('Another owner fetched evidence')
    paths=['backend/app.py','backend/store.py','backend/evaluations.py','backend/fleet.py','backend/config.py','frontend/app.js']
    return {'passed':True,'captured_at':datetime.now(timezone.utc).isoformat(),'schema_version':schema,
        'database_read_only_verified':True,'tls_verified_hostname_mode':True,'tls_version':tls['version'],
        'real_owner_authenticated':True,'other_owner_hidden':True,'invalid_key_rejected':True,
        'evidence':checks,'source_sha256':{p:hashlib.sha256((root/p).read_bytes()).hexdigest() for p in paths},
        'fleet_actual_azure_query_passed':True,'fleet_reported_nodes':len(fleet.json()['data']),
        'services_changed':False,'inference_requests':0,'application_port_opened':False,
        'scope':'Real Azure with source-isolated ASGI requests; production lifespan and background maintenance were not entered; no browser visual claim'}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--api-env',type=Path,required=True);p.add_argument('--access-env',type=Path,required=True)
    p.add_argument('--catalogue',type=Path,required=True);p.add_argument('--out',type=Path,required=True)
    p.add_argument('--inventory-device-indices');p.add_argument('--inventory-node-id')
    args=p.parse_args();os.umask(0o077)
    if args.out.exists():p.error('Preserve the closed report; select a new output path')
    try:
        values=environment(args.api_env);key=environment(args.access_env)['BATCHFORGE_API_KEY']
        options={k.lower():v for k,v in values.items()}
        options.update(quality_evidence_catalogue=str(args.catalogue),frontend_dir=str(Path(__file__).resolve().parent.parent/'frontend'),
            blob_storage_backend='inline',result_readers_enabled=False,result_archive_enabled=False,blob_gc_enabled=False)
        settings=Settings(**options)
        async def bounded():
            async with asyncio.timeout(60):return await qualify(settings,key)
        result=asyncio.run(bounded())
        if args.inventory_device_indices:
            from worker.inventory import collect,selection
            if not args.inventory_node_id:raise ValueError('Physical inventory requires explicit operator node identity')
            result['independent_physical_inventory']=collect(args.inventory_node_id,selection(args.inventory_device_indices))
            result['physical_inventory_registered']=False
    except Exception as exc:
        result={'passed':False,'error_type':type(exc).__name__,'requested_mode':'Read-only source-isolated ASGI qualification'}
    with args.out.open('x') as f:
        json.dump(result,f,indent=2,allow_nan=False);f.write('\n');f.flush();os.fsync(f.fileno())
    print(json.dumps({'passed':result['passed'],'error_type':result.get('error_type'),'report':str(args.out)}))
    if not result['passed']:raise SystemExit(1)


if __name__=='__main__':main()
