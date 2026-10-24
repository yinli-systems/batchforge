"""Independent rolling-upgrade runtime identity regression tests.

Uses a fresh database, never changes deployment resources or shared fixtures.
"""
import os
from pathlib import Path
import secrets

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool
import pytest

from backend.config import Settings
from backend.store import Store, APIError


@pytest.fixture(scope='module')
def review_database(request):
    source=os.getenv('TEST_DATABASE_URL')
    if not source:pytest.skip('Requires isolated real PostgreSQL')
    name='bf_v6_review_'+secrets.token_hex(8)
    with psycopg.connect(source,autocommit=True) as conn:
        conn.execute(sql.SQL('CREATE DATABASE {} TEMPLATE template0').format(sql.Identifier(name)))
    database=make_conninfo(source,dbname=name)
    try:
        root=Path(__file__).resolve().parents[1]
        with psycopg.connect(database) as conn:
            conn.execute((root/'backend/schema.sql').read_text())
            for number in range(2,getattr(request,'param',7)+1):
                conn.execute(next((root/'backend/migrations').glob(f'{number:04d}_*.sql')).read_text())
        yield database
    finally:
        with psycopg.connect(source,autocommit=True) as conn:
            conn.execute(sql.SQL('DROP DATABASE {} WITH(FORCE)').format(sql.Identifier(name)))


def profile(fingerprint):
    return dict(profile_id='test',runtime_fingerprint=fingerprint,
        prefill_tokens_per_second=1000,decode_tokens_per_second=100,hourly_cost_usd=1,
        calibration_label='synthetic regression fixture',cost_source='test only')


def registration(fingerprint):
    return dict(worker_id='worker',name='worker',model='model',concurrency=1,
        metadata={'ready_engine_count':1,'scheduling_profile':profile(fingerprint)})


async def test_legacy_registration_cannot_preserve_incompatible_typed_identity(review_database):
    async with AsyncConnectionPool(review_database,min_size=1,max_size=2,
            kwargs={'row_factory':dict_row,'autocommit':True},open=False) as pool:
        current=Store(pool,Settings(scheduler_policy='cost_aware'),schema_version=7)
        await current.register(registration('numerics-v1'))
        legacy=Store(pool,Settings(scheduler_policy='legacy'),schema_version=7)
        try:
            await legacy.register(registration('numerics-v2'))
        except APIError:
            return  # Rejecting an unsafe registration is also fail-closed.
        async with pool.connection() as conn:
            actual=await (await conn.execute("SELECT runtime_fingerprint FROM workers WHERE id='worker'")).fetchone()
        assert actual['runtime_fingerprint'] != 'numerics-v1'


@pytest.mark.parametrize('fingerprint',['numerics-v1',None])
def test_post_migration_legacy_dispatch_cannot_falsely_certify_first_dispatch(review_database,fingerprint):
    with psycopg.connect(review_database,autocommit=True,row_factory=dict_row) as conn:
        conn.execute('TRUNCATE idempotency,attempts,tasks,batches,files,workers,tenant_queues,api_keys,tenants CASCADE')
        for worker,runtime in [('old-runtime',fingerprint),('new-runtime','numerics-v2')]:
            conn.execute("""INSERT INTO workers(id,name,model,concurrency,metadata,scheduler_profile_id,runtime_fingerprint,
                prefill_tokens_per_second,decode_tokens_per_second,hourly_cost_usd)
                VALUES(%s,%s,'model',1,%s,%s,%s,%s,%s,%s)""",
                (worker,worker,Jsonb({'ready_engine_count':1,**({'scheduling_profile':profile(runtime)} if runtime else {})}),
                 'test' if runtime else None,runtime,1000 if runtime else None,100 if runtime else None,1 if runtime else None))
        conn.execute("INSERT INTO tenants(id,name) VALUES('tenant','Tenant')")
        conn.execute("INSERT INTO files(id,tenant_id,filename,kind) VALUES('input','tenant','input.jsonl','input')")
        conn.execute("INSERT INTO batches(id,tenant_id,input_file_id,model,status,total_count,pending_count) VALUES('batch','tenant','input','model','in_progress',2,2)")
        conn.execute("INSERT INTO tenant_queues(tenant_id,model,pending_count) VALUES('tenant','model',2)")
        for name in ['a','b']:
            conn.execute("INSERT INTO tasks(id,tenant_id,batch_id,custom_id,model,body,status,max_attempts) VALUES(%s,'tenant','batch',%s,'model',%s,'pending',3)",(name,name,Jsonb({'messages':[],'max_tokens':32})))
        claimed=conn.execute("SELECT batchforge_claim('old-runtime','model',1,30) AS r").fetchone()['r']['tasks']
        assert len(claimed)==1
        conn.execute("SELECT batchforge_claim_scheduled('new-runtime','model',1,30,'fair',60,60)")
        identity=conn.execute("SELECT runtime_fingerprint,runtime_compatibility_scope FROM batches WHERE id='batch'").fetchone()
        assert identity['runtime_fingerprint']=='numerics-v1' or identity['runtime_compatibility_scope']=='legacy_unverified'


@pytest.mark.parametrize('change',['profile','model','remove-profile'])
def test_old_binary_registration_invalidates_stale_typed_profile(review_database,change):
    name='worker-'+secrets.token_hex(8)
    with psycopg.connect(review_database,autocommit=True,row_factory=dict_row) as conn:
        metadata={'ready_engine_count':1,'scheduling_profile':profile('numerics-v1')}
        conn.execute("""INSERT INTO workers(id,name,model,concurrency,metadata,scheduler_profile_id,runtime_fingerprint,
            prefill_tokens_per_second,decode_tokens_per_second,hourly_cost_usd)
            VALUES(%s,%s,'model',1,%s,'test','numerics-v1',1000,100,1)""",(name,name,Jsonb(metadata)))
        # Ordinary readiness refresh with unchanged calibration must preserve it.
        metadata['ready_engine_count']=0
        conn.execute('UPDATE workers SET metadata=%s WHERE id=%s',(Jsonb(metadata),name))
        assert conn.execute('SELECT runtime_fingerprint FROM workers WHERE id=%s',(name,)).fetchone()['runtime_fingerprint']=='numerics-v1'
        if change=='model':
            conn.execute("UPDATE workers SET model='changed-model' WHERE id=%s",(name,))
        else:
            if change=='profile':metadata['scheduling_profile']=profile('numerics-v2')
            else:metadata.pop('scheduling_profile')
            conn.execute('UPDATE workers SET metadata=%s WHERE id=%s',(Jsonb(metadata),name))
        actual=conn.execute('SELECT scheduler_profile_id,runtime_fingerprint,prefill_tokens_per_second,decode_tokens_per_second,hourly_cost_usd FROM workers WHERE id=%s',(name,)).fetchone()
        assert all(value is None for value in actual.values())


@pytest.mark.parametrize('review_database',[6],indirect=True)
def test_v7_backfill_repairs_prior_unrecorded_dispatch_and_profile_drift(review_database):
    with psycopg.connect(review_database,autocommit=True,row_factory=dict_row) as conn:
        original=profile('numerics-v1')
        conn.execute("""INSERT INTO workers(id,name,model,concurrency,metadata,scheduler_profile_id,runtime_fingerprint,
            prefill_tokens_per_second,decode_tokens_per_second,hourly_cost_usd)
            VALUES('old','old','model',1,%s,'test','numerics-v1',1000,100,1)""",(Jsonb({'scheduling_profile':original}),))
        conn.execute("INSERT INTO tenants(id,name) VALUES('tenant','Tenant')")
        conn.execute("INSERT INTO files(id,tenant_id,filename,kind) VALUES('input','tenant','input','input')")
        conn.execute("INSERT INTO batches(id,tenant_id,input_file_id,model,status,total_count,pending_count) VALUES('batch','tenant','input','model','in_progress',1,1)")
        conn.execute("INSERT INTO tenant_queues(tenant_id,model,pending_count) VALUES('tenant','model',1)")
        conn.execute("INSERT INTO tasks(id,tenant_id,batch_id,custom_id,model,body,status,max_attempts) VALUES('task','tenant','batch','task','model','{}','pending',3)")
        assert len(conn.execute("SELECT batchforge_claim('old','model',1,30) AS r").fetchone()['r']['tasks'])==1
        conn.execute('UPDATE workers SET metadata=%s WHERE id=%s',(Jsonb({'scheduling_profile':profile('numerics-v2')}),'old'))
        assert conn.execute("SELECT runtime_fingerprint FROM workers WHERE id='old'").fetchone()['runtime_fingerprint']=='numerics-v1'
        conn.execute((Path(__file__).resolve().parents[1]/'backend/migrations/0007_runtime_registration_guard.sql').read_text())
        assert conn.execute("SELECT runtime_fingerprint FROM workers WHERE id='old'").fetchone()['runtime_fingerprint'] is None
        assert conn.execute("SELECT runtime_compatibility_scope FROM batches WHERE id='batch'").fetchone()['runtime_compatibility_scope']=='legacy_unverified'


@pytest.mark.parametrize('outcome',['new','cached','failed'])
async def test_retry_cleanup_failure_preserves_committed_response_or_primary_error(review_database,tmp_path,outcome):
    async with AsyncConnectionPool(review_database,min_size=1,max_size=3,
            kwargs={'row_factory':dict_row,'autocommit':True},open=False) as pool:
        store=Store(pool,Settings(model_name='model',blob_storage_backend='local',
            blob_storage_root=str(tmp_path/'objects')),schema_version=7)
        suffix=secrets.token_hex(8);tenant='tenant-'+suffix;source='source-'+suffix
        async with pool.connection() as conn,conn.transaction():
            await conn.execute('INSERT INTO tenants(id,name) VALUES(%s,%s)',(tenant,tenant))
            await conn.execute("INSERT INTO files(id,tenant_id,filename,kind) VALUES(%s,%s,'source','input')",('input-'+suffix,tenant))
            await conn.execute("INSERT INTO batches(id,tenant_id,input_file_id,model,status,total_count,failed_count) VALUES(%s,%s,%s,'model','failed',1,1)",(source,tenant,'input-'+suffix))
            await conn.execute("INSERT INTO tasks(id,tenant_id,batch_id,custom_id,model,body,status,max_attempts) VALUES(%s,%s,%s,'one','model',%s,'failed',3)",
                ('task-'+suffix,tenant,source,Jsonb({'model':'model','messages':[{'role':'user','content':'test'}],'max_tokens':32})))
        key='once' if outcome=='cached' else None
        previous=None
        if outcome=='cached':
            previous=await store.retry(tenant,source,key)
            async def miss_fast_replay(*args):return None
            # Simulate another request committing between the optimistic replay
            # check and the final idempotency transaction.
            store.committed_response=miss_fast_replay
        if outcome=='failed':
            async def fail_creation(*args,**kwargs):raise APIError(400,'original creation error','review_error')
            store.create_rows=fail_creation
        cleanup_calls=[]
        async def fail_cleanup(*args):
            cleanup_calls.append(args)
            raise psycopg.OperationalError('injected cleanup outage')
        store.blobs.discard_unsubmitted=fail_cleanup
        if outcome=='failed':
            with pytest.raises(APIError,match='original creation error'):
                await store.retry(tenant,source,key)
            assert len(cleanup_calls)==1
        else:
            result=await store.retry(tenant,source,key)
            assert result['status']=='in_progress'
            if outcome=='cached':
                assert result['id']==previous['id'] and len(cleanup_calls)==1
            else:
                assert not cleanup_calls
            async with pool.connection() as conn:
                committed=await (await conn.execute('SELECT count(*) AS n FROM batches WHERE tenant_id=%s',(tenant,))).fetchone()
            assert committed['n']==2
