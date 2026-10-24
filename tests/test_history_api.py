"""Real PostgreSQL + HTTP verification of schema 11 Store integration.

Every database, tenant, archive and fake worker belongs to this test module.
The worker reports deterministic fixture output; no GPU or external service is
used, and owner timestamp changes only accelerate age gates in the test DB.
"""
import asyncio
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
import secrets

from fastapi.testclient import TestClient
import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
import pytest

from backend.app import create_app
from backend.blob_storage import LocalBlobStore
from backend.config import Settings
from backend.history_retirement import configure_policy, history_page, idempotency_page
from backend.migrate import migrate_database
from backend.result_archival import ResultArchiver

SOURCE = os.environ.get('TEST_DATABASE_URL')
pytestmark = pytest.mark.skipif(not SOURCE, reason='Requires isolated real PostgreSQL')
TENANT_KEY = 'history-http-tenant-key-123456789'
OTHER_KEY = 'history-http-other-key-1234567890'
WORKER_TOKEN = 'history-http-worker-key-1234567'
PUBLIC = {'Authorization': 'Bearer '+TENANT_KEY}
OTHER = {'Authorization': 'Bearer '+OTHER_KEY}
INTERNAL = {'Authorization': 'Bearer '+WORKER_TOKEN}
MODEL = 'history-http-fixture-model'


@pytest.fixture
def database():
    name='bf_history_http_'+secrets.token_hex(8)
    with psycopg.connect(SOURCE,autocommit=True) as conn:
        conn.execute(sql.SQL('CREATE DATABASE {} TEMPLATE template0').format(sql.Identifier(name)))
    dsn=make_conninfo(SOURCE,dbname=name)
    try:yield dsn
    finally:
        with psycopg.connect(SOURCE,autocommit=True) as conn:
            conn.execute(sql.SQL('DROP DATABASE {} WITH(FORCE)').format(sql.Identifier(name)))


def settings(database,root):
    return Settings(database_url=database,batchforge_worker_token=WORKER_TOKEN,
        model_name=MODEL,blob_storage_backend='local',blob_storage_root=str(root),
        result_readers_enabled=True,result_archive_enabled=False,blob_gc_enabled=False,
        retention_admission_enabled=True,sql_completion_enabled=True,
        sql_claim_enabled=True,sql_recovery_enabled=True)


@pytest.fixture
def client(database,tmp_path):
    migrate_database(database,TENANT_KEY,target_version=11)
    with TestClient(create_app(settings(database,tmp_path/'objects'))) as c:
        c.database=database;c.objects=tmp_path/'objects'
        yield c


def connect(database):
    return psycopg.connect(database,autocommit=True,row_factory=dict_row,
                           options='-c statement_timeout=8000 -c lock_timeout=1000')


def upload(client,count=5,headers=PUBLIC):
    rows=[{'custom_id':'question-'+str(i),'method':'POST','url':'/v1/chat/completions',
        'body':{'model':MODEL,'messages':[{'role':'user','content':'public deterministic fixture '+str(i)}],
                'max_tokens':32}} for i in range(count)]
    content=''.join(json.dumps(row)+'\n' for row in rows).encode()
    result=client.post('/v1/files',headers=headers,files={'file':('fixture.jsonl',content)})
    assert result.status_code==200,result.text
    return result.json()['id']


def create(client,*,count=5,key=None,body=None):
    body=body or {'input_file_id':upload(client,count)}
    headers={**PUBLIC,**({'Idempotency-Key':key} if key else {})}
    result=client.post('/v1/batches',headers=headers,json=body)
    assert result.status_code==200,result.text
    return result.json(),body


def complete_batch(client,batch):
    registered=client.post('/internal/workers/register',headers=INTERNAL,json={
        'worker_id':'http-fixture-worker','name':'HTTP fixture worker','model':MODEL,'concurrency':64})
    assert registered.status_code==200,registered.text
    result=client.post('/internal/tasks/claim',headers=INTERNAL,json={
        'worker_id':'http-fixture-worker','model':MODEL,'limit':batch['request_counts']['total']})
    assert result.status_code==200,result.text
    claimed=result.json()['tasks']
    assert len(claimed)==batch['request_counts']['total']
    assert all(t['batch_id']==batch['id'] for t in claimed)
    for task in claimed:
        response={'model':MODEL,'choices':[{'message':{'role':'assistant','content':'deterministic answer'},'finish_reason':'stop'}],
                  'usage':{'prompt_tokens':5,'completion_tokens':3}}
        completed=client.post('/internal/tasks/'+task['id']+'/complete',headers=INTERNAL,
            json={'worker_id':'http-fixture-worker','attempt_token':task['attempt_token'],'response':response})
        assert completed.status_code==200,completed.text
    result=client.get('/v1/batches/'+batch['id'],headers=PUBLIC)
    assert result.status_code==200,result.text
    assert result.json()['status']=='completed'
    return result.json()


def age_cohort(client,batch):
    # No application endpoint can bypass age gates. An owner-controlled test
    # clock is modeled only inside this random disposable database.
    with connect(client.database) as conn:
        conn.execute("UPDATE batches SET created_at=clock_timestamp()-interval '120 seconds',completed_at=clock_timestamp()-interval '90 seconds' WHERE id=%s",(batch,))


async def maintenance(client,action,*,batch=None,limit=1000,floor=None,tenant='default'):
    async with AsyncConnectionPool(client.database,min_size=1,max_size=3,
            kwargs={'autocommit':True,'row_factory':dict_row},open=False) as pool:
        verifier=ResultArchiver(pool,LocalBlobStore(client.objects),max_archived_bytes_per_tenant=10000000)
        if action=='configure':
            return await configure_policy(pool,tenant,enabled=True,
                created_after=datetime.now(timezone.utc)-timedelta(seconds=180),
                min_age_seconds=60,replay_ttl_seconds=60)
        if action=='archive':return await verifier.archive_batch(tenant,batch)
        if action=='retire':return await history_page(pool,tenant,batch,limit=limit,apply=True,archiver=verifier)
        if action=='keys':return await idempotency_page(pool,tenant,floor,limit=limit,apply=True)
        raise AssertionError('Unknown test maintenance action')


def maintain(client,action,**args):return asyncio.run(maintenance(client,action,**args))


def overview(client,headers=PUBLIC):
    result=client.get('/v1/overview',headers=headers)
    assert result.status_code==200,result.text
    return result.json()


def counts(client):
    with connect(client.database) as conn:
        return conn.execute("""SELECT (SELECT count(*) FROM batches) AS batches,
          (SELECT count(*) FROM tasks) AS tasks,(SELECT count(*) FROM attempts) AS attempts,
          (SELECT count(*) FROM idempotency) AS keys""").fetchone()


def test_http_paged_retirement_preserves_results_usage_and_fences_retry(client):
    batch,_=create(client,count=5)
    terminal=complete_batch(client,batch)
    bid,oid=terminal['id'],terminal['output_file_id']
    age_cohort(client,bid)
    before=overview(client)
    raw=client.get('/v1/files/'+oid+'/content',headers=PUBLIC)
    assert raw.status_code==200 and len(raw.content.splitlines())==5
    assert before['stats']['completed_tasks']==5 and before['stats']['prompt_tokens']==25
    assert sum(b['tokens'] for b in before['throughput'])==40
    maintain(client,'configure')
    assert maintain(client,'archive',batch=bid)['purged']
    assert maintain(client,'retire',batch=bid,limit=2)['remaining_tasks']==3
    partial=client.get('/v1/batches/'+bid,headers=PUBLIC).json()
    assert partial['history_retention']['state']=='retiring'
    assert partial['history_retention']['retired_requests']==2
    assert partial['history_retention']['started_at'] and partial['history_retention']['retired_at'] is None
    for expected_remaining in (3,0):
        if expected_remaining==0:
            assert maintain(client,'retire',batch=bid,limit=1000)['remaining_tasks']==0
        current=overview(client)
        assert current['stats']==before['stats']
        assert current['throughput']==before['throughput']
        downloaded=client.get('/v1/files/'+oid+'/content',headers=PUBLIC)
        assert downloaded.status_code==200 and downloaded.content==raw.content
        previous=counts(client)
        retry=client.post('/v1/batches/'+bid+'/retry',headers=PUBLIC)
        assert retry.status_code==410,retry.text
        assert retry.json()['error']['code']=='history_retired'
        assert counts(client)==previous
    final=client.get('/v1/batches/'+bid,headers=PUBLIC).json()
    assert final['history_retention']['state']=='retired'
    assert final['history_retention']['retired_requests']==5
    assert final['history_retention']['retired_at']
    assert counts(client)['tasks']==counts(client)['attempts']==0
    with connect(client.database) as conn:
        assert conn.execute("SELECT retained_tasks,charged_payload_bytes FROM tenant_retention_usage WHERE tenant_id='default'").fetchone()=={'retained_tasks':0,'charged_payload_bytes':0}


def test_http_expired_bf1_replay_is_410_before_blob_io_without_new_work(client,monkeypatch):
    with connect(client.database) as conn:
        now=conn.execute("SELECT floor(extract(epoch FROM clock_timestamp()))::bigint AS n").fetchone()['n']
    key='bf1.'+str(now-120)+'.'+secrets.token_hex(8)
    batch,body=create(client,count=2,key=key)
    before=counts(client)
    replay=client.post('/v1/batches',headers={**PUBLIC,'Idempotency-Key':key},json=body)
    assert replay.status_code==200 and replay.json()==batch
    assert counts(client)==before
    complete_batch(client,batch)
    maintain(client,'configure')
    assert maintain(client,'keys',floor=now-61)['keys']==1
    before=counts(client)
    called=[]
    async def forbidden_blob_read(*args,**kwargs):
        called.append(True)
        raise AssertionError('Expired replay reached external blob I/O')
    monkeypatch.setattr(client.app.state.store.blobs,'read_input',forbidden_blob_read)
    for _ in range(2):
        result=client.post('/v1/batches',headers={**PUBLIC,'Idempotency-Key':key},json=body)
        assert result.status_code==410,result.text
        assert result.json()['error']['code']=='idempotency_expired'
        assert counts(client)==before
    assert not called


@pytest.mark.parametrize('key',['bf1.1000000000.aaaaaaaaaaaaaaaa','bf1.not-canonical','ordinary-legacy-key'])
def test_http_pre_v11_legacy_key_survives_upgrade_and_replay_floor(database,tmp_path,key):
    migrate_database(database,TENANT_KEY,target_version=10)
    app_settings=settings(database,tmp_path/'objects')
    with TestClient(create_app(app_settings)) as old:
        old.database=database;old.objects=tmp_path/'objects'
        batch,body=create(old,count=1,key=key)
        complete_batch(old,batch)
    migrate_database(database,target_version=11)
    with TestClient(create_app(app_settings)) as current:
        current.database=database;current.objects=tmp_path/'objects'
        assert current.app.state.store.schema_version==11
        maintain(current,'configure')
        with connect(database) as conn:
            now=conn.execute("SELECT floor(extract(epoch FROM clock_timestamp()))::bigint AS n").fetchone()['n']
        assert maintain(current,'keys',floor=now-61)['keys']==0
        before=counts(current)
        replay=current.post('/v1/batches',headers={**PUBLIC,'Idempotency-Key':key},json=body)
        assert replay.status_code==200,replay.text
        # Idempotency returns the originally accepted response, not a newly
        # materialized view of today's batch terminal status.
        assert replay.json()==batch
        assert counts(current)==before
        with connect(database) as conn:
            assert conn.execute('SELECT replay_epoch FROM idempotency WHERE key=%s',(key,)).fetchone()['replay_epoch'] is None


def test_http_other_tenant_cannot_read_or_retire_archived_history(client):
    batch,_=create(client,count=2);terminal=complete_batch(client,batch)
    bid,oid=terminal['id'],terminal['output_file_id']
    age_cohort(client,bid);maintain(client,'configure');maintain(client,'archive',batch=bid)
    with connect(client.database) as conn:
        conn.execute("INSERT INTO tenants(id,name) VALUES('other-http-tenant','Other test tenant')")
        conn.execute('INSERT INTO api_keys(key_hash,tenant_id) VALUES(%s,%s)',(hashlib.sha256(OTHER_KEY.encode()).hexdigest(),'other-http-tenant'))
    maintain(client,'configure',tenant='other-http-tenant')
    before=counts(client)
    for path in ('/v1/batches/'+bid,'/v1/files/'+oid,'/v1/files/'+oid+'/content'):
        result=client.get(path,headers=OTHER)
        assert result.status_code==404,result.text
    denied=maintain(client,'retire',batch=bid,tenant='other-http-tenant')
    assert denied=={'eligible':False,'reason':'batch_missing'}
    assert counts(client)==before
    assert overview(client,OTHER)['stats']['total_batches']==0
    assert client.get('/v1/files/'+oid+'/content',headers=PUBLIC).status_code==200
