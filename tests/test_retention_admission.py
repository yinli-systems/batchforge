"""Real PostgreSQL v9 quota, completion, archival and concurrent-writer proofs."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import secrets
import threading

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool
import pytest
from fastapi.testclient import TestClient

from backend.app import create_app
from backend.blob_storage import LocalBlobStore
from backend.config import Settings
from backend.migrate import migrate_database
from backend.result_archival import ResultArchiver, ResultQuotaExceeded
from backend.retention import retention_error

SOURCE = os.getenv('TEST_DATABASE_URL')
pytestmark = pytest.mark.skipif(not SOURCE, reason='Requires isolated real PostgreSQL')
KEY = 'retention-api-fixture-123456'
TOKEN = 'retention-worker-fixture-123456'
PUBLIC = {'Authorization': 'Bearer ' + KEY}
INTERNAL = {'Authorization': 'Bearer ' + TOKEN}
MODEL = 'retention-model'


@pytest.fixture
def database():
    name = 'bf_retention_' + secrets.token_hex(8)
    with psycopg.connect(SOURCE, autocommit=True) as c:
        c.execute(sql.SQL('CREATE DATABASE {} TEMPLATE template0').format(sql.Identifier(name)))
    dsn = make_conninfo(SOURCE, dbname=name)
    try: yield dsn
    finally:
        with psycopg.connect(SOURCE, autocommit=True) as c:
            c.execute(sql.SQL('DROP DATABASE {} WITH(FORCE)').format(sql.Identifier(name)))


def connect(dsn):
    return psycopg.connect(dsn, autocommit=True, row_factory=dict_row,
        options='-c statement_timeout=8000 -c lock_timeout=3000')


def setting(dsn, **kwargs):
    return Settings(database_url=dsn, batchforge_worker_token=TOKEN, model_name=MODEL,
        sql_completion_enabled=True, sql_claim_enabled=True, sql_recovery_enabled=True,
        retention_admission_enabled=True, **kwargs)


@pytest.fixture
def client(database):
    migrate_database(database, KEY, target_version=9)
    with TestClient(create_app(setting(database))) as c:
        c.database = database
        yield c


def limits(dsn, **values):
    with connect(dsn) as c:
        for key, value in values.items():
            assert key in {'max_retained_tasks','max_payload_bytes','max_response_bytes','max_error_bytes'}
            c.execute(sql.SQL('UPDATE tenant_retention_limits SET {}=%s WHERE tenant_id=\'default\'').format(sql.Identifier(key)), (value,))


def upload(client, count=1):
    rows = [dict(custom_id='row-' + str(i), method='POST', url='/v1/chat/completions',
        body={'model': MODEL, 'messages': [{'role': 'user', 'content': 'retention bound'}], 'max_tokens': 32}) for i in range(count)]
    content = ''.join(json.dumps(r) + '\n' for r in rows).encode()
    result = client.post('/v1/files', headers=PUBLIC, files={'file': ('fixture.jsonl',content)})
    assert result.status_code == 200, result.text
    return result.json()['id']


def submit(client, count=1, file=None, key=None):
    return client.post('/v1/batches', headers={**PUBLIC, **({'Idempotency-Key':key} if key else {})},
        json={'input_file_id': file or upload(client,count)})


def claim(client, count=1):
    response = client.post('/internal/workers/register', headers=INTERNAL,
        json={'worker_id': 'worker', 'name': 'worker', 'model': MODEL, 'concurrency': 64})
    assert response.status_code == 200, response.text
    result = client.post('/internal/tasks/claim', headers=INTERNAL,
        json={'worker_id': 'worker', 'model': MODEL, 'limit': count})
    assert result.status_code == 200, result.text
    return result.json()['tasks']


def response(text='answer'):
    return {'model': MODEL, 'choices': [{'message': {'role': 'assistant','content': text}, 'finish_reason':'stop'}],
        'usage': {'prompt_tokens': 5,'completion_tokens':3}}


def complete(client, task, text='answer'):
    return client.post(f"/internal/tasks/{task['id']}/complete", headers=INTERNAL,
        json={'worker_id':'worker','attempt_token':task['attempt_token'],'response':response(text)})


def usage(dsn):
    with connect(dsn) as c:
        row = c.execute("SELECT * FROM tenant_retention_usage WHERE tenant_id='default'").fetchone()
        actual = c.execute("SELECT count(*) AS n,coalesce(sum(retention_body_bytes+retention_response_bytes+retention_error_bytes),0) AS b FROM tasks WHERE tenant_id='default'").fetchone()
        assert (row['retained_tasks'],row['charged_payload_bytes']) == (actual['n'],actual['b'])
        return row


def archived(client, tmp_path, batch, *, pinned=False, max_bytes=100000):
    async def run():
        async with AsyncConnectionPool(client.database, min_size=1,max_size=4,
                kwargs={'autocommit':True,'row_factory':dict_row},open=False) as pool:
            archive = ResultArchiver(pool,LocalBlobStore(tmp_path/'objects'),max_archived_bytes_per_tenant=max_bytes)
            if pinned:
                with connect(client.database) as c:
                    file = c.execute('SELECT output_file_id FROM batches WHERE id=%s',(batch,)).fetchone()['output_file_id']
                    c.execute("INSERT INTO result_read_leases(id,tenant_id,batch_id,file_id,mode,expires_at) VALUES('pin','default',%s,%s,'db',clock_timestamp()+interval '5 minutes')",(batch,file))
            return await archive.archive_batch('default',batch)
    return asyncio.run(run())


def purge(dsn,tmp_path,batch):
    async def run():
        async with AsyncConnectionPool(dsn,min_size=1,max_size=4,
                kwargs={'autocommit':True,'row_factory':dict_row},open=False) as pool:
            return await ResultArchiver(pool,LocalBlobStore(tmp_path/'objects'),max_archived_bytes_per_tenant=100000).purge_batch('default',batch)
    return asyncio.run(run())


def test_last_retained_slot_two_api_replicas_are_atomic_and_losing_idempotency_is_rolled_back(client):
    limits(client.database,max_retained_tasks=1)
    files=[upload(client),upload(client)];barrier=threading.Barrier(2)
    with TestClient(create_app(setting(client.database))) as other,ThreadPoolExecutor(max_workers=2) as pool:
        def create(index,c):
            barrier.wait()
            return submit(c,file=files[index],key='last-slot-'+str(index))
        answers=[f.result(timeout=6) for f in [pool.submit(create,0,client),pool.submit(create,1,other)]]
    assert sorted(r.status_code for r in answers)==[200,429]
    rejected=next(r for r in answers if r.status_code==429)
    assert rejected.json()['error']['code']=='retained_task_limit'
    assert usage(client.database)['retained_tasks']==1
    with connect(client.database) as c:
        assert c.execute('SELECT count(*) AS n FROM batches').fetchone()['n']==1
        assert c.execute('SELECT count(*) AS n FROM idempotency').fetchone()['n']==1


def test_payload_limit_rolls_back_entire_multirow_batch_and_reservation_cannot_be_env_disabled(client):
    limits(client.database,max_response_bytes=1024,max_payload_bytes=200000)
    result=submit(client,2,key='atomic-two')
    assert result.status_code==429 and result.json()['error']['code']=='retained_payload_limit'
    assert usage(client.database)['retained_tasks']==0
    with connect(client.database) as c:
        assert c.execute('SELECT count(*) AS n FROM batches').fetchone()['n']==0
        assert c.execute('SELECT count(*) AS n FROM idempotency').fetchone()['n']==0
    with TestClient(create_app(setting(client.database).model_copy(update={'retention_admission_enabled':False}))) as false_flag:
        assert submit(false_flag,2).status_code==429


def test_complete_failure_and_cancellation_keep_full_reservations(client):
    assert submit(client,3).status_code==200
    tasks=claim(client,3);before=usage(client.database)
    assert complete(client,tasks[0]).status_code==200
    assert client.post(f"/internal/tasks/{tasks[1]['id']}/fail",headers=INTERNAL,
        json={'worker_id':'worker','attempt_token':tasks[1]['attempt_token'],'error':{'code':'engine','message':'failed'},'retryable':False}).status_code==200
    assert client.post(f"/v1/batches/{tasks[2]['batch_id']}/cancel",headers=PUBLIC).status_code==200
    assert usage(client.database)==before


def test_response_cap_413_rolls_back_and_reserved_completion_does_not_wait_for_tenant_or_ledger(client):
    limits(client.database,max_response_bytes=1024)
    assert submit(client).status_code==200
    task=claim(client)[0];before=usage(client.database)
    rejected=complete(client,task,'x'*1500)
    assert rejected.status_code==413 and rejected.json()['error']['code']=='retention_payload_too_large'
    assert usage(client.database)==before
    with connect(client.database) as blocker,ThreadPoolExecutor(max_workers=1) as pool:
        with blocker.transaction():
            blocker.execute("SELECT id FROM tenants WHERE id='default' FOR NO KEY UPDATE")
            blocker.execute("SELECT tenant_id FROM tenant_retention_usage WHERE tenant_id='default' FOR UPDATE")
            result=pool.submit(complete,client,task).result(timeout=2)
            assert result.status_code==200,result.text
    assert usage(client.database)==before
    assert complete(client,task).json()['duplicate'] is True


def test_error_attempt_bounds_and_accounting_columns_cannot_bypass_reservation(client):
    assert submit(client).status_code==200
    task=claim(client)[0]
    with connect(client.database) as c:
        for query,args,state in [
            ('SELECT batchforge_fail(%s,%s,%s,%s,false,1)',(task['id'],'worker',task['attempt_token'],Jsonb({'code':'engine','message':'bad','extra':'x'*32769})),'BP413'),
            ('UPDATE tasks SET retention_response_bytes=0 WHERE id=%s',(task['id'],),'BR409'),
            ('UPDATE tasks SET max_attempts=20 WHERE id=%s',(task['id'],),'BR409'),
            ("UPDATE tenant_retention_usage SET retained_tasks=0 WHERE tenant_id='default'",(),'BR403'),
            ("INSERT INTO attempts(token,task_id,batch_id,tenant_id,worker_id,number,status) VALUES('extra',%s,%s,'default','worker',4,'failed')",(task['id'],task['batch_id']),'BP413')]:
            with pytest.raises(psycopg.Error) as error:c.execute(query,args)
            assert error.value.sqlstate==state
    assert usage(client.database)['retained_tasks']==1


def test_verified_purge_releases_body_response_and_unused_error_allowance_but_seals_history(client,tmp_path):
    batch=submit(client).json()['id'];task=claim(client)[0]
    assert client.post(f"/internal/tasks/{task['id']}/fail",headers=INTERNAL,
        json={'worker_id':'worker','attempt_token':task['attempt_token'],'error':{'code':'retry','message':'diagnostic'},'retryable':True}).status_code==200
    with connect(client.database) as c:
        c.execute("UPDATE tasks SET ready_at=clock_timestamp() WHERE id=%s",(task['id'],))
        c.execute("UPDATE tenant_queues SET next_ready_at=clock_timestamp() WHERE tenant_id='default'")
    current=claim(client)[0];assert complete(client,current).status_code==200
    before=usage(client.database);result=archived(client,tmp_path,batch)
    assert result['purged'] is True
    after=usage(client.database)
    with connect(client.database) as c:
        exact=c.execute('SELECT coalesce(sum(batchforge_json_bytes(error)),0) AS n FROM attempts WHERE task_id=%s',(task['id'],)).fetchone()['n']
        assert after['charged_payload_bytes']==exact>0
        for query,args in [('UPDATE tasks SET body=%s WHERE id=%s',(Jsonb({'resurrect':True}),task['id'])),
                           ('UPDATE tasks SET response=%s WHERE id=%s',(Jsonb(response()),task['id'])),
                           ('UPDATE attempts SET error=%s WHERE token=%s',(Jsonb({'changed':True}),task['attempt_token']))]:
            with pytest.raises(psycopg.Error):c.execute(query,args)
    assert after['retained_tasks']==before['retained_tasks']==1
    assert after['charged_payload_bytes']<before['charged_payload_bytes']


def test_archive_quota_full_keeps_sql_charge_and_new_submission_fails_closed(client,tmp_path):
    batch=submit(client).json()['id'];task=claim(client)[0];assert complete(client,task).status_code==200
    with pytest.raises(ResultQuotaExceeded):archived(client,tmp_path,batch,max_bytes=1)
    before=usage(client.database)
    limits(client.database,max_payload_bytes=before['charged_payload_bytes'])
    result=submit(client,key='after-archive-full')
    assert result.status_code==429 and result.json()['error']['code']=='retained_payload_limit'
    with connect(client.database) as c:assert c.execute('SELECT response IS NOT NULL AS retained FROM tasks WHERE id=%s',(task['id'],)).fetchone()['retained']
    assert usage(client.database)==before


def test_create_and_purge_race_never_undercharges_or_deadlocks(client,tmp_path):
    batch=submit(client).json()['id'];assert complete(client,claim(client)[0]).status_code==200
    assert archived(client,tmp_path,batch,pinned=True)['purged'] is False
    before=usage(client.database);limits(client.database,max_payload_bytes=before['charged_payload_bytes'])
    next_file=upload(client)
    with connect(client.database) as c:c.execute("DELETE FROM result_read_leases WHERE id='pin'")
    barrier=threading.Barrier(2)
    def create():barrier.wait();return submit(client,file=next_file)
    def clear():barrier.wait();return purge(client.database,tmp_path,batch)
    with ThreadPoolExecutor(max_workers=2) as pool:
        a,b=pool.submit(create),pool.submit(clear)
        result,cleared=a.result(timeout=8),b.result(timeout=8)
    assert cleared and result.status_code in {200,429}
    if result.status_code==429:assert submit(client,file=next_file).status_code==200
    assert usage(client.database)['retained_tasks']==2


def test_delete_and_purge_race_releases_each_charge_exactly_once(client,tmp_path):
    batch=submit(client).json()['id'];task=claim(client)[0];assert complete(client,task).status_code==200
    assert archived(client,tmp_path,batch,pinned=True)['purged'] is False
    with connect(client.database) as c:
        c.execute("DELETE FROM result_read_leases WHERE id='pin'")
        with pytest.raises(psycopg.Error) as error:c.execute('DELETE FROM attempts WHERE task_id=%s',(task['id'],))
        assert error.value.sqlstate=='BR403'
    barrier=threading.Barrier(2)
    def remove():
        barrier.wait()
        with connect(client.database) as c,c.transaction():
            c.execute("SELECT batchforge_retention_authorize_delete('default')")
            c.execute('DELETE FROM attempts WHERE task_id=%s',(task['id'],))
            c.execute('DELETE FROM tasks WHERE id=%s',(task['id'],))
    def clear():barrier.wait();return purge(client.database,tmp_path,batch)
    with ThreadPoolExecutor(max_workers=2) as pool:
        a,b=pool.submit(remove),pool.submit(clear)
        a.result(timeout=8);assert b.result(timeout=8)
    assert usage(client.database)['retained_tasks']==usage(client.database)['charged_payload_bytes']==0


def test_bootstrap_over_limit_preserves_legacy_actuals_and_reserved_active_completion(database):
    migrate_database(database,KEY,target_version=8)
    with TestClient(create_app(setting(database).model_copy(update={'retention_admission_enabled':False}))) as c:
        c.database=database
        assert submit(c,2).status_code==200
        old,active=claim(c,2)
        assert complete(c,old).status_code==200
    with connect(database) as c:
        estimate=c.execute((Path(__file__).parents[1]/'ops/ha/retention_bootstrap_estimate.sql').read_text()).fetchone()
    migrate_database(database,target_version=9)
    assert usage(database)['charged_payload_bytes']==estimate['bootstrap_payload_charge_bytes']
    assert usage(database)['retained_tasks']==estimate['retained_tasks']
    with connect(database) as c:
        row=c.execute('SELECT retention_response_bytes,batchforge_json_bytes(response) AS actual FROM tasks WHERE id=%s',(old['id'],)).fetchone()
        assert row['retention_response_bytes']==row['actual']<1048576
    limits(database,max_retained_tasks=1,max_payload_bytes=1)
    with TestClient(create_app(setting(database))) as c:
        c.database=database
        assert submit(c).status_code==429
        assert complete(c,active).status_code==200
        assert c.get(f"/v1/batches/{active['batch_id']}",headers=PUBLIC).status_code==200
    assert usage(database)['retained_tasks']==2


def test_low_privilege_runtime_cannot_change_policy_forge_counters_or_delete_history(client):
    role='bf_retention_runtime_'+secrets.token_hex(6)
    with connect(client.database) as c:
        c.execute(sql.SQL('CREATE ROLE {} LOGIN').format(sql.Identifier(role)))
        c.execute(sql.SQL('GRANT USAGE ON SCHEMA public TO {}').format(sql.Identifier(role)))
        c.execute(sql.SQL('GRANT SELECT ON batchforge_schema_migrations,api_keys,tenants,tenant_retention_limits TO {}').format(sql.Identifier(role)))
        c.execute(sql.SQL('GRANT UPDATE(name) ON tenants TO {}').format(sql.Identifier(role)))
        c.execute(sql.SQL('GRANT SELECT,INSERT,UPDATE ON workers,files,batches,tasks,attempts,idempotency,tenant_queues TO {}').format(sql.Identifier(role)))
        c.execute(sql.SQL('GRANT SELECT,UPDATE ON tenant_retention_usage TO {}').format(sql.Identifier(role)))
    restricted=make_conninfo(client.database,user=role)
    try:
        with TestClient(create_app(setting(restricted))) as runtime:
            runtime.database=restricted
            limits(client.database,max_retained_tasks=1)
            assert submit(runtime).status_code==200
            task=claim(runtime)[0]
            assert complete(runtime,task).status_code==200
            assert submit(runtime).status_code==429
        with connect(restricted) as c:
            for query,state in [("UPDATE tenant_retention_limits SET max_retained_tasks=999999",'42501'),
                ("UPDATE tenant_retention_usage SET retained_tasks=0",'BR403'),
                ("SELECT batchforge_retention_authorize_delete('default')",'42501')]:
                with pytest.raises(psycopg.Error) as error:c.execute(query)
                assert error.value.sqlstate==state
        with connect(client.database) as c:
            # Even an accidental DELETE grant cannot grant the operator capability.
            c.execute(sql.SQL('GRANT DELETE ON tasks,attempts TO {}').format(sql.Identifier(role)))
        with connect(restricted) as c:
            c.execute("SET batchforge.retention_delete_tenant='default'")
            with pytest.raises(psycopg.Error) as error:c.execute('DELETE FROM attempts')
            assert error.value.sqlstate=='BR403'
        assert usage(client.database)['retained_tasks']==1
    finally:
        with connect(client.database) as c:
            c.execute(sql.SQL('DROP OWNED BY {}').format(sql.Identifier(role)))
            c.execute(sql.SQL('DROP ROLE {}').format(sql.Identifier(role)))


def test_bootstrap_preserves_large_legacy_errors_and_freezes_them_without_undercharging(database):
    migrate_database(database,KEY,target_version=8)
    with TestClient(create_app(setting(database).model_copy(update={'retention_admission_enabled':False}))) as c:
        c.database=database
        assert submit(c,2).status_code==200
        terminal,active=claim(c,2)
        with connect(database) as owner:
            owner.execute('SELECT batchforge_fail(%s,%s,%s,%s,false,1)',
                (terminal['id'],'worker',terminal['attempt_token'],Jsonb({'code':'old','message':'legacy','padding':'x'*40000})))
            owner.execute('SELECT batchforge_fail(%s,%s,%s,%s,true,1)',
                (active['id'],'worker',active['attempt_token'],Jsonb({'code':'old','message':'retry','padding':'x'*40000})))
    migrate_database(database,target_version=9)
    with connect(database) as c:
        row=c.execute('SELECT retention_error_bytes,retention_error_limit,retention_errors_sealed,batchforge_json_bytes(error) AS n FROM tasks WHERE id=%s',(terminal['id'],)).fetchone()
        assert row['retention_errors_sealed'] and row['retention_error_limit']>40000
        assert row['retention_error_bytes']==2*row['n']
        row=c.execute('SELECT retention_error_bytes,retention_error_limit,retention_errors_sealed FROM tasks WHERE id=%s',(active['id'],)).fetchone()
        assert not row['retention_errors_sealed'] and row['retention_error_bytes']>=4*row['retention_error_limit']
    assert usage(database)['retained_tasks']==2


def test_default_budget_is_conservative_and_database_functions_are_invoker_with_pinned_search_path(client):
    assert 1073741824//(1048576+4*32768)==910
    with connect(client.database) as c:
        rows=c.execute("SELECT proname,prosecdef,proconfig FROM pg_proc WHERE pronamespace='public'::regnamespace AND (proname LIKE 'batchforge_retention_%' OR proname='batchforge_json_bytes')").fetchall()
        assert len(rows)>=10
        assert all(not row['prosecdef'] and any(value.startswith('search_path=') for value in row['proconfig']) for row in rows)


def test_migration_does_not_invert_tenant_row_locks_and_old_cached_writer_is_guarded(database):
    migrate_database(database,KEY,target_version=8)
    with TestClient(create_app(setting(database).model_copy(update={'retention_admission_enabled':False}))) as c:
        c.database=database
        assert submit(c).status_code==200
        task=claim(c)[0]
        with connect(database) as blocker,ThreadPoolExecutor(max_workers=1) as pool:
            with blocker.transaction():
                blocker.execute("SELECT id FROM tenants WHERE id='default' FOR NO KEY UPDATE")
                migration=pool.submit(migrate_database,database,None,9)
                # Migration's relation-level gate must not wait for this
                # existing admission tuple lock while owning task DDL locks.
                assert migration.result(timeout=5)==[9]
                assert complete(c,task).status_code==200
        # This lifespan cached schema8, yet authoritative v9 triggers still cap
        # an old compatible writer after migration, without an API env bypass.
        limits(database,max_retained_tasks=1)
        assert submit(c).status_code==429
    assert usage(database)['retained_tasks']==1


def test_attempt_error_update_and_purge_race_preserves_exact_sealed_error_charge(client,tmp_path):
    batch=submit(client).json()['id'];task=claim(client)[0]
    assert complete(client,task).status_code==200
    assert archived(client,tmp_path,batch,pinned=True)['purged'] is False
    with connect(client.database) as c:c.execute("DELETE FROM result_read_leases WHERE id='pin'")
    barrier=threading.Barrier(2)
    def error_update():
        barrier.wait()
        with connect(client.database) as c:
            try:
                c.execute('UPDATE attempts SET error=%s WHERE token=%s',
                    (Jsonb({'code':'late_diagnostic','message':'kept exactly'}),task['attempt_token']))
                return 'updated'
            except psycopg.Error as error:
                assert error.sqlstate=='BR409'
                return 'sealed'
    def clear():barrier.wait();return purge(client.database,tmp_path,batch)
    with ThreadPoolExecutor(max_workers=2) as pool:
        changed,cleared=pool.submit(error_update),pool.submit(clear)
        assert changed.result(timeout=8) in {'updated','sealed'}
        assert cleared.result(timeout=8)
    with connect(client.database) as c:
        exact=c.execute('SELECT coalesce(sum(batchforge_json_bytes(error)),0) AS n FROM attempts WHERE task_id=%s',
            (task['id'],)).fetchone()['n']
        assert c.execute('SELECT retention_errors_sealed FROM tasks WHERE id=%s',(task['id'],)).fetchone()['retention_errors_sealed']
        with pytest.raises(psycopg.Error) as error:
            c.execute('UPDATE attempts SET error=%s WHERE token=%s',(Jsonb({'later':True}),task['attempt_token']))
        assert error.value.sqlstate=='BR409'
    assert usage(client.database)['charged_payload_bytes']==exact


def test_mixed_archive_releases_success_payload_but_keeps_failed_and_cancelled_reservations(client,tmp_path):
    batch=submit(client,3).json()['id'];first,failed,cancelled=claim(client,3)
    assert complete(client,first).status_code==200
    assert client.post(f"/internal/tasks/{failed['id']}/fail",headers=INTERNAL,
        json={'worker_id':'worker','attempt_token':failed['attempt_token'],
            'error':{'code':'permanent','message':'retained'},'retryable':False}).status_code==200
    assert client.post(f'/v1/batches/{batch}/cancel',headers=PUBLIC).status_code==200
    with connect(client.database) as c:
        before={row['id']:row for row in c.execute('SELECT id,body,response,retention_body_bytes,retention_response_bytes,retention_error_bytes FROM tasks')}
    assert archived(client,tmp_path,batch)['purged'] is True
    with connect(client.database) as c:
        after={row['id']:row for row in c.execute('SELECT id,body,response,retention_body_bytes,retention_response_bytes,retention_error_bytes FROM tasks')}
    assert after[first['id']]['body']=={} and after[first['id']]['response'] is None
    assert after[first['id']]['retention_body_bytes']==after[first['id']]['retention_response_bytes']==0
    for task in (failed,cancelled):assert after[task['id']]==before[task['id']]
    assert usage(client.database)['retained_tasks']==3


def test_lowered_operator_response_limit_does_not_revoke_existing_reservation(client):
    limits(client.database,max_response_bytes=2048)
    assert submit(client).status_code==200
    old=claim(client)[0]
    limits(client.database,max_response_bytes=1024)
    assert complete(client,old,'x'*1500).status_code==200
    assert submit(client).status_code==200
    new=claim(client)[0]
    assert complete(client,new,'x'*1500).status_code==413
    assert complete(client,new).status_code==200
    with connect(client.database) as c:
        caps={r['id']:r['retention_response_limit'] for r in c.execute('SELECT id,retention_response_limit FROM tasks')}
    assert caps=={old['id']:2048,new['id']:1024}


def test_database_error_mapping_covers_auxiliary_bounds_without_exposing_diagnostics(client):
    with connect(client.database) as c:
        for state,message,status,code in [
            ('BQ429','Auxiliary metadata quota reached: private-row-name',429,'metadata_retention_limit'),
            ('BP413','private-body-value',413,'retention_payload_too_large'),
            ('BR409','private-identity-value',409,'retention_conflict')]:
            with pytest.raises(psycopg.Error) as error:
                c.execute(sql.SQL("DO $$BEGIN RAISE EXCEPTION USING ERRCODE={}, MESSAGE={}; END$$").format(sql.Literal(state),sql.Literal(message)))
            mapped=retention_error(error.value)
            assert mapped[0]==status and mapped[2]==code and 'private' not in mapped[1]
