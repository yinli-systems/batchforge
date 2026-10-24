"""Real-PG v11 retirement: exact accounting, replay fences and failure atomicity."""
import asyncio
from datetime import datetime, timedelta, timezone
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
import pytest_asyncio

from backend.blob_storage import LocalBlobStore, BlobIntegrityError
from backend.history_retirement import history_page, idempotency_page, configure_policy
from backend.result_archival import ResultArchiver
from backend.migrate import MIGRATIONS
from ops.ha.local_pg17_restore import verify_database

SOURCE = os.environ.get('TEST_DATABASE_URL')
pytestmark = pytest.mark.skipif(not SOURCE, reason='Requires isolated real PostgreSQL')


@pytest.fixture(scope='module')
def history_database():
    name = 'bf_history_' + secrets.token_hex(8)
    with psycopg.connect(SOURCE, autocommit=True) as conn:
        conn.execute(sql.SQL('CREATE DATABASE {} TEMPLATE template0').format(sql.Identifier(name)))
    dsn = make_conninfo(SOURCE, dbname=name)
    try:
        with psycopg.connect(dsn) as conn:
            conn.execute(Path('backend/schema.sql').read_text())
            for n in range(2, 11):
                conn.execute(next(Path('backend/migrations').glob(f'{n:04d}_*.sql')).read_text())
            conn.execute("INSERT INTO tenants(id,name) VALUES('pre-v11','Legacy')")
            for key in ('bf1.1000000000.aaaaaaaaaaaaaaaa', 'bf1.malformed', 'arbitrary-legacy-key'):
                conn.execute("INSERT INTO idempotency(tenant_id,scope,key,body_hash,response) VALUES('pre-v11','create',%s,'hash','{}')", (key,))
            conn.execute(Path('backend/migrations/0011_history_retirement.sql').read_text())
            conn.execute('ALTER TABLE files VALIDATE CONSTRAINT files_result_has_batch')
            conn.execute('CREATE TABLE batchforge_schema_migrations(version integer PRIMARY KEY,checksum text NOT NULL,applied_at timestamptz NOT NULL DEFAULT clock_timestamp())')
            for version,_,checksum in MIGRATIONS:
                conn.execute('INSERT INTO batchforge_schema_migrations(version,checksum) VALUES(%s,%s)',(version,checksum))
        yield dsn
    finally:
        with psycopg.connect(SOURCE, autocommit=True) as conn:
            conn.execute(sql.SQL('DROP DATABASE {} WITH(FORCE)').format(sql.Identifier(name)))


@pytest_asyncio.fixture
async def history(history_database, tmp_path):
    async with AsyncConnectionPool(history_database, min_size=1, max_size=12,
            kwargs={'autocommit': True, 'row_factory': dict_row,
                    'options': '-c statement_timeout=8000 -c lock_timeout=1000'}, open=False) as pool:
        tenant = 'tenant_' + secrets.token_hex(6)
        async with pool.connection() as conn:
            await conn.execute('INSERT INTO tenants(id,name) VALUES(%s,%s)', (tenant, tenant))
            await conn.execute('SELECT public.batchforge_auxiliary_lock(%s)', (tenant,))
        adapter = LocalBlobStore(tmp_path / 'objects')
        archiver = ResultArchiver(pool, adapter, max_archived_bytes_per_tenant=10000000, purge_rows=1000)
        yield {'pool': pool, 'tenant': tenant, 'archive': archiver, 'adapter': adapter, 'dsn': history_database}


async def one(h, query, args=()):
    async with h['pool'].connection() as conn:
        return await (await conn.execute(query, args)).fetchone()


async def execute(h, query, args=()):
    async with h['pool'].connection() as conn:
        return await conn.execute(query, args)


async def policy(h, **overrides):
    args = dict(enabled=True, created_after=datetime.now(timezone.utc)-timedelta(seconds=600),
                min_age_seconds=60, replay_ttl_seconds=60)
    args.update(overrides)
    return await configure_policy(h['pool'], h['tenant'], **args)


async def terminal(h, count=5, *, statuses=None):
    tenant = h['tenant']; tag = secrets.token_hex(6)
    bid, fid, oid, eid, wid = ('batch_'+tag, 'input_'+tag, 'output_'+tag, 'error_'+tag, 'worker_'+tag)
    statuses = statuses or ['completed'] * count
    completed, failed, cancelled = (statuses.count(x) for x in ('completed', 'failed', 'cancelled'))
    async with h['pool'].connection() as conn, conn.transaction():
        await conn.execute("INSERT INTO workers(id,name,model,concurrency) VALUES(%s,'test','model',16)", (wid,))
        await conn.execute("INSERT INTO files(id,tenant_id,filename,kind) VALUES(%s,%s,'input','input')", (fid, tenant))
        await conn.execute("""INSERT INTO batches(id,tenant_id,input_file_id,model,status,total_count,completed_count,
            failed_count,cancelled_count,prompt_tokens,completion_tokens,created_at,completed_at)
            VALUES(%s,%s,%s,'model','completed',%s,%s,%s,%s,%s,%s,clock_timestamp()-interval '120 seconds',clock_timestamp()-interval '90 seconds')""",
            (bid, tenant, fid, count, completed, failed, cancelled, completed*5, completed*3))
        for kind, file, n in (('output', oid, completed), ('error', eid, failed+cancelled)):
            if n:
                await conn.execute('INSERT INTO files(id,tenant_id,filename,kind,batch_id) VALUES(%s,%s,%s,%s,%s)', (file, tenant, kind, kind, bid))
                await conn.execute(sql.SQL('UPDATE batches SET {}=%s WHERE id=%s').format(sql.Identifier(kind+'_file_id')), (file, bid))
        for i, status in enumerate(statuses):
            tid = 'task_'+tag+str(i)
            response = {'choices': [{'message': {'content': 'answer'}}]} if status == 'completed' else None
            error = None if status == 'completed' else {'code': 'failed'}
            await conn.execute("""INSERT INTO tasks(id,tenant_id,batch_id,custom_id,model,body,status,max_attempts,attempts,response,error,
                prompt_tokens,completion_tokens,finished_at) VALUES(%s,%s,%s,%s,'model','{"prompt":"public fixture"}',%s,3,1,%s,%s,%s,%s,clock_timestamp()-interval '90 seconds')""",
                (tid, tenant, bid, str(i), status, Jsonb(response) if response else None, Jsonb(error) if error else None,
                 5 if status == 'completed' else 0, 3 if status == 'completed' else 0))
            await conn.execute("""INSERT INTO attempts(token,task_id,batch_id,tenant_id,worker_id,number,status,
                prompt_tokens,completion_tokens,started_at,finished_at) VALUES(%s,%s,%s,%s,%s,1,%s,%s,%s,clock_timestamp()-interval '100 seconds',clock_timestamp()-interval '90 seconds')""",
                ('attempt_'+tag+str(i), tid, bid, tenant, wid, status, 5 if status == 'completed' else 0, 3 if status == 'completed' else 0))
    result = await h['archive'].archive_batch(tenant, bid)
    assert result['purged']
    return bid, oid


async def read_output(h, oid):
    async with h['archive'].open_file(h['tenant'], oid) as chunks:
        return b''.join([chunk async for chunk in chunks])


async def ledger(h):
    row = await one(h, 'SELECT * FROM tenant_retention_usage WHERE tenant_id=%s', (h['tenant'],))
    actual = await one(h, '''SELECT count(*) AS n,coalesce(sum(retention_body_bytes+retention_response_bytes+retention_error_bytes),0) AS bytes
        FROM tasks WHERE tenant_id=%s''', (h['tenant'],))
    assert (row['retained_tasks'], row['charged_payload_bytes']) == (actual['n'], actual['bytes'])
    return row


async def test_default_disabled_preview_and_explicit_new_cohort(history):
    h = history; bid, _ = await terminal(h)
    result = await history_page(h['pool'], h['tenant'], bid)
    assert result == {'eligible': False, 'reason': 'policy_disabled'}
    await policy(h, created_after=datetime.now(timezone.utc))
    assert (await history_page(h['pool'], h['tenant'], bid))['reason'] == 'cohort_or_age_or_not_terminal'
    assert (await ledger(h))['retained_tasks'] == 5


async def test_preview_then_paged_retirement_preserves_usage_and_archive_bytes(history):
    h = history; bid, oid = await terminal(h)
    await policy(h)
    raw = await read_output(h, oid)
    before = await one(h, 'SELECT * FROM batchforge_attempt_hourly WHERE tenant_id=%s', (h['tenant'],))
    preview = await history_page(h['pool'], h['tenant'], bid, limit=3)
    assert preview['eligible'] and preview['tasks'] == 3 and not preview['apply']
    assert (await ledger(h))['retained_tasks'] == 5
    first = await history_page(h['pool'], h['tenant'], bid, limit=3, apply=True, archiver=h['archive'])
    assert (first['tasks'], first['remaining_tasks']) == (3, 2)
    middle = await one(h, 'SELECT * FROM batches WHERE id=%s', (bid,))
    assert middle['history_retirement_started_at'] and not middle['history_retired_at']
    assert (middle['history_retired_completed'],middle['history_retired_prompt_tokens'],middle['history_retired_completion_tokens']) == (3,15,9)
    assert (await ledger(h))['retained_tasks'] == 2
    with psycopg.connect(h['dsn']) as restored:
        assert verify_database(restored,MIGRATIONS)['history_rollup_mismatches']==0
    assert await one(h, 'SELECT * FROM batchforge_attempt_hourly WHERE tenant_id=%s', (h['tenant'],)) == before
    second = await history_page(h['pool'], h['tenant'], bid, limit=3, apply=True, archiver=h['archive'])
    assert (second['tasks'],second['remaining_tasks']) == (2,0)
    final = await one(h, 'SELECT * FROM batches WHERE id=%s', (bid,))
    assert final['history_retired_at'] and final['history_retired_tasks'] == final['total_count'] == 5
    assert (final['history_retired_completed'],final['history_retired_prompt_tokens'],final['history_retired_completion_tokens']) == (5,25,15)
    assert (final['prompt_tokens'],final['completion_tokens']) == (25,15)
    assert (await ledger(h))['retained_tasks'] == 0
    assert await read_output(h, oid) == raw
    with psycopg.connect(h['dsn']) as restored:
        assert verify_database(restored,MIGRATIONS)['history_rollup_mismatches']==0
    assert (await history_page(h['pool'],h['tenant'],bid,apply=True,archiver=h['archive']))['reason'] == 'already_retired'
    assert await one(h, 'SELECT * FROM batchforge_attempt_hourly WHERE tenant_id=%s', (h['tenant'],)) == before


async def test_mixed_terminal_statuses_and_retired_summary_are_exact(history):
    h = history; bid, oid = await terminal(h, statuses=['completed','failed','cancelled','completed','failed'])
    await policy(h)
    result = await history_page(h['pool'],h['tenant'],bid,apply=True,archiver=h['archive'])
    assert result['tasks'] == 5
    final = await one(h, 'SELECT * FROM batches WHERE id=%s', (bid,))
    assert (final['history_retired_completed'],final['history_retired_failed'],final['history_retired_cancelled']) == (2,2,1)
    hourly = await one(h, 'SELECT * FROM batchforge_attempt_hourly WHERE tenant_id=%s', (h['tenant'],))
    assert (hourly['completed'],hourly['failed'],hourly['tokens']) == (2,2,16)
    assert (await ledger(h))['charged_payload_bytes'] == 0


async def test_crash_rollback_does_not_double_count_and_reentry_finishes(history):
    h = history; bid, _ = await terminal(h); await policy(h)
    preview = await history_page(h['pool'],h['tenant'],bid)
    with pytest.raises(RuntimeError):
        async with h['pool'].connection() as conn, conn.transaction():
            await conn.execute('SELECT batchforge_retire_history_page(%s,%s,2,true,%s)', (h['tenant'],bid,preview['manifest_sha256']))
            raise RuntimeError('simulated crash before commit')
    assert (await ledger(h))['retained_tasks'] == 5
    assert not (await one(h,'SELECT history_retirement_started_at AS started FROM batches WHERE id=%s',(bid,)))['started']
    assert (await one(h,'SELECT count(*) AS n FROM retired_attempt_hourly WHERE tenant_id=%s',(h['tenant'],)))['n'] == 0
    assert (await history_page(h['pool'],h['tenant'],bid,apply=True,archiver=h['archive']))['tasks'] == 5
    assert (await one(h,'SELECT sum(attempts) AS n FROM retired_attempt_hourly WHERE tenant_id=%s',(h['tenant'],)))['n'] == 5


async def test_active_reader_and_corrupt_archive_fail_closed(history):
    h = history; bid, oid = await terminal(h); await policy(h)
    async with h['archive'].open_file(h['tenant'],oid):
        assert (await history_page(h['pool'],h['tenant'],bid,apply=True,archiver=h['archive']))['reason'] == 'active_reader'
    chunk = await one(h,'SELECT object_key FROM result_archive_chunks WHERE tenant_id=%s LIMIT 1',(h['tenant'],))
    path = h['adapter'].root / chunk['object_key']
    original = path.read_bytes(); path.write_bytes(b'corrupt')
    try:
        with pytest.raises(BlobIntegrityError):
            await history_page(h['pool'],h['tenant'],bid,apply=True,archiver=h['archive'])
    finally: path.write_bytes(original)
    assert (await ledger(h))['retained_tasks'] == 5


async def test_concurrent_pages_skip_busy_tenant_and_never_duplicate_rollups(history):
    h = history; bid, _ = await terminal(h, count=23); await policy(h)
    results = await asyncio.gather(*(history_page(h['pool'],h['tenant'],bid,limit=3,apply=True,archiver=h['archive']) for _ in range(10)))
    assert any(r.get('tasks',0)>0 for r in results)
    for _ in range(20):
        r = await history_page(h['pool'],h['tenant'],bid,limit=3,apply=True,archiver=h['archive'])
        if r.get('reason') == 'already_retired': break
    assert (await ledger(h))['retained_tasks'] == 0
    assert (await one(h,'SELECT sum(attempts) AS n,sum(prompt_tokens) AS p FROM retired_attempt_hourly WHERE tenant_id=%s',(h['tenant'],))) == {'n':23,'p':115}


async def insert_key(h, key, response=None, scope='create'):
    return await one(h, '''INSERT INTO idempotency(tenant_id,scope,key,body_hash,response) VALUES(%s,%s,%s,'hash',%s)
        ON CONFLICT DO NOTHING RETURNING replay_epoch''', (h['tenant'],scope,key,Jsonb(response) if response is not None else None))


def timed_key(epoch, suffix=None):
    return 'bf1.'+str(epoch)+'.'+(suffix or secrets.token_hex(8))


async def test_replay_floor_compaction_410_without_row_and_no_legacy_deletion(history):
    h = history; bid,_ = await terminal(h); await policy(h)
    now = (await one(h,"SELECT floor(extract(epoch FROM clock_timestamp()))::bigint AS n"))['n']
    keys = [timed_key(now-120) for _ in range(3)]
    for key in keys: await insert_key(h,key,{'id':bid})
    await insert_key(h,'legacy-forever',{'id':bid})
    result = await idempotency_page(h['pool'],h['tenant'],now-61,limit=2)
    assert result['keys'] == 2 and not result['apply']
    assert (await one(h,'SELECT replay_floor AS f FROM tenant_history_retirement_policy WHERE tenant_id=%s',(h['tenant'],)))['f'] == 0
    result = await idempotency_page(h['pool'],h['tenant'],now-61,limit=2,apply=True)
    assert result['keys'] == 2
    for key in keys:
        with pytest.raises(psycopg.Error) as error:
            await one(h,'SELECT batchforge_idempotency_check(%s,%s,%s)',(h['tenant'],'create',key))
        assert error.value.sqlstate == 'BH410'
        with pytest.raises(psycopg.Error) as error: await insert_key(h,key,{'id':bid})
        assert error.value.sqlstate == 'BH410'
    assert (await idempotency_page(h['pool'],h['tenant'],now-61,apply=True))['keys'] == 1
    assert await insert_key(h,'legacy-forever',{'id':bid}) is None
    assert (await one(h,"SELECT rows,bytes FROM tenant_auxiliary_usage WHERE tenant_id=%s AND resource='idempotency'",(h['tenant'],))) == {'rows':1,'bytes':65536}
    assert await insert_key(h,timed_key(now),{'id':bid})


async def test_floor_refuses_unresolved_active_future_and_backward_keys(history):
    h = history; bid,_ = await terminal(h); await policy(h)
    now = (await one(h,"SELECT floor(extract(epoch FROM clock_timestamp()))::bigint AS n"))['n']
    key = timed_key(now-120); await insert_key(h,key)
    assert (await idempotency_page(h['pool'],h['tenant'],now-61,apply=True))['reason'] == 'unresolved_or_active_key'
    await execute(h,'UPDATE idempotency SET response=%s WHERE tenant_id=%s AND key=%s',(Jsonb({'id':bid}),h['tenant'],key))
    await execute(h,"UPDATE batches SET status='in_progress' WHERE id=%s",(bid,))
    assert (await idempotency_page(h['pool'],h['tenant'],now-61,apply=True))['reason'] == 'unresolved_or_active_key'
    await execute(h,"UPDATE batches SET status='completed' WHERE id=%s",(bid,))
    await idempotency_page(h['pool'],h['tenant'],now-61,apply=True)
    for floor in (now-62,now):
        with pytest.raises(psycopg.Error) as error: await idempotency_page(h['pool'],h['tenant'],floor,apply=True)
        assert error.value.sqlstate == 'BR409'
    for key in (timed_key(now+120),'bf1.bad','bf1.0000000010.aaaaaaaaaaaaaaaa'):
        with pytest.raises(psycopg.Error) as error: await insert_key(h,key,{'id':bid})
        assert error.value.sqlstate == 'BH400'


async def test_admission_and_floor_advance_are_serialized_under_tenant_lock(history):
    h=history; bid,_=await terminal(h); await policy(h)
    now=(await one(h,"SELECT floor(extract(epoch FROM clock_timestamp()))::bigint AS n"))['n']
    old=timed_key(now-120)
    async with h['pool'].connection() as conn,conn.transaction():
        await conn.execute('SELECT id FROM tenants WHERE id=%s FOR NO KEY UPDATE',(h['tenant'],))
        await conn.execute("INSERT INTO idempotency(tenant_id,scope,key,body_hash) VALUES(%s,'create',%s,'hash')",(h['tenant'],old))
        # An in-flight transaction is skipped, never swept or guessed complete.
        assert (await idempotency_page(h['pool'],h['tenant'],now-61,apply=True))['reason']=='tenant_busy_or_missing'
    assert (await idempotency_page(h['pool'],h['tenant'],now-61,apply=True))['reason']=='unresolved_or_active_key'
    await execute(h,'UPDATE idempotency SET response=%s WHERE tenant_id=%s AND key=%s',(Jsonb({'id':bid}),h['tenant'],old))
    await idempotency_page(h['pool'],h['tenant'],now-61,apply=True)
    # A losing writer, including an old API using raw INSERT, cannot reexecute.
    with pytest.raises(psycopg.Error) as error: await insert_key(h,old,{'id':bid})
    assert error.value.sqlstate=='BH410'


async def test_pre_v11_arbitrary_namespace_rows_keep_infinite_replay(history):
    h=history
    await configure_policy(h['pool'],'pre-v11',enabled=True,created_after=datetime.now(timezone.utc),min_age_seconds=60,replay_ttl_seconds=60)
    now=(await one(h,"SELECT floor(extract(epoch FROM clock_timestamp()))::bigint AS n"))['n']
    assert (await idempotency_page(h['pool'],'pre-v11',now-61,apply=True))['keys']==0
    for key in ('bf1.1000000000.aaaaaaaaaaaaaaaa','bf1.malformed','arbitrary-legacy-key'):
        await one(h,'SELECT batchforge_idempotency_check(%s,%s,%s)',('pre-v11','create',key))
        assert await one(h,"INSERT INTO idempotency(tenant_id,scope,key,body_hash) VALUES('pre-v11','create',%s,'hash') ON CONFLICT DO NOTHING RETURNING key",(key,)) is None
    with pytest.raises(psycopg.Error) as error:
        await one(h,'SELECT batchforge_idempotency_check(%s,%s,%s)',('pre-v11','different-scope','bf1.1000000000.aaaaaaaaaaaaaaaa'))
    assert error.value.sqlstate=='BH410'


async def test_preserved_batch_accounting_mismatch_refuses_retirement(history):
    h=history;bid,_=await terminal(h);await policy(h)
    await execute(h,'UPDATE batches SET prompt_tokens=prompt_tokens+1 WHERE id=%s',(bid,))
    with pytest.raises(psycopg.Error) as error: await history_page(h['pool'],h['tenant'],bid)
    assert error.value.sqlstate=='BR409'
    assert (await ledger(h))['retained_tasks']==5
    await execute(h,'UPDATE batches SET prompt_tokens=prompt_tokens-1 WHERE id=%s',(bid,))


async def test_runtime_has_no_retirement_or_policy_mutation_capability(history):
    h=history;bid,_=await terminal(h);await policy(h)
    role='bf_history_runtime_'+secrets.token_hex(6)
    async with h['pool'].connection() as conn:
        await conn.execute(sql.SQL('CREATE ROLE {}').format(sql.Identifier(role)))
        try:
            await conn.execute(sql.SQL('GRANT USAGE ON SCHEMA public TO {}').format(sql.Identifier(role)))
            await conn.execute(sql.SQL('GRANT SELECT ON tenants,batches,tasks,idempotency,tenant_history_retirement_policy TO {}').format(sql.Identifier(role)))
            await conn.execute(sql.SQL('GRANT UPDATE(name) ON tenants TO {}').format(sql.Identifier(role)))
            await conn.execute(sql.SQL('GRANT EXECUTE ON FUNCTION batchforge_idempotency_epoch(text),batchforge_idempotency_check(text,text,text),batchforge_idempotency_admit(text,text,text) TO {}').format(sql.Identifier(role)))
            await conn.execute(sql.SQL('SET ROLE {}').format(sql.Identifier(role)))
            await conn.execute('SELECT batchforge_idempotency_admit(%s,%s,%s)',(h['tenant'],'create','normal-legacy'))
            for query,args in [('SELECT batchforge_retire_history_page(%s,%s,100,true)',(h['tenant'],bid)),
                               ('UPDATE tenant_history_retirement_policy SET enabled=false WHERE tenant_id=%s',(h['tenant'],))]:
                with pytest.raises(psycopg.Error) as error: await conn.execute(query,args)
                assert error.value.sqlstate in ('42501','BR403')
        finally:
            await conn.execute('RESET ROLE')
            await conn.execute(sql.SQL('DROP OWNED BY {}').format(sql.Identifier(role)))
            await conn.execute(sql.SQL('DROP ROLE {}').format(sql.Identifier(role)))


@pytest.mark.parametrize('limit',[0,1001,True,'1'])
async def test_bounds_reject_before_connection(limit):
    with pytest.raises(ValueError): await history_page(None,'tenant','batch',limit=limit)


async def test_retirement_releases_last_task_slots_and_rejects_old_api_retry(history):
    h=history
    await execute(h,'UPDATE tenant_retention_limits SET max_retained_tasks=5 WHERE tenant_id=%s',(h['tenant'],))
    bid,_=await terminal(h);await policy(h)
    with pytest.raises(psycopg.Error) as error: await terminal(h,count=1)
    assert error.value.sqlstate=='BQ429'
    await history_page(h['pool'],h['tenant'],bid,limit=3,apply=True,archiver=h['archive'])
    assert (await ledger(h))['retained_tasks']==2
    await terminal(h,count=3)
    assert (await ledger(h))['retained_tasks']==5
    source=await one(h,'SELECT input_file_id FROM batches WHERE id=%s',(bid,))
    with pytest.raises(psycopg.Error) as error:
        await execute(h,"""INSERT INTO batches(id,tenant_id,input_file_id,model,status,total_count,pending_count,metadata)
         VALUES(%s,%s,%s,'model','in_progress',1,1,%s)""",('retry_'+secrets.token_hex(6),h['tenant'],source['input_file_id'],Jsonb({'retry_of':bid})))
    assert error.value.sqlstate=='BH411'
    with pytest.raises(psycopg.Error) as error:
        await execute(h,"""INSERT INTO tasks(id,tenant_id,batch_id,custom_id,model,body,status,max_attempts)
          VALUES(%s,%s,%s,'resurrection','model','{}','completed',3)""",('task_'+secrets.token_hex(6),h['tenant'],bid))
    assert error.value.sqlstate=='BH411'


async def test_writer_waiting_behind_floor_commit_rechecks_new_snapshot(history):
    h=history;await policy(h)
    now=(await one(h,"SELECT floor(extract(epoch FROM clock_timestamp()))::bigint AS n"))['n']
    key=timed_key(now-120)
    async with h['pool'].connection() as conn,conn.transaction():
        row=await (await conn.execute('SELECT batchforge_retire_idempotency_page(%s,%s,100,true) AS result',(h['tenant'],now-61))).fetchone()
        assert row['result']['eligible']
        pending=asyncio.create_task(insert_key(h,key))
        await asyncio.sleep(.05)
        assert not pending.done()
    with pytest.raises(psycopg.Error) as error: await pending
    assert error.value.sqlstate=='BH410'
    assert (await one(h,'SELECT count(*) AS n FROM idempotency WHERE tenant_id=%s',(h['tenant'],)))['n']==0


async def test_policy_floor_cannot_be_reset_by_direct_update_delete_or_insert(history):
    h=history;await policy(h)
    for query,args in [
        ('UPDATE tenant_history_retirement_policy SET replay_floor=1 WHERE tenant_id=%s',(h['tenant'],)),
        ('DELETE FROM tenant_history_retirement_policy WHERE tenant_id=%s',(h['tenant'],))]:
        with pytest.raises(psycopg.Error) as error: await execute(h,query,args)
        assert error.value.sqlstate=='BR403'
    with pytest.raises(ValueError): await configure_policy(h['pool'],h['tenant'],enabled=True,created_after=datetime.now())


async def test_null_apply_is_rejected_by_sql_not_treated_as_destructive(history):
    h=history;bid,_=await terminal(h);await policy(h)
    for query,args in [
      ('SELECT batchforge_retire_history_page(%s,%s,100,NULL)',(h['tenant'],bid)),
      ('SELECT batchforge_retire_idempotency_page(%s,0,100,NULL)',(h['tenant'],))]:
        with pytest.raises(psycopg.Error) as error: await execute(h,query,args)
        assert error.value.sqlstate=='BH400'
    assert (await ledger(h))['retained_tasks']==5


async def test_restore_verifier_rejects_tampered_retired_usage(history):
    h=history;bid,_=await terminal(h);await policy(h)
    await history_page(h['pool'],h['tenant'],bid,apply=True,archiver=h['archive'])
    with psycopg.connect(h['dsn']) as restored:
        assert verify_database(restored,MIGRATIONS)['history_rollup_mismatches']==0
    await execute(h,"UPDATE retired_attempt_hourly SET prompt_tokens=prompt_tokens+1 WHERE tenant_id=%s AND status='completed'",(h['tenant'],))
    try:
        with psycopg.connect(h['dsn']) as restored:
            with pytest.raises(RuntimeError,match='restored_invariant_mismatch'):
                verify_database(restored,MIGRATIONS)
    finally:
        await execute(h,"UPDATE retired_attempt_hourly SET prompt_tokens=prompt_tokens-1 WHERE tenant_id=%s AND status='completed'",(h['tenant'],))


async def corrupt_import_insert(h, query, args):
    # Deliberately model an owner/import corruption in a disposable database.
    # Ordinary application INSERT already rejects post-purge attempt insertion.
    async with h['pool'].connection() as conn,conn.transaction():
        await conn.execute('ALTER TABLE attempts DISABLE TRIGGER retention_attempt_guard')
        await conn.execute(query,args)
        await conn.execute('ALTER TABLE attempts ENABLE TRIGGER retention_attempt_guard')


@pytest.mark.parametrize('corruption',['missing','duplicate','tokens','accepted_failed'])
async def test_corrupt_accepted_attempt_evidence_is_not_destroyed(history,corruption):
    h=history
    bid,_=await terminal(h,statuses=['completed','failed','completed','completed','completed']);await policy(h)
    task=await one(h,"SELECT * FROM tasks WHERE batch_id=%s AND status=%s ORDER BY id LIMIT 1",(bid,'failed' if corruption=='accepted_failed' else 'completed'))
    attempt=await one(h,'SELECT * FROM attempts WHERE task_id=%s',(task['id'],))
    if corruption=='missing':
        # Owner-authorized destructive corruption only in this disposable test.
        async with h['pool'].connection() as conn,conn.transaction():
            await conn.execute('SELECT batchforge_retention_authorize_delete(%s)',(h['tenant'],))
            await conn.execute('DELETE FROM attempts WHERE token=%s',(attempt['token'],))
    elif corruption=='duplicate':
        await corrupt_import_insert(h,"""INSERT INTO attempts(token,task_id,batch_id,tenant_id,worker_id,number,status,prompt_tokens,completion_tokens,finished_at)
          VALUES(%s,%s,%s,%s,%s,2,'completed',5,3,clock_timestamp())""",('duplicate_'+secrets.token_hex(6),task['id'],bid,h['tenant'],attempt['worker_id']))
    elif corruption=='tokens':
        await execute(h,'UPDATE attempts SET prompt_tokens=prompt_tokens+1 WHERE token=%s',(attempt['token'],))
    else:
        await execute(h,"UPDATE attempts SET status='completed' WHERE token=%s",(attempt['token'],))
    before=await one(h,'SELECT count(*) AS n FROM attempts WHERE tenant_id=%s',(h['tenant'],))
    with pytest.raises(psycopg.Error) as error: await history_page(h['pool'],h['tenant'],bid,apply=True,archiver=h['archive'])
    assert error.value.sqlstate=='BR409'
    assert (await ledger(h))['retained_tasks']==5
    assert await one(h,'SELECT count(*) AS n FROM attempts WHERE tenant_id=%s',(h['tenant'],))==before
    assert (await one(h,'SELECT count(*) AS n FROM retired_attempt_hourly WHERE tenant_id=%s',(h['tenant'],)))['n']==0
    # Restore fixture integrity so subsequent whole-database restore validation
    # is independent of the deliberate corruption in this tenant.
    if corruption=='missing':
        await corrupt_import_insert(h,"""INSERT INTO attempts(token,task_id,batch_id,tenant_id,worker_id,number,status,prompt_tokens,completion_tokens,finished_at)
          VALUES(%s,%s,%s,%s,%s,1,'completed',5,3,clock_timestamp())""",(attempt['token'],task['id'],bid,h['tenant'],attempt['worker_id']))
    elif corruption=='duplicate':
        async with h['pool'].connection() as conn,conn.transaction():
            await conn.execute('SELECT batchforge_retention_authorize_delete(%s)',(h['tenant'],))
            await conn.execute('DELETE FROM attempts WHERE task_id=%s AND number=2',(task['id'],))
    elif corruption=='tokens':await execute(h,'UPDATE attempts SET prompt_tokens=prompt_tokens-1 WHERE token=%s',(attempt['token'],))
    else:await execute(h,"UPDATE attempts SET status='failed' WHERE token=%s",(attempt['token'],))


def test_migration_nowait_gate_fails_atomically_under_existing_reader():
    name='bf_history_gate_'+secrets.token_hex(6)
    with psycopg.connect(SOURCE,autocommit=True) as conn:
        conn.execute(sql.SQL('CREATE DATABASE {} TEMPLATE template0').format(sql.Identifier(name)))
    dsn=make_conninfo(SOURCE,dbname=name)
    try:
        with psycopg.connect(dsn) as conn:
            conn.execute(Path('backend/schema.sql').read_text())
            for n in range(2,11):conn.execute(next(Path('backend/migrations').glob(f'{n:04d}_*.sql')).read_text())
        with psycopg.connect(dsn) as reader:
            reader.execute('SELECT count(*) FROM batches')
            with psycopg.connect(dsn) as migrator:
                with pytest.raises(psycopg.Error) as error:
                    migrator.execute(Path('backend/migrations/0011_history_retirement.sql').read_text())
                assert error.value.sqlstate=='55P03'
                migrator.rollback()
                assert migrator.execute("SELECT to_regclass('public.tenant_history_retirement_policy')").fetchone()[0] is None
                assert not migrator.execute("SELECT 1 FROM information_schema.columns WHERE table_name='batches' AND column_name='history_retired_tasks'").fetchone()
    finally:
        with psycopg.connect(SOURCE,autocommit=True) as conn:
            conn.execute(sql.SQL('DROP DATABASE {} WITH(FORCE)').format(sql.Identifier(name)))


async def test_repeatable_read_stale_floor_writer_cannot_reintroduce_expired_key(history):
    h=history;await policy(h)
    now=(await one(h,"SELECT floor(extract(epoch FROM clock_timestamp()))::bigint AS n"))['n']
    key=timed_key(now-120)
    async with h['pool'].connection() as writer,writer.transaction():
        await writer.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
        await writer.execute('SELECT replay_floor FROM tenant_history_retirement_policy WHERE tenant_id=%s',(h['tenant'],))
        assert (await idempotency_page(h['pool'],h['tenant'],now-61,apply=True))['eligible']
        with pytest.raises(psycopg.Error) as error:
            await writer.execute("INSERT INTO idempotency(tenant_id,scope,key,body_hash) VALUES(%s,'create',%s,'hash')",(h['tenant'],key))
        assert error.value.sqlstate=='BR409'
    assert (await one(h,'SELECT count(*) AS n FROM idempotency WHERE tenant_id=%s',(h['tenant'],)))['n']==0
    # Repeatable read remains valid for a read-only replay check against its
    # current snapshot. Only writing through stale fences is unsupported.
    async with h['pool'].connection() as reader,reader.transaction():
        await reader.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        await reader.execute('SELECT batchforge_idempotency_check(%s,%s,%s)',(h['tenant'],'create','legacy-readonly'))


@pytest.mark.parametrize('operation',['task','retry'])
async def test_repeatable_read_stale_retirement_marker_cannot_resurrect_work(history,operation):
    h=history;bid,_=await terminal(h);await policy(h)
    original=await one(h,'SELECT input_file_id FROM batches WHERE id=%s',(bid,))
    async with h['pool'].connection() as writer,writer.transaction():
        await writer.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
        row=await (await writer.execute('SELECT history_retirement_started_at FROM batches WHERE id=%s',(bid,))).fetchone()
        assert row['history_retirement_started_at'] is None
        assert (await history_page(h['pool'],h['tenant'],bid,limit=2,apply=True,archiver=h['archive']))['tasks']==2
        with pytest.raises(psycopg.Error) as error:
            if operation=='task':
                await writer.execute("""INSERT INTO tasks(id,tenant_id,batch_id,custom_id,model,body,status,max_attempts)
                 VALUES(%s,%s,%s,'rr-resurrection','model','{}','pending',3)""",('rr_task_'+secrets.token_hex(6),h['tenant'],bid))
            else:
                await writer.execute("""INSERT INTO batches(id,tenant_id,input_file_id,model,status,total_count,pending_count,metadata)
                 VALUES(%s,%s,%s,'model','in_progress',1,1,%s)""",('rr_retry_'+secrets.token_hex(6),h['tenant'],original['input_file_id'],Jsonb({'retry_of':bid})))
        assert error.value.sqlstate=='BR409'
    assert (await ledger(h))['retained_tasks']==3
    assert (await one(h,'SELECT count(*) AS n FROM batches WHERE tenant_id=%s',(h['tenant'],)))['n']==1
