"""Real-PG hard metadata admission, replay and tenant-first maintenance races."""
import asyncio
import hashlib
import os
from pathlib import Path
import secrets
import time

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool
import pytest
import pytest_asyncio

from backend.auxiliary_admission import (tenant_prelock, release_read_lease,
    collect_expired_read_leases)
from backend.blob_storage import BlobCatalog, LocalBlobStore, BlobStorageError
from backend.result_archival import ResultArchiver
from backend.store import batch_view

MIGRATION = Path('backend/migrations/0010_auxiliary_metadata_admission.sql')


def base_schema(conn):
    conn.execute(Path('backend/schema.sql').read_text())
    for n in range(2, 10):
        conn.execute(next(Path('backend/migrations').glob(f'{n:04d}_*.sql')).read_text())


@pytest.fixture(scope='module')
def aux_database():
    source = os.getenv('TEST_DATABASE_URL')
    if not source: pytest.skip('Requires isolated real PostgreSQL')
    name = 'bf_aux_' + secrets.token_hex(8)
    with psycopg.connect(source, autocommit=True) as conn:
        conn.execute(sql.SQL('CREATE DATABASE {} TEMPLATE template0').format(sql.Identifier(name)))
    database = make_conninfo(source, dbname=name)
    try:
        with psycopg.connect(database) as conn:
            base_schema(conn); conn.execute(MIGRATION.read_text())
        yield database
    finally:
        with psycopg.connect(source, autocommit=True) as conn:
            conn.execute(sql.SQL('DROP DATABASE {} WITH(FORCE)').format(sql.Identifier(name)))


@pytest_asyncio.fixture
async def catalog(aux_database, tmp_path):
    async with AsyncConnectionPool(aux_database, min_size=1, max_size=12,
            kwargs={'autocommit': True, 'row_factory': dict_row}, open=False) as pool:
        tenant = 'tenant_' + secrets.token_hex(8)
        async with pool.connection() as conn:
            await conn.execute('INSERT INTO tenants(id,name) VALUES(%s,%s)', (tenant, tenant))
            await conn.execute('SELECT batchforge_auxiliary_lock(%s)', (tenant,))
        adapter = LocalBlobStore(tmp_path / 'objects')
        result = BlobCatalog(pool, adapter, max_stored_input_bytes_per_tenant=1000000,
            max_queued_tasks_per_tenant=100, read_lease_seconds=1)
        result.test_tenant = tenant
        yield result


async def execute(catalog, query, args=()):
    async with catalog.pool.connection() as conn: return await conn.execute(query, args)


async def one(catalog, query, args=()):
    return await (await execute(catalog, query, args)).fetchone()


async def limit(catalog, resource, **limits):
    for name, value in limits.items():
        assert name in {'max_rows', 'max_bytes', 'max_row_bytes'}
        await execute(catalog, sql.SQL('UPDATE tenant_auxiliary_limits SET {}=%s WHERE tenant_id=%s AND resource=%s').format(
            sql.Identifier(name)), (value, catalog.test_tenant, resource))


async def usage(catalog, resource):
    return await one(catalog, 'SELECT rows,bytes FROM tenant_auxiliary_usage WHERE tenant_id=%s AND resource=%s',
                     (catalog.test_tenant, resource))


async def idem(catalog, key):
    return await one(catalog, "INSERT INTO idempotency(tenant_id,scope,key,body_hash) VALUES(%s,'create',%s,'hash') ON CONFLICT DO NOTHING RETURNING key",
                     (catalog.test_tenant, key))


async def terminal(catalog, count=1):
    tenant = catalog.test_tenant; tag = secrets.token_hex(8)
    bid, fid, oid = 'batch_' + tag, 'file_' + tag, 'output_' + tag
    async with catalog.pool.connection() as conn, conn.transaction():
        await conn.execute("INSERT INTO files(id,tenant_id,filename,kind) VALUES(%s,%s,'input','input')", (fid, tenant))
        await conn.execute("INSERT INTO batches(id,tenant_id,input_file_id,model,status,total_count,completed_count) VALUES(%s,%s,%s,'model','completed',%s,%s)", (bid, tenant, fid, count, count))
        await conn.execute("INSERT INTO files(id,tenant_id,filename,kind,batch_id) VALUES(%s,%s,'output','output',%s)", (oid, tenant, bid))
        await conn.execute('UPDATE batches SET output_file_id=%s WHERE id=%s', (oid, bid))
        for n in range(count):
            await conn.execute("INSERT INTO tasks(id,tenant_id,batch_id,custom_id,model,body,status,max_attempts,response) VALUES(%s,%s,%s,%s,'model','{}','completed',3,%s)",
                               ('task_' + tag + str(n), tenant, bid, str(n), Jsonb({'choices': [{'message': {'content': 'ok'}}]})))
    return bid, oid


async def test_concurrent_upload_file_reservation_and_publish_at_exact_cap(catalog):
    await limit(catalog, 'input_files', max_rows=2)
    results = await asyncio.gather(*(catalog.put_input(catalog.test_tenant, str(i), b'same', 1)
                                    for i in range(10)), return_exceptions=True)
    assert sum(isinstance(r, dict) for r in results) == 2
    assert all(isinstance(r, dict) or (isinstance(r, psycopg.Error) and r.sqlstate == 'BQ429') for r in results)
    assert await usage(catalog, 'input_files') == {'rows': 2, 'bytes': 0}
    assert (await usage(catalog, 'upload_reservations'))['rows'] == 0
    assert (await usage(catalog, 'blob_generations'))['rows'] == 1


async def test_idempotency_replay_at_cap_and_concurrent_distinct_new_keys(catalog):
    await limit(catalog, 'idempotency', max_rows=2)
    results = await asyncio.gather(*(idem(catalog, str(i)) for i in range(10)), return_exceptions=True)
    accepted = [r['key'] for r in results if isinstance(r, dict)]
    assert len(accepted) == 2
    assert all(isinstance(r, dict) or (isinstance(r, psycopg.Error) and r.sqlstate == 'BQ429') for r in results)
    assert await idem(catalog, accepted[0]) is None  # conflict consumes no new credit
    assert await usage(catalog, 'idempotency') == {'rows': 2, 'bytes': 2 * 65536}


async def test_idempotency_reserved_response_and_byte_quota(catalog):
    await limit(catalog, 'idempotency', max_bytes=32, max_row_bytes=32)
    await idem(catalog, 'one')
    with pytest.raises(psycopg.Error) as error: await idem(catalog, 'two')
    assert error.value.sqlstate == 'BQ429'
    await execute(catalog, "UPDATE idempotency SET response='{}' WHERE tenant_id=%s", (catalog.test_tenant,))
    with pytest.raises(psycopg.Error) as error:
        await execute(catalog, 'UPDATE idempotency SET response=%s WHERE tenant_id=%s',
                      (Jsonb({'oversized': 'x' * 100}), catalog.test_tenant))
    assert error.value.sqlstate == 'BP413'
    assert (await usage(catalog, 'idempotency'))['bytes'] == 32
    assert await idem(catalog, 'one') is None


@pytest.mark.parametrize('character', ['\U0001f680', '\x01'])
async def test_maximum_metadata_unicode_and_escaped_control_response_fit_default_reservation(catalog, character):
    bid, _ = await terminal(catalog)
    metadata = {chr(i + 1) * 64: character * 512 for i in range(16)}
    await execute(catalog, 'UPDATE batches SET metadata=%s,model=%s WHERE id=%s',
                  (Jsonb(metadata), '\x01' * 256, bid))
    row = await one(catalog, 'SELECT * FROM batches WHERE id=%s', (bid,))
    response = batch_view(row)
    await idem(catalog, 'maximum-metadata')
    await execute(catalog, 'UPDATE idempotency SET response=%s WHERE tenant_id=%s',
                  (Jsonb(response), catalog.test_tenant))
    result = await one(catalog, 'SELECT octet_length(response::text) AS actual,auxiliary_reserved_bytes AS reserved FROM idempotency WHERE tenant_id=%s', (catalog.test_tenant,))
    assert 32768 < result['actual'] < result['reserved'] == 65536


async def test_failed_put_returns_file_credit_but_retired_generation_remains_bounded(catalog):
    await limit(catalog, 'blob_generations', max_rows=1)
    calls = []
    def fail(*args): calls.append(1); raise BlobStorageError('injected object failure')
    catalog.adapter.put_verified = fail
    with pytest.raises(BlobStorageError): await catalog.put_input(catalog.test_tenant, 'x', b'first', 1)
    assert (await usage(catalog, 'input_files'))['rows'] == 0
    assert (await usage(catalog, 'upload_reservations'))['rows'] == 0
    assert (await usage(catalog, 'blob_generations'))['rows'] == 1
    await catalog.collect_orphans(grace_seconds=0)
    assert (await usage(catalog, 'blob_generations'))['rows'] == 1
    with pytest.raises(psycopg.Error) as error: await catalog.put_input(catalog.test_tenant, 'y', b'second', 1)
    assert error.value.sqlstate == 'BQ429' and len(calls) == 1


async def test_expired_upload_cleanup_releases_reserved_file_and_inflight_credit(catalog):
    await limit(catalog, 'input_files', max_rows=1)
    token, _ = await catalog._reserve(catalog.test_tenant, b'first', 1)
    await execute(catalog, "UPDATE blob_upload_reservations SET expires_at=clock_timestamp()-interval '1s' WHERE id=%s", (token,))
    await catalog.collect_orphans()
    assert (await usage(catalog, 'input_files'))['rows'] == 0
    assert (await usage(catalog, 'upload_reservations'))['rows'] == 0
    assert await catalog.put_input(catalog.test_tenant, 'new', b'new', 1)


async def test_retention_clears_payload_credit_not_retained_file_metadata(catalog):
    await limit(catalog, 'input_files', max_rows=1)
    file = await catalog.put_input(catalog.test_tenant, 'old', b'old', 1)
    assert await catalog.discard_unsubmitted(catalog.test_tenant, file['id'])
    assert (await usage(catalog, 'input_files'))['rows'] == 1
    with pytest.raises(psycopg.Error) as error: await catalog.put_input(catalog.test_tenant, 'new', b'new', 1)
    assert error.value.sqlstate == 'BQ429'


async def test_input_oversize_rejected_before_put_or_metadata_charge(catalog):
    await limit(catalog, 'input_files', max_row_bytes=3)
    with pytest.raises(psycopg.Error) as error: await catalog.put_input(catalog.test_tenant, 'x', b'large', 1)
    assert error.value.sqlstate == 'BP413'
    assert (await usage(catalog, 'input_files'))['rows'] == 0
    assert not list(catalog.adapter.root.rglob('blob_*'))


async def test_full_input_credit_does_not_add_ledger_lock_to_result_file(catalog):
    await limit(catalog, 'input_files', max_rows=1)
    bid, oid = await terminal(catalog)
    async with catalog.pool.connection() as held, held.transaction():
        await held.execute('SELECT batchforge_auxiliary_lock(%s)', (catalog.test_tenant,))
        async with catalog.pool.connection() as other, other.transaction():
            await other.execute("SET LOCAL lock_timeout='300ms'")
            await other.execute("INSERT INTO files(id,tenant_id,filename,kind,batch_id) VALUES(%s,%s,'error','error',%s)", ('error_' + secrets.token_hex(8), catalog.test_tenant, bid))
    assert (await usage(catalog, 'input_files'))['rows'] == 1
    with pytest.raises(psycopg.errors.UniqueViolation):
        await execute(catalog, "INSERT INTO files(id,tenant_id,filename,kind,batch_id) VALUES(%s,%s,'output','output',%s)", ('dup_' + secrets.token_hex(8), catalog.test_tenant, bid))


async def test_archive_generations_chunks_tombstones_and_database_result_fallback(catalog):
    bid, oid = await terminal(catalog, count=2)
    await limit(catalog, 'archive_runs', max_rows=1)
    await limit(catalog, 'archive_chunks', max_rows=1)
    archive = ResultArchiver(catalog.pool, catalog.adapter, max_archived_bytes_per_tenant=100000)
    run, token = await archive._begin(catalog.test_tenant, bid)
    await archive._chunk(run, token, oid, 0, b'{"one":1}\n')
    with pytest.raises(psycopg.Error) as error: await archive._chunk(run, token, oid, 1, b'{"two":2}\n')
    assert error.value.sqlstate == 'BQ429'  # valid ordinal, exhausted chunk counter
    with pytest.raises(psycopg.Error) as error: await archive._chunk(run, token, oid, 2, b'{"three":3}\n')
    assert error.value.sqlstate == 'BP413'  # ordinal cannot exceed result rows
    await archive._abort(catalog.test_tenant, bid, run['id'], token)
    await archive.collect_orphans(grace_seconds=0)
    assert (await usage(catalog, 'archive_runs'))['rows'] == 1
    assert (await usage(catalog, 'archive_chunks'))['rows'] == 1
    with pytest.raises(psycopg.Error) as error: await archive._begin(catalog.test_tenant, bid)
    assert error.value.sqlstate == 'BQ429'
    async with archive.open_file(catalog.test_tenant, oid) as chunks:
        raw = b''.join([part async for part in chunks])
    assert b'ok' in raw  # failed maintenance never purged the accepted SQL result


async def test_reader_capacity_renewals_expiry_and_actual_delete_credit(catalog):
    file = await catalog.put_input(catalog.test_tenant, 'x', b'x', 1)
    await limit(catalog, 'input_read_leases', max_rows=1)
    first, _ = await catalog._lease(catalog.test_tenant, file['id']); lease, ref = first
    with pytest.raises(psycopg.Error) as error: await catalog._lease(catalog.test_tenant, file['id'])
    assert error.value.sqlstate == 'BQ429'
    await execute(catalog, "UPDATE blob_read_leases SET expires_at=clock_timestamp()+interval '20s' WHERE id=%s", (lease,))
    assert (await usage(catalog, 'input_read_leases'))['rows'] == 1
    await execute(catalog, "UPDATE blob_read_leases SET expires_at=clock_timestamp()-interval '1s' WHERE id=%s", (lease,))
    assert await collect_expired_read_leases(catalog.pool, 'blob_read_leases', 1) == 1
    assert (await usage(catalog, 'input_read_leases'))['rows'] == 0
    second, _ = await catalog._lease(catalog.test_tenant, file['id'])
    await release_read_lease(catalog.pool, 'blob_read_leases', catalog.test_tenant, second[0])
    assert (await usage(catalog, 'input_read_leases'))['rows'] == 0


async def test_expiry_sweep_rechecks_renewal_after_tenant_lock(catalog):
    file = await catalog.put_input(catalog.test_tenant, 'x', b'x', 1)
    first, _ = await catalog._lease(catalog.test_tenant, file['id']); lease = first[0]
    await execute(catalog, "UPDATE blob_read_leases SET expires_at=clock_timestamp()-interval '1s' WHERE id=%s", (lease,))
    async with catalog.pool.connection() as conn, conn.transaction():
        await tenant_prelock(conn, catalog.test_tenant)
        await conn.execute("UPDATE blob_read_leases SET expires_at=clock_timestamp()+interval '20s' WHERE id=%s", (lease,))
        sweep = asyncio.create_task(collect_expired_read_leases(catalog.pool, 'blob_read_leases', 1))
        await asyncio.sleep(.05)
        assert not sweep.done()
    assert await sweep == 0
    assert (await usage(catalog, 'input_read_leases'))['rows'] == 1


async def test_low_privilege_cannot_change_limits_usage_or_call_internal_adjust(catalog):
    role = 'aux_' + secrets.token_hex(8)
    async with catalog.pool.connection() as conn:
        await conn.execute(sql.SQL('CREATE ROLE {}').format(sql.Identifier(role)))
        try:
            await conn.execute(sql.SQL('GRANT USAGE ON SCHEMA public TO {}').format(sql.Identifier(role)))
            await conn.execute(sql.SQL('GRANT INSERT,SELECT ON idempotency TO {}').format(sql.Identifier(role)))
            # Even a future accidentally broad table grant cannot let the app
            # rewrite authoritative policy/counters.
            await conn.execute(sql.SQL('GRANT ALL ON tenant_auxiliary_limits,tenant_auxiliary_usage TO {}').format(sql.Identifier(role)))
            await conn.execute(sql.SQL('SET ROLE {}').format(sql.Identifier(role)))
            assert not (await (await conn.execute("SELECT has_function_privilege(current_user,'public.batchforge_auxiliary_adjust(text,text,bigint,bigint)','EXECUTE') AS allowed")).fetchone())['allowed']
            await conn.execute("INSERT INTO idempotency(tenant_id,scope,key,body_hash) VALUES(%s,'x','one','hash')", (catalog.test_tenant,))
            for statement in ['UPDATE tenant_auxiliary_limits SET max_rows=999999',
                    'UPDATE tenant_auxiliary_usage SET rows=0',
                    'TRUNCATE tenant_auxiliary_usage']:
                with pytest.raises(psycopg.Error) as error: await conn.execute(statement)
                assert error.value.sqlstate == 'BR403'
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                await conn.execute("SELECT batchforge_auxiliary_adjust('x','input_files',-1,0)")
        finally:
            await conn.execute('RESET ROLE')
            await conn.execute(sql.SQL('DROP OWNED BY {}').format(sql.Identifier(role)))
            await conn.execute(sql.SQL('DROP ROLE {}').format(sql.Identifier(role)))


async def test_transaction_rollback_returns_all_metadata_credits(catalog):
    with pytest.raises(RuntimeError):
        async with catalog.pool.connection() as conn, conn.transaction():
            await conn.execute("INSERT INTO files(id,tenant_id,filename,kind) VALUES(%s,%s,'one','input')", ('file_' + secrets.token_hex(8), catalog.test_tenant))
            raise RuntimeError('rollback caller')
    assert (await usage(catalog, 'input_files'))['rows'] == 0


def test_migration_backfills_overcap_history_without_deleting_or_disabling_reads():
    source = os.getenv('TEST_DATABASE_URL')
    if not source: pytest.skip('Requires isolated real PostgreSQL')
    name = 'bf_aux_old_' + secrets.token_hex(8)
    with psycopg.connect(source, autocommit=True) as conn:
        conn.execute(sql.SQL('CREATE DATABASE {} TEMPLATE template0').format(sql.Identifier(name)))
    database = make_conninfo(source, dbname=name)
    try:
        with psycopg.connect(database) as conn:
            base_schema(conn)
            conn.execute("INSERT INTO tenants(id,name) VALUES('old','old')")
            conn.execute("INSERT INTO files(id,tenant_id,filename,kind) SELECT 'old-'||n,'old','old','input' FROM generate_series(1,10001) n")
            conn.execute("INSERT INTO idempotency(tenant_id,scope,key,body_hash,response) VALUES('old','scope','key','hash',%s)",
                         (Jsonb({'historical': 'x' * 300000}),))
            conn.execute(MIGRATION.read_text())
        with psycopg.connect(database, autocommit=True) as conn:
            assert conn.execute("SELECT rows FROM tenant_auxiliary_usage WHERE tenant_id='old' AND resource='input_files'").fetchone()[0] == 10001
            assert conn.execute("SELECT count(*) FROM files WHERE tenant_id='old'").fetchone()[0] == 10001
            conn.execute("UPDATE files SET storage_deleted_at=clock_timestamp() WHERE id='old-1'")
            conn.execute("UPDATE idempotency SET response=response WHERE tenant_id='old'")
            with pytest.raises(psycopg.Error) as error:
                conn.execute("INSERT INTO files(id,tenant_id,filename,kind) VALUES('new','old','new','input')")
            assert error.value.sqlstate == 'BQ429'
            assert conn.execute("SELECT auxiliary_reserved_bytes FROM idempotency WHERE tenant_id='old'").fetchone()[0] > 300000
    finally:
        with psycopg.connect(source, autocommit=True) as conn:
            conn.execute(sql.SQL('DROP DATABASE {} WITH(FORCE)').format(sql.Identifier(name)))


async def test_existing_lease_renewal_survives_lowered_overcap_policy(catalog):
    file = await catalog.put_input(catalog.test_tenant, 'x', b'x', 1)
    leases = [await catalog._lease(catalog.test_tenant, file['id']) for _ in range(2)]
    await limit(catalog, 'input_read_leases', max_rows=1)
    for first, _ in leases:
        await execute(catalog, "UPDATE blob_read_leases SET expires_at=clock_timestamp()+interval '20s' WHERE id=%s", (first[0],))
    assert (await usage(catalog, 'input_read_leases'))['rows'] == 2
    with pytest.raises(psycopg.Error) as error: await catalog._lease(catalog.test_tenant, file['id'])
    assert error.value.sqlstate == 'BQ429'


@pytest.mark.parametrize('held_resource', ['tenant', 'batch_completion'])
def test_migration_nowait_refuses_existing_writers_and_rolls_back_cleanly(held_resource):
    source = os.getenv('TEST_DATABASE_URL')
    if not source: pytest.skip('Requires isolated real PostgreSQL')
    name = 'bf_aux_lock_' + secrets.token_hex(8)
    with psycopg.connect(source, autocommit=True) as conn:
        conn.execute(sql.SQL('CREATE DATABASE {} TEMPLATE template0').format(sql.Identifier(name)))
    database = make_conninfo(source, dbname=name)
    try:
        with psycopg.connect(database) as conn:
            base_schema(conn); conn.execute("INSERT INTO tenants(id,name) VALUES('held','held')")
            conn.execute("INSERT INTO files(id,tenant_id,filename,kind) VALUES('old-input','held','old','input')")
            conn.execute("INSERT INTO batches(id,tenant_id,input_file_id,model,status,total_count,completed_count) VALUES('old-batch','held','old-input','model','completed',1,1)")
        with psycopg.connect(database) as held:
            if held_resource == 'tenant':
                held.execute("SELECT id FROM tenants WHERE id='held' FOR NO KEY UPDATE")
            else:
                held.execute("UPDATE batches SET completed_at=clock_timestamp() WHERE id='old-batch'")
            with psycopg.connect(database) as migration:
                started = time.monotonic()
                with pytest.raises(psycopg.errors.LockNotAvailable): migration.execute(MIGRATION.read_text())
                assert time.monotonic() - started < 1
                migration.rollback()
                assert migration.execute("SELECT to_regclass('public.tenant_auxiliary_limits')").fetchone()[0] is None
                assert not migration.execute("SELECT 1 FROM information_schema.columns WHERE table_name='idempotency' AND column_name='auxiliary_reserved_bytes'").fetchone()
            # A later gate failure must release the earlier tenants relation gate.
            with psycopg.connect(database, options='-c lock_timeout=200') as observer:
                observer.execute("SELECT id FROM tenants WHERE id='held' FOR KEY SHARE")
        with psycopg.connect(database) as conn:
            conn.execute(MIGRATION.read_text())
            assert conn.execute("SELECT rows FROM tenant_auxiliary_usage WHERE tenant_id='held' AND resource='input_files'").fetchone()[0] == 1
            assert not conn.execute("SELECT 1 FROM pg_constraint WHERE connamespace='public'::regnamespace AND NOT convalidated").fetchone()
    finally:
        with psycopg.connect(source, autocommit=True) as conn:
            conn.execute(sql.SQL('DROP DATABASE {} WITH(FORCE)').format(sql.Identifier(name)))
