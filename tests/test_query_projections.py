"""Real PostgreSQL result-size and public-response regressions.

An explicit TEST_DATABASE_URL must point to an ephemeral CI PostgreSQL with
CREATE DATABASE privilege. Each case migrates and drops only its own random
database. Tests import canonical Store directly; no private candidate or hash
pin, GPU, HTTP endpoint, object service, or production fallback is used.
"""
from contextlib import asynccontextmanager
import json
import os
import secrets

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool
import pytest

from backend.config import Settings
from backend.migrate import SCHEMA_VERSION, migrate_database
from backend.store import APIError, Store, file_view, worker_view


class Database:
    def __init__(self, dsn):
        self.dsn = dsn

    def __repr__(self):
        return '<isolated query projection test database>'


@pytest.fixture
def database():
    source = os.environ.get('TEST_DATABASE_URL')
    if not source:
        pytest.skip('Set TEST_DATABASE_URL to an ephemeral PostgreSQL with CREATE DATABASE privilege')
    name = 'bf_query_projection_' + secrets.token_hex(8)
    with psycopg.connect(source, autocommit=True, connect_timeout=5) as admin:
        admin.execute(sql.SQL('CREATE DATABASE {} TEMPLATE template0').format(sql.Identifier(name)))
    dsn = make_conninfo(source, dbname=name)
    try:
        migrate_database(dsn)
        with psycopg.connect(dsn, autocommit=True, connect_timeout=5) as conn:
            conn.execute("INSERT INTO tenants(id,name) VALUES('default','Synthetic')")
        yield Database(dsn)
    finally:
        with psycopg.connect(source, autocommit=True, connect_timeout=5) as admin:
            admin.execute(sql.SQL('DROP DATABASE {} WITH(FORCE)').format(sql.Identifier(name)))


class ObservedCursor:
    def __init__(self, cursor, statement, log):
        self.cursor, self.statement, self.log = cursor, statement, log

    def observe(self, rows):
        result = self.cursor.pgresult
        self.log.append({
            'statement': self.statement,
            'columns': tuple(column.name for column in self.cursor.description),
            # Actual libpq result field bytes, before Python decoding. This
            # excludes protocol framing/TLS; it is not a provider billing meter.
            'result_field_bytes': sum(len(result.get_value(row, column) or b'')
                for row in range(result.ntuples) for column in range(result.nfields)),
            'metadata_json_bytes': sum(len(json.dumps(row['metadata'], sort_keys=True,
                separators=(',', ':')).encode()) for row in rows if 'metadata' in row),
        })
        return rows

    async def fetchone(self):
        row = await self.cursor.fetchone()
        self.observe([row] if row is not None else [])
        return row

    async def fetchall(self):
        return self.observe(await self.cursor.fetchall())


class ObservedConnection:
    def __init__(self, connection, log):
        self.connection, self.log = connection, log

    async def execute(self, statement, params=()):
        return ObservedCursor(await self.connection.execute(statement, params), statement, self.log)

    def __getattr__(self, name):
        return getattr(self.connection, name)


class ObservedPool:
    def __init__(self, pool):
        self.pool, self.log = pool, []

    @asynccontextmanager
    async def connection(self):
        async with self.pool.connection() as conn:
            yield ObservedConnection(conn, self.log)


@asynccontextmanager
async def store(database):
    settings = Settings(database_url=database.dsn, model_name='synthetic-model',
        batchforge_api_key='synthetic-not-secret', batchforge_worker_token='synthetic-not-secret',
        max_request_bytes=200000)
    async with AsyncConnectionPool(database.dsn, min_size=1, max_size=2, open=False,
            kwargs={'autocommit': True, 'row_factory': dict_row}) as actual:
        proxy = ObservedPool(actual)
        yield Store(proxy, settings, schema_version=SCHEMA_VERSION), proxy


@pytest.mark.asyncio
@pytest.mark.parametrize('metadata', [
    {'engine_count': 4, 'ready_engine_count': 3, 'unused': 'SYNTHETIC-LARGE-'*10000},
    {'engine_count': True, 'ready_engine_count': 5000, 'unused': {'payload': 'SYNTHETIC'*20000}},
    ['SYNTHETIC-NONOBJECT'],
    'SYNTHETIC-SCALAR',
    None,
])
async def test_overview_preserves_worker_view_without_fetching_unused_metadata(database, metadata):
    with psycopg.connect(database.dsn, autocommit=True, row_factory=dict_row) as conn:
        original = conn.execute("INSERT INTO workers(id,name,model,concurrency,metadata) "
            "VALUES('w','Synthetic','synthetic-model',64,%s) RETURNING *", (Jsonb(metadata),)).fetchone()
    expected = worker_view({**original, 'status': 'online'})
    async with store(database) as (subject, proxy):
        result = await subject.overview('default')
        event = next(item for item in proxy.log if 'FROM workers' in item['statement'])
    assert result['workers'] == [expected]
    assert 'metadata' not in result['workers'][0]
    assert event['metadata_json_bytes'] < 100
    assert event['result_field_bytes'] < 1024
    with psycopg.connect(database.dsn, autocommit=True) as conn:
        assert conn.execute("SELECT metadata FROM workers WHERE id='w'").fetchone()[0] == metadata


def content():
    return (json.dumps({'custom_id': 'large', 'method': 'POST', 'url': '/v1/chat/completions',
        'body': {'model': 'synthetic-model', 'messages': [
            {'role': 'user', 'content': 'SYNTHETIC-PROMPT-'*6000}], 'max_tokens': 8}})+'\n').encode()


@pytest.mark.asyncio
async def test_upload_retains_exact_input_without_returning_payload_to_client_cursor(database):
    payload = content()
    assert len(payload) > 100000
    async with store(database) as (subject, proxy):
        result = await subject.upload('default', 'large.jsonl', payload)
        event = next(item for item in proxy.log if 'INSERT INTO files' in item['statement'])
        assert 'content' not in event['columns']
        assert event['result_field_bytes'] < 1024
        assert result['bytes'] == len(payload)
        async with proxy.pool.connection() as conn:
            row = await (await conn.execute('SELECT * FROM files WHERE id=%s', (result['id'],))).fetchone()
    assert bytes(row['content']) == payload
    assert file_view(row) == result


@pytest.mark.asyncio
async def test_idempotency_replay_conflict_and_row_lock_survive_narrow_projection(database):
    async with store(database) as (subject, proxy):
        async with proxy.connection() as conn, conn.transaction():
            assert await subject.idempotency(conn, 'default', '/v1/batches', 'key', {'input_file_id': 'synthetic'}) is None
            await subject.remember(conn, 'default', '/v1/batches', 'key', {'id': 'batch-synthetic'})
        async with proxy.connection() as conn, conn.transaction():
            assert await subject.idempotency(conn, 'default', '/v1/batches', 'key', {'input_file_id': 'synthetic'}) == {'id': 'batch-synthetic'}
        with pytest.raises(APIError) as failure:
            async with proxy.connection() as conn, conn.transaction():
                await subject.idempotency(conn, 'default', '/v1/batches', 'key', {'input_file_id': 'different'})
        assert failure.value.status == 409 and failure.value.code == 'idempotency_conflict'
        selected = [item for item in proxy.log if 'FROM idempotency' in item['statement']]
        assert len(selected) == 3
        assert all(set(item['columns']) == {'body_hash', 'response'} for item in selected)
        assert all('FOR UPDATE' in item['statement'] for item in selected)


@pytest.mark.asyncio
@pytest.mark.parametrize('external', [False, True])
async def test_create_fetches_stored_content_only_when_inline_validation_needs_it(database, external):
    payload = content()
    with psycopg.connect(database.dsn, autocommit=True) as conn:
        conn.execute("INSERT INTO files(id,tenant_id,filename,kind,content,bytes) "
            "VALUES('input','default','input','input',%s,%s)", (payload, len(payload)))

    class ExternalInput:
        def __init__(self):
            self.admitted = []

        async def read_input(self, tenant, file_id):
            assert (tenant, file_id) == ('default', 'input')
            return payload

        async def admit_batch(self, conn, tenant, file_id, count):
            self.admitted.append((tenant, file_id, count))

    async with store(database) as (subject, proxy):
        if external:
            subject.blobs = ExternalInput()
        result = await subject.create('default', {'input_file_id': 'input'}, None)
        lookup = next(item for item in proxy.log if 'FROM files WHERE id=' in item['statement'])
        assert set(lookup['columns']) == ({'id'} if external else {'id', 'content'})
        if external:
            assert subject.blobs.admitted == [('default', 'input', 1)]
            assert lookup['result_field_bytes'] < 1024
        else:
            assert lookup['result_field_bytes'] >= len(payload)
    assert result['request_counts']['total'] == 1
    with psycopg.connect(database.dsn, autocommit=True) as conn:
        assert bytes(conn.execute("SELECT content FROM files WHERE id='input'").fetchone()[0]) == payload
        task_body = conn.execute('SELECT body FROM tasks WHERE batch_id=%s', (result['id'],)).fetchone()[0]
        assert task_body == json.loads(payload)['body']
