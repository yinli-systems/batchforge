"""Storage correctness: immutable artifacts plus real isolated PostgreSQL races.

PG tests create their own database under TEST_DATABASE_URL; they never truncate
shared test tables. The object adapter tests require no external service.
"""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
import hashlib
import io
import json
import os
from pathlib import Path
import secrets
import threading

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
import pytest
import pytest_asyncio

from backend.blob_storage import (BlobCatalog, BlobRef, BlobStorageError,
    BlobIntegrityError, BlobNotFound, BlobExpired, BlobQuotaExceeded,
    LocalBlobStore, S3BlobStore)


def reference(content=b'valid JSONL content\n', tenant='tenant-a', backend='local'):
    digest=hashlib.sha256(content).hexdigest();bid='blob_'+secrets.token_hex(16)
    return BlobRef(bid,tenant,digest,len(content),backend,
        f'objects/{hashlib.sha256(tenant.encode()).hexdigest()}/{digest}/{bid}')


def test_local_atomic_concurrent_publish_and_checksum_before_yield(tmp_path):
    store=LocalBlobStore(tmp_path/'objects');content=b'a'*200000;ref=reference(content)
    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(lambda _:store.put_verified(ref,content),range(8)))
    assert not list(store.root.rglob('*.tmp'))
    with store.open_verified(ref) as stream:assert stream.read()==content
    target=store.root/ref.object_key;assert target.stat().st_mode&0o777==0o600
    target.write_bytes(b'b'*len(content))
    yielded=False
    with pytest.raises(BlobIntegrityError):
        with store.open_verified(ref):yielded=True
    assert not yielded


def test_local_rejects_cross_namespace_and_symlink(tmp_path):
    store=LocalBlobStore(tmp_path/'objects');ref=reference()
    with pytest.raises(BlobIntegrityError):store.put_verified(replace(ref,tenant_id='../other'),b'valid JSONL content\n')
    outside=tmp_path/'outside';outside.mkdir()
    (store.root/'objects').symlink_to(outside,target_is_directory=True)
    with pytest.raises(BlobIntegrityError):store.put_verified(ref,b'valid JSONL content\n')
    assert not list(outside.iterdir())


class S3MemoryClient:
    def __init__(self):self.objects={};self.bodies=[]
    def put_object(self,**kwargs):
        self.objects[(kwargs['Bucket'],kwargs['Key'])]={'content':kwargs['Body'],'metadata':kwargs['Metadata']}
    def get_object(self,**kwargs):
        row=self.objects[(kwargs['Bucket'],kwargs['Key'])];body=io.BytesIO(row['content']);self.bodies.append(body)
        return {'Body':body,'ContentLength':len(row['content']),'Metadata':row['metadata']}
    def delete_object(self,**kwargs):self.objects.pop((kwargs['Bucket'],kwargs['Key']),None)


def test_s3_verifies_spooled_download_and_closes_corrupt_body():
    client=S3MemoryClient();store=S3BlobStore(client,'private-bucket',spool_bytes=4);content=b'hello world\n';ref=reference(content,backend='s3')
    store.put_verified(ref,content)
    with store.open_verified(ref) as stream:
        assert stream.read()==content
        assert stream._rolled
    key=next(iter(client.objects));client.objects[key]['content']=b'x'*len(content)
    yielded=False
    with pytest.raises(BlobIntegrityError):
        with store.open_verified(ref):yielded=True
    assert not yielded and all(body.closed for body in client.bodies)
    store.delete(ref);store.delete(ref)
    assert not client.objects


@pytest.fixture(scope='module')
def blob_dsn():
    dsn=os.getenv('TEST_DATABASE_URL')
    if not dsn:pytest.skip('Set TEST_DATABASE_URL for isolated real PostgreSQL storage tests')
    name='bf_blob_'+secrets.token_hex(8)
    with psycopg.connect(dsn,autocommit=True) as conn:conn.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
    values=conninfo_to_dict(dsn);values['dbname']=name;private=make_conninfo(**values)
    try:
        with psycopg.connect(private) as conn:
            for path in [Path('backend/schema.sql'),Path('backend/migrations/0002_sql_completion.sql'),Path('backend/migrations/0003_sql_claim.sql'),Path('backend/migrations/0005_blob_storage.sql')]:conn.execute(path.read_text())
        yield private
    finally:
        with psycopg.connect(dsn,autocommit=True) as conn:conn.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(name)))


@pytest_asyncio.fixture
async def catalog(blob_dsn,tmp_path):
    async with AsyncConnectionPool(blob_dsn,min_size=1,max_size=8,kwargs={'row_factory':dict_row},open=False) as pool:
        async with pool.connection() as conn:
            await conn.execute('TRUNCATE blob_read_leases,blob_upload_reservations,blob_objects,tenant_blob_usage,idempotency,attempts,tasks,batches,files,workers,tenant_queues CASCADE')
            await conn.execute("INSERT INTO tenants(id,name) VALUES('a','A'),('b','B') ON CONFLICT DO NOTHING")
        yield BlobCatalog(pool,LocalBlobStore(tmp_path/'objects'),max_stored_input_bytes_per_tenant=1000000,max_queued_tasks_per_tenant=100,read_lease_seconds=1)


async def row(catalog,statement,params=()):
    async with catalog.pool.connection() as conn:return await (await conn.execute(statement,params)).fetchone()


async def execute(catalog,statement,params=()):
    async with catalog.pool.connection() as conn:await conn.execute(statement,params)


async def expire_file(catalog,file):
    await execute(catalog,"UPDATE files SET storage_expires_at=clock_timestamp()-interval '1 second' WHERE id=%s",(file['id'],))


async def insert_batch(conn,file,number=1):
    return await (await conn.execute("INSERT INTO batches(id,tenant_id,input_file_id,model,status,total_count,pending_count) VALUES(%s,%s,%s,'test','in_progress',%s,%s) RETURNING *",('batch_'+secrets.token_hex(8),file['tenant_id'],file['id'],number,number))).fetchone()


async def test_concurrent_byte_admission_and_same_tenant_cas(catalog):
    catalog.max_bytes=100;content=b'x'*20
    results=await asyncio.gather(*(catalog.put_input('a',str(n),content,1) for n in range(12)),return_exceptions=True)
    files=[v for v in results if isinstance(v,dict)];errors=[v for v in results if isinstance(v,Exception)]
    assert len(files)==5 and all(isinstance(v,BlobQuotaExceeded) for v in errors)
    assert len({v['blob_id'] for v in files})==1
    assert all(v['content']is None for v in files)
    usage=await row(catalog,"SELECT * FROM tenant_blob_usage WHERE tenant_id='a'")
    assert (usage['stored_bytes'],usage['reserved_bytes'],usage['held_tasks'])==(100,0,5)
    assert await catalog.read_input('a',files[0]['id'])==content


async def test_upload_task_credit_cannot_bypass_queue_or_transaction(catalog):
    catalog.max_tasks=3
    first=await catalog.put_input('a','first',b'first',2)
    with pytest.raises(BlobQuotaExceeded,match='queue'):await catalog.put_input('a','second',b'second',2)
    with pytest.raises(RuntimeError):
        async with catalog.pool.connection() as conn,conn.transaction():
            await catalog.admit_batch(conn,'a',first['id'],2)
            raise RuntimeError('rollback batch')
    assert (await row(catalog,"SELECT held_tasks FROM tenant_blob_usage WHERE tenant_id='a'"))['held_tasks']==2
    async with catalog.pool.connection() as conn,conn.transaction():
        await catalog.admit_batch(conn,'a',first['id'],2);await insert_batch(conn,first,2)
    with pytest.raises(BlobQuotaExceeded):await catalog.put_input('a','blocked',b'x',2)
    last=await catalog.put_input('a','last',b'last',1)
    assert not await catalog.discard_unsubmitted('a',first['id'])
    assert await catalog.discard_unsubmitted('a',last['id'])
    assert not await catalog.discard_unsubmitted('a',last['id'])
    assert (await row(catalog,"SELECT held_tasks FROM tenant_blob_usage WHERE tenant_id='a'"))['held_tasks']==0


async def test_tenant_isolation_and_legacy_inline_reads(catalog):
    content=b'same content';a=await catalog.put_input('a','a',content,1);b=await catalog.put_input('b','b',content,1)
    assert a['blob_id']!=b['blob_id']
    with pytest.raises(BlobNotFound):await catalog.read_input('b',a['id'])
    assert not await catalog.discard_unsubmitted('b',a['id'])
    await execute(catalog,"INSERT INTO files(id,tenant_id,filename,kind,content,bytes) VALUES('legacy','a','legacy','input',%s,%s)",(content,len(content)))
    assert await catalog.read_input('a','legacy')==content
    await execute(catalog,"UPDATE files SET storage_expires_at=clock_timestamp()-interval '1 second' WHERE id='legacy'")
    await catalog.expire_inputs()
    assert await catalog.read_input('a','legacy')==content


async def test_failed_external_write_is_registered_quota_released_and_gc(catalog):
    original=catalog.adapter.put_verified
    def fail_after_write(ref,content):original(ref,content);raise BlobStorageError('injected storage failure')
    catalog.adapter.put_verified=fail_after_write
    with pytest.raises(BlobStorageError):await catalog.put_input('a','failure',b'payload',1)
    usage=await row(catalog,"SELECT * FROM tenant_blob_usage WHERE tenant_id='a'")
    assert (usage['stored_bytes'],usage['reserved_bytes'],usage['held_tasks'])==(0,0,0)
    obj=await row(catalog,'SELECT * FROM blob_objects');assert obj['state']=='deleting'
    assert (catalog.adapter.root/obj['object_key']).exists()
    assert (await catalog.collect_orphans(grace_seconds=0))['deleted_objects']==1
    assert not(catalog.adapter.root/obj['object_key']).exists()
    assert (await row(catalog,'SELECT state FROM blob_objects'))['state']=='deleted'


async def test_failed_database_publication_keeps_reapable_orphan(catalog):
    await execute(catalog,"CREATE FUNCTION fail_blob_file() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.blob_id IS NOT NULL THEN RAISE EXCEPTION 'injected'; END IF; RETURN NEW; END $$")
    await execute(catalog,'CREATE TRIGGER fail_blob_file BEFORE INSERT ON files FOR EACH ROW EXECUTE FUNCTION fail_blob_file()')
    try:
        with pytest.raises(psycopg.errors.RaiseException):await catalog.put_input('a','failure',b'payload',1)
    finally:
        await execute(catalog,'DROP TRIGGER fail_blob_file ON files');await execute(catalog,'DROP FUNCTION fail_blob_file()')
    assert (await row(catalog,'SELECT count(*) AS n FROM files'))['n']==0
    usage=await row(catalog,"SELECT * FROM tenant_blob_usage WHERE tenant_id='a'");assert usage['reserved_bytes']==usage['stored_bytes']==usage['held_tasks']==0
    assert (await catalog.collect_orphans(grace_seconds=0))['deleted_objects']==1


async def test_read_lease_pins_payload_through_retention_and_slow_client(catalog):
    content=b'x'*150000;file=await catalog.put_input('a','download',content,1)
    async with catalog.open_input('a',file['id']) as chunks:
        await expire_file(catalog,file);assert (await catalog.expire_inputs())['expired_files']==1
        await asyncio.sleep(1.2)
        assert (await catalog.collect_orphans(grace_seconds=0))['deleted_objects']==0
        # A slow consumer does not hold a database connection.
        assert (await row(catalog,'SELECT 1 AS n'))['n']==1
        assert b''.join([chunk async for chunk in chunks])==content
    assert (await catalog.collect_orphans(grace_seconds=0))['deleted_objects']==1
    with pytest.raises(BlobExpired):await catalog.read_input('a',file['id'])


async def test_retention_preserves_active_batch_and_only_releases_once(catalog):
    file=await catalog.put_input('a','active',b'payload',1)
    async with catalog.pool.connection() as conn,conn.transaction():
        await catalog.admit_batch(conn,'a',file['id'],1);batch=await insert_batch(conn,file)
    await expire_file(catalog,file);assert (await catalog.expire_inputs())['expired_files']==0
    await execute(catalog,"UPDATE batches SET status='completed',pending_count=0,completed_count=1 WHERE id=%s",(batch['id'],))
    assert (await catalog.expire_inputs())['expired_files']==1
    assert (await catalog.expire_inputs())['expired_files']==0
    assert (await row(catalog,"SELECT stored_bytes FROM tenant_blob_usage WHERE tenant_id='a'"))['stored_bytes']==0


async def test_delayed_gc_cannot_delete_reuploaded_generation(catalog):
    content=b'unchanged bytes';old=await catalog.put_input('a','old',content,1);assert await catalog.discard_unsubmitted('a',old['id'])
    entered,release=threading.Event(),threading.Event();original=catalog.adapter.delete
    def delayed_delete(ref):entered.set();assert release.wait(10);original(ref)
    catalog.adapter.delete=delayed_delete
    gc=asyncio.create_task(catalog.collect_orphans(grace_seconds=0))
    assert await asyncio.to_thread(entered.wait,5)
    try:
        new=await catalog.put_input('a','new',content,1)
        assert new['blob_id']!=old['blob_id']
    finally:release.set()
    assert (await gc)['deleted_objects']==1
    assert await catalog.read_input('a',new['id'])==content


async def test_live_objects_do_not_starve_bounded_gc_and_expired_leases_are_removed(catalog):
    live=await catalog.put_input('a','live',b'live',1)
    old=await catalog.put_input('a','discard',b'orphan',1)
    assert await catalog.discard_unsubmitted('a',old['id'])
    await execute(catalog,"INSERT INTO blob_read_leases(id,tenant_id,blob_id,expires_at) VALUES('expired','a',%s,clock_timestamp()-interval '1 second')",(live['blob_id'],))
    assert (await catalog.collect_orphans(limit=1,grace_seconds=0))['deleted_objects']==1
    assert (await row(catalog,'SELECT count(*) AS n FROM blob_read_leases'))['n']==0
    assert await catalog.read_input('a',live['id'])==b'live'


async def test_expired_upload_reservation_releases_bytes_and_task_credits(catalog):
    token,_=await catalog._reserve('a',b'payload',4)
    await execute(catalog,"UPDATE blob_upload_reservations SET expires_at=clock_timestamp()-interval '1 second' WHERE id=%s",(token,))
    assert (await catalog.collect_orphans(limit=1,grace_seconds=0))['expired_reservations_examined']==1
    usage=await row(catalog,"SELECT * FROM tenant_blob_usage WHERE tenant_id='a'")
    assert (usage['stored_bytes'],usage['reserved_bytes'],usage['held_tasks'])==(0,0,0)


async def test_cancelled_upload_late_sdk_write_remains_reapable(catalog):
    entered,release,finished=threading.Event(),threading.Event(),threading.Event()
    original=catalog.adapter.put_verified
    def late_write(ref,content):
        entered.set()
        try:
            assert release.wait(10)
            original(ref,content)
        finally:finished.set()
    catalog.adapter.put_verified=late_write
    upload=asyncio.create_task(catalog.put_input('a','cancelled',b'late payload',1))
    assert await asyncio.to_thread(entered.wait,5)
    try:
        upload.cancel()
        with pytest.raises(asyncio.CancelledError):await upload
        # The first delete can finish before the abandoned SDK write does.
        assert (await catalog.collect_orphans(grace_seconds=0))['deleted_objects']==1
    finally:release.set()
    assert await asyncio.to_thread(finished.wait,5)
    obj=await row(catalog,'SELECT * FROM blob_objects')
    assert obj['state']=='deleted' and (catalog.adapter.root/obj['object_key']).exists()
    await execute(catalog,"UPDATE blob_objects SET gc_after=clock_timestamp()-interval '1 second'")
    assert (await catalog.collect_orphans(grace_seconds=0))['deleted_objects']==1
    assert not(catalog.adapter.root/obj['object_key']).exists()
    usage=await row(catalog,"SELECT * FROM tenant_blob_usage WHERE tenant_id='a'")
    assert (usage['stored_bytes'],usage['reserved_bytes'],usage['held_tasks'])==(0,0,0)


async def test_cancelled_download_closes_late_sdk_stream_and_releases_lease(catalog):
    file=await catalog.put_input('a','download',b'payload',1)
    entered,release=threading.Event(),threading.Event();streams=[]
    original=catalog.adapter.open_verified
    @contextmanager
    def delayed(ref):
        entered.set();assert release.wait(10)
        with original(ref) as stream:
            streams.append(stream)
            yield stream
    catalog.adapter.open_verified=delayed
    download=asyncio.create_task(catalog.read_input('a',file['id']))
    assert await asyncio.to_thread(entered.wait,5)
    try:
        download.cancel();await asyncio.sleep(.05)
        assert not download.done()
        assert (await row(catalog,'SELECT count(*) AS n FROM blob_read_leases'))['n']==1
    finally:release.set()
    with pytest.raises(asyncio.CancelledError):await download
    assert len(streams)==1 and streams[0].closed
    assert (await row(catalog,'SELECT count(*) AS n FROM blob_read_leases'))['n']==0
