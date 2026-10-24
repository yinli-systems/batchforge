"""Real PostgreSQL result archival, publication, reader and orphan boundaries."""
import asyncio
import hashlib
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
import pytest_asyncio

from backend.blob_storage import LocalBlobStore, BlobIntegrityError, BlobBusy, BlobNotFound, BlobStorageError
from backend.result_archival import ResultArchiver, ResultQuotaExceeded


@pytest.fixture(scope='module')
def archive_database():
    source=os.getenv('TEST_DATABASE_URL')
    if not source:pytest.skip('Requires isolated real PostgreSQL')
    name='bf_archive_'+secrets.token_hex(8)
    with psycopg.connect(source,autocommit=True) as conn:
        conn.execute(sql.SQL('CREATE DATABASE {} TEMPLATE template0').format(sql.Identifier(name)))
    database=make_conninfo(source,dbname=name)
    try:
        root=Path(__file__).resolve().parents[1]
        with psycopg.connect(database) as conn:
            conn.execute((root/'backend/schema.sql').read_text())
            for version in range(2,9):conn.execute(next((root/'backend/migrations').glob(f'{version:04d}_*.sql')).read_text())
        yield database
    finally:
        with psycopg.connect(source,autocommit=True) as conn:
            conn.execute(sql.SQL('DROP DATABASE {} WITH(FORCE)').format(sql.Identifier(name)))


@pytest_asyncio.fixture
async def archiver(archive_database,tmp_path):
    async with AsyncConnectionPool(archive_database,min_size=1,max_size=6,
            kwargs={'autocommit':True,'row_factory':dict_row},open=False) as pool:
        async with pool.connection() as conn:
            await conn.execute('TRUNCATE result_read_leases,result_archive_chunks,result_archive_files,result_archive_runs,tenant_result_usage,idempotency,attempts,tasks,batches,files,workers,tenant_queues,api_keys,tenants CASCADE')
            await conn.execute("INSERT INTO tenants(id,name) VALUES('a','A'),('b','B')")
            await conn.execute("INSERT INTO workers(id,name,model,concurrency) VALUES('worker','worker','model',1)")
        yield ResultArchiver(pool,LocalBlobStore(tmp_path/'objects'),max_archived_bytes_per_tenant=100000,chunk_bytes=128,page_rows=1)


async def one(archiver,statement,args=()):
    async with archiver.pool.connection() as conn:return await (await conn.execute(statement,args)).fetchone()


async def execute(archiver,statement,args=()):
    async with archiver.pool.connection() as conn:await conn.execute(statement,args)


async def seed(archiver,*,tenant='a',states=('completed','completed','failed'),length=50):
    bid='batch_'+secrets.token_hex(8);iid='input_'+bid;oid='output_'+bid;eid='error_'+bid
    completed=states.count('completed');failed=states.count('failed');cancelled=states.count('cancelled')
    async with archiver.pool.connection() as conn,conn.transaction():
        await conn.execute("INSERT INTO files(id,tenant_id,filename,kind) VALUES(%s,%s,'input','input')",(iid,tenant))
        for kind,fid,n in [('output',oid,completed),('error',eid,failed+cancelled)]:
            if n:await conn.execute('INSERT INTO files(id,tenant_id,filename,kind,batch_id) VALUES(%s,%s,%s,%s,%s)',(fid,tenant,kind,kind,bid))
        await conn.execute("INSERT INTO batches(id,tenant_id,input_file_id,model,status,total_count,completed_count,failed_count,cancelled_count,output_file_id,error_file_id,prompt_tokens,completion_tokens,runtime_compatibility_scope) VALUES(%s,%s,%s,'model',%s,%s,%s,%s,%s,%s,%s,%s,%s,'legacy_unverified')",(bid,tenant,iid,'completed' if completed else 'failed',len(states),completed,failed,cancelled,oid if completed else None,eid if failed+cancelled else None,completed*5,completed*3))
        for number,status in enumerate(states):
            tid=bid+f'-{number}';token='attempt-'+tid
            body={'model':'model','messages':[{'role':'user','content':'retained input '+str(number)}],'max_tokens':32}
            response={'choices':[{'message':{'content':'答'*length}}],'usage':{'prompt_tokens':5,'completion_tokens':3}} if status=='completed' else None
            error={'code':'failed','message':'diagnostic'} if status!='completed' else None
            await conn.execute('INSERT INTO tasks(id,tenant_id,batch_id,custom_id,model,body,status,max_attempts,attempts,attempt_token,worker_id,response,error,prompt_tokens,completion_tokens) VALUES(%s,%s,%s,%s,\'model\',%s,%s,3,1,%s,\'worker\',%s,%s,%s,%s)',(tid,tenant,bid,str(number),Jsonb(body),status,token,Jsonb(response) if response else None,Jsonb(error) if error else None,5 if response else 0,3 if response else 0))
            await conn.execute('INSERT INTO attempts(token,task_id,batch_id,tenant_id,worker_id,number,status,prompt_tokens,completion_tokens) VALUES(%s,%s,%s,%s,\'worker\',1,%s,%s,%s)',(token,tid,bid,tenant,status,5 if response else 0,3 if response else 0))
    return bid,oid if completed else None,eid if failed+cancelled else None


async def download(archiver,tenant,fid):
    async with archiver.open_file(tenant,fid) as chunks:return b''.join([part async for part in chunks])


async def test_archive_atomic_manifest_and_completed_payload_purge_preserve_contract(archiver):
    bid,oid,eid=await seed(archiver)
    expected={fid:await download(archiver,'a',fid) for fid in [oid,eid]}
    before=await one(archiver,'SELECT sum(prompt_tokens) AS p,sum(completion_tokens) AS c,count(*) AS n FROM attempts')
    result=await archiver.archive_batch('a',bid)
    assert result['purged'] and result['files']==2
    assert result['bytes']==sum(map(len,expected.values()))
    for fid,content in expected.items():assert await download(archiver,'a',fid)==content
    complete=await one(archiver,"SELECT bool_and(response IS NULL AND body='{}'::jsonb) AS ok FROM tasks WHERE status='completed'")
    assert complete['ok']
    assert (await one(archiver,"SELECT body<>'{}'::jsonb AS ok FROM tasks WHERE status='failed'"))['ok']
    assert await one(archiver,'SELECT sum(prompt_tokens) AS p,sum(completion_tokens) AS c,count(*) AS n FROM attempts')==before
    assert (await archiver.archive_batch('a',bid))['archive_id']==result['archive_id']
    usage=await one(archiver,'SELECT * FROM tenant_result_usage')
    assert usage['stored_bytes']==result['bytes'] and usage['reserved_bytes']==0
    assert (await one(archiver,'SELECT max(bytes) AS n FROM result_archive_chunks'))['n']<=128


async def test_active_db_reader_finishes_without_holes_across_publication(archiver):
    archiver.lease_seconds=1
    bid,oid,_=await seed(archiver,states=('completed',)*3)
    expected=await download(archiver,'a',oid)
    async with archiver.open_file('a',oid) as chunks:
        first=await anext(chunks)
        result=await archiver.archive_batch('a',bid)
        assert not result['purged']
        await asyncio.sleep(1.2)
        assert not await archiver.purge_batch('a',bid)
        assert await download(archiver,'a',oid)==expected
        assert first+b''.join([part async for part in chunks])==expected
    assert await archiver.purge_batch('a',bid)
    assert await download(archiver,'a',oid)==expected


async def test_expired_db_reader_is_fenced_instead_of_silently_skipping_purged_rows(archiver):
    bid,oid,_=await seed(archiver,states=('completed',)*3)
    async with archiver.open_file('a',oid) as chunks:
        await anext(chunks)
        await execute(archiver,"UPDATE result_read_leases SET expires_at=clock_timestamp()-interval '1 second' WHERE mode='db'")
        assert (await archiver.archive_batch('a',bid))['purged']
        with pytest.raises(BlobBusy,match='lease expired'):await anext(chunks)


async def test_quota_refuses_before_object_write_and_preserves_database(archiver):
    bid,_,_=await seed(archiver)
    archiver.max_bytes=10;writes=[]
    original=archiver.adapter.put_verified
    def observe(*args):writes.append(args);return original(*args)
    archiver.adapter.put_verified=observe
    with pytest.raises(ResultQuotaExceeded):await archiver.archive_batch('a',bid)
    assert not writes
    assert (await one(archiver,"SELECT count(*) AS n FROM tasks WHERE status='completed' AND response IS NOT NULL"))['n']==2
    usage=await one(archiver,'SELECT * FROM tenant_result_usage')
    assert usage['stored_bytes']==usage['reserved_bytes']==0


async def test_publication_rollback_leaves_registered_orphans_and_database_payloads(archiver):
    bid,_,_=await seed(archiver)
    await execute(archiver,"CREATE FUNCTION inject_archive_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.state='ready' THEN RAISE EXCEPTION 'injected publication failure'; END IF; RETURN NEW; END $$")
    await execute(archiver,'CREATE TRIGGER inject_archive_failure BEFORE UPDATE ON result_archive_runs FOR EACH ROW EXECUTE FUNCTION inject_archive_failure()')
    try:
        with pytest.raises(psycopg.errors.RaiseException):await archiver.archive_batch('a',bid)
    finally:
        await execute(archiver,'DROP TRIGGER inject_archive_failure ON result_archive_runs');await execute(archiver,'DROP FUNCTION inject_archive_failure()')
    assert (await one(archiver,"SELECT count(*) AS n FROM tasks WHERE status='completed' AND response IS NOT NULL"))['n']==2
    usage=await one(archiver,'SELECT * FROM tenant_result_usage');assert usage['stored_bytes']==usage['reserved_bytes']==0
    count=(await one(archiver,'SELECT count(*) AS n FROM result_archive_chunks'))['n']
    assert count>1 and (await archiver.collect_orphans(grace_seconds=0))['deleted_objects']==count


async def test_abandoned_writer_expiry_releases_reservation_and_retries_new_generation(archiver):
    bid,oid,_=await seed(archiver)
    run,token=await archiver._begin('a',bid)
    await archiver._chunk(run,token,oid,0,b'partial outbox')
    await execute(archiver,"UPDATE result_archive_runs SET lease_expires_at=clock_timestamp()-interval '1 second'")
    assert (await archiver.collect_orphans(grace_seconds=0))['expired_runs_examined']==1
    usage=await one(archiver,'SELECT * FROM tenant_result_usage');assert usage['stored_bytes']==usage['reserved_bytes']==0
    assert (await archiver.archive_batch('a',bid))['archive_id']!=run['id']


async def test_corrupt_purged_archive_fails_closed_without_database_fallback(archiver):
    bid,oid,_=await seed(archiver)
    assert (await archiver.archive_batch('a',bid))['purged']
    piece=await one(archiver,'SELECT * FROM result_archive_chunks WHERE file_id=%s ORDER BY ordinal LIMIT 1',(oid,))
    (archiver.adapter.root/piece['object_key']).write_bytes(b'corruption')
    with pytest.raises(BlobIntegrityError):await download(archiver,'a',oid)
    with pytest.raises(BlobIntegrityError,match='require object-store recovery'):await archiver.discard_unpurged('a',bid)


async def test_manifest_tamper_is_detected_before_stream_is_exposed(archiver):
    bid,oid,_=await seed(archiver)
    await archiver.archive_batch('a',bid)
    await execute(archiver,'UPDATE result_archive_files SET bytes=bytes+1 WHERE file_id=%s',(oid,))
    exposed=False
    with pytest.raises(BlobIntegrityError):
        async with archiver.open_file('a',oid):exposed=True
    assert not exposed


async def test_unpurged_corrupt_generation_can_be_retired_and_rebuilt(archiver):
    bid,oid,_=await seed(archiver)
    expected=await download(archiver,'a',oid)
    async with archiver.open_file('a',oid):
        first=await archiver.archive_batch('a',bid)
        assert not first['purged']
        piece=await one(archiver,'SELECT * FROM result_archive_chunks WHERE file_id=%s ORDER BY ordinal LIMIT 1',(oid,))
        (archiver.adapter.root/piece['object_key']).write_bytes(b'corruption')
        assert not await archiver.purge_batch('a',bid)
    with pytest.raises(BlobIntegrityError):await archiver.purge_batch('a',bid)
    assert await archiver.discard_unpurged('a',bid)
    second=await archiver.archive_batch('a',bid)
    assert second['archive_id']!=first['archive_id'] and second['purged']
    await archiver.collect_orphans(grace_seconds=0)
    assert await download(archiver,'a',oid)==expected


async def test_tenant_isolation_in_reader_and_manifest_foreign_keys(archiver):
    bid,oid,_=await seed(archiver)
    result=await archiver.archive_batch('a',bid)
    with pytest.raises(BlobNotFound):await download(archiver,'b',oid)
    with pytest.raises(BlobNotFound):await archiver.archive_batch('b',bid)
    await execute(archiver,"INSERT INTO files(id,tenant_id,filename,kind) VALUES('foreign-file','b','foreign','output')")
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        await execute(archiver,"INSERT INTO result_archive_files(tenant_id,archive_id,file_id,kind,expected_count) VALUES('b',%s,'foreign-file','output',1)",(result['archive_id'],))


async def test_purge_is_bounded_keyset_progress_and_partial_archive_stays_complete(archiver):
    archiver.purge_rows=2
    bid,oid,_=await seed(archiver,states=('completed',)*5+('failed',))
    expected=await download(archiver,'a',oid)
    first=await archiver.archive_batch('a',bid)
    assert not first['purged'] and first['rows_purged']==2
    assert await download(archiver,'a',oid)==expected
    progress=await one(archiver,'SELECT * FROM result_archive_runs WHERE id=%s',(first['archive_id'],))
    assert progress['purge_started_at'] and progress['purged_at'] is None
    with pytest.raises(BlobIntegrityError):await archiver.discard_unpurged('a',bid)
    def reject_redundant_get(*args):raise AssertionError('Purge progress must not repeatedly GET the whole archive')
    archiver._verify_files=reject_redundant_get
    second=await archiver.archive_batch('a',bid)
    assert not second['purged'] and second['rows_purged']==4
    final=await archiver.archive_batch('a',bid)
    assert final['purged'] and final['rows_purged']==5
    assert (await one(archiver,"SELECT body<>'{}'::jsonb AS ok FROM tasks WHERE status='failed'"))['ok']
    assert await download(archiver,'a',oid)==expected


async def test_object_write_failure_after_payload_persists_is_reapable(archiver):
    bid,oid,_=await seed(archiver)
    expected=await download(archiver,'a',oid)
    original=archiver.adapter.put_verified
    def fail_after_write(*args):original(*args);raise BlobStorageError('injected object service outage')
    archiver.adapter.put_verified=fail_after_write
    with pytest.raises(BlobStorageError):await archiver.archive_batch('a',bid)
    assert await download(archiver,'a',oid)==expected
    assert (await archiver.collect_orphans(grace_seconds=0))['deleted_objects']==1
    usage=await one(archiver,'SELECT * FROM tenant_result_usage');assert usage['reserved_bytes']==usage['stored_bytes']==0


async def test_retired_archive_reader_pins_generation_until_download_finishes(archiver):
    bid,oid,_=await seed(archiver)
    expected=await download(archiver,'a',oid)
    async with archiver.open_file('a',oid):
        first=await archiver.archive_batch('a',bid)
        async with archiver.open_file('a',oid) as archive_reader:
            assert await archiver.discard_unpurged('a',bid)
            assert (await archiver.collect_orphans(grace_seconds=0))['deleted_objects']==0
            assert b''.join([piece async for piece in archive_reader])==expected
        assert (await archiver.collect_orphans(grace_seconds=0))['deleted_objects']>0
        second=await archiver.archive_batch('a',bid)
        assert second['archive_id']!=first['archive_id']
    assert await archiver.purge_batch('a',bid)
    assert await download(archiver,'a',oid)==expected


async def test_runtime_role_can_archive_without_ddl_or_privilege_escalation(archiver,archive_database):
    bid,oid,_=await seed(archiver)
    expected=await download(archiver,'a',oid)
    role='bf_result_runtime_'+secrets.token_hex(8)
    async with archiver.pool.connection() as conn:
        await conn.execute(sql.SQL('CREATE ROLE {} LOGIN').format(sql.Identifier(role)))
        await conn.execute(sql.SQL('GRANT USAGE ON SCHEMA public TO {}').format(sql.Identifier(role)))
        await conn.execute(sql.SQL('GRANT SELECT ON tenants TO {}').format(sql.Identifier(role)))
        await conn.execute(sql.SQL('GRANT UPDATE(name) ON tenants TO {}').format(sql.Identifier(role)))
        await conn.execute(sql.SQL('GRANT SELECT,UPDATE ON batches,files,tasks TO {}').format(sql.Identifier(role)))
        await conn.execute(sql.SQL('GRANT SELECT,INSERT,UPDATE ON tenant_result_usage,result_archive_runs,result_archive_files,result_archive_chunks TO {}').format(sql.Identifier(role)))
        await conn.execute(sql.SQL('GRANT SELECT,INSERT,UPDATE,DELETE ON result_read_leases TO {}').format(sql.Identifier(role)))
    try:
        async with AsyncConnectionPool(make_conninfo(archive_database,user=role),min_size=1,max_size=3,
                kwargs={'autocommit':True,'row_factory':dict_row},open=False) as pool:
            runtime=ResultArchiver(pool,archiver.adapter,max_archived_bytes_per_tenant=100000,chunk_bytes=128)
            async with pool.connection() as conn:
                with pytest.raises(psycopg.errors.InsufficientPrivilege):await conn.execute('CREATE TABLE forbidden_runtime_ddl(id int)')
                with pytest.raises(psycopg.errors.InsufficientPrivilege):await conn.execute("INSERT INTO api_keys(key_hash,tenant_id) VALUES('forbidden-key','a')")
                actual=await (await conn.execute('SELECT rolsuper,rolcreatedb,rolcreaterole FROM pg_roles WHERE rolname=current_user')).fetchone()
                assert not any(actual.values())
            assert (await runtime.archive_batch('a',bid))['purged']
            assert await download(runtime,'a',oid)==expected
    finally:
        async with archiver.pool.connection() as conn:
            await conn.execute(sql.SQL('DROP OWNED BY {}').format(sql.Identifier(role)))
            await conn.execute(sql.SQL('DROP ROLE {}').format(sql.Identifier(role)))


async def test_concurrent_archives_cannot_overshoot_tenant_result_quota(archiver):
    first,oid,_=await seed(archiver,states=('completed',))
    second,_,_=await seed(archiver,states=('completed',))
    archiver.max_bytes=len(await download(archiver,'a',oid))
    results=await asyncio.gather(archiver.archive_batch('a',first),archiver.archive_batch('a',second),return_exceptions=True)
    accepted=[result for result in results if isinstance(result,dict)]
    assert len(accepted)<=1
    assert all(isinstance(result,(dict,ResultQuotaExceeded)) for result in results)
    usage=await one(archiver,'SELECT * FROM tenant_result_usage')
    assert 0<=usage['stored_bytes']<=archiver.max_bytes and usage['reserved_bytes']==0
    assert (await one(archiver,"SELECT count(*) AS n FROM tasks WHERE status='completed' AND response IS NOT NULL"))['n']==2-len(accepted)


async def test_crash_after_manifest_commit_retries_without_double_charge(archiver):
    bid,oid,_=await seed(archiver)
    expected=await download(archiver,'a',oid);original=archiver.purge_batch
    async def crash_before_purge(*args):raise BlobStorageError('injected process loss after publication')
    archiver.purge_batch=crash_before_purge
    with pytest.raises(BlobStorageError):await archiver.archive_batch('a',bid)
    ready=await one(archiver,"SELECT * FROM result_archive_runs WHERE state='ready'")
    assert ready and ready['purge_started_at'] is None
    assert await download(archiver,'a',oid)==expected
    archiver.purge_batch=original
    retried=await archiver.archive_batch('a',bid)
    assert retried['archive_id']==ready['id'] and retried['purged']
    usage=await one(archiver,'SELECT * FROM tenant_result_usage')
    assert usage['stored_bytes']==ready['bytes'] and usage['reserved_bytes']==0


async def test_cancelled_archive_late_sdk_write_remains_known_and_reapable(archiver):
    bid,oid,_=await seed(archiver)
    expected=await download(archiver,'a',oid)
    entered,release,finished=threading.Event(),threading.Event(),threading.Event()
    original=archiver.adapter.put_verified
    def late_write(*args):
        entered.set()
        try:
            assert release.wait(10)
            original(*args)
        finally:finished.set()
    archiver.adapter.put_verified=late_write
    task=asyncio.create_task(archiver.archive_batch('a',bid))
    assert await asyncio.to_thread(entered.wait,5)
    try:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):await task
        assert (await archiver.collect_orphans(grace_seconds=0))['deleted_objects']==1
    finally:release.set()
    assert await asyncio.to_thread(finished.wait,5)
    piece=await one(archiver,'SELECT * FROM result_archive_chunks')
    assert piece['state']=='deleted' and (archiver.adapter.root/piece['object_key']).exists()
    await execute(archiver,"UPDATE result_archive_chunks SET gc_after=clock_timestamp()-interval '1 second'")
    assert (await archiver.collect_orphans(grace_seconds=0))['deleted_objects']==1
    assert not(archiver.adapter.root/piece['object_key']).exists()
    assert await download(archiver,'a',oid)==expected
    usage=await one(archiver,'SELECT * FROM tenant_result_usage')
    assert usage['reserved_bytes']==usage['stored_bytes']==0


async def test_concatenated_file_identity_is_verified_before_first_purge(archiver):
    bid,oid,_=await seed(archiver)
    expected=await download(archiver,'a',oid);original=archiver._chunk
    async def faulty_assembler(run,token,fid,ordinal,content):
        # Individually self-consistent chunks can still disagree with the full
        # file manifest, e.g. a producer bug before object hashing.
        if fid==oid and ordinal==0:content=b'X'+content[1:]
        return await original(run,token,fid,ordinal,content)
    archiver._chunk=faulty_assembler
    with pytest.raises(BlobIntegrityError,match='file checksum'):await archiver.archive_batch('a',bid)
    ready=await one(archiver,"SELECT * FROM result_archive_runs WHERE state='ready'")
    assert ready and ready['purge_started_at'] is None
    assert (await one(archiver,"SELECT count(*) AS n FROM tasks WHERE status='completed' AND response IS NOT NULL"))['n']==2
    assert await archiver.discard_unpurged('a',bid)
    archiver._chunk=original
    assert (await archiver.archive_batch('a',bid))['purged']
    assert await download(archiver,'a',oid)==expected
