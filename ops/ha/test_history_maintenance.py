"""Finite maintenance controller tests, including disposable real PostgreSQL."""
import argparse
import asyncio
from datetime import datetime,timedelta,timezone
import json
import os
from pathlib import Path
import secrets
import stat

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool
import pytest
import pytest_asyncio

from backend.blob_storage import LocalBlobStore
from backend.history_retirement import configure_policy
from backend.migrate import migrate_database
from backend.result_archival import ResultArchiver
from ops.ha import history_maintenance as hm

SOURCE=os.getenv('TEST_DATABASE_URL')


def arguments(tmp_path,**overrides):
    now=datetime.now(timezone.utc)
    values=dict(tenant='default',created_after=now-timedelta(seconds=600),min_age=60,
        max_cycles=2,execute=False,retire_keys=False,batch_limit=2,task_page_limit=2,
        key_page_limit=2,interval=10,archive_byte_limit=10000000,
        env_file=tmp_path/'owner.env',operation_dir=tmp_path/'operation',
        local_root=tmp_path/'objects',s3=False,bucket=None)
    values.update(overrides);return argparse.Namespace(**values)


@pytest.mark.parametrize('field,value',[('batch_limit',9),('task_page_limit',1001),('key_page_limit',0),
    ('interval',9),('max_cycles',0),('max_cycles',True),('min_age',59),('created_after',datetime.now())])
def test_bounds_rejected_before_database(tmp_path,field,value):
    args=arguments(tmp_path,**{field:value})
    with pytest.raises(ValueError):hm.validate_args(args)


def test_private_files_directory_flock_and_atomic_report(tmp_path):
    journal=hm.Journal(tmp_path/'private')
    with hm.operation_lock(journal.directory):
        with pytest.raises(BlockingIOError):
            with hm.operation_lock(journal.directory):pass
    journal.append('test',count=1);journal.save({'phase':'preview','secret':False})
    assert journal.load()=={'phase':'preview','secret':False}
    assert stat.S_IMODE(journal.directory.stat().st_mode)==0o700
    for name in ('maintenance.lock','journal.jsonl','report.json'):
        assert stat.S_IMODE((journal.directory/name).stat().st_mode)==0o600
    unsafe=tmp_path/'unsafe.env';unsafe.write_text('DATABASE_URL=secret');unsafe.chmod(0o644)
    with pytest.raises(ValueError):hm.private_file(unsafe)
    link=tmp_path/'link.env';link.symlink_to(unsafe)
    with pytest.raises(OSError):hm.private_file(link)
    assert not list(journal.directory.glob('*.partial'))


def test_scope_cannot_widen_or_cleanup_keys_outside_stricter_cli_cohort(tmp_path):
    args=arguments(tmp_path)
    policy=dict(enabled=True,eligible_created_after=args.created_after,min_age_seconds=60)
    hm.policy_scope(args,policy)
    args.created_after+=timedelta(seconds=1)
    hm.policy_scope(args,policy)
    args.retire_keys=True
    with pytest.raises(ValueError):hm.policy_scope(args,policy)
    args.retire_keys=False;args.min_age=59
    with pytest.raises(ValueError):hm.policy_scope(args,policy)


@pytest.fixture
def database():
    if not SOURCE:pytest.skip('Requires isolated real PostgreSQL')
    name='bf_maintenance_'+secrets.token_hex(6)
    with psycopg.connect(SOURCE,autocommit=True) as conn:
        conn.execute(sql.SQL('CREATE DATABASE {} TEMPLATE template0').format(sql.Identifier(name)))
    dsn=make_conninfo(SOURCE,dbname=name)
    try:
        migrate_database(dsn,'maintenance-test-api-key-123456',target_version=11)
        yield dsn
    finally:
        with psycopg.connect(SOURCE,autocommit=True) as conn:
            conn.execute(sql.SQL('DROP DATABASE {} WITH(FORCE)').format(sql.Identifier(name)))


@pytest_asyncio.fixture
async def context(database,tmp_path):
    async with AsyncConnectionPool(database,min_size=1,max_size=8,
        kwargs={'autocommit':True,'row_factory':dict_row},open=False) as pool:
        args=arguments(tmp_path)
        adapter=LocalBlobStore(tmp_path/'objects')
        await configure_policy(pool,'default',enabled=True,created_after=args.created_after,
                               min_age_seconds=60,replay_ttl_seconds=60)
        async with pool.connection() as conn:
            await conn.execute("SELECT batchforge_auxiliary_lock('default')")
        yield dict(pool=pool,args=args,adapter=adapter,database=database,tmp=tmp_path)


async def row(c,query,args=()):
    async with c['pool'].connection() as conn:return await (await conn.execute(query,args)).fetchone()


async def terminal(c,count=3,age=120):
    tag=secrets.token_hex(6);bid,fid,oid,wid=('batch_'+tag,'input_'+tag,'output_'+tag,'worker_'+tag)
    async with c['pool'].connection() as conn,conn.transaction():
        await conn.execute("INSERT INTO workers(id,name,model,concurrency) VALUES(%s,'test','model',8)",(wid,))
        await conn.execute("INSERT INTO files(id,tenant_id,filename,kind) VALUES(%s,'default','test','input')",(fid,))
        await conn.execute("""INSERT INTO batches(id,tenant_id,input_file_id,model,status,total_count,completed_count,
          prompt_tokens,completion_tokens,created_at,completed_at) VALUES(%s,'default',%s,'model','completed',%s,%s,%s,%s,
          clock_timestamp()-make_interval(secs=>%s),clock_timestamp()-interval '90 seconds')""",(bid,fid,count,count,count*5,count*3,age))
        await conn.execute("INSERT INTO files(id,tenant_id,filename,kind,batch_id) VALUES(%s,'default','output','output',%s)",(oid,bid))
        await conn.execute('UPDATE batches SET output_file_id=%s WHERE id=%s',(oid,bid))
        for n in range(count):
            tid='task_'+tag+str(n)
            await conn.execute("""INSERT INTO tasks(id,tenant_id,batch_id,custom_id,model,body,status,max_attempts,attempts,
              response,prompt_tokens,completion_tokens,finished_at) VALUES(%s,'default',%s,%s,'model','{}','completed',3,1,%s,5,3,clock_timestamp())""",
              (tid,bid,str(n),Jsonb({'choices':[{'message':{'content':'public test answer'}}]})))
            await conn.execute("""INSERT INTO attempts(token,task_id,batch_id,tenant_id,worker_id,number,status,
              prompt_tokens,completion_tokens,finished_at) VALUES(%s,%s,%s,'default',%s,1,'completed',5,3,clock_timestamp())""",
              ('attempt_'+tag+str(n),tid,bid,wid))
    archive=ResultArchiver(c['pool'],c['adapter'],max_archived_bytes_per_tenant=10000000)
    assert (await archive.archive_batch('default',bid))['purged']
    return bid,oid


def controller(c,**overrides):
    args=argparse.Namespace(**vars(c['args']));args.__dict__.update(overrides)
    journal=hm.Journal(c['tmp']/('operation_'+secrets.token_hex(4)))
    return hm.Maintainer(c['pool'],args,journal,c['adapter'])


async def test_preview_and_execution_are_bounded_and_preserve_accounting(context):
    c=context;bid,_=await terminal(c,count=5)
    preview=controller(c,max_cycles=1)
    before=await hm.accounting(c['pool'],'default')
    result=await preview.run()
    assert result['phase']=='complete' and result['cycles_started']==1
    assert result['totals']['retired_tasks']==0 and result['last_cycle']['eligible_tasks']==2
    assert result['totals']['archive_attempted_bytes']==0
    assert await hm.accounting(c['pool'],'default')==before
    worker=controller(c,execute=True,max_cycles=1,task_page_limit=2)
    result=await worker.run()
    assert result['totals']['retired_tasks']==2 and result['totals']['retired_attempts']==2
    assert result['totals']['archive_verified_bytes']>0
    assert result['last_cycle']['after']['retained_tasks']==3
    assert (await row(c,'SELECT history_retired_tasks AS n FROM batches WHERE id=%s',(bid,)))['n']==2
    resumed=hm.Maintainer(c['pool'],worker.args,worker.journal,c['adapter'])
    assert (await resumed.run())['cycles_started']==1
    assert (await hm.accounting(c['pool'],'default'))['retained_tasks']==3


async def test_keyset_rotation_does_not_let_pinned_reader_starve_another_batch(context):
    c=context;first,oid=await terminal(c,age=180);second,_=await terminal(c,age=120)
    async with c['pool'].connection() as conn:
        await conn.execute("""INSERT INTO result_read_leases(id,tenant_id,batch_id,file_id,mode,archive_id,expires_at)
         SELECT 'pinned','default',%s,%s,'archive',id,clock_timestamp()+interval '5 minutes'
         FROM result_archive_runs WHERE batch_id=%s""",(first,oid,first))
    worker=controller(c,execute=True,batch_limit=1)
    one=await worker.cycle();two=await worker.cycle()
    assert one['skip_counts']=={'active_reader':1}
    assert two['retired_tasks']==2 and two['pages'][0]['batch_id']==second
    assert (await row(c,'SELECT history_retired_tasks AS n FROM batches WHERE id=%s',(first,)))['n']==0


async def test_archive_byte_budget_and_stop_flag_prevent_more_work(context,monkeypatch):
    c=context;await terminal(c,count=3);await terminal(c,count=3)
    budget=controller(c,execute=True,archive_byte_limit=1)
    result=await budget.run() if budget.args.max_cycles==1 else await budget.cycle()
    assert result['skip_counts']=={'archive_byte_limit':1}
    assert (await hm.accounting(c['pool'],'default'))['retained_tasks']==6
    worker=controller(c,execute=True,max_cycles=3,batch_limit=2,task_page_limit=1)
    real=hm.history_page
    async def stop_after_page(*args,**kwargs):
        result=await real(*args,**kwargs);worker.stop.set();return result
    monkeypatch.setattr(hm,'history_page',stop_after_page)
    result=await worker.run()
    assert result['phase']=='stopped' and result['cycles_started']==1
    assert result['totals']['retired_tasks']==1
    assert len(result['last_cycle']['pages'])==1


async def test_accounting_error_stops_without_repair_or_retirement(context):
    c=context;bid,_=await terminal(c)
    async with c['pool'].connection() as conn:
        await conn.execute('UPDATE batches SET prompt_tokens=prompt_tokens+1 WHERE id=%s',(bid,))
    worker=controller(c,execute=True,max_cycles=1)
    with pytest.raises(hm.AccountingError):await worker.run()
    assert worker.journal.load()['phase']=='failed'
    assert (await row(c,'SELECT count(*) AS n FROM tasks'))['n']==3
    assert (await row(c,'SELECT prompt_tokens AS n FROM batches WHERE id=%s',(bid,)))['n']==16


async def test_unresolved_keys_do_not_move_floor_and_resolved_compaction_is_bounded(context):
    c=context;bid,_=await terminal(c)
    async with c['pool'].connection() as conn:
        now=(await (await conn.execute("SELECT floor(extract(epoch FROM clock_timestamp()))::bigint AS n")).fetchone())['n']
        for n in range(3):
            key='bf1.'+str(now-120)+'.'+secrets.token_hex(8)
            await conn.execute("INSERT INTO idempotency(tenant_id,scope,key,body_hash,response) VALUES('default','create',%s,'hash',%s)",
                (key,Jsonb({'id':bid}) if n else None))
        await conn.execute("INSERT INTO idempotency(tenant_id,scope,key,body_hash,response) VALUES('default','create','legacy','hash',%s)",(Jsonb({'id':bid}),))
    worker=controller(c,execute=True,retire_keys=True,max_cycles=1)
    result=await worker.run()
    assert result['last_cycle']['key_page']['reason']=='unresolved_or_active_key'
    assert (await row(c,"SELECT replay_floor AS n FROM tenant_history_retirement_policy WHERE tenant_id='default'"))['n']==0
    async with c['pool'].connection() as conn:
        await conn.execute("UPDATE idempotency SET response=%s WHERE response IS NULL",(Jsonb({'id':bid}),))
    worker=controller(c,execute=True,retire_keys=True,max_cycles=1)
    result=await worker.run()
    assert result['totals']['retired_keys']==2
    assert (await row(c,'SELECT count(*) AS n FROM idempotency'))['n']==2
    assert (await row(c,"SELECT count(*) AS n FROM idempotency WHERE key='legacy'"))['n']==1
    assert (await hm.accounting(c['pool'],'default'))['auxiliary_mismatches']==0


async def test_uncertain_page_journal_blocks_automatic_resume(context,monkeypatch):
    c=context;await terminal(c)
    worker=controller(c,execute=True,max_cycles=1)
    async def unknown_commit(*args,**kwargs):raise psycopg.OperationalError('simulated lost connection')
    monkeypatch.setattr(hm,'history_page',unknown_commit)
    with pytest.raises(psycopg.OperationalError):await worker.run()
    report=worker.journal.load()
    assert report['phase']=='failed' and report['pending_action']['kind']=='history_page'
    with pytest.raises(RuntimeError,match='lacks confirmation'):
        hm.Maintainer(c['pool'],worker.args,worker.journal,c['adapter'])


async def test_cross_operation_database_advisory_lock_refuses_second_operator(context):
    c=context;args=arguments(c['tmp'],max_cycles=1)
    journal=hm.Journal(c['tmp']/'advisory-operation')
    async with c['pool'].connection() as conn:
        await conn.execute("SELECT pg_advisory_lock(hashtextextended('batchforge-history-maintenance:'||%s,0))",('default',))
        try:
            with pytest.raises(RuntimeError,match='Another tenant'):
                await hm.execute(args,journal,{'DATABASE_URL':c['database']})
        finally:
            await conn.execute("SELECT pg_advisory_unlock(hashtextextended('batchforge-history-maintenance:'||%s,0))",('default',))


def test_real_cli_preview_private_env_and_finite_report(database,tmp_path):
    import subprocess
    import sys
    cutoff=datetime.now(timezone.utc)-timedelta(seconds=600)
    async def configure():
        async with AsyncConnectionPool(database,min_size=1,max_size=2,
            kwargs={'autocommit':True,'row_factory':dict_row},open=False) as pool:
            await configure_policy(pool,'default',enabled=True,created_after=cutoff,min_age_seconds=60,replay_ttl_seconds=60)
    asyncio.run(configure())
    envfile=tmp_path/'owner.env';envfile.write_text('DATABASE_URL='+database+'\n');envfile.chmod(0o600)
    operation=tmp_path/'cli-operation'
    result=subprocess.run([sys.executable,'-m','ops.ha.history_maintenance','--env-file',str(envfile),
        '--operation-dir',str(operation),'--tenant','default','--created-after',cutoff.isoformat(),
        '--min-age','60','--max-cycles','1'],text=True,capture_output=True,timeout=20)
    assert result.returncode==0,result.stdout+result.stderr
    assert database not in result.stdout+result.stderr
    output=json.loads(result.stdout)
    assert output['phase']=='complete' and output['totals']['retired_tasks']==0
    report=json.loads((operation/'report.json').read_text())
    assert report['cycles_started']==report['cycles_completed']==1
    assert report['binding']['execute'] is False
    assert stat.S_IMODE(operation.stat().st_mode)==0o700
    assert stat.S_IMODE((operation/'report.json').stat().st_mode)==0o600


async def test_source_change_stops_existing_cycle_before_any_page(tmp_path,monkeypatch):
    args=arguments(tmp_path,max_cycles=1)
    journal=hm.Journal(tmp_path/'source-bound')
    worker=hm.Maintainer(None,args,journal)
    journal.save(worker.report)
    original=hm.source_hashes();changed=dict(original)
    changed['backend/history_retirement.py']='0'*64
    calls=[]
    async def forbidden_page(*a,**k):calls.append(True);raise AssertionError('Unreviewed page')
    monkeypatch.setattr(hm,'history_page',forbidden_page)
    monkeypatch.setattr(hm,'source_hashes',lambda:changed)
    with pytest.raises(ValueError,match='source changed'):await worker.run()
    with pytest.raises(ValueError,match='binding differs'):hm.Maintainer(None,args,journal)
    assert not calls
    assert journal.load()['cycles_started']==0


async def test_confirmed_operation_refuses_changed_archive_identity_resume(context,monkeypatch):
    c=context;await terminal(c,count=3)
    worker=controller(c,execute=True,max_cycles=2)
    await worker.cycle()
    assert worker.report['totals']['retired_tasks']==2
    before=await hm.accounting(c['pool'],'default')
    calls=[]
    async def forbidden_page(*a,**k):calls.append(True);raise AssertionError('Wrong archive page')
    monkeypatch.setattr(hm,'history_page',forbidden_page)
    alternate=LocalBlobStore(c['tmp']/'other-archive-root')
    with pytest.raises(ValueError,match='binding differs'):
        hm.Maintainer(c['pool'],worker.args,worker.journal,alternate)
    # Also refuse an in-memory verifier destination change before the next
    # cycle, independently of any restart/resume path.
    worker.adapter=alternate
    with pytest.raises(ValueError,match='archive identity changed'):await worker.run()
    assert not calls and await hm.accounting(c['pool'],'default')==before


def test_archive_identity_is_canonical_and_never_accepts_credential_fields():
    original={'backend':'s3','endpoint':'https://EXAMPLE.com:443/','region':'eu-central-1',
              'bucket':'existing-bucket','prefix':'/batchforge/'}
    canonical=hm.canonical_archive_identity(original)
    assert canonical=={'backend':'s3','endpoint':'https://example.com','region':'eu-central-1',
                       'bucket':'existing-bucket','prefix':'batchforge'}
    assert hm.identity_hash(canonical)==hm.identity_hash(hm.canonical_archive_identity(canonical))
    for change in ({'endpoint':'https://user:password@example.com'}, {'access_key_id':'never-bound'}):
        value={**original,**change}
        with pytest.raises(ValueError):hm.canonical_archive_identity(value)
