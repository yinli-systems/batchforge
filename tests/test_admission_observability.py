"""Admission timing behavior against canonical backend imports.

Ten pure context cases always run. Seven PostgreSQL cases require an explicit
TEST_DATABASE_URL for an ephemeral CI PostgreSQL with CREATE DATABASE privilege.
Each case creates/migrates/drops only its own random database. No SQLite/fallback,
private .runtime helper, GPU, HTTP or external object service is used.
"""
from contextlib import asynccontextmanager
import asyncio,json,os,secrets,time
from types import SimpleNamespace
import pytest
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from prometheus_client import CollectorRegistry,Histogram,generate_latest
from backend.config import Settings

from backend import admission_metrics as M
from backend import store as S
from backend.migrate import migrate_database
from psycopg.conninfo import make_conninfo

SOURCE=os.environ.get('TEST_DATABASE_URL')
MODEL='Qwen/Qwen3-235B-A22B-Instruct-2507-FP8'

@pytest.fixture
def metrics(monkeypatch):
 registry=CollectorRegistry();hist=Histogram('batchforge_admission_stage_seconds','Synthetic test duration',('stage','outcome'),buckets=M.BUCKETS,registry=registry)
 monkeypatch.setattr(M,'ADMISSION_STAGE_SECONDS',hist)
 def value(stage,outcome='ok',suffix='count'):
  result=registry.get_sample_value('batchforge_admission_stage_seconds_'+suffix,{'stage':stage,'outcome':outcome});return result or 0
 return SimpleNamespace(registry=registry,value=value,text=lambda:generate_latest(registry).decode())

@pytest.mark.parametrize('error',(ValueError('SYNTHETIC_PRIVATE_ERROR'),asyncio.CancelledError('SYNTHETIC_PRIVATE_CANCEL')))
def test_error_and_cancellation_propagate_exact_object_without_secret_labels(metrics,error):
 with pytest.raises(type(error)) as seen:
  with M.admission_stage('total'):raise error
 assert seen.value is error
 outcome='cancelled' if isinstance(error,asyncio.CancelledError) else 'error'
 assert metrics.value('total',outcome)==1 and 'PRIVATE' not in metrics.text()
 with pytest.raises(ValueError,match='Unknown admission metric stage'):
  with M.admission_stage('TENANT_SECRET_OR_ARBITRARY_LABEL'):pass
 assert 'TENANT_SECRET' not in metrics.text()

class Context:
 def __init__(self,enter_error=None,exit_error=None,suppress=False):self.enter_error=enter_error;self.exit_error=exit_error;self.suppress=suppress;self.events=[]
 async def __aenter__(self):
  self.events.append('enter');await asyncio.sleep(.005)
  if self.enter_error:raise self.enter_error
  return 'VALUE'
 async def __aexit__(self,*exc):
  self.events.append(('exit',exc[0]));await asyncio.sleep(.005)
  if self.exit_error:raise self.exit_error
  return self.suppress

def test_observed_context_preserves_success_suppression_and_enter_failure(metrics):
 async def run():
  context=Context()
  async with M._observed_context(context,'pool_acquire','pool_release') as value:assert value=='VALUE'
  assert context.events==['enter',('exit',None)]
  suppressed=Context(suppress=True)
  async with M._observed_context(suppressed,'pool_acquire','pool_release'):raise ValueError('PRIVATE')
  assert suppressed.events==['enter',('exit',ValueError)]
  failed=Context(enter_error=LookupError('PRIVATE'))
  with pytest.raises(LookupError):
   async with M._observed_context(failed,'pool_acquire','pool_release'):pytest.fail('entered failed context')
  assert failed.events==['enter']
 asyncio.run(run())
 assert metrics.value('pool_acquire')==2 and metrics.value('pool_acquire','error')==1
 assert metrics.value('pool_release')==2 and metrics.value('pool_acquire',suffix='sum')>=.01
 assert 'PRIVATE' not in metrics.text()

@pytest.mark.parametrize('body_error',(None,ValueError('PRIVATE'),asyncio.CancelledError('PRIVATE')))
def test_transaction_commit_rollback_and_cancel_cleanup_are_not_retried(metrics,body_error):
 async def run():
  context=Context();connection=SimpleNamespace(transaction=lambda:context)
  try:
   async with M.observed_transaction(connection):
    if body_error:raise body_error
  except BaseException as error:assert error is body_error
  assert len(context.events)==2 and context.events[-1][1] is (type(body_error) if body_error else None)
 asyncio.run(run())
 stage='transaction_rollback' if body_error else 'transaction_commit'
 outcome='cancelled' if isinstance(body_error,asyncio.CancelledError) else 'error' if body_error else 'ok'
 assert metrics.value(stage)==1 and metrics.value('transaction',outcome)==1
 assert metrics.value('transaction_begin')==1

def test_commit_error_is_preserved_and_not_described_as_acknowledged_success(metrics):
 error=OSError('PRIVATE_UNKNOWN_COMMIT')
 async def run():
  context=Context(exit_error=error)
  with pytest.raises(OSError) as seen:
   async with M.observed_transaction(SimpleNamespace(transaction=lambda:context)):pass
  assert seen.value is error and len(context.events)==2
 asyncio.run(run())
 assert metrics.value('transaction_commit','error')==1 and metrics.value('transaction','error')==1
 assert metrics.value('transaction_commit')==0 and 'PRIVATE' not in metrics.text()

def test_overlapping_concurrent_stages_keep_independent_monotonic_timers(metrics):
 async def run():
  entered=asyncio.Event();release=asyncio.Event()
  async def one():
   with M.admission_stage('queue_lock'):entered.set();await release.wait()
  first=asyncio.create_task(one());await entered.wait();second=asyncio.create_task(one())
  await asyncio.sleep(.03);release.set();await asyncio.gather(first,second)
 asyncio.run(run())
 assert metrics.value('queue_lock')==2 and metrics.value('queue_lock',suffix='sum')>=.05
 assert set(M.OUTCOMES)=={'ok','error','cancelled'} and len(M.STAGES)==23

class Database:
 def __init__(self,dsn,owner_conn):self.dsn=dsn;self.owner_conn=owner_conn
 def __repr__(self):return '<isolated admission test database>'

@pytest.fixture
def database():
 if not SOURCE:pytest.skip('Set TEST_DATABASE_URL to an ephemeral PostgreSQL with CREATE DATABASE privilege')
 name='bf_admission_'+secrets.token_hex(8)
 with psycopg.connect(SOURCE,autocommit=True,connect_timeout=5) as admin:
  admin.execute(psycopg.sql.SQL('CREATE DATABASE {} TEMPLATE template0').format(psycopg.sql.Identifier(name)))
 dsn=make_conninfo(SOURCE,dbname=name);owner=None
 try:
  migrate_database(dsn,'synthetic-admission-bootstrap-key-not-real')
  owner=psycopg.connect(dsn,autocommit=True,row_factory=dict_row,connect_timeout=5,
                       options='-c statement_timeout=5000')
  yield Database(dsn,owner)
 finally:
  if owner is not None:owner.close()
  with psycopg.connect(SOURCE,autocommit=True,connect_timeout=5) as admin:
   admin.execute(psycopg.sql.SQL('DROP DATABASE {} WITH(FORCE)').format(psycopg.sql.Identifier(name)))

class SyntheticBlob:
 """Only a local in-memory fake of object read; no S3 implementation executed."""
 def __init__(self,raw):self.raw=raw;self.reads=0;self.error=None;self.barrier=None
 async def read_input(self,*args):
  self.reads+=1
  if self.barrier:
   self.barrier[0]+=1
   if self.barrier[0]>=2:self.barrier[1].set()
   await self.barrier[1].wait()
  if self.error:raise self.error
  return self.raw
 async def admit_batch(self,*args):pass

def payload(n=2):
 return b''.join((json.dumps({'custom_id':'SYNTHETIC-'+str(i),'method':'POST','url':'/v1/chat/completions','body':{'model':MODEL,'messages':[{'role':'user','content':'SYNTHETIC_NEVER_EXPORTED'}],'max_tokens':32}})+'\n').encode() for i in range(n))

@asynccontextmanager
async def store(database,blob=True):
 dsn=database.dsn
 async with AsyncConnectionPool(dsn,min_size=1,max_size=4,kwargs={'autocommit':True,'row_factory':dict_row,'options':'-c statement_timeout=5000'},open=False) as pool:
  await pool.wait();settings=Settings(database_url=dsn,model_name=MODEL,blob_storage_backend='inline',scheduler_policy='fair',retention_admission_enabled=True)
  value=S.Store(pool,settings,schema_version=11);file=await value.upload('default','synthetic.jsonl',payload())
  if blob:value.blobs=SyntheticBlob(payload())
  yield value,{'input_file_id':file['id'],'endpoint':'/v1/chat/completions','completion_window':'24h','metadata':{}}

def stats(database):
 row=database.owner_conn.execute("SELECT (SELECT count(*) FROM batches) AS batches,(SELECT count(*) FROM tasks) AS tasks,(SELECT count(*) FROM idempotency) AS keys,(SELECT retained_tasks FROM tenant_retention_usage WHERE tenant_id='default') AS retained").fetchone();return row

def test_real_pg_external_input_success_and_replay_skip_input_and_new_transaction(database,metrics):
 async def run():
  async with store(database) as (value,body):
   first=await value.create('default',body,'SYNTHETIC_STABLE_KEY');again=await value.create('default',body,'SYNTHETIC_STABLE_KEY')
   assert first==again and value.blobs.reads==1
 asyncio.run(run())
 assert stats(database)=={'batches':1,'tasks':2,'keys':1,'retained':2}
 assert metrics.value('total')==2 and metrics.value('idempotency_precheck')==2
 for stage in ('input_read_validate','transaction','transaction_begin','transaction_commit','idempotency_tenant_lock','retention_lock','queue_lock','task_insert','idempotency_remember'):assert metrics.value(stage)==1,stage
 assert metrics.value('pool_acquire')==3 and metrics.value('pool_release')==3
 assert 'SYNTHETIC_' not in metrics.text()

def test_real_pg_concurrent_keys_quota_refusal_rolls_back_entire_losing_batch(database,metrics):
 database.owner_conn.execute("UPDATE tenant_retention_limits SET max_retained_tasks=2 WHERE tenant_id='default'")
 async def run():
  async with store(database) as (value,body):
   value.blobs.barrier=[0,asyncio.Event()]
   result=await asyncio.gather(value.create('default',body,'SYNTHETIC_KEY_A'),value.create('default',body,'SYNTHETIC_KEY_B'),return_exceptions=True)
   assert sum(isinstance(v,dict) for v in result)==1
   errors=[v for v in result if isinstance(v,BaseException)];assert len(errors)==1 and isinstance(errors[0],S.APIError) and errors[0].status==429
 asyncio.run(run())
 assert stats(database)=={'batches':1,'tasks':2,'keys':1,'retained':2}
 assert metrics.value('total')==1 and metrics.value('total','error')==1
 assert metrics.value('task_insert')==1 and metrics.value('task_insert','error')==1
 assert metrics.value('transaction_commit')==1 and metrics.value('transaction_rollback')==1

async def wait_blocked(database,needle):
 deadline=time.monotonic()+2
 while time.monotonic()<deadline:
  database.owner_conn.execute('SELECT pg_stat_clear_snapshot()')
  row=database.owner_conn.execute("SELECT count(*) AS n FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE %s",('%'+needle+'%',)).fetchone()
  if row['n']:return
  await asyncio.sleep(.01)
 raise AssertionError('synthetic query did not reach expected row-lock wait')

def test_real_pg_cancel_at_first_tenant_lock_rolls_back_key_and_releases_pool(database,metrics):
 async def run():
  async with store(database) as (value,body):
   c=database.owner_conn;c.execute('BEGIN');c.execute("SELECT id FROM tenants WHERE id='default' FOR NO KEY UPDATE")
   task=asyncio.create_task(value.create('default',body,'SYNTHETIC_CANCEL_KEY'))
   try:
    await wait_blocked(database,'SELECT id FROM tenants');task.cancel()
    with pytest.raises(asyncio.CancelledError):await task
   finally:
    c.rollback()
    if not task.done():task.cancel();await asyncio.gather(task,return_exceptions=True)
   assert value.pool.get_stats()['pool_available']>=1
 asyncio.run(run())
 assert stats(database)=={'batches':0,'tasks':0,'keys':0,'retained':0}
 for stage in ('total','idempotency_tenant_lock','transaction'):assert metrics.value(stage,'cancelled')==1
 assert metrics.value('transaction_rollback')==1 and metrics.value('transaction_commit')==0

def test_real_pg_queue_lock_timer_includes_induced_wait_but_is_not_pg_wait_metric(database,metrics):
 database.owner_conn.execute("INSERT INTO tenant_queues(tenant_id,model) VALUES('default',%s)",(MODEL,))
 async def run():
  async with store(database,blob=False) as (value,body):
   c=database.owner_conn;c.execute('BEGIN');c.execute("SELECT tenant_id FROM tenant_queues WHERE tenant_id='default' FOR UPDATE")
   task=asyncio.create_task(value.create('default',body,'SYNTHETIC_QUEUE_KEY'))
   try:
    await wait_blocked(database,'SELECT tenant_id FROM tenant_queues');await asyncio.sleep(.08);c.rollback();result=await task
    assert result['request_counts']['total']==2
   finally:
    c.rollback()
    if not task.done():task.cancel();await asyncio.gather(task,return_exceptions=True)
 asyncio.run(run())
 assert metrics.value('queue_lock')==1 and metrics.value('queue_lock',suffix='sum')>=.08
 assert stats(database)=={'batches':1,'tasks':2,'keys':1,'retained':2}

def test_real_pg_object_failure_never_starts_write_transaction_or_exposes_error(database,metrics):
 error=OSError('SYNTHETIC_PRIVATE_S3_ENDPOINT_AND_KEY')
 async def run():
  async with store(database) as (value,body):
   value.blobs.error=error
   with pytest.raises(OSError) as seen:await value.create('default',body,'SYNTHETIC_FAIL_KEY')
   assert seen.value is error
 asyncio.run(run())
 assert stats(database)=={'batches':0,'tasks':0,'keys':0,'retained':0}
 assert metrics.value('input_read_validate','error')==metrics.value('total','error')==1
 assert metrics.value('transaction_begin')==0 and 'SYNTHETIC_' not in metrics.text()

def test_rollback_cleanup_failure_preserves_python_exception_semantics(metrics):
 body=ValueError('PRIVATE_BODY');cleanup=RuntimeError('PRIVATE_ROLLBACK')
 async def run():
  context=Context(exit_error=cleanup)
  with pytest.raises(RuntimeError) as seen:
   async with M.observed_transaction(SimpleNamespace(transaction=lambda:context)):raise body
  assert seen.value is cleanup and cleanup.__context__ is body and len(context.events)==2
 asyncio.run(run())
 assert metrics.value('transaction_rollback','error')==1 and metrics.value('transaction','error')==1
 assert metrics.value('transaction_commit')==0 and 'PRIVATE' not in metrics.text()

def test_transaction_begin_failure_has_no_exit_or_commit_observation(metrics):
 error=OSError('PRIVATE_BEGIN')
 async def run():
  context=Context(enter_error=error)
  with pytest.raises(OSError) as seen:
   async with M.observed_transaction(SimpleNamespace(transaction=lambda:context)):pytest.fail('unexpected body')
  assert seen.value is error and context.events==['enter']
 asyncio.run(run())
 assert metrics.value('transaction_begin','error')==1 and metrics.value('transaction','error')==1
 assert metrics.value('transaction_commit')==metrics.value('transaction_rollback')==0

def test_real_pg_concurrent_same_key_commits_one_batch_and_returns_identical_response(database,metrics):
 async def run():
  async with store(database) as (value,body):
   value.blobs.barrier=[0,asyncio.Event()]
   first,second=await asyncio.gather(value.create('default',body,'SYNTHETIC_SAME_KEY'),value.create('default',body,'SYNTHETIC_SAME_KEY'))
   assert first==second and value.blobs.reads==2
 asyncio.run(run())
 assert stats(database)=={'batches':1,'tasks':2,'keys':1,'retained':2}
 assert metrics.value('total')==2 and metrics.value('transaction_commit')==2
 assert metrics.value('idempotency_tenant_lock')==2 and metrics.value('task_insert')==1

def test_real_pg_cancellation_during_input_read_never_reaches_write_transaction(database,metrics):
 async def run():
  async with store(database) as (value,body):
   started=asyncio.Event()
   async def blocked(*args):started.set();await asyncio.Event().wait()
   value.blobs.read_input=blocked
   task=asyncio.create_task(value.create('default',body,'SYNTHETIC_READ_CANCEL'))
   await asyncio.wait_for(started.wait(),2);task.cancel()
   with pytest.raises(asyncio.CancelledError):await task
 asyncio.run(run())
 assert stats(database)=={'batches':0,'tasks':0,'keys':0,'retained':0}
 assert metrics.value('total','cancelled')==metrics.value('input_read_validate','cancelled')==1
 assert metrics.value('transaction_begin')==metrics.value('transaction_commit')==0
