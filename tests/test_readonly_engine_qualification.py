import asyncio
from contextlib import asynccontextmanager
import pytest
from fastapi.testclient import TestClient
from backend.config import Settings
from ops import qualify_engine_evidence as q


@pytest.mark.parametrize('method,path',[('POST','/v1/files'),('POST','/v1/batches'),('POST','/internal/tasks/complete'),
    ('DELETE','/v1/evaluations/x'),('GET','/v1/files/private/content')])
async def test_guard_denies_before_body_parsing_storage_or_app_invocation(method,path):
    async def app(*_):pytest.fail('Forbidden route must not reach the application')
    async def receive():pytest.fail('Forbidden body must not be read')
    messages=[]
    async def send(message):messages.append(message)
    await q.ReadOnlyRoutes(app)({'type':'http','method':method,'path':path},receive,send)
    assert messages[0]['status']==503 and b'read_only_qualification' in messages[1]['body']


def settings():return Settings(batch_submission_enabled=False,blob_gc_enabled=False,result_archive_enabled=False,
    batchforge_worker_token='test-worker-only-token-12345')


@pytest.mark.parametrize('flag',['batch_submission_enabled','blob_gc_enabled','result_archive_enabled','agent_api_enabled'])
def test_writable_or_effectful_configuration_is_rejected_before_candidate_start(flag):
    value=settings().model_copy(update={flag:True})
    with pytest.raises(ValueError):q.read_only_app(value)


def test_read_paths_keep_existing_tenant_auth_and_do_not_enter_writable_lifespan():
    wrapper=q.read_only_app(settings())
    class Store:
        async def tenant(self,key):return key
        async def list_engine_evaluations(self,tenant):return {'owner':tenant,'data':[]}
    wrapper.app.state.store=Store();client=TestClient(wrapper)
    assert client.get('/v1/engine-evaluations').status_code==401
    r=client.get('/v1/engine-evaluations',headers={'Authorization':'Bearer owner-a'})
    assert r.status_code==200 and r.json()['owner']=='owner-a'


def test_candidate_readiness_precedes_spa_catchall_and_requires_real_tenant_auth(monkeypatch):
    class Store:
        async def tenant(self,key):
            if key!='owner-a':raise q.APIError(401,'Invalid key','invalid_api_key')
            return 'tenant-a'
    class Cursor:
        async def fetchone(self):return {'mode':'on'}
    class Conn:
        async def execute(self,sql):return Cursor()
    class Pool:
        @asynccontextmanager
        async def connection(self):yield Conn()
    async def schema(*_,**kw):return 11
    monkeypatch.setattr(q,'verify_schema',schema)
    wrapper=q.read_only_app(settings());wrapper.app.state.store=Store();wrapper.app.state.pool=Pool()
    client=TestClient(wrapper)
    assert client.get('/ops/readonly-readyz').status_code==401
    assert client.get('/ops/readonly-readyz',headers={'Authorization':'Bearer wrong'}).status_code==401
    r=client.get('/ops/readonly-readyz',headers={'Authorization':'Bearer owner-a'})
    assert r.status_code==200 and r.headers['content-type']=='application/json'
    assert r.json()=={'status':'read_only_qualification','transaction_read_only':True,'schema_version':11}


@pytest.mark.parametrize('mode',['on','off'])
async def test_custom_lifespan_requires_server_readonly_and_starts_no_recovery_tasks(monkeypatch,mode):
    opened=[];calls=[]
    class Cursor:
        async def fetchone(self):return {'mode':mode}
    class Conn:
        async def execute(self,sql):calls.append(sql);return Cursor()
    class Pool:
        def __init__(self,*a,**kw):self.kw=kw;opened.append(self)
        async def open(self,**kw):pass
        async def close(self):self.closed=True
        @asynccontextmanager
        async def connection(self):yield Conn()
    async def schema(*_,**kw):return 11
    monkeypatch.setattr(q,'AsyncConnectionPool',Pool);monkeypatch.setattr(q,'verify_schema',schema)
    monkeypatch.setattr(asyncio,'create_task',lambda *_a,**_k:pytest.fail('No writable maintenance task may start'))
    wrapper=q.read_only_app(settings());app=wrapper.app
    async def enter():
        async with app.router.lifespan_context(app):assert app.state.store.schema_version==11
    if mode=='off':
        with pytest.raises(ValueError,match='Read-only candidate session'):await enter()
    else:await enter()
    assert 'default_transaction_read_only=on' in opened[0].kw['kwargs']['options']
    assert opened[0].closed and calls==["SELECT current_setting('transaction_read_only') AS mode"]
