"""Private Agent API, opt-in and additive to the existing Batch service.

Owner migration is a separate command. Startup never creates schemas or grants
privileges. Run admission uses the authenticated tenant and server model list;
model output cannot choose credentials, namespaces, limits or tool grants.
"""
from __future__ import annotations
from contextlib import asynccontextmanager,AsyncExitStack
from pathlib import Path
import asyncio,logging,hashlib,re
from fastapi import FastAPI,Depends,Header,Query,Request
from fastapi.responses import JSONResponse,StreamingResponse,FileResponse,RedirectResponse
from fastapi.exceptions import RequestValidationError
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool,PoolTimeout
import psycopg
from pydantic import Field
from backend.config import get_settings
from backend.store import APIError
from backend.migrate import verify_schema
from .migrate import verify
from .contracts import Strict,Policy,canonical,protocol_json
from .store import AgentStore,AgentError
from .executors import credentials
from .receipts import HTTPReceipts,reconcile
from .tools import SPECS

logger=logging.getLogger('batchforge.agents')


class RunRequest(Strict):
    goal:str=Field(min_length=1,max_length=32768)
    model:str=Field(min_length=1,max_length=256)
    allowed_tools:list[str]=Field(default_factory=lambda:list(Policy().allowed_tools),max_length=3)
    memory_namespace:str=Field(default='default',pattern=r'^[a-zA-Z0-9_-]{1,64}$')
    max_model_calls:int=Field(default=8,ge=1,le=32)
    max_tool_calls:int=Field(default=8,ge=0,le=64)
    max_generated_tokens:int=Field(default=4096,ge=64,le=32768)
    max_wall_seconds:int=Field(default=900,ge=30,le=3600)
    answer_format:str=Field(default='text',pattern=r'^(text|identifier|number)$')
    require_citations:bool=False
    planner_protocol:str=Field(default='json-v1',pattern=r'^(json-v1|schema-v1)$')
    tool_sequence:list[str]=Field(default_factory=list,max_length=8)

    def policy(self):
        return Policy(**{k:v for k,v in self.model_dump().items() if k not in ('goal','model','allowed_tools','tool_sequence')},
                      allowed_tools=tuple(self.allowed_tools),tool_sequence=tuple(self.tool_sequence))


class MemoryWrite(Strict):
    text:str=Field(max_length=32768)
    expected_version:int=Field(ge=0)


def create_app(settings=None,*,store=None,auth=None,executor_tenants=(),shared_backend=None,
               executor_keys_file=None,batch_endpoint=None,read_only=False):
    settings=settings or get_settings()
    @asynccontextmanager
    async def lifespan(app):
        async with AsyncExitStack() as stack:
            parent=shared_backend() if shared_backend else None
            if store is not None:
                selected,authenticate=store,auth
            elif parent is not None:
                selected,authenticate=AgentStore(parent.pool),parent.tenant
            else:
                pool=AsyncConnectionPool(settings.database_url,min_size=1,max_size=4,open=False,
                    kwargs={'autocommit':True,'row_factory':dict_row,'connect_timeout':5,
                            'options':'-c statement_timeout=10000 -c lock_timeout=2000 -c idle_in_transaction_session_timeout=15000'})
                await pool.open(wait=True,timeout=30);stack.push_async_callback(pool.close)
                selected=AgentStore(pool)
                async def authenticate(key):
                    async with pool.connection() as conn:
                        row=await (await conn.execute('SELECT tenant_id FROM public.api_keys WHERE key_hash=%s AND NOT revoked',
                            (hashlib.sha256(key.encode()).hexdigest(),))).fetchone()
                    if not row:raise AgentError(401,'invalid_api_key')
                    return row['tenant_id']
            if store is None:
                async with selected.pool.connection() as conn:
                    await verify_schema(conn,minimum_version=11);await verify(conn)
            tenants=set(executor_tenants);keys={}
            if executor_keys_file:
                keys=credentials(executor_keys_file)
                for tid,key in keys.items():
                    if await authenticate(key)!=tid:raise ValueError('Executor key does not belong to configured tenant')
                tenants=set(keys)
            reader=parent.batch_receipt if parent is not None else None
            if batch_endpoint:
                if not keys:raise ValueError('Protected executor registry required for child receipt lookup')
                http=await stack.enter_async_context(HTTPReceipts(batch_endpoint,keys));reader=http.read
            app.state.store=selected;app.state.auth=authenticate;app.state.executor_tenants=tenants;app.state.receipts=reader
            yield
    app=FastAPI(title='BatchForge Agents',version='0.1.0',lifespan=lifespan)

    def error(status,code):return JSONResponse(status_code=status,content={'error':{'code':code}})
    @app.middleware('http')
    async def bounded_json(request,call_next):
        if request.method in ('POST','PUT','PATCH'):
            if read_only:return error(403,'agent_read_only_snapshot')
            content=bytearray()
            async for part in request.stream():
                content.extend(part)
                if len(content)>196608:return error(413,'agent_request_too_large')
            request._body=bytes(content)
            if content:
                try:protocol_json(content)
                except (ValueError,UnicodeError,RecursionError):return error(422,'invalid_agent_json')
        response=await call_next(request)
        response.headers['Cache-Control']='no-store';response.headers['X-Content-Type-Options']='nosniff'
        response.headers['Content-Security-Policy']="default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; base-uri 'none'; frame-ancestors 'none'; form-action 'self'"
        response.headers['Referrer-Policy']='no-referrer'
        return response
    @app.exception_handler(AgentError)
    async def agent_error(_request,exc):return error(exc.status,exc.code)
    @app.exception_handler(APIError)
    async def auth_error(_request,exc):return error(exc.status,exc.code)
    @app.exception_handler(RequestValidationError)
    async def validation_error(_request,_exc):return error(422,'invalid_agent_request')
    @app.exception_handler(ValueError)
    async def invalid_value(_request,_exc):return error(422,'invalid_agent_value')
    @app.exception_handler(psycopg.Error)
    async def database_error(_request,exc):
        if exc.sqlstate in ('BA429','BA409'):return error(429 if exc.sqlstate=='BA429' else 409,'agent_quota_reached' if exc.sqlstate=='BA429' else 'agent_state_conflict')
        logger.warning('Agent database request unavailable: %s',type(exc).__name__)
        return error(503,'agent_store_unavailable')
    @app.exception_handler(PoolTimeout)
    async def pool_error(_request,_exc):return error(503,'agent_store_unavailable')

    async def tenant(authorization:str|None=Header(default=None)):
        if not authorization or not authorization.startswith('Bearer ') or len(authorization)>512:
            raise AgentError(401,'invalid_api_key')
        return await app.state.auth(authorization[7:])

    @app.get('/healthz')
    async def health():return {'service':'batchforge-agents','state':'process_alive'}
    @app.get('/readyz')
    async def ready():
        try:
            async with app.state.store.pool.connection() as conn:
                await verify_schema(conn,minimum_version=11);await verify(conn)
                row=await (await conn.execute("SELECT NOT pg_is_in_recovery() AND current_setting('transaction_read_only')='off' AS writable")).fetchone()
                if not row['writable'] and not read_only:raise RuntimeError('Database is read-only')
        except Exception:return error(503,'agent_store_unavailable')
        return {'service':'batchforge-agents','state':'read_only_snapshot' if read_only else 'ready'}
    @app.get('/v1/agent-capabilities')
    async def capabilities(tid=Depends(tenant)):
        return {'protocol':'batchforge.agent-capabilities.v1','tenant_id':tid,'models':list(settings.served_models()),
            'tools':SPECS,'planner_protocols':['json-v1']+(['schema-v1'] if settings.json_schema_enabled else []),
            'answer_formats':['text','identifier','number'],'max_tool_sequence':8,
            'admission_enabled':not read_only and settings.batch_submission_enabled and tid in app.state.executor_tenants,
            'memory_writes_enabled':not read_only,'cancellation_enabled':not read_only,
            'child_receipts_enabled':app.state.receipts is not None,'read_only_snapshot':read_only,
            'batch_console_path':'/' if shared_backend else None}
    @app.post('/v1/agent-runs')
    async def create(body:RunRequest,tid=Depends(tenant),idempotency_key:str=Header(min_length=1,max_length=128)):
        if tid not in app.state.executor_tenants:raise AgentError(503,'tenant_executor_not_configured')
        if not settings.batch_submission_enabled:raise AgentError(503,'admission_paused')
        if body.model not in settings.served_models():raise AgentError(400,'agent_model_not_served')
        if body.planner_protocol=='schema-v1' and not settings.json_schema_enabled:raise AgentError(400,'agent_schema_ingress_not_enabled')
        try:policy=body.policy()
        except ValueError:raise AgentError(422,'invalid_agent_policy') from None
        return await app.state.store.create(tid,idempotency_key,goal=body.goal,model=body.model,policy=policy)
    @app.get('/v1/agent-runs')
    async def runs(tid=Depends(tenant),limit:int=Query(default=20,ge=1,le=100),after:str|None=Query(default=None,max_length=80)):
        return await app.state.store.list(tid,limit=limit,after=after)
    @app.get('/v1/agent-runs/{rid}')
    async def get(rid:str,tid=Depends(tenant)):return await app.state.store.get(tid,rid)
    @app.get('/v1/agent-runs/{rid}/snapshot')
    async def snapshot(rid:str,tid=Depends(tenant)):
        run=await app.state.store.get(tid,rid)
        # Preserve Python's exact float spelling inside the byte commitments.
        # A JS re-serialization of e.g.42.0 is not the original state hash input.
        response=JSONResponse({'protocol':'batchforge.agent-snapshot.v1','run':run,
            'state_json':canonical(run['state']).decode(),'result_json':canonical(run['result']).decode()})
        response.headers['X-BatchForge-Agent-Snapshot-SHA256']=hashlib.sha256(response.body).hexdigest()
        return response
    @app.post('/v1/agent-runs/{rid}/cancel')
    async def cancel(rid:str,tid=Depends(tenant)):return await app.state.store.cancel(tid,rid)
    @app.get('/v1/agent-runs/{rid}/children/{bid}/receipt')
    async def child_receipt(rid:str,bid:str,tid=Depends(tenant)):
        run=await app.state.store.get(tid,rid)
        committed=next((r for r in run['state'].get('receipts',[]) if r.get('batch_id')==bid),None)
        if not committed or not re.fullmatch(r'batch_[A-Za-z0-9_-]{1,80}',bid):raise AgentError(404,'agent_child_not_found')
        if app.state.receipts is None:raise AgentError(503,'agent_child_receipt_unavailable')
        return reconcile(run,committed,await app.state.receipts(tid,bid))
    @app.get('/v1/agent-runs/{rid}/events')
    async def events(rid:str,tid=Depends(tenant),after:int=Query(default=-1,ge=-1),limit:int=Query(default=50,ge=1,le=100)):
        return await app.state.store.events_page(tid,rid,after=after,limit=limit)
    @app.get('/v1/agent-runs/{rid}/stream')
    async def stream(rid:str,tid=Depends(tenant),after:int=Query(default=-1,ge=-1)):
        # Authentication and ownership are resolved before opening the stream.
        await app.state.store.get(tid,rid)
        async def chunks():
            cursor=after
            for _ in range(15):
                result=await app.state.store.events_page(tid,rid,after=cursor,limit=100)
                for event in result['events']:
                    if event['revision']>cursor:
                        cursor=event['revision']
                        yield f'id: {cursor}\nevent: agent.event\ndata: '+canonical(event).decode()+'\n\n'
                yield ': heartbeat\n\n';await asyncio.sleep(2)
        return StreamingResponse(chunks(),media_type='text/event-stream',headers={'Cache-Control':'no-store','X-Accel-Buffering':'no'})
    @app.put('/v1/agent-memory/{namespace}/{key}')
    async def put(namespace:str,key:str,body:MemoryWrite,tid=Depends(tenant)):
        return await app.state.store.put_memory(tid,namespace,key,body.text,expected_version=body.expected_version)
    @app.get('/v1/agent-memory/{namespace}/{key}')
    async def memory(namespace:str,key:str,tid=Depends(tenant)):
        return await app.state.store.memory_get(tid,namespace,key)
    @app.get('/console',include_in_schema=False)
    async def console_redirect(request:Request):return RedirectResponse(str(request.url.path)+'/')
    @app.get('/console/{asset:path}',include_in_schema=False)
    async def console_asset(asset:str):
        if asset not in ('','index.html','app.js','core.js','styles.css'):raise AgentError(404,'not_found')
        target=Path(__file__).with_name('console')/(asset or 'index.html')
        if not target.is_file():raise AgentError(404,'not_found')
        return FileResponse(target)
    return app
