"""Private Agent API, opt-in and additive to the existing Batch service.

Owner migration is a separate command. Startup never creates schemas or grants
privileges. Run admission uses the authenticated tenant and server model list;
model output cannot choose credentials, namespaces, limits or tool grants.
"""
from __future__ import annotations
from contextlib import asynccontextmanager
import asyncio,logging,hashlib
from fastapi import FastAPI,Depends,Header,Query
from fastapi.responses import JSONResponse,StreamingResponse
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


def create_app(settings=None,*,store=None,auth=None,executor_tenants=()):
    settings=settings or get_settings()
    @asynccontextmanager
    async def lifespan(app):
        if store is not None:
            app.state.store=store;app.state.auth=auth;yield;return
        pool=AsyncConnectionPool(settings.database_url,min_size=1,max_size=4,open=False,
            kwargs={'autocommit':True,'row_factory':dict_row,'connect_timeout':5,
                    'options':'-c statement_timeout=10000 -c lock_timeout=2000 -c idle_in_transaction_session_timeout=15000'})
        await pool.open(wait=True,timeout=30)
        try:
            async with pool.connection() as conn:
                await verify_schema(conn,minimum_version=11);await verify(conn)
            async def authenticate(key):
                async with pool.connection() as conn:
                    row=await (await conn.execute('SELECT tenant_id FROM public.api_keys WHERE key_hash=%s AND NOT revoked',
                        (hashlib.sha256(key.encode()).hexdigest(),))).fetchone()
                if not row:raise AgentError(401,'invalid_api_key')
                return row['tenant_id']
            app.state.store=AgentStore(pool);app.state.auth=authenticate
            yield
        finally:await pool.close()
    app=FastAPI(title='BatchForge Agents',version='0.1.0',lifespan=lifespan)

    def error(status,code):return JSONResponse(status_code=status,content={'error':{'code':code}})
    @app.middleware('http')
    async def bounded_json(request,call_next):
        if request.method in ('POST','PUT','PATCH'):
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
                if not row['writable']:raise RuntimeError('Database is read-only')
        except Exception:return error(503,'agent_store_unavailable')
        return {'service':'batchforge-agents','state':'ready'}
    @app.post('/v1/agent-runs')
    async def create(body:RunRequest,tid=Depends(tenant),idempotency_key:str=Header(min_length=1,max_length=128)):
        if tid not in executor_tenants:raise AgentError(503,'tenant_executor_not_configured')
        if not settings.batch_submission_enabled:raise AgentError(503,'admission_paused')
        if body.model not in settings.served_models():raise AgentError(400,'agent_model_not_served')
        try:policy=body.policy()
        except ValueError:raise AgentError(422,'invalid_agent_policy') from None
        return await app.state.store.create(tid,idempotency_key,goal=body.goal,model=body.model,policy=policy)
    @app.get('/v1/agent-runs')
    async def runs(tid=Depends(tenant),limit:int=Query(default=20,ge=1,le=100),after:str|None=Query(default=None,max_length=80)):
        return await app.state.store.list(tid,limit=limit,after=after)
    @app.get('/v1/agent-runs/{rid}')
    async def get(rid:str,tid=Depends(tenant)):return await app.state.store.get(tid,rid)
    @app.post('/v1/agent-runs/{rid}/cancel')
    async def cancel(rid:str,tid=Depends(tenant)):return await app.state.store.cancel(tid,rid)
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
    return app
