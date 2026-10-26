"""Opt-in private Agent service and finite coordinator; no owner migration here."""
from __future__ import annotations
import argparse,asyncio,hashlib,os,signal,stat,time,json,logging
from pathlib import Path
import uvicorn
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from backend.config import get_settings
from .contracts import protocol_json as strict_json
from backend.migrate import verify_schema
from .migrate import verify
from .app import create_app
from .store import AgentStore
from .batch import BatchAdapter
from .runtime import Coordinator
from .workflows import WorkflowCoordinator
from .executors import credentials


async def run_coordinator(settings,keys,endpoint,seconds,*,workflow=False):
    stop=asyncio.Event();loop=asyncio.get_running_loop()
    for sig in (signal.SIGTERM,signal.SIGINT):loop.add_signal_handler(sig,stop.set)
    pool=AsyncConnectionPool(settings.database_url,min_size=1,max_size=4,open=False,kwargs={
        'autocommit':True,'row_factory':dict_row,'connect_timeout':5,
        'options':'-c statement_timeout=10000 -c lock_timeout=2000 -c idle_in_transaction_session_timeout=15000'})
    await pool.open(wait=True,timeout=30)
    adapter=None if workflow else BatchAdapter(pool,base_url=endpoint,tenant_keys=keys)
    try:
        async with pool.connection() as db:
            await verify_schema(db,minimum_version=11);await verify(db)
            for tid,key in keys.items():
                row=await (await db.execute('SELECT tenant_id FROM public.api_keys WHERE key_hash=%s AND NOT revoked',(hashlib.sha256(key.encode()).hexdigest(),))).fetchone()
                if not row or row['tenant_id']!=tid:raise ValueError('Executor credential does not belong to configured tenant')
        if workflow:
            if not settings.agent_workflows_enabled:raise ValueError('Workflow admission and compatible coordinators must be explicitly enabled together')
            coordinator=WorkflowCoordinator(AgentStore(pool),owner='workflow-coordinator-'+os.urandom(8).hex(),tenants=keys)
        else:coordinator=Coordinator(AgentStore(pool),adapter,owner='agent-coordinator-'+os.urandom(8).hex(),tenants=keys)
        deadline=time.monotonic()+seconds
        while not stop.is_set() and time.monotonic()<deadline:
            try:worked=await coordinator.tick()
            except Exception as exc:
                logging.getLogger('batchforge.agents').warning('Coordinator action interrupted: %s',type(exc).__name__)
                worked=False
            if not worked:
                try:await asyncio.wait_for(stop.wait(),timeout=1)
                except TimeoutError:pass
    finally:
        if adapter is not None:await adapter.client.aclose()
        await pool.close()


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('service',choices=['api','coordinator','workflow-coordinator'])
    p.add_argument('--executor-keys',type=Path,required=True);p.add_argument('--batch-url',default='http://127.0.0.1:8080')
    p.add_argument('--port',type=int,default=18170);p.add_argument('--seconds',type=int,default=3600)
    p.add_argument('--read-only',action='store_true',help='Inspect a preserved Agent database without mutation routes')
    args=p.parse_args()
    if not 1<=args.port<=65535 or not 30<=args.seconds<=50400:p.error('Bounded port and lifetime required')
    if args.read_only and args.service!='api':p.error('Read-only mode is available only for the inspection API; no coordinator starts')
    keys=credentials(args.executor_keys);settings=get_settings()
    if args.service=='api':uvicorn.run(create_app(settings,executor_keys_file=args.executor_keys,batch_endpoint=args.batch_url,read_only=args.read_only),host='127.0.0.1',port=args.port,access_log=False)
    else:asyncio.run(run_coordinator(settings,keys,args.batch_url,args.seconds,workflow=args.service=='workflow-coordinator'))


if __name__=='__main__':main()
