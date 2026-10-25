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


def credentials(path):
    # Credentials are never accepted from model/user request bodies. A symlink,
    # other-owned file or group-readable registry is not a protected executor.
    path=Path(path)
    try:fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW)
    except OSError:raise ValueError('Regular protected registry required') from None
    with os.fdopen(fd,'rb') as stream:
        st=os.fstat(stream.fileno())
        if not stat.S_ISREG(st.st_mode) or st.st_uid!=os.geteuid() or stat.S_IMODE(st.st_mode)&0o077 or st.st_size>65536:
            raise ValueError('Executor registry must be owner-only and bounded')
        raw=stream.read(65537)
    if len(raw)>65536:raise ValueError('Executor registry exceeds bound')
    value=strict_json(raw)
    if not isinstance(value,dict) or not 1<=len(value)<=64 or any(not isinstance(k,str) or not 1<=len(k)<=128 or not isinstance(v,str) or not 16<=len(v)<=256 for k,v in value.items()):
        raise ValueError('Explicit tenant-key registry required')
    return value


async def run_coordinator(settings,keys,endpoint,seconds):
    stop=asyncio.Event();loop=asyncio.get_running_loop()
    for sig in (signal.SIGTERM,signal.SIGINT):loop.add_signal_handler(sig,stop.set)
    pool=AsyncConnectionPool(settings.database_url,min_size=1,max_size=4,open=False,kwargs={
        'autocommit':True,'row_factory':dict_row,'connect_timeout':5,
        'options':'-c statement_timeout=10000 -c lock_timeout=2000 -c idle_in_transaction_session_timeout=15000'})
    await pool.open(wait=True,timeout=30)
    adapter=BatchAdapter(pool,base_url=endpoint,tenant_keys=keys)
    try:
        async with pool.connection() as db:
            await verify_schema(db,minimum_version=11);await verify(db)
            for tid,key in keys.items():
                row=await (await db.execute('SELECT tenant_id FROM public.api_keys WHERE key_hash=%s AND NOT revoked',(hashlib.sha256(key.encode()).hexdigest(),))).fetchone()
                if not row or row['tenant_id']!=tid:raise ValueError('Executor credential does not belong to configured tenant')
        coordinator=Coordinator(AgentStore(pool),adapter,owner='agent-coordinator-'+os.urandom(8).hex(),tenants=keys)
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
        await adapter.client.aclose();await pool.close()


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('service',choices=['api','coordinator'])
    p.add_argument('--executor-keys',type=Path,required=True);p.add_argument('--batch-url',default='http://127.0.0.1:8080')
    p.add_argument('--port',type=int,default=18170);p.add_argument('--seconds',type=int,default=3600)
    args=p.parse_args()
    if not 1<=args.port<=65535 or not 30<=args.seconds<=50400:p.error('Bounded port and lifetime required')
    keys=credentials(args.executor_keys);settings=get_settings()
    if args.service=='api':uvicorn.run(create_app(settings,executor_tenants=tuple(keys)),host='127.0.0.1',port=args.port,access_log=False)
    else:asyncio.run(run_coordinator(settings,keys,args.batch_url,args.seconds))


if __name__=='__main__':main()
