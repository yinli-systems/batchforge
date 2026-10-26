"""Bounded real Azure read-only qualification, without primary service changes.

The candidate uses the same API/Store routes with a read-only pool and no
maintenance tasks. A method/path guard precedes request parsing and S3 actions.
Normal /readyz remains503 because this app is not a writable primary replica.
"""
from __future__ import annotations
import argparse,asyncio,contextlib,hashlib,json,os,signal,socket,subprocess,sys,time
from contextlib import asynccontextmanager
from datetime import datetime,timezone
from pathlib import Path
import httpx,psycopg
from psycopg.rows import dict_row
from psycopg.conninfo import conninfo_to_dict
from psycopg_pool import AsyncConnectionPool
from fastapi import Request
from starlette.responses import JSONResponse
from backend.app import create_app
from backend.config import Settings
from backend.store import Store,APIError
from backend.migrate import verify_schema
from ops.engine_quality_campaign import acquire,release,save,process_environment,signal_exact
from ops.worker_latch_guard import Pins,ROOT,STOP
from ops.ha.retune_full_gpu import file_env
from ops.supervise import private_read
from ops.manage import process_identity
from ops.package_engine_evidence import package


def allowed_read(path):
    return path in ('/healthz','/readyz','/ops/readonly-readyz','/v1/fleet','/app.js','/style.css','/index.html') or any(
        path==prefix or path.startswith(prefix+'/') for prefix in ('/v1/evaluations','/v1/engine-evaluations')) or (
        path.startswith('/v1/batches/') and path.endswith('/receipt'))


class ReadOnlyRoutes:
    def __init__(self,app):self.app=app
    async def __call__(self,scope,receive,send):
        if scope['type']=='http' and (scope['method'] not in ('GET','HEAD') or not allowed_read(scope['path'])):
            return await JSONResponse({'error':{'message':'Read-only qualification endpoint','code':'read_only_qualification'}},
                status_code=503,headers={'Retry-After':'5'})(scope,receive,send)
        await self.app(scope,receive,send)


def read_only_app(settings):
    if settings.agent_api_enabled or settings.agent_workflows_enabled:raise ValueError('Qualification does not mount Agent executors')
    if settings.batch_submission_enabled or settings.blob_gc_enabled or settings.result_archive_enabled:raise ValueError('Read-only qualification flags required')
    app=create_app(settings)
    @asynccontextmanager
    async def lifespan(app):
        pool=AsyncConnectionPool(settings.database_url,min_size=1,max_size=2,open=False,timeout=3,
            kwargs={'autocommit':True,'row_factory':dict_row,'connect_timeout':5,
                'options':'-c default_transaction_read_only=on -c statement_timeout=5000 -c lock_timeout=1000'})
        await pool.open(wait=True,timeout=30)
        try:
            async with pool.connection() as conn:
                schema=await verify_schema(conn,minimum_version=settings.minimum_schema_version())
                row=await (await conn.execute("SELECT current_setting('transaction_read_only') AS mode")).fetchone()
                if row['mode']!='on':raise ValueError('Read-only candidate session required')
            app.state.pool=pool;app.state.settings=settings;app.state.worker_tokens=settings.worker_tokens()
            app.state.store=Store(pool,settings,schema_version=schema)
            # Do not enter create_app's writable lifespan or start a reaper,
            # archiver, collector or Agent coordinator in this candidate.
            yield
        finally:await pool.close()
    app.router.lifespan_context=lifespan
    @app.get('/ops/readonly-readyz')
    async def readonly_ready(request:Request):
        auth=request.headers.get('authorization','')
        if not auth.startswith('Bearer '):raise APIError(401,'API key required','invalid_api_key')
        await app.state.store.tenant(auth[7:])
        async with app.state.pool.connection() as conn:
            row=await (await conn.execute("SELECT current_setting('transaction_read_only') AS mode")).fetchone()
            schema=await verify_schema(conn,minimum_version=11)
        if row['mode']!='on':raise ValueError('Candidate lost read-only mode')
        return {'status':'read_only_qualification','transaction_read_only':True,'schema_version':schema}
    # create_app has already installed its SPA catch-all. A route appended
    # after that catch-all returns index.html instead of this authenticated
    # readiness receipt, so give the candidate-only route explicit priority.
    app.router.routes.insert(0,app.router.routes.pop())
    return ReadOnlyRoutes(app)


def serve(path):
    config=json.loads(private_read(path));source=Path(__file__).resolve().parents[1]
    if Path(config['source'])!=source:raise ValueError('Canary source differs')
    settings=Settings()
    if conninfo_to_dict(settings.database_url).get('sslmode')!='verify-full':raise ValueError('Verified Azure TLS configuration required')
    save(path.parent/'server-identity.json',{'identity':process_identity(os.getpid())})
    import uvicorn
    uvicorn.run(read_only_app(settings),host='127.0.0.1',port=18160,access_log=False,log_level='warning')


def qualify(base):
    if (base.parent!=ROOT.parent or base.resolve()!=base or not base.name.startswith('evidence-api-engine-product-qualification-')):
        raise ValueError('New owned qualification run required')
    if time.time()>STOP-1800:raise ValueError('Insufficient qualification/cleanup margin')
    base.mkdir(mode=0o700,exist_ok=False);locks=[];children=[];report={'phase':'preflight','passed':False,
        'new_gpu_tasks':0,'primary_services_changed':False,'production_database_mutated':False,'browser_visual_qa':False}
    source=Path(__file__).resolve().parents[1]
    try:
        locks=acquire();pins=Pins();pinned=pins.check(full=True)
        receipt=json.loads(private_read(source.parent/'source-receipt.json'))
        if any(hashlib.sha256((source/n).read_bytes()).hexdigest()!=sha for n,sha in receipt['files'].items()):raise ValueError('Candidate bytes changed')
        report['source_commit']=receipt['git_commit'];api=next(s for s in pins.services if s['role']=='api')
        env=process_environment(Path(f'/proc/{pins.identities["api-"+api["name"]]["pid"]}/environ').read_bytes());env.update(file_env(api['env_file']))
        key=file_env(ROOT.parent/'access.env')['BATCHFORGE_API_KEY']
        with psycopg.connect(env['DATABASE_URL'],autocommit=True,connect_timeout=5,
            options='-c default_transaction_read_only=on -c statement_timeout=5000') as db:
            if db.execute('SHOW transaction_read_only').fetchone()[0]!='on':raise ValueError('Read-only operator session required')
            tenant=db.execute('SELECT tenant_id FROM api_keys WHERE key_hash=%s AND NOT revoked',(hashlib.sha256(key.encode()).hexdigest(),)).fetchone()[0]
        b=ROOT.parent/'soak480-engine-quality-full-20261026-v1';c=ROOT.parent/'evidence-api-engine-contract-correction-20261026-v1'
        paths={'report':b/'run/report.json','plan':b/'run/plan.json','scores':b/'run/scores.jsonl',
            'audit':ROOT.parent/'evidence-api-engine-quality-full-audit-20261026-v1/validation.json',
            'campaign':b/'campaign.json','restore':b/'recovery-validation.json','correction':c/'run/report.json','correction_audit':c/'validation.json'}
        result=package(paths,base/'engine-evidence',tenant=tenant,eid='engine-squad-jsonspan-20261026',name='480B · Full SQuAD · engine only')
        report['package']=result
        env.update(PYTHONPATH=str(source),PYTHONSAFEPATH='1',PYTHONDONTWRITEBYTECODE='1',CUDA_VISIBLE_DEVICES='',
            FRONTEND_DIR=str(source/'frontend'),ENGINE_EVIDENCE_CATALOGUE=result['catalogue'],
            BATCH_SUBMISSION_ENABLED='false',BLOB_GC_ENABLED='false',RESULT_ARCHIVE_ENABLED='false',DB_POOL_MIN='1',DB_POOL_MAX='2',
            AGENT_API_ENABLED='false',AGENT_WORKFLOWS_ENABLED='false')
        with socket.socket() as probe:probe.bind(('127.0.0.1',18160))
        private=base/'serve-private.json';save(private,{'source':str(source)})
        with (base/'api-private.log').open('xb') as log:
            child=subprocess.Popen(['/usr/bin/timeout','--signal=TERM','--kill-after=15','600',sys.executable,'-m',
                'ops.qualify_engine_evidence','--serve-config',str(private)],env=env,cwd=source,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        children.append(process_identity(child.pid));report['candidate_wrapper_identity']=children[0];save(base/'validation.json',report)
        report['phase']='candidate_starting';save(base/'validation.json',report)
        with httpx.Client(base_url='http://127.0.0.1:18160',headers={'Authorization':'Bearer '+key},trust_env=False,timeout=10) as http:
            until=time.monotonic()+45
            while True:
                if child.poll() is not None:raise RuntimeError('Read-only candidate exited')
                try:r=http.get('/ops/readonly-readyz');ready=r.status_code==200 and r.json().get('transaction_read_only') is True
                except httpx.TransportError:ready=False
                if ready:break
                if time.monotonic()>=until:raise RuntimeError('Read-only qualification startup timeout')
                time.sleep(.3)
            report['database_read_only_verified']=True;report['schema_version']=r.json()['schema_version']
            report['phase']='http_readback';save(base/'validation.json',report)
            if http.get('/readyz').status_code!=503:raise ValueError('Read-only candidate must not pass primary writable readiness')
            for path in ('/v1/files','/v1/batches','/internal/tasks/complete'):
                denied=http.post(path,json={});assert denied.status_code==503 and denied.json()['error']['code']=='read_only_qualification'
            report['mutations_blocked_before_app']=True
            r=http.get('/v1/engine-evaluations');r.raise_for_status();views=r.json()['data'];assert len(views)==1
            r=http.get('/v1/engine-evaluations/'+views[0]['id']);r.raise_for_status()
            assert hashlib.sha256(r.content).hexdigest()==r.headers['X-BatchForge-Evidence-SHA256']
            assert views[0]['metrics']['exact_match_percent']<82 and views[0]['exact_results']==8667
            report['real_owner_authenticated']=True;report['engine_view_sha256']=hashlib.sha256(r.content).hexdigest()
            for path in ('/v1/engine-evaluations','/v1/evaluations'):
                assert http.get(path,headers={'Authorization':'Bearer invalid_qualification_key'}).status_code==401
            assert http.get('/v1/engine-evaluations/missing').status_code==404
            r=http.get('/v1/evaluations');r.raise_for_status();states=[x['quality_state'] for x in r.json()['data']]
            assert 'failed' in states and 'passed' in states;report['existing_batch_states']=states
            r=http.get('/app.js');r.raise_for_status();assert hashlib.sha256(r.content).hexdigest()==receipt['files']['frontend/app.js']
        save(base/'node-private.json',{'source':str(source),'base_url':'http://127.0.0.1:18160','api_key':key,'output':str(base/'readback.json')})
        report['phase']='console_client_readback';save(base/'validation.json',report)
        node=Path('/tmp/bf-ci-20261025-v19/runner/externals/node24/bin/node')
        with (base/'node-private.log').open('xb') as log:
            done=subprocess.run([str(node),str(source/'ops/verify_engine_evidence.mjs'),str(base/'node-private.json')],
                cwd=source,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,timeout=60)
        if done.returncode:raise RuntimeError('Actual console client verification failed')
        report['client_readback']=json.loads((base/'readback.json').read_text());assert report['client_readback']['passed']
        if pins.check(full=True)!=pinned:raise ValueError('Primary state changed during candidate qualification')
        report.update(phase='complete',passed=True,primary_pins_unchanged=True,frontend_source_sha256=receipt['files']['frontend/app.js'])
    except BaseException as exc:report.update(phase_before_failure=report['phase'],phase='failed',passed=False,error_type=type(exc).__name__)
    finally:
        identity_path=base/'server-identity.json'
        if identity_path.exists():children.append(json.loads(identity_path.read_text())['identity'])
        errors=[]
        for identity in reversed(children):
            if identity is not None:
                try:signal_exact(identity,grace=10,kill=True)
                except BaseException as exc:errors.append(type(exc).__name__)
        report['owned_candidate_processes_stopped']=all(i is None or process_identity(i['pid'])!=i for i in children)
        report['cleanup_errors']=errors
        if errors or not report['owned_candidate_processes_stopped']:report.update(passed=False,phase='failed')
        (base/'node-private.json').unlink(missing_ok=True);report['closed_at']=datetime.now(timezone.utc).isoformat()
        try:save(base/'validation.json',report)
        finally:release(locks)
    print(json.dumps({k:report.get(k) for k in ('phase','passed','error_type','owned_candidate_processes_stopped')}))
    if not report['passed']:raise SystemExit(1)


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run',type=Path);p.add_argument('--serve-config',type=Path)
    a=p.parse_args();os.umask(0o077)
    if a.serve_config:serve(a.serve_config)
    elif a.run:qualify(a.run)
    else:p.error('Owned run or internal serve configuration required')


if __name__=='__main__':main()
