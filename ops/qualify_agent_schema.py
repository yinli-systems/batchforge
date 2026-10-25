"""Finite private-ingress qualification. Never roll the primary API or worker.

The extra loopback API admits bounded schemas to the existing queue. The same
production worker/engine executes them; no second KV budget is allocated. An
independent OS timeout limits the extra API even if this controller exits.
"""
from __future__ import annotations
import argparse,contextlib,fcntl,hashlib,json,os,signal,socket,subprocess,sys,time
from datetime import datetime,timezone
from pathlib import Path
import httpx
from ops.ha.retune_full_gpu import file_env
from ops.manage import process_identity
from ops.qualify_agent_runtime import write
from ops.worker_latch_guard import Pins

ENDPOINT='http://127.0.0.1:18172'


def canary_environment(original,source):
    if original.get('BATCH_SUBMISSION_ENABLED','true')!='true':
        raise ValueError('A private canary cannot bypass an operator drain')
    return {**original,'PATH':'/usr/bin:/bin','HOME':'/root','LANG':'C.UTF-8',
        'PYTHONSAFEPATH':'1','PYTHONPATH':str(source),'PYTHONDONTWRITEBYTECODE':'1',
        'UVICORN_APP_DIR':str(source),'FRONTEND_DIR':str(source/'frontend'),
        'JSON_SCHEMA_ENABLED':'true','BATCH_SUBMISSION_ENABLED':'true','DB_POOL_MIN':'1','DB_POOL_MAX':'2',
        # Existing maintainers retain the original archive/retention policies.
        'BLOB_GC_ENABLED':'false','RESULT_ARCHIVE_ENABLED':'false'}


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run',required=True,type=Path)
    p.add_argument('--fixture-set',choices=['schema-contract-heldout-v1','workflow-contract-heldout-v1'],default='schema-contract-heldout-v1')
    p.add_argument('--apply',action='store_true');args=p.parse_args()
    if not args.apply:
        print(json.dumps({'decoder_tasks':4,'agent_cases':20,'max_child_tasks':84,
            'primary_changes':False,'extra_gpu_worker':False,'private_api_lifetime_seconds':2700}));return
    def interrupted(_signum,_frame):raise InterruptedError('Finite schema qualification interrupted')
    signal.signal(signal.SIGTERM,interrupted);signal.signal(signal.SIGINT,interrupted)
    root=Path('/root/batchforge-runtime/app');base=args.run.resolve();source=Path.cwd()
    if (base.parent!=root.parent or not base.name.startswith('evidence-api-agent-schema-') or base.exists()
        or datetime.now(timezone.utc)>=datetime(2026,10,26,8,tzinfo=timezone.utc)):
        raise ValueError('New owned run and at least one hour recovery margin required')
    os.umask(0o077);base.mkdir(mode=0o700);start=time.monotonic();api=None;child=None
    report={'phase':'prepared','passed':False,'primary_changes':False,'azure_agent_migration':False,
        'private_api':ENDPOINT,'max_child_tasks':84,'extra_gpu_worker':False}
    def save():write(base/'validation.json',report)
    save()
    try:
        own=(root/'.runtime/agent-schema-canary.lock').open('a');fcntl.flock(own,fcntl.LOCK_EX|fcntl.LOCK_NB)
        campaign=(root/'.runtime/product-campaign.lock').open('a');fcntl.flock(campaign,fcntl.LOCK_EX|fcntl.LOCK_NB)
        receipt=json.loads((source.parent/'source-receipt.json').read_bytes())
        for path,sha in receipt['files'].items():
            if hashlib.sha256((source/path).read_bytes()).hexdigest()!=sha:raise RuntimeError('Immutable source changed')
        report['source_commit']=receipt['git_commit'];pins=Pins(root);report['primary_before']=dict(pins.identities);pins.check(full=True)
        apis=[s for s in pins.services if s['role']=='api']
        for service in apis:canary_environment(file_env(service['env_file']),source)
        env=canary_environment(file_env(apis[0]['env_file']),source)
        with socket.socket() as probe:probe.bind(('127.0.0.1',18172))
        command=['/usr/bin/timeout','--signal=TERM','--kill-after=15','2700',sys.executable,'-m','uvicorn',
            'backend.app:app','--app-dir',str(source),'--host','127.0.0.1','--port','18172','--no-access-log']
        with (base/'api-private.log').open('xb') as log:
            api=subprocess.Popen(command,cwd=root,env=env,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        report['api_timeout_identity']=process_identity(api.pid);save()
        until=time.monotonic()+45
        while time.monotonic()<until:
            if api.poll() is not None:raise RuntimeError('Private API stopped at startup')
            try:
                ready=httpx.get(ENDPOINT+'/readyz',timeout=2,trust_env=False).status_code==200
            except httpx.TransportError:ready=False
            if ready:break
            time.sleep(.5)
        else:raise RuntimeError('Private API did not become ready')
        children=Path(f'/proc/{api.pid}/task/{api.pid}/children').read_text().split()
        if len(children)!=1:raise RuntimeError('Timeout must own exactly one API child')
        api_pid=int(children[0]);report['api_identity']=process_identity(api_pid)
        actual=dict(part.split(b'=',1) for part in Path(f'/proc/{api_pid}/environ').read_bytes().split(b'\0') if b'=' in part)
        for name in ('PYTHONSAFEPATH','PYTHONPATH','JSON_SCHEMA_ENABLED','DATABASE_URL','MODEL_NAME','BATCH_SUBMISSION_ENABLED'):
            if actual.get(name.encode())!=env.get(name,'true' if name=='BATCH_SUBMISSION_ENABLED' else '').encode():
                raise RuntimeError('Canary process environment differs')
        report['private_api_ready_and_source_bound']=True;pins.check(full=True)
        # Children take the same campaign lock independently. The private API
        # does not run a worker or a second archive/GC loop.
        fcntl.flock(campaign,fcntl.LOCK_UN)
        suffix=base.name.removeprefix('evidence-api-agent-schema-')
        steps=[('decoder','ops.qualify_agent_grammar',root.parent/('soak480-agent-runtime-grammar-'+suffix),[],330),
               ('agent','ops.qualify_agent_runtime',root.parent/('soak480-agent-runtime-schema-'+suffix),
                ['--fixture-set',args.fixture_set],1900)]
        report['children']=[]
        for label,module,run,extra,bound in steps:
            if run.exists():raise RuntimeError('Qualification directory already exists')
            report['phase']=label+'_running';save()
            with (base/(label+'-private.log')).open('xb') as log:
                child=subprocess.Popen([sys.executable,'-m',module,'--run',str(run),'--apply','--batch-url',ENDPOINT,*extra],
                    cwd=source,env={**env,'JSON_SCHEMA_ENABLED':'true'},stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
            report['child_identity']=process_identity(child.pid);save()
            child.wait(timeout=bound)
            result=json.loads((run/'report.json').read_bytes())
            report['children'].append({'kind':label,'run':str(run),'phase':result['phase'],'passed':result['passed'],
                'report_sha256':hashlib.sha256((run/'report.json').read_bytes()).hexdigest()})
            save()
            if child.returncode!=0 or not result['passed']:raise RuntimeError('Qualification failed; no automatic replay')
        pins.check(full=True);report['primary_after']=dict(pins.identities);report.update(phase='complete',passed=True)
    except BaseException as exc:
        report.update(phase_before_failure=report['phase'],phase='failed',passed=False,error_type=type(exc).__name__)
    finally:
        if child is not None and child.poll() is None:
            # Child has its own graceful cleanup; never signal an unrelated PID.
            with contextlib.suppress(ProcessLookupError):os.killpg(child.pid,signal.SIGTERM)
            try:child.wait(timeout=60)
            except subprocess.TimeoutExpired:report['child_cleanup_incomplete']=True
        if api is not None and api.poll() is None:
            with contextlib.suppress(ProcessLookupError):os.killpg(api.pid,signal.SIGTERM)
            try:api.wait(timeout=20)
            except subprocess.TimeoutExpired:
                with contextlib.suppress(ProcessLookupError):os.killpg(api.pid,signal.SIGKILL)
                api.wait(timeout=10)
        report['private_api_stopped']=api is None or api.poll() is not None
        report['controller_seconds']=time.monotonic()-start;save()


if __name__=='__main__':main()
