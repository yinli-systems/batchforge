"""Finite read-only console qualification over an existing closed real cohort.

Only the preserved isolated native PG and one extra loopback API start. Both
have independent OS lifetimes. Primary services, Azure data and GPU stay intact.
"""
from __future__ import annotations
import argparse,contextlib,fcntl,hashlib,json,os,pwd,signal,socket,subprocess,sys,time
from datetime import datetime,timezone
from pathlib import Path
import httpx,psycopg
from psycopg.conninfo import conninfo_to_dict
from ops.qualify_agent_runtime import write
from ops.worker_latch_guard import Pins
from agents.migrate import CHECKSUM
from ops.supervise import private_read


def require_closed(report):
    if (report.get('phase')!='complete' or report.get('passed') is not True or not report.get('isolated_pg_stopped')
        or report.get('plan',{}).get('fixture_set')!='workflow-contract-heldout-v1'
        or len(report.get('task_results',[]))!=20 or not all(r['passed'] for r in report['task_results'])
        or len(report.get('child_receipts',[]))!=44 or len(report.get('artifact_readbacks',[]))!=88):
        raise ValueError('Closed real ordered-tool cohort required; no replay')


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run',required=True,type=Path);p.add_argument('--cohort',required=True,type=Path)
    p.add_argument('--apply',action='store_true');args=p.parse_args()
    if not args.apply:print(json.dumps({'new_gpu_tasks':0,'production_mutations':False,'read_only_native_pg':True,'browser_visual_qa':False}));return
    def interrupted(_signal,_frame):raise InterruptedError('Read-only qualification interrupted')
    signal.signal(signal.SIGTERM,interrupted);signal.signal(signal.SIGINT,interrupted)
    root=Path('/root/batchforge-runtime/app');base=args.run.resolve();cohort=args.cohort.resolve();source=Path.cwd()
    if (base.parent!=root.parent or not base.name.startswith('evidence-api-agent-console-') or base.exists()
        or cohort.parent!=root.parent or not cohort.name.startswith('soak480-agent-runtime-')
        or datetime.now(timezone.utc)>=datetime(2026,10,26,8,30,tzinfo=timezone.utc)):
        raise ValueError('New owned qualification and bounded existing-cohort readback required')
    os.umask(0o077);base.mkdir(mode=0o700);children=[];report={'phase':'prepared','passed':False,'new_gpu_tasks':0,'primary_services_changed':False,'production_database_mutated':False}
    def save():write(base/'validation.json',report)
    save()
    try:
        lock=(root/'.runtime/product-campaign.lock').open('a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        prior=json.loads((cohort/'report.json').read_bytes());require_closed(prior)
        manifest=json.loads((source.parent/'source-receipt.json').read_bytes())
        for file,sha in manifest['files'].items():
            if hashlib.sha256((source/file).read_bytes()).hexdigest()!=sha:raise RuntimeError('Immutable console source changed')
        if manifest['files']['agents/schema.sql']!=CHECKSUM:raise RuntimeError('Agent SQL checksum changed')
        pins=Pins(root);pins.check(full=True);report['primary_before']=dict(pins.identities);report['source_commit']=manifest['git_commit']
        stage=Path('/tmp')/('bf-'+cohort.name);cfg=json.loads(private_read(cohort/'.runtime/config.json'));dbinfo=conninfo_to_dict(cfg['state_dsn']);account=pwd.getpwnam('nobody')
        if (stage.is_symlink() or not stage.is_dir() or stage.stat().st_uid!=account.pw_uid
            or (stage/'data').is_symlink() or (stage/'data').stat().st_uid!=account.pw_uid
            or (stage/'socket').is_symlink() or (stage/'data/postmaster.pid').exists()
            or (stage/'data/PG_VERSION').read_text().strip()!='17' or dbinfo.get('host')!=str(stage/'socket')
            or dbinfo.get('port')!='55484' or dbinfo.get('user')!='agent_canary' or dbinfo.get('dbname')!='postgres' or dbinfo.get('password')):
            raise RuntimeError('Refuse a live or unqualified control database')
        with socket.socket() as probe:probe.bind(('127.0.0.1',18174))
        toolchain=Path('/tmp/bf-ci-20261025-v19/toolchain')
        def drop():os.setgroups([]);os.setgid(account.pw_gid);os.setuid(account.pw_uid)
        def spawn(command,env,cwd,*,demote=False,label):
            with (base/(label+'-private.log')).open('xb') as output:
                child=subprocess.Popen(command,env=env,cwd=cwd,preexec_fn=drop if demote else None,stdin=subprocess.DEVNULL,stdout=output,stderr=subprocess.STDOUT,start_new_session=True)
            children.append((child,os.pidfd_open(child.pid)));report.setdefault('owned_children',[]).append({'label':label,'pid':child.pid});return child
        pg_env={'PATH':'/usr/bin:/bin','HOME':str(stage),'LANG':'C','LC_ALL':'C','LD_LIBRARY_PATH':str(toolchain/'usr/lib/x86_64-linux-gnu')}
        pg=spawn(['/usr/bin/timeout','--signal=INT','--kill-after=15','900',str(toolchain/'usr/lib/postgresql/17/bin/postgres'),
            '-D',str(stage/'data'),'-h','','-p','55484','-k',str(stage/'socket'),'-c','default_transaction_read_only=on'],pg_env,stage,demote=True,label='postgres')
        until=time.monotonic()+30
        while True:
            if pg.poll() is not None:raise RuntimeError('Native read-only database stopped')
            try:
                with psycopg.connect(cfg['state_dsn'],connect_timeout=2,options='-c default_transaction_read_only=on') as db:
                    assert db.execute('SHOW transaction_read_only').fetchone()[0]=='on'
                break
            except psycopg.OperationalError:
                if time.monotonic()>until:raise
                time.sleep(.2)
        env={'PATH':'/usr/bin:/bin','HOME':'/root','LANG':'C.UTF-8','PYTHONPATH':str(source),'PYTHONSAFEPATH':'1','PYTHONDONTWRITEBYTECODE':'1',
            'DATABASE_URL':cfg['state_dsn'],'MODEL_NAME':prior['child_receipts'][0]['receipt']['model'],'JSON_SCHEMA_ENABLED':'true','BATCH_SUBMISSION_ENABLED':'false','CUDA_VISIBLE_DEVICES':''}
        api=spawn(['/usr/bin/timeout','--signal=TERM','--kill-after=15','900',sys.executable,'-m','agents','api','--executor-keys',str(cohort/'.runtime/executors.json'),
                   '--batch-url','http://127.0.0.1:8080','--port','18174','--read-only'],env,source,label='api')
        until=time.monotonic()+30
        while time.monotonic()<until:
            if api.poll() is not None:raise RuntimeError('Read-only console API stopped')
            try:ready=httpx.get('http://127.0.0.1:18174/readyz',timeout=2,trust_env=False).status_code==200
            except httpx.TransportError:ready=False
            if ready:break
            time.sleep(.25)
        else:raise RuntimeError('Read-only console API unavailable')
        report['phase']='client_verification';save()
        node=Path('/tmp/bf-ci-20261025-v19/runner/externals/node24/bin/node')
        verifier=spawn([str(node),str(source/'ops/verify_agent_console.mjs'),str(source),str(cohort),'http://127.0.0.1:18174',str(base/'readback.json')],
                       {'PATH':'/usr/bin:/bin','HOME':'/root'},source,label='node')
        verifier.wait(timeout=600);validation=json.loads((base/'readback.json').read_bytes());report['readback']=validation
        if verifier.returncode or validation.get('passed') is not True:raise RuntimeError('Real console readback failed; preserve this run')
        pins.check(full=True);report['primary_after']=dict(pins.identities);report.update(phase='complete',passed=True)
    except BaseException as exc:report.update(phase_before_failure=report['phase'],phase='failed',passed=False,error_type=type(exc).__name__)
    finally:
        errors=[]
        for child,fd in reversed(children):
            if child.poll() is None:
                with contextlib.suppress(ProcessLookupError):signal.pidfd_send_signal(fd,signal.SIGTERM)
                try:child.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    with contextlib.suppress(ProcessLookupError):signal.pidfd_send_signal(fd,signal.SIGINT)
                    try:child.wait(timeout=15)
                    except subprocess.TimeoutExpired:errors.append('owned_child_still_exiting')
            os.close(fd)
        report['cleanup_errors']=errors;report['owned_children_stopped']=all(child.poll() is not None for child,_ in children)
        if errors:report.update(phase='failed',passed=False)
        report['closed_at']=datetime.now(timezone.utc).isoformat();save()


if __name__=='__main__':main()
