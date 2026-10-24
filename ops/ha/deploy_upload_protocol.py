"""Prepare, then explicitly apply one gateway/upload-runner protocol bundle.

Only the gateway is rolled. The runner source is copied passively. A missing
gateway diagnostic token is appended from API-a in RAM; no secret is reported.
Uncertain or foreign process/source state requires manual recovery.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack
import fcntl
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shlex
import stat
import tarfile
import time

import httpx
import psycopg

from ops.ha.adaptive_qwen235b import identity, private_operation_directory
from ops.ha.deploy_adaptive_worker_source import atomic_source, source_state
from ops.ha.finalize_api_policy import values
from ops.ha.history_maintenance import private_file
from ops.ha.set_scheduler_policy import PolicyManager, atomic_bytes, timestamp
from ops.supervise import ROOT, load_manifest, private_write

PATHS=frozenset({'ops/gateway/worker_gateway.py','scripts/sustained_benchmark.py'})
DEPENDENCIES=('ops/ha/deploy_upload_protocol.py','ops/manage.py','ops/supervise.py',
 'ops/ha/adaptive_qwen235b.py','ops/ha/deploy_adaptive_worker_source.py',
 'ops/ha/set_scheduler_policy.py','ops/ha/finalize_api_policy.py','ops/ha/history_maintenance.py')
TOKEN='BATCHFORGE_WORKER_TOKEN'


def digest(raw):return hashlib.sha256(raw).hexdigest()


def archive_sources(path,expected):
    if not re.fullmatch('[0-9a-f]{64}',expected):raise ValueError('Exact bundle SHA256 required')
    raw=private_file(path,2*1024**2)
    if digest(raw)!=expected:raise ValueError('Bundle hash differs')
    sources={}
    with tarfile.open(fileobj=io.BytesIO(raw),mode='r:*') as archive:
        for member in archive:
            if member.name not in PATHS or member.name in sources or not member.isreg() or not 0<member.size<=512*1024:
                raise ValueError('Exactly two regular reviewed source paths required')
            content=archive.extractfile(member).read(512*1024+1)
            if len(content)!=member.size:raise ValueError('Bundle size differs')
            compile(content,member.name,'exec',dont_inherit=True);sources[member.name]=content
    if set(sources)!=PATHS:raise ValueError('Incomplete protocol bundle')
    return sources


def validation_binding(report_path,manifest_path,sources):
    raw=private_file(report_path,2*1024**2);manifest_raw=private_file(manifest_path,2*1024**2)
    report=json.loads(raw);manifest=json.loads(manifest_raw)
    required=('passed','temporary_cluster_stopped','temporary_cluster_removed','unix_socket_only',
        'source_files_unchanged_after_tests','import_origins_verified_inside_snapshot')
    if (any(report.get(key) is not True for key in required) or report.get('phase')!='complete'
        or not str(report.get('server_version','')).startswith('17.')
        or report.get('remaining_scoped_test_processes')!=[]
        or report.get('pytest',{}).get('exit_status')!=0 or report.get('pytest',{}).get('skips')!=[]
        or not report.get('pytest',{}).get('passed',0)
        or report.get('production_database_accessed') is not False
        or report.get('production_credentials_inherited') is not False
        or report.get('source_database_url_supplied') is not False
        or type(report.get('server_os_uid')) is not int or report['server_os_uid']<=0
        or report.get('source_manifest_sha256')!=digest(manifest_raw)
        or report.get('source_tree_sha256')!=manifest.get('source_tree_sha256')
        or report.get('commit')!=manifest.get('commit')
        or not re.fullmatch('[0-9a-f]{40}',manifest.get('commit',''))
        or manifest.get('contains_application_configuration') is not False
        or any(manifest.get('files',{}).get(name)!=digest(raw) for name,raw in sources.items())):
        raise ValueError('Passed cleaned-up PostgreSQL17 validation of exact bundle required')
    files=manifest['files']
    if digest(json.dumps(files,sort_keys=True,separators=(',',':')).encode())!=manifest['source_tree_sha256']:
        raise ValueError('Validation manifest tree hash differs')
    return {'report_sha256':digest(raw),'manifest_sha256':digest(manifest_raw),'commit':report['commit']}


def token_bytes(raw,api_raw):
    token=values(api_raw).get(TOKEN,'')
    if not 24<=len(token)<=4096 or '\n' in token or '\r' in token or '\0' in token:
        raise ValueError('Existing bounded API worker token required')
    assignments=[line for line in raw.decode().splitlines() if re.match(r'^\s*(?:export\s+)?'+TOKEN+r'\s*=',line)]
    if len(assignments)>1:raise ValueError('Duplicate gateway worker token')
    if assignments:
        if values(raw).get(TOKEN)!=token:raise ValueError('Existing gateway token differs; no rotation authorized')
        return raw,token
    return raw+(b'' if not raw or raw.endswith(b'\n') else b'\n')+(TOKEN+'='+shlex.quote(token)+'\n').encode(),token


def scoped_caches(root,remove=False):
    entries=[]
    for name in sorted(PATHS):
        source=root/name;directory=source.parent/'__pycache__'
        if not directory.exists() and not directory.is_symlink():continue
        info=directory.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid!=os.geteuid() or stat.S_IMODE(info.st_mode)&0o7022:
            raise ValueError('Unsafe scoped bytecode directory')
        pattern=re.compile(re.escape(source.stem)+r'\.[A-Za-z0-9_-]+(?:\.opt-[0-9]+)?\.pyc\Z')
        for path in directory.iterdir():
            if not pattern.fullmatch(path.name):continue
            info=path.lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_uid!=os.geteuid() or stat.S_IMODE(info.st_mode)&0o7022:
                raise ValueError('Unsafe scoped bytecode entry')
            entries.append((path,info.st_dev,info.st_ino))
    if remove:
        for path,device,inode in entries:
            info=path.lstat()
            if (info.st_dev,info.st_ino)!=(device,inode):raise RuntimeError('Scoped bytecode changed')
            path.unlink()
        for directory in {path.parent for path,_,_ in entries}:
            fd=os.open(directory,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
            try:os.fsync(fd)
            finally:os.close(fd)
    return [str(path.relative_to(root)) for path,_,_ in entries]


class ProtocolDeployment:
    def __init__(self,*,root,run,manifest,services,sources,archive_sha,validation,
                 manager=None,identity_reader=None,drained=None,protocol=None,effective=None,writer=atomic_source):
        if set(sources)!=PATHS:raise ValueError('Exactly two sources required')
        self.root,self.run,self.manifest,self.services,self.sources=root,run,manifest,services,sources
        expected={('api','a'),('api','b'),('gateway','edge'),('worker','qwen235b'),('engine','qwen235b')}
        if {(s['role'],s['name']) for s in services}!=expected or len(services)!=5:
            raise ValueError('Exact current five managed services plus watchdog required')
        self.gateway=next(s for s in services if s['role']=='gateway')
        self.api=next(s for s in services if s['role']=='api' and s['name']=='a')
        if self.gateway.get('port')!=8080 or sorted((s['name'],s.get('port')) for s in services if s['role']=='api')!=[('a',8082),('b',8083)]:
            raise ValueError('Exact current gateway/API ports required')
        self.watchdog={'role':'supervisor','name':'primary','env_file':str(root.parent/'supervisor.env'),
            'python':str(root.parent/'venv/bin/python')}
        self.all=[*services,self.watchdog];self.manager=manager or PolicyManager(root=root,timeout=30)
        self.identity=identity_reader or (lambda s:identity(root,s));self.drained=drained or self._drained
        self.protocol=protocol or self._protocol;self.effective=effective or self._effective;self.writer=writer
        self.original_env={str(Path(s['env_file'])):private_file(Path(s['env_file'])) for s in self.all}
        self.gateway_env=str(Path(self.gateway['env_file']))
        self.patched_env,self.token=token_bytes(self.original_env[self.gateway_env],self.original_env[str(Path(self.api['env_file']))])
        self.expected_env=dict(self.original_env)
        self.manifest_raw=private_file(manifest);self.ids={self.key(s):self.identity(s) for s in self.all}
        if not all(self.ids.values()):raise RuntimeError('All six owned services must be live')
        self.expected_ids=dict(self.ids);self.states={};self.original_bytes={}
        for name in PATHS:
            parent=(root/name).parent
            if parent.resolve()!=parent:raise ValueError('Source parent cannot traverse symlinks')
            self.states[name],self.original_bytes[name]=source_state(root/name)
            if not self.states[name]['exists']:raise ValueError('Existing protocol sources required')
            compile(sources[name],name,'exec',dont_inherit=True)
        self.expected_states=dict(self.states)
        self.dependencies={name:digest((root/name).read_bytes()) for name in DEPENDENCIES}
        binding={'archive_sha256':archive_sha,'sources':{name:digest(raw) for name,raw in sources.items()},
            'original_sources':self.states,'manifest_sha256':digest(self.manifest_raw),
            'environment_sha256':{path:digest(raw) for path,raw in self.original_env.items()},
            'identities':self.ids,'dependencies':self.dependencies,'validation':validation}
        self.report_path=run/'report.json'
        if self.report_path.exists():
            previous=json.loads(private_file(self.report_path,1024**2))
            if previous.get('phase')!='prepared' or previous.get('binding')!=binding:
                raise RuntimeError('Prepared operation changed or uncertain; no automatic replay')
            self.report=previous
            for name in PATHS:
                if private_file(run/'original'/name,512*1024)!=self.original_bytes[name] or private_file(run/'staged'/name,512*1024)!=sources[name]:
                    raise RuntimeError('Private source snapshot changed')
            if private_file(run/'gateway.env.original')!=self.original_env[self.gateway_env] or private_file(run/'gateway.env.staged')!=self.patched_env:
                raise RuntimeError('Private environment snapshot changed')
        else:
            if any(run.iterdir()):raise RuntimeError('Incomplete operation requires manual inspection')
            self.report={'phase':'prepared','prepared_at':timestamp(),'binding':binding,'events':[],
                'scope':'Only gateway roll; passive runner copy; no database mutation/inference restart',
                'changed_environment_keys':[TOKEN] if self.patched_env!=self.original_env[self.gateway_env] else []}
            for group,payloads in (('original',self.original_bytes),('staged',sources)):
                for name,raw in payloads.items():
                    target=run/group/name;target.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
                    atomic_source(target,raw,0o600,time.time_ns())
            atomic_bytes(run/'gateway.env.original',self.original_env[self.gateway_env]);atomic_bytes(run/'gateway.env.staged',self.patched_env)
            self.save()

    @staticmethod
    def key(service):return service['role']+'-'+service['name']

    def save(self):
        private_write(self.report_path,self.report)
        fd=os.open(self.run,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        try:os.fsync(fd)
        finally:os.close(fd)

    def guard(self):
        if private_file(self.manifest)!=self.manifest_raw:raise RuntimeError('Manifest drift')
        if any(digest((self.root/name).read_bytes())!=value for name,value in self.dependencies.items()):raise RuntimeError('Controller dependency drift')
        if any(private_file(Path(path))!=raw for path,raw in self.expected_env.items()):raise RuntimeError('Environment drift')
        if {self.key(s):self.identity(s) for s in self.all}!=self.expected_ids:raise RuntimeError('Foreign or unexpected process identity')
        if any(source_state(self.root/name)[0]!=state for name,state in self.expected_states.items()):raise RuntimeError('Foreign source drift')

    def _drained(self):
        dsn=values(self.original_env[str(Path(self.api['env_file']))])['DATABASE_URL']
        with psycopg.connect(dsn,connect_timeout=5,options='-c default_transaction_read_only=on -c statement_timeout=5000') as conn:
            if conn.execute("SELECT count(*) FROM tasks WHERE status IN ('pending','running')").fetchone()[0]:raise RuntimeError('Global tasks must be drained')

    def _effective(self,patched):
        expected=self.expected_ids[self.key(self.gateway)]
        if expected is None or self.identity(self.gateway)!=expected:raise RuntimeError('Gateway identity changed before token check')
        pid=expected['pid']
        raw=Path(f'/proc/{pid}/environ').read_bytes().split(b'\0')
        if self.identity(self.gateway)!=expected:raise RuntimeError('Gateway identity changed during token check')
        found=[part.partition(b'=')[2].decode() for part in raw if part.startswith((TOKEN+'=').encode())]
        wanted=values(self.expected_env[self.gateway_env]).get(TOKEN)
        if found!=([wanted] if wanted is not None else []):raise RuntimeError('Effective gateway token mismatch')

    def _protocol(self):
        with httpx.Client(base_url='http://127.0.0.1:8080',trust_env=False,follow_redirects=False,timeout=3) as client:
            unauthorized=client.get('/internal/gateway/status')
            response=client.get('/internal/gateway/status',headers={'Authorization':'Bearer '+self.token})
            data=response.json()
            if (unauthorized.status_code!=401 or response.status_code!=200 or data.get('component')!='replica-router'
                or data.get('diagnostic_version')!=1 or data.get('ready_replicas')!=2
                or data.get('configured_replicas')!=2 or data.get('mutation_replays')!=0
                or not isinstance(data.get('outcome_events'),list)):
                raise RuntimeError('New authenticated gateway protocol not ready')

    def event(self,service,action):
        self.guard();key=self.key(service);self.report['pending_action']={'service':key,'action':action};self.save()
        result,error=self.manager.invoke(service,action)
        if error:raise RuntimeError('Managed action outcome unknown')
        current=self.identity(service)
        if action=='stop':
            if result.get('status') not in ('stopped','not_running') or current is not None:raise RuntimeError('Stop not confirmed')
        elif result.get('status')!='started' or current is None or current['pid']!=result.get('pid'):
            raise RuntimeError('Start identity not confirmed')
        self.expected_ids[key]=current;self.report['events'].append({'at':timestamp(),'service':key,'action':action,'identity':current})
        self.report['pending_action']=None;self.save();self.guard()

    def healthy(self,patched):
        deadline=time.monotonic()+45
        while True:
            self.guard()
            if all(self.manager.readiness(s) is True for s in self.services if s.get('port') is not None):
                try:
                    self.effective(patched)
                    if patched:self.protocol()
                    return
                except (httpx.HTTPError,ValueError,RuntimeError):
                    if time.monotonic()>=deadline:raise
            if time.monotonic()>=deadline:raise RuntimeError('Managed readiness timeout')
            time.sleep(.5)

    def apply(self):
        if self.report['phase']!='prepared':raise RuntimeError('Prepared operation required')
        self.guard();self.drained();scoped_caches(self.root)
        self.report.update(phase='applying',started_at=timestamp());self.save()
        try:
            self.event(self.watchdog,'stop');self.drained();self.event(self.gateway,'stop')
            for name in sorted(PATHS):
                self.guard();old=self.states[name];mtime=max(time.time_ns(),(old['mtime_ns']//10**9+1)*10**9)
                self.report['pending_action']={'source':name};self.save()
                self.writer(self.root/name,self.sources[name],old['mode'],mtime)
                state,_=source_state(self.root/name)
                if state['sha256']!=digest(self.sources[name]):raise RuntimeError('Installed source differs')
                self.expected_states[name]=state;self.report['pending_action']=None;self.save()
            self.guard();self.report['pending_action']={'environment':self.gateway_env};self.save()
            atomic_bytes(Path(self.gateway_env),self.patched_env);self.expected_env[self.gateway_env]=self.patched_env
            self.report['pending_action']=None;self.report['bytecode_evicted']=scoped_caches(self.root,True);self.save()
            self.event(self.gateway,'start');self.healthy(True);self.drained();self.guard()
            self.report.update(phase='gateway_verified',gateway_identity=self.expected_ids[self.key(self.gateway)]);self.save()
        except BaseException as error:
            self.report.update(phase='rolling_back',failure_type=type(error).__name__);self.save()
            try:
                if self.expected_ids[self.key(self.watchdog)] is not None:
                    raise RuntimeError('Watchdog stop not confirmed; no rollback mutation')
                # A writer may atomically commit and then raise. Reconcile only
                # exact old/reviewed bytes; never accept a foreign process.
                for name in PATHS:
                    state,_=source_state(self.root/name)
                    if state!=self.expected_states[name]:
                        if state.get('sha256') not in (self.states[name]['sha256'],digest(self.sources[name])):raise RuntimeError('Foreign source during rollback')
                        self.expected_states[name]=state
                current=private_file(Path(self.gateway_env))
                if current not in (self.original_env[self.gateway_env],self.patched_env):raise RuntimeError('Foreign environment during rollback')
                self.expected_env[self.gateway_env]=current;self.guard()
                if self.expected_ids[self.key(self.gateway)] is not None:self.event(self.gateway,'stop')
                for name in PATHS:
                    self.guard();old=self.states[name]
                    atomic_source(self.root/name,self.original_bytes[name],old['mode'],old['mtime_ns'])
                    self.expected_states[name]=source_state(self.root/name)[0]
                atomic_bytes(Path(self.gateway_env),self.original_env[self.gateway_env]);self.expected_env[self.gateway_env]=self.original_env[self.gateway_env]
                self.report['rollback_bytecode_evicted']=scoped_caches(self.root,True)
                self.event(self.gateway,'start');self.healthy(False);self.guard()
                self.report.update(phase='rolled_back',pending_action=None);self.save()
            except BaseException as rollback:
                self.report.update(phase='manual_recovery_required',rollback_failure_type=type(rollback).__name__);self.save()
            if self.report['phase']=='rolled_back':self.restore_watchdog()
            raise
        self.restore_watchdog()
        self.report.update(phase='complete',completed_at=timestamp(),protected_pids_env_manifest_unchanged=True,pending_action=None)
        self.save();return self.report

    def restore_watchdog(self):
        try:
            self.guard();self.healthy(self.report['phase']=='gateway_verified')
            if self.expected_ids[self.key(self.watchdog)] is None:self.event(self.watchdog,'start')
            self.guard();self.report['watchdog_restored']=True;self.save()
        except BaseException as error:
            self.report.update(phase='manual_recovery_required',watchdog_restored=False,watchdog_failure_type=type(error).__name__);self.save();raise


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive',type=Path,required=True);parser.add_argument('--sha256',required=True)
    parser.add_argument('--operation',required=True);parser.add_argument('--apply',action='store_true')
    parser.add_argument('--manifest',type=Path,default=ROOT/'.runtime/services-primary.json')
    parser.add_argument('--validation-report',type=Path,required=True);parser.add_argument('--validation-manifest',type=Path,required=True)
    args=parser.parse_args();os.umask(0o077)
    with ExitStack() as stack:
        fd=os.open(ROOT/'.runtime/upload-protocol.lock',os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
        stack.callback(os.close,fd);info=os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid!=os.geteuid() or info.st_mode&0o077 or info.st_nlink!=1:
            raise ValueError('Private owned deployment lock required')
        fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
        sources=archive_sources(args.archive,args.sha256)
        validation=validation_binding(args.validation_report,args.validation_manifest,sources)
        manifest,services,_=load_manifest(args.manifest)
        run=private_operation_directory(ROOT,args.operation)
        if args.apply and not (run/'report.json').exists():raise RuntimeError('Prepare before apply')
        controller=ProtocolDeployment(root=ROOT,run=run,manifest=manifest,services=services,sources=sources,
            archive_sha=args.sha256,validation=validation)
        if args.apply:controller.apply()
        print(json.dumps({'phase':controller.report['phase'],'report':str(controller.report_path)}))


if __name__=='__main__':
    try:main()
    except Exception as error:
        print(json.dumps({'error_type':type(error).__name__}),flush=True);raise SystemExit(1) from None
