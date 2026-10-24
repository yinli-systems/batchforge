import hashlib
import io
import json
import os
from pathlib import Path
import tarfile

import httpx
import pytest

from ops.ha import deploy_upload_protocol as deploy


def private(path,raw):
    path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(raw);path.chmod(0o600);return path


def bundle(tmp_path,extra=None):
    stream=io.BytesIO()
    with tarfile.open(fileobj=stream,mode='w:gz') as archive:
        rows=[(name,b'VALUE=2\n',tarfile.REGTYPE) for name in sorted(deploy.PATHS)]
        if extra:rows.append(extra)
        for name,raw,kind in rows:
            info=tarfile.TarInfo(name);info.size=len(raw);info.type=kind
            archive.addfile(info,io.BytesIO(raw))
    raw=stream.getvalue();return private(tmp_path/'bundle.tar.gz',raw),deploy.digest(raw)


def test_exact_bundle_compiles_and_hash_is_bound(tmp_path):
    path,digest=bundle(tmp_path)
    assert set(deploy.archive_sources(path,digest))==deploy.PATHS
    with pytest.raises(ValueError):deploy.archive_sources(path,'0'*64)


@pytest.mark.parametrize('name,kind',[('../escape.py',tarfile.REGTYPE),('other.py',tarfile.REGTYPE),
    ('ops/gateway/worker_gateway.py',tarfile.REGTYPE),('scripts/sustained_benchmark.py',tarfile.SYMTYPE)])
def test_bundle_rejects_traversal_extra_duplicate_and_link(tmp_path,name,kind):
    path,digest=bundle(tmp_path,(name,b'VALUE=2\n',kind))
    with pytest.raises(ValueError):deploy.archive_sources(path,digest)


def test_only_missing_token_is_appended_without_reformatting_or_secret_metadata():
    token='private-worker-token-'+('a'*32);raw=b'# untouched\r\nSOME_KEY="literal value"'
    patched,value=deploy.token_bytes(raw,('BATCHFORGE_WORKER_TOKEN='+token+'\n').encode())
    assert patched==raw+b'\n'+('BATCHFORGE_WORKER_TOKEN='+token+'\n').encode() and value==token
    assert deploy.token_bytes(patched,('BATCHFORGE_WORKER_TOKEN='+token).encode())[0]==patched
    with pytest.raises(ValueError):deploy.token_bytes(b'BATCHFORGE_WORKER_TOKEN=other\n',('BATCHFORGE_WORKER_TOKEN='+token).encode())
    with pytest.raises(ValueError):deploy.token_bytes(patched+patched,('BATCHFORGE_WORKER_TOKEN='+token).encode())


def validation_files(tmp_path,sources):
    files={name:deploy.digest(raw) for name,raw in sources.items()}
    tree=deploy.digest(json.dumps(files,sort_keys=True,separators=(',',':')).encode())
    manifest={'commit':'a'*40,'files':files,'source_tree_sha256':tree,'contains_application_configuration':False}
    path=private(tmp_path/'validation-manifest.json',json.dumps(manifest).encode())
    report=dict(phase='complete',passed=True,temporary_cluster_stopped=True,temporary_cluster_removed=True,
        unix_socket_only=True,source_files_unchanged_after_tests=True,import_origins_verified_inside_snapshot=True,
        server_version='17.11 (Ubuntu)',remaining_scoped_test_processes=[],pytest={'exit_status':0,'skips':[],'passed':100},
        production_database_accessed=False,production_credentials_inherited=False,source_database_url_supplied=False,
        server_os_uid=65534,source_manifest_sha256=deploy.digest(path.read_bytes()),source_tree_sha256=tree,commit='a'*40)
    return private(tmp_path/'validation-report.json',json.dumps(report).encode()),path,report


@pytest.mark.parametrize('field,value',[('passed',False),('temporary_cluster_removed',False),
    ('temporary_cluster_stopped',False),('server_version','16.2'),('remaining_scoped_test_processes',[{'pid':123}]),
    ('server_os_uid',0),('source_database_url_supplied',True),('source_manifest_sha256','0'*64)])
def test_validation_refuses_unclean_wrong_version_or_unbound_report(tmp_path,field,value):
    sources={name:b'VALUE=2\n' for name in deploy.PATHS}
    report,manifest,data=validation_files(tmp_path,sources)
    assert deploy.validation_binding(report,manifest,sources)['commit']=='a'*40
    data[field]=value;report.write_text(json.dumps(data))
    with pytest.raises(ValueError):deploy.validation_binding(report,manifest,sources)


@pytest.fixture
def setup(tmp_path):
    root=tmp_path/'app';root.mkdir();runtime=root/'.runtime';runtime.mkdir(mode=0o700)
    token='private-same-pod-worker-'+('z'*32);services=[]
    for index,(role,name,port) in enumerate([('api','a',8082),('api','b',8083),('gateway','edge',8080),
        ('engine','qwen235b',8000),('worker','qwen235b',8092)]):
        raw=b'SAFE_LITERAL="preserved"\n'
        if role=='api':raw+=('BATCHFORGE_WORKER_TOKEN='+token+'\nDATABASE_URL=not-used\n').encode()
        path=private(runtime/(role+'-'+name+'.env'),raw)
        services.append(dict(role=role,name=name,port=port,env_file=str(path),python='/private/python'))
    private(root.parent/'supervisor.env',b'SAFE_LITERAL=unchanged\n')
    manifest=private(runtime/'services-primary.json',json.dumps({'services':services}).encode())
    for name in deploy.PATHS:private(root/name,b'VALUE=1\n')
    for name in deploy.DEPENDENCIES:private(root/name,b'# fixture dependency\n')
    run=runtime/'protocol-op';run.mkdir(mode=0o700)
    ids={s['role']+'-'+s['name']:dict(pid=100+i,start_ticks=str(i),cwd=str(root)) for i,s in enumerate(services)}
    ids['supervisor-primary']=dict(pid=200,start_ticks='200',cwd=str(root));events=[]
    class Manager:
        next_pid=300
        def invoke(self,service,action):
            key=deploy.ProtocolDeployment.key(service);events.append((key,action))
            if action=='stop':ids[key]=None;return {'status':'stopped'},None
            self.next_pid+=1;ids[key]=dict(pid=self.next_pid,start_ticks=str(self.next_pid),cwd=str(root))
            return {'status':'started','pid':self.next_pid},None
        def readiness(self,service):return ids[deploy.ProtocolDeployment.key(service)] is not None
    manager=Manager();sources={name:b'VALUE=2\n' for name in deploy.PATHS}
    def make(**kwargs):
        return deploy.ProtocolDeployment(root=root,run=run,manifest=manifest,services=services,sources=sources,
            archive_sha='1'*64,validation={'report_sha256':'2'*64},manager=manager,
            identity_reader=lambda s:ids[deploy.ProtocolDeployment.key(s)],drained=lambda:None,
            protocol=lambda:None,effective=lambda patched:None,**kwargs)
    return root,run,ids,events,manager,make


def test_preparation_preserves_live_sources_services_and_private_token(setup):
    root,run,ids,events,manager,make=setup;before=dict(ids);controller=make()
    assert not events and ids==before and controller.report['phase']=='prepared'
    assert all((root/name).read_bytes()==b'VALUE=1\n' for name in deploy.PATHS)
    assert controller.token not in json.dumps(controller.report)
    assert (run/'gateway.env.staged').stat().st_mode&0o777==0o600
    assert make().report==controller.report


def test_success_rolls_only_gateway_and_watchdog_preserving_other_bytes(setup):
    root,run,ids,events,manager,make=setup;before=dict(ids);controller=make()
    originals=dict(controller.original_env);result=controller.apply()
    assert result['phase']=='complete' and result['watchdog_restored'] is True
    assert events==[('supervisor-primary','stop'),('gateway-edge','stop'),('gateway-edge','start'),('supervisor-primary','start')]
    assert all(ids[key]==value for key,value in before.items() if key not in ('gateway-edge','supervisor-primary'))
    assert all(Path(path).read_bytes()==raw for path,raw in originals.items() if path!=controller.gateway_env)
    assert all((root/name).read_bytes()==b'VALUE=2\n' for name in deploy.PATHS)
    assert controller.token not in json.dumps(result)
    with pytest.raises(RuntimeError):make()


@pytest.mark.parametrize('after_replace',[False,True])
def test_copy_failure_restores_exact_sources_env_then_known_healthy_watchdog(setup,after_replace):
    root,run,ids,events,manager,make=setup
    def writer(path,*args):
        if path.name=='sustained_benchmark.py':
            if after_replace:deploy.atomic_source(path,*args)
            raise OSError('write failed')
        deploy.atomic_source(path,*args)
    controller=make(writer=writer)
    with pytest.raises(OSError):controller.apply()
    assert controller.report['phase']=='rolled_back' and controller.report['watchdog_restored'] is True
    assert all((root/name).read_bytes()==b'VALUE=1\n' for name in deploy.PATHS)
    assert Path(controller.gateway_env).read_bytes()==controller.original_env[controller.gateway_env]
    assert ids['gateway-edge'] and ids['supervisor-primary']


def test_foreign_source_failure_never_restores_watchdog_or_overwrites_foreign_bytes(setup):
    root,run,ids,events,manager,make=setup
    def writer(path,*args):path.write_bytes(b'FOREIGN=1\n');raise OSError('foreign write')
    controller=make(writer=writer)
    with pytest.raises(OSError):controller.apply()
    assert controller.report['phase']=='manual_recovery_required'
    assert ids['supervisor-primary'] is None and ('supervisor-primary','start') not in events
    assert any((root/name).read_bytes()==b'FOREIGN=1\n' for name in deploy.PATHS)


def test_uncertain_gateway_start_is_manual_without_signalling_unconfirmed_process(setup):
    root,run,ids,events,manager,make=setup;original=manager.invoke
    def invoke(service,action):
        result=original(service,action)
        if service['role']=='gateway' and action=='start':return None,'timeout_after_start'
        return result
    manager.invoke=invoke;controller=make()
    with pytest.raises(RuntimeError):controller.apply()
    assert controller.report['phase']=='manual_recovery_required'
    assert events.count(('gateway-edge','stop'))==1 and ids['supervisor-primary'] is None


def test_failed_protocol_validation_restores_old_gateway_env_sources_before_watchdog(setup):
    root,run,ids,events,manager,make=setup;controller=make();healthy=controller.healthy
    def verify(patched):
        if patched:raise RuntimeError('new protocol not ready')
        return healthy(False)
    controller.healthy=verify
    with pytest.raises(RuntimeError,match='new protocol'):controller.apply()
    assert controller.report['phase']=='rolled_back' and controller.report['watchdog_restored']
    assert Path(controller.gateway_env).read_bytes()==controller.original_env[controller.gateway_env]
    assert all((root/name).read_bytes()==b'VALUE=1\n' for name in deploy.PATHS)
    assert events[-1]==('supervisor-primary','start')


def test_unconfirmed_watchdog_stop_does_not_touch_gateway(setup):
    root,run,ids,events,manager,make=setup;original=manager.invoke
    def invoke(service,action):
        if service['role']=='supervisor' and action=='stop':return None,'timeout_unknown'
        return original(service,action)
    manager.invoke=invoke;controller=make()
    with pytest.raises(RuntimeError):controller.apply()
    assert controller.report['phase']=='manual_recovery_required' and not events
    assert all((root/name).read_bytes()==b'VALUE=1\n' for name in deploy.PATHS)


@pytest.mark.parametrize('fault',[None,'unauthorized_allowed','one_replica','old_protocol'])
def test_protocol_verification_is_get_only_auth_denied_and_exact_two_ready(setup,monkeypatch,fault):
    root,run,ids,events,manager,make=setup;controller=make();calls=[];client_type=httpx.Client
    def upstream(request):
        calls.append(request)
        if not request.headers.get('Authorization'):
            return httpx.Response(200 if fault=='unauthorized_allowed' else 401,json={'error':'unauthorized'})
        assert request.headers['Authorization']=='Bearer '+controller.token
        return httpx.Response(200,json={'component':'replica-router','diagnostic_version':0 if fault=='old_protocol' else 1,
            'ready_replicas':1 if fault=='one_replica' else 2,'configured_replicas':2,'mutation_replays':0,'outcome_events':[]})
    monkeypatch.setattr(deploy.httpx,'Client',lambda **kwargs:client_type(**kwargs,transport=httpx.MockTransport(upstream)))
    if fault:
        with pytest.raises(RuntimeError):controller._protocol()
    else:controller._protocol()
    assert len(calls)==2 and all(r.method=='GET' and r.url.path=='/internal/gateway/status' for r in calls)


def test_effective_token_reads_only_owned_gateway_process_and_compares_in_ram(setup,monkeypatch):
    root,run,ids,events,manager,make=setup;controller=make();read_bytes=Path.read_bytes;seen=[]
    controller.expected_env[controller.gateway_env]=controller.patched_env
    wanted=Path('/proc/'+str(ids['gateway-edge']['pid'])+'/environ')
    def read(path):
        if path==wanted:seen.append(path);return b'OTHER=private\0'+('BATCHFORGE_WORKER_TOKEN='+controller.token).encode()+b'\0'
        return read_bytes(path)
    monkeypatch.setattr(Path,'read_bytes',read)
    controller._effective(True);assert seen==[wanted]
    controller.expected_env[controller.gateway_env]=controller.original_env[controller.gateway_env]
    with pytest.raises(RuntimeError):controller._effective(False)


@pytest.mark.parametrize('when',['before','during'])
def test_effective_token_rejects_identity_change_before_or_during_private_read(setup,monkeypatch,when):
    root,run,ids,events,manager,make=setup;controller=make();original_identity=controller.identity;seen=[]
    controller.expected_env[controller.gateway_env]=controller.patched_env
    changed={'value':when=='before'}
    def identity(service):
        result=original_identity(service)
        return {**result,'pid':999} if changed['value'] and service['role']=='gateway' else result
    controller.identity=identity
    original_read=Path.read_bytes;wanted=Path('/proc/'+str(ids['gateway-edge']['pid'])+'/environ')
    def read(path):
        if path==wanted:
            seen.append(path);changed['value']=True
            return ('BATCHFORGE_WORKER_TOKEN='+controller.token).encode()+b'\0'
        return original_read(path)
    monkeypatch.setattr(Path,'read_bytes',read)
    with pytest.raises(RuntimeError,match='identity changed'):controller._effective(True)
    assert len(seen)==(0 if when=='before' else 1)


@pytest.mark.parametrize('kind',['source','env','pid','manifest','dependency'])
def test_prepared_drift_rejected_before_any_service_action(setup,kind):
    root,run,ids,events,manager,make=setup;controller=make()
    if kind=='source':(root/'scripts/sustained_benchmark.py').write_bytes(b'FOREIGN=2\n')
    elif kind=='env':Path(controller.gateway_env).write_bytes(b'OTHER=2\n')
    elif kind=='pid':ids['engine-qwen235b']={**ids['engine-qwen235b'],'pid':999}
    elif kind=='manifest':controller.manifest.write_bytes(b'{}')
    else:(root/deploy.DEPENDENCIES[0]).write_bytes(b'# changed')
    with pytest.raises(RuntimeError):controller.apply()
    assert not events


def test_bytecode_eviction_is_scoped_and_rejects_symlinks_before_any_deletion(tmp_path):
    for name in deploy.PATHS:
        directory=(tmp_path/name).parent/'__pycache__';directory.mkdir(mode=0o700,parents=True)
        private(directory/(Path(name).stem+'.cpython-312.pyc'),b'cache')
        private(directory/'unrelated.cpython-312.pyc',b'preserved')
    directory=tmp_path/'ops/gateway/__pycache__';(directory/'worker_gateway.cpython-313.pyc').symlink_to('/tmp/no-touch')
    with pytest.raises(ValueError):deploy.scoped_caches(tmp_path,True)
    assert (directory/'worker_gateway.cpython-312.pyc').exists()
    (directory/'worker_gateway.cpython-313.pyc').unlink()
    assert len(deploy.scoped_caches(tmp_path,True))==2
    assert all((tmp_path/name).parent.joinpath('__pycache__/unrelated.cpython-312.pyc').exists() for name in deploy.PATHS)
