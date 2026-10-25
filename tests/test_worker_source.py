import hashlib
import json
from pathlib import Path
import subprocess
import sys

import pytest
from ops.worker_source import PROBED_SOURCES,source_environment,probe_source


def environment():
    return {'MODEL_NAME':'test-model','WORKER_ID':'existing-worker','BATCHFORGE_WORKER_TOKEN':'a'*32,
        'WORKER_MAX_INFLIGHT':'8','WORKER_TOKEN_BUDGET':'16384','ENGINE_MAX_MODEL_LEN':'2048',
        'WORKER_ADAPTIVE_KV_ENABLED':'false','SCHEDULER_PROFILE_JSON':'',
        'CONTROL_PLANE_URL':'http://127.0.0.1:1','INFERENCE_BASE_URLS':'http://127.0.0.1:1',
        'PRIVATE_TEST_SENTINEL':'must-not-be-returned'}


def test_source_environment_only_changes_pointer_and_explicit_inventory_configuration(tmp_path):
    original=environment();copy=original.copy()
    env=source_environment(original,tmp_path,node_id='explicit-node',gpu_indices=(0,1,2,3))
    assert original==copy and all(env[k]==v for k,v in original.items())
    assert env['PYTHONSAFEPATH']=='1' and env['PYTHONPATH']==str(tmp_path)
    assert env['WORKER_INVENTORY_GPU_INDICES']=='0,1,2,3'
    assert set(env)-set(original)=={'PYTHONPATH','PYTHONSAFEPATH','WORKER_INVENTORY_NODE_ID','WORKER_INVENTORY_GPU_INDICES'}


@pytest.mark.parametrize('source,node,indices',[
    ('relative','node',(0,)),('/workspace/source','node',(0,)),('/approved:other','node',(0,)),
    ('/approved','',(0,)),('/approved','node',(0,0)),('/approved','node',(True,)),('/approved','node',()),
])
def test_ambiguous_source_or_unbounded_device_selection_is_rejected(source,node,indices):
    with pytest.raises(ValueError):source_environment(environment(),source,node_id=node,gpu_indices=indices)


def test_a_source_rollout_cannot_accidentally_create_a_new_worker_identity(tmp_path):
    env=environment();del env['WORKER_ID']
    with pytest.raises(ValueError,match='explicit worker'):source_environment(env,tmp_path,node_id='node',gpu_indices=(0,))


def package(path,label):
    p=path/'worker';p.mkdir(parents=True)
    (p/'__init__.py').write_text('')
    (p/'__main__.py').write_text('import json,sys; print(json.dumps({"origin":'+repr(label)+',"safe_path":sys.flags.safe_path}))')


def test_actual_minus_m_resolution_proves_pythonpath_alone_keeps_the_old_cwd_package(tmp_path):
    cwd=tmp_path/'live-root';candidate=tmp_path/'candidate';package(cwd,'old');package(candidate,'candidate')
    env={'PYTHONPATH':str(candidate)}
    command=[sys.executable,'-m','worker']
    old=json.loads(subprocess.check_output(command,cwd=cwd,env=env))
    new=json.loads(subprocess.check_output(command,cwd=cwd,env={**env,'PYTHONSAFEPATH':'1'}))
    assert old=={'origin':'old','safe_path':False}
    assert new=={'origin':'candidate','safe_path':True}


def test_real_worker_definitions_are_probed_without_registration_ports_or_inference(tmp_path):
    source=Path(__file__).resolve().parents[1]
    shadow=tmp_path/'worker';shadow.mkdir()
    (shadow/'__init__.py').write_text('raise RuntimeError("must-not-be-returned")')
    manifest={name:hashlib.sha256((source/name).read_bytes()).hexdigest() for name in PROBED_SOURCES}
    env=source_environment(environment(),source,node_id='explicit-node',gpu_indices=(0,1,2,3))
    result=probe_source(sys.executable,source,env,tmp_path,manifest)
    assert result['passed'] and result['worker_id']=='existing-worker' and result['slots']==8
    assert result['registration_performed'] is result['inference_performed'] is result['services_changed'] is False
    assert 'must-not-be-returned' not in json.dumps(result)
    assert result['source_files']==manifest
    unsafe={k:v for k,v in env.items() if k!='PYTHONSAFEPATH'}
    with pytest.raises(RuntimeError,match='origin'):
        probe_source(sys.executable,source,unsafe,tmp_path,manifest)
    manifest['worker/runtime.py']='f'*64
    with pytest.raises(RuntimeError,match='receipt'):
        probe_source(sys.executable,source,env,tmp_path,manifest)
