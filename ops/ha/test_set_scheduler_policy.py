from pathlib import Path
import pytest
from ops.ha.set_scheduler_policy import PolicyRoll,atomic_bytes,policy_bytes
from ops.supervise import private_write


def test_only_policy_assignment_changes_and_duplicate_is_rejected():
    source=b"# keep\nMODEL_NAME='openai/gpt-oss-120b'\nSCHEDULER_POLICY=cost_aware\nRESULT_READERS_ENABLED=true\nRESULT_ARCHIVE_ENABLED=false\nSECRET='literal $() and spaces'\n"
    changed=policy_bytes(source,'fair')
    assert changed==source.replace(b'SCHEDULER_POLICY=cost_aware',b'SCHEDULER_POLICY=fair')
    with pytest.raises(ValueError):policy_bytes(source+b'SCHEDULER_POLICY=fair\n','fair')


class Manager:
    def __init__(self,fail=None):self.calls=[];self.fail=fail
    def invoke(self,service,action):
        self.calls.append((service['role'],service['name'],action))
        if self.fail==(service['name'],action):self.fail=None;return None,'injected'
        return {'status':'stopped' if action=='stop' else 'started','running':True,'pid':123},None
    def readiness(self,service):return True


def prepared(tmp_path,fail=None):
    runtime=tmp_path/'.runtime';runtime.mkdir()
    services=[]
    for role,name,port in [('api','a',8082),('api','b',8083),('worker','gptoss120',None),('engine','gptoss120',8300),('gateway','edge',8080)]:
        env=runtime/f'{role}-{name}.env';atomic_bytes(env,b'SCHEDULER_POLICY=cost_aware\nRESULT_READERS_ENABLED=true\nWORKER_MAX_INFLIGHT=64\n')
        services.append(dict(role=role,name=name,port=port,env_file=str(env)))
    manifest=runtime/'services.json';private_write(manifest,{'services':services})
    manager=Manager(fail)
    roll=PolicyRoll(root=tmp_path,services=services,watchdog=dict(role='supervisor',name='primary'),
        manifest_path=manifest,report_path=runtime/'report.json',policy='fair',manager=manager,
        identity=lambda s:dict(pid=123,start_ticks='456',cwd=str(tmp_path)),
        assert_drained=lambda:None,verify_policy=lambda s:None)
    return roll,manager


def test_only_apis_roll_and_watchdog_restored(tmp_path):
    roll,manager=prepared(tmp_path)
    assert roll.apply()['phase']=='complete'
    assert all(action=='status' or role in {'api','supervisor'} for role,_,action in manager.calls)
    assert manager.calls[-1]==('supervisor','primary','start')
    assert all(Path(s['env_file']).read_bytes().endswith(b'WORKER_MAX_INFLIGHT=64\n') for s in roll.services)


@pytest.mark.parametrize('failure',[('primary','stop'),('b','start')])
def test_failure_restores_watchdog_and_original_api_envs(tmp_path,failure):
    roll,manager=prepared(tmp_path,failure)
    with pytest.raises(RuntimeError):roll.apply()
    assert roll.report['phase']=='rolled_back' and roll.report['watchdog_restored']
    assert manager.calls[-1]==('supervisor','primary','start')
    assert all(path.read_bytes()==original for path,original in roll.original.items())


def test_active_tasks_refuse_before_any_stop(tmp_path):
    roll,manager=prepared(tmp_path)
    def active():raise RuntimeError('active')
    roll.assert_drained=active
    with pytest.raises(RuntimeError):roll.apply()
    assert not manager.calls
