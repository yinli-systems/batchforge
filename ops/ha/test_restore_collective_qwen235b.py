import copy
import json
from pathlib import Path

import pytest

from ops.ha import restore_collective_qwen235b as module
from ops.ha.test_collective_qwen235b import prepared as candidate_prepared


def activated(tmp_path):
    activation, manager, _ = candidate_prepared(tmp_path)
    activation.apply()
    run = tmp_path / '.runtime/restore'; run.mkdir(mode=0o700)
    manager.calls.clear()
    kwargs = dict(root=tmp_path, run=run, activation_run=activation.run,
        activation_sha256=module.sha((activation.run / 'report.json').read_bytes()),
        manifest_path=activation.manifest_path, watchdog=activation.watchdog,
        support=activation.support, manager=manager, identity_reader=manager.identity,
        assert_drained=lambda: None, engine_observer=lambda: {'engine_running_waiting_zero': True},
        worker_observer=lambda profile: {'registration_verified': True, 'runtime_fingerprint': profile['runtime_fingerprint']},
        path_observer=lambda disabled: {'disable_custom_all_reduce': disabled, 'symmetric_memory_allreduce': False},
        clock=activation.clock, sleep=activation.sleep)
    return activation, manager, kwargs


def test_default_preparation_is_read_only_and_uses_real_manifest_validation(tmp_path):
    activation, manager, kwargs = activated(tmp_path)
    original_report = (activation.run / 'report.json').read_bytes()
    current = activation.manifest_path.read_bytes()
    roll = module.RestoreCollective(**kwargs)
    assert manager.calls == [] and activation.manifest_path.read_bytes() == current
    assert roll.report['phase'] == 'prepared' and not roll.report['restored']
    assert roll.budget == 298384
    assert (activation.run / 'report.json').read_bytes() == original_report
    assert all(s['python'].endswith('/.venv/bin/python') for s in roll.services)
    assert 'worker-token-placeholder' not in json.dumps(roll.report)


def test_success_restores_exact_manifest_budget_profile_and_protects_api(tmp_path):
    activation, manager, kwargs = activated(tmp_path)
    original_report = (activation.run / 'report.json').read_bytes()
    roll = module.RestoreCollective(**kwargs)
    observed = []
    def engine():
        assert manager.identity(roll.worker) is None
        assert not any(s['role'] == 'worker' for s in json.loads(roll.manifest_path.read_bytes())['services'])
        observed.append('engine'); return {'engine_running_waiting_zero': True}
    def worker(profile):
        observed.append(profile['profile_id']); return {'registration_verified': True}
    roll.engine_observer, roll.profile_observer = engine, worker
    report = roll.apply()
    assert report['phase'] == 'complete' and report['restored'] and report['watchdog_restored']
    assert report['restored_engine_preflight']['preserved_budget_tokens'] == 298384
    assert report['restored_engine_preflight']['collective']['disable_custom_all_reduce'] is False
    assert observed == [roll.profile['profile_id'], 'engine', roll.original_profile['profile_id']]
    assert roll.manifest_path.read_bytes() == activation.original
    assert all(path.read_bytes() == raw for path, raw in roll.env_bytes.items())
    assert not any(key.startswith(('api-', 'gateway-')) for key, _, _ in manager.calls)
    assert all(manager.identity(s) == roll.before[roll.key(s)] for s in roll.protected)
    assert (activation.run / 'report.json').read_bytes() == original_report


@pytest.mark.parametrize('change', ['sha', 'activation', 'snapshot', 'pid', 'start_ticks', 'env', 'source', 'manifest'])
def test_stale_completed_state_refuses_restoration_before_any_service_mutation(tmp_path, change):
    activation, manager, kwargs = activated(tmp_path)
    if change == 'sha': kwargs['activation_sha256'] = '0' * 64
    elif change == 'activation': (activation.run / 'report.json').write_bytes(b'{}')
    elif change == 'snapshot': (activation.run / 'original-engine.env').write_bytes(b'CHANGED=1\n')
    elif change in {'pid', 'start_ticks'}: manager.identities['engine-qwen235b'][change] = 'changed'
    elif change == 'env': Path(activation.worker['env_file']).write_bytes(b'CHANGED=1\n')
    elif change == 'source': Path(next(iter(activation.support['source_sha256']))).write_text('# changed\n')
    elif change == 'manifest': activation.manifest_path.write_bytes(activation.original)
    with pytest.raises(RuntimeError): module.RestoreCollective(**kwargs)
    assert manager.calls == []
    assert not (kwargs['run'] / 'report.json').exists()


def test_reprepare_preserves_evidence_and_apply_rejects_new_pid(tmp_path):
    activation, manager, kwargs = activated(tmp_path)
    roll = module.RestoreCollective(**kwargs)
    before = (roll.restore_run / 'report.json').read_bytes()
    again = module.RestoreCollective(**kwargs, previous=copy.deepcopy(roll.report))
    assert (roll.restore_run / 'report.json').read_bytes() == before
    manager.identities['worker-qwen235b']['start_ticks'] = 'replacement'
    with pytest.raises(RuntimeError): again.apply()
    assert manager.calls == [] and (roll.restore_run / 'report.json').read_bytes() == before


def test_active_cohort_refuses_preparation_and_apply(tmp_path):
    _, manager, kwargs = activated(tmp_path)
    def busy(): raise RuntimeError('active cohort')
    with pytest.raises(RuntimeError, match='active cohort'):
        module.RestoreCollective(**{**kwargs, 'assert_drained': busy})
    assert manager.calls == []
    roll = module.RestoreCollective(**kwargs); roll.assert_drained = busy
    with pytest.raises(RuntimeError, match='active cohort'): roll.apply()
    assert manager.calls == []


@pytest.mark.parametrize('failure', ['cache', 'protocol', 'registration'])
def test_restore_failure_excludes_worker_and_requires_manual_recovery(tmp_path, failure):
    activation, manager, kwargs = activated(tmp_path)
    original_report = (activation.run / 'report.json').read_bytes()
    roll = module.RestoreCollective(**kwargs)
    def fail(): raise RuntimeError('protocol mismatch')
    def registry(profile):
        if profile == roll.original_profile: raise RuntimeError('registration mismatch')
        return {'registration_verified': True}
    if failure == 'cache': manager.cache_log = 'GPU KV cache size: 295,040 tokens\n'
    elif failure == 'protocol': roll.engine_observer = fail
    else: roll.profile_observer = registry
    with pytest.raises(RuntimeError): roll.apply()
    report = roll.report
    assert report['phase'] == 'manual_recovery_required' and report['restored'] is False
    assert report['worker_excluded_and_owned_worker_stopped'] is True and report['watchdog_restored'] is True
    assert not any(s['role'] == 'worker' for s in json.loads(roll.manifest_path.read_bytes())['services'])
    assert manager.identity(roll.worker) is None
    assert not any(key.startswith(('api-', 'gateway-')) for key, _, _ in manager.calls)
    assert (activation.run / 'report.json').read_bytes() == original_report
    if failure != 'registration':
        assert not any(key == 'worker-qwen235b' and action == 'start' for key, action, _ in manager.calls)


def test_foreign_manifest_mid_restore_keeps_watchdog_stopped_and_never_overwrites(tmp_path):
    _, manager, kwargs = activated(tmp_path)
    roll = module.RestoreCollective(**kwargs)
    foreign = b'{"foreign":true}\n'
    def mutate(service):
        if service['role'] == 'worker': roll.manifest_path.write_bytes(foreign)
    manager.after_stop = mutate
    with pytest.raises(RuntimeError): roll.apply()
    assert roll.report['phase'] == 'manual_recovery_required'
    assert roll.report['worker_excluded_and_owned_worker_stopped'] is False
    assert roll.report['watchdog_restored'] is False
    assert roll.manifest_path.read_bytes() == foreign
    assert manager.identity(roll.watchdog) is None
