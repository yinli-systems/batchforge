import copy
import json
from pathlib import Path

import pytest

from ops.ha import collective_qwen235b as module
from ops.ha.retune_full_gpu import file_env, write_env
from ops.ha.test_adaptive_qwen235b import prepared as static_fixture
from ops.supervise import private_write


def prepared(tmp_path):
    base, manager = static_fixture(tmp_path)
    engine = file_env(Path(base.engine['env_file'])); engine['ENGINE_MAX_NUM_SEQS'] = '64'
    write_env(Path(base.engine['env_file']), engine)
    worker = file_env(Path(base.worker['env_file']))
    profile = json.loads(worker['SCHEDULER_PROFILE_JSON'])
    profile['profile_id'] = 'primary-h100-tp4-qwen235b-fp8-adaptive64'
    worker.update(WORKER_MAX_INFLIGHT='64', WORKER_TOKEN_BUDGET='298384', WORKER_ADAPTIVE_KV_ENABLED='true',
                  SCHEDULER_PROFILE_JSON=json.dumps(profile))
    write_env(Path(base.worker['env_file']), worker)
    base.engine['gpu'] = '0,1,2,3'
    interpreter = tmp_path / '.venv/bin/python'
    interpreter.parent.mkdir(parents=True)
    interpreter.write_text('#!/bin/sh\nexit 0\n'); interpreter.chmod(0o700)
    for service in base.services:
        service['python'] = str(interpreter)
    private_write(base.manifest_path, {'services': base.services})
    run = tmp_path / '.runtime/collective'; run.mkdir(mode=0o700)
    source = tmp_path / 'pinned-support.py'; source.write_text('# support\n')
    kwargs = dict(root=tmp_path, run=run, manifest_path=base.manifest_path,
        services=base.services, watchdog=base.watchdog,
        support={'source_sha256': {str(source): module.sha(source.read_bytes())},
                 'managed_argv_changes_only_disable_custom_all_reduce': True},
        manager=manager, identity_reader=manager.identity, assert_drained=lambda: None,
        engine_observer=lambda: {'engine_running_waiting_zero': True},
        worker_observer=lambda profile: {'runtime_fingerprint': profile['runtime_fingerprint']},
        path_observer=lambda disabled: {'disable_custom_all_reduce': disabled, 'symmetric_memory_allreduce': False},
        clock=base.clock, sleep=base.sleep)
    return module.CollectiveRoll(**kwargs), manager, kwargs


def test_prepare_preserves_budget_rates_price_and_changes_numerical_fingerprint(tmp_path):
    roll, manager, _ = prepared(tmp_path)
    assert manager.calls == []
    original, candidate = roll.original_profile, roll.profile
    assert candidate['runtime_fingerprint'] != original['runtime_fingerprint']
    assert candidate['runtime_fingerprint'] == module.nccl_profile(original)[0]['runtime_fingerprint']
    for key in ('prefill_tokens_per_second', 'decode_tokens_per_second', 'hourly_cost_usd'):
        assert candidate[key] == original[key]
    assert 'not-recalibrated' in candidate['calibration_label']
    assert candidate['cost_source'].startswith(original['cost_source'] + ';')
    worker = file_env(roll.run / 'worker.env')
    assert worker['WORKER_TOKEN_BUDGET'] == '298384' and worker['WORKER_MAX_INFLIGHT'] == '64'
    engine = file_env(roll.run / 'engine.env')
    assert engine['ENGINE_DISABLE_CUSTOM_ALL_REDUCE'] == '1' and engine['VLLM_ALLREDUCE_USE_SYMM_MEM'] == '0'
    assert {k: v for k, v in engine.items() if k not in {'ENGINE_DISABLE_CUSTOM_ALL_REDUCE', 'VLLM_ALLREDUCE_USE_SYMM_MEM'}} == roll.engine_env
    assert (roll.run / 'original-manifest.json').read_bytes() == roll.original
    assert 'worker-token-placeholder' not in json.dumps(roll.report)
    assert 'private-key-placeholder' not in json.dumps(roll.report)


def test_success_requires_exact_budget_and_protocol_before_starting_candidate_worker(tmp_path):
    roll, manager, _ = prepared(tmp_path)
    observed = []
    def engine_probe():
        assert manager.identity(roll.worker) is None
        assert not any(s['role'] == 'worker' for s in json.loads(roll.manifest_path.read_bytes())['services'])
        observed.append('engine'); return {'quiescent': True}
    def worker_probe(profile):
        observed.append('candidate' if profile == roll.profile else 'original')
        return {'runtime_fingerprint': profile['runtime_fingerprint']}
    roll.engine_observer, roll.profile_observer = engine_probe, worker_probe
    report = roll.apply()
    assert report['phase'] == 'complete' and report['watchdog_restored']
    assert report['candidate_engine_preflight']['preserved_budget_tokens'] == 298384
    assert observed == ['original', 'engine', 'candidate']
    assert not any(key.startswith(('api-', 'gateway-')) for key, _, _ in manager.calls)
    assert all(path.read_bytes() == raw for path, raw in roll.env_bytes.items())


@pytest.mark.parametrize('changed', ['env', 'pid', 'start_ticks', 'source', 'snapshot'])
def test_changed_prepared_state_blocks_all_service_mutations(tmp_path, changed):
    roll, manager, _ = prepared(tmp_path)
    if changed == 'env': Path(roll.worker['env_file']).write_bytes(b'CHANGED=1\n')
    elif changed in {'pid', 'start_ticks'}: manager.identities['api-a'][changed] = 'changed'
    elif changed == 'source': Path(next(iter(roll.support['source_sha256']))).write_text('# changed\n')
    else: (roll.run / 'original-engine.env').write_bytes(b'CHANGED=1\n')
    with pytest.raises(RuntimeError): roll.apply()
    assert manager.calls == []


def test_prepare_rerun_keeps_existing_snapshot_bytes_and_refuses_live_identity_drift(tmp_path):
    roll, manager, kwargs = prepared(tmp_path)
    saved = {path.name: path.read_bytes() for path in roll.run.iterdir()}
    second = module.CollectiveRoll(**kwargs, previous=copy.deepcopy(roll.report))
    assert saved == {path.name: path.read_bytes() for path in roll.run.iterdir()}
    manager.identities['worker-qwen235b']['start_ticks'] = 'different-launch'
    with pytest.raises(RuntimeError, match='refusing snapshot overwrite'):
        module.CollectiveRoll(**kwargs, previous=copy.deepcopy(second.report))
    assert saved == {path.name: path.read_bytes() for path in roll.run.iterdir()}


def test_nondrained_baseline_never_stops_services(tmp_path):
    roll, manager, _ = prepared(tmp_path)
    def busy(): raise RuntimeError('active tasks')
    roll.assert_drained = busy
    with pytest.raises(RuntimeError, match='active tasks'): roll.apply()
    assert manager.calls == []


def test_insufficient_candidate_cache_rolls_back_and_never_starts_candidate_worker(tmp_path):
    roll, manager, _ = prepared(tmp_path)
    def before(service):
        if service['role'] == 'engine':
            cache = 295040 if service['env_file'] == roll.candidate_engine['env_file'] else 314096
            manager.cache_log = f'GPU KV cache size: {cache:,} tokens\n'
    manager.before_start = before
    with pytest.raises(RuntimeError, match='preserve the baseline token budget'): roll.apply()
    assert roll.report['phase'] == 'rolled_back' and roll.report['watchdog_restored']
    assert roll.report['rollback_engine_preflight']['measured_cache_tokens'] == 314096
    assert roll.manifest_path.read_bytes() == roll.original
    assert not any(key == 'worker-qwen235b' and action == 'start' and path == roll.candidate_worker['env_file']
                   for key, action, path in manager.calls)
    assert manager.identity(roll.worker) is not None


def test_candidate_registry_failure_restores_original_adaptive_profile_and_manifest(tmp_path):
    roll, manager, _ = prepared(tmp_path)
    profiles = []
    def observer(profile):
        profiles.append(profile['runtime_fingerprint'])
        if profile == roll.profile: raise RuntimeError('new registration differs')
        return {'registration_verified': True}
    roll.profile_observer = observer
    with pytest.raises(RuntimeError, match='new registration differs'): roll.apply()
    assert profiles == [roll.original_profile['runtime_fingerprint'], roll.profile['runtime_fingerprint'], roll.original_profile['runtime_fingerprint']]
    assert roll.report['phase'] == 'rolled_back' and roll.report['rollback_worker_preflight']['registration_verified']
    assert roll.manifest_path.read_bytes() == roll.original
    assert all(path.read_bytes() == raw for path, raw in roll.env_bytes.items())


def test_rollback_capacity_failure_excludes_unvalidated_worker_from_watchdog_manifest(tmp_path):
    roll, manager, _ = prepared(tmp_path)
    manager.cache_log = 'GPU KV cache size: 295,040 tokens\n'
    with pytest.raises(RuntimeError): roll.apply()
    assert roll.report['phase'] == 'manual_recovery_required'
    assert not any(s['role'] == 'worker' for s in json.loads(roll.manifest_path.read_bytes())['services'])
    assert manager.identity(roll.worker) is None


def test_rollback_registration_failure_stops_already_started_unvalidated_original_worker(tmp_path):
    roll, manager, _ = prepared(tmp_path)
    calls = 0
    def observer(profile):
        nonlocal calls
        calls += 1
        if calls > 1: raise RuntimeError('candidate and original registrations both fail')
        return {'registration_verified': True}
    roll.profile_observer = observer
    with pytest.raises(RuntimeError): roll.apply()
    assert roll.report['phase'] == 'manual_recovery_required'
    assert roll.report['worker_excluded_and_owned_worker_stopped'] is True
    assert manager.identity(roll.worker) is None and roll.report['watchdog_restored'] is True
    assert not any(s['role'] == 'worker' for s in json.loads(roll.manifest_path.read_bytes())['services'])


def test_foreign_manifest_during_activation_prevents_watchdog_restart_or_overwrite(tmp_path):
    roll, manager, _ = prepared(tmp_path)
    foreign = b'{"foreign":true}\n'
    def mutate(service):
        if service['role'] == 'worker': roll.manifest_path.write_bytes(foreign)
    manager.after_stop = mutate
    with pytest.raises(RuntimeError): roll.apply()
    assert roll.report['phase'] == 'manual_recovery_required'
    assert roll.report['worker_excluded_and_owned_worker_stopped'] is False
    assert roll.report['watchdog_restored'] is False and manager.identity(roll.watchdog) is None
    assert roll.manifest_path.read_bytes() == foreign


@pytest.mark.parametrize('change', [
    ('engine', 'ENGINE_DISABLE_CUSTOM_ALL_REDUCE', '1'),
    ('engine', 'VLLM_ALLREDUCE_USE_SYMM_MEM', '1'),
    ('engine', 'ENGINE_MAX_NUM_SEQS', '16'),
    ('worker', 'WORKER_ADAPTIVE_KV_ENABLED', 'false'),
    ('worker', 'WORKER_TOKEN_BUDGET', '298385'),
])
def test_constructor_refuses_incompatible_baseline(tmp_path, change):
    roll, manager, kwargs = prepared(tmp_path)
    role, key, value = change
    path = Path(getattr(roll, role)['env_file']); env = file_env(path); env[key] = value; write_env(path, env)
    with pytest.raises(ValueError): module.CollectiveRoll(**kwargs, previous=roll.report)
    assert manager.calls == []
