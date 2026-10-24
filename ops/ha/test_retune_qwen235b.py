import json
from pathlib import Path

import pytest

from ops.ha.retune_qwen235b import calibrated_profile, private_profile_directory, retuned_worker
from ops.ha.switch_qwen235b import phase_calibration
from ops.ha.test_switch_qwen235b import prepared, profile, records


def test_static16_worker_requires_full_context_and_fresh_rates_preserves_runtime():
    original = profile()
    fit = phase_calibration(records(), concurrency=16)
    new = calibrated_profile(original, fit, 16.1, 'f' * 64)
    env = retuned_worker({'WORKER_MAX_INFLIGHT': '8', 'WORKER_TOKEN_BUDGET': '147520',
                          'INFERENCE_TIMEOUT_SECONDS': '1800', 'FEATURE_DYNAMIC_KV': 'false',
                          'SCHEDULER_PROFILE_JSON': json.dumps(original)}, new)
    assert env['WORKER_TOKEN_BUDGET'] == '295040'
    assert env['WORKER_MAX_INFLIGHT'] == '16'
    assert env['FEATURE_DYNAMIC_KV'] == 'false'
    assert env['INFERENCE_TIMEOUT_SECONDS'] == '1800'
    assert new['runtime_fingerprint'] == original['runtime_fingerprint']
    assert new['hourly_cost_usd'] == 16.1 / 16
    assert 'c16' in new['calibration_label']
    assert 'SCHEDULER_PROFILE_JSON' not in retuned_worker(env)


def test_profile_directory_must_be_private_and_inside_runtime(tmp_path):
    runtime = tmp_path / '.runtime'; runtime.mkdir(mode=0o700)
    path = private_profile_directory(tmp_path, runtime / 'traces')
    assert path.stat().st_mode & 0o077 == 0
    for bad in (tmp_path / 'outside', runtime):
        with pytest.raises(ValueError): private_profile_directory(tmp_path, bad)
    (runtime / 'link').symlink_to(path, target_is_directory=True)
    with pytest.raises(ValueError): private_profile_directory(tmp_path, runtime / 'link')


def test16_rejects_rounded_concurrency_when_exact_token_capacity_is_short(tmp_path):
    switch, manager = prepared(tmp_path)
    switch.slots = 16
    old = manager.invoke
    def invoke(service, action):
        result = old(service, action)
        if (service['role'], service['name'], action) == ('engine', 'qwen235b', 'start'):
            (tmp_path / '.runtime/engine-qwen235b.log').write_text(
                'GPU KV cache size: 295,039 tokens\nMaximum concurrency for 18,440 tokens per request: 16.00x\n')
        return result
    manager.invoke = invoke
    with pytest.raises(RuntimeError, match='insufficient_fresh'):
        switch.apply(assert_drained=lambda: None, calibration_and_profile=lambda _: profile())
    assert switch.report['phase'] == 'rolled_back'
    assert ('worker', 'qwen235b', 'start') not in manager.calls


def test_same_service_identity_retune_failure_restores_original_environment(tmp_path):
    switch, manager = prepared(tmp_path)
    switch.old_engine['name'] = switch.old_worker['name'] = 'qwen235b'
    switch.slots = 16
    manager.running['qwen235b'] = True
    original_environment = Path(switch.old_worker['env_file']).read_bytes()
    old_invoke = manager.invoke
    started_environments = []

    def invoke(service, action):
        result = old_invoke(service, action)
        if service['role'] == 'engine' and action == 'start':
            started_environments.append(service['env_file'])
            (tmp_path / '.runtime/engine-qwen235b.log').write_text(
                'GPU KV cache size: 316,144 tokens\nMaximum concurrency for 18,440 tokens per request: 17.14x\n')
        return result

    def calibration_failure(_):
        raise ValueError('fresh calibration intentionally failed')

    manager.invoke = invoke
    with pytest.raises(ValueError, match='fresh calibration'):
        switch.apply(assert_drained=lambda: None, calibration_and_profile=calibration_failure)
    assert switch.report['phase'] == 'rolled_back'
    assert started_environments == [switch.new_engine['env_file'], switch.old_engine['env_file']]
    assert json.loads(switch.manifest_path.read_text()) == switch.original
    assert Path(switch.old_worker['env_file']).read_bytes() == original_environment
