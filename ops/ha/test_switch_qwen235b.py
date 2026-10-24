import json

import pytest

from ops.ha.retune_full_gpu import write_env
from ops.ha.switch_qwen235b import (
    CONTEXT, MODEL, SLOTS, Switch, digest, engine_candidate, phase_calibration,
    scheduling_profile, worker_candidate,
)
from ops.supervise import private_write


def records():
    return [dict(status='completed', prompt_tokens=p, completion_tokens=o,
                 rendered_prompt_tokens=p, output_token_cap=o,
                 ttft_seconds=p / 2000,
                 stream_span_per_reported_output_token_proxy_seconds=1 / 50)
            for p in (128, 512, 2048, 8192, 16384) for o in (32, 128, 512, 2048)]


def profile():
    return scheduling_profile(phase_calibration(records()), {'model': MODEL, 'revision': 'pinned'}, 16.1, 'a' * 64)


def test_positive_phase_nnls_uses_actual_observations_and_rejects_incomplete_matrix():
    result = phase_calibration(records())
    assert result['prefill_http_first_text_proxy']['tokens_per_second'] == pytest.approx(2000)
    assert result['decode_http_stream_span_proxy']['tokens_per_second'] == pytest.approx(50)
    assert result['prefill_http_first_text_proxy']['rmse_seconds'] == pytest.approx(0)
    for bad in (records()[:-1], [records()[0]] * 20):
        with pytest.raises(ValueError):
            phase_calibration(bad)
    bad = records(); bad[0]['completion_tokens'] -= 1
    with pytest.raises(ValueError, match='exact fixed work'):
        phase_calibration(bad)
    bad = records(); bad[0]['ttft_seconds'] = float('nan')
    with pytest.raises(ValueError, match='positive finite'):
        phase_calibration(bad)


def test_worker_configuration_keeps_worker_credentials_but_discards_other_secrets_and_old_rates():
    source = {'BATCHFORGE_WORKER_TOKEN': 'worker-only-placeholder',
              'CONTROL_PLANE_URLS': 'http://127.0.0.1:8082,http://127.0.0.1:8083',
              'INFERENCE_BASE_URLS': 'http://127.0.0.1:8300',
              'DATABASE_URL': 'must-not-copy', 'BATCHFORGE_API_KEY': 'must-not-copy',
              'AWS_SECRET_ACCESS_KEY': 'must-not-copy', 'SCHEDULER_PROFILE_JSON': 'old-rates'}
    draft = worker_candidate(source)
    assert 'SCHEDULER_PROFILE_JSON' not in draft
    assert not {'DATABASE_URL', 'BATCHFORGE_API_KEY', 'AWS_SECRET_ACCESS_KEY'} & draft.keys()
    result = worker_candidate(source, profile())
    assert result['BATCHFORGE_WORKER_TOKEN'] == source['BATCHFORGE_WORKER_TOKEN']
    assert result['MODEL_NAME'] == MODEL
    assert result['WORKER_ID'] == 'primary-qwen235b-tp4'
    assert int(result['WORKER_TOKEN_BUDGET']) == SLOTS * CONTEXT
    assert json.loads(result['SCHEDULER_PROFILE_JSON'])['prefill_tokens_per_second'] == pytest.approx(2000)
    assert source['SCHEDULER_PROFILE_JSON'] == 'old-rates'


def test_engine_candidate_does_not_inherit_unrelated_credentials_or_architecture_override(tmp_path):
    value = engine_candidate({'DATABASE_URL': 'not-copy', 'BATCHFORGE_API_KEY': 'not-copy',
                              'VLLM_ATTENTION_BACKEND': 'old-architecture', 'OMP_NUM_THREADS': '8'}, tmp_path)
    assert not {'DATABASE_URL', 'BATCHFORGE_API_KEY', 'VLLM_ATTENTION_BACKEND'} & value.keys()
    assert value['OMP_NUM_THREADS'] == '8'
    assert value['ENGINE_TENSOR_PARALLEL_SIZE'] == '4'
    assert value['MODEL_NAME'] == MODEL


class FakeManager:
    def __init__(self, root, *, crash=False):
        self.root, self.crash, self.calls = root, crash, []
        self.running = {'gptoss120': True, 'qwen235b': False, 'primary': True}

    def invoke(self, service, action):
        name, role = service['name'], service['role']
        self.calls.append((role, name, action))
        if action == 'status':
            return {'running': self.running.get(name, True), 'pid': 123}, None
        if action == 'stop':
            if role == 'engine':
                self.running[name] = False
            return {'status': 'stopped'}, None
        if action == 'start':
            if role == 'engine':
                self.running[name] = not (name == 'qwen235b' and self.crash)
                if name == 'qwen235b':
                    (self.root / '.runtime/engine-qwen235b.log').write_text(
                        'GPU KV cache size: 316,144 tokens\n'
                        'Maximum concurrency for 18,440 tokens per request: 9.5x\n')
            return {'status': 'started', 'pid': 456}, None
        raise AssertionError(action)

    def readiness(self, service):
        return self.running.get(service['name'], True)

    def start_gate(self, service):
        self.calls.append((service['role'], service['name'], 'gate'))
        return True, None


def prepared(tmp_path, *, crash=False):
    runtime = tmp_path / '.runtime'; runtime.mkdir()
    run = runtime / 'switch'; run.mkdir()
    old_env = run / 'old-worker.env'
    write_env(old_env, {'BATCHFORGE_WORKER_TOKEN': 'worker-placeholder',
                        'INFERENCE_BASE_URLS': 'http://127.0.0.1:8300'})
    old_engine_env = run / 'old-engine.env'; write_env(old_engine_env, {'MODEL_NAME': 'old'})
    old_engine = dict(role='engine', name='gptoss120', env_file=str(old_engine_env))
    old_worker = dict(role='worker', name='gptoss120', env_file=str(old_env))
    new_engine = dict(role='engine', name='qwen235b', env_file=str(run / 'engine.env'))
    new_worker = dict(role='worker', name='qwen235b', env_file=str(run / 'worker.env'))
    api = dict(role='api', name='a')
    original = {'services': [old_engine, old_worker, api]}
    candidate = {'services': [new_engine, new_worker, api]}
    manifest = runtime / 'services-primary.json'; private_write(manifest, original)
    manager = FakeManager(tmp_path, crash=crash)
    switch = Switch(root=tmp_path, run=run, manager=manager, manifest_path=manifest,
        original=original, candidate=candidate, old_engine=old_engine, old_worker=old_worker,
        new_engine=new_engine, new_worker=new_worker, watchdog=dict(role='supervisor', name='primary'),
        protected_services=[api], original_env_hashes={str(old_env): digest(old_env), str(old_engine_env): digest(old_engine_env)})
    return switch, manager


def test_worker_is_not_exposed_until_calibration_completes(tmp_path):
    switch, manager = prepared(tmp_path)
    drained_calls = []
    def calibrate(_):
        assert ('worker', 'qwen235b', 'start') not in manager.calls
        assert not any(s['role'] == 'worker' for s in json.loads(switch.manifest_path.read_text())['services'])
        return profile()
    switch.apply(assert_drained=lambda: drained_calls.append(True), calibration_and_profile=calibrate)
    assert len(drained_calls) == 2
    assert switch.report['phase'] == 'complete'
    assert json.loads(switch.manifest_path.read_text()) == switch.candidate
    assert json.loads((switch.run / 'report.json').read_text())['profile']['runtime_fingerprint'].startswith('sha256:')
    assert [action for role, _, action in manager.calls if role == 'api'] == ['status', 'status']


@pytest.mark.parametrize('failure', ['engine', 'calibration'])
def test_failed_engine_or_calibration_rolls_back_exact_old_manifest(tmp_path, failure):
    switch, manager = prepared(tmp_path, crash=failure == 'engine')
    def calibrate(_):
        raise ValueError('calibration intentionally failed')
    with pytest.raises((RuntimeError, ValueError)):
        switch.apply(assert_drained=lambda: None, calibration_and_profile=calibrate)
    assert switch.report['phase'] == 'rolled_back'
    assert json.loads(switch.manifest_path.read_text()) == switch.original
    assert ('worker', 'qwen235b', 'start') not in manager.calls
    assert ('engine', 'gptoss120', 'gate') in manager.calls
    assert ('worker', 'gptoss120', 'start') in manager.calls


def test_pending_work_refuses_before_any_service_mutation(tmp_path):
    switch, manager = prepared(tmp_path)
    def active():
        raise RuntimeError('not drained')
    with pytest.raises(RuntimeError, match='not drained'):
        switch.apply(assert_drained=active, calibration_and_profile=lambda _: profile())
    assert all(action == 'status' for _, _, action in manager.calls)
    assert json.loads(switch.manifest_path.read_text()) == switch.original
