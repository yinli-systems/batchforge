import copy
import json
from pathlib import Path

import httpx
import pytest

from ops.ha.adaptive_qwen235b import (AdaptiveRoll, CONTEXT, MAX_BUDGET, MODEL, REVISION,
    admission_budget, candidate_worker, fresh_cache, metric_values, prepared_snapshot_matches,
    private_operation_directory, sha)
from ops.ha.retune_full_gpu import file_env, write_env
from ops.supervise import private_write


def profile():
    return {'profile_id': 'primary-h100-tp4-qwen235b-fp8-seq16', 'runtime_fingerprint': 'sha256:' + 'd' * 64,
        'prefill_tokens_per_second': 2200, 'decode_tokens_per_second': 90, 'hourly_cost_usd': 16.10 / 16,
        'calibration_label': 'actual-fixed20-c16-http-phase-NNLS:12345678', 'cost_source': 'operator-allocation-divided-by-16'}


class Manager:
    def __init__(self, root, services):
        self.root, self.calls = root, []
        self.identities = {AdaptiveRoll.key(s): {'pid': i + 100, 'start_ticks': str(i + 800), 'cwd': str(root)}
                           for i, s in enumerate(services)}
        self.counter = 500
        self.cache_log = 'GPU KV cache size: 316,144 tokens\nMaximum concurrency for 18,440 tokens per request: 17.14x\n'
        self.candidate_gate = True
        self.rollback_gate = True
        self.crash_worker = False
        self.before_start = lambda service: None
        self.after_stop = lambda service: None

    def identity(self, service): return copy.deepcopy(self.identities.get(AdaptiveRoll.key(service)))

    def invoke(self, service, action):
        key = AdaptiveRoll.key(service)
        self.calls.append((key, action, service['env_file']))
        if action == 'stop':
            self.identities[key] = None
            self.after_stop(service)
            return {'status': 'stopped'}, None
        if action == 'start':
            self.before_start(service)
            self.counter += 1
            self.identities[key] = {'pid': self.counter, 'start_ticks': str(self.counter + 1000), 'cwd': str(self.root)}
            if service['role'] == 'engine':
                with (self.root / '.runtime/engine-qwen235b.log').open('a') as stream:
                    stream.write(self.cache_log)
            if service['role'] == 'worker' and service['env_file'].endswith('/adaptive/worker.env') and self.crash_worker:
                self.identities[key] = None
            return {'status': 'started', 'pid': self.counter}, None
        raise AssertionError(action)

    def readiness(self, service): return self.identity(service) is not None

    def start_gate(self, service):
        self.calls.append((AdaptiveRoll.key(service), 'gate', service['env_file']))
        allowed = self.candidate_gate if service['env_file'].endswith('/adaptive/engine.env') else self.rollback_gate
        return allowed, None if allowed else 'gpu_memory_not_released'


class Clock:
    def __init__(self): self.time = 0
    def now(self): return self.time
    def sleep(self, seconds): self.time += seconds


def prepared(tmp_path, **kwargs):
    runtime = tmp_path / '.runtime'; runtime.mkdir(mode=0o700)
    run = runtime / 'adaptive'; run.mkdir(mode=0o700)
    services = []
    for role, name, port in [('api', 'a', 8082), ('api', 'b', 8083), ('gateway', 'edge', 8080),
                             ('engine', 'qwen235b', 8335), ('worker', 'qwen235b', 9106)]:
        path = runtime / (role + '-' + name + '.env')
        write_env(path, {'MODEL_NAME': MODEL})
        services.append({'role': role, 'name': name, 'env_file': str(path), 'port': port})
    engine, worker = services[-2:]
    write_env(Path(engine['env_file']), {'MODEL_NAME': MODEL, 'ENGINE_REVISION': REVISION,
        'ENGINE_TENSOR_PARALLEL_SIZE': '4', 'ENGINE_MAX_NUM_SEQS': '16', 'ENGINE_MAX_MODEL_LEN': str(CONTEXT),
        'ENGINE_MAX_NUM_BATCHED_TOKENS': '4096', 'ENGINE_DTYPE': 'bfloat16', 'ENGINE_GPU_MEMORY_UTILIZATION': '.90',
        'ENGINE_ENABLE_PREFIX_CACHING': '0', 'ENGINE_GENERATION_CONFIG': 'vllm', 'ENGINE_GPU_MEMORY_MARGIN_MIB': '2048',
        'ENGINE_ENABLE_CHUNKED_PREFILL': '1',
        'VLLM_TORCH_PROFILER_DIR': str(runtime / 'traces')})
    write_env(Path(worker['env_file']), {'MODEL_NAME': MODEL, 'WORKER_ID': 'primary-qwen235b-tp4',
        'INFERENCE_BASE_URLS': 'http://127.0.0.1:8335', 'WORKER_MAX_INFLIGHT': '16',
        'WORKER_TOKEN_BUDGET': str(16 * CONTEXT), 'ENGINE_MAX_MODEL_LEN': str(CONTEXT),
        'BATCHFORGE_WORKER_TOKEN': 'worker-token-placeholder', 'INFERENCE_API_KEY': 'private-key-placeholder',
        'SCHEDULER_PROFILE_JSON': json.dumps(profile())})
    watchdog = {'role': 'supervisor', 'name': 'primary', 'env_file': str(runtime / 'supervisor.env')}
    write_env(Path(watchdog['env_file']), {'SUPERVISOR_MANIFEST': str(runtime / 'services-primary.json')})
    manifest = runtime / 'services-primary.json'; private_write(manifest, {'services': services})
    manager = Manager(tmp_path, [*services, watchdog]); clock = Clock()
    roll = AdaptiveRoll(root=tmp_path, run=run, manifest_path=manifest, services=services, watchdog=watchdog,
        manager=manager, identity_reader=manager.identity, assert_drained=lambda: None,
        engine_observer=lambda: {'engine_running_waiting_zero': True}, worker_observer=lambda budget: {'budget': budget},
        clock=clock.now, sleep=clock.sleep, **kwargs)
    return roll, manager


def test_budget_uses_exact_fresh_capacity_not_rounded_full_context_ratio():
    assert admission_budget(316144) == 300000
    assert admission_budget(295040) == 280288
    assert admission_budget(316144, 250000) == 250000
    assert fresh_cache('GPU KV cache size: 316,144 tokens\nGPU KV cache size: 295,040 tokens\n') == 295040
    for invalid in [None, True, 0, -16, 316143, 316144.0, 18448]:
        with pytest.raises(ValueError): admission_budget(invalid)
    for invalid in [0, True, 300001, 320000]:
        with pytest.raises(ValueError): admission_budget(316144, invalid)
    with pytest.raises(ValueError): fresh_cache('Maximum concurrency for 18,440 tokens per request: 64.00x\n')


def test_profile_is_a_new_explicit_projection_and_source_credentials_are_not_reported():
    old = {'SCHEDULER_PROFILE_JSON': json.dumps(profile()), 'BATCHFORGE_WORKER_TOKEN': 'keep-this-private'}
    new = candidate_worker(old, MAX_BUDGET, 16.10)
    updated = json.loads(new['SCHEDULER_PROFILE_JSON'])
    assert updated['runtime_fingerprint'] == profile()['runtime_fingerprint']
    assert updated['prefill_tokens_per_second'] == profile()['prefill_tokens_per_second']
    assert updated['decode_tokens_per_second'] == profile()['decode_tokens_per_second']
    assert updated['profile_id'] != profile()['profile_id'] and 'not-recalibrated' in updated['calibration_label']
    assert updated['hourly_cost_usd'] == profile()['hourly_cost_usd']
    assert updated['cost_source'].startswith(profile()['cost_source'] + ';')
    assert 'claimlimit-not-new-priced-execution-capacity' in updated['cost_source']
    assert new['WORKER_ADAPTIVE_KV_ENABLED'] == 'true' and new['WORKER_TOKEN_BUDGET'] == '300000'
    assert old['SCHEDULER_PROFILE_JSON'] == json.dumps(profile())


def test_more_claim_slots_or_different_allocation_argument_cannot_discount_calibrated_execution_price():
    old = {'SCHEDULER_PROFILE_JSON': json.dumps(profile())}
    for allocation in (0, 1, 16.10, 10000):
        result = candidate_worker(old, MAX_BUDGET, allocation)
        candidate = json.loads(result['SCHEDULER_PROFILE_JSON'])
        assert candidate['hourly_cost_usd'] == profile()['hourly_cost_usd']
        assert candidate['prefill_tokens_per_second'] == profile()['prefill_tokens_per_second']
        assert candidate['decode_tokens_per_second'] == profile()['decode_tokens_per_second']
        assert 'claimlimit64-not-newpricedexecutioncapacity' in candidate['calibration_label']


def test_prepare_only_has_no_manager_or_inference_actions_and_changes_only_maxseq(tmp_path):
    roll, manager = prepared(tmp_path)
    assert manager.calls == [] and roll.manifest_path.read_bytes() == roll.original
    old, new = roll.engine_env, file_env(roll.run / 'engine.env')
    assert {k: v for k, v in old.items() if k != 'ENGINE_MAX_NUM_SEQS'} == {k: v for k, v in new.items() if k != 'ENGINE_MAX_NUM_SEQS'}
    assert new['ENGINE_MAX_NUM_SEQS'] == '64' and not (roll.run / 'worker.env').exists()
    assert 'worker-token-placeholder' not in json.dumps(roll.report)
    assert 'private-key-placeholder' not in json.dumps(roll.report)


def test_full_roll_does_not_start_worker_until_fresh_cache_and_engine_checks_pass(tmp_path):
    roll, manager = prepared(tmp_path)
    original_bytes = {p: p.read_bytes() for p in roll.env_bytes}
    observed = []
    def engine_check():
        assert manager.identity(roll.worker) is None
        assert not any(s['role'] == 'worker' for s in json.loads(roll.manifest_path.read_bytes())['services'])
        assert not (roll.run / 'worker.env').exists()
        observed.append('engine')
        return {'engine_running_waiting_zero': True}
    def worker_check(budget):
        observed.append('worker')
        assert budget == 300000 and file_env(roll.run / 'worker.env')['WORKER_MAX_INFLIGHT'] == '64'
        return {'registered_capacity': 64, 'budget': budget}
    roll.engine_observer, roll.worker_observer = engine_check, worker_check
    report = roll.apply()
    assert report['phase'] == 'complete' and report['watchdog_restored']
    assert report['measured_cache_tokens'] == 316144 and report['worker_budget_tokens'] == 300000
    assert observed == ['engine', 'worker']
    assert all(p.read_bytes() == raw for p, raw in original_bytes.items())
    assert all(not key.startswith(('api-', 'gateway-')) for key, _, _ in manager.calls)
    assert manager.identity(roll.engine) != roll.before[roll.key(roll.engine)]
    assert all(manager.identity(s) == roll.before[roll.key(s)] for s in roll.protected)


def test_stale_previous_cache_log_cannot_qualify_new_engine(tmp_path):
    roll, manager = prepared(tmp_path)
    (tmp_path / '.runtime/engine-qwen235b.log').write_text('GPU KV cache size: 316,144 tokens\n')
    manager.cache_log = 'GPU KV cache size: 18,432 tokens\nMaximum concurrency for 18,440 tokens per request: 64.00x\n'
    with pytest.raises(ValueError): roll.apply()
    assert roll.report['phase'] == 'rolled_back' and roll.report['watchdog_restored']
    assert roll.manifest_path.read_bytes() == roll.original
    assert not any(action == 'start' and path == roll.candidate_worker['env_file'] for _, action, path in manager.calls)


@pytest.mark.parametrize('failure', ['tokenizer_or_busy', 'worker_validation', 'worker_start'])
def test_failed_preflight_or_worker_restores_exact_static16_and_watchdog(tmp_path, failure):
    roll, manager = prepared(tmp_path)
    old_env = {p: p.read_bytes() for p in roll.env_bytes}
    def fail(*_): raise RuntimeError('injected preflight failure')
    if failure == 'tokenizer_or_busy': roll.engine_observer = fail
    elif failure == 'worker_validation': roll.worker_observer = fail
    else: manager.crash_worker = True
    with pytest.raises(RuntimeError): roll.apply()
    assert roll.report['phase'] == 'rolled_back' and roll.report['watchdog_restored']
    assert roll.manifest_path.read_bytes() == roll.original
    assert all(p.read_bytes() == raw for p, raw in old_env.items())
    assert manager.identity(roll.engine) is not None and manager.identity(roll.worker) is not None
    assert (roll.key(roll.engine), 'gate', roll.engine['env_file']) in manager.calls


def test_refused_candidate_memory_gate_is_bounded_and_rollback_has_fresh_gate(tmp_path):
    roll, manager = prepared(tmp_path); manager.candidate_gate = False
    with pytest.raises(RuntimeError, match='memory gate'): roll.apply()
    assert roll.clock() == 120 and roll.report['phase'] == 'rolled_back'
    assert not any(a == 'start' and p == roll.candidate_engine['env_file'] for _, a, p in manager.calls)
    assert (roll.key(roll.engine), 'gate', roll.engine['env_file']) in manager.calls


def test_partial_stop_error_retries_exact_identity_then_restores_on_preflight_failure(tmp_path):
    roll, manager = prepared(tmp_path)
    original = manager.invoke
    failed = []
    def invoke(service, action):
        result = original(service, action)
        if service['role'] == 'worker' and action == 'stop' and not failed:
            failed.append(True)
            return None, 'lost-manager-reply-after-stop'
        return result
    manager.invoke = invoke
    def fail(): raise RuntimeError('protocol validation refused')
    roll.engine_observer = fail
    with pytest.raises(RuntimeError): roll.apply()
    assert failed and roll.report['phase'] == 'rolled_back' and roll.report['watchdog_restored']
    assert roll.clock() == 2 and manager.identity(roll.worker) is not None


def test_rollback_memory_failure_reports_manual_recovery_and_never_starts_unvalidated_candidate_worker(tmp_path):
    roll, manager = prepared(tmp_path); manager.rollback_gate = False
    def fail(): raise RuntimeError('invalid tokenizer')
    roll.engine_observer = fail
    with pytest.raises(RuntimeError): roll.apply()
    assert roll.report['phase'] == 'manual_recovery_required' and roll.report['watchdog_restored']
    assert not any(a == 'start' and p == roll.candidate_worker['env_file'] for _, a, p in manager.calls)
    assert roll.manifest_path.read_bytes() == roll.original


def test_changed_protected_pid_or_pending_work_refuses_before_any_stop(tmp_path):
    roll, manager = prepared(tmp_path)
    manager.identities[roll.key(roll.protected[0])]['start_ticks'] = 'replacement'
    with pytest.raises(RuntimeError, match='identity'): roll.apply()
    assert manager.calls == []
    manager.identities[roll.key(roll.protected[0])] = roll.before[roll.key(roll.protected[0])]
    def pending(): raise RuntimeError('cohort active')
    roll.assert_drained = pending
    with pytest.raises(RuntimeError, match='active'): roll.apply()
    assert manager.calls == []


def test_foreign_environment_change_is_not_overwritten_by_rollback(tmp_path):
    roll, manager = prepared(tmp_path)
    original_path = Path(roll.engine['env_file'])
    def changed():
        original_path.write_bytes(original_path.read_bytes() + b'FOREIGN=value\n')
        return {'engine_running_waiting_zero': True}
    roll.engine_observer = changed
    with pytest.raises(RuntimeError): roll.apply()
    assert roll.report['phase'] == 'manual_recovery_required'
    assert b'FOREIGN=value' in original_path.read_bytes()
    assert roll.report['watchdog_restored']
    assert not any(s['role'] == 'worker' for s in json.loads(roll.manifest_path.read_bytes())['services'])


def test_worker_metrics_are_not_allowed_to_hide_nan_missing_or_duplicate_pause(tmp_path):
    names = ['budget', 'paused']
    assert metric_values('budget 300000\npaused 0\n', names) == {'budget': 300000, 'paused': 0}
    for bad in ['budget 300000\n', 'budget NaN\npaused 0\n', 'budget 300000\npaused 0\npaused 0\n']:
        with pytest.raises(RuntimeError): metric_values(bad, names)


def test_replaying_prepared_operation_pins_all_identities_code_budget_and_private_environment(tmp_path):
    roll, _ = prepared(tmp_path)
    original = dict(manifest=roll.original, environment=roll.env_bytes, identities=roll.before,
        sources={'worker/runtime.py': 'a' * 64}, controller_sha='b' * 64,
        maximum_budget=MAX_BUDGET, allocation_hourly=16.10)
    roll.report.update(worker_source_sha256=original['sources'], controller_sha256=original['controller_sha'])
    assert prepared_snapshot_matches(roll.report, **original)
    for field, changed in [('manifest', b'{"foreign":true}'), ('sources', {'worker/runtime.py': 'c' * 64}),
            ('controller_sha', 'd' * 64), ('maximum_budget', 250000), ('allocation_hourly', 0),
            ('identities', {**roll.before, 'supervisor-primary': {'pid': 99, 'start_ticks': '99', 'cwd': str(tmp_path)}}),
            ('environment', {**roll.env_bytes, Path(roll.worker['env_file']): b'FOREIGN=value\n'})]:
        assert not prepared_snapshot_matches(roll.report, **{**original, field: changed})
    roll.report['phase'] = 'applying'
    assert not prepared_snapshot_matches(roll.report, **original)


def test_operation_directory_refuses_symlink_public_permissions_and_path_escape(tmp_path):
    (tmp_path / '.runtime').mkdir(mode=0o700)
    own = private_operation_directory(tmp_path, 'adaptive-v1')
    assert own.stat().st_mode & 0o077 == 0
    own.chmod(0o755)
    with pytest.raises(ValueError): private_operation_directory(tmp_path, 'adaptive-v1')
    (tmp_path / '.runtime/link').symlink_to(own, target_is_directory=True)
    for invalid in ['../escape', 'link', 'has space']:
        with pytest.raises(ValueError): private_operation_directory(tmp_path, invalid)


def test_initial_watchdog_stop_failure_preserves_worker_supervision_and_exact_original_manifest(tmp_path):
    roll, manager = prepared(tmp_path)
    old_worker = manager.identity(roll.worker)
    original = manager.invoke
    attempts = []
    def invoke(service, action):
        result = original(service, action)
        if service['role'] == 'supervisor' and action == 'stop':
            attempts.append(True)
            if len(attempts) <= 61:
                return None, 'watchdog-stop-reply-lost'
        return result
    manager.invoke = invoke
    with pytest.raises(RuntimeError, match='manager operation'): roll.apply()
    assert roll.report['phase'] == 'rolled_back' and roll.report['watchdog_restored']
    assert roll.manifest_path.read_bytes() == roll.original
    assert manager.identity(roll.worker) == old_worker
    assert not any(key.startswith(('engine-', 'worker-')) for key, _, _ in manager.calls)


@pytest.mark.parametrize('invalid', [None, 'busy', 'count', 'model', 'version'])
def test_private_engine_preflight_only_tokenizes_and_requires_exact_protocol_and_quiescence(tmp_path, invalid):
    roll, _ = prepared(tmp_path)
    paths = []
    def response(request):
        paths.append(request.url.path)
        data = {'/version': {'version': '0.10.2' if invalid != 'version' else 'unknown'},
            '/v1/models': {'data': [{'id': MODEL if invalid != 'model' else 'other-model'}]},
            '/tokenize': {'count': 3 if invalid != 'count' else 4, 'tokens': [1, 2, 3], 'max_model_len': CONTEXT}}
        if request.url.path == '/metrics':
            return httpx.Response(200, text='vllm:num_requests_running 0\nvllm:num_requests_waiting '+('1' if invalid == 'busy' else '0')+'\n')
        if request.url.path == '/tokenize':
            body = json.loads(request.content)
            assert body['model'] == MODEL and body['add_generation_prompt'] is True
            assert body['continue_final_message'] is False and body['add_special_tokens'] is False
        return httpx.Response(200, json=data[request.url.path])
    roll._client = lambda: httpx.Client(transport=httpx.MockTransport(response), base_url='http://127.0.0.1:8335')
    if invalid is None:
        observed = roll._observe_engine()
        assert observed['engine_running_waiting_zero'] and observed['tokenizer_probe_count'] == 3
        assert set(paths) == {'/version', '/v1/models', '/tokenize', '/metrics'}
    else:
        with pytest.raises((RuntimeError, ValueError)): roll._observe_engine()
    assert all('/completions' not in path and 'profile' not in path for path in paths)
