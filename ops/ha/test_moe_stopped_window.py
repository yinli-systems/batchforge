import json
import copy
from pathlib import Path
from types import SimpleNamespace

import pytest

from ops.ha import moe_stopped_window as module
from ops.supervise import private_write


class Manager:
    def __init__(self, root):
        self.root, self.calls = root, []
        self.running = {(role, name): True for role, name in
                        [('engine', 'qwen235b'), ('worker', 'qwen235b'), ('supervisor', 'primary'), ('api', 'a')]}
        self.gate = True

    def invoke(self, service, action):
        key = service['role'], service['name']; self.calls.append((*key, action))
        if action == 'status': return {'running': self.running[key], 'pid': 123}, None
        self.running[key] = action == 'start'
        if key[0] == 'engine' and action == 'start':
            (self.root / '.runtime/engine-qwen235b.log').write_text(
                'GPU KV cache size: 316,144 tokens\nMaximum concurrency for 18,440 tokens per request: 17.14x\n')
        return {'status': action}, None

    def readiness(self, service): return self.running[service['role'], service['name']]
    def start_gate(self, _): return self.gate, None


def prepared(root):
    runtime = root / '.runtime'; runtime.mkdir()
    run = runtime / 'window'; run.mkdir()
    services = [dict(role=role, name=name) for role, name in
                [('engine', 'qwen235b'), ('worker', 'qwen235b'), ('supervisor', 'primary'), ('api', 'a')]]
    for service in services:
        env = runtime / (service['role'] + '.env'); env.write_text('SAFE_TEST=1\n')
        service['env_file'] = str(env)
    manifest = runtime / 'services.json'; original = {'services': services}; private_write(manifest, original)
    manager = Manager(root)
    value = module.Window(root=root, run=run, manager=manager, engine=services[0], worker=services[1],
        watchdog=services[2], protected=[services[3]], manifest=manifest, original=original, hashes={},
        identity_reader=lambda _: {'pid': 123, 'start_ticks': '456', 'cwd': str(root)})
    return value, manager


@pytest.mark.parametrize('failed', [False, True])
def test_microbenchmark_success_or_failure_always_restores_owned16_and_leaves_api(tmp_path, failed):
    window, manager = prepared(tmp_path)
    def children():
        assert not any(manager.running[key] for key in [('engine', 'qwen235b'), ('worker', 'qwen235b'), ('supervisor', 'primary')])
        return [{'passed': not failed} for _ in range(4)]
    if failed:
        with pytest.raises(RuntimeError, match='original deployment restored'):
            window.apply(drained=lambda: None, children=children)
    else: window.apply(drained=lambda: None, children=children)
    assert window.report['restored'] is True
    assert all(manager.running.values())
    assert window.report['restored_kv_cache_tokens'] == 316144
    assert not any(role == 'api' for role, _, _ in manager.calls)
    assert json.loads(window.manifest.read_text()) == window.original


def test_global_drain_blocks_before_any_service_stop(tmp_path):
    window, manager = prepared(tmp_path)
    def active(): raise RuntimeError('work remains')
    with pytest.raises(RuntimeError, match='work remains'):
        window.apply(drained=active, children=lambda: [])
    assert all(action == 'status' for _, _, action in manager.calls)


def test_restore_memory_failure_keeps_worker_stopped_for_operator(tmp_path):
    window, manager = prepared(tmp_path); manager.gate = False
    with pytest.raises(RuntimeError, match='operator restore'):
        window.apply(drained=lambda: None, children=lambda: [{'passed': True}] * 4)
    assert window.report['phase'] == 'restore_blocked'
    assert manager.running['worker', 'qwen235b'] is False
    assert ('worker', 'qwen235b', 'start') not in manager.calls


def test_child_cleanup_never_signals_reused_pid(monkeypatch, tmp_path):
    expected = {'pid': 99, 'start_ticks': '1', 'cwd': '/owned'}
    monkeypatch.setattr(module.engine_members, 'identity', lambda _: dict(expected, start_ticks='2'))
    monkeypatch.setattr(module.os, 'pidfd_open', lambda _: pytest.fail('Reused PID must not be opened for signalling'), raising=False)
    monkeypatch.setattr(module.engine_members, 'reap', lambda path, record, grace_seconds: {'status': 'nothing_to_reap'})
    result = module.stop_exact_child({'process': SimpleNamespace(pid=99), 'identity': expected, 'members_path': tmp_path / 'members'})
    assert result['status'] == 'nothing_to_reap'


def test_child_command_is_bounded_and_never_installs_config(tmp_path):
    command = module.child_command('/engine/python', tmp_path, tmp_path / 'upstream.py', tmp_path / 'report.json', (16, 32))
    assert command[-5:] == ['--batches', '16', '32', '--max-seconds', '300']
    assert module.CHILD_SECONDS == 330
    assert module.BATCHES == ((1, 2), (4, 8), (16, 32), (64,))


@pytest.mark.parametrize('changed', ['manifest', 'env', 'pid', 'start_ticks', 'cwd'])
def test_prepared_binding_drift_refuses_stop(tmp_path, changed):
    window, manager = prepared(tmp_path)
    expected = {'manifest_sha256': 'a', 'environment_sha256': {'worker.env': 'b'},
                'owned_identities': {'api-a': {'pid': 12, 'start_ticks': '34', 'cwd': '/app'}}}
    actual = copy.deepcopy(expected)
    if changed == 'manifest': actual['manifest_sha256'] = 'changed'
    elif changed == 'env': actual['environment_sha256']['worker.env'] = 'changed'
    else: actual['owned_identities']['api-a'][changed] = 'changed'
    with pytest.raises(RuntimeError, match='refusing snapshot overwrite or service stop'):
        window.apply(drained=lambda: None, children=lambda: pytest.fail('No GPU work'),
                     before_stop=lambda: module.require_prepared_binding(expected, actual))
    assert all(action == 'status' for _, _, action in manager.calls)


@pytest.mark.parametrize('changed', ['start_ticks', 'env'])
def test_restore_detects_protected_api_pid_reuse_or_environment_change(tmp_path, changed):
    window, manager = prepared(tmp_path)
    identity = {'pid': 123, 'start_ticks': '456', 'cwd': str(tmp_path)}
    window.identity_reader = lambda _: dict(identity)
    def children():
        if changed == 'start_ticks': identity['start_ticks'] = '789'
        else: Path(window.protected[0]['env_file']).write_text('SAFE_TEST=2\n')
        return [{'passed': True}] * 4
    with pytest.raises(RuntimeError, match='operator restore'):
        window.apply(drained=lambda: None, children=children)
    assert window.report['phase'] == 'restore_blocked'


def test_prepare_only_rerun_cannot_overwrite_snapshot_on_env_drift(monkeypatch, tmp_path):
    tmp_path = tmp_path / 'app'; tmp_path.mkdir()
    runtime = tmp_path / '.runtime'; runtime.mkdir()
    model = module.MODEL
    engine_env = tmp_path / 'engine.env'
    worker_env = tmp_path / 'worker.env'
    supervisor_env = tmp_path.parent / 'supervisor.env'
    engine_env.write_text(f'MODEL_NAME={model}\nENGINE_MAX_NUM_SEQS=16\n')
    worker_env.write_text('WORKER_MAX_INFLIGHT=16\n')
    supervisor_env.write_text('SAFE_TEST=1\n')
    for path in (engine_env, worker_env, supervisor_env): path.chmod(0o600)
    services = [{'role': 'engine', 'name': 'qwen235b', 'env_file': str(engine_env), 'gpu': '0,1,2,3'},
                {'role': 'worker', 'name': 'qwen235b', 'env_file': str(worker_env)}]
    manifest = runtime / 'services-primary.json'
    manifest.write_text(json.dumps({'services': services}, indent=4) + '\n')
    manifest.chmod(0o600)
    upstream = runtime / 'benchmark_moe_v0102.py'; upstream.write_text('# test pinned source\n')
    monkeypatch.setattr(module, 'ROOT', tmp_path)
    monkeypatch.setattr(module, 'UPSTREAM_SHA', module.digest(upstream))
    monkeypatch.setattr(module, 'load_manifest', lambda _: (manifest, services, {}))
    monkeypatch.setattr(module, 'service_identity', lambda root, service: {'pid': 12, 'start_ticks': '34', 'cwd': str(root)})
    monkeypatch.setattr('sys.argv', ['moe_stopped_window'])
    module.main()
    run = runtime / 'qwen235b-moe-window-v1'
    originals = {p.name: p.read_bytes() for p in run.iterdir() if p.name != 'window.lock'}
    assert originals['original-manifest.json'] == manifest.read_bytes()
    module.main()
    assert originals == {p.name: p.read_bytes() for p in run.iterdir() if p.name != 'window.lock'}
    worker_env.write_text('WORKER_MAX_INFLIGHT=16\nCHANGED=1\n')
    with pytest.raises(RuntimeError, match='refusing snapshot overwrite'):
        module.main()
    assert originals == {p.name: p.read_bytes() for p in run.iterdir() if p.name != 'window.lock'}
