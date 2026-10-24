import fcntl
import hashlib
import io
import json
import os
import py_compile
import subprocess
import sys
import tarfile

import pytest

from ops.ha import deploy_adaptive_worker_source as source


def archive(tmp_path, records):
    memory = io.BytesIO()
    with tarfile.open(fileobj=memory, mode='w:gz') as bundle:
        for name, payload, kind in records:
            info = tarfile.TarInfo(name)
            info.type = kind
            info.size = len(payload) if kind == tarfile.REGTYPE else 0
            bundle.addfile(info, io.BytesIO(payload) if info.size else None)
    data = memory.getvalue()
    path = tmp_path / 'six.tar.gz'
    path.write_bytes(data); path.chmod(0o600)
    return path, hashlib.sha256(data).hexdigest()


def records():
    return [(name, b'VALUE = 2\n', tarfile.REGTYPE) for name in sorted(source.PATHS)]


def test_exact_archive_compiles_without_executing(tmp_path):
    rows = records(); rows[0] = (rows[0][0], b'raise RuntimeError("must not run")\n', tarfile.REGTYPE)
    path, sha = archive(tmp_path, rows)
    assert set(source.archive_sources(path, sha)) == source.PATHS


@pytest.mark.parametrize('extra', [
    ('worker/._runtime.py', b'', tarfile.REGTYPE),
    ('../worker/runtime.py', b'', tarfile.REGTYPE),
    ('worker/runtime.py', b'x=1', tarfile.REGTYPE),
    ('unrelated', b'', tarfile.SYMTYPE),
])
def test_archive_rejects_unknown_duplicate_traversal_and_link(tmp_path, extra):
    path, sha = archive(tmp_path, records() + [extra])
    with pytest.raises(ValueError): source.archive_sources(path, sha)


def test_archive_rejects_hash_and_syntax(tmp_path):
    path, sha = archive(tmp_path, records())
    with pytest.raises(ValueError): source.archive_sources(path, '0' * 64)
    rows = records(); rows[0] = (rows[0][0], b'not python : :\n', tarfile.REGTYPE)
    path, sha = archive(tmp_path, rows)
    with pytest.raises(SyntaxError): source.archive_sources(path, sha)


@pytest.fixture
def deployment(tmp_path):
    root = tmp_path / 'app'; runtime = root / '.runtime'; runtime.mkdir(parents=True)
    (root / 'worker').mkdir()
    run = runtime / 'source-op'; run.mkdir(mode=0o700)
    services = []
    for role, name in [('api', 'a'), ('api', 'b'), ('gateway', 'edge'), ('engine', 'qwen235b'), ('worker', 'qwen235b')]:
        env = runtime / (role + '-' + name + '.env'); env.write_text('SAFE_OPTION=unchanged\n'); env.chmod(0o600)
        if role == 'worker':
            env.write_text('MODEL_NAME=Qwen/Qwen3-235B-A22B-Instruct-2507-FP8\nWORKER_MAX_INFLIGHT=16\nWORKER_TOKEN_BUDGET=295040\nENGINE_MAX_MODEL_LEN=18440\n')
        services.append({'role': role, 'name': name, 'env_file': str(env)})
    supervisor = root.parent / 'supervisor.env'; supervisor.write_text('SAFE_OPTION=unchanged\n'); supervisor.chmod(0o600)
    manifest = runtime / 'services-primary.json'; manifest.write_text(json.dumps({'services': services})); manifest.chmod(0o600)
    identities = {s['role'] + '-' + s['name']: {'pid': i + 100, 'start_ticks': str(i), 'cwd': str(root)} for i, s in enumerate(services)}
    identities['supervisor-primary'] = {'pid': 200, 'start_ticks': '200', 'cwd': str(root)}
    for name in source.PATHS - {'worker/kv_admission.py'}:
        path = root / name; path.write_bytes(b'VALUE = 1\n'); path.chmod(0o644)
    sources = {name: b'VALUE = 2\n' for name in source.PATHS}
    def make(writer=source.atomic_source, **kwargs):
        return source.SourceDeployment(root=root, run=run, manifest_path=manifest, services=services,
            sources=sources, archive_sha='1' * 64, writer=writer,
            identity_reader=lambda s: dict(identities[s['role'] + '-' + s['name']]), assert_drained=lambda: None, **kwargs)
    return root, run, identities, make


def test_prepare_has_private_complete_snapshots_no_source_or_pid_changes(deployment):
    root, run, ids, make = deployment
    deploy = make()
    assert deploy.report['phase'] == 'prepared'
    assert deploy.report['original_sources']['worker/kv_admission.py'] == {'exists': False}
    assert (run / 'original/worker/runtime.py').read_bytes() == b'VALUE = 1\n'
    assert (root / 'worker/runtime.py').read_bytes() == b'VALUE = 1\n'
    assert not (root / 'worker/kv_admission.py').exists()
    assert (run / 'original/worker/runtime.py').stat().st_mode & 0o777 == 0o600


def test_copy_holds_exact_manage_lock_and_preserves_all_non_source_state(deployment):
    root, run, ids, make = deployment
    calls = []
    def writer(*args):
        with (root / '.runtime/worker-qwen235b.lock').open('a') as lock:
            with pytest.raises(BlockingIOError): fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        calls.append(args[0]); source.atomic_source(*args)
    deploy = make(writer); before = dict(ids); result = deploy.apply()
    assert len(calls) == 6 and ids == before
    assert result['phase'] == 'complete' and result['feature_activated'] is False
    assert result['all_service_pids_environments_manifest_unchanged'] is True
    assert all((root / name).read_bytes() == b'VALUE = 2\n' for name in source.PATHS)
    assert (root / 'worker/runtime.py').stat().st_mode & 0o777 == 0o644


@pytest.mark.parametrize('after_replace', [False, True])
def test_copy_failure_restores_bytes_modes_and_removes_previously_absent_module(deployment, after_replace):
    root, run, ids, make = deployment
    def writer(path, *args):
        if path.name == 'metrics.py':
            if after_replace: source.atomic_source(path, *args)
            raise OSError('injected write failure')
        source.atomic_source(path, *args)
    deploy = make(writer)
    with pytest.raises(OSError): deploy.apply()
    assert deploy.report['phase'] == 'rolled_back'
    for name in source.PATHS - {'worker/kv_admission.py'}:
        assert (root / name).read_bytes() == b'VALUE = 1\n'
        assert (root / name).stat().st_mode & 0o777 == 0o644
    assert not (root / 'worker/kv_admission.py').exists()


def test_foreign_bytes_are_never_overwritten_during_rollback(deployment):
    root, run, ids, make = deployment
    def writer(path, *args):
        source.atomic_source(path, *args)
        if path.name == 'config.py':
            path.write_bytes(b'FOREIGN = True\n')
            raise OSError('foreign actor')
    deploy = make(writer)
    with pytest.raises(OSError): deploy.apply()
    assert deploy.report['phase'] == 'manual_recovery_required'
    assert (root / 'worker/config.py').read_bytes() == b'FOREIGN = True\n'


def test_preflight_source_mutation_and_busy_role_lock_refuse_before_copy(deployment):
    root, run, ids, make = deployment
    deploy = make()
    path = root / '.runtime/worker-qwen235b.lock'
    with path.open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(BlockingIOError): deploy.apply()
    (root / 'worker/config.py').write_bytes(b'FOREIGN = True\n')
    with pytest.raises(RuntimeError): deploy.apply()
    assert deploy.report['phase'] == 'prepared'
    assert (root / 'worker/runtime.py').read_bytes() == b'VALUE = 1\n'


def test_same_second_source_copy_invalidates_timestamp_bytecode(deployment, monkeypatch):
    root, run, ids, make = deployment
    now = 1700000000 * 10**9
    for name in source.PATHS - {'worker/kv_admission.py'}: os.utime(root / name, ns=(now, now))
    monkeypatch.setattr(source.time, 'time_ns', lambda: now)
    deploy = make(); deploy.apply()
    assert (root / 'worker/runtime.py').stat().st_mtime_ns // 10**9 != now // 10**9


def test_prepared_replay_never_adopts_replacement_process_or_overwrites_evidence(deployment):
    root, run, ids, make = deployment
    deploy = make(); original_report = (run / 'report.json').read_bytes()
    previous = json.loads(original_report)
    ids['worker-qwen235b']['pid'] += 1
    with pytest.raises(RuntimeError): make(prepared_report=previous)
    assert (run / 'report.json').read_bytes() == original_report
    assert (root / 'worker/runtime.py').read_bytes() == b'VALUE = 1\n'


def fresh_runtime_value(root):
    return subprocess.check_output([sys.executable, '-I', '-B', '-c',
        'import sys; sys.path.insert(0, sys.argv[1]); import worker.runtime; print(worker.runtime.VALUE)',
        str(root)], text=True).strip()


def unchecked_runtime(root):
    return py_compile.compile(str(root / 'worker/runtime.py'), doraise=True,
        invalidation_mode=py_compile.PycInvalidationMode.UNCHECKED_HASH)


def test_unchecked_hash_cache_evicted_and_fresh_process_loads_new_code(deployment):
    root, run, ids, make = deployment
    cached = unchecked_runtime(root)
    unrelated = root / 'worker/__pycache__/unrelated.cpython-312.pyc'
    unrelated.write_bytes(b'leave this unrelated cache alone')
    assert fresh_runtime_value(root) == '1'
    deploy = make(); report = deploy.apply()
    assert not os.path.exists(cached)
    assert fresh_runtime_value(root) == '2'
    assert unrelated.read_bytes() == b'leave this unrelated cache alone'
    assert 'runtime.' in ''.join(report['bytecode_evicted_on_apply'])


def test_rollback_evicts_new_unchecked_cache_before_loading_original_code(deployment):
    root, run, ids, make = deployment
    def writer(path, *args):
        source.atomic_source(path, *args)
        if path.name == 'runtime.py':
            unchecked_runtime(root)
            assert fresh_runtime_value(root) == '2'
            raise OSError('failure after cached replacement')
    deploy = make(writer)
    with pytest.raises(OSError): deploy.apply()
    assert deploy.report['phase'] == 'rolled_back'
    assert 'runtime.' in ''.join(deploy.report['bytecode_evicted_on_rollback'])
    assert fresh_runtime_value(root) == '1'


@pytest.mark.parametrize('unsafe', ['directory_symlink', 'cache_symlink'])
def test_unsafe_bytecode_precheck_refuses_before_source_writes(deployment, tmp_path, unsafe):
    root, run, ids, make = deployment
    cache = root / 'worker/__pycache__'
    target = tmp_path / 'unrelated'; target.mkdir()
    if unsafe == 'directory_symlink':
        cache.symlink_to(target, target_is_directory=True)
    else:
        cache.mkdir()
        foreign = target / 'foreign.pyc'; foreign.write_bytes(b'leave alone')
        (cache / 'runtime.cpython-312.pyc').symlink_to(foreign)
    deploy = make()
    with pytest.raises(ValueError): deploy.apply()
    assert deploy.report['phase'] == 'prepared'
    assert (root / 'worker/runtime.py').read_bytes() == b'VALUE = 1\n'
    assert not (root / 'worker/kv_admission.py').exists()
