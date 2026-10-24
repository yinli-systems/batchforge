"""History rollout ownership, commit ambiguity and isolated real PostgreSQL checks."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import secrets
import shlex
import tarfile

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
import pytest

from backend.migrate import MIGRATIONS, migrate_database
from ops.ha import deploy_history_retirement as deploy
from ops.ha.set_scheduler_policy import atomic_bytes

ROOT = Path(__file__).resolve().parents[2]


class Manager:
    def __init__(self, ids, root):
        self.ids, self.root, self.calls, self.fail = ids, root, [], None
        self.counter = 1000
    def invoke(self, service, action):
        key = service['role']+'-'+service['name']
        self.calls.append((key, action))
        if self.fail == (key, action):
            self.fail = None
            return None, 'injected'
        if action == 'stop': self.ids[key] = None
        if action == 'start':
            self.counter += 1
            self.ids[key] = {'pid': self.counter, 'start_ticks': str(self.counter), 'cwd': str(self.root)}
        return {'status': action, 'pid': self.ids[key]['pid'] if self.ids[key] else None,
                'running': self.ids[key] is not None}, None
    def readiness(self, service): return True


@pytest.fixture
def prepared(tmp_path, monkeypatch):
    root = tmp_path/'app'
    runtime = root/'.runtime'
    runtime.mkdir(parents=True)
    for name in ('backend/migrations', 'ops/ha'): (root/name).mkdir(parents=True)
    for name in deploy.DEPENDENCIES: (root/name).write_bytes(b'# old helper\n')
    for name in set(deploy.MIGRATION_FILES[:10]) | (deploy.PATHS-{'backend/history_retirement.py', 'backend/migrations/0011_history_retirement.sql'}):
        (root/name).write_bytes(b'VALUE=1\n' if name.endswith('.py') else b'-- immutable fixture\n')
    services = []
    for role, name, port in [('api', 'a', 8082), ('api', 'b', 8083), ('gateway', 'edge', 8080), ('engine', 'qwen235b', 8300), ('worker', 'qwen235b', 9105)]:
        env = runtime/(role+'-'+name+'.env')
        atomic_bytes(env, b'SCHEDULER_POLICY=cost_aware\nRESULT_READERS_ENABLED=true\nRESULT_ARCHIVE_ENABLED=true\n')
        services.append({'role': role, 'name': name, 'port': port, 'env_file': str(env)})
    watchdog = {'role': 'supervisor', 'name': 'primary', 'env_file': str(runtime/'watchdog.env')}
    atomic_bytes(Path(watchdog['env_file']), b'SUPERVISOR_MANIFEST=private\n')
    manifest = runtime/'services.json'
    atomic_bytes(manifest, json.dumps({'services': services}).encode())
    ids = {s['role']+'-'+s['name']: {'pid': n+100, 'start_ticks': str(n), 'cwd': str(root)}
           for n, s in enumerate([*services, watchdog])}
    manager = Manager(ids, root)
    monkeypatch.setattr(deploy, 'proofs', lambda *a, **k: {'output_file_id': 'qa20', 'output_sha256': 'a'*64, 'output_bytes': 10})
    sources = {p: b'VALUE=2\n' if p.endswith('.py') else b'-- candidate\n' for p in deploy.PATHS}
    sources['backend/migrate.py'] = (ROOT/'backend/migrate.py').read_bytes()
    run = runtime/'history-op'
    run.mkdir(mode=0o700)
    def make(**kwargs):
        return deploy.HistoryRoll(root=root, services=services, watchdog=watchdog, manifest_path=manifest,
            report_path=run/'report.json', run=run, sources=sources, bundle_sha='1'*64,
            proof_spec={}, owner_env=runtime/'never-open-owner.env', access_env=runtime/'access.env',
            manager=manager, identity=lambda s: ids[s['role']+'-'+s['name']], assert_drained=lambda: None,
            observe=kwargs.pop('observe', lambda service, version: {'ready': True, 'schema_prefix': version}),
            schema_check=kwargs.pop('schema_check', lambda version: {k: 0 for k in deploy.INVARIANTS}), **kwargs)
    return root, run, ids, manager, sources, make


def test_prepare_is_immutable_private_and_does_not_restart_or_change_sources(prepared):
    root, run, ids, manager, sources, make = prepared
    roll = make()
    assert manager.calls == [] and roll.report['phase'] == 'prepared'
    assert (root/'backend/store.py').read_bytes() == b'VALUE=1\n'
    assert not (root/'backend/history_retirement.py').exists()
    assert (run/'original/backend/store.py').stat().st_mode & 0o777 == 0o600
    previous = json.loads((run/'report.json').read_bytes())
    assert make(previous=previous).report == previous
    ids['api-a']['start_ticks'] = 'foreign'
    with pytest.raises(RuntimeError, match='drift'): make(previous=previous)


@pytest.mark.parametrize('change', ['environment', 'manifest', 'source', 'immutable', 'private-snapshot'])
def test_prepared_apply_rejects_any_bound_drift(prepared, change):
    root, run, ids, manager, sources, make = prepared
    roll = make()
    previous = json.loads((run/'report.json').read_bytes())
    path = {'environment': Path(roll.apis[0]['env_file']), 'manifest': roll.manifest_path,
            'source': root/'backend/store.py', 'immutable': root/'backend/schema.sql',
            'private-snapshot': run/'original/backend/store.py'}[change]
    if change in {'environment', 'manifest', 'private-snapshot'}: atomic_bytes(path, b'FOREIGN=1\n')
    else: path.write_bytes(b'FOREIGN=1\n')
    with pytest.raises((ValueError, RuntimeError)): make(previous=previous)
    assert manager.calls == []


def test_success_compatibility_roll_then_schema11_preserves_inference_and_api_env(prepared):
    root, run, ids, manager, sources, make = prepared
    observations = []
    roll = make(observe=lambda s, version: observations.append((s['role'], s['name'], version)) or {'ready': True})
    protected = {key: dict(value) for key, value in ids.items() if key.startswith(('engine-', 'worker-', 'gateway-'))}
    envs = {path: path.read_bytes() for path in roll.original}
    def migration():
        assert observations == [('api', 'a', 10), ('api', 'b', 10)]
        roll._mark_commit({'all_ledgers': 0})
        return {'all_ledgers': 0}
    roll.migrate = migration
    roll.apply()
    assert roll.report['phase'] == 'complete' and roll.report['watchdog_restored']
    assert {key: ids[key] for key in protected} == protected
    assert all(path.read_bytes() == raw for path, raw in envs.items())
    assert all(not key.startswith(('engine-', 'worker-', 'gateway-')) for key, _ in manager.calls)
    assert (root/'backend/history_retirement.py').read_bytes() == sources['backend/history_retirement.py']
    assert [('api', 'a', 11), ('api', 'b', 11), ('gateway', 'edge', 11)] == observations[2:5]


def test_precommit_failure_restores_old_registry_and_removes_only_new_sources(prepared):
    root, run, ids, manager, sources, make = prepared
    roll = make()
    roll.migrate = lambda: (_ for _ in ()).throw(RuntimeError('before commit'))
    with pytest.raises(RuntimeError, match='before commit'): roll.apply()
    assert roll.report['phase'] == 'rolled_back' and roll.report['watchdog_restored']
    assert (root/'backend/migrate.py').read_bytes() == b'VALUE=1\n'
    assert not (root/'backend/history_retirement.py').exists()
    assert not (root/'backend/migrations/0011_history_retirement.sql').exists()


def test_commit_ambiguity_never_restores_old_registry_even_if_database_stayed10(prepared):
    root, run, ids, manager, sources, make = prepared
    def schema(version):
        if version != 10: raise RuntimeError('still schema10')
        return {k: 0 for k in deploy.INVARIANTS}
    roll = make(schema_check=schema)
    def ambiguous():
        roll._mark_commit({'all_ledgers': 0})
        raise OSError('lost COMMIT acknowledgement')
    roll.migrate = ambiguous
    with pytest.raises(OSError): roll.apply()
    assert roll.report['phase'] == 'forward_repair_required' and roll.report['watchdog_restored']
    assert roll.report['recovery_schema_prefix'] == 10
    assert (root/'backend/migrate.py').read_bytes() == sources['backend/migrate.py']
    assert (root/'backend/migrations/0011_history_retirement.sql').exists()


def test_failed_recovery_verification_keeps_watchdog_stopped(prepared):
    root, run, ids, manager, sources, make = prepared
    def observe(service, version): raise RuntimeError('checksum mismatch')
    roll = make(observe=observe)
    with pytest.raises(RuntimeError): roll.apply()
    assert roll.report['phase'] == 'manual_recovery_required'
    assert ids['supervisor-primary'] is None and not roll.report['watchdog_restored']
    assert ('supervisor-primary', 'start') not in manager.calls


def test_foreign_source_after_partial_copy_is_preserved_and_watchdog_stays_stopped(prepared):
    root, run, ids, manager, sources, make = prepared
    def writer(path, raw, mode, mtime):
        if path.name == 'store.py':
            path.write_bytes(b'FOREIGN=True\n')
            raise OSError('injected foreign replacement')
        deploy.atomic_source(path, raw, mode, mtime)
    roll = make(writer=writer)
    with pytest.raises(OSError): roll.apply()
    assert roll.report['phase'] == 'manual_recovery_required'
    assert (root/'backend/store.py').read_bytes() == b'FOREIGN=True\n'
    assert ids['supervisor-primary'] is None


def test_cache_evicts_only_candidate_modules_and_rejects_cache_symlinks(prepared):
    root, run, ids, manager, sources, make = prepared
    cache = root/'backend/__pycache__'
    cache.mkdir()
    scoped = cache/'store.cpython-311.pyc'
    other = cache/'app.cpython-311.pyc'
    scoped.write_bytes(b'scoped unchecked hash cache')
    other.write_bytes(b'unrelated preserved')
    assert deploy.evict_cache(root) == [scoped.name]
    assert other.read_bytes() == b'unrelated preserved'
    scoped.symlink_to(other)
    with pytest.raises(ValueError): deploy.evict_cache(root)
    assert other.exists()


def test_api_locks_are_nonblocking_and_reject_nonprivate_roles(prepared):
    import fcntl
    root, run, ids, manager, sources, make = prepared
    with deploy.api_locks(root):
        with pytest.raises(BlockingIOError):
            with deploy.api_locks(root): pass
    (root/'.runtime/api-a.lock').chmod(0o644)
    with pytest.raises(ValueError):
        with deploy.api_locks(root): pass


def test_bundled_registry_validation_is_ram_only_and_requires_exact_complete_prefix(prepared, monkeypatch):
    root, run, ids, manager, sources, make = prepared
    migrations = deploy.known_migrations(root, sources)
    monkeypatch.setattr(deploy.psycopg, 'connect', lambda *a, **k: (_ for _ in ()).throw(AssertionError('no connection')))
    deploy.validate_bundled_registry(root, sources, migrations)
    wrong = dict(sources, **{'backend/migrate.py': sources['backend/migrate.py'].replace(b'SCHEMA_VERSION = 11', b'SCHEMA_VERSION = 10')})
    with pytest.raises(ValueError): deploy.validate_bundled_registry(root, wrong, migrations)
    wrong['backend/migrate.py'] = sources['backend/migrate.py'].replace(b"(11, '0011_history_retirement.sql')", b"(11, '0010_auxiliary_metadata_admission.sql')")
    with pytest.raises(ValueError): deploy.validate_bundled_registry(root, wrong, migrations)


def test_bundle_is_exact_regular_hashed_and_never_extracts_unknown_paths(tmp_path):
    def archive(extra=None):
        memory = io.BytesIO()
        with tarfile.open(fileobj=memory, mode='w:gz') as output:
            for name in [*sorted(deploy.PATHS), *([extra] if extra else [])]:
                raw = b'VALUE=2\n' if name.endswith('.py') else b'-- reviewed\n'
                member = tarfile.TarInfo(name)
                member.size = len(raw)
                output.addfile(member, io.BytesIO(raw))
        path = tmp_path/'candidate.tar.gz'
        atomic_bytes(path, memory.getvalue())
        return path, deploy.digest(memory.getvalue())
    path, sha = archive()
    assert set(deploy.archive_sources(path, sha)) == deploy.PATHS
    with pytest.raises(ValueError): deploy.archive_sources(path, '0'*64)
    for name in ('../outside', 'backend/app.py', 'backend/store.py'):
        path, sha = archive(name)
        with pytest.raises(ValueError): deploy.archive_sources(path, sha)


def synthetic_proof_spec(tmp_path):
    """Self-contained validator metadata; never a deployed production proof."""
    snapshot = tmp_path/'same-pod.dump'
    atomic_bytes(snapshot, b'fixture PostgreSQL archive')
    sha, size = deploy.digest(snapshot.read_bytes()), snapshot.stat().st_size
    zero = {key: 0 for key in deploy.INVARIANTS}
    records = {
        'fresh_backup': {'observed_at': datetime.now(timezone.utc).isoformat(), 'compatible': True,
            'archive_list_verified': True, 'database_modified': False, 'snapshot_path': str(snapshot),
            'snapshot_sha256': sha, 'snapshot_bytes': size},
        'current_restore': {'passed': True, 'restore_verified': True, 'schema_version': 10,
            'snapshot_sha256': sha, 'snapshot_bytes': size, 'live_database_modified': False,
            'temporary_cluster_stopped': True, 'temporary_cluster_removed': True,
            'migration_checksums': {str(n): checksum for n, _, checksum in MIGRATIONS if n <= 10}, **zero},
        'production': {
            'synthetic_test_fixture': True,
            'phase': 'passed',
            'checks': {name: True for name in (
                'cohort_completed', 'accepted_once', 'accepted_usage_matches',
                'archive_published', 'archive_checksum_verified', 'payloads_cleared',
                'api_a_download_verified', 'api_b_download_verified',
                'gateway_download_verified', 'downloads_byte_identical')},
            'after': {'batch': {'total_count': 20, 'completed_count': 20,
                                'output_file_id': 'synthetic-qa20-output'},
                      'tasks': {'payload_cleared': 20}},
            'original_output_sha256': deploy.digest(b'synthetic archived QA20 output fixture'),
            'original_output_bytes': len(b'synthetic archived QA20 output fixture'),
            'downloads': [{'port': port,
                           'sha256': deploy.digest(b'synthetic archived QA20 output fixture'),
                           'bytes': len(b'synthetic archived QA20 output fixture'),
                           'byte_identical': True} for port in (8080, 8082, 8083)]}}
    spec = {}
    def write():
        for name, record in records.items():
            path = tmp_path/(name+'.json')
            atomic_bytes(path, json.dumps(record).encode())
            spec[name] = {'path': str(path), 'sha256': deploy.digest(path.read_bytes())}
        return spec
    return records, write, snapshot


@pytest.fixture
def proof_spec(tmp_path):
    return synthetic_proof_spec(tmp_path)


def actual_migrations(): return [(n, statement.encode()) for n, statement, _ in MIGRATIONS]


def test_proof_fixture_is_self_contained_in_code_only_checkout(tmp_path, monkeypatch):
    code_only = tmp_path/'code-only'
    code_only.mkdir()
    monkeypatch.setitem(globals(), 'ROOT', code_only)
    records, write, snapshot = synthetic_proof_spec(tmp_path)
    assert not (code_only/'ops/reports').exists()
    assert records['production']['synthetic_test_fixture'] is True
    result = deploy.proofs(write(), actual_migrations())
    assert result['output_file_id'] == 'synthetic-qa20-output'
    assert result['output_sha256'] == records['production']['original_output_sha256']


def test_fresh_same_pod_backup_and_actual_restore_do_not_require_or_claim_export(proof_spec):
    records, write, snapshot = proof_spec
    result = deploy.proofs(write(), actual_migrations())
    assert result['fresh_snapshot_sha256'] == deploy.digest(snapshot.read_bytes())
    assert result['off_pod_copy_verified_by_operation'] is False
    assert result['off_pod_export'] == 'not_authorized_not_attempted'


@pytest.mark.parametrize('change', ['stale', 'checksum', 'size', 'mode', 'restore', 'ledger', 'schema', 'download'])
def test_bad_backup_restore_or_historical_checksum_fails_closed(proof_spec, change):
    records, write, snapshot = proof_spec
    if change == 'stale': records['fresh_backup']['observed_at'] = (datetime.now(timezone.utc)-timedelta(hours=2)).isoformat()
    if change == 'checksum': snapshot.write_bytes(b'x'*snapshot.stat().st_size)
    if change == 'size': records['fresh_backup']['snapshot_bytes'] += 1
    if change == 'mode': snapshot.chmod(0o644)
    if change == 'restore': records['current_restore']['temporary_cluster_stopped'] = False
    if change == 'ledger': records['current_restore']['auxiliary_ledger_mismatches'] = 1
    if change == 'schema': records['current_restore']['schema_version'] = 11
    if change == 'download': records['production']['downloads'][1]['sha256'] = '0'*64
    with pytest.raises(ValueError): deploy.proofs(write(), actual_migrations())


@pytest.fixture
def database():
    source = os.environ.get('TEST_DATABASE_URL')
    if not source: pytest.skip('Requires isolated real PostgreSQL')
    name = 'bf_history_roll_'+secrets.token_hex(8)
    with psycopg.connect(source, autocommit=True) as conn:
        conn.execute(sql.SQL('CREATE DATABASE {} TEMPLATE template0').format(sql.Identifier(name)))
        if not conn.execute("SELECT 1 FROM pg_roles WHERE rolname='bf_runtime'").fetchone(): conn.execute('CREATE ROLE bf_runtime')
    dsn = make_conninfo(source, dbname=name)
    try:
        migrate_database(dsn, 'history-roll-fixture-key', target_version=10)
        yield dsn
    finally:
        with psycopg.connect(source, autocommit=True) as conn:
            conn.execute(sql.SQL('DROP DATABASE {} WITH(FORCE)').format(sql.Identifier(name)))


def test_real_atomic11_preserves_ledgers_has_disabled_policy_and_no_runtime_retirement(database):
    seen = []
    with psycopg.connect(database, autocommit=True) as conn:
        result = deploy.migrate_atomic(conn, actual_migrations(), (ROOT/'ops/ha/runtime_grants.sql').read_bytes(), lambda evidence: seen.append(evidence))
        deploy.validate_prefix(conn, actual_migrations(), 11)
        assert conn.execute('SELECT count(*) FROM tenant_history_retirement_policy').fetchone()[0] == 0
        assert result['runtime_retire_execute'] is False
        assert all(result[key] == 0 for key in deploy.INVARIANTS)
    assert len(seen) == 1


def test_real_precommit_failure_rolls_ddl_ledger_and_grants_back_together(database):
    def fail(evidence): raise RuntimeError('injected before COMMIT')
    with psycopg.connect(database, autocommit=True) as conn:
        with pytest.raises(RuntimeError):
            deploy.migrate_atomic(conn, actual_migrations(), (ROOT/'ops/ha/runtime_grants.sql').read_bytes(), fail)
        deploy.validate_prefix(conn, actual_migrations(), 10)
        assert conn.execute("SELECT to_regclass('public.tenant_history_retirement_policy')").fetchone()[0] is None


def test_real_owner_rr_default_is_overridden_only_for_migration_transaction(database):
    with psycopg.connect(database, autocommit=True) as owner:
        owner.execute('SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ')
        assert owner.execute("SELECT current_setting('transaction_isolation')").fetchone()[0] == 'repeatable read'
        observed = []
        def before_commit(evidence):
            isolation, default = owner.execute("SELECT current_setting('transaction_isolation'),current_setting('default_transaction_isolation')").fetchone()
            observed.append((isolation, default))
            assert isolation == 'read committed' and default == 'repeatable read'
        deploy.migrate_atomic(owner, actual_migrations(), (ROOT/'ops/ha/runtime_grants.sql').read_bytes(), before_commit)
        assert observed == [('read committed', 'repeatable read')]
        assert owner.execute("SELECT current_setting('transaction_isolation'),current_setting('default_transaction_isolation')").fetchone() == ('repeatable read', 'repeatable read')
        deploy.validate_prefix(owner, actual_migrations(), 11)


def test_real_gate_fails_fast_without_waiting_for_reader_or_changing_schema(database):
    with psycopg.connect(database) as reader, psycopg.connect(database, autocommit=True) as owner:
        reader.execute('SELECT count(*) FROM batches')
        with pytest.raises(psycopg.errors.LockNotAvailable):
            deploy.migrate_atomic(owner, actual_migrations(), (ROOT/'ops/ha/runtime_grants.sql').read_bytes(), lambda evidence: None)
        deploy.validate_prefix(owner, actual_migrations(), 10)
        assert owner.execute("SELECT to_regclass('public.tenant_history_retirement_policy')").fetchone()[0] is None


def test_real_preflight_uses_one_readonly_snapshot_across_concurrent_ledger_commit(database, tmp_path, monkeypatch):
    """A publisher-style atomic rows+counter commit cannot split the proof."""
    with psycopg.connect(database, autocommit=True) as owner:
        deploy.migrate_atomic(owner, actual_migrations(), (ROOT/'ops/ha/runtime_grants.sql').read_bytes(), lambda evidence: None)
        # This role belongs solely to this disposable Unix-only test cluster.
        owner.execute('ALTER ROLE bf_runtime LOGIN')
    runtime = make_conninfo(database, user='bf_runtime')
    env = tmp_path/'runtime.env'
    atomic_bytes(env, ('DATABASE_URL='+shlex.quote(runtime)+'\n').encode())
    roll = object.__new__(deploy.HistoryRoll)
    roll.apis = [{'env_file': str(env)}]
    roll.migrations = actual_migrations()
    original = deploy.admission_invariants
    observed = []
    def concurrent_commit(conn, version):
        assert conn.execute("SELECT current_setting('transaction_isolation'),current_setting('transaction_read_only')").fetchone() == ('repeatable read', 'on')
        before = conn.execute('SELECT count(*) FROM files').fetchone()[0]
        with psycopg.connect(database, autocommit=True) as owner:
            owner.execute("INSERT INTO files(id,tenant_id,filename,kind) VALUES('concurrent-ledger-file','default','fixture','input')")
        after = conn.execute('SELECT count(*) FROM files').fetchone()[0]
        observed.append((before, after))
        assert after == before
        return original(conn, version)
    monkeypatch.setattr(deploy, 'admission_invariants', concurrent_commit)
    result = roll._schema_check(11)
    assert observed == [(0, 0)] and all(result[key] == 0 for key in deploy.INVARIANTS)
    with psycopg.connect(database) as owner:
        assert owner.execute('SELECT count(*) FROM files').fetchone()[0] == 1


def test_real_legacy_runtime_truncate_grant_rejects_migration_atomically(database):
    with psycopg.connect(database,autocommit=True) as owner:
        owner.execute('GRANT TRUNCATE ON attempts TO bf_runtime')
        try:
            assert owner.execute("SELECT has_table_privilege('bf_runtime','attempts','DELETE'),has_table_privilege('bf_runtime','attempts','TRUNCATE')").fetchone()==(False,True)
            with pytest.raises(RuntimeError,match='Runtime history administration privileges'):
                deploy.migrate_atomic(owner,actual_migrations(),(ROOT/'ops/ha/runtime_grants.sql').read_bytes(),lambda evidence:None)
            deploy.validate_prefix(owner,actual_migrations(),10)
            assert owner.execute("SELECT to_regclass('public.tenant_history_retirement_policy')").fetchone()[0] is None
        finally:
            owner.execute('REVOKE TRUNCATE ON attempts FROM bf_runtime')
        result=deploy.migrate_atomic(owner,actual_migrations(),(ROOT/'ops/ha/runtime_grants.sql').read_bytes(),lambda evidence:None)
        assert result['runtime_history_delete'] is False and result['runtime_history_truncate'] is False


def test_real_noinherit_owner_membership_rejects_migration_atomically(database):
    # Role changes belong solely to this disposable isolated PostgreSQL cluster.
    with psycopg.connect(database,autocommit=True) as owner:
        owner_role=owner.execute('SELECT current_user').fetchone()[0]
        inherited=owner.execute("SELECT rolinherit FROM pg_roles WHERE rolname='bf_runtime'").fetchone()[0]
        owner.execute('ALTER ROLE bf_runtime NOINHERIT')
        granted=False
        try:
            owner.execute(sql.SQL('GRANT {} TO bf_runtime').format(sql.Identifier(owner_role)))
            granted=True
            assert owner.execute("SELECT pg_has_role('bf_runtime',%s,'USAGE'),pg_has_role('bf_runtime',%s,'MEMBER')",(owner_role,owner_role)).fetchone()==(False,True)
            with pytest.raises(RuntimeError,match='Runtime history administration privileges'):
                deploy.migrate_atomic(owner,actual_migrations(),(ROOT/'ops/ha/runtime_grants.sql').read_bytes(),lambda evidence:None)
            deploy.validate_prefix(owner,actual_migrations(),10)
            assert owner.execute("SELECT to_regclass('public.tenant_history_retirement_policy')").fetchone()[0] is None
        finally:
            if granted:owner.execute(sql.SQL('REVOKE {} FROM bf_runtime').format(sql.Identifier(owner_role)))
            owner.execute('ALTER ROLE bf_runtime '+('INHERIT' if inherited else 'NOINHERIT'))
        result=deploy.migrate_atomic(owner,actual_migrations(),(ROOT/'ops/ha/runtime_grants.sql').read_bytes(),lambda evidence:None)
        assert result['runtime_owner_membership'] is False
