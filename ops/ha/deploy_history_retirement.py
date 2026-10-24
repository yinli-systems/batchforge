"""Prepare a guarded API-only v10 -> v11 rollout; no retirement policy enabled.

Only --apply after an immutable preparation may roll APIs and migrate. This
controller never stops inference or exports a backup. An attempted COMMIT is
forward-only even if its acknowledgement is lost. Credentials remain private.
"""
from __future__ import annotations

import argparse
import ast
from contextlib import ExitStack, contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import io
import json
import os
from pathlib import Path
import re
import stat
import sys
import tarfile
import time

import httpx
import psycopg
from psycopg import sql

from ops.ha.adaptive_qwen235b import private_operation_directory
from ops.ha.deploy_adaptive_worker_source import atomic_source, source_state
from ops.ha.deploy_storage_admission import bound_owner_dsn, validate_prefix
from ops.ha.finalize_api_policy import values
from ops.ha.local_pg17_restore import admission_invariants
from ops.ha.set_scheduler_policy import PolicyRoll, atomic_bytes, owned_identity, timestamp
from ops.supervise import ROOT, load_manifest, private_read

PATHS = frozenset({'backend/store.py', 'backend/migrate.py', 'backend/retention.py',
    'backend/history_retirement.py', 'backend/migrations/0011_history_retirement.sql',
    'ops/ha/runtime_grants.sql'})
MIGRATION_FILES = ('backend/schema.sql', 'backend/migrations/0002_sql_completion.sql',
    'backend/migrations/0003_sql_claim.sql', 'backend/migrations/0004_sql_failure_recovery.sql',
    'backend/migrations/0005_blob_storage.sql', 'backend/migrations/0006_cost_scheduler.sql',
    'backend/migrations/0007_runtime_registration_guard.sql', 'backend/migrations/0008_result_archival.sql',
    'backend/migrations/0009_retention_admission.sql', 'backend/migrations/0010_auxiliary_metadata_admission.sql',
    'backend/migrations/0011_history_retirement.sql')
DEPENDENCIES = ('ops/manage.py', 'ops/supervise.py', 'ops/ha/set_scheduler_policy.py',
    'ops/ha/deploy_adaptive_worker_source.py', 'ops/ha/adaptive_qwen235b.py',
    'ops/ha/deploy_storage_admission.py', 'ops/ha/finalize_api_policy.py',
    'ops/ha/retune_full_gpu.py', 'ops/ha/local_pg17_restore.py')
STEMS = {Path(p).stem for p in PATHS if p.endswith('.py')}
BYTECODE = re.compile(r'^(?:' + '|'.join(sorted(STEMS)) + r')\.[A-Za-z0-9_-]+(?:\.opt-[0-9]+)?\.pyc$')
INVARIANTS = ('batch_count_state_usage_mismatches', 'accepted_attempt_count_usage_mismatches',
    'tenant_queue_pending_mismatches', 'unvalidated_constraints', 'retention_ledger_mismatches',
    'retention_undercharged_tasks', 'retention_missing_tenant_ledgers', 'auxiliary_ledger_mismatches')
RETIRE_FUNCTIONS = ('batchforge_retire_history_page(text,text,integer,boolean,text)',
                    'batchforge_retire_idempotency_page(text,bigint,integer,boolean)')
HISTORY_TABLES = ('batches', 'tasks', 'attempts', 'idempotency', 'files', 'workers',
    'blob_objects', 'result_archive_runs', 'result_archive_files', 'result_archive_chunks',
    'tenant_history_retirement_policy', 'retired_attempt_hourly')


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def archive_sources(path, expected):
    if not re.fullmatch('[0-9a-f]{64}', expected):
        raise ValueError('Reviewed bundle SHA256 required')
    raw = private_read(path, 4*1024**2)
    if digest(raw) != expected:
        raise ValueError('Bundle SHA256 differs')
    result = {}
    with tarfile.open(fileobj=io.BytesIO(raw), mode='r:*') as archive:
        for member in archive:
            if member.name not in PATHS or member.name in result or not member.isreg() or not 0 <= member.size <= 512*1024:
                raise ValueError('Exactly the six reviewed regular history-retirement paths are required')
            source = archive.extractfile(member)
            if source is None:
                raise ValueError('Missing archive member')
            value = source.read(512*1024+1)
            if len(value) != member.size:
                raise ValueError('Source size differs')
            if member.name.endswith('.py'):
                compile(value, member.name, 'exec', dont_inherit=True)
            result[member.name] = value
    if set(result) != PATHS:
        raise ValueError('Incomplete source bundle')
    return result


def private_sources(root):
    for name in ('backend', 'backend/migrations', 'ops', 'ops/ha'):
        info = (root/name).lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o022:
            raise ValueError('Source directory is unsafe')


def cache_entries(root):
    directory = root/'backend/__pycache__'
    if not directory.exists() and not directory.is_symlink():
        return []
    info = directory.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o022:
        raise ValueError('Backend cache directory is unsafe')
    result = []
    for path in directory.iterdir():
        if not BYTECODE.fullmatch(path.name):
            continue
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o022:
            raise ValueError('Scoped backend cache is unsafe')
        result.append((path, info.st_dev, info.st_ino))
    return result


def evict_cache(root):
    entries = cache_entries(root)
    for path, device, inode in entries:
        info = path.lstat()
        if (info.st_dev, info.st_ino) != (device, inode):
            raise RuntimeError('Backend cache changed during eviction')
        path.unlink()
    if entries:
        descriptor = os.open(entries[0][0].parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    return [path.name for path, _, _ in entries]


@contextmanager
def api_locks(root):
    with ExitStack() as stack:
        for name in ('a', 'b'):
            fd = os.open(root/'.runtime'/('api-'+name+'.lock'), os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
                os.close(fd)
                raise ValueError('API role lock must be private and owned')
            stream = stack.enter_context(os.fdopen(fd, 'w'))
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


def known_migrations(root, sources):
    return [(n, sources[name] if n == 11 else (root/name).read_bytes())
            for n, name in enumerate(MIGRATION_FILES, 1)]


def validate_bundled_registry(root, sources, migrations):
    """Evaluate only the pure registry-building AST using a read-only RAM FS.

    The running controller may have imported the old v10 backend already. Do
    not import/reload the candidate module, evaluate imports/main/functions,
    instantiate an app, or open a database to learn its migration registry.
    """
    names = {'SCHEMA_VERSION', 'SCHEMA_SQL', 'SCHEMA_CHECKSUM', 'SQL_COMPLETION_SQL', 'SQL_CLAIM_SQL', 'MIGRATIONS'}
    parsed = ast.parse(sources['backend/migrate.py'], filename='candidate/backend/migrate.py')
    selected = []
    for node in parsed.body:
        if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id in names for target in node.targets):
            selected.append(node)
        elif isinstance(node, ast.For) and any(isinstance(child, ast.AugAssign) and isinstance(child.target, ast.Name)
                and child.target.id == 'MIGRATIONS' for child in node.body):
            selected.append(node)
    module = ast.Module(body=selected, type_ignores=[])
    allowed_types = (ast.Module, ast.Assign, ast.AugAssign, ast.For, ast.Name, ast.Load, ast.Store,
        ast.Tuple, ast.Constant, ast.Add, ast.Call, ast.Attribute)
    allowed_names = names | {'Path', 'hashlib', '__file__', 'version', 'filename', 'statement'}
    allowed_attributes = {'with_name', 'joinpath', 'read_text', 'encode', 'hexdigest', 'sha256'}
    for node in ast.walk(module):
        if (not isinstance(node, allowed_types) or isinstance(node, ast.Name) and node.id not in allowed_names
                or isinstance(node, ast.Attribute) and (node.attr not in allowed_attributes or not isinstance(node.ctx, ast.Load))):
            raise ValueError('Candidate migration registry must be a pure reviewed construction')
    raw = {str(root/name): statement for name, (_, statement) in zip(MIGRATION_FILES, migrations)}
    class MemoryPath:
        def __init__(self, path): self.path = Path(path)
        def with_name(self, name): return MemoryPath(self.path.with_name(name))
        def joinpath(self, name): return MemoryPath(self.path/name)
        def read_text(self):
            if str(self.path) not in raw:
                raise ValueError('Candidate registry reads an unknown migration path')
            return raw[str(self.path)].decode()
    namespace = {'__builtins__': {}, 'Path': MemoryPath, 'hashlib': hashlib,
                 '__file__': str(root/'backend/migrate.py')}
    exec(compile(module, 'candidate-registry-only', 'exec', dont_inherit=True), namespace)
    expected = tuple((n, statement.decode(), digest(statement)) for n, statement in migrations)
    if namespace.get('SCHEMA_VERSION') != 11 or namespace.get('MIGRATIONS') != expected:
        raise ValueError('Candidate registry differs from exact immutable1..11 prefix')


def snapshot_digest(path, size):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, 'rb') as source:
        info = os.fstat(source.fileno())
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                or stat.S_IMODE(info.st_mode) != 0o600 or info.st_size != size):
            raise ValueError('Same-Pod backup must be an owned mode0600 exact-size regular file')
        return hashlib.file_digest(source, 'sha256').hexdigest()


def proofs(spec, migrations, now=None):
    now = now or datetime.now(timezone.utc)
    if set(spec) != {'fresh_backup', 'current_restore', 'production'}:
        raise ValueError('Three exact same-Pod backup/restore/archive proofs required')
    records, hashes = {}, {}
    for name, entry in spec.items():
        if set(entry) != {'path', 'sha256'}:
            raise ValueError('Proof path and reviewed SHA256 required')
        raw = private_read(Path(entry['path']), 2*1024**2)
        if digest(raw) != entry['sha256']:
            raise ValueError('Proof SHA256 differs')
        records[name], hashes[name] = json.loads(raw), digest(raw)
    backup, restore, production = (records[k] for k in ('fresh_backup', 'current_restore', 'production'))
    observed = datetime.fromisoformat(backup.get('observed_at', '').replace('Z', '+00:00'))
    snapshot = backup.get('snapshot_sha256', '')
    size = backup.get('snapshot_bytes', 0)
    if (observed.tzinfo is None or not 0 <= (now-observed).total_seconds() <= 3600
            or not backup.get('compatible') or not backup.get('archive_list_verified')
            or backup.get('database_modified') is not False or not re.fullmatch('[0-9a-f]{64}', snapshot)
            or type(size) is not int or not 0 < size <= 512*1024**2
            or snapshot_digest(Path(backup['snapshot_path']), size) != snapshot):
        raise ValueError('Fresh compatible exact same-Pod backup required')
    if (not restore.get('passed') or not restore.get('restore_verified') or restore.get('schema_version') != 10
            or restore.get('snapshot_sha256') != snapshot or restore.get('snapshot_bytes') != size
            or restore.get('migration_checksums') != {str(n): digest(statement) for n, statement in migrations if n <= 10}
            or not restore.get('temporary_cluster_stopped') or not restore.get('temporary_cluster_removed')
            or restore.get('live_database_modified') is not False
            or any(restore.get(k) != 0 for k in INVARIANTS)):
        raise ValueError('Passed exact schema10 same-Pod restore required')
    checks = production.get('checks', {})
    batch = production.get('after', {}).get('batch', {})
    downloads = production.get('downloads', [])
    output_sha, output_bytes = production.get('original_output_sha256', ''), production.get('original_output_bytes', 0)
    if (production.get('phase') != 'passed' or len(checks) != 10 or any(value is not True for value in checks.values())
            or batch.get('total_count') != 20 or batch.get('completed_count') != 20
            or production.get('after', {}).get('tasks', {}).get('payload_cleared') != 20
            or not batch.get('output_file_id') or not re.fullmatch('[0-9a-f]{64}', output_sha)
            or type(output_bytes) is not int or not 0 < output_bytes <= 16*1024**2
            or len(downloads) != 3 or {entry.get('port') for entry in downloads} != {8080, 8082, 8083}
            or any(entry.get('sha256') != output_sha or entry.get('bytes') != output_bytes
                   or entry.get('byte_identical') is not True for entry in downloads)):
        raise ValueError('Passed exact archivedQA20 three-port checksum proof required')
    return {'sha256': hashes, 'fresh_snapshot_sha256': snapshot, 'fresh_snapshot_bytes': size,
        'backup_scope': 'same-Pod archive and isolated local restore; no export',
        'off_pod_copy_verified_by_operation': False, 'off_pod_export': 'not_authorized_not_attempted',
        'output_file_id': batch['output_file_id'], 'output_sha256': output_sha, 'output_bytes': output_bytes}


def runtime_privileges(conn):
    role = 'bf_runtime'
    denied = []
    for table in HISTORY_TABLES:
        for privilege in ('DELETE','TRUNCATE'):
            if conn.execute('SELECT has_table_privilege(%s,%s,%s)', (role, 'public.'+table, privilege)).fetchone()[0]:
                denied.append(privilege.lower()+':'+table)
    for table in ('tenant_history_retirement_policy', 'retired_attempt_hourly'):
        if conn.execute("SELECT has_table_privilege(%s,%s,'INSERT,UPDATE,TRUNCATE')", (role, 'public.'+table)).fetchone()[0]:
            denied.append('mutate:'+table)
    for name in RETIRE_FUNCTIONS:
        if conn.execute("SELECT has_function_privilege(%s,%s,'EXECUTE')", (role, 'public.'+name)).fetchone()[0]:
            denied.append('execute:'+name)
    if conn.execute("SELECT has_schema_privilege(%s,'public','CREATE')", (role,)).fetchone()[0]:
        denied.append('schema_create')
    if conn.execute("SELECT pg_has_role(%s,pg_get_userbyid(relowner),'MEMBER') FROM pg_class WHERE oid='public.tasks'::regclass", (role,)).fetchone()[0]:
        denied.append('owner_membership')
    if denied:
        raise RuntimeError('Runtime history administration privileges must be absent')
    return {'runtime_history_delete': False, 'runtime_history_truncate': False, 'runtime_policy_write': False,
            'runtime_retire_execute': False, 'runtime_owner_membership': False}


def invariants(conn, version):
    result = admission_invariants(conn, version)
    result['batch_count_state_usage_mismatches'] = conn.execute('''WITH actual AS (
        SELECT batch_id,count(*) total,count(*) FILTER(WHERE status='pending') pending,
        count(*) FILTER(WHERE status='running') running,count(*) FILTER(WHERE status='completed') completed,
        count(*) FILTER(WHERE status='failed') failed,count(*) FILTER(WHERE status='cancelled') cancelled,
        coalesce(sum(prompt_tokens),0) prompt,coalesce(sum(completion_tokens),0) completion FROM tasks GROUP BY batch_id)
        SELECT count(*) FROM batches b LEFT JOIN actual a ON a.batch_id=b.id WHERE
        (b.total_count,b.pending_count,b.running_count,b.completed_count,b.failed_count,b.cancelled_count,b.prompt_tokens,b.completion_tokens)
        IS DISTINCT FROM (coalesce(a.total,0),coalesce(a.pending,0),coalesce(a.running,0),coalesce(a.completed,0),coalesce(a.failed,0),coalesce(a.cancelled,0),coalesce(a.prompt,0),coalesce(a.completion,0))''').fetchone()[0]
    result['accepted_attempt_count_usage_mismatches'] = conn.execute('''WITH accepted AS (
        SELECT task_id,count(*) n,sum(prompt_tokens) p,sum(completion_tokens) c FROM attempts WHERE status='completed' GROUP BY task_id)
        SELECT count(*) FROM tasks t LEFT JOIN accepted a ON a.task_id=t.id WHERE
        (t.status='completed' AND (coalesce(a.n,0)<>1 OR t.prompt_tokens<>a.p OR t.completion_tokens<>a.c))
        OR (t.status<>'completed' AND coalesce(a.n,0)<>0)''').fetchone()[0]
    result['tenant_queue_pending_mismatches'] = conn.execute('''SELECT count(*) FROM tenant_queues q WHERE q.pending_count<>
        (SELECT count(*) FROM tasks t WHERE t.tenant_id=q.tenant_id AND t.model=q.model AND t.status='pending')''').fetchone()[0]
    result['unvalidated_constraints'] = conn.execute("SELECT count(*) FROM pg_constraint WHERE connamespace='public'::regnamespace AND NOT convalidated").fetchone()[0]
    if any(result[k] != 0 for k in INVARIANTS):
        raise RuntimeError('History deployment ledger invariant failed')
    if version == 11:
        enabled, retired, markers = conn.execute('''SELECT
            (SELECT count(*) FROM tenant_history_retirement_policy WHERE enabled),
            (SELECT count(*) FROM retired_attempt_hourly),
            (SELECT count(*) FROM batches WHERE history_retirement_started_at IS NOT NULL
                OR history_retired_at IS NOT NULL OR history_retired_tasks<>0)''').fetchone()
        if enabled or retired or markers:
            raise RuntimeError('History retirement must remain disabled and unused during rollout')
        result.update(history_policies_enabled=0, retired_attempt_buckets=0, batch_retirement_markers=0)
        result.update(runtime_privileges(conn))
    return result


def migrate_atomic(conn, migrations, grants, before_commit):
    try:
        # A pre-lock prefix read must not pin an older RR snapshot that hides
        # work committed just before the complete NOWAIT migration gate.
        conn.execute('BEGIN ISOLATION LEVEL READ COMMITTED')
        conn.execute("SET LOCAL lock_timeout='1s'")
        conn.execute("SET LOCAL statement_timeout='60s'")
        if not conn.execute('SELECT pg_try_advisory_xact_lock(%s)', (1936150117,)).fetchone()[0]:
            raise RuntimeError('Migration advisory lock unavailable')
        validate_prefix(conn, migrations, 10)
        conn.execute('LOCK TABLE public.tenants IN EXCLUSIVE MODE NOWAIT')
        conn.execute('LOCK TABLE public.batches,public.idempotency IN ACCESS EXCLUSIVE MODE NOWAIT')
        conn.execute('LOCK TABLE public.tasks,public.attempts IN SHARE ROW EXCLUSIVE MODE NOWAIT')
        if conn.execute("SELECT count(*) FROM tasks WHERE status IN ('pending','running')").fetchone()[0]:
            raise RuntimeError('Active tasks during migration')
        before = invariants(conn, 10)
        for version, statement in migrations:
            if version == 11:
                conn.execute(statement.decode())
                conn.execute('INSERT INTO batchforge_schema_migrations(version,checksum) VALUES(%s,%s)', (version, digest(statement)))
        database = conn.execute('SELECT current_database()').fetchone()[0]
        text = grants.decode()
        token = 'GRANT CONNECT ON DATABASE batchforge TO bf_runtime;'
        if text.count(token) != 1:
            raise ValueError('Unexpected runtime grant script')
        text = text.replace(token, sql.SQL('GRANT CONNECT ON DATABASE {} TO bf_runtime;').format(sql.Identifier(database)).as_string(conn))
        conn.execute(text)
        validate_prefix(conn, migrations, 11)
        after = invariants(conn, 11)
        if {k: after[k] for k in INVARIANTS} != before:
            raise RuntimeError('Migration changed source ledger invariants')
        if conn.execute("SELECT count(*) FROM tasks WHERE status IN ('pending','running')").fetchone()[0]:
            raise RuntimeError('Active tasks before commit')
        before_commit(after)  # fsynced intent BEFORE the ambiguous COMMIT boundary.
        conn.commit()
        return after
    except BaseException:
        conn.rollback()
        raise


class HistoryRoll(PolicyRoll):
    def __init__(self, *, run, sources, bundle_sha, proof_spec, owner_env, access_env,
                 previous=None, migrate=None, observe=None, schema_check=None, writer=atomic_source, **kwargs):
        super().__init__(policy='cost_aware', **kwargs)
        if set(sources) != PATHS:
            raise ValueError('Exactly the six reviewed source paths required')
        self.before = {key: dict(value) if value else None for key, value in self.before.items()}
        self.run, self.sources, self.bundle_sha = run, sources, bundle_sha
        self.owner_env, self.access_env, self.proof_spec = owner_env, access_env, proof_spec
        self.migrations = known_migrations(self.root, sources)
        validate_bundled_registry(self.root, sources, self.migrations)
        self.proofs = proofs(proof_spec, self.migrations)
        self.migrate, self.observe, self.schema_check = migrate or self._migrate, observe or self._observe, schema_check or self._schema_check
        self.writer, self.commit_attempted, self.copied = writer, False, False
        private_sources(self.root)
        cache_entries(self.root)
        self.snapshot, self.original_sources = {}, {}
        for name in sorted(PATHS):
            state, raw = source_state(self.root/name)
            self.snapshot[name] = state
            if raw is not None:
                self.original_sources[name] = raw
        self.watchdog_bytes = private_read(Path(self.watchdog['env_file']))
        self.expected = {self.key(s): dict(self.identity(s)) if self.identity(s) else None for s in [*self.services, self.watchdog]}
        if not all(self.expected.values()):
            raise ValueError('All owned services must be live at preparation')
        self.guard_sources = {name: digest((self.root/name).read_bytes()) for name in (*MIGRATION_FILES[:10], *DEPENDENCIES)}
        self.guard_sources[str(Path(__file__).resolve())] = digest(Path(__file__).read_bytes())
        self.assert_drained()
        prepared_invariants = self.schema_check(10)
        binding = {'bundle_sha256': bundle_sha, 'source_sha256': {n: digest(raw) for n, raw in sources.items()},
            'original_sources': self.snapshot, 'manifest_sha256': digest(self.original_manifest),
            'environment_sha256': {str(p): digest(raw) for p, raw in self.original.items()},
            'protected_environment_sha256': {str(p): checksum for p, checksum in self.protected_env.items()},
            'watchdog_environment_sha256': digest(self.watchdog_bytes), 'owned_identities': self.expected,
            'guard_sources': self.guard_sources, 'proof_evidence': self.proofs}
        if previous is not None:
            if previous.get('phase') != 'prepared' or previous.get('binding') != binding:
                raise RuntimeError('Prepared operation drift; refusing replay')
            for name, raw in self.original_sources.items():
                if private_read(run/'original'/name, 512*1024) != raw:
                    raise RuntimeError('Private source snapshot changed')
            for index, raw in enumerate(self.original.values()):
                if private_read(run/f'api-{index}.env') != raw:
                    raise RuntimeError('Private environment snapshot changed')
            self.report = previous
        else:
            if any(run.iterdir()):
                raise RuntimeError('Operation directory not empty')
            for name, raw in self.original_sources.items():
                path = run/'original'/name
                path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                atomic_bytes(path, raw)
            for index, raw in enumerate(self.original.values()):
                atomic_bytes(run/f'api-{index}.env', raw)
            self.report = {'phase': 'prepared', 'prepared_at': timestamp(), 'operation': 'schema10-to11-history-retirement',
                'prepared_active_tasks': 0, 'prepared_schema_prefix': 10, 'prepared_invariants': prepared_invariants,
                'binding': binding, 'events': [], 'changed_environment_keys': [], 'history_policy_enabled': False,
                'off_pod_export': 'not_authorized_not_attempted',
                'limitations': ['Same-Pod backup is not a separate failure domain or an object-store backup',
                    'No history policy, row retirement, replay-floor advance or off-Pod export is performed',
                    'Any COMMIT attempt requires forward-only schema-compatible recovery']}
            self.save()

    def save(self):
        # File AND parent-directory fsync make commit intent durable before COMMIT.
        atomic_bytes(self.report_path, json.dumps(self.report, separators=(',', ':'), allow_nan=False).encode()+b'\n')

    def guard(self):
        super().guard()
        if private_read(Path(self.watchdog['env_file'])) != self.watchdog_bytes:
            raise RuntimeError('Watchdog environment changed')
        for service in [*self.services, self.watchdog]:
            if self.identity(service) != self.expected[self.key(service)]:
                raise RuntimeError('Owned service identity changed')
        for name, checksum in self.guard_sources.items():
            path = Path(name) if Path(name).is_absolute() else self.root/name
            if digest(path.read_bytes()) != checksum:
                raise RuntimeError('Guarded source changed')
        if proofs(self.proof_spec, self.migrations) != self.proofs:
            raise RuntimeError('Proof evidence changed')
        for name in PATHS:
            state, _ = source_state(self.root/name)
            if self.copied:
                if not state['exists'] or state['sha256'] != digest(self.sources[name]):
                    raise RuntimeError('Candidate source changed')
            elif state != self.snapshot[name]:
                raise RuntimeError('Original source changed')
        if any(private_read(path) != raw for path, raw in self.original.items()):
            raise RuntimeError('API environment changed')
        if not all(self.manager.readiness(s) for s in self.protected):
            raise RuntimeError('Preserved inference or gateway is not ready')

    def invoke(self, service, action):
        key, actual = self.key(service), self.identity(service)
        if actual is not None and actual != self.expected[key]:
            raise RuntimeError('Refuse a foreign owned-record replacement')
        value = super().invoke(service, action)
        current = self.identity(service)
        self.expected[key] = dict(current) if current else None
        if action == 'stop' and current is not None or action == 'start' and current is None:
            raise RuntimeError('Owned service state differs after operation')
        return value

    def _copy(self, rollback=False):
        private_sources(self.root)
        with api_locks(self.root):
            cache_entries(self.root)
            for name in PATHS:
                state, _ = source_state(self.root/name)
                if state['exists'] and state['sha256'] not in {digest(self.sources[name]), self.snapshot[name].get('sha256')}:
                    raise RuntimeError('Refuse foreign source overwrite')
            for name in sorted(PATHS):
                path = self.root/name
                if rollback and name not in self.original_sources:
                    path.unlink(missing_ok=True)
                    continue
                raw = self.original_sources[name] if rollback else self.sources[name]
                old = self.snapshot[name].get('mtime_ns', 0)
                self.writer(path, raw, self.snapshot[name].get('mode', 0o644), old if rollback else max(time.time_ns(), old+2_000_000_000))
                if name.endswith('.py'):
                    compile(path.read_bytes(), name, 'exec', dont_inherit=True)
            self.report['bytecode_evicted_on_'+('rollback' if rollback else 'apply')] = evict_cache(self.root)
        self.copied = not rollback

    def _migrate(self):
        runtime_dsns = [values(private_read(Path(api['env_file'])))['DATABASE_URL'] for api in self.apis]
        dsn = bound_owner_dsn(private_read(self.owner_env), runtime_dsns)
        with psycopg.connect(dsn, autocommit=True, connect_timeout=10) as conn:
            if conn.execute('SELECT current_user').fetchone()[0] == 'bf_runtime':
                raise RuntimeError('Runtime is not migration owner')
            return migrate_atomic(conn, self.migrations, self.sources['ops/ha/runtime_grants.sql'], self._mark_commit)

    def _schema_check(self, version):
        env = values(private_read(Path(self.apis[0]['env_file'])))
        with psycopg.connect(env['DATABASE_URL'], connect_timeout=5,
                options='-c default_transaction_read_only=on -c statement_timeout=15000') as conn:
            # Publisher/reader accounting may commit while this read-only gate
            # recomputes multiple ledgers. Compare one MVCC snapshot throughout.
            conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
            validate_prefix(conn, self.migrations, version)
            if conn.execute('SELECT current_user').fetchone()[0] != 'bf_runtime':
                raise RuntimeError('API must use runtime credentials')
            return invariants(conn, version)

    def _mark_commit(self, evidence):
        self.commit_attempted = True
        self.report.update(phase='commit_attempted', commit_attempted_at=timestamp(), precommit_invariants=evidence)
        self.save()

    def _observe(self, service, schema):
        current = self.identity(service)
        if service['role'] == 'api':
            environment = {}
            for entry in Path(f'/proc/{current["pid"]}/environ').read_bytes().split(b'\0'):
                key, sep, value = entry.partition(b'=')
                if sep:
                    if key.decode() in environment:
                        raise RuntimeError('Duplicate effective API environment')
                    environment[key.decode()] = value.decode()
            expected = values(self.original[Path(service['env_file'])])
            if any(environment.get(k) != v for k, v in expected.items()):
                raise RuntimeError('Effective API configuration differs')
        evidence = self.schema_check(schema)
        access = values(private_read(self.access_env))['BATCHFORGE_API_KEY']
        total, sha = 0, hashlib.sha256()
        with httpx.Client(trust_env=False, timeout=10, headers={'Authorization': 'Bearer '+access}) as client:
            response = client.get(f'http://127.0.0.1:{service["port"]}/readyz')
            body = response.json()
            if response.status_code != 200 or body.get('status') != 'ready' or service['role'] == 'gateway' and body.get('ready_replicas') != 2:
                raise RuntimeError('Control plane not ready')
            with client.stream('GET', f'http://127.0.0.1:{service["port"]}/v1/files/{self.proofs["output_file_id"]}/content') as output:
                output.raise_for_status()
                for chunk in output.iter_bytes():
                    total += len(chunk)
                    if total > self.proofs['output_bytes']:
                        raise RuntimeError('Historical result exceeds reviewed bound')
                    sha.update(chunk)
        if total != self.proofs['output_bytes'] or sha.hexdigest() != self.proofs['output_sha256']:
            raise RuntimeError('Historical archivedQA20 checksum differs')
        if self.identity(service) != current:
            raise RuntimeError('Service identity changed during verification')
        return {'schema_prefix': schema, 'ready': True, 'old_qa20_bytes': total,
                'old_qa20_sha256': sha.hexdigest(), 'invariants': evidence}

    def _roll(self, schema):
        for api in self.apis:
            self.guard()
            self.assert_drained()
            self.invoke(api, 'stop')
            self.invoke(api, 'start')
            self.await_ready(api)
            self.report.setdefault('api_observations', []).append({'at': timestamp(), 'service': self.key(api), **self.observe(api, schema)})
            self.save()
            self.guard()

    def recovery_readiness(self):
        self.guard()
        self.assert_drained()
        versions = [11, 10] if self.commit_attempted else [10]
        for version in versions:
            try:
                self.schema_check(version)
                for service in [*self.apis, *[s for s in self.protected if s['role'] == 'gateway']]:
                    self.observe(service, version)
                return version
            except Exception:
                if version == versions[-1]:
                    raise
        raise RuntimeError('Unproven recovery schema')

    def apply(self):
        self.guard()
        self.assert_drained()
        self.report.update(phase='applying', started_at=timestamp())
        self.save()
        maintenance, source_attempted, restore_watchdog = False, False, False
        try:
            maintenance = True
            self.invoke(self.watchdog, 'stop')
            self.guard()
            self.assert_drained()
            source_attempted = True
            self._copy()
            self.guard()
            self._roll(10)
            self.report['migration_invariants'] = self.migrate()
            if not self.commit_attempted:
                raise RuntimeError('Migration did not bind commit boundary')
            self.report.update(phase='committed', committed_at=timestamp())
            self.save()
            self._roll(11)
            self.report['gateway_observations'] = [self.observe(s, 11) for s in self.protected if s['role'] == 'gateway']
            self.guard()
            self.assert_drained()
            self.report.update(phase='complete', completed_at=timestamp(), non_api_identities_unchanged=True,
                               api_environment_bytes_unchanged=True, history_policy_enabled=False)
            restore_watchdog = True
        except BaseException as error:
            self.report.update(phase='forward_repair_required' if self.commit_attempted else 'rolling_back', failure_type=type(error).__name__)
            self.save()
            try:
                for api in self.apis:
                    self.invoke(api, 'stop')
                if not self.commit_attempted and source_attempted:
                    self._copy(rollback=True)
                for api in self.apis:
                    if private_read(Path(api['env_file'])) != self.original[Path(api['env_file'])]:
                        raise RuntimeError('Refuse a foreign API environment')
                    self.invoke(api, 'start')
                    self.await_ready(api)
                self.report['recovery_schema_prefix'] = self.recovery_readiness()
                if not self.commit_attempted:
                    self.report['phase'] = 'rolled_back'
                restore_watchdog = True
            except BaseException as recovery:
                self.report.update(phase='manual_recovery_required', recovery_failure_type=type(recovery).__name__)
            raise
        finally:
            if maintenance and restore_watchdog:
                try:
                    self.recovery_readiness()
                    self.invoke(self.watchdog, 'start')
                    if not self.identity(self.watchdog):
                        raise RuntimeError('Restored watchdog absent')
                    self.report['watchdog_restored'] = True
                except BaseException as error:
                    self.report.update(phase='manual_recovery_required', watchdog_restored=False, watchdog_failure_type=type(error).__name__)
                    self.save()
                    raise
            elif maintenance:
                self.report['watchdog_restored'] = False
            self.save()
        return self.report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive', type=Path, required=True)
    parser.add_argument('--sha256', required=True)
    parser.add_argument('--proof-spec', type=Path, required=True)
    parser.add_argument('--owner-env', type=Path, default=ROOT.parent/'neon-owner.env')
    parser.add_argument('--access-env', type=Path, required=True)
    parser.add_argument('--operation', default='history-retirement-v11-v1')
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    os.umask(0o077)
    run = private_operation_directory(ROOT, args.operation)
    fd = os.open(ROOT/'.runtime/history-retirement-rollout.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        previous = json.loads(private_read(run/'report.json', 2*1024**2)) if (run/'report.json').exists() else None
        if args.apply and previous is None:
            raise RuntimeError('Immutable preparation required before apply')
        manifest, services, _ = load_manifest(ROOT/'.runtime/services-primary.json')
        watchdog = {'role': 'supervisor', 'name': 'primary', 'env_file': str(ROOT.parent/'supervisor.env'),
                    'python': str(ROOT.parent/'venv/bin/python')}
        def identity(service):
            try:
                return owned_identity(ROOT, service)
            except RuntimeError:
                return None
        roll = HistoryRoll(root=ROOT, run=run, services=services, watchdog=watchdog, manifest_path=manifest,
            report_path=run/'report.json', sources=archive_sources(args.archive, args.sha256), bundle_sha=args.sha256,
            proof_spec=json.loads(private_read(args.proof_spec)), owner_env=args.owner_env, access_env=args.access_env,
            previous=previous, identity=identity)
        if args.apply:
            roll.apply()
    print(json.dumps({'report': str(run/'report.json'), 'phase': roll.report['phase']}))


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print(json.dumps({'error_type': type(error).__name__}), file=sys.stderr)
        raise SystemExit(1) from None
