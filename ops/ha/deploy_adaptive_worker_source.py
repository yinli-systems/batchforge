"""Stage or atomically copy six compatible worker modules; never restart services.

Default is preparation only. Apply belongs after all timed/profile windows in a
drained operator window. Subsequent static16 rollback reloads these compatible,
default-OFF modules; it does not restore the previously loaded binary exactly.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import io
import json
import os
from pathlib import Path
import re
import secrets
import stat
import tarfile
import time

from ops import manage
from ops.ha.retune_full_gpu import file_env
from ops.ha.set_scheduler_policy import timestamp
from ops.supervise import ROOT, load_manifest, private_read, private_write

PATHS = frozenset('worker/' + name for name in (
    '__init__.py', '__main__.py', 'config.py', 'runtime.py', 'metrics.py', 'kv_admission.py'))
MAX_ARCHIVE = 2 * 1024 * 1024
MAX_SOURCE = 512 * 1024
BYTECODE = re.compile(r'^(?:__init__|__main__|config|runtime|metrics|kv_admission)\.[A-Za-z0-9_-]+(?:\.opt-[0-9]+)?\.pyc$')


def digest(data):
    return hashlib.sha256(data).hexdigest()


def archive_sources(archive, expected_sha):
    if not re.fullmatch('[0-9a-f]{64}', expected_sha):
        raise ValueError('Expected archive SHA256 must be lowercase hex')
    raw = private_read(archive, limit=MAX_ARCHIVE)
    if digest(raw) != expected_sha:
        raise ValueError('Archive SHA256 differs')
    sources = {}
    with tarfile.open(fileobj=io.BytesIO(raw), mode='r:*') as bundle:
        for member in bundle:
            if (member.name not in PATHS or member.name in sources or not member.isreg()
                    or member.size < 0 or member.size > MAX_SOURCE):
                raise ValueError('Archive must contain exactly the six regular worker modules')
            stream = bundle.extractfile(member)
            if stream is None:
                raise ValueError('Archive member unavailable')
            payload = stream.read(MAX_SOURCE + 1)
            if len(payload) != member.size:
                raise ValueError('Archive member size differs')
            compile(payload, member.name, 'exec', dont_inherit=True)
            sources[member.name] = payload
    if set(sources) != PATHS:
        raise ValueError('Archive is missing a worker module')
    return sources


def source_state(path):
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except FileNotFoundError:
        return {'exists': False}, None
    with os.fdopen(descriptor, 'rb') as stream:
        info = os.fstat(stream.fileno())
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                or stat.S_IMODE(info.st_mode) & 0o7022):
            raise ValueError('Worker source must be an owned non-writable-to-others regular file')
        raw = stream.read(MAX_SOURCE + 1)
        if len(raw) > MAX_SOURCE:
            raise ValueError('Worker source exceeds bound')
    return {'exists': True, 'mode': stat.S_IMODE(info.st_mode),
            'sha256': digest(raw), 'mtime_ns': info.st_mtime_ns}, raw


def atomic_source(path, payload, mode, mtime_ns):
    temporary = path.with_name(path.name + '.' + secrets.token_hex(8) + '.tmp')
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(descriptor, 'wb') as stream:
            stream.write(payload); stream.flush(); os.fchmod(stream.fileno(), mode)
            os.fsync(stream.fileno())
        os.utime(temporary, ns=(mtime_ns, mtime_ns), follow_symlinks=False)
        temporary.replace(path)
        parent = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try: os.fsync(parent)
        finally: os.close(parent)
    finally:
        temporary.unlink(missing_ok=True)


def bytecode_entries(root):
    """Validate the entire scoped cache set before deleting any entry."""
    directory = root / 'worker' / '__pycache__'
    try:
        info = directory.lstat()
    except FileNotFoundError:
        return []
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid()
            or stat.S_IMODE(info.st_mode) & 0o7022):
        raise ValueError('Worker bytecode directory must be owned, real and non-writable to others')
    entries = []
    for path in directory.iterdir():
        if not BYTECODE.fullmatch(path.name):
            continue
        info = path.lstat()
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                or stat.S_IMODE(info.st_mode) & 0o7022):
            raise ValueError('Scoped worker bytecode must be an owned regular file')
        entries.append((path, info.st_dev, info.st_ino))
    return entries


def evict_bytecode(root):
    # Timestamp changes do not invalidate UNCHECKED_HASH caches. Do not inspect
    # or remove unrelated module caches, nor traverse a cache-directory symlink.
    entries = bytecode_entries(root)
    removed = []
    for path, device, inode in entries:
        info = path.lstat()
        if (info.st_dev, info.st_ino) != (device, inode):
            raise RuntimeError('Worker bytecode changed during cache eviction')
        path.unlink()
        removed.append(path.name)
    if entries:
        descriptor = os.open(entries[0][0].parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try: os.fsync(descriptor)
        finally: os.close(descriptor)
    return removed


class SourceDeployment:
    def __init__(self, *, root, run, manifest_path, services, sources, archive_sha,
                 identity_reader=None, assert_drained=None, writer=atomic_source, prepared_report=None):
        self.root, self.run, self.manifest = root, run, manifest_path
        self.services, self.sources, self.writer = services, sources, writer
        if set(sources) != PATHS:
            raise ValueError('Exactly six worker modules are required')
        for name, payload in sources.items():
            compile(payload, name, 'exec', dont_inherit=True)
        workers = [s for s in services if s['role'] == 'worker']
        if len(workers) != 1 or workers[0]['name'] != 'qwen235b':
            raise ValueError('Requires the sole managed qwen235b worker')
        if (root / 'worker').is_symlink() or (root / 'worker').resolve() != root / 'worker':
            raise ValueError('Worker package must be the owned app directory')
        self.worker = workers[0]
        self.watchdog = {'role': 'supervisor', 'name': 'primary',
                         'env_file': str(root.parent / 'supervisor.env')}
        self.identity_reader = identity_reader or self._identity
        self.assert_drained = assert_drained or self._drained
        self.original_manifest = private_read(manifest_path)
        self.env_hashes = {s['env_file']: digest(private_read(Path(s['env_file'])))
                           for s in [*services, self.watchdog]}
        worker_env = file_env(Path(self.worker['env_file']))
        if (worker_env.get('MODEL_NAME') != 'Qwen/Qwen3-235B-A22B-Instruct-2507-FP8'
                or worker_env.get('WORKER_MAX_INFLIGHT') != '16'
                or worker_env.get('WORKER_TOKEN_BUDGET') != '295040'
                or worker_env.get('ENGINE_MAX_MODEL_LEN') != '18440'
                or worker_env.get('WORKER_ADAPTIVE_KV_ENABLED', 'false').lower() not in {'false', '0'}):
            raise ValueError('Compatibility copy requires the existing pinned static16 default-OFF worker')
        self.identities = self._identities()
        if not all(self.identities.values()):
            raise RuntimeError('Every managed service must be running')
        self.original, self.original_bytes = {}, {}
        for name in sorted(PATHS):
            state, raw = source_state(root / name)
            if not state['exists'] and name != 'worker/kv_admission.py':
                raise ValueError('Only the new kv_admission module may be initially absent')
            self.original[name], self.original_bytes[name] = state, raw
        self.expected = dict(self.original)
        if prepared_report is not None and (
                prepared_report['identities_before'] != self.identities
                or prepared_report['environment_sha256'] != self.env_hashes
                or prepared_report['manifest_sha256'] != digest(self.original_manifest)
                or prepared_report['original_sources'] != self.original
                or prepared_report['archive_sha256'] != archive_sha
                or prepared_report['source_sha256'] != {name: digest(raw) for name, raw in sources.items()}):
            raise RuntimeError('Prepared snapshot changed; refusing to replace its evidence')
        self.report = {'phase': 'prepared', 'prepared_at': timestamp(), 'archive_sha256': archive_sha,
            'source_sha256': {name: digest(raw) for name, raw in sources.items()},
            'original_sources': self.original, 'identities_before': self.identities,
            'manifest_sha256': digest(self.original_manifest), 'environment_sha256': self.env_hashes,
            'no_service_actions': True, 'feature_activated': False,
            'bytecode_policy': 'Evict only the six worker modules owned regular __pycache__ variants under the worker role lock, on apply and rollback; never restore old bytecode.',
            'rollback_boundary': 'Subsequent static16 worker restart reloads compatible new default-OFF source, not the previously loaded binary exactly.',
            'limitations': ['Per-file atomic replacement is protected by the managed worker role lock; no unrelated actor may edit source concurrently.',
                'A foreign-byte or uncertain rollback failure requires manual repair before any future worker restart.']}
        for group in ('original', 'staged'):
            (run / group / 'worker').mkdir(parents=True, mode=0o700, exist_ok=True)
        for name, raw in self.original_bytes.items():
            if raw is not None:
                atomic_source(run / 'original' / name, raw, 0o600, time.time_ns())
        for name, raw in sources.items():
            atomic_source(run / 'staged' / name, raw, 0o600, time.time_ns())
        self.save()

    @staticmethod
    def key(service): return service['role'] + '-' + service['name']

    def _identity(self, service):
        record = manage.owned_record(self.root / '.runtime' / (self.key(service) + '.json'))
        return None if record is None else {k: record[k] for k in ('pid', 'start_ticks', 'cwd')}

    def _identities(self):
        return {self.key(s): self.identity_reader(s) for s in [*self.services, self.watchdog]}

    def _drained(self):
        import psycopg
        api = next(s for s in self.services if s['role'] == 'api')
        env = file_env(Path(api['env_file']))
        with psycopg.connect(env['DATABASE_URL'], connect_timeout=5,
                options='-c default_transaction_read_only=on -c statement_timeout=5000') as conn:
            if conn.execute("SELECT count(*) FROM tasks WHERE status IN ('pending','running')").fetchone()[0]:
                raise RuntimeError('Source deployment requires a drained operator window')

    def save(self): private_write(self.run / 'report.json', self.report)

    def guard(self):
        if (private_read(self.manifest) != self.original_manifest
                or self._identities() != self.identities
                or any(digest(private_read(Path(p))) != h for p, h in self.env_hashes.items())):
            raise RuntimeError('Managed PID, manifest or environment changed')
        if any(source_state(self.root / name)[0] != state for name, state in self.expected.items()):
            raise RuntimeError('Worker source changed outside this controller')

    def apply(self):
        lock_path = self.root / '.runtime' / 'worker-qwen235b.lock'
        descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, 'w') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.guard(); self.assert_drained()
            bytecode_entries(self.root)  # Reject unsafe caches before any source write.
            self.report.update(phase='copying', copy_started_at=timestamp()); self.save()
            touched = []
            try:
                for name in sorted(PATHS):
                    self.guard()
                    old = self.original[name]
                    # Avoid a same-second/same-size timestamp-pyc cache hit.
                    now = time.time_ns()
                    if old['exists'] and now // 10**9 == old['mtime_ns'] // 10**9:
                        now = (now // 10**9 + 1) * 10**9
                    touched.append(name)
                    self.writer(self.root / name, self.sources[name], old.get('mode', 0o600), now)
                    self.expected[name] = source_state(self.root / name)[0]
                    if self.expected[name]['sha256'] != digest(self.sources[name]):
                        raise RuntimeError('Copied worker source checksum differs')
                self.guard()
                for name in sorted(PATHS):
                    state, payload = source_state(self.root / name)
                    if state['sha256'] != digest(self.sources[name]): raise RuntimeError('Final source hash differs')
                    compile(payload, name, 'exec', dont_inherit=True)
                self.report['bytecode_evicted_on_apply'] = evict_bytecode(self.root)
                self.guard(); self.assert_drained()
                self.report.update(phase='complete', completed_at=timestamp(),
                    identities_after=self._identities(), all_service_pids_environments_manifest_unchanged=True,
                    installed_sources=self.expected, worker_role_lock_held_through_validation=True)
            except BaseException as error:
                self.report.update(phase='rolling_back', failure_type=type(error).__name__); self.save()
                try:
                    # A failing writer may have committed its atomic replace before raising.
                    for name in touched:
                        current, _ = source_state(self.root / name)
                        if current != self.expected[name]:
                            if current.get('sha256') != digest(self.sources[name]):
                                raise RuntimeError('Refusing to overwrite foreign worker source')
                            self.expected[name] = current
                    self.guard()
                    for name in reversed(touched):
                        self.guard(); old = self.original[name]
                        if old['exists']:
                            atomic_source(self.root / name, self.original_bytes[name], old['mode'], old['mtime_ns'])
                        else:
                            (self.root / name).unlink(missing_ok=True)
                        self.expected[name] = source_state(self.root / name)[0]
                    self.report['bytecode_evicted_on_rollback'] = evict_bytecode(self.root)
                    self.guard()
                    self.report.update(phase='rolled_back', rollback_completed_at=timestamp(),
                        all_service_pids_environments_manifest_unchanged=True)
                except BaseException as rollback:
                    self.report.update(phase='manual_recovery_required', rollback_failure_type=type(rollback).__name__)
                self.save(); raise
            self.save()
        return self.report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive', type=Path, required=True)
    parser.add_argument('--sha256', required=True)
    parser.add_argument('--operation', default='adaptive-worker-source-six-v1')
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args(); os.umask(0o077)
    if not re.fullmatch('[A-Za-z0-9_-]{1,80}', args.operation): parser.error('Invalid operation name')
    run = ROOT / '.runtime' / args.operation
    if run.is_symlink(): raise RuntimeError('Operation directory cannot be a symlink')
    run.mkdir(mode=0o700, exist_ok=True)
    descriptor = os.open(ROOT / '.runtime' / 'adaptive-worker-source.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, 'w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        sources = archive_sources(args.archive, args.sha256)
        manifest, services, _ = load_manifest(ROOT / '.runtime/services-primary.json')
        previous = json.loads(private_read(run / 'report.json')) if (run / 'report.json').exists() else None
        if previous is None and any(run.iterdir()):
            raise RuntimeError('Incomplete operation directory requires inspection')
        if previous is not None:
            if (previous.get('phase') != 'prepared' or previous.get('archive_sha256') != args.sha256
                    or previous.get('controller_sha256') != digest(Path(__file__).read_bytes())
                    or previous.get('manifest_sha256') != digest(private_read(manifest))
                    or any(digest(private_read(Path(p))) != h for p, h in previous['environment_sha256'].items())
                    or any(source_state(ROOT / name)[0] != state for name, state in previous['original_sources'].items())
                    or any(digest(private_read(run / 'staged' / name, limit=MAX_SOURCE)) != digest(raw) for name, raw in sources.items())):
                raise RuntimeError('Prepared snapshot or source changed; use a new reviewed operation')
        deploy = SourceDeployment(root=ROOT, run=run, manifest_path=manifest, services=services,
            sources=sources, archive_sha=args.sha256, prepared_report=previous)
        deploy.report['controller_sha256'] = digest(Path(__file__).read_bytes()); deploy.save()
        if args.apply: deploy.apply()
    print(json.dumps({'phase': deploy.report['phase'], 'report': str(run / 'report.json'),
                     'source_only': True, 'feature_activated': False}), flush=True)


if __name__ == '__main__':
    try: main()
    except Exception as error:
        print(json.dumps({'error_type': type(error).__name__}), flush=True)
        raise SystemExit(1) from None
