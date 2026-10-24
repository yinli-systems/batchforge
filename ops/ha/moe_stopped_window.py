"""Explicit four-GPU microbenchmark window; always restore the current seq16 engine.

No selected kernel configuration is installed. Each child owns one GPU and a
330-second wall limit; cleanup uses captured PID/start-time identities only.
"""
from __future__ import annotations

import argparse
import fcntl
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import time

import psycopg

from ops import engine_members
from ops.ha.moe_microbenchmark import UPSTREAM_SHA
from ops.ha.retune_full_gpu import file_env, logged_capacity, write_env
from ops.ha.switch_qwen235b import CONTEXT, MODEL, digest, utc_now
from ops.manage import owned_record
from ops.supervise import Manager, ROOT, load_manifest, private_read, private_write

BATCHES = ((1, 2), (4, 8), (16, 32), (64,))
CHILD_SECONDS = 330


def service_identity(root, service):
    record = owned_record(root / '.runtime' / (service['role'] + '-' + service['name'] + '.json'))
    if record is None:
        raise RuntimeError('Every prepared service must have a live owned identity')
    current = engine_members.identity(record['pid'])
    if not engine_members.matches_leader(current, record):
        raise RuntimeError('Owned process changed while its identity was being captured')
    return current


def deployment_binding(root, manifest, services):
    """Bind exact manifest bytes, private env bytes and Linux launch identities."""
    return {'manifest_sha256': digest(manifest),
            'environment_sha256': {s['env_file']: digest(s['env_file']) for s in services},
            'owned_identities': {s['role'] + '-' + s['name']: service_identity(root, s) for s in services}}


def require_prepared_binding(expected, current):
    if not isinstance(expected, dict) or expected != current:
        raise RuntimeError('Prepared deployment differs; refusing snapshot overwrite or service stop')


def verify_snapshot_files(run, expected):
    names = {'original-engine.env', 'original-worker.env', 'original-manifest.json'}
    if not isinstance(expected, dict) or set(expected) != names or any(
            digest(run / name) != expected[name] for name in names):
        raise RuntimeError('Prepared rollback snapshot bytes changed')


def child_command(python, root, upstream, report, batches):
    return [python, '-m', 'ops.ha.moe_microbenchmark', '--execute', '--root', str(root),
            '--upstream', str(upstream), '--report', str(report), '--batches',
            *(str(batch) for batch in batches), '--max-seconds', '300']


def stop_exact_child(job):
    """Never signal a reused PID or an unobserved process group."""
    process, record = job['process'], job['identity']
    if engine_members.matches_leader(engine_members.identity(process.pid), record):
        engine_members.capture(job['members_path'], record)
        try: descriptor = os.pidfd_open(process.pid)
        except ProcessLookupError:
            return engine_members.reap(job['members_path'], record, grace_seconds=1)
        try:
            if engine_members.matches_leader(engine_members.identity(process.pid), record):
                signal.pidfd_send_signal(descriptor, signal.SIGTERM)
                try: process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    if engine_members.matches_leader(engine_members.identity(process.pid), record):
                        signal.pidfd_send_signal(descriptor, signal.SIGKILL)
                        process.wait(timeout=5)
        except ProcessLookupError:
            pass
        finally:
            os.close(descriptor)
    return engine_members.reap(job['members_path'], record, grace_seconds=1)


def run_children(root, run, python, upstream):
    jobs = []
    try:
        for gpu, batches in enumerate(BATCHES):
            folder = run / f'gpu{gpu}'; folder.mkdir(mode=0o700)
            environment = {key: value for key, value in os.environ.items()
                           if key in {'PATH', 'HOME', 'LANG', 'LC_ALL', 'LD_LIBRARY_PATH'}}
            environment.update(CUDA_VISIBLE_DEVICES=str(gpu), PYTHONPATH=str(root),
                               TRITON_CACHE_DIR=str(folder / 'triton-cache'), OMP_NUM_THREADS='4')
            report = folder / 'microbenchmark.json'
            command = child_command(python, root, upstream, report, batches)
            with (folder / 'child.log').open('ab') as log:
                process = subprocess.Popen(command, cwd=root, env=environment,
                    stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                    start_new_session=True, close_fds=True)
            identity = engine_members.identity(process.pid)
            if identity is None or identity['pgid'] != process.pid or identity['session'] != process.pid or identity['cwd'] != str(root):
                raise RuntimeError('Child ownership could not be established; fresh memory gate must catch any unknown process')
            job = {'process': process, 'identity': identity, 'gpu': gpu, 'batches': batches,
                   'started_monotonic': time.monotonic(), 'members_path': folder / 'members.json',
                   'report_path': report, 'timed_out': False}
            jobs.append(job)
            engine_members.capture(job['members_path'], identity)
            private_write(folder / 'launch.json', {'gpu': gpu, 'batches': batches,
                'identity': identity, 'command': command, 'started_at': utc_now(), 'hard_timeout_seconds': CHILD_SECONDS})
        while any(job['process'].poll() is None for job in jobs):
            for job in jobs:
                if job['process'].poll() is not None: continue
                engine_members.capture(job['members_path'], job['identity'])
                if time.monotonic() - job['started_monotonic'] >= CHILD_SECONDS:
                    job['timed_out'] = True
                    job['cleanup'] = stop_exact_child(job)
                    if job['process'].poll() is None:
                        raise RuntimeError('Timed-out child could not be safely terminated')
            time.sleep(.25)
    finally:
        for job in jobs:
            try: job['cleanup'] = stop_exact_child(job)
            except Exception as error:
                job['cleanup'] = {'status': 'cleanup_failed', 'error_type': type(error).__name__}
    results = []
    for job in jobs:
        try: report = json.loads(private_read(job['report_path'])) if job['report_path'].exists() else None
        except (OSError, ValueError): report = None
        results.append({'gpu': job['gpu'], 'batches': job['batches'],
                        'returncode': job['process'].poll(), 'timed_out': job['timed_out'],
                        'cleanup': job['cleanup'], 'report': str(job['report_path']),
                        'report_sha256': digest(job['report_path']) if report is not None else None,
                        'passed': report is not None and report.get('passed') is True and job['process'].poll() == 0
                                  and not job['timed_out'] and job['cleanup'].get('status') != 'cleanup_failed'
                                  and job['cleanup'].get('remaining', 0) == 0})
    return results


class Window:
    def __init__(self, *, root, run, manager, engine, worker, watchdog, protected, manifest, original, hashes,
                 identity_reader=None):
        self.root, self.run, self.manager = root, run, manager
        self.engine, self.worker, self.watchdog = engine, worker, watchdog
        self.protected, self.manifest, self.original, self.hashes = protected, manifest, original, hashes
        self.identity_reader = identity_reader or (lambda service: service_identity(self.root, service))
        self.report = {'phase': 'prepared', 'created_at': utc_now(), 'events': [],
                       'installs_any_config': False, 'restored': False}

    def save(self): private_write(self.run / 'report.json', self.report)

    def invoke(self, service, action):
        value, error = self.manager.invoke(service, action)
        if error: raise RuntimeError('Owned service operation failed')
        self.report['events'].append({'service': service['role'] + '-' + service['name'], 'action': action, 'result': value})
        self.save()
        return value

    def ready(self, service, timeout):
        until = time.monotonic() + timeout
        while time.monotonic() < until:
            if self.manager.readiness(service): return
            if not self.invoke(service, 'status')['running']: raise RuntimeError('Owned service exited during restore')
            time.sleep(2)
        raise RuntimeError('Owned service restore readiness timed out')

    def identities(self):
        return {s['role'] + '-' + s['name']: {
            'identity': self.identity_reader(s), 'env_sha256': digest(s['env_file'])} for s in self.protected}

    def apply(self, *, drained, children, before_stop=lambda: None):
        original_pids = self.identities()
        drained()
        for service in (self.engine, self.worker, self.watchdog):
            if not self.invoke(service, 'status')['running']: raise RuntimeError('All owned services must initially be running')
        before_stop()
        failure = None
        log = self.root / '.runtime/engine-qwen235b.log'
        offset = log.stat().st_size if log.exists() else 0
        try:
            self.report.update(phase='stopping', started_at=utc_now()); self.save()
            self.invoke(self.watchdog, 'stop')
            self.invoke(self.worker, 'stop')
            drained()
            self.invoke(self.engine, 'stop')
            self.report['phase'] = 'microbenchmarking'; self.save()
            self.report['children'] = children(); self.save()
            if len(self.report['children']) != 4 or not all(r['passed'] for r in self.report['children']):
                raise RuntimeError('One or more bounded microbenchmarks failed')
        except BaseException as error:
            failure = error
            self.report.update(failure_type=type(error).__name__)
        finally:
            self.report['phase'] = 'restoring'; self.save()
            try:
                if json.loads(private_read(self.manifest)) != self.original or any(digest(p) != value for p, value in self.hashes.items()):
                    raise RuntimeError('Original deployment changed externally during the stopped window')
                if not self.invoke(self.engine, 'status')['running']:
                    allowed, reason = self.manager.start_gate(self.engine)
                    self.report['restore_memory_gate'] = {'allowed': allowed, 'reason': reason}; self.save()
                    if not allowed: raise RuntimeError('Fresh engine restore memory gate failed')
                    self.invoke(self.engine, 'start')
                    self.ready(self.engine, 1200)
                    with log.open('rb') as handle:
                        handle.seek(offset); startup = handle.read().decode(errors='replace')
                    capacity = logged_capacity(startup)
                    tokens = re.findall(r'GPU KV cache size: ([0-9,]+) tokens', startup)
                    tokens = min(int(value.replace(',', '')) for value in tokens) if tokens else None
                    self.report.update(restored_full_context_capacity=capacity, restored_kv_cache_tokens=tokens)
                    if capacity is None or capacity < 16 or tokens is None or tokens < 16 * CONTEXT:
                        raise RuntimeError('Restored engine cannot safely admit sixteen complete contexts')
                else:
                    self.ready(self.engine, 1200)
                if not self.invoke(self.worker, 'status')['running']: self.invoke(self.worker, 'start')
                self.ready(self.worker, 60)
                if not self.invoke(self.watchdog, 'status')['running']: self.invoke(self.watchdog, 'start')
                if self.identities() != original_pids: raise RuntimeError('Protected API or gateway identity changed')
                self.report.update(restored=True, phase='failed_restored' if failure else 'complete', finished_at=utc_now())
                self.save()
            except BaseException as restore:
                self.report.update(phase='restore_blocked', restore_failure_type=type(restore).__name__, finished_at=utc_now()); self.save()
                raise RuntimeError('Microbenchmark window needs operator restore') from None
        if failure: raise RuntimeError('Microbenchmark failed; original deployment restored') from None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--upstream', type=Path, default=ROOT / '.runtime/benchmark_moe_v0102.py')
    args = parser.parse_args()
    os.umask(0o077)
    run = ROOT / '.runtime/qwen235b-moe-window-v1'; run.mkdir(mode=0o700, exist_ok=True)
    lock = (run / 'window.lock').open('a'); fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    previous = json.loads(private_read(run / 'report.json')) if (run / 'report.json').exists() else None
    if previous is not None and previous['phase'] != 'prepared':
        raise RuntimeError('Existing stable microbenchmark window must be inspected instead of resubmitted')
    if digest(args.upstream) != UPSTREAM_SHA: raise ValueError('Pinned upstream source hash differs')
    manifest, services, _ = load_manifest(ROOT / '.runtime/services-primary.json')
    original = json.loads(private_read(manifest))
    engine = next(s for s in services if (s['role'], s['name']) == ('engine', 'qwen235b'))
    worker = next(s for s in services if (s['role'], s['name']) == ('worker', 'qwen235b'))
    engine_env, worker_env = file_env(engine['env_file']), file_env(worker['env_file'])
    if (engine_env.get('MODEL_NAME') != MODEL or engine_env.get('ENGINE_MAX_NUM_SEQS') != '16'
            or worker_env.get('WORKER_MAX_INFLIGHT') != '16' or engine.get('gpu') != '0,1,2,3'):
        raise ValueError('Requires the current Qwen235B static16 four-GPU deployment')
    watchdog = {'role': 'supervisor', 'name': 'primary', 'env_file': str(ROOT.parent / 'supervisor.env'), 'python': str(ROOT.parent / 'venv/bin/python')}
    bound_services = [*services, watchdog]
    binding = deployment_binding(ROOT, manifest, bound_services)
    source_hashes = {'controller_sha256': digest(__file__),
                    'microbenchmark_sha256': digest(Path(__file__).with_name('moe_microbenchmark.py')),
                    'upstream_sha256': UPSTREAM_SHA}
    if previous is not None:
        require_prepared_binding(previous.get('prepared_deployment'), binding)
        verify_snapshot_files(run, previous.get('snapshot_sha256'))
        if any(previous.get(key) != value for key, value in source_hashes.items()):
            raise RuntimeError('Prepared experiment implementation changed')
    elif args.apply:
        raise RuntimeError('An explicit preparation is required before apply')
    else:
        write_env(run / 'original-engine.env', engine_env)
        write_env(run / 'original-worker.env', worker_env)
        # Preserve the exact manifest bytes, including any formatting.
        with (run / 'original-manifest.json').open('wb') as handle:
            handle.write(Path(manifest).read_bytes())
    window = Window(root=ROOT, run=run, manager=Manager(timeout=300), engine=engine, worker=worker,
        watchdog=watchdog, protected=[s for s in services if s['role'] in {'api', 'gateway'}],
        manifest=manifest, original=original,
        hashes={str(manifest): digest(manifest), **binding['environment_sha256']})
    if previous is not None:
        window.report = previous
    else:
        window.report.update(**source_hashes, prepared_deployment=binding,
            snapshot_sha256={name: digest(run / name) for name in
                            ('original-engine.env', 'original-worker.env', 'original-manifest.json')},
            gpu_batches=BATCHES, hard_child_timeout_seconds=CHILD_SECONDS)
        window.save()
    if not args.apply:
        print(json.dumps({'report': str(run / 'report.json'), 'phase': 'prepared', 'activated': False})); return
    api_env = file_env(next(s for s in services if s['role'] == 'api')['env_file'])
    def drained():
        with psycopg.connect(api_env['DATABASE_URL'], connect_timeout=10) as conn:
            if conn.execute("SELECT count(*) FROM tasks WHERE status IN ('pending','running')").fetchone()[0]:
                raise RuntimeError('All platform work must be terminal before the stopped GPU window')
    def before_stop():
        require_prepared_binding(window.report['prepared_deployment'], deployment_binding(ROOT, manifest, bound_services))
        verify_snapshot_files(run, window.report['snapshot_sha256'])
    window.apply(drained=drained, children=lambda: run_children(ROOT, run, engine['python'], args.upstream),
                 before_stop=before_stop)
    print(json.dumps({'report': str(run / 'report.json'), 'phase': window.report['phase'], 'restored': window.report['restored']}))


if __name__ == '__main__': main()
