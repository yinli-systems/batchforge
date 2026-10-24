"""Prepare a guarded static16 -> exact-token64 Qwen235 worker experiment.

Default invocation only writes private candidate files. Apply requires a drained
operator window; it changes only the sole owned engine/worker and watchdog.
The budget is single-worker accounting, never a physical or distributed KV fence.
No kernel benchmark, profiler RPC or tuned-kernel configuration is installed.
"""
from __future__ import annotations

import argparse
import copy
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import sys
import time

import httpx

from backend.scheduling import validate_profile
from ops import manage
from ops.ha.finalize_api_policy import values
from ops.ha.retune_full_gpu import file_env, write_env
from ops.ha.set_scheduler_policy import atomic_bytes, timestamp
from ops.ha.switch_qwen235b import CONTEXT, MODEL, REVISION
from ops.supervise import Manager, ROOT, load_manifest, private_read, private_write
from worker.kv_admission import engine_quiescent, token_count, tokenizer_body

SLOTS = 64
MAX_BUDGET = 300000
BLOCK = 16
WORKER_SOURCES = ('worker/config.py', 'worker/runtime.py', 'worker/kv_admission.py', 'worker/metrics.py')


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def admission_budget(cache_tokens, maximum=MAX_BUDGET):
    if (type(cache_tokens) is not int or cache_tokens <= 0 or cache_tokens % BLOCK
            or type(maximum) is not int or not BLOCK <= maximum <= MAX_BUDGET or maximum % BLOCK):
        raise ValueError('Cache and budget must be positive, bounded whole16-token blocks')
    budget = min(maximum, (cache_tokens * 95 // 100 // BLOCK) * BLOCK)
    if budget < ((CONTEXT + BLOCK - 1) // BLOCK) * BLOCK:
        raise ValueError('Fresh conservative budget cannot hold one full context')
    return budget


def fresh_cache(log):
    observed = re.findall(r'GPU KV cache size: ([0-9,]+) tokens', log)
    if not observed:
        raise ValueError('Fresh engine log contains no exact cache token capacity')
    tokens = min(int(value.replace(',', '')) for value in observed)
    admission_budget(tokens)
    return tokens


def candidate_worker(original, budget, allocation_hourly):
    if not math.isfinite(allocation_hourly) or allocation_hourly < 0:
        raise ValueError('Allocation must be finite and nonnegative')
    if type(budget) is not int or not ((CONTEXT + BLOCK - 1) // BLOCK) * BLOCK <= budget <= MAX_BUDGET or budget % BLOCK:
        raise ValueError('Worker budget must hold one context within the bounded whole-block budget')
    profile = validate_profile(json.loads(original['SCHEDULER_PROFILE_JSON']))
    source_id = profile['profile_id']
    profile.update(profile_id='primary-h100-tp4-qwen235b-fp8-adaptive64',
        calibration_label=f'projected-from-static16;claimlimit64-not-newpricedexecutioncapacity;rates-not-recalibrated;source:{source_id}',
        cost_source=profile['cost_source'] + ';adaptive64-claimlimit-not-new-priced-execution-capacity')
    result = dict(original)
    result.update(WORKER_ADAPTIVE_KV_ENABLED='true', WORKER_MAX_INFLIGHT=str(SLOTS),
        WORKER_TOKEN_BUDGET=str(budget), ENGINE_MAX_MODEL_LEN=str(CONTEXT),
        SCHEDULER_PROFILE_JSON=json.dumps(validate_profile(profile), separators=(',', ':')))
    return result


def identity(root, service):
    record = manage.owned_record(root / '.runtime' / f"{service['role']}-{service['name']}.json")
    return None if record is None else {key: record[key] for key in ('pid', 'start_ticks', 'cwd')}


def metric_values(text, names):
    result = {}
    for line in text.splitlines():
        if not line or line.startswith('#'):
            continue
        fields = line.split()
        if len(fields) == 2 and fields[0] in names:
            if fields[0] in result:
                raise RuntimeError('Duplicate worker admission gauge')
            value = float(fields[1])
            if not math.isfinite(value) or value < 0:
                raise RuntimeError('Invalid worker admission gauge')
            result[fields[0]] = value
    if set(result) != set(names):
        raise RuntimeError('Worker admission gauges missing')
    return result


def prepared_snapshot_matches(previous, *, manifest, environment, identities, sources,
                              controller_sha, maximum_budget, allocation_hourly):
    return (previous.get('phase') == 'prepared'
        and previous.get('original_manifest_sha256') == sha(manifest)
        and previous.get('original_environment_sha256') == {str(p): sha(raw) for p, raw in environment.items()}
        and previous.get('identities_before') == identities
        and previous.get('worker_source_sha256') == sources
        and previous.get('controller_sha256') == controller_sha
        and previous.get('budget_upper_bound') == maximum_budget
        and previous.get('allocation_hourly_usd') == allocation_hourly)


def private_operation_directory(root, name):
    if not re.fullmatch(r'[a-zA-Z0-9_-]{1,80}', name):
        raise ValueError('Invalid operation name')
    run = root / '.runtime' / name
    if run.is_symlink():
        raise ValueError('Operation directory cannot be a symlink')
    run.mkdir(mode=0o700, exist_ok=True)
    info = run.stat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) & 0o077:
        raise ValueError('Operation directory must be owner-only')
    return run


class AdaptiveRoll:
    def __init__(self, *, root, run, manifest_path, services, watchdog,
                 allocation_hourly=16.10, maximum_budget=MAX_BUDGET,
                 manager=None, identity_reader=None, assert_drained=None,
                 engine_observer=None, worker_observer=None, clock=time.monotonic, sleep=time.sleep):
        self.root, self.run, self.manifest_path = root, run, manifest_path
        self.services, self.watchdog = services, watchdog
        self.manager = manager or Manager(root=root, timeout=300)
        self.identity = identity_reader or (lambda s: identity(root, s))
        self.assert_drained = assert_drained or self._drained
        self.engine_observer = engine_observer or self._observe_engine
        self.worker_observer = worker_observer or self._observe_worker
        self.clock, self.sleep = clock, sleep
        self.allocation_hourly, self.maximum_budget = allocation_hourly, maximum_budget
        admission_budget(320000, maximum_budget)
        if not math.isfinite(allocation_hourly) or allocation_hourly < 0:
            raise ValueError('Allocation must be finite and nonnegative')
        engines = [s for s in services if s['role'] == 'engine']
        workers = [s for s in services if s['role'] == 'worker']
        if len(engines) != 1 or len(workers) != 1:
            raise ValueError('Adaptive admission requires exactly one engine and one worker')
        self.engine, self.worker = engines[0], workers[0]
        if self.engine['name'] != 'qwen235b' or self.worker['name'] != 'qwen235b':
            raise ValueError('Requires the current owned Qwen235 service names')
        self.protected = [s for s in services if s['role'] not in {'worker', 'engine'}]
        if any(s['role'] not in {'api', 'gateway'} for s in self.protected):
            raise ValueError('Unexpected service in single-owner deployment')
        self.original = private_read(manifest_path)
        self.document = json.loads(self.original)
        self.env_bytes = {Path(s['env_file']): private_read(Path(s['env_file'])) for s in [*services, watchdog]}
        watchdog_env = values(self.env_bytes[Path(watchdog['env_file'])])
        if Path(watchdog_env.get('SUPERVISOR_MANIFEST', '')).resolve() != manifest_path.resolve():
            raise ValueError('Watchdog must follow this exact guarded manifest')
        self.engine_env, self.worker_env = (values(self.env_bytes[Path(s['env_file'])]) for s in [self.engine, self.worker])
        self._validate_current()
        self.before = {self.key(s): self.identity(s) for s in [*services, watchdog]}
        if not all(self.before.values()):
            raise RuntimeError('All original owned services including watchdog must be running')
        self.expected = dict(self.before)
        self.candidate_engine = {**self.engine, 'env_file': str(run / 'engine.env')}
        self.candidate_worker = {**self.worker, 'env_file': str(run / 'worker.env')}
        engine_env = {**self.engine_env, 'ENGINE_MAX_NUM_SEQS': str(SLOTS)}
        write_env(run / 'engine.env', engine_env)
        self.candidate = copy.deepcopy(self.document)
        for item in self.candidate['services']:
            if self.key(item) == self.key(self.engine): item['env_file'] = self.candidate_engine['env_file']
            if self.key(item) == self.key(self.worker): item['env_file'] = self.candidate_worker['env_file']
        self.engine_only = copy.deepcopy(self.candidate)
        self.engine_only['services'] = [s for s in self.engine_only['services'] if s['role'] != 'worker']
        self.allowed_manifest_bytes = {self.original}
        self.candidate_engine_bytes = private_read(run / 'engine.env')
        self.report = {'phase': 'prepared', 'operation': 'qwen235-static16-to-adaptive64',
            'prepared_at': timestamp(), 'model': MODEL, 'revision': REVISION, 'claimed_capacity': SLOTS,
            'budget_upper_bound': maximum_budget, 'cache_fraction_numerator': 95,
            'allocation_hourly_usd': allocation_hourly,
            'cache_fraction_denominator': 100, 'block_tokens': BLOCK,
            'original_manifest_sha256': sha(self.original), 'identities_before': self.before,
            'original_environment_sha256': {str(p): sha(raw) for p, raw in self.env_bytes.items()},
            'events': [], 'activation_requires_drained_window': True,
            'limitations': ['Budget bounds this single worker accounting, not physical KV or another caller',
                '64 claimed tasks include tokenizer/budget/report waiters, not64 GPU-running sequences',
                'Scheduler rates are copied from static16 and are not recalibrated at adaptive64',
                'Static16 hourly price calibration is preserved; more claim slots do not establish cheaper execution',
                'No direct engine clients or profiler sessions may overlap adaptive admission',
                'Latched transport/cancel uncertainty requires operator intervention; no automatic resume']}
        private_write(run / 'original-manifest.json', self.document)
        atomic_bytes(run / 'original-engine.env', self.env_bytes[Path(self.engine['env_file'])])
        atomic_bytes(run / 'original-worker.env', self.env_bytes[Path(self.worker['env_file'])])

    @staticmethod
    def key(service): return service['role'] + '-' + service['name']

    def _validate_current(self):
        e, w = self.engine_env, self.worker_env
        if (e.get('MODEL_NAME') != MODEL or e.get('ENGINE_REVISION') != REVISION
                or e.get('ENGINE_TENSOR_PARALLEL_SIZE') != '4' or e.get('ENGINE_MAX_NUM_SEQS') != '16'
                or e.get('ENGINE_MAX_MODEL_LEN') != str(CONTEXT)
                or e.get('ENGINE_MAX_NUM_BATCHED_TOKENS') != '4096'
                or e.get('ENGINE_DTYPE') != 'bfloat16' or float(e.get('ENGINE_GPU_MEMORY_UTILIZATION', '0')) != .90
                or e.get('ENGINE_ENABLE_PREFIX_CACHING') not in {'0', 'false'}
                or e.get('ENGINE_ENABLE_CHUNKED_PREFILL') not in {'1', 'true'}
                or e.get('ENGINE_GENERATION_CONFIG') != 'vllm'
                or w.get('MODEL_NAME') != MODEL or w.get('ENGINE_MAX_MODEL_LEN') != str(CONTEXT)
                or w.get('WORKER_MAX_INFLIGHT') != '16' or w.get('WORKER_TOKEN_BUDGET') != str(16 * CONTEXT)
                or w.get('WORKER_ADAPTIVE_KV_ENABLED', 'false').lower() not in {'false', '0'}
                or w.get('INFERENCE_BASE_URLS') != f"http://127.0.0.1:{self.engine['port']}"):
            raise ValueError('Requires pinned235B static16/.90/4096/context18440 with a sole local engine URL')
        validate_profile(json.loads(w['SCHEDULER_PROFILE_JSON']))

    def save(self): private_write(self.run / 'report.json', self.report)

    def _drained(self):
        import psycopg
        env = file_env(next(s for s in self.services if s['role'] == 'api')['env_file'])
        with psycopg.connect(env['DATABASE_URL'], connect_timeout=5,
                options='-c default_transaction_read_only=on -c statement_timeout=5000') as conn:
            if conn.execute("SELECT count(*) FROM tasks WHERE status IN ('pending','running')").fetchone()[0]:
                raise RuntimeError('Platform cohorts must be terminal before adaptive activation')

    def guard(self, *, initial=False):
        if private_read(self.manifest_path) not in self.allowed_manifest_bytes:
            raise RuntimeError('Manifest changed outside this controller')
        if any(private_read(path) != raw for path, raw in self.env_bytes.items()):
            raise RuntimeError('Original service environment changed outside this controller')
        if private_read(self.run / 'engine.env') != self.candidate_engine_bytes:
            raise RuntimeError('Candidate engine environment changed')
        if hasattr(self, 'candidate_worker_bytes') and private_read(self.run / 'worker.env') != self.candidate_worker_bytes:
            raise RuntimeError('Candidate worker environment changed')
        check = [*self.services, self.watchdog]
        expected = self.before if initial else self.expected
        if any(self.identity(s) != expected[self.key(s)] for s in check):
            raise RuntimeError('Owned service identity changed outside this controller')
        ready = self.services if initial else self.protected
        if not all(self.manager.readiness(s) for s in ready):
            raise RuntimeError('Original or protected service is not ready')

    def _manifest(self, document=None):
        if document is None:
            raw = self.original
        else:
            raw = json.dumps(document, separators=(',', ':')).encode()
        if private_read(self.manifest_path) not in self.allowed_manifest_bytes:
            raise RuntimeError('Refusing to overwrite a foreign manifest')
        atomic_bytes(self.manifest_path, raw)
        self.allowed_manifest_bytes.add(raw)

    def invoke(self, service, action):
        key = self.key(service)
        current = self.identity(service)
        if current is not None and current != self.expected.get(key):
            raise RuntimeError('Refusing to mutate a replacement owned service identity')
        if action == 'start' and current is not None:
            return {'status': 'already_running', 'pid': current['pid']}
        result, error = self.manager.invoke(service, action)
        if error:
            raise RuntimeError('Owned manager operation failed')
        current = self.identity(service)
        if action == 'start':
            if result.get('status') != 'started' or current is None or result.get('pid') != current['pid']:
                raise RuntimeError('Started service identity is uncertain')
            self.expected[key] = current
        elif action == 'stop':
            if result.get('status') not in {'stopped', 'not_running'} or current is not None:
                raise RuntimeError('Stopped service identity is uncertain')
            self.expected[key] = None
        self.report['events'].append({'at': timestamp(), 'service': key, 'action': action,
            'status': result.get('status'), 'identity': current})
        self.save()
        return result

    def stop(self, service):
        until = self.clock() + 120
        while True:
            try: return self.invoke(service, 'stop')
            except RuntimeError:
                if self.clock() >= until: raise
                # The expected identity remains pinned while tracked CUDA ranks
                # exit. A new leader can never be adopted by this retry loop.
                self.sleep(2)

    def gate(self, engine):
        until = self.clock() + 120
        while True:
            allowed, reason = self.manager.start_gate(engine)
            self.report['last_memory_gate'] = {'allowed': allowed, 'reason': reason}; self.save()
            if allowed: return
            if self.clock() >= until: raise RuntimeError('Fresh per-rank GPU memory gate refused')
            self.sleep(2)

    def ready(self, service, seconds):
        until = self.clock() + seconds
        while self.clock() < until:
            if self.identity(service) != self.expected[self.key(service)]:
                raise RuntimeError('Owned service exited or changed during readiness')
            if self.manager.readiness(service): return
            self.sleep(1)
        raise RuntimeError('Owned service readiness timeout')

    def _client(self):
        headers = ({'Authorization': 'Bearer ' + self.worker_env['INFERENCE_API_KEY']}
                   if self.worker_env.get('INFERENCE_API_KEY') else {})
        return httpx.Client(trust_env=False, timeout=10, headers=headers)

    def _observe_engine(self):
        url = self.worker_env['INFERENCE_BASE_URLS']
        with self._client() as client:
            def get(path):
                response = client.get(url + path); response.raise_for_status()
                if len(response.content) > 2 * 1024 * 1024: raise RuntimeError('Engine observation exceeds bound')
                return response
            if get('/version').json() != {'version': '0.10.2'}:
                raise RuntimeError('Exact adaptive tokenizer protocol version differs')
            if [row['id'] for row in get('/v1/models').json()['data']] != [MODEL]:
                raise RuntimeError('Private engine model differs')
            response = client.post(url + '/tokenize', json=tokenizer_body({'model': MODEL,
                'messages': [{'role': 'user', 'content': 'BatchForge tokenizer protocol check.'}]}, MODEL))
            response.raise_for_status()
            if len(response.content) > 2 * 1024 * 1024: raise RuntimeError('Tokenizer observation exceeds bound')
            count = token_count(response.json(), CONTEXT)
            if not engine_quiescent(get('/metrics').text):
                raise RuntimeError('Private engine is not quiescent; no worker may start')
        return {'version': '0.10.2', 'model': MODEL, 'tokenizer_probe_count': count,
                'context': CONTEXT, 'engine_running_waiting_zero': True}

    def _observe_worker(self, budget):
        import psycopg
        expected = {'batchforge_worker_capacity': SLOTS, 'batchforge_worker_ready_engines': 1,
            'batchforge_worker_adaptive_kv_budget_tokens': budget, 'batchforge_worker_adaptive_kv_paused': 0,
            'batchforge_worker_adaptive_kv_waiting': 0, 'batchforge_worker_inflight': 0,
            'batchforge_worker_reserved_context_tokens': 0}
        with self._client() as client:
            response = client.get(f"http://127.0.0.1:{self.worker['port']}/metrics"); response.raise_for_status()
            if len(response.content) > 2 * 1024 * 1024: raise RuntimeError('Worker observation exceeds bound')
            observed = metric_values(response.text, expected)
        if observed != expected: raise RuntimeError('Adaptive worker admission gauges differ')
        env = file_env(next(s for s in self.services if s['role'] == 'api')['env_file'])
        profile = json.loads(values(self.candidate_worker_bytes)['SCHEDULER_PROFILE_JSON'])
        with psycopg.connect(env['DATABASE_URL'], connect_timeout=5,
                options='-c default_transaction_read_only=on -c statement_timeout=5000') as conn:
            row = conn.execute("""SELECT concurrency,scheduler_profile_id,runtime_fingerprint,
                coalesce((metadata->>'ready_engine_count')::integer,0),metadata->'kv_admission'
                FROM workers WHERE id=%s AND model=%s AND last_seen_at>clock_timestamp()-interval '60 seconds'""",
                (self.worker_env['WORKER_ID'], MODEL)).fetchone()
        if (not row or row[:4] != (SLOTS, profile['profile_id'], profile['runtime_fingerprint'], 1)
                or row[4] != {'protocol': 'vllm-0.10.2-qwen-chat', 'budget': budget,
                             'block_size': 16, 'single_worker_single_engine': True, 'paused': False}):
            raise RuntimeError('Registered adaptive profile or budget differs')
        return {'gauges': observed, 'registered_capacity': SLOTS, 'profile_id': profile['profile_id'],
                'runtime_fingerprint': profile['runtime_fingerprint'], 'registration_verified': True}

    def apply(self):
        self.guard(initial=True); self.assert_drained()
        self.report.update(phase='applying', activation_started_at=timestamp()); self.save()
        maintenance_attempted = False
        worker_stop_attempted = False
        try:
            maintenance_attempted = True
            self.stop(self.watchdog)
            self.guard()
            worker_stop_attempted = True
            self.stop(self.worker)
            self.assert_drained()
            self.stop(self.engine)
            self.guard()
            self.gate(self.candidate_engine)
            log = self.root / '.runtime' / ('engine-' + self.engine['name'] + '.log')
            previous_log = log.stat() if log.exists() else None
            offset = previous_log.st_size if previous_log is not None else 0
            self._manifest(self.engine_only)
            self.invoke(self.candidate_engine, 'start'); self.ready(self.candidate_engine, 1200)
            if not log.exists() or log.stat().st_size < offset:
                raise RuntimeError('Fresh engine log missing or truncated')
            current_log = log.stat()
            if previous_log is not None and (previous_log.st_dev, previous_log.st_ino) != (current_log.st_dev, current_log.st_ino):
                raise RuntimeError('Engine log replaced during startup; freshness is ambiguous')
            with log.open('rb') as stream:
                stream.seek(offset); raw = stream.read(16 * 1024 * 1024 + 1)
            if len(raw) > 16 * 1024 * 1024: raise RuntimeError('Fresh engine startup log exceeds bound')
            cache = fresh_cache(raw.decode(errors='replace'))
            budget = admission_budget(cache, self.maximum_budget)
            self.report.update(measured_cache_tokens=cache, worker_budget_tokens=budget,
                startup_cache_evidence=[line for line in raw.decode(errors='replace').splitlines()
                    if 'GPU KV cache size:' in line or 'Maximum concurrency for' in line])
            self.report['engine_preflight'] = self.engine_observer()
            self.guard(); self.assert_drained()
            worker = candidate_worker(self.worker_env, budget, self.allocation_hourly)
            write_env(self.run / 'worker.env', worker)
            self.candidate_worker_bytes = private_read(self.run / 'worker.env')
            self.report['profile'] = json.loads(worker['SCHEDULER_PROFILE_JSON'])
            self.report['phase'] = 'starting_validated_adaptive_worker'; self.save()
            self._manifest(self.candidate)
            self.invoke(self.candidate_worker, 'start'); self.ready(self.candidate_worker, 60)
            self.report['worker_preflight'] = self.worker_observer(budget)
            self.guard(); self.assert_drained()
            self.report.update(phase='complete', completed_at=timestamp(), protected_services_unchanged=True,
                engine_identity_after=self.identity(self.candidate_engine), worker_identity_after=self.identity(self.candidate_worker))
        except BaseException as error:
            self.report.update(phase='rolling_back', failure_type=type(error).__name__); self.save()
            try:
                # A restored watchdog must never adopt an unvalidated candidate
                # worker if rollback itself becomes blocked.
                self._manifest(self.engine_only)
                self.stop(self.watchdog)
                if worker_stop_attempted:
                    self.stop(self.candidate_worker)
                    self.stop(self.candidate_engine)
                    self.guard()
                    # Restore exact manifest bytes; original environment files
                    # were never rewritten by this controller.
                    self._manifest()
                    self.gate(self.engine)
                    self.invoke(self.engine, 'start'); self.ready(self.engine, 1200)
                    self.invoke(self.worker, 'start'); self.ready(self.worker, 60)
                    self.guard()
                else:
                    self._manifest()
                    self.guard()
                self.report.update(phase='rolled_back', rollback_completed_at=timestamp())
            except BaseException as rollback:
                self.report.update(phase='manual_recovery_required', rollback_failure_type=type(rollback).__name__)
            raise
        finally:
            if maintenance_attempted:
                try:
                    self.invoke(self.watchdog, 'start')
                    if self.identity(self.watchdog) != self.expected[self.key(self.watchdog)]:
                        raise RuntimeError('Restored watchdog identity differs')
                    self.report['watchdog_restored'] = True
                    self.report['watchdog_identity_after'] = self.identity(self.watchdog)
                except BaseException as watchdog_error:
                    self.report.update(phase='manual_recovery_required', watchdog_restored=False,
                        watchdog_failure_type=type(watchdog_error).__name__)
                    self.save(); raise
            self.save()
        return self.report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--operation', default='qwen235b-adaptive64-v1')
    parser.add_argument('--maximum-budget', type=int, default=MAX_BUDGET)
    parser.add_argument('--allocation-hourly', type=float, default=16.10)
    args = parser.parse_args(); os.umask(0o077)
    run = private_operation_directory(ROOT, args.operation)
    lock_path = ROOT / '.runtime/qwen235b-maintenance.lock'
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        previous = json.loads(private_read(run / 'report.json')) if (run / 'report.json').exists() else None
        if previous is not None and previous.get('phase') != 'prepared':
            raise RuntimeError('Existing operation requires inspection; do not repeat or resume uncertain activation')
        manifest, services, _ = load_manifest(ROOT / '.runtime/services-primary.json')
        watchdog = {'role': 'supervisor', 'name': 'primary', 'env_file': str(ROOT.parent / 'supervisor.env'),
                    'python': str(ROOT.parent / 'venv/bin/python')}
        controller_sha = sha(Path(__file__).read_bytes())
        sources = {p: sha((ROOT / p).read_bytes()) for p in WORKER_SOURCES}
        if previous is not None and not prepared_snapshot_matches(previous, manifest=private_read(manifest),
                environment={Path(s['env_file']): private_read(Path(s['env_file'])) for s in [*services, watchdog]},
                identities={AdaptiveRoll.key(s): identity(ROOT, s) for s in [*services, watchdog]},
                sources=sources, controller_sha=controller_sha,
                maximum_budget=args.maximum_budget, allocation_hourly=args.allocation_hourly):
            raise RuntimeError('Prepared deployment snapshot changed; use a newly reviewed operation')
        roll = AdaptiveRoll(root=ROOT, run=run, manifest_path=manifest, services=services, watchdog=watchdog,
            maximum_budget=args.maximum_budget, allocation_hourly=args.allocation_hourly)
        roll.report['controller_sha256'] = controller_sha
        roll.report['worker_source_sha256'] = sources
        roll.save()
        if args.apply: roll.apply()
    print(json.dumps({'report': str(run / 'report.json'), 'phase': roll.report['phase'],
                      'activated': roll.report['phase'] == 'complete'}), flush=True)


if __name__ == '__main__':
    try: main()
    except Exception as error:
        print(json.dumps({'error_type': type(error).__name__}), file=sys.stderr, flush=True)
        raise SystemExit(1) from None
