"""Prepare an isolated custom-all-reduce -> NCCL comparison on adaptive64.

Preparation never changes services. Apply preserves the current admission budget,
changes the numerical runtime fingerprint, and restores the original deployment
on failure. It does not benchmark, install kernels, or alter API configuration.
"""
from __future__ import annotations

import argparse
import copy
import fcntl
import json
import os
from pathlib import Path
import shlex
import time
from types import SimpleNamespace

from backend.scheduling import runtime_fingerprint, validate_profile
from ops import manage
from ops.ha.adaptive_qwen235b import (AdaptiveRoll, BLOCK, CONTEXT, MAX_BUDGET, MODEL,
    REVISION, SLOTS, WORKER_SOURCES, admission_budget, fresh_cache, identity,
    metric_values, private_operation_directory, sha)
from ops.ha.finalize_api_policy import values
from ops.ha.set_scheduler_policy import atomic_bytes, timestamp
from ops.supervise import Manager, ROOT, load_manifest, private_read, private_write

VLLM_FILES = {
    'envs.py': '4e4cf97fdef1aac3b0df34a12b16f64d08d43b3dbcec82475c7d0f3c5f4f5589',
    'config/parallel.py': 'a98bc5f7d64fc76cb36167c8cd2114e23df9e89fc4aa8f92c82d182bfb99d95b',
    'distributed/device_communicators/cuda_communicator.py': 'bf33cb36c338f8afffc1ae4ca370545b81abab753f247de6afce2117e13ffcd1',
    'v1/worker/gpu_worker.py': '8a50c7c633ea5f9e14360029e1551929975a91cdaa750de2be3ae9922bdb1f5f',
}


def env_bytes(environment):
    return ''.join(f'{key}={shlex.quote(value)}\n' for key, value in environment.items()).encode()


def nccl_profile(original):
    result = validate_profile(original)
    parent = result['runtime_fingerprint']
    descriptor = {'parent_runtime_fingerprint': parent, 'vllm_version': '0.10.2',
                  'collective_path': 'disable_custom_all_reduce-pynccl-fallback',
                  'symmetric_memory_allreduce': False, 'vllm_source_sha256': VLLM_FILES}
    result.update(profile_id='primary-h100-tp4-qwen235b-fp8-adaptive64-nccl',
        runtime_fingerprint=runtime_fingerprint(descriptor),
        calibration_label='static16-rates-not-recalibrated;collective-path-changed;source:' + result['profile_id'],
        cost_source=result['cost_source'] + ';collective-path-not-recalibrated')
    return validate_profile(result), descriptor


def source_evidence(root, engine):
    base = Path(engine['python']).parent.parent / 'lib/python3.12/site-packages/vllm'
    hashes = {str(base / name): sha((base / name).read_bytes()) for name in VLLM_FILES}
    if any(hashes[str(base / name)] != checksum for name, checksum in VLLM_FILES.items()):
        raise RuntimeError('Installed vLLM implementation differs from the inspected0.10.2 source')
    argument_source = base / 'engine/arg_utils.py'
    if '"--disable-custom-all-reduce"' not in argument_source.read_text():
        raise RuntimeError('Installed vLLM does not advertise the required CLI switch')
    hashes[str(argument_source)] = sha(argument_source.read_bytes())
    for name in ('ops/manage.py', 'ops/supervise.py', 'ops/engine_members.py', 'ops/host_memory.py',
                 'ops/ha/adaptive_qwen235b.py', 'ops/ha/finalize_api_policy.py', 'ops/ha/switch_qwen235b.py',
                 'ops/ha/retune_full_gpu.py', 'ops/ha/set_scheduler_policy.py', 'backend/scheduling.py', *WORKER_SOURCES):
        hashes[str(root / name)] = sha((root / name).read_bytes())
    original = values(private_read(Path(engine['env_file'])))
    args = SimpleNamespace(role='engine', python=engine['python'], port=engine['port'], gpu=engine['gpu'])
    before = manage.command(args, original)
    after = manage.command(args, {**original, 'ENGINE_DISABLE_CUSTOM_ALL_REDUCE': '1'})
    if '--disable-custom-all-reduce' in before or after.count('--disable-custom-all-reduce') != 1:
        raise RuntimeError('Managed engine command does not support the reviewed single flag')
    if [arg for arg in after if arg != '--disable-custom-all-reduce'] != before:
        raise RuntimeError('Managed candidate changes more than the requested CLI switch')
    return {'source_sha256': hashes, 'managed_argv_changes_only_disable_custom_all_reduce': True,
            'symmetric_memory_default_proof': {'path': str(base / 'envs.py'),
                'sha256': VLLM_FILES['envs.py'], 'environment_variable': 'VLLM_ALLREDUCE_USE_SYMM_MEM', 'default': '0'},
            'fallback_source': str(base / 'distributed/device_communicators/cuda_communicator.py')}


def running_collective(root, engine, disabled):
    record = manage.owned_record(root / '.runtime/engine-qwen235b.json')
    if record is None: raise RuntimeError('No live owned Qwen engine')
    command = Path(f'/proc/{record["pid"]}/cmdline').read_bytes().rstrip(b'\0').split(b'\0')
    actual = [value.decode() for value in command]
    if actual != record['command'] or actual.count('--disable-custom-all-reduce') != int(disabled):
        raise RuntimeError('Actual owned engine argv does not prove the expected collective setting')
    environment = dict(value.split(b'=', 1) for value in Path(f'/proc/{record["pid"]}/environ').read_bytes().split(b'\0') if b'=' in value)
    symmetric = environment.get(b'VLLM_ALLREDUCE_USE_SYMM_MEM', b'0')
    if symmetric != b'0': raise RuntimeError('Symmetric-memory collective must be OFF for this comparison')
    if identity(root, engine) != {key: record[key] for key in ('pid', 'start_ticks', 'cwd')}:
        raise RuntimeError('Owned engine identity changed while inspecting argv')
    return {'disable_custom_all_reduce': disabled, 'actual_argv_matches_owned_record': True,
            'symmetric_memory_allreduce': False}


class CollectiveRoll(AdaptiveRoll):
    def __init__(self, *, root, run, manifest_path, services, watchdog, support,
                 previous=None, manager=None, identity_reader=None, assert_drained=None,
                 engine_observer=None, worker_observer=None, path_observer=None,
                 clock=time.monotonic, sleep=time.sleep):
        self.root, self.run, self.manifest_path = root, run, manifest_path
        self.services, self.watchdog = services, watchdog
        self.manager = manager or Manager(root=root, timeout=300)
        self.identity = identity_reader or (lambda service: identity(root, service))
        self.assert_drained = assert_drained or self._drained
        self.engine_observer = engine_observer or self._observe_engine
        self.profile_observer = worker_observer or self._observe_profile
        self.path_observer = path_observer or (lambda disabled: running_collective(root, self.engine, disabled))
        self.clock, self.sleep = clock, sleep
        engines, workers = ([s for s in services if s['role'] == role] for role in ('engine', 'worker'))
        if len(engines) != 1 or len(workers) != 1:
            raise ValueError('Requires one owned engine and one owned worker')
        self.engine, self.worker = engines[0], workers[0]
        self.protected = [s for s in services if s['role'] not in {'engine', 'worker'}]
        if any(s['role'] not in {'api', 'gateway'} for s in self.protected): raise ValueError('Unexpected protected service')
        self.original = private_read(manifest_path); self.document = json.loads(self.original)
        self.env_bytes = {Path(s['env_file']): private_read(Path(s['env_file'])) for s in [*services, watchdog]}
        if Path(values(self.env_bytes[Path(watchdog['env_file'])]).get('SUPERVISOR_MANIFEST', '')).resolve() != manifest_path.resolve():
            raise ValueError('Watchdog must follow the exact guarded manifest')
        self.engine_env, self.worker_env = (values(self.env_bytes[Path(s['env_file'])]) for s in (self.engine, self.worker))
        self._validate_current()
        self.before = {self.key(s): self.identity(s) for s in [*services, watchdog]}
        if not all(self.before.values()): raise RuntimeError('All prepared services must have live owned identities')
        self.expected = dict(self.before)
        self.support = support
        self.profile, self.runtime_descriptor = nccl_profile(self.original_profile)
        self.candidate_engine = {**self.engine, 'env_file': str(run / 'engine.env')}
        self.candidate_worker = {**self.worker, 'env_file': str(run / 'worker.env')}
        self.candidate_engine_bytes = env_bytes({**self.engine_env,
            'ENGINE_DISABLE_CUSTOM_ALL_REDUCE': '1', 'VLLM_ALLREDUCE_USE_SYMM_MEM': '0'})
        self.candidate_worker_bytes = env_bytes({**self.worker_env,
            'SCHEDULER_PROFILE_JSON': json.dumps(self.profile, separators=(',', ':'))})
        self.candidate = copy.deepcopy(self.document)
        for service in self.candidate['services']:
            if self.key(service) == self.key(self.engine): service['env_file'] = self.candidate_engine['env_file']
            if self.key(service) == self.key(self.worker): service['env_file'] = self.candidate_worker['env_file']
        self.engine_only = copy.deepcopy(self.candidate)
        self.engine_only['services'] = [s for s in self.engine_only['services'] if s['role'] != 'worker']
        self.original_engine_only = copy.deepcopy(self.document)
        self.original_engine_only['services'] = [s for s in self.original_engine_only['services'] if s['role'] != 'worker']
        self.allowed_manifest_bytes = {self.original}
        snapshot = {'original-manifest.json': self.original,
                    'original-engine.env': self.env_bytes[Path(self.engine['env_file'])],
                    'original-worker.env': self.env_bytes[Path(self.worker['env_file'])],
                    'engine.env': self.candidate_engine_bytes, 'worker.env': self.candidate_worker_bytes}
        binding = {'manifest_sha256': sha(self.original),
            'environment_sha256': {str(path): sha(raw) for path, raw in self.env_bytes.items()},
            'owned_identities': self.before, 'support': support, 'worker_budget_tokens': self.budget,
            'snapshot_sha256': {name: sha(raw) for name, raw in snapshot.items()}, 'controller_sha256': sha(Path(__file__).read_bytes())}
        if previous is not None:
            if previous.get('phase') != 'prepared' or previous.get('prepared_binding') != binding:
                raise RuntimeError('Prepared deployment changed; refusing snapshot overwrite or service stop')
            if any(private_read(run / name) != raw for name, raw in snapshot.items()):
                raise RuntimeError('Prepared private snapshot changed')
            self.report = previous
        else:
            if any(run.iterdir()): raise RuntimeError('Incomplete preparation requires inspection')
            for name, raw in snapshot.items(): atomic_bytes(run / name, raw)
            self.report = {'phase': 'prepared', 'prepared_at': timestamp(),
                'operation': 'qwen235-adaptive64-custom-collective-to-nccl', 'model': MODEL, 'revision': REVISION,
                'prepared_binding': binding, 'original_profile': self.original_profile, 'profile': self.profile,
                'runtime_descriptor': self.runtime_descriptor, 'worker_budget_tokens': self.budget,
                'events': [], 'installs_kernel_configuration': False,
                'limitations': ['Rates and price retain static16 calibration and are not recalibrated for adaptive64 or NCCL',
                    'Custom collective activity includes waiting/spin; this experiment does not measure network bandwidth',
                    '64 claimed tasks are not necessarily64 GPU-running sequences',
                    'Benchmark submission and final configuration selection require a separate explicit operation']}
            self.save()

    def _validate_current(self):
        e, w = self.engine_env, self.worker_env
        self.budget = int(w.get('WORKER_TOKEN_BUDGET', '0'))
        if (self.engine['name'] != 'qwen235b' or self.worker['name'] != 'qwen235b' or self.engine.get('gpu') != '0,1,2,3'
                or e.get('MODEL_NAME') != MODEL or e.get('ENGINE_REVISION') != REVISION
                or e.get('ENGINE_TENSOR_PARALLEL_SIZE') != '4' or e.get('ENGINE_MAX_NUM_SEQS') != '64'
                or e.get('ENGINE_MAX_MODEL_LEN') != str(CONTEXT) or e.get('ENGINE_MAX_NUM_BATCHED_TOKENS') != '4096'
                or e.get('ENGINE_DTYPE') != 'bfloat16' or float(e.get('ENGINE_GPU_MEMORY_UTILIZATION', '0')) != .90
                or e.get('ENGINE_ENABLE_PREFIX_CACHING') not in {'0', 'false'}
                or e.get('ENGINE_ENABLE_CHUNKED_PREFILL') not in {'1', 'true'} or e.get('ENGINE_GENERATION_CONFIG') != 'vllm'
                or e.get('ENGINE_DISABLE_CUSTOM_ALL_REDUCE', '0') != '0' or e.get('VLLM_ALLREDUCE_USE_SYMM_MEM', '0') != '0'
                or w.get('MODEL_NAME') != MODEL or w.get('ENGINE_MAX_MODEL_LEN') != str(CONTEXT)
                or w.get('WORKER_MAX_INFLIGHT') != '64' or w.get('WORKER_ADAPTIVE_KV_ENABLED', '').lower() not in {'true', '1'}
                or w.get('INFERENCE_BASE_URLS') != f"http://127.0.0.1:{self.engine['port']}"
                or not ((CONTEXT + BLOCK - 1) // BLOCK) * BLOCK <= self.budget <= MAX_BUDGET or self.budget % BLOCK):
            raise ValueError('Requires stable adaptive64/TP4/.90/4096/context18440 with current bounded exact-token admission')
        self.original_profile = validate_profile(json.loads(w['SCHEDULER_PROFILE_JSON']))
        if self.original_profile['profile_id'] != 'primary-h100-tp4-qwen235b-fp8-adaptive64':
            raise ValueError('Requires the original profiled adaptive64 custom-collective baseline')

    def guard(self, *, initial=False):
        super().guard(initial=initial)
        if any(sha(Path(path).read_bytes()) != checksum for path, checksum in self.support['source_sha256'].items()):
            raise RuntimeError('Reviewed manager, runtime or worker implementation changed')
        binding = self.report['prepared_binding']
        if sha(Path(__file__).read_bytes()) != binding['controller_sha256']:
            raise RuntimeError('Collective controller implementation changed')
        if any(sha(private_read(self.run / name)) != checksum for name, checksum in binding['snapshot_sha256'].items()):
            raise RuntimeError('Prepared rollback/candidate snapshot changed')

    def _observe_profile(self, profile):
        import psycopg
        expected = {'batchforge_worker_capacity': SLOTS, 'batchforge_worker_ready_engines': 1,
            'batchforge_worker_adaptive_kv_budget_tokens': self.budget, 'batchforge_worker_adaptive_kv_paused': 0,
            'batchforge_worker_adaptive_kv_waiting': 0, 'batchforge_worker_inflight': 0,
            'batchforge_worker_reserved_context_tokens': 0}
        with self._client() as client:
            response = client.get(f"http://127.0.0.1:{self.worker['port']}/metrics"); response.raise_for_status()
            if len(response.content) > 2 * 1024 * 1024: raise RuntimeError('Worker metrics exceed bound')
            gauges = metric_values(response.text, expected)
        if gauges != expected: raise RuntimeError('Worker admission gauges differ from the preserved budget')
        env = values(self.env_bytes[Path(next(s for s in self.services if s['role'] == 'api')['env_file'])])
        with psycopg.connect(env['DATABASE_URL'], connect_timeout=5,
                options='-c default_transaction_read_only=on -c statement_timeout=5000') as conn:
            row = conn.execute("""SELECT concurrency,scheduler_profile_id,runtime_fingerprint,
                coalesce((metadata->>'ready_engine_count')::integer,0),metadata->'kv_admission'
                FROM workers WHERE id=%s AND model=%s AND last_seen_at>clock_timestamp()-interval '60 seconds'""",
                (self.worker_env['WORKER_ID'], MODEL)).fetchone()
        if not row or row[:4] != (64, profile['profile_id'], profile['runtime_fingerprint'], 1) or row[4] != {
                'protocol': 'vllm-0.10.2-qwen-chat', 'budget': self.budget, 'block_size': 16,
                'single_worker_single_engine': True, 'paused': False}:
            raise RuntimeError('Worker registration does not match the exact profile, fingerprint and preserved budget')
        return {'gauges': gauges, 'registration_verified': True, 'profile_id': profile['profile_id'],
                'runtime_fingerprint': profile['runtime_fingerprint']}

    def start_engine(self, service, disabled):
        log = self.root / '.runtime/engine-qwen235b.log'
        previous = log.stat() if log.exists() else None
        offset = previous.st_size if previous else 0
        self.gate(service); self.invoke(service, 'start'); self.ready(service, 1200)
        current = log.stat()
        if current.st_size < offset or previous and (previous.st_dev, previous.st_ino) != (current.st_dev, current.st_ino):
            raise RuntimeError('Fresh engine log continuity cannot be established')
        with log.open('rb') as file: file.seek(offset); raw = file.read(16 * 1024**2 + 1)
        if len(raw) > 16 * 1024**2: raise RuntimeError('Fresh engine log exceeds bound')
        cache = fresh_cache(raw.decode(errors='replace'))
        if admission_budget(cache, self.budget) != self.budget:
            raise RuntimeError('Fresh95%cache cannot preserve the baseline token budget')
        return {'measured_cache_tokens': cache, 'preserved_budget_tokens': self.budget,
                'collective': self.path_observer(disabled), 'protocol': self.engine_observer()}

    def apply(self):
        self.guard(initial=True); self.assert_drained()
        self.report['original_collective_proof'] = self.path_observer(False)
        self.profile_observer(self.original_profile)
        self.report.update(phase='applying', activation_started_at=timestamp()); self.save()
        maintenance = worker_stopped = False
        try:
            maintenance = True; self.stop(self.watchdog); self.guard()
            worker_stopped = True; self.stop(self.worker); self.assert_drained(); self.stop(self.engine); self.guard()
            self._manifest(self.engine_only)
            self.report['candidate_engine_preflight'] = self.start_engine(self.candidate_engine, True)
            self.guard(); self.assert_drained(); self._manifest(self.candidate)
            self.invoke(self.candidate_worker, 'start'); self.ready(self.candidate_worker, 60)
            self.report['candidate_worker_preflight'] = self.profile_observer(self.profile)
            self.guard(); self.assert_drained()
            self.report.update(phase='complete', completed_at=timestamp(), protected_services_unchanged=True,
                engine_identity_after=self.identity(self.candidate_engine), worker_identity_after=self.identity(self.candidate_worker))
        except BaseException as error:
            self.report.update(phase='rolling_back', failure_type=type(error).__name__); self.save()
            try:
                self._manifest(self.engine_only); self.stop(self.watchdog)
                if worker_stopped:
                    self.stop(self.candidate_worker); self.stop(self.candidate_engine); self.guard()
                    self._manifest(self.original_engine_only)
                    self.report['rollback_engine_preflight'] = self.start_engine(self.engine, False)
                    self._manifest()
                    self.invoke(self.worker, 'start'); self.ready(self.worker, 60)
                    self.report['rollback_worker_preflight'] = self.profile_observer(self.original_profile)
                else: self._manifest()
                self.guard(); self.assert_drained()
                self.report.update(phase='rolled_back', rollback_completed_at=timestamp())
            except BaseException as rollback:
                self.report.update(phase='manual_recovery_required', rollback_failure_type=type(rollback).__name__)
                # Keep the watchdog from adopting an unvalidated worker.
                try:
                    self._manifest(self.original_engine_only)
                    self.stop(self.watchdog); self.stop(self.worker)
                    self.report['worker_excluded_and_owned_worker_stopped'] = True
                except BaseException as exclusion:
                    self.report.update(worker_excluded_and_owned_worker_stopped=False,
                        exclusion_failure_type=type(exclusion).__name__)
            raise
        finally:
            if maintenance:
                safe = self.report['phase'] in {'complete', 'rolled_back'} or self.report.get('worker_excluded_and_owned_worker_stopped') is True
                if safe:
                    try:
                        self.invoke(self.watchdog, 'start')
                        self.report.update(watchdog_restored=True, watchdog_identity_after=self.identity(self.watchdog))
                    except BaseException as error:
                        self.report.update(phase='manual_recovery_required', watchdog_restored=False, watchdog_failure_type=type(error).__name__)
                        self.save(); raise
                else: self.report['watchdog_restored'] = False
            self.save()
        return self.report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--operation', default='qwen235b-nccl-adaptive64-v1')
    args = parser.parse_args(); os.umask(0o077)
    run = private_operation_directory(ROOT, args.operation)
    fd = os.open(ROOT / '.runtime/qwen235b-maintenance.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        previous = json.loads(private_read(run / 'report.json')) if (run / 'report.json').exists() else None
        if args.apply and previous is None: raise RuntimeError('Explicit immutable preparation is required before apply')
        manifest, services, _ = load_manifest(ROOT / '.runtime/services-primary.json')
        engine = next(s for s in services if s['role'] == 'engine')
        watchdog = {'role': 'supervisor', 'name': 'primary', 'env_file': str(ROOT.parent / 'supervisor.env'),
                    'python': str(ROOT.parent / 'venv/bin/python')}
        roll = CollectiveRoll(root=ROOT, run=run, manifest_path=manifest, services=services, watchdog=watchdog,
                              support=source_evidence(ROOT, engine), previous=previous)
        if args.apply: roll.apply()
    print(json.dumps({'report': str(run / 'report.json'), 'phase': roll.report['phase'],
                      'activated': roll.report['phase'] == 'complete'}))


if __name__ == '__main__': main()
