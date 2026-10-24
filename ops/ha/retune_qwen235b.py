"""Prepare/apply Qwen235B TP4 static full-context8 ->16, with idle tracing configured.

No profile endpoint is invoked. The worker stays absent during a fresh direct
20-cell calibration. Failed startup/capacity/calibration rolls back the8-slot
manifest and environments through the same owned-process switch implementation.
"""
from __future__ import annotations

import argparse
import asyncio
import copy
import fcntl
import json
import math
import os
from pathlib import Path
import stat
import subprocess

import psycopg

from backend.scheduling import validate_profile
from ops.ha.retune_full_gpu import file_env, write_env
from ops.ha.switch_qwen235b import (CONTEXT, MODEL, REVISION, WORKLOAD20_SHA256,
                                   Switch, digest, phase_calibration)
from ops.supervise import Manager, ROOT, load_manifest, private_read, private_write
from scripts import engine_matrix

SLOTS = 16


def private_profile_directory(root, requested):
    root = root.resolve()
    path = Path(os.path.abspath(requested))
    runtime = root / '.runtime'
    if path == runtime or not path.is_relative_to(runtime) or path.resolve() != path:
        raise ValueError('Profiler directory must be a nonsymlink child of app/.runtime')
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    for parent in (path, *path.parents):
        if parent == runtime:
            break
        info = parent.stat()
        if info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) & 0o077:
            raise ValueError('Profiler directory and its private ancestors must be owner-only')
    return path


def retuned_worker(source, profile=None):
    value = dict(source)
    value.pop('SCHEDULER_PROFILE_JSON', None)
    value.update(WORKER_MAX_INFLIGHT=str(SLOTS), WORKER_TOKEN_BUDGET=str(SLOTS * CONTEXT),
                 ENGINE_MAX_MODEL_LEN=str(CONTEXT))
    if profile is not None:
        value['SCHEDULER_PROFILE_JSON'] = json.dumps(validate_profile(profile), separators=(',', ':'))
    return value


def calibrated_profile(original, fit, hourly, checksum):
    value = validate_profile(original)
    value.update(profile_id='primary-h100-tp4-qwen235b-fp8-seq16',
                 prefill_tokens_per_second=fit['prefill_http_first_text_proxy']['tokens_per_second'],
                 decode_tokens_per_second=fit['decode_http_stream_span_proxy']['tokens_per_second'],
                 hourly_cost_usd=hourly / SLOTS,
                 calibration_label=f'actual-qwen235b-fixed20-c16-http-phase-NNLS:{checksum[:16]}',
                 cost_source=f'operator-allocation-{hourly:g}-USD-hour-divided-by-16-slots;assumes-full-occupancy')
    return validate_profile(value)


def profiler_source_evidence(python):
    code = '''from pathlib import Path
import importlib.util,json,hashlib
base=Path(importlib.util.find_spec('vllm').origin).parent
paths=['v1/worker/gpu_worker.py','entrypoints/openai/api_server.py']
print(json.dumps({p:hashlib.sha256((base/p).read_bytes()).hexdigest() for p in paths}))'''
    observed = json.loads(subprocess.check_output([python, '-c', code], text=True))
    expected = {'v1/worker/gpu_worker.py': '8a50c7c633ea5f9e14360029e1551929975a91cdaa750de2be3ae9922bdb1f5f',
                'entrypoints/openai/api_server.py': '40b1245fcfa5a18f215a232fc25c99293f16c37dac9c4bf0238d49a1614d7563'}
    if observed != expected:
        raise ValueError('Installed profiler implementation differs from reviewed vLLM0.10.2 source')
    return {'source_sha256': observed, 'initialization': 'Constructed but not started; explicit profile RPC starts/stops capture',
            'timed_runs': 'No start_profile or stop_profile call is made by this controller'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--profile-dir', type=Path, default=ROOT / '.runtime/qwen235b-traces-v1')
    parser.add_argument('--allocation-hourly', type=float, default=16.10)
    args = parser.parse_args()
    if not math.isfinite(args.allocation_hourly) or args.allocation_hourly < 0:
        parser.error('Allocation must be finite and nonnegative')
    os.umask(0o077)
    run = ROOT / '.runtime/qwen235b-seq16-v1'
    run.mkdir(mode=0o700, exist_ok=True)
    lock = (run / 'controller.lock').open('a'); fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    if (run / 'report.json').exists() and json.loads((run / 'report.json').read_text())['phase'] != 'prepared':
        raise RuntimeError('Existing retune requires inspection instead of duplicate submission')
    manifest_path, services, _ = load_manifest(ROOT / '.runtime/services-primary.json')
    original = json.loads(private_read(manifest_path))
    engine = next(s for s in services if (s['role'], s['name']) == ('engine', 'qwen235b'))
    worker = next(s for s in services if (s['role'], s['name']) == ('worker', 'qwen235b'))
    old_engine, old_worker = file_env(engine['env_file']), file_env(worker['env_file'])
    if (old_engine.get('MODEL_NAME') != MODEL or old_engine.get('ENGINE_REVISION') != REVISION
            or old_engine.get('ENGINE_MAX_NUM_SEQS') != '8' or old_worker.get('WORKER_MAX_INFLIGHT') != '8'):
        raise ValueError('Requires the verified pinned235B8-slot deployment')
    source = ROOT / '.runtime/qwen235b-v1'
    if digest(source / 'workload-20.jsonl') != WORKLOAD20_SHA256:
        raise ValueError('Frozen public20-cell matrix changed')
    original_profile = validate_profile(json.loads(old_worker['SCHEDULER_PROFILE_JSON']))
    profiler = profiler_source_evidence(engine['python'])
    profile_dir = private_profile_directory(ROOT, args.profile_dir)
    next_engine = dict(old_engine)
    next_engine.update(ENGINE_MAX_NUM_SEQS='16', ENGINE_MAX_NUM_BATCHED_TOKENS='4096',
        ENGINE_GPU_MEMORY_UTILIZATION='.90', ENGINE_MAX_MODEL_LEN=str(CONTEXT),
        VLLM_TORCH_PROFILER_DIR=str(profile_dir), VLLM_TORCH_PROFILER_WITH_STACK='0',
        VLLM_TORCH_PROFILER_RECORD_SHAPES='0', VLLM_TORCH_PROFILER_WITH_PROFILE_MEMORY='0',
        VLLM_TORCH_PROFILER_WITH_FLOPS='0', VLLM_RPC_TIMEOUT='600000')
    write_env(run / 'engine.env', next_engine)
    write_env(run / 'worker.env', retuned_worker(old_worker))
    write_env(run / 'original-engine.env', old_engine)
    write_env(run / 'original-worker.env', old_worker)
    private_write(run / 'original-manifest.json', original)
    candidate = copy.deepcopy(original)
    for item in candidate['services']:
        if (item['role'], item['name']) in {('engine', 'qwen235b'), ('worker', 'qwen235b')}:
            item['env_file'] = str(run / (item['role'] + '.env'))
    candidate_path = ROOT / '.runtime/qwen235b-seq16-v1-candidate.json'
    private_write(candidate_path, candidate)
    _, candidates, _ = load_manifest(candidate_path)
    new_engine = next(s for s in candidates if (s['role'], s['name']) == ('engine', 'qwen235b'))
    new_worker = next(s for s in candidates if (s['role'], s['name']) == ('worker', 'qwen235b'))
    watchdog = dict(role='supervisor', name='primary', env_file=str(ROOT.parent / 'supervisor.env'), python=str(ROOT.parent / 'venv/bin/python'))
    switch = Switch(root=ROOT, run=run, manager=Manager(timeout=300), manifest_path=manifest_path,
        original=original, candidate=candidate, old_engine=engine, old_worker=worker,
        new_engine=new_engine, new_worker=new_worker, watchdog=watchdog,
        protected_services=[s for s in services if s['role'] in {'api', 'gateway'}],
        original_env_hashes={s['env_file']: digest(s['env_file']) for s in (engine, worker)},
        slots=SLOTS, configure_worker=retuned_worker)
    switch.report.update(operation='235B static full-context8-to16', controller_sha256=digest(__file__),
        switch_implementation_sha256=digest(Path(__file__).with_name('switch_qwen235b.py')),
        original_manifest_sha256=digest(manifest_path), original_profile=original_profile,
        profile_directory=str(profile_dir), profiler=profiler, candidate_manifest=str(candidate_path))
    switch.save()
    if not args.apply:
        print(json.dumps({'report': str(run / 'report.json'), 'phase': 'prepared', 'activated': False})); return
    api_env = file_env(next(s for s in services if s['role'] == 'api')['env_file'])
    def drained():
        with psycopg.connect(api_env['DATABASE_URL'], connect_timeout=10) as conn:
            active = conn.execute("SELECT count(*) FROM tasks WHERE model=%s AND status IN ('pending','running')", (MODEL,)).fetchone()[0]
        if active:
            raise RuntimeError('235B platform cohorts must be terminal before retune')
    def calibrate(_):
        args = argparse.Namespace(url=f'http://127.0.0.1:{engine["port"]}', workload=source / 'workload-20.jsonl',
            manifest=source / 'workload-20-manifest.json', engine_label='qwen235b-fp8-tp4-seq16-calibration',
            policy='fifo', concurrency=SLOTS, context_limit=CONTEXT, request_timeout=1200,
            hourly_cost=allocation, force_length=True)
        previous = os.environ.get('INFERENCE_API_KEY'); os.environ['INFERENCE_API_KEY'] = old_worker.get('INFERENCE_API_KEY', '')
        try:
            result = asyncio.run(engine_matrix.run(args))
        finally:
            if previous is None: os.environ.pop('INFERENCE_API_KEY', None)
            else: os.environ['INFERENCE_API_KEY'] = previous
        private_write(run / 'direct-calibration.json', result)
        if result.get('passed') is not True or result['summary']['completed'] != 20:
            raise RuntimeError('Fresh16-slot calibration did not complete all20 requests')
        fit = phase_calibration(result['records'], concurrency=SLOTS)
        private_write(run / 'calibration-fit.json', fit)
        checksum = digest(run / 'direct-calibration.json')
        switch.report.update(calibration=fit, direct_calibration_sha256=checksum, profiler_started=False)
        return calibrated_profile(original_profile, fit, allocation, checksum)
    allocation = args.allocation_hourly
    switch.apply(assert_drained=drained, calibration_and_profile=calibrate)
    print(json.dumps({'report': str(run / 'report.json'), 'phase': switch.report['phase']}))


if __name__ == '__main__':
    main()
