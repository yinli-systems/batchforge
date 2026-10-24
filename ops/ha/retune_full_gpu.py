"""Prepare/apply one guarded 120B TP4 retune; no inference or model changes.

Default invocation only prepares private candidate files. Apply only after the
operator's workload comparison has finished. Existing owned-process primitives
perform identity checking, child cleanup and the four-rank memory start gate.
"""
from __future__ import annotations

import argparse
import copy
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import time

from backend.scheduling import validate_profile
from ops.supervise import Manager, ROOT, load_manifest, private_read, private_write


def file_env(path):
    result = {}
    for line in private_read(Path(path)).decode().splitlines():
        line = line.strip().removeprefix('export ').strip()
        if not line or line.startswith('#'):
            continue
        key, sep, value = line.partition('=')
        if not sep or not key.replace('_', '').isalnum():
            raise ValueError('invalid private environment assignment')
        result[key] = ' '.join(shlex.split(value, comments=True))
    return result


def worker_candidate(env, slots, allocation_hourly):
    result = dict(env)
    profile = validate_profile(json.loads(env['SCHEDULER_PROFILE_JSON']))
    profile['profile_id'] = f'primary-h100-tp4-gptoss120-seq{slots}'
    profile['hourly_cost_usd'] = allocation_hourly / slots
    profile['calibration_label'] = f'projection-from-seq8-to-seq{slots};rates-not-recalibrated'
    profile['cost_source'] = f'operator-allocation-{allocation_hourly:g}-USD-hour-divided-by-{slots}-slots;assumes-full-occupancy'
    validate_profile(profile)
    result.update(WORKER_MAX_INFLIGHT=str(slots), WORKER_TOKEN_BUDGET=str(slots * 18440),
                  ENGINE_MAX_MODEL_LEN='18440', SCHEDULER_PROFILE_JSON=json.dumps(profile, separators=(',', ':')))
    return result


def write_env(path, value):
    fd = os.open(path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w') as handle:
        handle.write(''.join(f'{k}={shlex.quote(v)}\n' for k, v in value.items()))
        handle.flush()
        os.fsync(handle.fileno())


def logged_capacity(log_text):
    values = re.findall(r'Maximum concurrency for 18,440 tokens per request: ([0-9.]+)x', log_text)
    return min(map(float, values)) if values else None


def candidate_manifest_path(root, run_name):
    return root / '.runtime' / (run_name + '-candidate.json')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--slots', type=int, choices=[32, 64], default=64)
    parser.add_argument('--batch-tokens', type=int, choices=[4096, 8192], default=4096)
    parser.add_argument('--allocation-hourly', type=float, default=16.10)
    args = parser.parse_args()
    os.umask(0o077)
    run = ROOT / '.runtime' / f'full120-seq{args.slots}-batch{args.batch_tokens}-v1'
    run.mkdir(mode=0o700, exist_ok=True)
    lock = (run / 'controller.lock').open('a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    report_path = run / 'report.json'
    if report_path.exists() and json.loads(report_path.read_text()).get('phase') in {'complete', 'rolled_back'}:
        print(json.dumps({'existing_terminal_report': str(report_path)}))
        return
    manifest_path, services, _ = load_manifest(ROOT / '.runtime/services-primary.json')
    original = json.loads(private_read(manifest_path))
    engine = next(s for s in services if s['role'] == 'engine' and s['name'] == 'gptoss120')
    worker = next(s for s in services if s['role'] == 'worker' and s['name'] == 'gptoss120')
    original_engine = file_env(engine['env_file'])
    original_worker = file_env(worker['env_file'])
    if original_engine['MODEL_NAME'] != 'openai/gpt-oss-120b' or original_engine.get('ENGINE_TENSOR_PARALLEL_SIZE') != '4':
        raise ValueError('Retune requires the existing GPT-OSS-120B TP4 deployment')
    new_engine = {**original_engine, 'ENGINE_MAX_NUM_SEQS': str(args.slots),
                  'ENGINE_GPU_MEMORY_UTILIZATION': '.85', 'ENGINE_MAX_NUM_BATCHED_TOKENS': str(args.batch_tokens),
                  'ENGINE_MAX_MODEL_LEN': '18440', 'ENGINE_GPU_MEMORY_MARGIN_MIB': '2048'}
    new_worker = worker_candidate(original_worker, args.slots, args.allocation_hourly)
    write_env(run / 'engine.env', new_engine)
    write_env(run / 'worker.env', new_worker)
    write_env(run / 'original-engine.env', original_engine)
    write_env(run / 'original-worker.env', original_worker)
    private_write(run / 'original-manifest.json', original)
    candidate = copy.deepcopy(original)
    for service in candidate['services']:
        if (service['role'], service['name']) == ('engine', 'gptoss120'):
            service['env_file'] = str(run / 'engine.env')
        if (service['role'], service['name']) == ('worker', 'gptoss120'):
            service['env_file'] = str(run / 'worker.env')
    candidate_manifest = candidate_manifest_path(ROOT, run.name)
    private_write(candidate_manifest, candidate)
    _, candidates, _ = load_manifest(candidate_manifest)
    next_engine = next(s for s in candidates if s['role'] == 'engine' and s['name'] == 'gptoss120')
    next_worker = next(s for s in candidates if s['role'] == 'worker' and s['name'] == 'gptoss120')
    report = {'phase': 'prepared', 'created_at': datetime.now(timezone.utc).isoformat(),
              'slots': args.slots, 'batch_tokens': args.batch_tokens, 'fraction': .85,
              'worker_token_budget': args.slots * 18440,
              'prior_profile': json.loads(original_worker['SCHEDULER_PROFILE_JSON']),
              'profile': json.loads(new_worker['SCHEDULER_PROFILE_JSON']),
              'candidate_manifest': str(candidate_manifest),
              'original_manifest_sha256': hashlib.sha256(private_read(manifest_path)).hexdigest(),
              'controller_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              'events': []}
    def save():
        private_write(report_path, report)
    save()
    if not args.apply:
        print(json.dumps({'prepared_report': str(report_path), 'activated': False}))
        return
    manager = Manager(timeout=300)
    watchdog = {'role': 'supervisor', 'name': 'primary', 'env_file': str(ROOT.parent / 'supervisor.env'),
                'python': str(ROOT.parent / 'venv/bin/python')}
    def invoke(service, action):
        result, error = manager.invoke(service, action)
        if error:
            raise RuntimeError(f'{service["role"]}-{service["name"]}:{action}:{error}')
        report['events'].append({'service': service['role']+'-'+service['name'], 'action': action, 'result': result})
        save()
        return result
    protected = {s['role']+'-'+s['name']: invoke(s, 'status')['pid'] for s in services if s['role'] in {'api', 'gateway'}}
    import psycopg
    api_env = file_env(next(s for s in services if s['role'] == 'api')['env_file'])
    with psycopg.connect(api_env['DATABASE_URL'], connect_timeout=10) as conn:
        active = conn.execute("SELECT count(*) FROM tasks WHERE model=%s AND status IN ('pending','running')", ('openai/gpt-oss-120b',)).fetchone()[0]
    if active:
        report['phase'] = 'refused_active_work'; report['active_tasks'] = active; save()
        raise RuntimeError('Wait for all current 120B tasks before retuning')
    log = ROOT / '.runtime/engine-gptoss120.log'
    offset = log.stat().st_size
    changed = False
    def await_engine(service, seconds=600):
        until = time.monotonic()+seconds
        while time.monotonic()<until:
            if manager.readiness(service):
                return
            status, error = manager.invoke(service, 'status')
            if error or not status['running']:
                raise RuntimeError('engine_exited_during_start')
            time.sleep(2)
        raise RuntimeError('engine_start_timeout')
    try:
        report['phase'] = 'applying'; save()
        invoke(watchdog, 'stop')
        invoke(worker, 'stop')
        changed = True
        invoke(engine, 'stop')
        allowed, reason = manager.start_gate(next_engine)
        report['fresh_start_gate'] = {'allowed': allowed, 'reason': reason}; save()
        if not allowed:
            raise RuntimeError('fresh_gpu_memory_gate_failed')
        private_write(manifest_path, candidate)
        invoke(next_engine, 'start')
        await_engine(next_engine)
        with log.open('rb') as handle:
            handle.seek(offset); new_log=handle.read().decode(errors='replace')
        capacity = logged_capacity(new_log)
        report['measured_full_context_capacity'] = capacity
        report['startup_capacity_lines'] = [re.sub(r'\x1b\[[0-9;]*m', '', line) for line in new_log.splitlines()
            if any(part in line for part in ('Model loading took', 'Available KV cache memory', 'GPU KV cache size', 'Maximum concurrency for'))]
        save()
        if capacity is None or capacity < args.slots:
            raise RuntimeError('insufficient_fresh_full_context_KV_capacity')
        invoke(next_worker, 'start')
        until = time.monotonic()+30
        while time.monotonic()<until and not manager.readiness(next_worker):
            time.sleep(1)
        if not manager.readiness(next_worker):
            raise RuntimeError('worker_not_ready_after_retune')
        invoke(watchdog, 'start')
        observed = {s['role']+'-'+s['name']: invoke(s, 'status')['pid'] for s in services if s['role'] in {'api', 'gateway'}}
        if observed != protected:
            raise RuntimeError('protected_service_identity_changed')
        report['protected_pids_unchanged'] = True
        report['gpu_memory_csv'] = subprocess.check_output(['nvidia-smi', '--query-gpu=index,memory.total,memory.used,memory.free', '--format=csv,noheader,nounits'], text=True)
        report['phase'] = 'complete'; report['finished_at'] = datetime.now(timezone.utc).isoformat(); save()
    except BaseException as exc:
        report['failure_type'] = type(exc).__name__
        if type(exc) is RuntimeError:
            report['failure_reason'] = str(exc)
        report['phase'] = 'rolling_back'; save()
        if changed:
            invoke(watchdog, 'stop')
            invoke(next_worker, 'stop')
            invoke(next_engine, 'stop')
            private_write(manifest_path, original)
            allowed, reason = manager.start_gate(engine)
            if not allowed:
                report['phase'] = 'rollback_blocked'; report['rollback_gate_reason'] = reason; save()
                raise RuntimeError('Rollback memory gate failed; watchdog remains paused') from None
            invoke(engine, 'start'); await_engine(engine)
            invoke(worker, 'start')
        invoke(watchdog, 'start')
        report['phase'] = 'rolled_back'; save()
        raise
    print(json.dumps({'report': str(report_path), 'phase': report['phase']}))


if __name__ == '__main__':
    main()
