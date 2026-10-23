"""Private run controller: fixed released harness, observation only, no service mutation."""
import argparse
import asyncio
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
import time

import httpx

APP = Path('/root/batchforge-runtime/app')
sys.path.insert(0, str(APP))
from scripts import engine_matrix

ROOT = APP / '.runtime/token-matrix-gptoss120'

def now():
    return datetime.now(timezone.utc).isoformat()

def save(path, value):
    temporary = path.with_suffix(path.suffix + '.tmp')
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w') as f:
        json.dump(value, f, indent=2)
        f.write('\n')
    temporary.replace(path)

def identity(pid):
    try:
        raw = Path(f'/proc/{pid}/stat').read_text()
        fields = raw[raw.rfind(')') + 2:].split()
        if fields[0] == 'Z':
            return None
        return {'pid': pid, 'start_ticks': fields[19],
                'cwd': str(Path(f'/proc/{pid}/cwd').resolve())}
    except (OSError, ValueError, IndexError):
        return None

def descendants(record):
    expected = {k: record[k] for k in ('pid', 'start_ticks', 'cwd')}
    if identity(record['pid']) != expected:
        raise RuntimeError('Owned engine leader identity changed')
    owned = {record['pid']: expected}
    pending = [record['pid']]
    while pending:
        parent = pending.pop()
        try:
            tasks = list(Path(f'/proc/{parent}/task').iterdir())
        except OSError:
            continue
        for task in tasks:
            try:
                children = map(int, (task / 'children').read_text().split())
                for pid in children:
                    current = identity(pid)
                    if pid not in owned and current and current['cwd'] == str(APP):
                        owned[pid] = current
                        pending.append(pid)
            except OSError:
                continue
    return owned

async def measure(policy, record):
    report_path = ROOT / f'{policy}-report.json'
    if report_path.exists():
        raise RuntimeError(f'Refusing duplicate policy {policy}')
    samples, pmon_samples, errors = [], [], []
    members = descendants(record)
    stop = asyncio.Event()
    started = time.monotonic()

    async def engine_observer():
        async with httpx.AsyncClient(trust_env=False, timeout=3) as client:
            ticks = 0
            while not stop.is_set():
                at = now()
                try:
                    response = await client.get('http://127.0.0.1:8300/metrics')
                    response.raise_for_status()
                    values = {}
                    for line in response.text.splitlines():
                        if line and not line.startswith('#') and any(s in line for s in (
                            'num_requests_running{', 'num_requests_waiting{',
                            'gpu_cache_usage_perc{', 'kv_cache_usage_perc{')):
                            label, value = line.rsplit(' ', 1)
                            values[label.split('{')[0]] = float(value)
                    samples.append({'at': at, 'values': values})
                    if ticks % 30 == 0:
                        print(json.dumps({'phase': policy, 'elapsed_seconds': round(time.monotonic()-started, 1),
                                          'observed_engine_gauges': values}), flush=True)
                    ticks += 1
                except (httpx.HTTPError, ValueError) as exc:
                    errors.append({'at': at, 'source': 'engine_metrics', 'error_type': type(exc).__name__})
                try:
                    await asyncio.wait_for(stop.wait(), .5)
                except TimeoutError:
                    pass

    async def pmon_observer():
        process = await asyncio.create_subprocess_exec('nvidia-smi', 'pmon', '-s', 'u', '-d', '1',
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        try:
            while not stop.is_set():
                try:
                    line = await asyncio.wait_for(process.stdout.readline(), 2)
                except TimeoutError:
                    continue
                if not line:
                    break
                parts = line.decode(errors='replace').split()
                if len(parts) < 5 or not parts[0].isdigit() or not parts[1].isdigit():
                    continue
                pid = int(parts[1])
                if pid not in members or identity(pid) != members[pid]:
                    continue
                # Retain only kernel-proven owned PID rows; omit unrelated rows and command names.
                pmon_samples.append({'at': now(), 'gpu': int(parts[0]), 'pid': pid,
                    'sm_percent': int(parts[3]) if parts[3].isdigit() else None,
                    'memory_activity_percent': int(parts[4]) if parts[4].isdigit() else None})
        finally:
            if process.returncode is None:
                process.terminate()
            await process.wait()
            stderr = await process.stderr.read()
            if stderr:
                errors.append({'source': 'pmon', 'stderr_sha256': hashlib.sha256(stderr).hexdigest()})

    observers = [asyncio.create_task(engine_observer()), asyncio.create_task(pmon_observer())]
    args = argparse.Namespace(url='http://127.0.0.1:8300', workload=ROOT/'workload.jsonl',
        manifest=ROOT/'manifest.json', context_limit=18440, policy=policy, concurrency=1,
        force_length=True, request_timeout=600, hourly_cost=None,
        engine_label='real:GPT-OSS120B;TP4;4sharedH100;MXFP4-Marlin;BF16-activations;ctx18440;seq1;batched1024;fraction0.24')
    try:
        report = await engine_matrix.run(args)
    finally:
        stop.set()
        await asyncio.gather(*observers)
    lo, hi = report['measurement_started_at'], report['finished_at']
    samples = [s for s in samples if lo <= s['at'] <= hi]
    pmon_samples = [s for s in pmon_samples if lo <= s['at'] <= hi]
    grouped = {}
    for row in pmon_samples:
        key = str(row['gpu'])
        grouped.setdefault(key, []).append(row['sm_percent'])
    report['gpu_observation'] = {
        'scope': 'Only kernel-descendant PIDs of verified managed engine leader; other GPU workloads excluded',
        'sampling_interval_seconds': 1,
        'owned_engine_pid': record['pid'], 'owned_process_identities': list(members.values()),
        'per_gpu_owned_process_sm_percent': {
            gpu: {'samples': len(v), 'valid_samples': sum(x is not None for x in v),
                  'mean': sum(x for x in v if x is not None)/sum(x is not None for x in v) if any(x is not None for x in v) else None,
                  'max': max((x for x in v if x is not None), default=None)}
            for gpu, v in grouped.items()},
        'raw_samples_file': f'{policy}-observation-private.json',
        'per_process_not_physical_gpu_utilization': True,
        'not_supported_or_missing': not bool(pmon_samples),
        'engine_gauge_samples': len(samples),
        'engine_gauge_maxima': {key: max(s['values'].get(key, 0) for s in samples)
                               for key in {k for s in samples for k in s['values']}},
        'errors': errors,
        'cost': None,
        'limitations': ['pmon samples device-driver process activity rather than exclusive GPU ownership',
                        'KV cache and running/waiting gauges are separate from SM utilization',
                        'No marginal GPU billing rate is available']}
    save(ROOT/f'{policy}-observation-private.json', {'engine': samples, 'pmon_owned_only': pmon_samples})
    save(report_path, report)
    print(json.dumps({'phase': policy, 'finished_at': now(), 'passed': report['passed'], 'summary': report['summary']}), flush=True)
    return {'policy': policy, 'passed': report['passed'], 'report': str(report_path)}

async def main():
    lock = ROOT/'matrix-controller.started.json'
    fd = os.open(lock, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w') as f:
        json.dump({'pid': os.getpid(), 'started_at': now()}, f)
    record = json.loads((APP/'.runtime/engine-gptoss120.json').read_text())
    assert record['pid'] == 139720
    assert hashlib.sha256((ROOT/'workload.jsonl').read_bytes()).hexdigest() == 'ffe741d9c87bd52ecf571f4b5c6a8ae5c5eac3d46f997399f49b2ff4dcc3f634'
    result = {'started_at': now(), 'policies': [], 'passed': False,
              'warmup': 'Earlier untimed natural 42 answer and exact16K+2K generation; raw proof stored separately',
              'controller_scope': 'Observation and direct engine HTTP only; no service, engine, or worker configuration mutation'}
    try:
        for policy in ('fifo', 'shortest', 'class-fair'):
            result['policies'].append(await measure(policy, record))
        result['passed'] = all(p['passed'] for p in result['policies'])
    except BaseException as exc:
        result['error_type'] = type(exc).__name__
        raise
    finally:
        result['finished_at'] = now()
        save(ROOT/'controller-report.json', result)

if __name__ == '__main__':
    asyncio.run(main())
