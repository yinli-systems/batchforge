"""Finite, restart-safe engine benchmark with durable evidence on an existing volume.

Never starts/stops an engine, provisions hardware, or retries inference. A phase
intent is durable before its first HTTP request. Reusing an output directory is
refused, including after a crash: unknown inference outcomes are never replayed.
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import threading
import time
from types import SimpleNamespace
from urllib.parse import urlparse
from urllib.request import ProxyHandler, build_opener

from scripts import engine_matrix
from ops.storage_semantics import require_journal_filesystem


def now():
    return datetime.now(timezone.utc).isoformat()


def write_json(path, value):
    temporary = path.with_suffix(path.suffix + '.tmp')
    with temporary.open('x') as output:
        json.dump(value, output, indent=2, allow_nan=False)
        output.write('\n')
        output.flush()
        os.fsync(output.fileno())
    temporary.replace(path)
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


class Journal:
    def __init__(self, path):
        self.output = path.open('x')

    def __call__(self, event):
        self.output.write(json.dumps({'at': now(), **event}, allow_nan=False) + '\n')
        self.output.flush()
        os.fsync(self.output.fileno())

    def close(self):
        self.output.close()


def sample(url, path, stop):
    http = build_opener(ProxyHandler({}))
    with path.open('x') as output:
        while not stop.is_set():
            row = {'at': now(), 'monotonic': time.monotonic()}
            try:
                result = subprocess.run(['nvidia-smi', '--query-gpu=index,uuid,name,memory.total,memory.used,utilization.gpu,power.draw',
                    '--format=csv,noheader,nounits'], capture_output=True, text=True, timeout=5, check=True)
                row['gpu_csv'] = result.stdout.strip().splitlines()
            except (OSError, subprocess.SubprocessError) as error:
                row['gpu_error'] = type(error).__name__
            try:
                with http.open(url + '/metrics', timeout=3) as response:
                    body = response.read(262145)
                if len(body) > 262144:
                    raise ValueError('metrics size exceeded')
                row['engine_metrics'] = [line for line in body.decode().splitlines()
                    if not line.startswith('#') and any(key in line for key in (
                        'num_requests_running', 'num_requests_waiting', 'cache_usage_perc',
                        'prompt_tokens_total', 'generation_tokens_total', 'request_success_total', 'num_preemptions'))]
            except Exception as error:
                row['metrics_error'] = type(error).__name__
            output.write(json.dumps(row) + '\n')
            output.flush()
            os.fsync(output.fileno())
            stop.wait(1)


def execute(args):
    endpoint = urlparse(args.url)
    if endpoint.scheme != 'http' or endpoint.hostname not in {'127.0.0.1', 'localhost', '::1'} or endpoint.username:
        raise ValueError('Only an existing Pod-local HTTP engine is allowed')
    require_journal_filesystem(args.out)
    args.out.mkdir(mode=0o700, parents=True, exist_ok=False)
    os.umask(0o077)
    lock_name = hashlib.sha256(args.url.encode()).hexdigest()[:16]
    with open('/tmp/batchforge-gpu-campaign-' + lock_name + '.lock', 'a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        for name, source in [('workload.jsonl', args.workload), ('manifest.json', args.manifest)]:
            (args.out / name).write_bytes(source.read_bytes())
        sources = {str(Path(__file__).name): hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                   'engine_matrix.py': hashlib.sha256(Path(engine_matrix.__file__).read_bytes()).hexdigest()}
        plan = {'created_at': now(), 'pid': os.getpid(), 'url': args.url,
            'policies': args.policies, 'concurrency': args.concurrency, 'context_limit': args.context_limit,
            'request_timeout': args.request_timeout, 'source_sha256': sources,
            'cost': None, 'cost_note': 'No current provider invoice supplied; cost is unmeasured',
            'scope': 'Existing engine only; client dispatch policies, not platform placement',
            'forced_output': True, 'workload_sha256': hashlib.sha256(args.workload.read_bytes()).hexdigest()}
        write_json(args.out / 'plan.json', plan)
        sampler_stop = threading.Event()
        sampler = threading.Thread(target=sample, args=(args.url.rstrip('/'), args.out / 'samples.jsonl', sampler_stop), daemon=True)
        sampler.start()
        completed = []
        try:
            for index, policy in enumerate(args.policies):
                phase = f'{index:02d}-{policy}'
                journal = Journal(args.out / (phase + '-events.jsonl'))
                journal({'event': 'phase_started', 'policy': policy})
                config = SimpleNamespace(workload=args.out / 'workload.jsonl', manifest=args.out / 'manifest.json',
                    url=args.url, policy=policy, concurrency=args.concurrency, context_limit=args.context_limit,
                    request_timeout=args.request_timeout, hourly_cost=None, force_length=True, engine_label=args.engine_label)
                try:
                    report = asyncio.run(engine_matrix.run(config, event_sink=journal))
                    write_json(args.out / (phase + '-report.json'), report)
                    journal({'event': 'phase_finished', 'passed': report['passed']})
                    completed.append({'phase': phase, 'passed': report['passed'], 'summary': report['summary']})
                    print(json.dumps(completed[-1]), flush=True)
                    if not report['passed']:
                        raise RuntimeError('Inference failure: campaign stopped without retry')
                finally:
                    journal.close()
            write_json(args.out / 'result.json', {'passed': True, 'finished_at': now(), 'phases': completed})
        except BaseException as error:
            write_json(args.out / 'result.json', {'passed': False, 'finished_at': now(),
                'error_type': type(error).__name__, 'phases': completed,
                'recovery': 'Inspect durable request intents and terminals; never replay unknown outcomes'})
            raise
        finally:
            sampler_stop.set()
            sampler.join(10)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', default='http://127.0.0.1:8300')
    parser.add_argument('--workload', type=Path, required=True)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--engine-label', required=True)
    parser.add_argument('--policies', nargs='+', choices=['fifo', 'shortest', 'class-fair'], default=['fifo'])
    parser.add_argument('--concurrency', type=int, default=16)
    parser.add_argument('--context-limit', type=int, default=18440)
    parser.add_argument('--request-timeout', type=float, default=600)
    args = parser.parse_args()
    if not 1 <= args.concurrency <= 64 or not 0 < args.request_timeout <= 1800:
        parser.error('Concurrency must be 1..64 and timeout 0..1800 seconds')
    execute(args)


if __name__ == '__main__':
    main()
