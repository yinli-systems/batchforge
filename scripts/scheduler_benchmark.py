"""Replay a frozen mixed workload through the durable API and measure DB latency.

Run on the inference host. Reports contain task metadata, never model responses,
credentials, or prompts. GPU observations are physical-device activity and must
not be attributed exclusively to BatchForge on a shared host.
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

import httpx
import psycopg
from psycopg.rows import dict_row

from scripts import benchmark
from ops.manage import load_env


def save(path, data):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_suffix('.tmp')
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w') as handle:
        json.dump(data, handle, indent=2, default=str)
        handle.write('\n')
    temporary.replace(path)


async def run(args):
    root = args.report.parent
    if args.report.exists():
        raise ValueError('Refusing to overwrite an experiment report')
    env = load_env(args.env_file)
    os.environ['BATCHFORGE_API_KEY'] = env['BATCHFORGE_API_KEY']
    source = args.workload.read_bytes()
    rows = [json.loads(line) for line in source.splitlines() if line.strip()]
    repeated = []
    for repeat in range(args.repeats):
        for row in rows:
            item = {**row, 'body':dict(row['body']), 'custom_id': f"repeat{repeat}-{row['custom_id']}"}
            if args.fixed_output:
                item['body']['min_tokens'] = item['body'].get('max_tokens',item['body'].get('max_completion_tokens',256))
            repeated.append(item)
    workload = root / (args.label + '-input.jsonl')
    fd = os.open(workload, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w') as f:
        for row in repeated:
            f.write(json.dumps(row, ensure_ascii=False) + '\n')
    stop = asyncio.Event()
    samples = []

    async def observe():
        async with httpx.AsyncClient(timeout=3, trust_env=False) as client:
            while not stop.is_set():
                sample = {'at': datetime.now(timezone.utc).isoformat()}
                try:
                    reply = await client.get(args.engine_url + '/metrics')
                    reply.raise_for_status()
                    sample['engine'] = {}
                    for line in reply.text.splitlines():
                        if line and not line.startswith('#') and any(s in line for s in
                            ('num_requests_running{', 'num_requests_waiting{', 'gpu_cache_usage_perc{', 'kv_cache_usage_perc{')):
                            key, value = line.rsplit(' ', 1)
                            sample['engine'][key.split('{')[0]] = float(value)
                    proc = await asyncio.create_subprocess_exec('nvidia-smi',
                        '--query-gpu=index,utilization.gpu,memory.used,memory.total,power.draw',
                        '--format=csv,noheader,nounits', stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.DEVNULL)
                    stdout, _ = await asyncio.wait_for(proc.communicate(), 4)
                    sample['physical_gpus'] = [[float(v.strip()) for v in line.split(',')]
                                               for line in stdout.decode().splitlines()]
                except Exception as exc:
                    sample['error_type'] = type(exc).__name__
                samples.append(sample)
                try:
                    await asyncio.wait_for(stop.wait(), 1)
                except TimeoutError:
                    pass

    observer = asyncio.create_task(observe())
    params = argparse.Namespace(url=args.url, model=rows[0]['body']['model'],
        tasks=len(repeated), batches=args.batches, max_tokens=2048, temperature=0,
        prompt='', engine_label=args.label, deadline=3600, request_timeout=120,
        poll=.5, workload_dir=root / (args.label + '-submitted'), input_jsonl=workload,
        output_dir=root / (args.label + '-outputs'), submission_concurrency=4)
    started = time.monotonic()
    try:
        report = await benchmark.run(params)
    finally:
        stop.set()
        await observer
    report['experiment'] = {'label': args.label, 'source_sha256': hashlib.sha256(source).hexdigest(),
        'source_repetitions': args.repeats, 'policy_is_server_side': True,
        'controlled_output_length':args.fixed_output,
        'physical_gpu_metrics_include_other_processes': True}
    with psycopg.connect(env['DATABASE_URL'], row_factory=dict_row) as conn:
        version = conn.execute('SELECT max(version) AS version FROM batchforge_schema_migrations').fetchone()['version']
        tasks = conn.execute("""SELECT t.id,t.custom_id,t.status,t.worker_id,t.attempts,
            t.prompt_tokens,t.completion_tokens,
            extract(epoch from (t.finished_at-b.created_at))::float8 AS completion_seconds,
            extract(epoch from (min(a.started_at)-b.created_at))::float8 AS first_queue_seconds,
            count(*) FILTER (WHERE a.status='completed') AS accepted_attempts
            FROM tasks t JOIN batches b ON b.id=t.batch_id
            LEFT JOIN attempts a ON a.task_id=t.id WHERE t.batch_id=ANY(%s)
            GROUP BY t.id,b.created_at ORDER BY t.custom_id""", (report['batch_ids'],)).fetchall()
        decisions = []
        if version >= 6:
            decisions = conn.execute("""SELECT a.task_id,a.number,a.status,a.scheduling_decision,
                extract(epoch FROM a.finished_at-a.started_at)::float8 AS observed_attempt_seconds
                FROM attempts a WHERE a.batch_id=ANY(%s) ORDER BY a.task_id,a.number""", (report['batch_ids'],)).fetchall()
    report['database_schema_version'] = version
    report['task_completion_seconds'] = benchmark.distribution([r['completion_seconds'] for r in tasks if r['completion_seconds'] is not None])
    report['task_first_queue_seconds'] = benchmark.distribution([r['first_queue_seconds'] for r in tasks if r['first_queue_seconds'] is not None])
    report['accepted_exactly_once'] = len(tasks) == len(repeated) and all(r['accepted_attempts'] == 1 for r in tasks)
    report['passed'] &= report['accepted_exactly_once']
    report['tasks'] = tasks
    caps = {row['custom_id']:row['body'].get('max_tokens',row['body'].get('max_completion_tokens',256)) for row in repeated}
    groups = {}
    for task in tasks:
        cap = caps[task['custom_id'].removeprefix(report['run_id']+'-')]
        task['output_cap'] = cap
        ratio=task['prompt_tokens']/cap
        label='prefill_heavy' if ratio>=16 else 'decode_heavy' if ratio<=4 else 'balanced'
        groups.setdefault(label,[]).append(task)
    report['by_class'] = {label:{'tasks':len(group),'completion_seconds':benchmark.distribution([r['completion_seconds'] for r in group if r['completion_seconds'] is not None]),
        'prompt_tokens':sum(r['prompt_tokens'] for r in group),'completion_tokens':sum(r['completion_tokens'] for r in group)} for label,group in groups.items()}
    report['workload_class_definition'] = 'observed prompt/requested output cap >=16 prefill_heavy, <=4 decode_heavy, otherwise balanced'
    if args.fixed_output:
        report['all_output_lengths_exact'] = all(t['completion_tokens']==t['output_cap'] for t in tasks)
        report['passed'] &= report['all_output_lengths_exact']
    report['attempt_decisions'] = decisions
    report['observed_policies'] = sorted({d['scheduling_decision'].get('policy', 'legacy') for d in decisions}) if decisions else ['legacy']
    report['observed_prompt_tokens_per_second'] = report['observed_prompt_tokens'] / report['elapsed_seconds']
    report['observed_completion_tokens_per_second'] = report['observed_completion_tokens'] / report['elapsed_seconds']
    report['gpu_observation'] = {'samples': len(samples), 'measurement_seconds_including_validation': time.monotonic()-started,
        'engine_maxima': {key: max(s.get('engine', {}).get(key, 0) for s in samples)
                          for key in {key for s in samples for key in s.get('engine', {})}},
        'physical_device_utilization': {str(gpu): benchmark.distribution([row[1] for s in samples for row in s.get('physical_gpus', []) if int(row[0]) == gpu]) for gpu in range(4)}}
    # Allocation scenarios are not invoice or marginal-cost measurements.
    hourly = args.hourly_cost
    report['cost'] = None if hourly is None else {'assumed_whole_pod_usd_per_hour': hourly,
        'basis': 'operator supplied whole-Pod allocation scenario; not current invoice or marginal inference cost',
        'run_allocation_usd': hourly * report['elapsed_seconds'] / 3600,
        'usd_per_million_durable_completions': hourly * report['elapsed_seconds'] / 3600 / report['completed'] * 1e6 if report['completed'] else None}
    save(root / (args.label + '-observations.json'), samples)
    save(args.report, report)
    print(json.dumps({k: report[k] for k in ('passed','run_id','completed','elapsed_seconds','completed_tasks_per_second','task_completion_seconds','accepted_exactly_once','cost')}), flush=True)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--env-file', type=Path, required=True)
    parser.add_argument('--workload', type=Path, required=True)
    parser.add_argument('--report', type=Path, required=True)
    parser.add_argument('--label', required=True)
    parser.add_argument('--repeats', type=int, default=5)
    parser.add_argument('--batches', type=int, default=1)
    parser.add_argument('--url', default='http://127.0.0.1:8080')
    parser.add_argument('--engine-url', default='http://127.0.0.1:8300')
    parser.add_argument('--hourly-cost', type=float)
    parser.add_argument('--fixed-output', action='store_true',help='Set min_tokens equal to the cap; measures fixed work, not output quality')
    args = parser.parse_args()
    if args.repeats < 1 or args.batches < 1:
        parser.error('positive repetitions and batches required')
    args.report.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    report = asyncio.run(run(args))
    raise SystemExit(0 if report['passed'] else 1)


if __name__ == '__main__':
    main()
