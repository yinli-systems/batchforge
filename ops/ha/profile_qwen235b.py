"""One explicit, untimed four-rank CUDA activity probe after Qwen235B benchmarks.

No service is restarted and no profiler is installed. Four frozen public-text
requests are issued once; profile stop is attempted even after a failed start.
An uncertain stop is reported for operator recovery, never silently retried.
"""
from __future__ import annotations

import argparse
import asyncio
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import time

import httpx
import psycopg
from prometheus_client.parser import text_string_to_metric_families

from ops import engine_members
from ops.ha.retune_full_gpu import file_env
from ops.ha.retune_qwen235b import private_profile_directory, profiler_source_evidence
from ops.ha.switch_qwen235b import CONTEXT, MODEL, WORKLOAD20_SHA256, digest, utc_now
from ops.manage import owned_record
from ops.supervise import ROOT, load_manifest, private_read, private_write
from scripts import engine_matrix
from scripts.trace_intervals import analyze

COUNTERS = ('vllm:request_success_total', 'vllm:prompt_tokens_total', 'vllm:generation_tokens_total')
GAUGES = ('vllm:num_requests_running', 'vllm:num_requests_waiting')


def probe_workload(payload, manifest):
    if hashlib.sha256(payload).hexdigest() != WORKLOAD20_SHA256:
        raise ValueError('Frozen public workload changed')
    selected = []
    for raw in payload.splitlines():
        row = json.loads(raw)
        sample = manifest['examples'][row['custom_id']]
        if sample['rendered_prompt_tokens'] in (128, 2048, 8192, 16384) and sample['output_token_cap'] == 32:
            selected.append(row)
    if len(selected) != 4:
        raise ValueError('Expected four distinct public profiling inputs')
    body = b''.join(json.dumps(row, separators=(',', ':')).encode() + b'\n' for row in selected)
    result = dict(manifest, examples={r['custom_id']: manifest['examples'][r['custom_id']] for r in selected},
                  workload_sha256=hashlib.sha256(body).hexdigest())
    return body, result


def engine_metrics(payload):
    totals = {name: 0.0 for name in COUNTERS + GAUGES}
    found = set()
    for family in text_string_to_metric_families(payload):
        for sample in family.samples:
            if sample.name in totals and sample.labels.get('model_name') == MODEL and sample.labels.get('engine') == '0':
                totals[sample.name] += sample.value
                found.add(sample.name)
    if found != set(totals):
        raise ValueError('Expected model/engine-scoped counters and admission gauges are absent')
    return totals


def rank_mapping(log, members, *, identity=engine_members.identity):
    current = {member['pid']: member for member in members if identity(member['pid']) == member}
    ranks = {}
    for rank, pid in re.findall(r'\(Worker_TP([0-9]+) pid=([0-9]+)\)', log):
        rank, pid = int(rank), int(pid)
        if pid in current:
            if rank in ranks and ranks[rank] != current[pid]:
                raise ValueError('Ambiguous live TP rank identity')
            ranks[rank] = current[pid]
    if set(ranks) != {0, 1, 2, 3} or len({r['pid'] for r in ranks.values()}) != 4:
        raise ValueError('Four live TP ranks must match owned engine membership and startup logs')
    return ranks


def map_traces(paths, ranks):
    result = {}
    for rank, record in ranks.items():
        matches = [p for p in paths if re.search(r'_' + str(record['pid']) + r'\.[0-9]+\.pt\.trace\.json\.gz$', p.name)]
        if len(matches) != 1:
            raise ValueError('Expected exactly one fresh trace from each verified TP rank PID')
        result[rank] = matches[0]
    if len(set(result.values())) != len(paths):
        raise ValueError('Unmapped trace files appeared during the capture')
    return result


def ranks_from_retune(retune, leader, members, *, identity=engine_members.identity):
    starts = [event['result']['pid'] for event in retune.get('events', [])
              if event.get('service') == 'engine-qwen235b' and event.get('action') == 'start']
    if retune.get('phase') != 'complete' or starts != [leader['pid']]:
        raise ValueError('Live owned leader must be the exact engine launch in the completed seq16 report')
    return rank_mapping('\n'.join(retune['startup_evidence_lines']), members, identity=identity)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--max-trace-mib', type=int, default=256)
    args = parser.parse_args()
    if not 1 <= args.max_trace_mib <= 2048:
        parser.error('Expanded per-rank trace limit must be 1–2048 MiB')
    if not args.execute:
        print(json.dumps({'activated': False, 'requires': 'completed seq16 timed cohorts and explicit --execute'})); return
    os.umask(0o077)
    run = ROOT / '.runtime/qwen235b-trace-probe-v1'
    run.mkdir(mode=0o700, exist_ok=True)
    lock = (run / 'probe.lock').open('a'); fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    if (run / 'report.json').exists():
        raise RuntimeError('Stable trace probe already started; inspect it rather than resubmit')
    _, services, _ = load_manifest(ROOT / '.runtime/services-primary.json')
    engine = next(s for s in services if (s['role'], s['name']) == ('engine', 'qwen235b'))
    worker = next(s for s in services if (s['role'], s['name']) == ('worker', 'qwen235b'))
    engine_env, worker_env = file_env(engine['env_file']), file_env(worker['env_file'])
    retune = json.loads(private_read(ROOT / '.runtime/qwen235b-seq16-v1/report.json'))
    if (retune['phase'] != 'complete' or engine_env.get('MODEL_NAME') != MODEL
            or engine_env.get('ENGINE_MAX_NUM_SEQS') != '16' or worker_env.get('WORKER_MAX_INFLIGHT') != '16'):
        raise ValueError('Requires the successfully calibrated static16 deployment')
    profiler_source_evidence(engine['python'])
    trace_dir = private_profile_directory(ROOT, engine_env['VLLM_TORCH_PROFILER_DIR'])
    leader_path = ROOT / '.runtime/engine-qwen235b.json'
    leader = owned_record(leader_path)
    if not leader or not engine_members.capture(leader_path.with_suffix('.members.json'), leader):
        raise RuntimeError('Cannot verify current owned engine descendants')
    members = engine_members.read(leader_path.with_suffix('.members.json'))['members']
    ranks = ranks_from_retune(retune, leader, members)
    api_env = file_env(next(s for s in services if s['role'] == 'api')['env_file'])
    def drained():
        with psycopg.connect(api_env['DATABASE_URL'], connect_timeout=10) as conn:
            if conn.execute("SELECT count(*) FROM tasks WHERE status IN ('pending','running')").fetchone()[0]:
                raise RuntimeError('All platform work must be terminal before an isolated trace probe')
    drained()
    source = ROOT / '.runtime/qwen235b-v1'
    payload, manifest = probe_workload((source / 'workload-20.jsonl').read_bytes(), json.loads((source / 'workload-20-manifest.json').read_text()))
    (run / 'public-workload.jsonl').write_bytes(payload)
    private_write(run / 'public-workload-manifest.json', manifest)
    report = {'phase': 'prepared', 'created_at': utc_now(), 'controller_pid': os.getpid(), 'controller_sha256': digest(__file__),
              'scope': 'Separate untimed four-request CUDA activity probe; not a throughput benchmark',
              'engine_leader_identity': leader, 'rank_identities': ranks, 'profile_directory': str(trace_dir),
              'public_workload_sha256': manifest['workload_sha256'], 'profiler_stop_confirmed': False}
    def save(): private_write(run / 'report.json', report)
    save()
    previous_files = set(trace_dir.glob('*.pt.trace.json.gz'))
    headers = {'Authorization': 'Bearer ' + worker_env['INFERENCE_API_KEY']} if worker_env.get('INFERENCE_API_KEY') else {}
    with httpx.Client(base_url=f'http://127.0.0.1:{engine["port"]}', trust_env=False, headers=headers, timeout=180) as client:
        def metrics():
            response = client.get('/metrics'); response.raise_for_status()
            return engine_metrics(response.text)
        start_attempted = False
        try:
            before = metrics()
            if any(before[name] != 0 for name in GAUGES): raise RuntimeError('Engine is not idle')
            report.update(phase='starting_profile', metrics_before=before); save()
            start_attempted = True
            response = client.post('/start_profile'); response.raise_for_status()
            report.update(phase='profiling', capture_started_at=utc_now()); save()
            options = argparse.Namespace(url=str(client.base_url), workload=run / 'public-workload.jsonl',
                manifest=run / 'public-workload-manifest.json', engine_label='untimed-qwen235b-cuda-activity-probe',
                policy='fifo', concurrency=4, context_limit=CONTEXT, request_timeout=180, hourly_cost=None, force_length=True)
            previous = os.environ.get('INFERENCE_API_KEY'); os.environ['INFERENCE_API_KEY'] = worker_env.get('INFERENCE_API_KEY', '')
            try: result = asyncio.run(engine_matrix.run(options))
            finally:
                if previous is None: os.environ.pop('INFERENCE_API_KEY', None)
                else: os.environ['INFERENCE_API_KEY'] = previous
            private_write(run / 'inference.json', result)
            if not result.get('passed') or result['summary']['completed'] != 4:
                raise RuntimeError('One or more public profiling requests failed')
        except BaseException as error:
            report.update(phase='failed', failure_type=type(error).__name__)
            raise
        finally:
            if start_attempted:
                try:
                    response = client.post('/stop_profile'); response.raise_for_status()
                    report.update(profiler_stop_confirmed=True, capture_finished_at=utc_now())
                except Exception as error:
                    report.update(phase='failed', stop_failure_type=type(error).__name__)
            save()
        if not report['profiler_stop_confirmed']:
            raise RuntimeError('Profiler stop is unconfirmed; operator recovery required before new timed work')
        # vLLM exports worker counters asynchronously; allow bounded propagation
        # after capture, never extend the profile while waiting for metrics.
        until = time.monotonic() + 30
        while True:
            after = metrics()
            delta = {name: after[name] - before[name] for name in COUNTERS}
            if delta == dict(zip(COUNTERS, (4.0, 26752.0, 128.0))) and all(after[name] == 0 for name in GAUGES): break
            if time.monotonic() >= until:
                report.update(phase='failed', metrics_after=after, metrics_delta=delta); save()
                raise RuntimeError('Observed engine counters do not isolate exactly the four public profiling requests')
            time.sleep(1)
    drained()
    if owned_record(leader_path) != leader or any(engine_members.identity(r['pid']) != r for r in ranks.values()):
        raise RuntimeError('Owned engine identity changed during profiling')
    traces = map_traces(set(trace_dir.glob('*.pt.trace.json.gz')) - previous_files, ranks)
    report.update(phase='analyzing', metrics_after=after, metrics_delta=delta); save()
    summaries = {rank: dict(analyze(path, args.max_trace_mib * 1024**2),
                           trace_file=str(path), compressed_sha256=digest(path),
                           compressed_bytes=path.stat().st_size) for rank, path in traces.items()}
    report.update(phase='complete', finished_at=utc_now(), ranks=summaries, all_four_tp_ranks_present=True,
        limitations=['Kernel-name categories are heuristic and do not measure SM/Tensor Core utilization or FLOPS',
                     'Communication intervals can include waiting or spin kernels',
                     'Independent rank windows are not summed into a global elapsed time',
                     'The four-request trace has profiler overhead and differs from the timed16-slot workload'])
    save()
    print(json.dumps({'report': str(run / 'report.json'), 'phase': 'complete', 'all_four_tp_ranks_present': True}))


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        path = ROOT / '.runtime/qwen235b-trace-probe-v1/report.json'
        if path.exists():
            failure = json.loads(private_read(path))
            if failure.get('controller_pid') == os.getpid() and failure.get('phase') != 'complete':
                failure.update(phase='failed', failure_type=type(error).__name__, finished_at=utc_now())
                private_write(path, failure)
        print(json.dumps({'phase': 'failed', 'failure_type': type(error).__name__, 'report': str(path)}))
        raise SystemExit(1) from None
