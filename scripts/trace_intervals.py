"""Summarize per-rank CUDA kernel intervals without summing overlapping wall time.

Explicit rank=file mappings must come from verified engine process identities.
Kernel-name classes are heuristic; this does not measure SM/Tensor Core activity.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
import gzip
import hashlib
import json
import math
from pathlib import Path


def category(name):
    name = name.lower()
    # These observed vLLM TP kernels perform custom cross-device collectives;
    # their intervals may include peer waiting/spin, not just data movement.
    if any(s in name for s in ('vllm::cross_device_reduce_1stage<', 'vllm::cross_device_reduce_2stage<')):
        return 'communication_custom_collective'
    if any(s in name for s in ('nccl', 'allreduce', 'all_reduce', 'allgather', 'all_gather', 'reducescatter')):
        return 'communication'
    if any(s in name for s in ('attention', 'flashattn', 'flash_attn', 'flash_fwd', 'flash_bwd', 'fmha')):
        return 'attention'
    if any(s in name for s in ('moe', 'expert')):
        return 'moe_and_expert'
    if any(s in name for s in ('gemm', 'matmul', 'xmma', 'cutlass')):
        return 'gemm'
    return 'other'


def summarize_events(events):
    boundaries = defaultdict(lambda: defaultdict(int))
    counts, duration_sums = defaultdict(int), defaultdict(float)
    kernel_intervals = defaultdict(list)
    devices = set()
    for event in events:
        if event.get('ph') != 'X' or event.get('cat') != 'kernel':
            continue
        start, duration = event.get('ts'), event.get('dur')
        if (not isinstance(start, (int, float)) or not isinstance(duration, (int, float))
                or not math.isfinite(start) or not math.isfinite(duration) or duration < 0):
            raise ValueError('CUDA kernel intervals must have finite timestamps and nonnegative durations')
        if not duration:
            continue
        label = category(event.get('name', ''))
        boundaries[start][label] += 1
        boundaries[start + duration][label] -= 1
        counts[label] += 1
        duration_sums[label] += duration
        kernel_intervals[event.get('name', '')].append((start, start + duration))
        if 'device' in event.get('args', {}):
            devices.add(str(event['args']['device']))
    if not boundaries:
        raise ValueError('Trace has no complete CUDA kernel intervals')
    if len(devices) > 1:
        raise ValueError('A per-rank trace unexpectedly contains more than one CUDA device')
    active, union, exclusive = defaultdict(int), defaultdict(float), defaultdict(float)
    total_union = cross_category_overlap = same_category_overlap = 0.0
    previous = min(boundaries)
    for point, changes in sorted(boundaries.items()):
        width = point - previous
        categories = {key for key, count in active.items() if count > 0}
        if categories:
            total_union += width
            for key in categories:
                union[key] += width
            if len(categories) == 1:
                exclusive[next(iter(categories))] += width
            else:
                cross_category_overlap += width
            if sum(active.values()) > len(categories):
                same_category_overlap += width
        for key, change in changes.items():
            active[key] += change
            if active[key] < 0:
                raise ValueError('Invalid interval event ordering')
        previous = point
    span = max(boundaries) - min(boundaries)
    kernels = []
    for name, intervals in kernel_intervals.items():
        merged = []
        for start, end in sorted(intervals):
            if merged and start <= merged[-1][1]:
                merged[-1][1] = max(merged[-1][1], end)
            else:
                merged.append([start, end])
        kernels.append({'name_sample': name[:512], 'full_name_sha256': hashlib.sha256(name.encode()).hexdigest(),
                        'category': category(name), 'kernel_count': len(intervals),
                        'interval_union_seconds': sum(end - start for start, end in merged) / 1e6,
                        'summed_durations_seconds_not_wall_time': sum(end - start for start, end in intervals) / 1e6})
    kernels.sort(key=lambda row: row['interval_union_seconds'], reverse=True)
    return {'kernel_count': sum(counts.values()), 'observed_cuda_device_ids': sorted(devices),
            'first_kernel_to_last_kernel_span_seconds': span / 1e6,
            'any_kernel_interval_union_seconds': total_union / 1e6,
            'fraction_of_kernel_span_with_any_kernel': total_union / span if span else 0,
            'cross_category_overlap_union_seconds': cross_category_overlap / 1e6,
            'same_category_concurrency_union_seconds': same_category_overlap / 1e6,
            'top_kernels_by_individual_interval_union': kernels[:20],
            'categories': {key: {'kernel_count': counts[key],
                'interval_union_seconds': union[key] / 1e6,
                'exclusive_interval_seconds': exclusive[key] / 1e6,
                'summed_kernel_durations_seconds_not_wall_time': duration_sums[key] / 1e6}
                for key in sorted(counts)}}


def analyze(path, maximum_bytes):
    opener = gzip.open if path.suffix == '.gz' else open
    with opener(path, 'rb') as handle:
        raw = handle.read(maximum_bytes + 1)
    if len(raw) > maximum_bytes:
        raise ValueError('Expanded trace exceeds explicit analysis byte limit')
    data = json.loads(raw)
    result = summarize_events(data['traceEvents'])
    result['uncompressed_trace_sha256'] = hashlib.sha256(raw).hexdigest()
    result['uncompressed_bytes'] = len(raw)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rank-trace', action='append', required=True, help='Verified numeric rank=trace.json[.gz]')
    parser.add_argument('--report', type=Path, required=True)
    parser.add_argument('--max-trace-mib', type=int, default=256)
    args = parser.parse_args()
    if not 1 <= args.max_trace_mib <= 2048:
        parser.error('Trace byte limit must be1–2048 MiB')
    ranks = {}
    for value in args.rank_trace:
        rank, separator, path = value.partition('=')
        if not separator or not rank.isdecimal() or int(rank) in ranks:
            parser.error('Each rank must be a unique nonnegative integer')
        ranks[int(rank)] = analyze(Path(path), args.max_trace_mib * 1024**2)
    result = {'scope': 'Separate untimed CUDA activity traces; not throughput benchmark or SM utilization',
              'ranks': ranks, 'all_four_tp_ranks_present': set(ranks) == {0, 1, 2, 3},
              'limitations': ['Rank mapping must be verified against owned engine PIDs by the caller',
                              'Categories use kernel-name heuristics, not instruction or hardware-counter attribution',
                              'Communication intervals may include waiting or spin kernels',
                              'Custom vLLM cross-device reduce intervals are separated from NCCL by exact observed kernel names',
                              'Category interval unions overlap; exclusive unions plus cross-category overlap partition total union',
                              'Ranks have independent trace windows; their durations are not summed into global elapsed time',
                              'Activity intervals do not establish Tensor Core utilization, FLOPS or memory bandwidth']}
    args.report.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({'report': str(args.report), 'ranks': sorted(ranks), 'all_four_tp_ranks_present': result['all_four_tp_ranks_present']}))


if __name__ == '__main__':
    main()
