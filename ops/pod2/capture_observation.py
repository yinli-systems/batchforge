#!/usr/bin/env python3
"""Freeze fully contained samples from app-owned secondary observer streams.

No credentials, database connections, GPU allocations, or service operations.
Multiple existing streams support a benchmark spanning a no-overlap handoff.
"""
from __future__ import annotations

import argparse
import datetime
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[2]


def instant(value):
    parsed = datetime.datetime.fromisoformat(value)
    if parsed.utcoffset() is None:
        raise ValueError('Observation bounds must have an explicit timezone')
    return parsed


def contained(samples, start, end):
    if end <= start:
        raise ValueError('Observation end must follow start')
    return sorted((sample for sample in samples
                   if instant(sample['started_at']) >= start and instant(sample['finished_at']) <= end),
                  key=lambda sample: instant(sample['started_at']))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--start', required=True)
    parser.add_argument('--end', required=True)
    parser.add_argument('--label', required=True, help='New immutable slice/report label')
    parser.add_argument('--stream', action='append', required=True, help='Existing basename under .runtime/observability')
    args = parser.parse_args()
    if len(set(args.stream)) != len(args.stream):
        parser.error('Repeated source streams would double-count observations')
    start, end = instant(args.start), instant(args.end)
    if not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,79}', args.label):
        parser.error('Use a bounded lowercase report label')
    samples, sources = [], []
    for name in args.stream:
        if not re.fullmatch(r'controlplane-secondary-[a-z0-9-]+\.jsonl', name):
            parser.error('Use an existing secondary observer stream basename')
        path = ROOT / '.runtime/observability' / name
        if path.is_symlink() or not path.is_file():
            raise RuntimeError('Observer source must be an existing regular app file')
        data = path.read_bytes()
        sources.append({'path': str(path.relative_to(ROOT)), 'bytes_at_capture': len(data),
                        'sha256_at_capture': hashlib.sha256(data).hexdigest()})
        for line in data.splitlines():
            try:
                samples.append(json.loads(line))
            except json.JSONDecodeError:
                # A currently active stream can end with one incomplete line.
                if line != data.splitlines()[-1]:
                    raise
    selected = contained(samples, start, end)
    if not selected:
        raise RuntimeError('No fully contained observation samples')
    raw = ''.join(json.dumps(sample, separators=(',', ':')) + '\n' for sample in selected).encode()
    target = ROOT / f'.runtime/observability/controlplane-secondary-{args.label}.jsonl'
    if target.exists():
        if target.is_symlink() or target.read_bytes() != raw:
            raise RuntimeError('Immutable observation slice already differs')
    else:
        fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, 'wb') as handle:
            handle.write(raw)
    spec = importlib.util.spec_from_file_location('bf_observer', ROOT / 'scripts/observe_runtime.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    report = module.summarize(selected)
    partial = min(instant(row['started_at']) for row in samples) > start or max(instant(row['finished_at']) for row in samples) < end
    report.update(requested_window_start=start.isoformat(), requested_window_end=end.isoformat(),
                  window_rule='sample.started_at >= window_start AND sample.finished_at <= window_end',
                  partial_coverage=partial, source_streams=sources, original_stream_preserved=True,
                  coverage_limitation='Only fully-contained samples are retained; first scrape and endpoint slivers are excluded from metric deltas',
                  frozen_raw_path=str(target.relative_to(ROOT)), frozen_raw_bytes=len(raw),
                  frozen_raw_sha256=hashlib.sha256(raw).hexdigest())
    report['maximum_between_sample_gap_seconds'] = max(
        (instant(right['started_at']) - instant(left['finished_at'])).total_seconds()
        for left, right in zip(selected, selected[1:])) if len(selected) > 1 else None
    destination = ROOT / f'.runtime/pod2-{args.label}-secondary-observation.json'
    descriptor = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, 'w') as handle:
        json.dump(report, handle, indent=2)
        handle.write('\n')
    print(json.dumps({key: report[key] for key in ('sample_count', 'first_sample_at', 'last_sample_at',
                                                  'partial_coverage', 'frozen_raw_sha256', 'frozen_raw_bytes')}), flush=True)


if __name__ == '__main__':
    main()
