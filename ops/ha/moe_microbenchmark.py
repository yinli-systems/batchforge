"""Bounded Qwen235B FP8 MoE search; produces evidence, never installs a config.

Requires an owned stopped engine/worker and a hash-pinned upstream benchmark.
Synthetic kernel timing is separate from actual end-to-end inference throughput.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import statistics
import time

from ops.manage import owned_record
from ops.supervise import private_write

UPSTREAM_SHA = 'a435cb2aef625eb321466cf853df83c3523dd49bdbbd5871ffcb75d0f7f39dc7'
UPSTREAM_URL = 'https://raw.githubusercontent.com/vllm-project/vllm/v0.10.2/benchmarks/kernels/benchmark_moe.py'


def candidates(default):
    """Vary row tiling only; retain the checkpoint's 128x128 FP8 block shape."""
    result = [dict(default)]
    for rows in (16, 32):
        for groups in (1, 32):
            for stages in (2, 3):
                item = dict(default, BLOCK_SIZE_M=rows, GROUP_SIZE_M=groups, num_stages=stages)
                if item not in result:
                    result.append(item)
    return result


def verify_model(config):
    expected = {'hidden_size': 4096, 'moe_intermediate_size': 1536,
                'num_experts': 128, 'num_experts_per_tok': 8}
    if any(config.get(k) != v for k, v in expected.items()):
        raise ValueError('Expected exact Qwen235B TP4 expert dimensions')
    quant = config.get('quantization_config', {})
    if quant.get('quant_method') != 'fp8' or quant.get('weight_block_size') != [128, 128]:
        raise ValueError('Expected checkpoint FP8 128x128 weight blocks')
    return expected


def finite_samples(values):
    if not values or any(not math.isfinite(v) or v <= 0 for v in values):
        raise ValueError('Kernel times must be finite and positive')
    return statistics.median(values)


def run(args):
    root = args.root.resolve(strict=True)
    if args.report.exists():
        raise RuntimeError('Existing stable report must be inspected, not overwritten')
    if any(owned_record(root / ('.runtime/' + role + '-qwen235b.json')) for role in ('engine', 'worker')):
        raise RuntimeError('Owned Qwen engine and worker must be stopped first')
    if owned_record(root / '.runtime/supervisor-primary.json'):
        raise RuntimeError('Pause the owned watchdog before the stopped-engine experiment')
    raw = args.upstream.read_bytes()
    if hashlib.sha256(raw).hexdigest() != UPSTREAM_SHA:
        raise ValueError('Upstream pinned benchmark source checksum differs')
    config_path = root.parent / 'models/qwen3-235b-fp8/config.json'
    verify_model(json.loads(config_path.read_text()))
    import importlib.metadata
    if importlib.metadata.version('vllm') != '0.10.2':
        raise ValueError('This experiment requires the inspected vLLM0.10.2')
    import torch
    torch.set_default_device('cuda')
    torch.set_default_dtype(torch.bfloat16)
    if torch.cuda.device_count() != 1 or 'H100' not in torch.cuda.get_device_name(0):
        raise ValueError('Use one explicitly selected H100 per child process')
    free_bytes, total_bytes = torch.cuda.mem_get_info()
    if free_bytes < total_bytes * .9:
        raise RuntimeError('Insufficient fresh GPU memory headroom')
    spec = importlib.util.spec_from_file_location('pinned_vllm_moe_bench', args.upstream)
    upstream = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(upstream)
    from vllm.model_executor.layers.fused_moe import override_config
    from vllm.model_executor.layers.fused_moe.fused_moe import fused_moe, get_default_config

    report = {'passed': False, 'upstream_url': UPSTREAM_URL, 'upstream_sha256': UPSTREAM_SHA,
              'controller_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              'gpu': torch.cuda.get_device_name(0), 'cuda_visible_devices': os.environ.get('CUDA_VISIBLE_DEVICES'),
              'fresh_free_gpu_bytes': free_bytes, 'total_gpu_bytes': total_bytes,
              'model_config_sha256': hashlib.sha256(config_path.read_bytes()).hexdigest(),
              'shape': {'experts': 128, 'shard_intermediate': 384, 'hidden': 4096, 'topk': 8, 'block_shape': [128, 128]},
              'results': [], 'installs_any_config': False,
              'limitations': ['Synthetic per-layer MoE timing, not model throughput or semantic quality.',
                  'Candidates must produce bit-identical BF16 results to the default on the same seeded synthetic inputs.',
                  'The upstream timer uses CUDA graph replay; the current full engine uses eager execution.',
                  'The cooperative time bound is checked between calls; the outer runner must enforce a hard child-process timeout.',
                  'Row-tiling candidates only; this is a bounded search, not a globally optimal kernel claim.']}
    args.report.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    private_write(args.report, report)
    started = time.monotonic()
    try:
        for batch in args.batches:
            if time.monotonic() - started > args.max_seconds:
                raise TimeoutError('Bounded experiment time exhausted')
            torch.manual_seed(943 + batch)
            x = torch.randn(batch, 4096, dtype=torch.bfloat16) * .1
            w1 = torch.randn(128, 768, 4096, dtype=torch.float16).to(torch.float8_e4m3fn)
            w2 = torch.randn(128, 4096, 384, dtype=torch.float16).to(torch.float8_e4m3fn)
            s1 = torch.rand(128, 6, 32, dtype=torch.float32) * .01
            s2 = torch.rand(128, 32, 3, dtype=torch.float32) * .01
            gating = torch.randn(batch, 128, dtype=torch.float32)
            default = get_default_config(batch, 128, 384, 4096, 8, 'fp8_w8a8', [128, 128])
            def forward(cfg):
                with override_config(cfg):
                    return fused_moe(x.clone(), w1, w2, gating, 8, renormalize=True,
                        inplace=False, use_fp8_w8a8=True, w1_scale=s1, w2_scale=s2, block_shape=[128, 128])
            reference = forward(default)
            torch.cuda.synchronize()
            if not torch.isfinite(reference).all().item() or not reference.count_nonzero().item():
                raise RuntimeError('Reference output must be finite and nonzero')
            entry = {'batch_tokens': batch, 'default_config': default, 'candidates': []}
            report['results'].append(entry)
            for candidate in candidates(default):
                if time.monotonic() - started > args.max_seconds:
                    raise TimeoutError('Bounded experiment time exhausted')
                record = {'config': candidate, 'bit_identical_to_default': False}
                entry['candidates'].append(record)
                try:
                    actual = forward(candidate)
                    torch.cuda.synchronize()
                    record['bit_identical_to_default'] = torch.equal(reference, actual)
                    record['max_abs_difference'] = float((reference.float()-actual.float()).abs().max().item())
                    if not record['bit_identical_to_default']:
                        continue
                    samples = []
                    for trial in range(3):
                        torch.manual_seed(1103 + batch)
                        samples.append(upstream.benchmark_config(candidate, batch, 128, 768, 4096, 8,
                            torch.bfloat16, True, False, num_iters=10, block_quant_shape=[128, 128]))
                    record.update(samples_microseconds=samples, median_microseconds=finite_samples(samples))
                except Exception as error:
                    record['error_type'] = type(error).__name__
                finally:
                    private_write(args.report, report)
            valid = [r for r in entry['candidates'] if 'median_microseconds' in r]
            if not valid or valid[0]['config'] != default:
                raise RuntimeError('No validated default timing')
            winner = min(valid, key=lambda r: r['median_microseconds'])
            entry.update(selected_config=winner['config'], selected_microseconds=winner['median_microseconds'],
                default_microseconds=valid[0]['median_microseconds'],
                default_over_selected_ratio=valid[0]['median_microseconds']/winner['median_microseconds'])
            del reference, actual, x, w1, w2, s1, s2, gating
            torch.cuda.empty_cache()
        report['passed'] = True
    except BaseException as error:
        report['error_type'] = type(error).__name__
        raise
    finally:
        report['elapsed_seconds'] = time.monotonic()-started
        private_write(args.report, report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--root', type=Path, default=Path('/root/batchforge-runtime/app'))
    parser.add_argument('--upstream', type=Path, required=True)
    parser.add_argument('--report', type=Path, required=True)
    parser.add_argument('--batches', type=int, nargs='+', required=True)
    parser.add_argument('--max-seconds', type=int, default=300)
    args = parser.parse_args()
    if (len(set(args.batches)) != len(args.batches) or any(b not in (1,2,4,8,16,32,64) for b in args.batches)
            or not 60 <= args.max_seconds <= 600):
        parser.error('Bounded unique decode sizes and a60–600s limit required')
    if not args.execute:
        print(json.dumps({'execute': False, 'upstream_sha256': UPSTREAM_SHA, 'batch_tokens': args.batches,
                          'candidate_count_per_batch': 9, 'installs_any_config': False}))
        return
    result = run(args)
    print(json.dumps({'passed': result['passed'], 'report': str(args.report)}))


if __name__ == '__main__':
    main()
