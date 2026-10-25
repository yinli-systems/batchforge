# CUDA graph A/B on Qwen3-235B-A22B-Instruct-2507-FP8 (TP4, 4x H100)

Date: 2026-10-25. Engine: vLLM 0.10.2, revision `e156cb4e...`, TP4, BF16 KV,
max-model-len 18440, max-num-seqs 64, gpu-memory-utilization 0.90, prefix caching off.

## Workload (fixed, same for every run)

`vllm bench serve --dataset-name random --random-input-len 4096 --random-output-len 512
--ignore-eos --num-prompts 256 --max-concurrency 64`. Saturating closed-loop
throughput test. It is not an open-loop or production arrival test.

## Configurations

| Label | Setting |
|---|---|
| eager | `--enforce-eager` (the production configuration before this work) |
| graph-b4096 | `ENGINE_ENFORCE_EAGER=0`, `FULL_DECODE_ONLY`, capture sizes 1..64, prefill budget 4096 |
| graph-b8192 | as above, `max-num-batched-tokens 8192` (one run) |
| graph-b2048 | as above, `max-num-batched-tokens 2048` |

## Results

Full per-run table: [summary.csv](summary.csv). Medians over three runs:

| Config | Output tok/s | Median TTFT | P99 TTFT | Median TPOT | E2E P95 |
|---|---|---|---|---|---|
| eager (A, A2, detail; 9 runs) | ~642 | 594 ms | 12.1 s | ~96 ms | 60.4 s |
| graph-b4096 (3 runs) | 1,225.7 | 791 ms | 12.1 s | 50.2 ms | not measured |
| graph-b2048 (3 runs) | 1,191.4 | 589 ms | 14.2 s | 51.2 ms | 38.4 s |
| graph-b8192 (1 run) | 1,249.0 | 1,078 ms | 11.2 s | 48.8 ms | 34.4 s |

E2E figures come from per-request `ttfts` and `itls`: E2E = TTFT + sum of inter-token
latencies. Only runs saved with `--save-detailed` have them.

## Quality

Labelled spot-check (sentiment, summary, extraction; 130 requests per concurrency):
identical on eager and graph-b4096 (sentiment 0.90, summary 1.00, extraction 1.00).
Only graph-b4096 was checked, not graph-b2048 or b8192.

## Protocol

A/B/A: eager (A) → graph-b4096 (B) → eager (A2). Acceptance rule from
`docs/next-performance-experiments.md`: at least 5% median whole-cohort gain, no P95
regression beyond 10%, no quality regression.

- Throughput gate: passed (about +89% output tok/s, A and A2 agree within 1.6%).
- Quality gate: passed on the small spot-check for graph-b4096.
- P95 gate: not evaluated for graph-b4096, because no detailed run was saved.
  For graph-b2048 it is well inside the limit (38.4 s vs 60.4 s).
- The TTFT tail (P99 about 14.2 s, up about 17%) is a regression that the protocol does not gate directly.

## Caveats

- Single engine, single fixed fixture. Not an open-loop or rolling-arrival test.
- The graph-b8192 result is a single run.
- The vLLM log did not show an explicit "graph capturing finished" line. The settings
  were confirmed in the live process arguments and by the throughput change, not from capture logs.
- KV cache reported 314,096 tokens with graphs; not compared against eager in this report.

## Scripts

- [run_candidate.sh](run_candidate.sh): applies one graph variant via the process manager, waits for readiness, runs one detailed benchmark.
- [eager_detail.sh](eager_detail.sh): restores the eager configuration and runs three detailed benchmarks.

Raw per-request JSON, server logs and engine environment files are not in this repository.
