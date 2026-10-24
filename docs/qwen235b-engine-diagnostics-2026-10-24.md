# Qwen235B direct-engine diagnostics — October 24, 2026

The Neon quota outage prevents new durable-platform experiments. These separate
finite tests exercise the already-running engine through Pod-local HTTP while
database qualification proceeds. They bypass API admission, scheduler, leases,
durable acceptance and the worker's adaptive KV gate. No platform throughput,
heterogeneous-fleet savings, daily capacity or whole-Pod HA is established here.

## Cold-first 20-cell burst

[The actual report](../ops/reports/qwen235b-engine20-cold-burst-2026-10-24.json)
and [all 162 GPU/engine metadata samples](../ops/reports/qwen235b-engine20-cold-burst-gpu-samples-2026-10-24.jsonl)
preserve the measurement. No unrecorded warmup preceded it: engine counters
were zero after the known eager recovery. Exact process generations, serving
sources, model profile and actual eager execution were unchanged through the
test. Public fixture prompts and raw generated responses remain private on the
Pod.

The checkpoint has **235B total / 22B active MoE parameters**, revision
`e156cb4efae43fbee1a1ab073f946a1377e6b969`. Four H100 80GB devices use TP4,
FP8 weights, BF16 KV, eager execution, an 18,440-token context limit,
4,096 batched tokens, at most 64 sequences, chunked prefill and no prefix cache.
Twenty cells cover the Cartesian product of inputs 128/512/2,048/8,192/16,384
and outputs 32/128/512/2,048, at HTTP concurrency 16. Fixed min/max output
length and ignored EOS measure capacity, not response quality.

| Metric | Actual result |
| --- | ---: |
| Completed requests / errors | 20 / 0 |
| Engine counter deltas | 20 successes, 109,056 input tokens, 13,600 output tokens |
| Burst wall seconds | 162.421265 |
| Generated tokens/s | 83.732878 |
| Client completion P50 / P95 / P99 seconds | 29.432 / 161.724 / 162.282 |
| First visible text P50 / P95 / P99 seconds | 1.984 / 4.075 / 4.076 |
| GPU 0/1/2/3 sample-mean utilization | 93.284% / 86.685% / 36.364% / 74.975% |

Each device reached 100% in at least one sample, but GPU2's mean was only
36.364%. This is evidence of uneven sampled utilization, not proof that all
four GPUs are saturated. NVML utilization is not SM occupancy or achieved FLOPs;
the cause needs a separate controlled investigation. Sampling targeted one
second; timestamps retain actual intervals and missing intervals are not
filled with zeros. The 20 observations provide weak evidence about P99.

First visible text includes network and engine queueing. SSE chunks are not
tokens. No per-token latency, GPU-kernel TTFT or quality claim follows. The
whole-Pod $16.10/hour allocation is an operator scenario, not an invoice:
this window allocates $0.726384, or $53.4106 per million generated tokens for
this small cold burst. It is not directly comparable to a different warmed
workload or an accepted-task cost.

## Warm mixed burst and separate regime plans

The next separately prepared diagnostic contains 80 requests: 16 repeats of
128/32, 2,048/128, 8,192/512, 16,384/2,048 and 128/2,048, at concurrency 16.
Its exact planned work is 430,080 input / 76,288 output tokens. It starts from
the recorded cumulative counters left by the 20-cell test; it is not a cold
run or an equal-work comparison with that test. A 1,200-second global deadline,
private bounded SSE capture, per-request durable submission intents, no HTTP
retries and exact counter-delta checks protect interpretation of uncertain
requests. Final measurements must be added only after the actual run completes.

Decode-heavy 64×128/2,048 at concurrency 64 and prefill-heavy
16×16,384/32 at concurrency 16 are separately preregistered exploratory
regimes. They require independent preparation/execution receipts and do not
automatically run after the mixed burst. Their different work and concurrency
must not be presented as a matched scheduler optimization.
