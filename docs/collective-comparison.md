# Qwen235B collective comparison and restoration

The four-H100 Qwen235B deployment retained vLLM's custom collective after a
controlled comparison on October 24, 2026. The NCCL fallback completed the same
100 tasks, but took 4.90% longer and increased completion P95 by 5.77% in this
single comparison. The original adaptive64 configuration was restored and
verified at **03:30:21 UTC**. This is evidence for the selected deployment and
workload, not a universal ranking of collective implementations.

## Controlled workload

Both paths used the official `Qwen/Qwen3-235B-A22B-Instruct-2507-FP8`, revision
`e156cb4efae43fbee1a1ab073f946a1377e6b969`: 235B total and 22B active parameters,
TP4, BF16 compute, FP8 block weights, context18,440, max64 engine sequences,
max4,096 batched tokens, GPU memory fraction0.90, eager execution, chunked
prefill enabled and prefix caching disabled. The numerical change was the
installed vLLM0.10.2 `--disable-custom-all-reduce` switch. Symmetric-memory
all-reduce stayed off. Actual leader argv matched the owned process record.

Each cohort repeated the same public 20-cell fixture five times: inputs
128/512/2,048/8,192/16,384 tokens crossed with outputs32/128/512/2,048. The fixture
SHA256 was `52f84aec744faa37b8d6d2166693536f9f8d8778642b29a58b4e6aa08ac2fb4f`.
Each run completed100/100 with one accepted attempt per task, exactly545,280
prompt tokens and68,000 generated tokens, exact output caps, and100 final text
rows. These forced-length outputs establish capacity and output shape, not
answer quality. The archive publisher remained off for both timed cohorts.

| Metric | Custom collective | NCCL fallback | NCCL change |
| --- | ---: | ---: | ---: |
| Cohort duration, seconds | 244.281180 | 256.250889 | +4.90% |
| Completion P50, seconds | 54.408186 | 55.736429 | +2.44% |
| Completion P95, seconds | 220.534685 | 233.261589 | +5.77% |
| Completion P99, seconds | 240.953150 | 252.006948 | +4.59% |
| First queue P50, seconds | 1.771529 | 1.630025 | −7.99% |
| Whole-Pod allocation scenario, USD | 1.092480 | 1.146011 | +4.90% |

Queue P50 improved under NCCL; the result was not worse in every metric.
Allocation cost uses the explicit $16.10/hour scenario, not an invoice or
marginal inference cost. See the [full comparison and exact phase histograms](../ops/reports/primary-qwen235b-nccl-adaptive64-platform-analysis-2026-10-24.json)
and [custom baseline](../ops/reports/primary-qwen235b-adaptive64-platform-analysis-2026-10-24.json).

Both workers retained64 claim slots and the same **298,384-token** block-rounded
admission budget. Fresh physical cache was314,096 tokens for custom and314,544
for NCCL; the additional NCCL capacity was not admitted.64 claimed tasks include
tokenization, reservation waiters, inference and reporting and do not mean64
full-context GPU requests. The NCCL observation sampled at most51 engine-running
requests and297,568 logical reserved tokens. These are sampled workload maxima,
not promises about continuously observed concurrency or physical allocation.

The NCCL profile received a distinct runtime fingerprint. Prefill/decode rates
and slot price were retained from the earlier static16 calibration and explicitly
marked not recalibrated; changing64 claim slots did not divide the price by64.
There was one serial run per path, an engine restart between runs, and one
physical GPU type. No statistical significance, heterogeneous-hardware savings,
sustained daily capacity, or model-only SM/FLOP attribution is established.

## Trace and topology interpretation

The earlier separate four-request CUDA capture contained all four verified TP
rank traces plus one CPU-only API leader trace. The original strict-mapper
failure was preserved; offline continuation analyzed the same files without
reissuing requests. The [second offline analysis](../ops/reports/primary-qwen235b-trace-offline-analysis-v2-2026-10-24.json)
classified the observed `vllm::cross_device_reduce_1stage` kernels separately
from NCCL. Their per-rank wall-time unions varied substantially; such intervals
can include waiting or spin and do not prove link-bandwidth saturation.

After NCCL100 became terminal and a fresh query observed zero active tasks,
[read-only topology and link status](../ops/reports/primary-qwen235b-gpu-topology-2026-10-24.json)
showed all GPU0–3 pairs as `NV18`. Each card reported18 link entries at
26.562GB/s; no inactive entry appeared. GPUs0/1 had NUMA affinity0 and GPUs2/3
affinity1. These are topology/link-state observations, not measured transfer
bandwidth or latency. Combined with the slower NCCL experiment, the evidence
does not support declaring the custom collective itself a proven network
bottleneck.

## Safe activation and executable restoration

Only the reviewed process-manager flag support and two new controllers were
deployed. The original manager source was preserved privately, its SHA checked,
and only scoped manager bytecode was evicted. The [source-stage report](../ops/reports/primary-qwen235b-collective-source-stage-2026-10-24.json)
proves that service identities, environments and manifest were unchanged during
that source-only step. No backend or API helper was synchronized with it.

`ops/ha/collective_qwen235b.py` and
`ops/ha/restore_collective_qwen235b.py` default to preparation. Their snapshots
bind exact manifest/environment bytes, owned PID/start/cwd identities,
manager/runtime/worker source hashes and the numerical profile. Activation and
restoration use separate unique durable job identities. The restoration also
requires the exact completed activation-report SHA; it never rewrites that
historical report.35 focused regressions cover successful transitions, stale
report/source/PID rejection and incomplete-recovery failures.

After drain, each operation pauses the owned watchdog and worker, stops the
owned engine, and starts with an engine-only manifest. A fresh cache log must
support the unchanged budget below95% of physical capacity. Actual collective
argv, tokenizer protocol, engine quiescence, worker gauges and registered profile
must all pass before the worker and watchdog resume. Failed recovery excludes
the worker and reports manual recovery; a foreign manifest or identity prevents
blind mutation and watchdog restart.

The [NCCL activation](../ops/reports/primary-qwen235b-nccl-adaptive64-activation-2026-10-24.json)
completed at03:12:54 UTC. The separate restoration was first
[prepared against the real candidate](../ops/reports/primary-qwen235b-custom-restore-prepared-2026-10-24.json),
then explicitly applied after the comparison. The [completed restoration](../ops/reports/primary-qwen235b-custom-restored-2026-10-24.json)
records fresh physical cache314,096, the original298,384 budget and original
`primary-h100-tp4-qwen235b-fp8-adaptive64` profile with fingerprint
`sha256:62fdcd18e5c2fb3a3c9cb1be1e2a8ed1bda02f728ff6f705189e0eed8bf8dca3`.
At completion, engine188251, worker190897 and watchdog190984 were owned and
ready; API169861/169870 and gateway162130 retained their original identities and
environment bytes. These identities describe that restoration event, not a
claim that future maintenance will preserve them indefinitely.
