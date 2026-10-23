# Validation

Current evidence comes from real PostgreSQL, native services on both existing
GPU Pods, and public-document inference on Qwen2.5-7B. Private SSH ingress is the
owner's selected policy. Short runs below establish observed behavior, not a
24-hour capacity guarantee or a production availability SLA.

## Correctness and deployability

The frozen v3 implementation passed the full local suite of 162 tests, with
one Linux-only case skipped on macOS, before the subsequent outage-probe
additions. It exercises all four combinations of Python/SQL claim and completion
paths over real PostgreSQL: tenant isolation, capacity, admission and fair queue
accounting, cancellation/deadline races, lease expiry, duplicate replay,
transactional usage, terminal output metadata, least-privilege runtime operation,
compatible migration rollouts and two TCP API replicas surviving an owned API
process crash. New device-enumeration and backup regressions are included in the
final check command, `python devtools/run_checks.py`.

The persisted console action regressions passed 3/3 using Node v24.21.0. They
cover cancellation/completion races and a committed retry whose acknowledgement
is lost, including recovery after tab reload. Backup helper checks passed 5/5:
version mismatch rejection, private password delivery, atomic archive visibility,
SHA validation and failure cleanup. Wheel construction succeeded and its archive
contains `schema.sql` and the additive migration SQL. Container builds have not
been exercised; the native deployment is the verified path.

The local physical replication probe also verified synchronous acknowledgement,
64 surviving committed rows after primary failure, standby read-only behavior,
and manual promotion after fencing the stopped primary. It uses one machine and
is separate from the deployed managed Neon topology; it is not cross-cloud or
multi-region HA evidence.

## Managed state and independent control planes

The [Neon cutover](../ops/reports/neon-cutover-2026-10-24.json) drained all source
writers, copied 2,603 tasks across nine application tables into an empty target,
and verified ordered row hashes/counts before committing. The old local source
is fenced read-only. Both Pods now use direct, verify-full PostgreSQL 17 TLS
connections with a restricted runtime login. The operator laptop's old worker
broker was closed; workers call their own Pod's loopback control plane.

Each Pod runs two stateless APIs behind its own gateway and an app-local process
watchdog. A gateway does not replay uncertain mutation requests. Worker failover
selects a different origin for the next explicit request and retains the same
attempt token for uncertain completion acknowledgement. Process supervision is
not Pod replacement or independent client ingress. The Neon free plan has no
production SLA; no database failure-domain or zero-RTO guarantee is inferred
from four API processes.

The owner-approved restricted machine SSH link proved direct Pod-to-Pod operation
before the second Pod obtained independent APIs. It is no longer required in the
inference path. No existing unrelated GPU process or Pod configuration was reset.

## Real 7B configuration

- Model: `Qwen/Qwen2.5-7B-Instruct`, revision
  `a09a35458c702b33eeacc393d103063234e8bc28`, 7,615,616,512 parameters.
- BF16, vLLM 0.10.2, Torch 2.8.0/CUDA 12.8, Transformers 4.55.2,
  Tokenizers 0.21.4, Hugging Face Hub 0.34.4.
- Independent TP1 engines, eager execution, 4,096-token context, eight sequences
  per engine and 4,096 maximum batched tokens. Prefix caching explicitly disabled.
- Primary: four shared H100 80 GB devices, application memory target 0.24 each.
  Existing other workloads remained running; this is not an isolated full-H100
  capacity measurement.
- Second: 24 GB RTX PRO 6000 Blackwell MIG slices, application memory target 0.82.
  Native CUDA exposes sixteen unique MIG UUIDs. Torch's default NVML enumeration
  returned six physical-device entries; using only that count initially omitted
  ten accessible slices. Explicit UUID selection and per-slice real allocation
  checks correct the discovery path. A MIG slice is not a full physical GPU.

See [primary model evidence](../ops/reports/primary-qwen7b-upgrade-2026-10-24.json),
[second model load](../ops/reports/pod2-7b-engine-load-2026-10-24.json), and
[device mapping](../ops/reports/pod2-device-visibility-2026-10-24.json).
Every engine must pass real generation before its worker is started. Cold first
requests on the second Pod took much longer than warm requests; readiness at
`/v1/models` alone does not establish warmed generation latency.

## Fixed-input public-document comparison

The source is SQuAD 1.1 development (CC BY-SA 4.0), dataset SHA256
`95aa6a52d5d6a735563366753ca50492a658031da74f301ac5238b03966972c9`.
The deterministic 2,048-example analysis input SHA256 is
`8f81023c1ee309f945c1f5f2b0c15cbe2f17e7e6306a6d8b761f77396ca3f6e2`.
Messages remain identical between runs; the unique run ID only prefixes custom
IDs. Real passages are packed to a target of 700 words; output cap is 512 tokens.
Actual average input was 1,156.96 tokens. This public data may occur in training
and is not customer traffic or a contamination-free model evaluation.

| Configuration | Real tasks | Observed seconds | Completed/s | Mean output tokens | Failed tasks |
| --- | ---: | ---: | ---: | ---: | ---: |
| Python completion, 4 H100 + 6 MIG | 2,048 | 746.013 | 2.745 | 191.50 | 0 |
| SQL v2 completion, same 10 engines | 2,048 | 233.624 | 8.766 | 191.58 | 0 |

This observation is a 3.19× end-to-end increase. Both runs used identical total
input tokens (2,369,454); output totals were 392,194 and 392,357. All ten workers
accepted real results. Downloaded output IDs, durable task and batch counts,
accepted attempts and token totals matched, with no repeated attempts. Each run
has exact start/end times and separately timed output verification. Submission
acceptance is not counted as completed inference.

The worker multi-origin patch was deployed alongside SQL v2. The before/after
worker snapshots disclose selected origin and failover counters; do not describe
this as a single-code-change laboratory experiment. Shared GPU load also remains
a confounder. The separate synthetic state-machine comparison is explicitly not
an HTTP or GPU benchmark.

Reports: [baseline](../ops/reports/squad7b-analysis-2048-baseline-2026-10-24.json),
[baseline attribution](../ops/reports/squad7b-analysis-2048-baseline-attribution-2026-10-24.json),
[SQL v2](../ops/reports/squad7b-analysis-2048-sql10-2026-10-24.json),
[SQL v2 attribution](../ops/reports/squad7b-analysis-2048-sql10-attribution-2026-10-24.json).

The primary's completion handling mean fell from about 1.50 seconds to 0.28
seconds; p95 fell from histogram bucket (5,7.5] to (.75,1] seconds. These are API
handling times, not GPU latency. Exact sample windows are recorded. Histogram
percentiles are bucket bounds, not invented point estimates. Database lock wait
samples and low GPU occupancy supported the diagnosis of remote round trips
inside serialized critical sections. Claiming still held the tenant queue lock
across many network calls and became the next optimization target.

## Output quality is separate

The baseline produced 38 invalid JSON envelopes and one length-truncated output.
Its field-type contract success was 98.14%, answer exact match 65.77% and answer
token F1 80.10%, counting invalid and missing responses as zero. These are this
custom packed-context task's scores, not official SQuAD scores. SQL v2 produced
41 invalid envelopes, one truncated output, exact match 65.58% and F1 80.01%.
Different GPU kernels/batching can produce slightly different responses despite
the fixed seed and temperature.

The requested 180–220-word analysis was usually not followed: valid baseline
responses had a 91-word median, and only 1/2,048 met the requested length. SQL v2
had a 92-word median and zero in that range. Therefore neither row represents
512 generated tokens/task or uniformly valid useful output. There is no
independent grading of evidence grounding or explanation quality.

The answer and translation profiles must record their own actual token lengths,
truncation and JSON rates. A JSON translation envelope is not a translation
fidelity score. See [baseline quality](../ops/reports/squad7b-analysis-2048-baseline-quality-2026-10-24.json).

## Backup recovery

The current PostgreSQL 17.11 dump was restored into a newly created empty Neon
database using the version-matched official client. Restoration verified 118
batches and 4,651 completed tasks/attempts, 2,543,062 input and 428,722 output
tokens, all batch/task status and usage invariants, and table/usage hashes. The
restore database was removed; production was never a restore target. The source
was not modified. See [restore proof](../ops/reports/managed-backup-restore-2026-10-24.json)
and [backup procedure](backup.md). A local dump and provider snapshot are not an
automatically retained disaster-recovery policy.

## Historical small-model release

The earlier Qwen2.5-0.5B release measured 78.01 tasks/second for 1,024 short
classification tasks with only 14 output tokens each. Its two-Pod run used one
MIG worker through the former operator relay. Those measurements are retained
for history and cannot be compared as 7B or long-output capacity:
[initial refill run](../ops/reports/gpu-refill-2026-10-23.json),
[two-Pod run](../ops/reports/gpu-multipod-2026-10-23.json).

The earlier isolated HTTP chaos test used marked synthetic responses, killed its
own worker and dropped a committed claim acknowledgement. It recovered four
expired attempts and produced ten unique outputs; stale completion returned
409. It is protocol evidence, never GPU throughput:
[HTTP chaos](../ops/reports/http-chaos-2026-10-23.json).

## Remaining limits

GitHub Actions is configured, but the hosted run could not start because GitHub
reported account payment/spending restrictions. Local verification is separate:
[blocked run](https://github.com/yinli-systems/batchforge/actions/runs/40013353844).
Payments/invoicing, independent public ingress, scheduled off-host retention,
whole-Pod replacement, database failure-domain recovery and a sustained
10-million-task day have not been established. See [scaling analysis](scaling.md)
for the measured gap and requirements.
