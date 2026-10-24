# Sustained and workload-regime results — October 24, 2026

The schema10 mixed-workload baseline completed **600 of 720 planned tasks** on
one four-H100 80GB Pod running
`Qwen/Qwen3-235B-A22B-Instruct-2507-FP8` with TP4. All 600 accepted tasks
completed once, but the full admission plan did not finish before its time
limit: the report correctly records `passed=false`. The interrupted 64KiB
response-reservation pilot is separate evidence, not a matched performance
comparison. This document reports completed measurements only.

## Original schema10 sustained baseline

The [original report](../ops/reports/qwen235b-soak30m-baseline-report-2026-10-24.json)
and [controller record](../ops/reports/qwen235b-soak30m-baseline-controller-2026-10-24.json)
describe a fixed-output mixed workload with inputs spanning 128–16,384 tokens
and outputs of 32/128/512/2,048 tokens. The configuration requested 720 tasks,
20 per batch, at an offered 0.4 tasks/s, with a 1,800-second admission window,
an 80-task inflight limit and two concurrent submitters. Actual wall time,
including drain and validation, was **1,936.918705 seconds**.

| Observed result | Value |
| --- | ---: |
| Accepted batches / tasks | 30 / 600 |
| Completed / failed / cancelled / unfinished accepted tasks | 600 / 0 / 0 / 0 |
| Planned tasks not admitted before the cutoff | 120 |
| Accepted attempts / duplicate accepted attempts / tasks retried | 600 / 0 / 0 |
| Actual prompt / generated tokens | 3,271,680 / 408,000 |
| Completed tasks/s over the reported wall time | 0.309770 |
| Create HTTP 429 responses | 281 |
| Upload 429 / uncertain create responses | 0 / 0 |

The 281 responses count rejected **create attempts**, not 281 distinct tasks
or inference failures. Accepted usage matched durable task usage, and every
accepted output had the requested fixed length. The
[output collection](../ops/reports/qwen235b-soak30m-baseline-output-collection-2026-10-24.json)
preserves the partial-plan status; it does not relabel the workload as passed.
The [runtime proof](../ops/reports/qwen235b-soak30m-baseline-runtime-proof-2026-10-24.json)
binds the original report to its engine/worker configuration and service source.

### Arrival latency includes admission backpressure

The report keeps two clocks separate. Database completion latency begins at
`batch.created_at`; scheduled-arrival latency begins at the persisted job start
plus that cohort's deterministic release offset. The latter includes upload,
429 backoff and local submission delay before durable admission.

| Completed-task latency, seconds | P50 | P95 | P99 |
| --- | ---: | ---: | ---: |
| Durable batch creation → accepted task finish | 35.521168 | 183.904873 | 190.192664 |
| Scheduled arrival → accepted task finish | 219.938646 | 501.024344 | 643.408575 |

Both rows use the 600 completed tasks and linear interpolation at `(n−1)×p`.
The 120 unadmitted tasks have no completion latency and are excluded, not
assigned zero latency. A separate 30-batch scheduled-arrival-to-durable-admission
population has P95/P99 **361.013350/523.058945 seconds**. API HTTP-duration
histogram buckets are another measurement; they are not task inference latency.

### Reservation pressure and allocated cost

The original response allowance was 1MiB per newly admitted task. The first
quota sample recorded 26,943 retained tasks and 205,935,962 charged payload bytes;
the last periodic sample recorded 27,543 retained tasks and 326,476,227 charged
bytes against a **50,000-task / 335,544,320-byte (320MiB)** tenant limit. That
last sample preceded the final terminal reconciliation and is not a final
instantaneous database snapshot. Background archive/purge and sampling continue
on their own schedules.

The [offline response audit](../ops/reports/qwen235b-soak30m-baseline-response-audit-2026-10-24.json)
checked all 600 collected responses without database or HTTP calls. The largest
canonical response body was **13,665 bytes**; all observed responses fit 65,536
bytes. Retrospectively, the response-allowance component for these 600 tasks is
629,145,600 bytes at 1MiB versus 39,321,600 bytes at 64KiB, a 93.75% reservation
reduction. This arithmetic excludes inputs and error allowances, does not
measure PostgreSQL heap/TOAST/index/WAL bytes, and does not guarantee that future
responses fit the smaller ceiling.

At the explicitly supplied **$16.10 per whole-Pod hour** scenario, the report
allocates **$8.662331** to the measured wall time. That includes idle time,
backoff, drain, validation and any disconnect wall time. It is neither a provider
invoice nor marginal inference cost, and is not a long-term cost qualification.
The four physical-GPU sampled mean utilizations were **87.80%, 72.24%, 69.34%
and 76.14%**, from 189 observations per device. These are device-wide activity
samples, not model-only Tensor Core utilization or proof of continuous occupancy.

## Interrupted 64KiB pilot and upload safety

The [policy record](../ops/reports/qwen235b-soak30m-response64k-policy-2026-10-24.json)
changed only the allowance for future responses from 1MiB to 64KiB. It preserved
the total payload quota, retained-task cap and existing tasks' immutable ceilings.
The subsequent attempt stopped with `ObserverFailure`; it did not become a
second completed 30-minute run.

The [reconciliation](../ops/reports/qwen235b-interrupted-response64k-pilot-evidence-2026-10-24.json)
proves **60 accepted / 60 completed / zero failed**, accepted once with matching
usage, real model/identity/output validation, 327,168 prompt tokens and 40,800
generated tokens. The [60-response audit](../ops/reports/qwen235b-interrupted-response64k-pilot-audit-2026-10-24.json)
found every collected response within 64KiB; the largest was 12,582 bytes.
The fourth upload's exact HTTP response was not recorded. A later observation
found no matching file, reservation or generation, but this absence does not
prove what response was received or establish a definitive root cause. The
original journal was preserved and the unknown upload was not automatically
resubmitted. `full_30_minute_comparison_valid=false` remains part of the evidence.

The repository now implements a conservative protocol in the
[gateway](../ops/gateway/worker_gateway.py) and
[sustained runner](../scripts/sustained_benchmark.py):

- Only a `POST /v1/files` with no query and one valid per-attempt nonce can
  receive the gateway's versioned `not-forwarded` proof, and only when its ready
  upstream set is empty **before any upstream send**.
- The gateway strips all three reserved proof headers from forwarded requests
  and every upstream response. It never automatically replays a mutation.
- The runner accepts that proof only from the exact loopback upload endpoint,
  with the current nonce, singleton headers and the bounded fixed error body.
  It journals the result before a bounded admission-window retry with a new
  nonce. Generic 503 responses, send/stream failures and transport exceptions
  remain uncertain; they do not authorize an automatic re-upload.

[Gateway tests](../ops/gateway/test_gateway.py),
[runner tests](../tests/test_sustained_benchmark.py) and the
[guarded deployment controller](../ops/ha/deploy_upload_protocol.py) cover these
semantics. Source implementation and tests do not retroactively establish the
missing pilot response or create a matched A/B result.

## Separate prefill-heavy and decode-heavy cohorts

Two further schema10/eager-mode cohorts each completed 100 fixed-output tasks
once, with zero failures or cancellations and exact output lengths. They used
the same protected sources, environments and process identities. These are
different workload regimes, **not a scheduling-policy A/B comparison**.

| Measurement | Prefill-heavy 100 | Decode-heavy 100 |
| --- | ---: | ---: |
| Actual prompt tokens | 1,228,800 | 32,000 |
| Actual generated tokens | 8,000 | 128,000 |
| Cohort seconds | 86.443976 | 255.329430 |
| Completed tasks/s | 1.156819 | 0.391651 |
| Observed prompt tokens/s | 14,214.987057 | 125.328287 |
| Observed generated tokens/s | 92.545489 | 501.313147 |
| Task completion P50 seconds | 53.525530 | 128.668152 |
| P95 seconds | 83.379998 | 212.381859 |
| P99 seconds | 84.613885 | 254.087056 |
| Allocated cohort cost at $16.10/Pod-hour | $0.386597 | $1.141890 |
| GPU samples per device | 79 | 233 |
| Mean GPU activity, devices 0/1/2/3 | 91.87% / 86.62% / 87.46% / 91.95% | 89.12% / 71.58% / 68.86% / 77.67% |

The [prefill report](../ops/reports/qwen235b-prefill100-report-2026-10-24.json)
and [decode report](../ops/reports/qwen235b-decode100-report-2026-10-24.json)
contain the measured populations and limitations. Token rates divide actual
cohort token totals by cohort wall time; they are not isolated prefill/decode
kernel rates. The GPU observations are device-wide samples. Short-burst cost
allocations do not establish sustained cost under admission pressure.

The original controller raised a post-run report-field `KeyError` **after
prefill had completed and before decode started**. The
[original journal](../ops/reports/qwen235b-prefill100-original-controller-journal-2026-10-24.json)
was retained. The [independent continuation](../ops/reports/qwen235b-regime100-reconciliation-2026-10-24.json)
verified the existing prefill result and ran only decode, preserving the source
and runtime binding. It did not rerun prefill or rewrite the interrupted
controller as a successful original execution.

## Historical adaptive100 comparison

Earlier finite cohorts used the same mixed fixed100 fixture: 545,280 prompt
tokens and 68,000 generated tokens, with the archive publisher off. The
[static16 analysis](../ops/reports/primary-qwen235b-seq16-platform-analysis-2026-10-24.json)
and [adaptive64 analysis](../ops/reports/primary-qwen235b-adaptive64-platform-analysis-2026-10-24.json)
record:

| Configuration | Cohort seconds | Task completion P50 / P95 / P99 seconds |
| --- | ---: | --- |
| Static16 | 466.368680 | 52.915453 / 454.555047 / 456.579551 |
| Adaptive64 claims | 244.281180 | 54.408186 / 220.534685 / 240.953150 |

The observed throughput ratio was 1.909147; P95/P99 fell 51.48%/47.23%, while
P50 rose 2.82% and mean engine TTFT increased from 0.941603 to 4.243439 seconds.
This was one serial cohort per configuration, with exact-token admission and
engine concurrency changed together. It does not isolate a kernel effect,
establish statistical replication or qualify a sustainable arrival rate.
Sixty-four claims include tokenization, reservation waiting, engine work and
reporting; they are not 64 full-context GPU executions or 64 separately priced
slots. The scheduling profile's static16 proxy rates were explicitly retained
as unrecalibrated.

## Narrow public-answer quality check

The [public-answer100 report](../ops/reports/qwen235b-public-answer100-report-2026-10-24.json)
completed 100 natural-generation requests once with zero failures. The
[quality evaluation](../ops/reports/qwen235b-public-answer100-quality-2026-10-24.json)
recorded **exact match 0.81** and **token F1 0.911571646**; all 100 outputs met
the JSON contract, with no missing, duplicate or length-truncated outputs.
This is a narrow constructed public SQuAD 1.1 development subset, not an
official SQuAD score or an independent broad model-quality evaluation. Public
examples may be training-contaminated; missing/invalid outputs score zero.

These natural answers generated only 1,407 tokens in total, unlike the
fixed-output capacity cohorts. Their task rate must not be compared directly
with the long-output stress workload as evidence of a performance improvement.
The [quality controller record](../ops/reports/qwen235b-public-answer100-controller-2026-10-24.json)
preserves its dataset, fixture and runtime provenance without publishing raw
questions or answers.

## Remaining production boundaries

Schema10 admission bounds retained task count, logical payload bytes and
auxiliary metadata. Automatic verified archive/purge releases completed payload
credits; it does **not** remove retained task/attempt history or make batch,
file, idempotency and archive metadata unbounded. The recorded auxiliary limits
include 4,096 idempotency rows, 2,000 input files, 2,000 archive runs and 20,000
archive chunks per tenant. Ready archive bytes and stored input bytes have
their own quotas. The [storage observation](../ops/reports/single-pod-storage-capacity-observation-2026-10-24.json)
also records a 1GiB provider branch logical-size limit; its PostgreSQL physical
allocation observation is a different quantity. No storage growth rate or
indefinite retention capacity has been qualified.

The model uses all four GPUs as one TP4 engine on one Pod. Two API processes
and durable database leases do not provide another compute failure domain:
whole-Pod failure removes inference capacity. This evidence does not establish
compute high availability, heterogeneous-GPU fleet performance, cross-node
recovery under these workloads or a daily task-capacity guarantee. Accepted-once
counts describe durable database acceptance, not exactly-once GPU execution
under faults.

A fresh, matched long-duration pair can evaluate the response-reservation
change under one frozen source/runtime configuration. That pair must retain
scheduled-arrival latency, admission rejections, full-plan completion, storage
backlog and whole-Pod cost accounting. No schema11 deployment, fresh matched
pair, or result graph is claimed by this document.
