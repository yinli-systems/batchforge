# Technical evidence index — October 24, 2026

**Latest operational boundary:** Neon now rejects SQL connections because the
project's monthly network-transfer allowance is exhausted. Historical readiness
and accepted-task results below describe their recorded windows. A separate
[Render Free qualification](free-postgres-validation-2026-10-24.md) has passed
strict TLS, full schema11 rollback and runtime SQL ACL checks; old data has not
been migrated or replaced. The [new direct-engine diagnostics](qwen235b-engine-diagnostics-2026-10-24.md)
continue four-H100 measurements without claiming durable platform completions.

BatchForge has run the official Qwen3-235B-A22B-Instruct-2507-FP8 checkpoint on
one four-H100 80GB Pod. The model has 235B total and 22B active parameters. The
checkpoint revision is `e156cb4efae43fbee1a1ab073f946a1377e6b969`; execution uses
TP4, block FP8 weights with dynamic activation quantization, BF16 runtime
dtype/KV cache, an 18,440-token context limit, 4,096 batched tokens, eager mode,
chunked prefill and no prefix cache. The current selected
path uses custom collectives and adaptive exact-token admission with 64 claim
slots and a 298,384-token logical budget. This is one physical GPU type, not a
measured comparison of heterogeneous hardware.

The [final API policy rollout](../ops/reports/final-api-policy-live-deployment-2026-10-24.json)
subsequently made this model the console/API default and enabled result
publication for batches created at or after 03:59:21.961089 UTC, with a
120-second terminal-age requirement before archival and purge. Older batches
are excluded by that cutoff. [Readiness verification](../ops/reports/final-api-policy-final-readiness-2026-10-24.json)
passed on both API replicas and the gateway without changing the GPU services.

## Equal-work inference measurements

These historical cohorts ran with the archive publisher **OFF**. Each completed
the same 100-task fixture exactly once at the acceptance boundary, with 545,280
actual prompt tokens and 68,000 generated tokens. The fixture covers inputs
128/512/2,048/8,192/16,384 and outputs 32/128/512/2,048. Fixed output caps measure
capacity; they do not establish answer correctness.

| Configuration | Total seconds | Generated tokens/s | Completion P50 / P95 / P99 seconds | Evidence |
| --- | ---: | ---: | --- | --- |
| Static 8 slots | 809.616 | 83.990 | 71.077 / 650.589 / 767.970 | [Analysis](../ops/reports/primary-qwen235b-seq8-platform-analysis-2026-10-24.json) |
| Static 16 slots | 466.369 | 145.807 | 52.915 / 454.555 / 456.580 | [Analysis](../ops/reports/primary-qwen235b-seq16-platform-analysis-2026-10-24.json) |
| Adaptive 64 claims, custom collective | 244.281 | 278.368 | 54.408 / 220.535 / 240.953 | [Analysis](../ops/reports/primary-qwen235b-adaptive64-platform-analysis-2026-10-24.json) |
| Adaptive 64 claims, NCCL-only collective | 256.251 | 265.365 | 55.736 / 233.262 / 252.007 | [Analysis](../ops/reports/primary-qwen235b-nccl-adaptive64-platform-analysis-2026-10-24.json) |

The custom adaptive run achieved 3.314 times the static8 throughput on this
fixture, using 30.17% of its elapsed time. This was one serial cohort per configuration, not a replicated
statistical result or a sustained daily capacity test. Adaptive64 did not
improve every percentile over static16: its P50 was slightly higher while P95
and P99 fell. The NCCL-only candidate was 4.90% slower overall and had worse
completion tails; its queue P50 improved. The prior custom path was restored
through the reviewed recovery controller. See the
[comparison and limits](collective-comparison.md) and
[actual restoration](../ops/reports/primary-qwen235b-custom-restored-2026-10-24.json).

The $16.10/hour whole-Pod allocation is an operator-supplied scenario, not an
invoice or marginal inference price. Adaptive64 keeps the measured static16
HTTP phase proxies and its slot-price estimate explicitly labeled as
unrecalibrated. Sixty-four claims include tokenization, reservation waiting and
reporting; they are not 64 priced execution slots or 64 full-context GPU
requests. No ten-million-task/day capacity or linear fleet scaling is proven.

## Kernel and communication evidence

| Evidence | Proven result | Boundary |
| --- | --- | --- |
| [Same-capture offline trace analysis](../ops/reports/primary-qwen235b-trace-offline-analysis-v2-2026-10-24.json) | Four TP rank traces plus one verified CPU-only leader trace; profiler stop and exact four-request token deltas verified | Four short profiled requests, separate from timed cohorts. Interval unions account for overlap; kernel activity is not Tensor Core/FLOP utilization. |
| [Original capture report](../ops/reports/primary-qwen235b-trace-capture-original-2026-10-24.json) | Original strict mapper failure remains preserved; continuation used the same files without repeating inference | Extra owned CPU trace required explicit offline classification. The original report was not rewritten as passed. |
| [Private trace preservation](../ops/reports/primary-qwen235b-trace-local-preservation-2026-10-24.json) | All five raw trace gzip files copied privately and SHA-verified | Raw traces are not committed to the repository. |
| [Bounded four-GPU MoE window](../ops/reports/primary-qwen235b-moe-window-2026-10-24.json) | Exact stopped-window restoration succeeded; no candidate configuration installed | Eight alternatives per shape failed the strict bit-identical BF16 guard before timing. No alternative speedup was established; M=1–64 does not cover 4,096-token prefill. |
| [Read-only GPU topology](../ops/reports/primary-qwen235b-gpu-topology-2026-10-24.json) | NV18 between all four GPU pairs; 18 reported active links per GPU | Link state and nominal link rate are not measured end-to-end bandwidth. Long custom-collective kernels can include waiting or spinning. |

## Recovery, storage and accounting

| Evidence | Proven result | Boundary |
| --- | --- | --- |
| [Real worker fault100](../ops/reports/primary-scheduler-seq64-worker-fault100-analysis-2026-10-24.json) | 100 accepted completions, 64 expired attempts recovered; worker capacity changed 64→2→64 | This earlier cohort used GPT-OSS-120B. It proves durable acceptance fencing, not exactly-once GPU execution, a 235B fault rerun, or whole-Pod failover. |
| [Schema10 live storage rollout](../ops/reports/storage-admission-live-deployment-2026-10-24.json) | Exact migration ledger, four admission invariants, two ready API replicas and unchanged GPU services | Logical JSON/row accounting does not equal physical PostgreSQL/TOAST/index/WAL usage. Publisher remained OFF during rollout. |
| [Fresh schema10 backup](../ops/reports/schema10-fresh-backup-result-2026-10-24.json) | 127,944,921-byte custom-format snapshot, SHA `302d3f2e7bf4e9b9246b663c4bad9cfc12d5e2b32bd212fc408f96d5ea44b2bd` | Archive listing alone is not a restore test. |
| [Actual schema10 PG17 restore](../ops/reports/schema10-postgres17-local-restore-2026-10-24.json) | New nobody-owned Unix-only PostgreSQL 17.11 cluster; 23 tables with 26,823 task rows and 27,451 attempts restored; ten migration checksums, counters, queues, four admission ledgers and function signatures validated; cluster stopped and removed | Same-Pod functional restore. `--no-owner/--no-acl` excludes production roles, credentials and EXECUTE grants; external object-store data is outside this dump. Timing is not a recovery SLA. |
| [Restore binding](../ops/reports/schema10-postgres17-local-restore-binding-2026-10-24.json) | Exact snapshot/helper/source binding, unchanged six service identities and configuration files | The schema10 dump has **not** been exported off the Pod. Automatic approval rejected that transfer; explicit user authorization is pending. |

The previously verified schema8 off-Pod copy is a separate historical snapshot.
It must not be represented as an off-Pod copy of the new schema10 data.

## Quality and final storage-enabled cohort

The [historical public-gold QA20 baseline](../ops/reports/primary-qwen235b-adaptive64-baseline-answer20-quality-2026-10-24.json)
produced valid JSON for all 20 examples, exact-match 16/20 and token F1 0.873973.
It used natural generation capped at 96 tokens. This is a small public SQuAD
sample, potentially training-contaminated, not an official SQuAD score or a
general model-quality qualification.

The [final schema10 publisher-ON fixed100](../ops/reports/primary-qwen235b-final-source-platform-analysis-2026-10-24.json)
passed with all 100 tasks accepted once, one attempt each, zero failures and
the same exact 545,280/68,000 prompt/generated token totals. It took
250.149091 seconds, yielding 271.838 generated tokens/s; completion P50/P95/P99
were 56.604520/228.293969/246.166256 seconds. At the stated whole-Pod scenario,
that measurement allocates $1.118722 to the cohort.

This final cohort was 2.402% slower than the historical custom adaptive run.
Schema10 admission, publisher enablement and one functional negative HTTP
probe changed together; these single samples do not isolate the cost of any
one change. The initial instrumentation preflight failed to serialize a
PostgreSQL Decimal before submission; original source/logs and proof of zero
submitted work remain linked in the analysis. Only the successful actual
100-task continuation is included in its inference metrics.

The [one real HTTP backpressure probe](../ops/reports/primary-qwen235b-final-source-http-backpressure-2026-10-24.json)
returned `429 retained_payload_limit` in 0.503138 seconds. All seven checks
passed: no new batch, task or idempotency residue, unchanged cohort identities,
conserved accepted progress and derived ledger, and no budget release during
the probe. This controlled-window SQL-before/HTTP/SQL-after experiment is not
an atomic dry run or a guarantee against arbitrary object-store tail latency.
An uncertain or successful response would fail the proof without a retry.

The [final natural QA20 and automatic archive analysis](../ops/reports/primary-qwen235b-final-answer20-analysis-2026-10-24.json)
(SHA `9e92abeed5b04d9ff55c2fcb12f4c14e4fb5427d5b56186ca6473515e7e79758`)
verified all 20 accepted once, one attempt each, 21,783 input/267 generated
tokens and a 6.319519-second inference window. Exact match stayed 16/20 and
F1 0.873973. The same small public fixture does not provide an independent
quality population. Inference plus validation took 7.368587 seconds, allocating
$0.032954 at the stated whole-Pod scenario.

The background publisher alone archived and purged all 20 payloads, returning
23,700,308 reserved bytes. All ten lifecycle checks passed: unchanged accepted
usage, task/attempt identities and batch counters, ledger equality, and the
original 17,841 output bytes downloaded identically through 8080/8082/8083.
The output SHA was `7516276bd07acfdb7ac856ed1b515bb0ff27d181275c5966cc96108a233e5e0e`.
Archive observation separately lasted 122.557682 seconds and allocated
$0.548105; the full 129.927987-second validation window allocated $0.581067.
This waiting cost is not zero or GPU inference time.

The [final read-only runtime observation](../ops/reports/single-pod-final-runtime-state-2026-10-24.json)
found both new cohorts' 120 payloads purged across six archive runs, all four
admission invariants zero, no active work and all three ingress ports ready.
The tenant retained 26,943 task histories and charged 205,935,962 payload bytes.
At its measured finite-run compute rate, the 50,000 retained-task cap would be
reached in about 16 hours without history retirement. Automatic payload purge
therefore does not establish sustained daily capacity. The
[full validation report](single-pod-validation-2026-10-24.md) records these
retention limits, the 704 passing isolated Linux/PostgreSQL tests and hosted
CI's separate billing-related failure before any test step.
