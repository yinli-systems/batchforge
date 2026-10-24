# Single-Pod validation, 24 October 2026

This deployment has one Pod with four H100 80GB GPUs. The two unrelated service
families were stopped with the owner's explicit permission. GPU memory reclaimed
was 220,436 MiB. The API replicas, private gateway and worker remain on that one
Pod; Neon PostgreSQL and private S3 provide separate data fault domains. Two API
processes on the same Pod do not provide whole-Pod compute failover.

## Equal-work scheduling comparison

The public SQuAD-text matrix covers all combinations of 128, 512, 2,048, 8,192,
16,384 input tokens and 32, 128, 512, 2,048 output tokens. Prefix caching is off.
Each 100-task fixed-length run generates exactly 68,000 output tokens from
545,280 input tokens. Completion percentiles measure database batch creation to
accepted durable task completion, including queueing. They are not engine-only
service latencies. Model output correctness is a separate evaluation.

| GPT-OSS-120B configuration | Tasks | Wall seconds | Tasks/s | P50 seconds | P95 seconds | P99 seconds |
|---|---:|---:|---:|---:|---:|---:|
| FAIR, 8 slots | 100 | 198.197474 | 0.504547 | 89.462731 | 162.042405 | 187.966966 |
| Cost aware, 8 slots | 100 | 210.959606 | 0.474024 | 22.162883 | 171.587937 | 179.734662 |
| FAIR, 64 slots | 100 | 56.418490 | 1.772469 | 18.462872 | 53.000382 | 53.339588 |
| Cost aware, 64 slots | 100 | 54.419908 | 1.837563 | 17.216289 | 51.695280 | 52.352007 |
| Cost aware, 64 slots | 400 | 143.266120 | 2.792007 | 40.417200 | 116.145882 | 139.851518 |

All five runs accepted every task once, with no failures or cancellations. The
400-task cohort has 2,181,120 input and 272,000 output tokens. Its greater
throughput reflects a longer, fuller queue; it is not an equal-size policy A/B.
Its pre/post engine histogram counters contain exactly 400 observations and no
counter reset. Protected service identities, environment hashes and the live
manifest remained unchanged during this isolated run.

For that 400-task run the engine histogram stage means were 0.163623 seconds
prefill, 17.682991 decode, 0.880563 engine queue, 1.082504 time to first token
and 18.758391 end to end. The P95 histogram bucket upper bounds were 0.5, 50,
5 and 50 seconds for prefill, decode, time to first token and end to end,
respectively. These stages are wall times under batching, not
CUDA kernel timings, and bucket bounds are not exact percentiles. Platform
first-queue mean was separately 30.899 seconds. The 271,600 inter-token
observations averaged 0.026043 seconds per output-token interval.

The 64-slot retune also increased the GPU memory fraction from 0.26 to 0.85 and
the batched-token limit from 1,024 to 4,096, and deployed schema 8 leased result
readers. The 3.51x FAIR and 3.88x cost-aware throughput ratios over the 8-slot
runs describe complete configuration changes. They do not isolate concurrency,
SQL changes or GPU kernels. The serial 64-slot policy pair favors cost aware by
3.67% throughput, but has no independent repetitions or confidence interval.
At 8 slots cost aware improved the median but reduced total throughput. The
implementation does not assume that its policy always wins.

The scheduler's actual heterogeneous-profile routing, cheaper fresh-peer
deferral, aging/deadline overrides and runtime fencing have real PostgreSQL
tests. These measurements have only one physical GPU type and one TP4 worker;
they do not establish heterogeneous-device cost savings.

## Failure recovery and storage

The WAN expired-lease experiment runs against independent disposable databases
on the actual managed Neon service from the Pod. For 200 expired tasks, the old
path took 2.634510 seconds and the SQL path took 0.174232 seconds, a 15.12x
ratio. A separate 1,000-task SQL run took 0.326484 seconds. Sixteen accepted
results, their usage and queue counters remained consistent. These measure
bounded state transitions, not AI inference throughput or GPU repair time.

A separate real 100-task worker-fault cohort captured 64 valid leases, killed
the owned worker, and observed all 64 expire and return to pending after
90.6218 seconds. The probe restarted a two-slot worker, observed three legal
capacity samples, gracefully drained two captured in-flight requests, and
restored the original 64-slot worker while 97 tasks were still pending. Running
work subsequently reached 64. All 100 tasks completed with exactly one accepted
attempt each; 64 had an expired earlier attempt. For one of those same reclaimed
tasks, both its old heartbeat and a valid old completion returned 409 without
changing accepted state or usage.

This was an operator-controlled worker crash and restart-time capacity change;
the APIs automatically reclaimed expired leases. It did not hot-shrink active
GPU work, reset a GPU, kill the engine, migrate KV state or test whole-Pod loss.
Engine and API process identities remained unchanged. The functional cohort
overlapped CPU-only backup restoration and is not an isolated throughput A/B.
Physical inference can repeat after a lost lease despite one durable acceptance.

Input artifacts use private verified S3 objects, generation-fenced keys,
tenant reservations before I/O, read leases and bounded garbage collection.
Result archival uses immutable chunk manifests, complete checksums, leased
downloads and bounded completed-payload purge. An isolated Neon-branch/S3 proof
archived 20 existing inference results and cleared their completed SQL payloads;
all 52,200 output bytes remained identical and attempt/usage counts unchanged.
The production proof subsequently did the same for one existing 20-task batch:
all bytes matched through the gateway and both API replicas; completed task
payload bytes fell from 578,410 to 40 while every batch counter, identity,
attempt and usage total remained unchanged. It used four bounded five-row
purge pages. Publication for other batches remained off during timed testing.

Before that purge, a fresh 121,441,717-byte schema-8 snapshot was copied off
the Pod and its SHA-256 verified. It was restored with PostgreSQL 17.11 into a
new unprivileged Unix-socket-only temporary local cluster. Schema, counters,
usage, accepted attempts and queue invariants passed; the cluster was stopped
and removed. This validates that snapshot's SQL recovery, not object-store,
role/ACL, region-loss or whole-provider recovery.

Archival does not delete task/attempt history or failed-task retry bodies.
Allocated PostgreSQL disk size need not shrink after logical payload cleanup.
Database-owned admission now bounds new work while archival is blocked; archive
quota exhaustion retains SQL data and rejects further over-budget submissions.
Ready output expiry, history/index retirement and a global physical database
high-water gate remain separate work.

The production v8-to-v10 migration completed on both API replicas after an
actual fresh off-Pod snapshot and a drained, fail-fast lock check. It installed
retained-task/payload admission, eight auxiliary metadata budgets, runtime grants
and the production policy in one owner transaction. The 11-file bundle came
from checkpoint `5448ee8`; GPU engine, worker and gateway identities were
unchanged. A compatibility roll preceded the transaction and a second roll
enabled the minimum-schema gate. Both APIs and the gateway downloaded the
existing archived20 result with the original SHA-256.

The drained bootstrap charge was 205,935,962 bytes across 26,823 tasks. The
production tenant now has a 320 MiB payload budget and 50,000 retained-task cap.
The exact next fixed100 reserves 120,540,265 bytes, leaving only 9,068,093 bytes;
this gate includes actual PostgreSQL-canonical body sizes. Completion preserves
its response/error reservations until verified archival purge releases them,
so enabling archival before further cohorts is necessary. Auxiliary budgets
include 128 MiB of idempotency reservations at 64 KiB/key, an effective maximum
of 2,048 such keys before that byte cap, despite a larger row cap.

Production checks found zero counter/undercharge/missing-ledger mismatches and
verified the low-privilege runtime cannot execute the five private auxiliary
definer functions or delete task/attempt history. The v9 usage table intentionally
retains UPDATE permission for invoker accounting triggers; its direct-mutation
guard remains enabled. See the
[rollout procedure](storage-admission-rollout.md),
[capacity proof](../ops/reports/storage-admission-pre-migration-capacity-2026-10-24.json)
and [actual privilege inventory](../ops/reports/storage-admission-production-lowpriv-2026-10-24.json).
These logical reservations are not heap/TOAST/index/WAL bounds. The managed
project's observed branch logical-size limit is 1 GiB; neither that limit nor
the quotas establish unlimited retention or sustained daily capacity.

A fresh schema10 custom-format snapshot (127,944,921 bytes, SHA-256
`302d3f2e7bf4e9b9246b663c4bad9cfc12d5e2b32bd212fc408f96d5ea44b2bd`)
was actually restored into a separate unprivileged PostgreSQL17.11 cluster on
the Pod. Exact migration checksums, accepted usage, queue counters, all admission
ledgers and constraints passed. Pure restore took 10.051 seconds; the cluster
was stopped and removed. The [restore report](../ops/reports/schema10-postgres17-local-restore-2026-10-24.json)
records this functional recovery, not a recovery-time SLA or role/ACL/object-store
recovery. Export of this new snapshot to the operator machine is pending explicit
permission after automatic approval rejected that transfer. It has **not** been
verified off-Pod; the earlier schema8 off-Pod snapshot remains a separate proof.

## Larger model activation

The official `Qwen/Qwen3-235B-A22B-Instruct-2507-FP8` checkpoint is pinned to
`e156cb4efae43fbee1a1ab073f946a1377e6b969`. All 24 weight shards were verified
against their official LFS hashes before activation. It has 235B total and 22B
active parameters, so its compute demand should not be inferred from total
parameter count alone. It uses the same four physical H100 GPUs, TP4, FP8
weights and dynamically quantized activations, with BF16 runtime dtype and KV
cache. Runtime dtype does not identify every kernel's arithmetic or accumulation
precision.

The guarded switch preserved API/gateway identities, measured fresh memory
headroom, and withheld its worker until direct calibration passed. Startup
reported 55.1856 GiB of weights and 14.17 GiB KV memory per rank, 316,144 cache
tokens and 17.14 full 18,440-token contexts. The initial configuration has eight
slots, 4,096 batched tokens, eager execution and prefix caching disabled.
All 20 exact-length public-text cells passed: 109,056 input and 13,600 output
tokens in 199.0767 seconds. This is an engine calibration rather than an
end-to-end durable platform benchmark or natural-answer quality result.
The observed FlashAttention backend message refers to the vLLM V1 engine;
it does not establish a FlashAttention algorithm version.

The Qwen scheduling profile uses new HTTP phase calibration from these actual
requests, with positive-rate fits and residual errors. Its first-text proxy
includes queueing and one decode token; stream duration is affected by transport
buffering. Neither is an isolated kernel rate, and 120B measurements are not
reused as Qwen measurements. Subsequent platform/concurrency measurements are
recorded independently.

The next guarded restart raised the static full-context limit to 16. Fresh
startup again measured 316,144 KV tokens and 17.14 full contexts, exceeding
the 295,040-token reservation. A separate direct 20-cell calibration passed
all exact token totals in 176.515110 seconds. Its HTTP proxy fits were
5,573.560942 prefill tokens/s and 11.775314 decode tokens/s, with residuals
retained in the report. These fitted request rates are not aggregate device
throughput. The 100-task platform comparison is a separate measurement.

The initial eight-slot durable platform run accepted 100/100 exact-length
requests in 809.615794 seconds: 545,280 input and 68,000 output tokens,
0.123515 tasks/s, completion P50/P95/P99 of 71.076502/650.589254/767.969815
seconds. Every task had one accepted attempt and no retries or failures. The
engine's measured mean decode stage was 56.123523 seconds per request, with
67,900 inter-token intervals averaging 0.082656 seconds. Its whole-Pod
allocation scenario is $3.620782 per 100, or
$36,207.82 per million for this workload at the assumed $16.10/hour.

The following natural-length summary cohort completed 20/20 in 199.276531
seconds, with 109,056 input and 6,728 generated tokens. All 20 had final text;
13 reached their output cap and 7 stopped naturally. Completion P50/P95/P99
were 23.610710/125.397426/178.249579 seconds. These summaries have no gold
reference and no correctness score. A smaller finite cohort can have worse
allocation cost per completion despite generating fewer tokens.

The subsequent static16 fixed100 used the same 235B input bytes and exact
545,280/68,000 input/output totals. All 100 completed once, without retries,
failures or capped-length mismatches, in 466.368680 seconds (0.214423 tasks/s).
Completion P50/P95/P99 were 52.915453/454.555047/456.579551 seconds.
Protected service identities, environment files and the manifest were unchanged
during the run; a fresh global query confirmed no pending/running tasks after it.
Relative to static8, this serial configuration comparison improved throughput
by 73.6%, reduced P95 by 30.1% and P99 by 40.5%, and reduced allocated cost by
42.4%: $2.085704 per 100, or $20,857.04 per million at the same hourly scenario.
The concurrency and fitted scheduler profile both changed; this is not an
isolated kernel improvement or a replicated statistical estimate.

Exact-token admission then raised the claim limit to 64 while reserving each
request's actual rendered prompt plus its maximum output, rounded to 16-token
blocks. Fresh startup measured 314,096 KV tokens; the 95% guard selected a
298,384-token worker budget. The same fixed100 completed in 244.281180 seconds,
100/100 accepted once, with the same exact input/output totals and no retries.

| Qwen235B configuration | Seconds / 100 | Tasks/s | Completion P50 | P95 | P99 | Allocated $ / million |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Static8 | 809.616 | 0.123515 | 71.077 | 650.589 | 767.970 | 36,207.82 |
| Static16 | 466.369 | 0.214423 | 52.915 | 454.555 | 456.580 | 20,857.04 |
| Adaptive64, custom collective | 244.281 | 0.409364 | 54.408 | 220.535 | 240.953 | 10,924.80 |
| Adaptive64, NCCL experiment | 256.251 | 0.390243 | 55.736 | 233.262 | 252.007 | 11,460.11 |
| Adaptive64, final schema10, publisher ON | 250.149 | 0.399762 | 56.605 | 228.294 | 246.166 | 11,187.22 |

The process-lifetime reservation peak was 298,240 tokens, within the budget;
239 worker samples reported no pause or observation error. Claimed work peaked
at 64, while sampled engine-running work peaked at 51 and physical KV cache
usage at 83.01%. These are different populations. Compared with static16,
throughput improved 1.909x and P95/P99 fell 51.5%/47.2%, but P50 increased 2.8%
and mean engine TTFT rose from 0.942 to 4.243 seconds. More batching favors
batch makespan and tail completion here, not every interactive latency metric.
The copied static16 scheduling rates and slot-hour price were not recalibrated
or divided by 64; allocated costs use measured whole-Pod elapsed time.

The NCCL counterfactual preserved that budget, fixture, projected profile rates,
memory fraction and batching limits. Its 100 tasks also completed once with the
exact token totals. Makespan increased 4.90%, P95 5.77% and P99 4.59%; queue
median improved 7.99%. This single serial pair does not prove a universal
collective ranking. The original custom configuration was actually restored
and reverified, including its registry fingerprint and fresh KV capacity.
All GPU pairs report NV18 links; link status is not measured bandwidth.
See [the collective experiment](collective-comparison.md) for source, topology,
negative result and restoration evidence.

The final source run `bench-7e815935e9bd` used schema10, retained-payload and
metadata admission, both API publishers ON and one conditional negative HTTP
probe. Every task again completed once with the same exact token totals, zero
failures/retries and 100 observations in each request-stage histogram. All
protected sources, service identities, environments and the manifest remained
unchanged. Its 250.149091-second makespan was 2.402% longer than the historical
244.281180-second publisher-OFF adaptive run; P50/P95/P99 increased
4.04%/3.52%/2.16%. This compares complete production configurations, not an
isolated causal estimate for schema10 or archival. Relative to static8, the
final configuration's observed throughput improved about 3.24x.

After the 100 durable submissions, an additional 20-task submission reused an
existing input and exceeded the remaining payload budget. The real API returned
`429 retained_payload_limit` in 0.503138 seconds. No probe batch, task, attempt
or idempotency row remained; normal completion progress was preserved and the
ledger matched task-derived totals. This was one POST without retry. The
operator's exclusive workload window and archive-grace checks constrained the
probe; client timeouts do not cancel an arbitrarily delayed server request.
The [final analysis](../ops/reports/primary-qwen235b-final-source-platform-analysis-2026-10-24.json)
indexes the original metadata and all seven checks. The first controller launch
failed before submission on JSON serialization of PostgreSQL `Decimal` values;
its source/log are preserved. An explicitly guarded continuation proved zero
earlier submissions and ran the same stable job once with normalized evidence.

A separate natural-output public SQuAD answer cohort completed 20/20 in
7.956454 seconds, using 21,783 input and 267 output tokens with no truncation.
All 20 satisfied the JSON contract; exact match was 16/20 (80%) and token F1
87.397%. The public dataset can be training-contaminated, the sample is small,
and this is not an official SQuAD score or general customer-quality guarantee.
It is separate from the fixed-length throughput measurements and the final
automatic archive validation below.

The final publisher-ON natural cohort `bench-5b18ae857c7b` completed 20/20 once
in 6.319519 seconds, again with 21,783 input and 267 generated tokens. Exact match
remained 16/20 and F1 87.397%. Inference plus validation took 7.368587 seconds;
its assumed allocation was $0.032954, or $0.002060 per exact-match answer.
This is the same small public fixture, not an independent quality population.
The [final QA/archive analysis](../ops/reports/primary-qwen235b-final-answer20-analysis-2026-10-24.json)
binds the metadata hashes, answer scoring and lifecycle checks.

Its background publisher then automatically archived and purged all 20 completed
payloads, returning 23,700,308 bytes of reservation. The observer did not invoke
archive or purge. Task/attempt identities, accepted token metering and all batch
counters remained unchanged, and both before/after tenant ledgers matched their
derived rows. All 17,841 downloaded bytes matched the original through ports
8080, 8082 and 8083, SHA-256
`7516276bd07acfdb7ac856ed1b515bb0ff27d181275c5966cc96108a233e5e0e`.
Archive observation took 122.558 seconds separately from inference: keeping the
whole Pod allocated for that observation adds a $0.548105 scenario cost, giving
$0.581067 across the entire 129.928-second validation window. That wait is not
zero-cost or GPU inference time. This verifies this new post-cutoff batch's
automatic SQL/S3 lifecycle, not arbitrary history retirement or provider DR.
The final read-only runtime observation subsequently found all six new archive
runs complete and all 120 new task payloads purged. The tenant's payload charge
returned to its pre-run 205,935,962 bytes while retained task count increased
to 26,943, preserving accepted history. All four admission invariants were zero,
no tasks remained active and ports 8080/8082/8083 were ready200. The
[runtime state](../ops/reports/single-pod-final-runtime-state-2026-10-24.json)
also records current auxiliary usage and the unresolved snapshot-export boundary.

An untimed four-request CUDA capture verified all four TP ranks and physical
CUDA devices. The original capture's strict file mapper rejected a small,
CPU-only engine-leader trace; the original failed report remains preserved.
An offline continuation analyzed those same five hash-pinned files, with no
new inference/profile calls. Overlap-aware per-rank unions show substantial
custom-collective activity (about 0.919/3.734/2.894/3.025 seconds across the four
ranks), including possible waiting/spin. These are neither Tensor Core
utilization nor measured network bandwidth. The separate four-GPU MoE search
rejected every alternate tiling at its strict BF16 bit-identity gate, before
timing alternatives. No tuned kernel configuration was installed; ratios of
1.0 represent the remaining default, not evidence that alternatives were slower.

The 120B and 235B matrices use different tokenizer-specific text boundaries and
therefore different input bytes, although their requested token-length grid
and the fixed100 actual input/output totals match. Comparing the optimized
120B64 configuration with Qwen235B8 also changes quantization and concurrency.
It is a practical configuration comparison, not an isolated parameter-count
or model-architecture effect. Adaptive admission is evaluated separately from
these completed static8/static16 references.

## Distance to ten million tasks per day

Ten million completions per day require 115.740741 completions/s. Extrapolating
the 400-task 120B burst gives about 241,229 tasks/day per four-GPU Pod at 100%
duty, or 192,984 at 80%. Ideal linear arithmetic therefore gives 42 Pods (168
GPUs) or 52 Pods (208 GPUs), respectively. This is a planning scenario, not a
measured multi-Pod result, 24-hour capacity guarantee or purchase recommendation.
Model, token distribution, batching and useful-answer rate change this estimate.

Using the historical whole-Pod allocation scenario of $16.10/hour gives about
$0.640718 for the 400-task run, or $1,601.795 per million durable completions.
At 52 constantly billed Pods this would be $20,092.80/day before database,
storage, networking and operational overhead. The $16.10 figure is not a current
invoice, marginal inference cost or sales price. A measured duty cycle and an
actual provider quote are needed before making a spend commitment.

For the larger Qwen235B adaptive100 workload, the same arithmetic gives about
35,369 tasks/day at 100% duty. Ten million would require 283 four-GPU Pods under
ideal linear scaling, or 354 at 80% duty, before control-plane/storage/network
constraints. This is a finite-burst extrapolation, not measured daily or
multi-Pod capacity. Its much larger allocation per million also shows why task
token distribution and model choice must accompany every capacity claim.

Using the final schema10/publisher-ON 250.149091-second sample instead gives
34,539 tasks/day under ideal full duty, or 290 four-GPU Pods for ten million;
at 80% duty the same linear arithmetic gives 362 Pods (1,448 GPUs). Allocated
cost was $1.118722 per100, or $11,187.22 per million, at the historical assumed
whole-Pod $16.10/hour. These remain extrapolations from100 tasks; no sustained
daily arrival test, heterogeneous fleet or quoted current rental cost was
measured. The current four GPUs do not demonstrate ten-million-per-day capacity.
This is compute-only arithmetic: the current tenant already retains about
26,943 tasks against its 50,000 cap, leaving about 23,057. Automatic payload
purge does not delete retained history or refund that task count. At the sample's
rate, a continuously submitting tenant would reach that cap in about 16 hours,
before completing one projected day. Input-file and idempotency history also
have finite caps. Safe automated history/idempotency retirement and fleet-scale
admission are prerequisites for sustained capacity; merely multiplying GPU
count does not resolve them.

The next scalability requirements are independently deployed compute and entry
points, multiple calibrated GPU pools, per-tenant admission and retention,
database capacity planning under sustained traffic, object recovery, fault
domain placement and long-duration arrival-driven tests. GPU activity includes
communication/spin kernels; NVML percentages and memory occupancy alone do not
establish Tensor Core saturation.

## Reproducibility

See `ops/reports/primary-scheduler-seq64-ab-analysis-2026-10-24.json` for the
source hashes, exact run IDs, raw evidence hashes, policy decisions, per-class
latencies and all confounds. The original metrics-preflight and report-size
controller failures are preserved; neither caused duplicate workload submission.
Secret-bearing environments, raw model outputs and database snapshots remain
outside Git. Core result archival was pushed as `f1d45b7`.

Checkpoint `5448ee8aabe2d55b8575894c6a39f0dd6d340221` passed 683 local tests
with five platform skips. Its exact committed snapshot was separately validated
on the Pod against a new unprivileged, Unix-only PostgreSQL 17.11 cluster:
**688 passed, zero skipped**, 140 Python files compiled, no production
credentials inherited and no production database accessed. The temporary
cluster was stopped and removed, with no remaining scoped test process.
The [Linux report](../ops/reports/single-pod-linux-pg17-validation-2026-10-24.json)
binds the commit, source manifest, binary and test-log hashes.
The final code checkpoint `4752605da4819a148f2bb292fff6a6e2e0eb84f5`, including
the independently reviewed one-POST backpressure probe, subsequently passed
**704 tests, zero skipped**, in a second isolated PostgreSQL17.11 run. All 142
Python files compiled and the temporary cluster was stopped and removed;
[its report](../ops/reports/single-pod-final-source-linux-pg17-validation-2026-10-24.json)
binds all 159 source files. This CPU test window ended before the final GPU run.
GitHub-hosted checks for both code checkpoints did not start any test step because
of the account's payment/spending limit. The
[final code status report](../ops/reports/single-pod-final-source-hosted-ci-status-2026-10-24.json)
preserves this distinction; independent validation is not a hosted CI pass.
