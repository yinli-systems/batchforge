# Validation

This page preserves the real PostgreSQL and Qwen2.5-7B validation history.
The deployment now serves GPT-OSS-120B on both existing GPU Pods; see
[100B validation](100b-validation.md) for its separate measurements. Private SSH ingress is the
owner's selected policy. Short runs below establish observed behavior, not a
24-hour capacity guarantee or a production availability SLA.

## Correctness and deployability

The latest implementation passed the full local suite of **321 tests**, with
three Linux-only cases skipped on macOS (24.54 seconds, isolated real PostgreSQL).
The final supervisor and owned-descendant suite also ran on Linux: **50 passed
in 2.85 seconds**, including the macOS-skipped cases, leader loss, orphan cleanup
and preservation of an unrelated process. It used an isolated app copy with
CUDA hidden and no database or live service mutations. The initial observer
incorrectly looked for PID files deliberately removed by successful cleanup;
that raw error remains recorded. A follow-up checked the five exact test PIDs
and found none running ([final report](../ops/reports/linux-ops-final-2026-10-24.json),
[initial observer](../ops/reports/linux-ops-final-initial-observer-2026-10-24.json)).
The full suite exercises all four combinations of Python/SQL claim and completion
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
before the second Pod obtained independent APIs. The fault experiment subsequently
stopped and removed that link from the active primary service manifest. No
existing unrelated GPU process or Pod configuration was reset.

During an 8,192-task real 7B load, all three primary control-plane URLs were
stopped for 120.044924 seconds, 21:56:03.156472–21:58:03.201409 UTC. The secondary
continued accepting durable results, increasing its count for that exact run
from 591 to 3,196 during the outage. All 61 primary attempts whose leases were
valid at the captured database clock were reclaimed through actual lease expiry;
the original token then received HTTP 409. Accepted state and usage invariants
matched. The four primary engines and workers kept the same process identities.

After restoration, an exact owned API-a process was killed under continued load.
The watchdog restored a new ready API-a in 5.823 seconds while gateway readiness
samples remained successful through API-b. This is an application control-plane
outage simulation and process-recovery observation, not whole-Pod power-loss,
database failover, a recovery SLA or independent ingress availability.
See [the actual fault report](../ops/reports/controlplane-outage-bench-b868274f8fe7-2026-10-24.json).

The entire fault load subsequently finished all 8,192 tasks, with zero terminal
failures/cancellations and exactly 8,192 accepted attempts plus 61 expired
attempts. Downloaded unique outputs and accepted token totals matched the
database. Its 246.786-second duration includes the outage and uses a larger,
different-size corpus; it is recovery evidence, rather than another controlled
throughput comparison. Of those accepted analysis responses, 160 failed the
requested field contract and seven hit the output cap, reported separately.

The secondary's strict outage window also recorded three failed metric scrapes
and substantially higher API completion latency. HTTP complete counts include
retries/non-success responses and are not unique durable task counts. Recovery
latency and scrape failures remain operational findings despite final task
integrity passing.

## Real 7B configuration

- Model: `Qwen/Qwen2.5-7B-Instruct`, revision
  `a09a35458c702b33eeacc393d103063234e8bc28`, 7,615,616,512 parameters.
- BF16, vLLM 0.10.2, Torch 2.8.0/CUDA 12.8, Transformers 4.55.2,
  Tokenizers 0.21.4, Hugging Face Hub 0.34.4.
- Independent TP1 engines, eager execution, 4,096-token context and 4,096 maximum
  batched tokens. Prefix caching explicitly disabled. Initial comparisons use
  eight sequences per engine; the final separate tuning run uses sixteen.
  All twenty engines passed real sixteen-request generation probes at unchanged
  memory budgets before activation. H100 KV capacity was 77,024 tokens per
  engine, MIG capacity 85,440, above the 65,536-token worker reservation.
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
| Python claim/completion, 4 H100 + 6 MIG | 2,048 | 746.013 | 2.745 | 191.50 | 0 |
| SQL v2 completion, same 10 engines | 2,048 | 233.624 | 8.766 | 191.58 | 0 |
| SQL v2 completion, 4 H100 + 16 MIG | 2,048 | 262.548 | 7.800 | 191.05 | 0 |
| SQL v3 claim + v2 completion, same 20 engines | 2,048 | 86.950 | 23.554 | 191.06 | 0 |
| Same SQL paths/devices, 16 sequences per engine | 2,048 | 59.803 | 34.246 | 191.12 | 0 |

This observation is a 3.19× end-to-end increase. Both runs used identical total
input tokens (2,369,454); output totals were 392,194 and 392,357. All ten workers
accepted real results. Downloaded output IDs, durable task and batch counts,
accepted attempts and token totals matched, with no repeated attempts. Each run
has exact start/end times and separately timed output verification. Submission
acceptance is not counted as completed inference.

Adding ten MIG engines initially reduced completed throughput to 7.80/s. Moving
claim selection and accounting into one database statement then reached 23.55/s
on that same twenty-engine, eight-sequence configuration, a 3.02× observation.
All twenty workers accepted results, with 2,048 accepted attempts and matching
durable usage. These are single ordered runs on shared hardware; they do not
establish statistical confidence or linear hardware scaling.

The separately gated sixteen-sequence run completed in 59.803 seconds,
34.245613/s, 45.4% above the eight-sequence v3 observation. All twenty workers
accepted results; 2,048 tasks/accepted attempts and token totals matched, with no
retries. A secondary mid-run snapshot measured 255 leased/inflight tasks across
its sixteen workers, with all ready and all already contributing results.
Configured capacity totals 320 slots across both Pods; this is not a guarantee
of 320 requests completing at once or a daily throughput claim.
Retuning restarted engines/workers and reset worker origin selection; the old
v3 run had one worker on direct API-a. This remains an ordered configuration
comparison, rather than an isolated concurrency-only experiment.

The worker multi-origin patch was deployed alongside SQL v2. The before/after
worker snapshots disclose selected origin and failover counters; do not describe
this as a single-code-change laboratory experiment. Shared GPU load also remains
a confounder. The separate synthetic state-machine comparison is explicitly not
an HTTP or GPU benchmark.

Reports: [baseline](../ops/reports/squad7b-analysis-2048-baseline-2026-10-24.json),
[baseline attribution](../ops/reports/squad7b-analysis-2048-baseline-attribution-2026-10-24.json),
[SQL v2](../ops/reports/squad7b-analysis-2048-sql10-2026-10-24.json),
[SQL v2 attribution](../ops/reports/squad7b-analysis-2048-sql10-attribution-2026-10-24.json),
[twenty-engine SQL v2](../ops/reports/squad7b-analysis-2048-sql20-v2-2026-10-24.json),
[SQL v3](../ops/reports/squad7b-analysis-2048-sql20-v3-2026-10-24.json),
[SQL v3 attribution](../ops/reports/squad7b-analysis-2048-sql20-v3-attribution-2026-10-24.json),
[sixteen-sequence load](../ops/reports/squad7b-analysis-2048-sql20-v3-seq16-2026-10-24.json),
[sixteen-sequence attribution](../ops/reports/squad7b-analysis-2048-sql20-v3-seq16-attribution-2026-10-24.json).

The primary's completion handling mean fell from about 1.50 seconds to 0.28
seconds; p95 fell from histogram bucket (5,7.5] to (.75,1] seconds. These are API
handling times, not GPU latency. Exact sample windows are recorded. Histogram
percentiles are bucket bounds, not invented point estimates. Database lock wait
samples and low GPU occupancy supported the diagnosis of remote round trips
inside serialized critical sections. Claiming still held the tenant queue lock
across many network calls and became the next optimization target.

On the twenty-engine v3 run, primary API completion handling averaged 28.68 ms
and claim averaged 41.81 ms, compared with 391.35 ms and 129.75 ms in the
twenty-engine v2 run. Sampled transaction-lock waits decreased substantially.
The primary observation report records its slightly bracketing timestamp bounds;
the attribution report separately uses only fully contained samples. One second
Pod worker started the v3 run on direct API-a after the previous rollout;
failover counter increments during the run were zero. This origin difference is
retained as a confounder, rather than resetting counters for the comparison.

## Output quality is separate

The baseline produced 38 invalid JSON envelopes and one length-truncated output.
Its field-type contract success was 98.14%, answer exact match 65.77% and answer
token F1 80.10%, counting invalid and missing responses as zero. These are this
custom packed-context task's scores, not official SQuAD scores. SQL v2 produced
41 invalid envelopes, one truncated output, exact match 65.58% and F1 80.01%.
Different GPU kernels/batching can produce slightly different responses despite
the fixed seed and temperature.

The twenty-engine v3 run produced 41 invalid JSON envelopes and two truncated
responses, with 97.998% field-contract success, 65.576% answer exact match and
79.934% answer token F1. Median analysis length remained 91 words; only 2/2,048
met the requested word range. See [v3 quality](../ops/reports/squad7b-analysis-2048-sql20-v3-quality-2026-10-24.json).

The sixteen-sequence run had 36 invalid field-contract responses, two length
limits, 98.242% field-contract success, 65.381% custom answer exact match and
80.007% token F1. Median analysis length was still 91 words; only 1/2,048 met
the word range. This fixed analysis workload omits `response_format`, so the
new JSON-object guard is deliberately not activated in the comparison.

The requested 180–220-word analysis was usually not followed: valid baseline
responses had a 91-word median, and only 1/2,048 met the requested length. SQL v2
had a 92-word median and zero in that range. Therefore neither row represents
512 generated tokens/task or uniformly valid useful output. There is no
independent grading of evidence grounding or explanation quality.

The first JSON-object answer and translation profiles exposed additional failures:

| Original generation config, 16 slots/engine | Expected | Completed | Failed | Seconds | Accepted/s | Mean accepted output tokens |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Short answer, cap 96, target 700 input words | 2,048 | 2,045 | 3 | 30.574 | 66.888 | 14.59 |
| Chinese translation, cap 1,024, target 250 input words | 2,048 | 1,872 | 176 | 256.947 | 7.286 | 412.86 |

Both benchmark reports correctly set `passed=false`; their terminal failures
are preserved. Answer attempts included nine rejected JSON responses across
three tasks. Translation had 666 rejected responses, 283 tasks with multiple
attempts and 176 terminal failures. Accepted task/batch usage matched downloaded
unique outputs in both cases. These different profiles are not fixed-prompt
throughput comparisons: context target, instruction, actual tokens and output
caps differ.

The short-answer contract passed for 99.658% of all expected tasks, including
three missing outputs and four accepted JSON objects with incorrect field types
or missing required fields. Its custom exact match was 70.801%, token F1 84.420%.
The translation field contract passed for 91.406% of expected tasks; every
accepted translation passed that field check, but the 176 failures score zero.
All accepted translations stopped naturally. No independent translation fidelity
score is inferred from a valid JSON envelope.

The answer worker counters observed 10,530 prompt and 864 generated tokens in
rejected responses, separate from accepted usage. Counter creation timestamps
matched before/after on all twenty workers; this does not turn process counters
into a durable billing ledger. The console now shows exact failed/cancelled
counts whenever present, avoiding a misleading rounded `100.0%` success display.

For the original translation run, the twenty worker counters observed 666
rejected responses using 335,734 prompt and 337,090 generated tokens. Those
generated tokens equal 43.6% of the accepted generated-token total and are
excluded from accepted usage. The same isolated worker window observed 450
control-origin failovers without counter recreation. This is a significant
retry/recovery performance finding, not a successful reliability benchmark.
Attribution to a specific control-plane path requires additional profiling.

See [baseline quality](../ops/reports/squad7b-analysis-2048-baseline-quality-2026-10-24.json),
[short-answer results](../ops/reports/squad7b-answer-2048-json20-2026-10-24.json),
[answer rejected compute](../ops/reports/squad7b-answer-2048-json20-rejected-compute-2026-10-24.json).

## Backup recovery

The earlier schema-v2 PostgreSQL 17.11 dump was restored into a newly created empty Neon
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
[blocked run](https://github.com/yinli-systems/batchforge/actions/runs/40028627846).
Payments/invoicing, independent public ingress, scheduled off-host retention,
whole-Pod replacement, database failure-domain recovery and a sustained
10-million-task day have not been established. See [scaling analysis](scaling.md)
for the measured gap and requirements.
