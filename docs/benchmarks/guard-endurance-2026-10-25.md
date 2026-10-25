# Finite source-aware guard qualification and workload

Status: **real guard-triggered qualification, finite deployment and800-task
endurance all passed**. The earlier48-task command qualification stays
closed and independent. Serving API/worker source stays748dc10, model480B-total
/35B-active MoE on the existing fourH100s; no cloud resources or plans change.

`ops.worker_latch_guard` is prepare-only unless `--apply`. One guard owns a
separate lock, has an absolute deadline no later than09:00UTC2026-10-26 and a
monotonic lifetime bound, and starts no recovery in its last600 seconds. It can
invoke recovery at most twice during that lifetime. The existing native helper
also charges its persistent two/hour budget, including failed intents. Neither
budget is reset. A read-only budget peek waits for natural hourly expiry instead
of spawning doomed helpers. Corrupt/future data stays closed; a missing required
ledger cannot silently create a fresh guard budget. Graceful stop lets an identified in-flight native helper finish;
a360s helper overrun is retained as an identified independent child, not killed
between worker stop and watchdog restoration or resubmitted under a new ID.

The guard pins current source/module bytes, every file configuration and actual
owned worker environment, all six managed identities, manifest and live manager
bytes. It never adopts a foreign epoch. After its own successful recovery only
the reported worker and owned watchdog may change identity. Healthy samples
need no database scans or engine queries. Missing/nonfinite telemetry is unknown,
never zero; repeated unavailable observations terminate without blind repair.

Two fresh independent latch/quiescence observations are required before invoking
the helper, which independently repeats its native checks. The guard passes a
SHA256 of its pinned state; the helper compares the full state **after obtaining
both recovery/campaign boundaries**. A race changing source/config/PID therefore
refuses before charging an intent or signaling. The guard waits for unknown
campaign owners. An explicit fixed-study attachment requires that campaign's
live exact launcher and open lock descriptor. Recovery changes its worker epoch;
original workload continuity failure must remain failed even after delivery.

Qualification stages, in order:

1. Native healthy/no-mutation: three observations with recovery permission and
   at least600s action margin, then close. All six identities, environment bytes
   and persistent budget must stay unchanged, with zero helper invocations.
2. A new48-task app-transport trial uses the previous8+40 public64-token-output
   protocol and independent pidfd engine-parent resume. The parent temporarily
   sets30s inference timeout, freezes the profile, arms the actual finite guard,
   and releases the recovery lock while retaining the explicit campaign lock.
   The guard must observe healthy state before the fault and autonomously detect
   and recover the real latch; the parent provides no fallback restart. Original
   configurations restore only after the guard and all accepted work close.
   Exact receipts, accepted-result usage, identities and normal S3 archive/byte
   readback must pass. This is not physical GPU failure or a latency guarantee
   for the original1200s timeout. One19:00UTC intent is already budgeted.
3. Only after real qualification, enable a finite guard on the restored normal
   profile. Native preparation/healthy proof alone is insufficient. Keep its
   exact source, launcher, deadline, state hash and budget inspectable.
4. Preregister one800-task/40-batch mixed fixed-output workload, offered1task/s,
   max128 in flight,5s GPU/API/engine/DB samples,3600s admission plus1800s drain.
   Reuse the already retained public20-cell input without uploading; verify
   owner/SHA/bytes/TTL and fixture multiset again. Forty batches plan544,000
   output tokens. After the48 new qualification tasks, the last known1135 task
   headroom would leave1087; this workload must recheck actual headroom and byte
   capacity, never raise limits, create another tenant or retire history.

The post-Fleet800 study is a reference with matching fixture/offered-rate/size,
not randomized repeated ABBA evidence for guard overhead or a significant
speedup. Record DB-acceptance and scheduled-arrival P50/P95/P99 separately,
whole-wall token throughput, device-wide utilization and sample gaps. A clean
workload proves observation during healthy execution; the separate fault trial
tests recovery. Failed epochs and cohorts stay preserved. Use the confirmed
$16.10 whole-Pod/hour allocation, not invoice or marginal/quality-answer cost.
All evidence is allowlist-exported and SHA-checked; no raw tenant outputs,
credentials or weights leave the Pod. This does not establish whole-Pod HA.

## Actual qualification and finite deployment

Sourceb80391c passed native PostgreSQL17.11 CI: **1,542 Python tests,0 skips,
23 console tests**, Node24.22.1/UID65534, all one-job runners deregistered.
Its [healthy native proof](platform480-2026-10-25/evidence-api-worker-latch-guard-candidate-20261025-v2-native-healthy/validation.json)
ran three observations with recovery permission and full time margin but zero
helper invocations. All six identities/configuration bytes/budget stayed fixed.
The locked helper matched the exact observer state SHA before healthy refusal.
Earlier3c7cf02 CI passed1,536 tests. Budget waiting adds no reset or replay.

The [v3 real trial](platform480-2026-10-25/soak480-latch-qualification-20261025-v3-complete/campaign.json)
delivered48/48,3,072 accepted output tokens,48 accepted attempts and32 earlier
timeout attempts. The finite guard autonomously recovered40 pending identities
with no parent fallback. Its two quiescence observations were19:29:48.111 and
19:29:55.890UTC; the helper's locked state SHA matched. First quiescence
observation→verified progress was17.819s, including polling/native checks.
The45s resume target measured45.004s; one paused-engine ReadTimeout is retained.
Guard reports save state changes/counters rather than every healthy poll; the
separate three-observation proof is not a full raw poll trace.

Original1200s timeout/API gates/watchdog were restored without errors.
The [independent audit](platform480-2026-10-25/evidence-api-latch-recovery-audit-20261025-v2-complete/validation.json)
verified48 normal DB payload purges, identities retained and all four S3
input/output objects by bytes/SHA. This is an app-transport fault under temporary
30s timeout, not physical GPU failure or original-timeout RTO qualification.

At19:35:56UTC the [normal-profile finite guard startup](platform480-2026-10-25/evidence-api-worker-latch-guard-candidate-20261025-v2-enabled/passive-preflight.json)
passed: PID642163/start48039170, services/budget unchanged. It ends09:00UTC
2026-10-26 with an08:50UTC action cutoff. Two recent qualification intents
initially exhausted the hourly budget; it waits for natural expiry. Two lifetime
invocations and two intents/hour both apply. Startup is not a completed lifetime.

## Closed800-task workload

The [closed study](platform480-2026-10-25/soak480-guard-endurance-20261025-v1-complete/run/report.json)
ran19:37:42.617–19:59:10.848UTC: **800/800,0 failed/retried**,800 accepted
attempts, exact output lengths and conserved usage. Model/source/configuration
and all app epochs stayed fixed; zero guard invocations. Four cards each had200
samples, mean96.395/96.905/95.985/82.000%, all max100;0 observation errors,
max gap7.326s and no gap over2×5s. These are device-wide activity samples,
not FLOP saturation measurements.

| Measure | Earlier post-Fleet800 | Finite guard800 |
| --- | ---: | ---: |
| Whole wall, seconds | 1270.536 | 1288.223 |
| Accepted output tokens/s | 428.166 | 422.287 |
| DB acceptance→completion P95, seconds | 250.846 | 255.147 |
| DB acceptance→completion P99, seconds | 258.444 | 261.885 |
| Scheduled arrival→completion P95, seconds | 431.547 | 456.771 |
| Scheduled arrival→completion P99, seconds | 488.814 | 503.539 |
| Whole-Pod allocation at$16.10/h | $5.6821 | $5.7612 |

The fixture bytes/order,800 tasks and offered1task/s match, but this later
single observation is not randomized repeated ABBA or a causal overhead
estimate. Throughput was1.37% lower; this does not isolate the guard's cost.
The raw inherited comparison label refers to older differently offered studies;
for this direct800 reference input/order/rate match. Raw reports stay unchanged.
Different quantile populations must not be subtracted as a per-task breakdown.

The [closed archive/guard audit](platform480-2026-10-25/evidence-api-guard-endurance-readback-20261025-v1-complete/validation.json)
found40 ready S3 archives,800 normal DB response purges and800 retained identities.
Indices0/20/39 passed strict receipts and six input/output byte reads: four
independent objects because input is reused, not all40 outputs. The finite guard
remained armed with zero invocations. Input24h/archival120s and limits/plans
stayed fixed. Retained49,713/50,000 leaves287 tasks; agent work must respect
that headroom. Allocation is not invoice, marginal or answer-quality cost;
fixed-output compute has no semantic quality grade.
