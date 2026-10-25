# Finite source-aware guard qualification and workload

Status: preregistered; implementation/local tests are not deployment or a real
guard-triggered recovery result. The earlier48-task command qualification stays
closed and independent. Serving API/worker source stays748dc10, model480B-total
/35B-active MoE on the existing fourH100s; no cloud resources or plans change.

`ops.worker_latch_guard` is prepare-only unless `--apply`. One guard owns a
separate lock, has an absolute deadline no later than09:00UTC2026-10-26 and a
monotonic lifetime bound, and starts no recovery in its last600 seconds. It can
invoke recovery at most twice during that lifetime. The existing native helper
also charges its persistent two/hour budget, including failed intents. Neither
budget is reset. Graceful stop lets an identified in-flight native helper finish;
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
