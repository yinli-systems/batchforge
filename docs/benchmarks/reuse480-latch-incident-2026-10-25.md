# Unplanned admission latch during the 3,600-task reuse study

The higher-arrival-rate study **did not remain continuously healthy**. A
recorded `engine_unavailable` attempt at 16:17:05 UTC was followed by
`kv_admission_paused`. The exact-token limiter deliberately latched rather than
assuming an uncertain remote invocation had freed GPU KV. After outstanding
requests finished, worker inflight, reserved tokens and engine requests were
zero, but admission stayed paused with **127 durable pending tasks**.

The worker process remained alive. The supervisor reports readiness but only
automatically restarts dead processes; it does not clear this safety latch.
API/database readiness and recent GPU averages therefore did not establish
execution health. The initial monitoring check omitted instantaneous worker
readiness/latch inspection and missed the transition. The completion plateau
then exposed the problem. This detection/recovery delay is retained rather than
removed from the workload wall time.

The [preserved observation snapshot](platform480-2026-10-25/soak480-reuse-20261025-v1-incident-recovery/evidence-manifest.json)
contains **203 all-four-device zero-utilization samples**, first at
**16:18:10.560 UTC**, last at **16:39:51.009 UTC**: a 21-minute-40-second span
between observed endpoints, not a claim of continuous measurement. Engine token
counters also stopped advancing. No samples were zero-filled. The
[sanitized read-only attempt diagnosis](platform480-2026-10-25/evidence-api-reuse-recovery-20261025-v2-diagnosis/sql-validation.json)
preserves the failure codes. The low-level HTTP transport exception was not
persisted, so it is not attributed to a particular TCP, proxy or server bug.

## Bounded same-source recovery

The first recovery launcher failed before service operations because the live
application root did not contain the newer `ops.product_campaign` helper.
[Its original source/log/failed preflight](platform480-2026-10-25/evidence-api-reuse-recovery-20261025-v1-failed/evidence-manifest.json)
were preserved. Version 2 imported the already qualified immutable operator
modules while explicitly controlling the actual live application root.
No executing source file or failed directory was overwritten.

The successful recovery ran **16:39:49–16:40:15 UTC** and:

1. Verified exact managed identities, unchanged worker source/environment
   hashes, zero database-running tasks, zero worker inflight/reservations, and
   two fresh quiescent-engine observations. Durable pending work was retained.
2. Paused the app-owned watchdog, restarted **only the same worker/configuration**,
   and revalidated its normal startup readiness. It performed no engine, API,
   model, schema, quota, cancellation, history deletion or cloud operation.
3. Verified completed tasks advanced **2,213 → 2,226**, all **127** pending task
   identities remained in the ledger, all other service identities were unchanged,
   and the watchdog restarted successfully.

The worker PID changed **574316 → 595638**; engine PID **573119** did not change.
The [closed recovery receipt and exact executed source](platform480-2026-10-25/evidence-api-reuse-recovery-20261025-v2-complete/evidence-manifest.json)
are SHA-verified on the Mac. At 16:44:24 UTC, a new actual GPU sample reported
98%, 98%, 100%, 99%; all three API/gateway readiness checks and worker readiness
returned 200. These are recovery observations, not an uninterrupted stress pass.

## Consequences and prevention

The original workload continues with the same input, idempotency identities,
source and configuration. It keeps the full outage in its timings and metering.
Even if every task later completes and the child delivery report passes, the
parent campaign's original managed-identity guard will detect the worker
replacement. **Do not bypass that guard or promote the run to a clean endurance
success.** The final delivery, retries, tails and recovery outcome need separate
classification. Fleet promotion still requires a reviewed successful qualification.

`ops.stall_watch.assess` adds read-only health classification from fresh worker
readiness, the instantaneous KV pause latch and live engine/worker counters.
It catches a paused queue even when historical GPU averages remain high. Stale,
missing or untyped telemetry cannot establish quiescence. It never restarts a
service or authorizes an unsafe limiter reset. Focused detector/release/source
tests passed **41** checks. Operational monitoring must read worker `/readyz`
and latch metrics directly, not substitute a PID or API 200 response.

A permanent bounded, independently quiescence-validated recovery path remains
follow-up work. Merely toggling the latch after a health probe would weaken the
safety invariant and is not implemented. This incident is evidence against a
production-HA or uninterrupted-stability claim.
