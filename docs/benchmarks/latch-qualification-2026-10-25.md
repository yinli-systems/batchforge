# Preregistered live-latch recovery qualification

Status: **finite real fault qualification passed**. Serving API/worker source
stays748dc10. Operator sourceca3d8f4 is separate from that immutable snapshot;
its exact-head native CI passed1,502 Python and23 console tests.

The existing480B-total/35B-active MoE on the same fourH100s receives8 calibration
tasks followed by40 fault tasks. All use the already pinned public fixture's
128-token cell, fixed64-token output and seed20261025. This is a recovery trial,
not a quality score or a matched performance improvement. Existing tenant
retention headroom must cover48 tasks; limits and policies stay unchanged.

Both primary create/retry gates close and all previous work drains. Only the
same worker's operational inference timeout changes temporarily from1200 to30
seconds; source, model, runtime fingerprint, exact-token32, KV budget, leases,
database and storage policies stay fixed. Eight normal tasks must all finish
within20 seconds before proceeding. An authenticated loopback-only canary admits
the two fixed cohorts, each with an idempotent create replay.

A separate new-session guard binds a pidfd to the exact engine API-parent's
PID/start/cwd. Its armed handshake is mandatory before STOP. Actual running
inference must then be observed independently in worker reservations, engine
running-request metrics and durable running tasks. Only this application API
parent receives one bounded STOP with a45-second resume target, then CONT. The guard resumes
on its absolute deadline or caller EOF; logging failures cannot precede resume.
Native disposable-child probes must verify deadline and caller-death behavior
before this trial can run. Killing the guard itself or losing the whole Pod is
outside that mechanism's guarantee. This does not simulate a physical GPU fault.

The finite controller waits for actual latch plus no active worker reservations,
no DB-running tasks and an independently idle engine. The existing source-aware
recovery command takes fresh observations twice again, owns its recovery lock,
charges the persistent two/hour attempt budget and restarts only the unchanged
temporary-config worker. The parent holds the campaign lock throughout; the
helper verifies that live controller's exact launcher identity and open lock.
Accepted task identities, exactly-one accepted result and usage must reconcile;
physical inference may repeat. All48 tasks must finish with fixed output length.

After all accepted work and engine reservations drain, original configuration
bytes, admission gates and watchdog are restored and verified. Failure evidence
remains failed. No cancellation, replacement cohort, quota increase, cloud
operation, source overwrite or unobserved proxy is permitted. The independent
rescue is established before any pause, including if the parent later fails.

The baseline latch incident and original failed endurance report remain intact.
Native healthy refusal and unit tests alone do not qualify real recovery. A
successful finite trial also does not establish permanent automatic supervision,
whole-Pod failover or production high availability.

## Closed result and retained failure

The initial [v1 controller failure](platform480-2026-10-25/soak480-latch-qualification-20261025-v1-failed/initial-failure.json)
preceded all task submissions and fault injection. It mistook a portless
watchdog for an HTTP service, then recorded a restoration wait failure for that
same reason. Independent checks proved zero accepted batches, exact original
configuration bytes restored, ready APIs/worker and the live watchdog. The
failed report stays failed. The follow-up adds explicit managed-identity
validation for daemons; it does not invent a health endpoint.

The new [v2 trial](platform480-2026-10-25/soak480-latch-qualification-20261025-v2-complete/campaign.json)
ran18:58:32.695–19:00:19.913UTC,107.219 whole-controller seconds. Eight normal
calibration tasks finished; upload/create/replay/drain wall was15.427s, while
DB acceptance→completion P95 was5.059s. Independent disposable CPU-child tests
had already verified timer and caller-death resume without touching services.

Actual engine-running metrics, DB-running tasks and worker reservations were
captured before STOP. The pidfd guard resumed by deadline after **45.045s**;
the0.045s scheduler overshoot is retained, not rounded into a hard real-time
guarantee. After resume, all40 fault task identities were pending, zero DB tasks
were running, worker inflight/reserved/waiting were zero and the real bound
engine was independently idle. The [recovery command receipt](platform480-2026-10-25/evidence-api-worker-recovery-latch-qualification-20261025-v2-complete/validation.json)
contains the repeated fresh observations, source/proc-environment binding,
persistent intent, same-config worker restart and restored watchdog.

Detection at18:59:57.146 to completed recovery verification at19:00:07.991 was
**10.845s**. The accepted cohort delivered **48/48**,3,072 output tokens,
48 accepted attempts and32 earlier `kv_admission_timeout` retrying attempts.
No replacement batch/task identity was created. Physical inference may have
repeated; this proves accepted-result/usage conservation, not once-only compute.
The fault cohort's DB completion P50/P95/P99 were60.589/63.302/63.309s.
Different cohort sizes mean the8-task calibration is not a matched performance
baseline or a measured improvement. No output quality grade is inferred.

Original1200-second worker timeout, API gates and all backed-up configuration
bytes were restored with no restoration errors. Engine/gateway identities did
not change. The experiment lowered timeout to30 seconds only for qualification;
it does not prove a10.845s incident detection/recovery time for the original
long timeout, a physically failed GPU or a lost Pod.

The independent [storage/source audit](platform480-2026-10-25/evidence-api-latch-recovery-audit-20261025-v1-complete/validation.json)
verified two ready S3 archives,48 normally purged DB response payloads, all48
identities retained, strict accepted-attempt/usage receipts and all four distinct
input/output objects by authenticated byte count/SHA. Existing24h input and120s
archive policies remained fixed. Retained tasks are48,865/50,000, headroom1,135.
All evidence was allowlist-exported to the Mac and transfer-SHA checked.

At operator-confirmed$16.10 whole-Pod/hour, the107.218707s controller window
allocates **$0.479506** including maintenance and the injected pause. This is
not an invoice, answer-quality price, marginal cost or billing savings; the
later read-only archive audit is outside that controller window. See the
[source-bound allocation](latch-qualification-cost-allocation-2026-10-25.json).
Permanent finite-guard deployment remains the next reliability task.
