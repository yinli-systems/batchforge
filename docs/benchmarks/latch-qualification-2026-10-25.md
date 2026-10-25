# Preregistered live-latch recovery qualification

Status: prepared, not yet executed or qualified. Serving API/worker source stays
748dc10. Operator code is separate from that immutable service snapshot.

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
parent receives one bounded STOP, at most45 seconds, then CONT. The guard resumes
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
