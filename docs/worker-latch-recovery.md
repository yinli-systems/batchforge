# Source-aware, bounded worker-latch recovery

The 3,600-task study exposed a live exact-token worker that stayed paused after
an uncertain engine invocation. Process liveness did not recover execution.
`ops.recover_latched_worker` is a concrete native command for guarded same-source
recovery. A finite real480B transport-fault trial has now qualified this command;
it is now used by an installed **finite guard ending09:00UTC2026-10-26**;
it is not a permanent service or whole-Pod failover. The earlier800-task
study closed independently and its evidence stays unchanged.

The command is prepare-only without `--apply`. It requires the current480B TP4
manifest, source-selected safe-path worker, pinned runtime fingerprint,32 slots,
164,256-token budget and18,440 context. It probes actual module origins from the
worker's selected immutable source, never the obsolete `/root/app/worker` files.
It hashes the manifest, all service configurations, current source receipt and
live manager sources, and checks exact managed PID/start/cwd identities.

Recovery needs a live503 worker with its KV latch set, no worker inflight,
reserved or waiting tokens, zero DB-running tasks, retained pending identities,
exact advertised model/version and two fresh independent quiescent-engine
observations. It checks again after pausing the owned watchdog. Healthy workers,
stale/missing/nonfinite telemetry, unknown versions, busy engines, any active
lease or changed source/configuration are refused. No limiter is simply reset.

A persistent fsynced attempt intent is charged before any signal: at most two
attempts per hour, including unsuccessful attempts. Corrupt/future budget data
fails closed. Only the same worker is restarted, with unchanged configuration;
engine, API, model, quota, retention and cloud actions are excluded. Pending IDs
must remain present and at least one must reach completion within the bounded
verification interval. Watchdog restoration is attempted even if a journal step
fails; a source/config drift can block unsafe restoration and keeps the result
failed. Clean process exit alone is not a successful recovery receipt.

An active fixed-study exception requires its stable live launcher identity and
open campaign-lock descriptor, plus the exclusive recovery lock. Other mutation
controllers must take that same recovery lock. The helper does not take over
unidentified controllers or guess PIDs from command lines. It never cancels
accepted tasks or invents a replacement job identity. A recovered worker changes
the experiment's process identity; the original continuity failure is retained.

Local fault-ordering and invariant tests cover late quiescence loss, budget
persistence/failure, source drift after stop, manager failure, missing progress,
journal failure and watchdog recovery failure. Native PostgreSQL17 CI and a
separate real fault-injection qualification are required before enablement.
The [actual48-task qualification](benchmarks/latch-qualification-2026-10-25.md)
passed with independent engine resume, same-config worker recovery and strict
ledger/S3 verification. A future finite health guard can invoke this command
after detection. The [finite integration and800-task study](benchmarks/guard-endurance-2026-10-25.md)
have since passed; permanent service/whole-Pod failover remain separate work.

The recovery endpoint must exactly match the worker's actual single private
inference URL and metrics host/port. A quiet manifest engine cannot authorize
recovery of a worker pointed at a different node, path or unobserved proxy.
A follow-up rejects that mismatch before observations or any service signal;
seven endpoint-binding regression cases cover this safety boundary.

The live owned worker process environment is also checked against every pinned
worker-file key, before observations and after restart. Missing, stale or
duplicate effective keys fail without logging values. A file-only source probe
cannot establish what an already running worker loaded. The endpoint/effective
configuration follow-up passed93 related local tests. It is included in the
finite guard source; serving API/worker modules remain748dc10.

## Actual native healthy-worker refusal

Immutable candidatef1caf9a passed a read-only native check on the Pod: actual
engine/metrics binding, definition-only module origins and every effective
worker-file key matched. The command then refused the healthy200/unlatched
worker before any service signal; all six managed identities stayed unchanged.
The [driver, source pins and actual observations](benchmarks/platform480-2026-10-25/evidence-api-worker-recovery-candidate-20261025-v1-native-healthy/validation.json)
are exported with transfer SHA verification. This verifies healthy-state refusal,
not a successful recovery, installed daemon or fault-injection qualification.
The serving worker/API remain immutable748dc10.

## Actual finite fault recovery

Operator sourceca3d8f4 admitted8 calibration plus40 fault tasks. An independent
pidfd guard resumed the exact engine API parent after its45-second target
deadline (observed45.045s). All40 fault tasks were retained pending when the
worker became safely latched. The controller automatically invoked the command;
fresh engine/DB/worker quiescence, actual source and effective configuration
checks passed. Detection to completed recovery verification was10.845s.
All48 tasks delivered64 output tokens, with48 accepted attempts and32 earlier
timeout attempts. Both strict ledger receipts and all four input/output S3
objects subsequently passed independent byte readback after normal DB purge.

The temporary operational timeout was30 seconds; original1200-second bytes,
primary admission gates and watchdog were restored. This does not demonstrate
the same detection latency under the original long timeout. The existing
two-attempt/hour persistent budget consumed one intent; no limiter was reset.
The first controller run failed before submissions due a portless watchdog
readiness bug; its failed report and zero-acceptance/restoration proof remain.
No serving module source, model, cloud resource or storage policy changed.

The observer passes a state SHA through `--expected-state`; the native helper
compares source/config/PID state after both locks. The guard needs two fresh
quiescence observations and never adopts a foreign epoch. Healthy polls do no
DB scans. Exhausted budgets wait for natural expiry; corrupt/future/missing
required ledgers stay closed. A helper overrun remains an identified independent
child, never KILLed between worker stop and watchdog restoration. The real guard
trial and clean800-task study passed; normal1200s-profile deployment is finite.
