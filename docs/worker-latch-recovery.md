# Source-aware, bounded worker-latch recovery

The 3,600-task study exposed a live exact-token worker that stayed paused after
an uncertain engine invocation. Process liveness did not recover execution.
`ops.recover_latched_worker` is a concrete native command for guarded same-source
recovery; it is **not deployed or fault-qualified yet**, and is not an installed
automatic watchdog. The current fixed 800-task study remains untouched.

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
A future finite health guard can invoke this command after detection; that
integration and qualification remain separate work.

The recovery endpoint must exactly match the worker's actual single private
inference URL and metrics host/port. A quiet manifest engine cannot authorize
recovery of a worker pointed at a different node, path or unobserved proxy.
A follow-up rejects that mismatch before observations or any service signal;
seven endpoint-binding regression cases cover this safety boundary.

The live owned worker process environment is also checked against every pinned
worker-file key, before observations and after restart. Missing, stale or
duplicate effective keys fail without logging values. A file-only source probe
cannot establish what an already running worker loaded. The endpoint/effective
configuration follow-up passed93 related local tests; it is not deployed.
