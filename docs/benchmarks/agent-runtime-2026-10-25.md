# Real Agent runtime qualification protocol

This is a **preregistered synthetic Agent correctness and recovery canary**,
not a throughput/saturation benchmark, hidden benchmark or Azure Agent rollout.
Twenty cases cover8 arithmetic,4 memory,4 memory→calculation, and4 poisoned
memory documents. The frozen source fixture defines expected answers and actual
required tool results; matching a final answer without invoking the required
tool does not pass. Gold values are used by grading, never output parsing or
repair. Capabilities remain enforced even if the model follows an attack.

Each run may reserve at most4 model calls,3 tools and2,048 generated tokens,
with1,200s wall deadline. Thus no more than80 existing-tenant Batch tasks can be
admitted. Check fresh retained-task and conservative payload reservation
headroom first. Do not change quotas, create another production tenant or delete
history. The primary Batch API keeps its admission gates and retention policy.
The current engine/model/worker configurations stay unchanged.

The new Agent schema has not been migrated into Azure because the configured
runtime role cannot create schemas and no protected Azure owner credential has
been located. Qualification therefore uses a native PostgreSQL17 Unix-only
cluster **on the existing container disk for Agent state only**. It is private,
test-only and stopped after the trial; files are preserved. Its state is not
whole-Pod durable. Model steps and accepted metering still traverse the actual
existing Batch API, Azure and S3, using the real480B MoE/fourH100 service.
No paid inference API or additional cloud resource is created. Do not report
this as an Azure Agent production migration.

The private loopback Agent API receives all20 run creations, verifies same-key
replay, and serves the final state/events. Four finite coordinator processes
consume those durable runs. Exactly one coordinator exits itself immediately
after a real child Batch ACK and before the Agent commit. Its replacement waits
for natural lease expiration, replays the original child key/file/body and must
reconcile the same Batch ID. No engine or existing worker is paused/restarted.

Verify every Agent event chain from genesis through bounded API pages. Restart
only the private canary Agent API and read back identical event heads/results.
Read every child Batch strict receipt and reconcile usage/accepted identities;
wait for normal S3 archival/DB-payload purge, then byte/SHA-read every child
input/output artifact. Existing primary identities must not change. Raw model
responses, executor keys, DSNs and test database dumps are not exported.

The live finite GPU guard is left unmodified and waits at the unknown campaign
lock boundary. Do not claim automatic repair coverage for this new campaign.
An unexpected stop closes producers and preserves failure before diagnostics;
never blindly replay the canary directory or its accepted Agent runs. The
control DB/API/coordinators are the only new **application processes**, on the
already running Pod. A later true Azure deployment needs owner migration,
grant/compatibility checks, private canary qualification and retained rollback.

The controller requires at least2,100s before09:00UTC2026-10-26. It bounds main
execution at1,500s plus archival/readback and cleanup. Report answer correctness,
required tool correctness, citations, safe capability enforcement, delivery,
coordinator recovery and storage separately. Cost is measured whole-controller
wall at the user-confirmed$16.10/Pod-hour; it includes waiting and audit and is
not an invoice, marginal cost or a large-scale Agent capacity estimate.

Source: `ops.qualify_agent_runtime`; execute only an immutable source receipt
whose actual CPU CI passed. The schema/API/harness code is separate from the
still-serving748dc10 primary. This protocol alone is not a passed GPU result.

The first native canary (`soak480-agent-runtime-20261025-v1`, source90289ea)
failed its initial managed-identity check: a duplicated identity convention used
`watchdog-primary.json` while the actual portless daemon uses
`supervisor-primary.json`. It failed before connecting Azure, starting the test
database/API or accepting any GPU task. The original failed report and separate
diagnosis are preserved. The successor reuses the already qualified
`ops.worker_latch_guard.Pins` for all six actual source/config/process identities;
it requires a new source receipt, CI qualification and run directory. No original
failure is rewritten as success.
