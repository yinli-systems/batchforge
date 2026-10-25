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

## Closed v2 outcome

Sourcea2732e4 passed actual CI
[40235466421](https://github.com/yinli-systems/batchforge/actions/runs/40235466421)
with1,604 Python/zero skips and23 Node tests. The new immutable canary then
delivered **20/20 Agent runs through44 real model steps**,44 accepted attempts
and44 observed attempts. Accepted usage was21,734 prompt and1,693 completion
tokens. The coordinator's injected post-ACK death recovered the original Batch
ID after the120s lease expired; it did not create a second child. Agent event
chains and results survived the private Agent API restart. All88 child artifact
reads passed byte/SHA checks after normal archival/purge. All six primary
identities stayed unchanged; the test API/coordinators/PG stopped, data files
were preserved, and the finite primary guard returned armed with zero actions.

**Strict quality failed:16/20**, with all four poisoned-document cases failing
exact answer text while still completing memory retrieval and verified citations.
Private diagnostics found the expected code literal inside extra answer text;
that observation is not extraction/repair and does not change their failed
grade. No ungranted tool was executed. Delivery, safe capability enforcement,
citations and recovery do not turn this into a passed quality cohort. The
parent report remains `quality_failed`, `passed:false`.

The394.059s whole-controller window allocates$1.762319 at the confirmed whole-Pod
rate. It includes native setup, coordinator failure/lease waiting and artifact
audit; no invoice, marginal cost, saturation capacity or qualified-cohort price
is inferred. This first pilot did not export per-Agent completion-time samples;
do not invent Agent P95/P99 from the platform's earlier batch studies. Native
Agent state is still container-local qualification state, not Azure Agent
production persistence. Remaining retained-task headroom was243/50,000.

The [source-bound summary](../../ops/reports/agent-runtime-v2-summary-2026-10-25.json)
and [unmodified closed report](platform480-2026-10-25/soak480-agent-runtime-20261025-v2-closed-quality-failed/report.json)
retain the failure. Next: enforce an explicit generic output value contract with
budgeted model correction and frozen new heldout cases. Do not use the expected
answer to parse or repair output, loosen this grade, or overwrite this trial.
