# Submission backpressure and storage growth

The original audit below describes schema v8. Its exact file hashes and line references are in
[the source audit](../ops/reports/storage-capacity-source-audit-2026-10-24.json).
Defaults below are code defaults, not a claim about the current deployment's
private environment. This document records source properties and real isolated
PostgreSQL tests, not a throughput, physical-storage or storage-cost qualification.

## v9/v10 admission: deployed and checked

Schema9 and10 were committed together on2026-10-24 at03:44:43UTC from source
checkpoint `5448ee8aabe2d55b8575894c6a39f0dd6d340221`. Both API replicas then
rolled with retention enabled; the watchdog was restored by03:44:49UTC. The
[executed deployment report](../ops/reports/storage-admission-live-deployment-2026-10-24.json)
(SHA `1ae36514cfb4dcc6d4e47ba04400c0915fc717d71845040b82bd9d41867e3995`)
records zero retained-ledger, undercharge, missing-ledger and auxiliary-ledger
mismatches, identical old20 downloads on both APIs/gateway, and unchanged
engine/worker/gateway identities. The clean11-file bundle SHA was
`f344c1420f69826d9a3c8e45ef41aa4e70c3e9ebc20e9f9b4116c739a20e00ee`.
This qualifies the executed rollout, not indefinite storage capacity.

The subsequent [API policy roll](../ops/reports/final-api-policy-live-deployment-2026-10-24.json)
completed at 03:55:58 UTC, preserving the inference pool, profile, gateway and
all other API settings. The publisher is now enabled only for batches created
after **2026-10-24 03:59:21.961089 UTC**, with a minimum terminal age of 120
seconds and purge pages of at most 1,000 tasks. Historical batches remain outside
that cutoff. Publication is asynchronous: the age threshold does not promise
that credits are released exactly 120 seconds after completion.

Before activation, a new schema10 dump and an
[actual isolated PostgreSQL 17.11 restore](../ops/reports/schema10-postgres17-local-restore-2026-10-24.json)
passed, including all four storage ledgers; the temporary cluster was stopped
and removed. The new schema10 dump's **off-Pod copy is not verified**: automatic
approval rejected that export and explicit user authorization is pending.
The earlier schema8 dump was separately verified off-Pod. These are distinct
recovery proofs and do not establish complete off-site disaster recovery for
the current schema.

Production policy for tenant`default` is50,000 retained tasks,320MiB charged
task payload,1MiB per response and32KiB per task/attempt error. The generic
new-tenant defaults below remain100,000/1GiB. V10 additionally bounds retained
input files including future-file reservations to2,000; uploads32;
idempotency4,096 rows plus128MiB reserved responses at64KiB/key; blob
generations2,000; archive runs2,000; chunks20,000; and each reader class128.
All states and tombstones count until actual authorized row retirement. Output
and error files are bounded by retained batches without an auxiliary quota lock
on completion. See [the separate v10 contract](auxiliary-retention.md).

The [production read-only ACL inventory](../ops/reports/storage-admission-production-lowpriv-2026-10-24.json)
checks the source database's actual privileges separately from restore tests:
`bf_runtime` and PUBLIC cannot execute any of the five private auxiliary
security-definer functions; runtime cannot change their policies/counters or
delete retained task/history rows and has no owner/DDL/key-management ability.
V9 derived usage intentionally retains an UPDATE ACL for nested invoker
triggers, protected by its enabled outer-write guard. No destructive negative
permission test was run against production.

[Migration 9](../backend/migrations/0009_retention_admission.sql) adds durable,
operator-owned per-tenant limits and derived usage counters. Every new task is
charged its canonical JSON body bytes, a fixed maximum response reservation and
`(max_attempts + 1) * max_error_bytes` for its task error plus attempt errors.
The defaults are 100,000 retained task rows, 1 GiB charged payload, a 1 MiB
response cap and a 32 KiB cap on each error. Attempts are bounded to 1–20 per
task. Counters include active, completed, failed and cancelled tasks; leaving the
live queue does not return these credits. Input/task/attempt identity and
accounting fields cannot be changed to evade the reservation.

The database trigger admits or refuses each insert in the same transaction as
its batch and idempotency record. A quota failure is HTTP 429 and rolls back the
whole batch. Response or error bytes above the task's original reservation are
HTTP 413 and do not accept the completion/failure. Policy is stored in PostgreSQL;
an API environment cannot widen it or switch off installed enforcement. The
optional API flag only requires schema 9 at readiness. A policy reduction does
not revoke a previously accepted task's response reservation.

Verified v8 publication continues unchanged. Its completed-payload purge now
also releases the body and response reservations, and converts the unused error
allowance into the exact retained task/attempt error bytes. Error history is
then sealed and purged bodies/results cannot be refilled. Failed and cancelled
payloads keep their reservations. A full archive quota therefore eventually
causes **new submission rejection**, while reserved completions and historical
reads remain available. This closes the v8 task-payload growth path described
below; it does not add automatic archive expiry or history retirement.

Creation takes tenant → usage ledger → queue/batch/task locks. Existing v8 purge
takes tenant → batch/task → usage ledger. Every ledger writer first holds the
same tenant umbrella, so the late purge ledger lock cannot race another writer
for that tenant. Completion only checks its existing task-local bounds; it adds
no tenant or usage lock or database round trip. Migration bootstraps existing
rows from actual canonical JSON bytes, reserves future response/errors for
active rows, and permits an over-limit historical balance. New inserts then
fail until publication or an operator frees sufficient credits. The
[SELECT-only estimate](../ops/ha/retention_bootstrap_estimate.sql) reproduces this
bootstrap without migrating or changing data.

Retained task counts are released only by an explicit, owner-authorized cleanup
transaction. The release does not automatically delete histories or repair
batch/file/idempotency/archive references: a complete coherent retirement
operator is still needed. Normal runtime credentials cannot change limits,
forge counters, authorize deletion or delete task/attempt history. This release
does not retire historical metering/audit records automatically.

The defaults deliberately fail closed well before the task-count cap for a
large unpurged workload: with three attempts, each new task reserves at least
`1,048,576 + 4 * 32,768 = 1,179,648` bytes **plus its body**. Thus a 1 GiB budget
holds at most 910 such unpurged tasks, fewer with real bodies. At 10 million/day
and a minimum publication age of 120 seconds, these minimum reservations alone
would require about 15.26 GiB before archive latency, bodies or errors. Even
after successful purge, a 100,000 retained-task limit would be reached in 14.4
minutes at that arrival rate without explicit retirement. These are arithmetic
bounds, not observed throughput or a claim that the defaults support that rate.

Logical JSON charge is **not** PostgreSQL physical database/TOAST/index/WAL size.
It excludes batches, files, idempotency responses, object generations, archive
runs/chunks, reader leases, and other metadata. Their separate admission work
and physical-storage headroom must be qualified independently. A generic 1 GiB
logical default must not be equated with a project's 1 GiB physical quota.

The new isolated real-PostgreSQL tests cover two-API last-slot races, full-batch
rollback, installed enforcement with the flag off, reserved completion while
tenant and usage rows are blocked, oversize rollback, low-privilege writes,
bootstrap over-limit adoption, archive quota exhaustion, create/purge and
delete/purge races, attempt-error/purge races, and mixed terminal retention.
They do not claim production migration or steady-state cleanup capacity.

## Original v8 admission audit

The HTTP middleware caps each upload and other write request before routing.
JSONL validation caps file bytes, row count, row bytes and requested output
tokens. Defaults are 10 MiB per upload, 10,000 rows, 128 KiB per row and 4,096
output tokens; all are operator configurable. The internal completion request
also passes the general write-body cap: `2 * max_request_bytes + 65,536` bytes,
320 KiB at the default. This is a byte limit on one HTTP request, not a tenant's
aggregate completed-result budget. The worker checks reported generation/context
token bounds, but reads the engine response with a normal buffered HTTP request;
there is no separate streaming byte ceiling on that response before JSON parsing.

For external inputs, `BlobCatalog._reserve` locks the tenant and usage row, then
checks stored plus reserved input bytes and active plus held task credits before
object I/O. `admit_batch` consumes upload task credits in the task-insert
transaction. `Store.create_rows` also serializes admission on the tenant row and
rejects pending plus running work above the tenant queue limit. Concurrent
submissions cannot independently spend the same credits. Defaults are 256 MiB
of input file storage and 100,000 pending/running task slots per tenant. Storage
quota refusal is returned as HTTP 429. The inline input backend bypasses the
external input byte/reservation quota; its batch queue bound still applies.

External input expiry releases quota only after no referring batch is active;
bounded GC honors upload generations and live read leases. Expiry clears the
file's blob reference, then eventually deletes its object. It retains the SQL
file metadata and generation tombstone. These maintenance jobs require their
enable flag and a healthy storage backend; configured retention is not a promise
that cleanup will finish on time during an outage.

## Original v8 publisher boundary

The result archiver reserves output bytes under a tenant usage lock before each
object PUT and limits stored plus reserved archive bytes; the code default is
1 GiB per tenant. It verifies chunk identity, concatenated file checksum/count
and an immutable ready manifest before purging a bounded page of completed
`tasks.body` and `tasks.response`. Database-reader leases prevent premature
purge. Each call purges at most `result_archive_purge_rows` completed tasks
(default 1,000, maximum 10,000). This is a meaningful safe payload offload.

**The archive quota does not reject new batch submissions or reserve future
output space.** It is checked when background archival writes a chunk. The
upload/create paths check input bytes and live task counts; completion accepts
the result into PostgreSQL without consulting `tenant_result_usage`. If an
archive quota is full, the publisher records an error and retries later while
accepted database responses remain intact. Ready archives have no automatic
expiry, and archives whose purge has started cannot be discarded through the
unpurged repair method. Without a separately implemented lifecycle, raising a
quota only postpones exhaustion.

Consequently, bounded input bytes, bounded live queue length and bounded archive
bytes do **not** bound accumulated database payload. The same retained input can
be used by more than one batch; each batch copies task bodies into PostgreSQL.
As completed work leaves the live queue, more work can be admitted even when its
previous results cannot be archived. A tenant can remain within both submission
bounds while its unarchived responses and duplicated task bodies keep growing.

Even successful publication leaves these histories growing with accepted work:

| Data | Current lifecycle |
| --- | --- |
| Completed task rows and attempts | Rows, IDs, counts, accepted usage and attempt history remain; only completed task body/response are cleared. |
| Failed/cancelled tasks | Their task bodies and error payloads remain, including after the error JSONL is archived. |
| Batches, files and idempotency responses | No age/count deletion policy in the inspected implementation. Per-request metadata is bounded by HTTP size, not by total retained history. |
| Input blob generations | Deleted objects retain SQL generation fences; input file rows are retained after expiry. |
| Result archive runs/files/chunks | Ready objects have no expiry; failed/deleted generation and chunk metadata remain as fences. Repeated failed archival can add generations even without new submitted work. |

The publisher is also a bounded maintenance loop: each API selects at most four
eligible batches, processes them serially, then sleeps 25–35 seconds. A batch
may need several later visits to finish its purge. Slow object I/O, active
readers, quota errors and eligibility scans all add delay. This bounds work per
sweep; it does not establish that the service can keep up with its accepted
result arrival rate. There is no submission gate based on unarchived bytes,
oldest unarchived age, a database high-water mark or a global storage budget.

## The ten-million-per-day boundary

Ten million tasks per 24 hours is an average **115.7407 accepted completions/s**,
before retries, heartbeats, claims, readers and archival work. With at least one
attempt per task and no history deletion, that adds at least 10 million task
rows and 10 million attempt rows per day even after every completed payload is
offloaded. This is row arithmetic, not a measured byte footprint or throughput.

The SQL completion path already uses one autocommit `SELECT
batchforge_complete(...)`; it does not hold locks across a sequence of Python
SQL requests or client `BEGIN`/`COMMIT` calls. Internal worker authentication is
an in-memory credential comparison. It would be incorrect to describe this
enabled path as the old many-WAN-round-trip completion transaction.

The remaining contention is concrete: completion updates the worker row, locks
the tenant/model queue row, locks the batch, then locks the task and atomically
updates task/attempt/batch usage. Concurrent completions and heartbeats sharing
a worker serialize on its row; workers serving the same tenant/model also meet
the same queue row; a large hot batch shares one batch counter row. Server-side
claim/fail/recovery paths share these lock-order conventions. Additional API
replicas do not shard those PostgreSQL rows. The SQL/Python feature flags matter:
the fallback completion path still makes multiple awaited SQL statements inside
an explicit transaction.

A single dominant serialized key would need less than 8.64 ms of average
serialized server work per accepted completion to sustain 115.7407/s, with less
headroom once competing operations are included. That is a necessary arithmetic
condition, not an observed lock duration or a benchmark result. WAN RTT alone is
not the lock-held time of the one-statement path; independent concurrent calls
can overlap network transit. The inspected code alone cannot prove either that
115.7407/s is attainable or that it is impossible.

## Remaining implementation priorities

1. Operate the deployed v9/v10 limits with measured cleanup lag and conservative
   physical-storage headroom. Expose oldest pending archive and reserved payload
   bytes. Per-tenant task and auxiliary limits do not form a global physical
   database/WAL high-water gate; operator-created tenants/keys/workers and logs
   require their own lifecycle. Do not raise limits automatically on exhaustion.
2. Define a customer-visible result/history retention and idempotency replay
   horizon. Implement bounded/partitioned retirement of tasks, attempts,
   batches/files and result objects while preserving a compact metering ledger
   and safe stale-token/replay behavior. Treat generation fences separately:
   deleting them blindly would lose the late-SDK-write/deferred-delete safety
   already implemented. Cover failed/cancelled task bodies as well as success.
3. Move publication into explicitly provisioned maintenance capacity, measure
   backlog drain versus arrival, and qualify a disposable real-PostgreSQL
   control-plane load at 115.7407 completions/s plus realistic claims,
   heartbeats, quotas, readers and archive traffic. Vary one hot tenant/batch
   versus many tenants/workers. Measure server lock waits/WAL/pool occupancy
   before changing lock granularity or sharding counters. No new load test was
   run for this audit.
