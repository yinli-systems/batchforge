# Architecture and correctness

```mermaid
flowchart LR
  Client[Private console / API client] --> G1[Pod 1 private gateway]
  G1 --> A1[Pod 1 API replicas]
  G2[Pod 2 private gateway] --> A2[Pod 2 API replicas]
  A1 --> PG[(Managed PostgreSQL durable state)]
  A2 --> PG
  W1[Pod 1 workers] --> G1
  W2[Pod 2 workers] --> G2
  W1 --> E1[H100 inference engines]
  W2 --> E2[Blackwell MIG inference engines]
```

## Durability

PostgreSQL is the source of truth for input files, batches, tasks, attempts and
usage. A successful submission commits the task set transactionally. Workers
claim bounded sets of available tasks with `FOR UPDATE SKIP LOCKED`.
Multiple API/worker processes coordinate through the same database.

Each Pod runs two API processes and its own health-aware loopback gateway. The
gateway streams responses and forwards a mutation once; a lost response is not
permission to replay an uncertain POST against another API. Workers may switch
configured origins for their next request, retaining the same completion fence
when acknowledgement is uncertain. Neither Pod requires an operator laptop
relay to reach the database. Private client ingress still depends on an operator
SSH path; public ingress was explicitly deferred.

API startup verifies a checksum-protected migration ledger and performs no DDL
or key bootstrap. The owner runs additive migrations separately. Runtime roles
cannot create schema objects or administer tenant keys. Compatible replicas can
run against v1 before migration v2 and then opt into its SQL execution path.

## Execution and result acceptance

A network failure can happen after GPU execution but before result submission.
Physical inference is therefore at least once. Each claim has an attempt token
and a lease. Committing a response requires the current worker, token and valid
lease. Replaying an accepted completion is idempotent; a superseded or cancelled
attempt is rejected. Usage commits in the same transaction as the accepted
result, preventing duplicate accounting.

The optional v2 completion and heartbeat functions execute their whole critical
section in one database call. They run as SECURITY INVOKER with the same table
permissions, lock worker then tenant queue then batch then task, and evaluate
the database clock after waiting for locks. A replay of an already accepted
worker/token remains duplicate-safe after a batch becomes terminal. Completion
also creates terminal output/error file metadata atomically. The Python path
remains available for a controlled comparison and feature rollback; rolling back
the feature flag does not undo the additive migration.

Additive v3 moves claiming into the same single-call form, with a separate flag.
It locks the worker before checking active capacity, materializes at most eight
oldest ready tenant queues with SKIP LOCKED, selects a live batch and bounded task
rows, creates fresh attempts, and updates batch/queue counters in one statement
transaction. A failed attempt insertion rolls back the entire claim. The Python
path remains supported for compatibility and controlled comparison. Enabling
the claim flag requires schema v3 on every serving replica's known ledger prefix.

Expired work becomes eligible again with a bounded retry budget. Cancellation
prevents new claims and fences outstanding attempts. In-flight GPU execution
may consume compute until the engine request is cancelled or finishes.

## Admission and scheduling

Workers claim tasks only for their configured model and available execution
capacity. Concurrency and context reservations bound outstanding inference.
Database claim/recovery work must remain bounded and indexed. Request-size,
upload-size, batch-size and output-token caps are enforced by the control plane.
See implementation and validation for current fairness behavior and limits.

## Isolation

Public resource reads and writes are tenant scoped. API keys are hashed before
storage. Internal worker calls require a separate shared token. Workers are
trusted infrastructure: this token grants access to queued inference inputs.
Production deployments should isolate internal routes with network policy and
rotate the token.

## Scaling boundaries

One managed PostgreSQL deployment is the durability and coordination boundary.
The current free Neon project has no production SLA. Application replicas and
local process watchdogs do not replace database recovery, whole-Pod replacement
or independent client ingress. This is not a multi-region consensus
implementation. Large JSONL
files and retention eventually belong in object storage; task tables eventually
need partitioning and archival. Measure database I/O, lock contention, claim
latency and GPU utilization before introducing a second queue system.
Per-batch counter contention and recovery traffic must be included in scale tests.
See [capacity analysis](scaling.md) for the daily target and measurement limits.

## Evidence

Correctness under duplicate requests, worker loss, cancellation races and stale
results is tested independently of throughput. Performance reports must include
workload, hardware, model revision, input/output lengths and latency distribution.
Queue acceptance rate and completed GPU work are different metrics.
