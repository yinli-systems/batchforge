# Admission stage timings

BatchForge exposes `batchforge_admission_stage_seconds{stage,outcome}` through
the existing worker-authenticated API `/metrics` endpoint. The histogram helps
locate admission stalls such as those observed in the
[policy160 experiment](benchmarks/qwen235b-policy160-results-2026-10-24.md).
It measures elapsed monotonic time around awaited application operations; it
does not establish that PostgreSQL lock contention caused that experiment.

The implementation is in [admission_metrics.py](../backend/admission_metrics.py)
and [Store](../backend/store.py). It adds no SQL, migration, grant, request retry,
configuration flag, admission-policy change or new endpoint.

## Metric contract

`stage` has exactly 23 allowed values and `outcome` is `ok`, `error` or `cancelled`:
at most 69 histogram label combinations per API process. Tenant/task/batch IDs,
models, paths, SQL, keys, prompts, responses and exception text are never labels.
Unknown stage names are rejected. Series are created when their path executes;
an absent series is not evidence of zero latency. Each API process has its own
histograms, which reset when that process restarts.

Finite bucket bounds in seconds are:
`0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 20, 40, 80, 160`,
followed by Prometheus's `+Inf` bucket. Percentiles estimated from these buckets
are approximate. A tail beyond 160 seconds has no finite bucket upper bound.

| Stages | Timed operation |
| --- | --- |
| `total` | `Store.create`, including its awaited cleanup |
| `idempotency_precheck` | Fast committed-response lookup before external input I/O |
| `input_read_validate` | Blob-catalog input read, reader-lease work and validation |
| `inline_validation` | Validation of inline input content |
| `pool_acquire`, `pool_release` | Store's connection-context entry and exit |
| `transaction` | Transaction-context entry, body and exit |
| `transaction_begin`, `transaction_commit`, `transaction_rollback` | The corresponding awaited transaction-context boundary |
| `idempotency`, `idempotency_tenant_lock` | Write-transaction key resolution and its tenant umbrella lock statement |
| `input_lookup`, `input_admission` | Input-file lookup and blob-catalog batch admission |
| `tenant_lock`, `retention_lock` | Tenant lock statement and retention-lock function call |
| `queue_precheck`, `queue_insert`, `queue_lock`, `queue_update` | Queue limit lookup and queue-row operations |
| `batch_insert`, `task_insert`, `idempotency_remember` | Batch insertion, existing task `executemany`, and response remembrance |

Stages overlap and must not be summed as independent wall time. A database-stage
duration includes client scheduling, network round trips, PostgreSQL execution
and any lock waiting inside that call. It is not an isolated PostgreSQL wait
measurement. The input stage similarly includes more than S3 transfer time, and
does not instrument every pool used internally by the blob catalog. Shared
idempotency and row-creation helpers can also emit samples during retry flows;
their sample counts need not equal `total` samples.

`ok` means that timed context exited normally. `error` and `cancelled` preserve
the original exception or cancellation and add no retry or compensation. The
async wrappers preserve ordinary context entry, exit, suppression and cleanup
behavior. An error or cancellation while awaiting COMMIT remains an unknown
transaction outcome; the metric is not evidence that the transaction rolled
back. A rollback-stage sample describes cleanup, not an independently verified
database result.

## Validation and release scope

[Portable tests](../tests/test_admission_observability.py) contain ten pure
context cases and seven PostgreSQL cases. They cover exception identity,
cancellation, suppression, commit-unknown handling, reader cancellation,
concurrent quota admission, same-key replay and measured tenant/queue waits.
PostgreSQL cases require an explicit `TEST_DATABASE_URL` for an ephemeral CI
server with `CREATE DATABASE` permission; each creates, migrates and drops only
its own random database. They use no GPU, external object service or production
database. Without that CI setting the seven database cases explicitly skip.

[Local validation provenance](../ops/reports/admission-stage-observability-local-validation-2026-10-24.json)
records 17/17 passing cases on a private Unix-only PostgreSQL 16.2 using the exact
source bytes later committed in `8f23c44e5145891b935ea389caa1001854ad36da`.
That record is local correctness evidence. The older 693d658 Linux/PG17 report
does not cover this feature. A new committed code-only snapshot of 8f23c44 must
pass the Linux/PG17 checks before API rollout; no such pass or production
deployment is claimed by the local provenance record.

After deployment, compare independent API replicas and separate successful,
failed and cancelled paths under a controlled workload. Preserve the original
arrival trace and durable task/usage accounting. These timers do not turn the
invalid-arrival policy160 pair into a causal scheduling comparison, and they
measure neither GPU kernels nor time to first token.
