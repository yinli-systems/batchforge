# Bounded terminal history retirement

Schema 11 adds an **owner-run, default-off maintenance path** for the retained-task limit. Installing the migration deletes nothing, enables no tenant, rewrites no historical task payload and starts no background sweeper. It must first be tested on an isolated PostgreSQL database. Migration gates take `tenants EXCLUSIVE`, then `batches/idempotency ACCESS EXCLUSIVE`, then `tasks/attempts SHARE ROW EXCLUSIVE`, all `NOWAIT`; a busy relation rejects the entire migration rather than queueing a DDL upgrade behind API work. The new indexes scan metadata; no old task payload is rewritten. Existing production history is outside this rollout: an operator must explicitly select a new `eligible_created_after` cohort before enabling a policy.

This is retirement of task/attempt detail after its retention period. Batch identities, business counters, accepted token usage, files, archive manifests and result objects remain. The original batch and output URLs therefore keep working. Retrying a partly or fully retired source returns HTTP 410 `history_retired`, including through an old API attempting a raw retry insert. No GPU inference is executed by maintenance.

## Safety and accounting

A history page requires all of these conditions, rechecked under database locks:

- Tenant policy enabled, an explicit created-after cohort, and terminal time older than the configured minimum age. The default age is 86,400 seconds; the minimum 60-second setting is for isolated validation.
- Terminal batch counters, no pending/running task or attempt, and no unexpired task lease.
- A ready, complete, previously verified and purged archive; completed task payloads must already be cleared. The owner module freshly verifies every immutable object and the aggregate manifest outside the database transaction before applying a page. SQL then rechecks the generation and manifest.
- No active result reader, including archive readers, and no building writer.
- Preserved batch totals equal retained task totals plus already-retired totals, for each terminal status and both token counters. Every completed retained task must have exactly one accepted completed attempt with matching token usage; non-completed tasks must have no accepted completion. Missing, duplicate or inconsistent accepted evidence fails closed before deletion.

Each transaction retires at most 1,000 tasks and their at most 20 attempts per task. It acquires locks in tenant → queue → batch → task order and skips a busy tenant, queue, batch or task. The CLI sets a one-second lock timeout and ten-second statement timeout. The eligibility and accounting scan is confined to the selected batch; the page limit bounds deleted rows, not the total rows examined in that scan. There is no tenant-wide automatic delete.

In the same transaction, attempts are moved into `retired_attempt_hourly`, attempts/tasks are deleted, schema 9 triggers release their exact admission charge, and the batch's `history_retired_*` counters advance. A crash before commit rolls back all four operations. Repeating a page cannot double-count accepted usage. A completed retirement retains an explicit timestamp and task count. Batch business counters never decrease.

`batchforge_attempt_hourly` combines live attempts and retired buckets. All hour boundaries are UTC. The console's schema 11 series uses the current UTC hour and the previous 23 hours: it is 24 calendar-hour buckets, rather than a second-precise sliding window. Total lifetime usage still comes from unchanged batch aggregates. Failed, retrying, expired and cancelled attempt buckets remain available in the rollup, even though the console plots completed/failed counts and accepted tokens.

The restore verifier checks retained + retired batch counts/status/token totals and the accepted hourly rollup, rather than skipping retired batches. Runtime credentials cannot delete detail, execute the retirement functions, change policy, or write retired aggregates. New functions use a fixed `pg_catalog,public` search path; none adds a SECURITY DEFINER administrator capability to the runtime.

## Explicit replay horizon for new keys

Ordinary existing idempotency keys keep their unlimited replay contract and are never deleted by this path. Even a pre-schema-11 row whose arbitrary key happens to resemble the new namespace retains its original full `(tenant, scope, key)` semantics.

Clients may opt into the new namespace:

```text
bf1.<10-digit Unix seconds>.<16–64 lowercase hexadecimal nonce>
```

The timestamp is part of this key's retention contract. Use the current UTC Unix time when creating a new key and reuse that exact key after uncertain responses. The database rejects malformed/noncanonical values and timestamps more than 60 seconds ahead of its own clock. A tenant replay floor begins at zero. An operator may advance it monotonically, no further than database time minus `replay_ttl_seconds` (86,400 seconds by default). The minimum guaranteed horizon is measured from the embedded timestamp; it is not a fresh TTL from each retry or from later task completion. There is no automatic expiry until the owner explicitly advances the floor.

Advancing the floor refuses to cross any schema 11 time-scoped key with an unresolved NULL response or an active/missing associated batch. Admission, floor advance and deletion share the same tenant lock. A writer waiting behind a committed floor change rechecks the new floor. Every expired time-scoped key returns HTTP 410 `idempotency_expired`, both while its row remains and after compaction; inserting the old key cannot start a second execution. Newer keys and legacy keys remain usable.

Compaction deletes at most 1,000 resolved time-scoped keys created within the explicit policy cohort per transaction. The persisted floor is the durable fence; physical rows can drain over multiple pages without weakening replay safety. The existing auxiliary triggers release each deleted key's reservation exactly once. The policy cannot be deleted or its floor reset through ordinary policy DML.

Write admission requires PostgreSQL `READ COMMITTED`, as used by the API. The database's idempotency, task-insert and retry-insert fences reject `REPEATABLE READ` and `SERIALIZABLE` transactions with `BR409`: an older transaction snapshot must never observe a superseded replay floor or retirement marker after acquiring a tenant lock. Read-only replay checks, usage views, accounting snapshots and restore validation may still use a repeatable snapshot. No meaningless tenant-row updates are used to manufacture MVCC versions.

This contract requires upgraded API replicas for consistent fast-path HTTP 410 errors. The database INSERT trigger also protects submissions from older API replicas, while the database retry guard protects retired source history.

## Owner commands

Keep the maintenance connection in a mode-0600 environment file outside Git. The CLI reads `BATCHFORGE_MAINTENANCE_DATABASE_URL` by default; it never accepts a password on the command line. Runtime credentials are deliberately insufficient. These are command templates, not instructions to enable cleanup of existing production history.

First choose a **new** explicit cohort, then configure its policy:

```sh
python -m backend.history_retirement configure --tenant TEST_TENANT \
  --enable --created-after 2026-10-24T12:00:00+00:00 \
  --min-age-seconds 86400 --replay-ttl-seconds 86400 --apply
```

Preview a single known terminal batch. Preview does not retire rows or publish objects:

```sh
python -m backend.history_retirement preview --tenant TEST_TENANT \
  --batch NEW_COHORT_BATCH --limit 1000
```

After reviewing that preview, apply one page using the existing archive backend. Choose `--local-root` for an existing private local artifact directory, or `--s3 --bucket EXISTING_BUCKET` with existing restricted S3 credentials in the environment:

```sh
python -m backend.history_retirement retire --tenant TEST_TENANT \
  --batch NEW_COHORT_BATCH --limit 1000 --s3 --bucket EXISTING_BUCKET --apply
```

A second invocation is explicit, bounded and safe after an uncertain maintenance commit. Stop when `remaining_tasks` is zero or `already_retired` is returned; a busy result is a retryable maintenance outcome, not permission to widen the cohort.

Time-scoped key cleanup also has a preview and a separate apply action:

```sh
python -m backend.history_retirement keys-preview --tenant TEST_TENANT \
  --floor APPROVED_UNIX_SECONDS --limit 1000
python -m backend.history_retirement keys-retire --tenant TEST_TENANT \
  --floor APPROVED_UNIX_SECONDS --limit 1000 --apply
```

The CLI reports only bounded metadata, error type and SQLSTATE. It never prints credentials, request bodies or generated output.

## Validation and remaining limits

`tests/test_history_retirement.py` runs against a new private Unix-socket PostgreSQL cluster and a random disposable database. It covers atomic `NOWAIT` migration refusal under an active reader, default-off/cohort protection, preview, partial/full retirement, mixed terminal states, exact retained admission ledgers, byte-identical output downloads, preserved UTC usage, actual cap exhaustion/re-admission, rollback/reentry, concurrent pages, active readers, corrupt objects, old-API resurrection/retry fences, legacy replay, unresolved/live keys, floor monotonicity, writer/floor commit races, NULL apply rejection, missing/duplicate/token-mismatched accepted evidence refusal, runtime privilege restrictions and restore rejection of a corrupted rollup. No production snapshot or credential is needed.

This does **not** remove the other finite storage limits. Batch rows, input-file metadata, archive runs/chunks, immutable result objects and legacy idempotency rows are deliberately retained. Retired hourly aggregates also grow with tenant/time. There is no claim of a physical heap/TOAST/index/WAL bound, secure erasure, S3 disaster recovery, an unlimited retention service, or a million-task/day result. Large archives are freshly downloaded for each applied page, so maintenance egress and verification work can become expensive; a later design can introduce durable verifier certificates with explicit freshness and integrity rules. Fleet scale requires independent capacity, storage lifecycle policy and measurements.
