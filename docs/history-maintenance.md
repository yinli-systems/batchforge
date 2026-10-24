# Finite owner history maintenance

`python -m ops.ha.history_maintenance` is a deployable, independent maintenance process for schema 11. It does not start with the API, touch GPU processes, change admission limits, enable a policy, shorten a retention period, or restart a service. Preview is the default. Execution additionally requires `--execute`, an explicit tenant and created-after cohort, an already enabled database policy, and an explicit finite `--max-cycles`.

The initial rollout is limited to newly created validation cohorts. A later default-tenant policy must select a cutoff **after the existing baseline/soak history**; the previous 26,943 retained tasks are not authorized for retirement by this controller's existence. Use at least 3,600 seconds of task age and the existing 86,400-second replay TTL for that rollout. Policy creation remains a separate reviewed owner action, not part of the sweep.

## Scope and work limits

The CLI's `--created-after` must equal or be later than the enabled policy's `eligible_created_after`, and `--min-age` must be at least its `min_age_seconds`. Each cycle rechecks these constraints. A change that makes the running process's scope too broad stops it, rather than silently changing its remit.

Each cycle examines at most 1–8 terminal batches. A candidate must be old enough, within the explicit cohort, have no pending/running counters, have a ready and purged archive, and not be fully retired. Candidates use a persisted `(created_at, id)` keyset. It wraps at the end, advances after a busy/pinned candidate and visits a batch at most once per cycle, so one pinned reader cannot monopolize maintenance.

Each selected batch gets at most one page of 1–1,000 task retirements per cycle. The existing `backend.history_retirement` verifier downloads and checks immutable archive objects before the atomic database page. All of the schema 11 terminal/lease/reader/accepted-attempt/accounting fences still apply. The maximum is therefore `batch_limit × task_page_limit` tasks per cycle, with at most 20 attempts per task. The database statements use a one-second lock timeout and ten-second statement timeout. Ordinary transaction lock/deadlock/retry outcomes skip that history candidate and rotate; cycles wait at least ten seconds (30 seconds by default), preventing a tight retry loop.

Archive verification also has a per-cycle byte budget, `--archive-byte-limit`, defaulting to 256 MiB and capped at 1 GiB. The controller checks each immutable chunk's declared bytes before fetching it. A budget exception leaves the current page unretired, advances the cursor, and permits another cycle to try other batches. An archive larger than the budget needs a reviewed larger limit; the controller never bypasses verification. Reported attempted/verified bytes are immutable payload bytes, not a provider's billable egress measurement: SDK retries and protocol overhead are outside this meter.

`--retire-keys` is off by default. If enabled, its cutoff must exactly equal the database policy's cutoff, because the existing key primitive has no additional narrower cutoff parameter. The controller locks the tenant and policy row, rechecks that binding, and derives the replay floor from database time minus the policy's existing TTL. It cannot choose a shorter TTL, guess that an unresolved commit succeeded, or delete a legacy key. A NULL response or live/missing associated batch refuses the floor advance. Physical compaction is bounded by `--key-page-limit` (1–1,000). The durable replay floor continues to reject expired time-scoped keys with 410 after their physical rows disappear.

## Accounting, exclusivity and crash behavior

Before and after every cycle, one read-only `REPEATABLE READ` snapshot validates the selected tenant's retained task/payload ledger, all eight auxiliary ledgers, live accepted attempt identities/token usage, retained-plus-retired batch aggregates, retired accepted usage and queue counters. It does not compare absolute before/after totals: concurrent legitimate inference may change those totals. Any inconsistency stops the process without repair or further retirement. These checks scan the selected tenant's thin accounting columns; large tenants still require measurement of their database overhead.

One operator holds a mode-0600 `flock` file inside its private operation directory. A session-level PostgreSQL advisory lock additionally excludes a second controller for the same tenant/database even if it uses a different operation directory or host. The lock is held on a dedicated owner connection, separately from the bounded work pool.

The operation directory is mode 0700; environment, lock, journal and report files must be owned and private regular files. Symlink file inputs and hard-linked private files are rejected. The owner connection is read from a restricted environment file into memory. No DSN, password, request text, generated output or credential is put on the command line or into a report.

The append-only journal is fsynced at action boundaries and limited to 32 MiB. Reports are written to mode-0600 temporary files, fsynced, atomically replaced, and their directory fsynced. Confirmed page/key results update cumulative counts immediately. Reports retain the current cycle's before/after accounting, per-page result/skip reason, elapsed time, archive bytes, rotating cursor, completed/started cycles and the immutable operation binding.

The operation binding also includes SHA256 values for this controller and `backend/history_retirement.py`, `backend/result_archival.py`, `backend/blob_storage.py` and `backend/migrate.py`. Every cycle rechecks those source files before selecting or retiring data. An archive identity hash binds either the canonical local artifact root or the credential-free canonical S3 endpoint, region, bucket and object prefix. The CLI constructs that identity from its loaded configuration and checks it against the actual verifier. Restarting with different source or an archive destination refuses the old operation directory, and a destination changed in memory stops the next cycle. Credential values and password rotation are excluded from this binding. Preview records its selected identity too, or an explicit empty identity when no archive backend is selected.

A crash or lost database acknowledgement can leave `pending_action` without confirmation. The process marks failure and refuses to automatically resume that operation. Review its journal and database state before starting a separately reviewed operation; the controller never infers a commit outcome. A completed operation reopened with the exact same binding performs no extra cycles. Changed bindings require a new operation directory. Cumulative counts mean confirmed returned operations; an unresolved action is explicitly unconfirmed.

SIGINT/SIGTERM sets a stop flag and lets the current page finish its verification/transaction boundary. It starts no next page or key compaction, performs the post-cycle accounting check and writes a stopped report. It does not restart itself or another service. Stop latency includes an in-progress archive verifier's existing SDK timeouts; it is not a fixed shutdown-time SLA. Force-killing the process can require the pending-action review described above.

## Owner command templates

These commands do not authorize enabling old production history. First prepare an enabled **new-cohort** policy separately and review a preview. Keep an owner environment file outside Git with mode 0600. It must contain `BATCHFORGE_MAINTENANCE_DATABASE_URL` or `DATABASE_URL`; S3 execution also needs the existing restricted `AWS_ENDPOINT_URL_S3`, `AWS_REGION`, `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` values. No credentials are supplied as arguments.

One finite preview cycle:

```sh
python -m ops.ha.history_maintenance \
  --env-file PRIVATE_OWNER_ENV_FILE --operation-dir PRIVATE_NEW_OPERATION_DIR \
  --tenant NEW_TEST_TENANT --created-after 2026-10-24T12:00:00+00:00 \
  --min-age 3600 --max-cycles 1 --batch-limit 2 --task-page-limit 1000
```

A reviewed 20-cycle task sweep using the existing archive bucket:

```sh
python -m ops.ha.history_maintenance \
  --env-file PRIVATE_OWNER_ENV_FILE --operation-dir PRIVATE_NEW_EXECUTION_DIR \
  --tenant NEW_TEST_TENANT --created-after 2026-10-24T12:00:00+00:00 \
  --min-age 3600 --max-cycles 20 --interval 30 \
  --batch-limit 2 --task-page-limit 1000 --archive-byte-limit 268435456 \
  --execute --s3 --bucket EXISTING_BUCKET
```

For a local artifact backend, replace `--s3 --bucket ...` with `--local-root EXISTING_PRIVATE_ARCHIVE_DIRECTORY`. Add `--retire-keys --key-page-limit 1000` only after reviewing its explicit replay-horizon contract and exact-cutoff restriction. The process exits after the finite cycle limit or a stop signal. Deploying a daemon/restart policy is a separate operational decision; no new resource is created here.

## Validation and remaining storage limits

`ops/ha/test_history_maintenance.py` uses disposable real PostgreSQL databases. Tests cover argument bounds and private files, both singleton locks, source/archive binding change refusal before any page call, finite CLI preview, mutation-free preview, exact page limits and accounting, pinned-reader rotation, archive byte exhaustion, safe stop boundaries, inconsistent accounting refusal without repair, unresolved key refusal, bounded key compaction with legacy preservation, and refusal to resume an uncertain page.

This path frees task/attempt detail and eligible time-scoped key reservations. It intentionally retains batches, input-file metadata, ready archive metadata/chunks, immutable objects, legacy keys and hourly aggregates. Those resources still have finite quotas and need their own reviewed lifecycle. This is not an unlimited storage claim or evidence of million-task/day fleet capacity. It does not export a database backup or add off-Pod recovery.
