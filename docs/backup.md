# PostgreSQL backups and recovery checks

The managed database runs PostgreSQL 17. A PostgreSQL 16 `pg_dump` cannot back it up. `devtools/backup_postgres.py` now checks the server and both client versions before creating a file. Select a compatible toolchain with `--bin-dir` or `PG_BIN_DIR`; the command-line option takes precedence. Without either, the development `pgserver` client remains the fallback, subject to the same check.

## Latest schema v8 recovery proof

On **2026-10-24 at 01:52:17 UTC**, after the isolated 400-task inference run,
the operator created a fresh custom-format snapshot before enabling the first
selected production result purge. The [backup and off-Pod copy report](../ops/reports/result-archival-pre-purge-backup-2026-10-24.json)
records **121,441,717 bytes**, 157 table-of-contents entries, SHA256
`9e7785956991298dabd9efebf35bb695c37155ee56d00a266b8c37adb9508ce3`,
PostgreSQL/client 17.11 and TLS `verify-full`. The workstation copy matched
that SHA and used file mode `0600`. The archive manifest correctly retains
`restore_verified:false`; the separate restore report supplies recovery proof.

The [actual restore](../ops/reports/result-archival-pre-purge-restore-2026-10-24.json)
ran at **01:56:19.787388–01:56:34.538073 UTC** in a new private local PostgreSQL
17.11 cluster. Its OS user was `nobody` (UID 65534), its random directory and
Unix socket were private, and `listen_addresses=''` was verified. The source
archive and its temporary copy both matched the exact snapshot SHA. No source
database, production service, production cluster role or system package was changed.
The cluster was stopped and removed in `finally`.

The restored snapshot contained **387 batches, 26,283 tasks and 26,847 attempts**.
All eight immutable migration checksums matched. Per-batch task/state/usage
counters, accepted-attempt uniqueness and usage, tenant queue counts and
constraint validation had zero mismatches. Restored task usage was 30,958,031
prompt tokens and 5,053,323 generated tokens. Ten application SQL functions were
present as SECURITY INVOKER with configured search paths. All **19 public
tables** have deterministic row digests. These hash the restored archive; they
do not claim equality against the subsequently changing live database.

Pure restore took **9.595 seconds**; staging, restore, checks and cleanup took
**14.750 seconds**. CPU-only restoration overlapped the separately labelled
worker fault drill. These are observed functional recovery durations, not an
isolated performance comparison or a recovery-time guarantee.

The [server toolchain provenance](../ops/reports/result-archival-pg17-local-toolchain-2026-10-24.json)
records a freshly verified official PGDG `postgresql-17` package,
`17.11-1.pgdg24.04+2`, 16,949,784 bytes, SHA256
`d5b58058a82a3f344f832aea2cc54badc4f6265c25c960752cb1f5df5d2180ee`.
The retained InRelease signature, signed package-index hash and package hash
were checked before extraction into an application-owned directory. Core
`postgres`, `initdb` and `pg_ctl` had no missing dynamic libraries. JIT was
disabled; optional extensions and LLVM dependencies were not validated. No
global `apt` installation or operating-system service was used.

`ops/ha/local_pg17_restore.py` is the reusable local verifier. It accepts a
hash-pinned backup manifest and already verified extracted server/client roots;
it accepts no destination DSN and reads no live credentials. Because restore
uses `--no-owner --no-acl`, this does **not** restore `bf_runtime` permissions,
cluster roles or credentials. External input/result objects are also outside
the SQL archive. The [subsequent selected 20-result production purge](../ops/reports/result-archival-live-production-2026-10-24.json)
was gated on this successful restore; it is separate evidence from backup
recovery. Scheduled encrypted retention, object-store recovery and whole-provider
or region-loss recovery remain unproven.

## Historical schema v3 recovery proof

The earlier drill ran on **2026-10-23 at 23:25:37–23:26:26 UTC**, after both
Pods' timed inference experiments and before the engine-loss workload. The
application stayed online. The [workflow report](../ops/reports/managed-backup-schema3-workflow-2026-10-24.json)
records a fresh capacity check, one consistent custom-format dump, one restore
into a new uniquely named empty database on the same Neon branch, verification,
and deletion of that exact temporary database. No source rows, source grants,
or application services were changed.

The [archive manifest](../ops/reports/managed-backup-schema3-archive-2026-10-24.json)
records **103,379,878 bytes**, 74 table-of-contents entries and SHA256
`a87e67c3f947913672a40ccd76f431858fbd03d0505ad30f51ebf03be9fa297d`.
PostgreSQL server and both clients were 17.11; the direct connection used TLS
`verify-full`. Creating the dump took 21.55 seconds. Its manifest deliberately
retains `restore_verified:false`; archive readability and successful recovery
are separate checks.

The [restore report](../ops/reports/managed-backup-schema3-restore-2026-10-24.json)
passed with 343 batches, 25,134 tasks and 25,691 attempts. Restored task and batch
usage both contained 24,740,759 prompt tokens and 4,347,447 completion tokens.
All three immutable migration checksums matched. The claim, completion and
heartbeat SQL functions were present as SECURITY INVOKER with fixed search
paths. Per-batch counters, accepted-attempt uniqueness and accepted usage had
zero mismatches; no constraint was unvalidated. The pure restore step took
8.45 seconds; create/restore/check/remove took 26.51 seconds. These are observed
durations for this snapshot, not recovery-time guarantees.

All ten restored tables and the task-usage ledger have deterministic row
digests. **These hash the restored archive, not the subsequently changing live
database.** No all-table equality comparison against the live source is claimed;
idle worker registrations can still change live rows. The temporary database
`bf_restore_a5846989945c4255a4a9` was removed successfully. Because the restore
used `--no-owner --no-acl`, this drill does **not** establish restoration of
`bf_runtime` permissions, cluster roles or credentials. A real cutover must
reapply reviewed least-privilege grants to its destination and recover secrets
separately; the drill did not alter source permissions.

At 23:27:53 UTC the same archive was copied outside the Pod to the workstation,
verified against the exact SHA256 and byte count, then atomically published
under `.runtime/backups/batchforge-20261023T232538Z-5ac7d056.dump`.
The [offhost report](../ops/reports/managed-backup-schema3-offhost-2026-10-24.json)
records directory mode `0700`, file mode `0600`, removal of the transfer partial,
and preservation of older backups. The archive stays out of Git. This is one
private copy; at-rest encryption, scheduled retention and a recovery owner have
not been configured by this drill.

The Free plan currently advertises 1 GB/project, while this project's API
reports a 1,073,741,824-byte branch limit. The operator used a more conservative
1,000,000,000-byte limit and an 850,000,000-byte admission threshold. Immediately
before the dump, the source occupied 253,566,976 physical bytes and both
connectable non-template databases on the branch occupied 261,439,488 bytes.
Adding a 1.5-times-source restore allowance plus 128 MiB reserve yielded
776,007,680 bytes, below that threshold. The capacity gate was rechecked before
creating the temporary database. Read-only project metadata showed no other
live branches. Physical database size, branch logical size and provider billing
metrics have different definitions; this conservative check is not a storage
accounting guarantee. No paid resource or plan upgrade was created.
[Capacity evidence](../ops/reports/managed-backup-schema3-capacity-2026-10-24.json),
[sanitized project metadata](../ops/reports/managed-backup-schema3-neon-metadata-2026-10-24.json),
[current pricing](https://neon.com/pricing),
[October 2026 free-storage change](https://neon.com/blog/neon-free-plan-1-gb-per-project).

The Free plan also includes **100 CU-hours per project per month**. A compute
continuously active at 0.25 CU for a 30-day month would use
`0.25 × 24 × 30 = 180 CU-hours`; 100 CU-hours would cover 400 such active hours,
about 16.7 days. **This is steady-active arithmetic, not observed billing, an
invoice forecast, or the current remaining quota balance.** Continuous worker
polling and heartbeats can keep a database active, so scale-to-zero cannot be
assumed to make an always-active deployment fit the monthly allowance. Measure
actual consumption and quota before promising sustained availability; this
drill did not change the plan. [Official compute allowance](https://neon.com/pricing).

## Create a private snapshot

The primary Pod has an isolated PGDG 17.11 client installation at `/root/batchforge-runtime/postgres-client-17/bin`. The following commands read the owner URL from a private environment file, without placing the URL or password in process arguments:

```sh
cd /root/batchforge-runtime/app
/root/batchforge-runtime/venv/bin/python devtools/backup_postgres.py \
  --env-file /root/batchforge-runtime/neon-owner.env \
  --bin-dir /root/batchforge-runtime/postgres-client-17/bin \
  --check-only

/root/batchforge-runtime/venv/bin/python devtools/backup_postgres.py \
  --env-file /root/batchforge-runtime/neon-owner.env \
  --bin-dir /root/batchforge-runtime/postgres-client-17/bin \
  --output-dir /root/batchforge-runtime/app/.runtime/managed-backups \
  --report /root/batchforge-runtime/app/.runtime/managed-backup-archive.json
```

The helper creates a consistent custom-format archive while the application stays online. It bounds the initial table-lock wait to ten seconds. A hidden partial file is renamed to its final name only after `pg_dump`, `pg_restore --list`, and SHA256 verification succeed. Interrupted or failed dumps remove the partial file. The output directory is mode `0700`; archives and metadata are mode `0600`.

Connection passwords are delivered through a temporary mode `0600` pgpass file and removed afterwards. Child processes do not receive `DATABASE_URL` or `PGPASSWORD`. The source URL's TLS settings, including `verify-full` and channel binding, are preserved. Archive metadata records the effective server/client versions, TLS use, size, SHA256, and table-of-contents count. Metadata excludes the source URL and password. A separate `--report` destination replaces the default sidecar location; protect it and the dump equally.

An archive may contain prompts, responses, tenant records, and hashed API keys. Keep archives out of Git and public artifacts. Local filesystem permissions are not encryption. A scheduled retention policy, encrypted backup storage, and a recovery owner remain deployment requirements; this change does not claim those are configured.

## Verify the client supply chain

The actual installation used official PostgreSQL packages for the observed Ubuntu 24.04 `noble` / `amd64` host. No global package manager, operating-system service, or unrelated workload was changed. Packages were downloaded from the [official PGDG APT repository](https://apt.postgresql.org/pub/repos/apt/) and extracted into the application directory with `dpkg-deb --extract`.

The repository's `InRelease` signature was verified using the [PostgreSQL signing key](https://www.postgresql.org/media/keys/ACCC4CF8.asc), fingerprint `B97B0AFCAA1A47F044F244A07FCC7D46ACCC4CF8`. The signed release hashes authenticated the `main` and `17` package indexes; those indexes authenticated each downloaded `.deb`. The matching `libpq5` came from the `17` component. App-local wrappers prepend `/root/batchforge-runtime/postgres-client-17/rootfs/usr/lib/x86_64-linux-gnu` to `LD_LIBRARY_PATH` for their child process, then execute the real binaries in `rootfs/usr/lib/postgresql/17/bin`. Use the wrapper directory with `--bin-dir`; no global library configuration or shell environment change is needed. `pg_dump`, `pg_restore`, and `psql --version` all reported 17.11, and `ldd` confirmed the app-local `libpq` path.

| Package | Version | SHA256 |
| --- | --- | --- |
| `postgresql-client-17` | `17.11-1.pgdg24.04+2` | `b3b071b67a814a382516d6f6241b529c52f909672457e2f48083acce047b634f` |
| `libpq5` | `17.11-1.pgdg24.04+2` | `42548e7f855a646f2dc58c1aa1cfd1464678fc0e8ac04096a66d6354bca5ed10` |

Full URLs, signed-index hashes, signature status, dependency metadata, and observed versions are recorded in [the toolchain report](../ops/reports/managed-backup-toolchain-2026-10-24.json). The private installation also retains the signed release and package indexes under its `verification` directory. Reinstallations must verify the current signed indexes for the actual host distribution and architecture; do not treat this version table as a substitute for signature verification. See the [official Ubuntu installation guidance](https://www.postgresql.org/download/linux/ubuntu/).

## Restore into a new database

`pg_restore --list` checks archive readability; it does not prove recovery. A recovery drill must restore into a newly created, uniquely named empty database or a separate empty cluster. Keep the source database name out of the restore target. Use the matching client toolchain, preserve certificate verification, and use a temporary private pgpass file. `pg_restore` needs `--no-owner --no-acl --no-password --exit-on-error`; pass only the non-secret new database name with `--dbname` and supply host/user/TLS settings through libpq environment variables. Never use `--clean` against the application database.

After restoring, verify:

- All expected tables and the versioned migration records are present, and constraints are validated.
- Each batch's total and five state counters equal the restored task rows for that batch.
- Prompt and completion token totals match between tasks and batches, both globally and per batch.
- Sorted restored rows and the task usage ledger have recorded deterministic SHA256 digests. Hash canonical row JSON with an eight-byte big-endian length prefix before each UTF-8 row, so row boundaries are unambiguous.
- The temporary database is disconnected and removed after the report is written. Cleanup must identify only that drill's exact generated database name.

The earlier schema v2 snapshot is retained in [the archive report](../ops/reports/managed-backup-archive-2026-10-24.json): 10,246,398 bytes, SHA256 `2d5c959b0ade7f794500d26abc62df7d1dc2f46cca2ffc2ca033279d16d65f77`, 73 table-of-contents entries, PostgreSQL 17.11, TLS `verify-full`. [The preflight report](../ops/reports/managed-backup-preflight-2026-10-24.json) also records the intentional PostgreSQL 16 rejection. The archive report itself correctly says `restore_verified: false`: the separate restore drill supplies that additional evidence.

The [actual recovery report](../ops/reports/managed-backup-restore-2026-10-24.json) passed on 2026-10-23 at 21:09 UTC. It restored the hash-checked archive into a newly generated empty Neon PostgreSQL 17.11 database, then removed that exact disposable database. No application service or source database was changed. The drill restored 118 batches, 4,651 tasks, and 4,651 attempts, with zero per-batch count/state/usage mismatches and zero unvalidated constraints. Task and batch totals both contained 2,543,062 prompt tokens and 428,722 completion tokens. The report includes all ten table row counts/checksums, a task usage ledger SHA256, schema version 2, and verification-script/helper SHA256 values. The restore step took 1.95 seconds; the complete create/restore/check/remove drill took 4.67 seconds. These durations describe this particular 10 MB snapshot, not a recovery-time guarantee for a larger dataset.

A verified SCP copy of this exact archive is also stored on the local workstation under `.runtime/backups/batchforge-20261023T210307Z-b8e28561.dump`, independently of the primary Pod container. It was staged privately, SHA256-checked against the source archive metadata, and published locally only after the hash matched. The directory is mode `0700`, the file is mode `0600`, and both `.runtime/` and `*.dump` are excluded from Git. [The offhost-copy report](../ops/reports/managed-backup-offhost-2026-10-24.json) records the size, checksum, permissions, and transfer verification. This is a verified local copy, not scheduled backup retention or separately encrypted object storage.

The older `devtools/verify_backup.py` starts a local development cluster using its bundled `pgserver` toolchain. Do not use that PostgreSQL 16 toolchain to validate a PostgreSQL 17 archive.

Run the credential, version, and failure-publication regressions with `python -m pytest -q devtools/test_backup_postgres.py`. These five tests use mocked subprocess results and no live database. They complement the real archive and restore evidence; they do not establish recovery by themselves.

This is a database snapshot, not a complete disaster-recovery system. Cluster-global roles, external infrastructure, secret recovery, retention, and storage outside the Pod need separate policies. Recovery on the same Neon branch verifies the dump and application counters but does not demonstrate recovery from a provider or region outage. The [PostgreSQL 17 `pg_dump` documentation](https://www.postgresql.org/docs/17/app-pgdump.html) describes version compatibility and snapshot scope.
