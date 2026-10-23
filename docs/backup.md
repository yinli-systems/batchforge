# PostgreSQL backups and recovery checks

The managed database runs PostgreSQL 17. A PostgreSQL 16 `pg_dump` cannot back it up. `devtools/backup_postgres.py` now checks the server and both client versions before creating a file. Select a compatible toolchain with `--bin-dir` or `PG_BIN_DIR`; the command-line option takes precedence. Without either, the development `pgserver` client remains the fallback, subject to the same check.

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

The current snapshot is recorded in [the archive report](../ops/reports/managed-backup-archive-2026-10-24.json): 10,246,398 bytes, SHA256 `2d5c959b0ade7f794500d26abc62df7d1dc2f46cca2ffc2ca033279d16d65f77`, 73 table-of-contents entries, PostgreSQL 17.11, TLS `verify-full`. [The preflight report](../ops/reports/managed-backup-preflight-2026-10-24.json) also records the intentional PostgreSQL 16 rejection. The archive report itself correctly says `restore_verified: false`: the separate restore drill supplies that additional evidence.

The [actual recovery report](../ops/reports/managed-backup-restore-2026-10-24.json) passed on 2026-10-23 at 21:09 UTC. It restored the hash-checked archive into a newly generated empty Neon PostgreSQL 17.11 database, then removed that exact disposable database. No application service or source database was changed. The drill restored 118 batches, 4,651 tasks, and 4,651 attempts, with zero per-batch count/state/usage mismatches and zero unvalidated constraints. Task and batch totals both contained 2,543,062 prompt tokens and 428,722 completion tokens. The report includes all ten table row counts/checksums, a task usage ledger SHA256, schema version 2, and verification-script/helper SHA256 values. The restore step took 1.95 seconds; the complete create/restore/check/remove drill took 4.67 seconds. These durations describe this particular 10 MB snapshot, not a recovery-time guarantee for a larger dataset.

A verified SCP copy of this exact archive is also stored on the local workstation under `.runtime/backups/batchforge-20261023T210307Z-b8e28561.dump`, independently of the primary Pod container. It was staged privately, SHA256-checked against the source archive metadata, and published locally only after the hash matched. The directory is mode `0700`, the file is mode `0600`, and both `.runtime/` and `*.dump` are excluded from Git. [The offhost-copy report](../ops/reports/managed-backup-offhost-2026-10-24.json) records the size, checksum, permissions, and transfer verification. This is a verified local copy, not scheduled backup retention or separately encrypted object storage.

The older `devtools/verify_backup.py` starts a local development cluster using its bundled `pgserver` toolchain. Do not use that PostgreSQL 16 toolchain to validate a PostgreSQL 17 archive.

Run the credential, version, and failure-publication regressions with `python -m pytest -q devtools/test_backup_postgres.py`. These five tests use mocked subprocess results and no live database. They complement the real archive and restore evidence; they do not establish recovery by themselves.

This is a database snapshot, not a complete disaster-recovery system. Cluster-global roles, external infrastructure, secret recovery, retention, and storage outside the Pod need separate policies. Recovery on the same Neon branch verifies the dump and application counters but does not demonstrate recovery from a provider or region outage. The [PostgreSQL 17 `pg_dump` documentation](https://www.postgresql.org/docs/17/app-pgdump.html) describes version compatibility and snapshot scope.
