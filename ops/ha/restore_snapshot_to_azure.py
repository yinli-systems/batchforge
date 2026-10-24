"""Restore the hash-pinned, locally verified BatchForge snapshot into a new Azure DB.

The source Neon project is left untouched. The destination database is dedicated
to this point-in-time import; an existing destination is never overwritten.
Run on the primary RunPod as root from the BatchForge checkout.
"""
from __future__ import annotations

import argparse
import getpass
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from datetime import datetime, timezone

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row
from psycopg import sql

ROOT = Path(__file__).resolve().parents[2]
HOST = "batchforge-pg-frc-20261025.postgres.database.azure.com"
ADMIN = "bfadmin"
RUNTIME = "bf_runtime"
CA = "/etc/ssl/certs/ca-certificates.crt"
DEST_DB = "batchforge_import_20261025"
ARCHIVE = Path("/root/batchforge-runtime/app/.runtime/backups/batchforge-20261024T094044Z-2522a776.dump")
EXPECTED_SHA256 = "e24edc2c3a9461486e3c38ae1743c3929afff61fbdb2d41cf33057666539f8e7"
EXPECTED_BYTES = 128439422
EXPECTED_REPORT = Path("/root/batchforge-runtime/app/.runtime/migration-reports/source-snapshot-restore-proof.json")
PSQL = Path("/root/batchforge-runtime/postgres-client-17/bin/psql")
PG_RESTORE = Path("/root/batchforge-runtime/postgres-client-17/bin/pg_restore")
CONFIG = Path("/root/batchforge-runtime")
sys.path.insert(0, str(ROOT))


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def pgpass_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace(":", "\\:").replace("\n", "")


def conninfo(db: str) -> str:
    return make_conninfo(host=HOST, port=5432, dbname=db, user=ADMIN,
                         sslmode="verify-full", sslrootcert=CA,
                         connect_timeout=12)


def main() -> None:
    phase = "preflight"
    parser = argparse.ArgumentParser()
    parser.add_argument("--resume", action="store_true",
                        help="verify/grant an already restored destination without restoring again")
    args = parser.parse_args()
    if os.geteuid() != 0:
        raise SystemExit("Run as root on the primary RunPod")
    if not ARCHIVE.is_file() or ARCHIVE.stat().st_size != EXPECTED_BYTES or sha256(ARCHIVE) != EXPECTED_SHA256:
        raise SystemExit("Pinned snapshot size/hash check failed")
    expected = json.loads(EXPECTED_REPORT.read_text())
    if not expected.get("passed") or expected.get("snapshot_sha256") != EXPECTED_SHA256:
        raise SystemExit("Existing isolated-restore evidence does not match snapshot")
    password = getpass.getpass("Azure bfadmin password (hidden): ")
    if not password:
        raise SystemExit("Empty admin password rejected")

    CONFIG.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(CONFIG, 0o700)
    fd, name = tempfile.mkstemp(prefix=".azure-import-", suffix=".pgpass", dir=CONFIG)
    pgpass = Path(name)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            for db in ("postgres", DEST_DB):
                f.write(":".join(pgpass_escape(v) for v in
                    (HOST, "5432", db, ADMIN, password)) + "\n")
        env = os.environ.copy()
        env["PGPASSFILE"] = str(pgpass)
        env["PGSSLMODE"] = "verify-full"
        env["PGSSLROOTCERT"] = CA

        phase = "Azure admin connection"
        # psycopg is in this process, so the child-only PGPASSFILE environment
        # below is not visible here. Pass the secret through libpq's password
        # parameter (never a DSN, command argument, or log line).
        with psycopg.connect(conninfo("postgres"), password=password,
                             autocommit=True, row_factory=dict_row) as c:
            exists = c.execute("SELECT 1 FROM pg_database WHERE datname=%s", (DEST_DB,)).fetchone()
            if args.resume and not exists:
                raise RuntimeError("resume_destination_missing")
            if not args.resume and exists:
                raise RuntimeError("Dedicated destination already exists; refusing overwrite")
            if not args.resume:
                phase = "create isolated import database"
                c.execute(sql.SQL("CREATE DATABASE {} OWNER {}").format(
                    sql.Identifier(DEST_DB), sql.Identifier(ADMIN)))

        if not args.resume:
            phase = "restore pinned snapshot"
            subprocess.run([str(PG_RESTORE), "--no-owner", "--no-acl", "--exit-on-error",
                            "--single-transaction", "--host", HOST, "--port", "5432",
                            "--username", ADMIN, "--dbname", DEST_DB, str(ARCHIVE)],
                           env=env, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL, check=True, timeout=1800)

        phase = "verify restored schema and all table digests"
        from backend.migrate import MIGRATIONS
        from ops.ha.local_pg17_restore import verify_database
        # verify_database consumes tuple rows and its own internal helpers rely
        # on positional indexing. Keep psycopg's default tuple row factory here.
        with psycopg.connect(conninfo(DEST_DB), password=password) as c:
            actual = verify_database(c, MIGRATIONS)
        if actual.get("tables") != expected.get("tables"):
            raise RuntimeError("Restored table row-count/digest verification failed")
        actual_ledger = {str(k): v for k, v in actual.get("migration_checksums", {}).items()}
        if actual_ledger != expected.get("migration_checksums"):
            raise RuntimeError("Restored migration ledger differs from verified snapshot")

        phase = "apply least-privilege runtime grants"
        grant_sql = (ROOT / "ops/ha/runtime_grants.sql").read_text()
        grant_sql = grant_sql.replace(
            "GRANT CONNECT ON DATABASE batchforge TO bf_runtime;",
            f"GRANT CONNECT ON DATABASE {DEST_DB} TO bf_runtime;", 1)
        fd, grant_name = tempfile.mkstemp(prefix=".azure-import-grants-", suffix=".sql", dir=CONFIG)
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(grant_sql)
        try:
            subprocess.run([str(PSQL), "-X", "-w", "-v", "ON_ERROR_STOP=1",
                            conninfo(DEST_DB), "-f", grant_name], env=env,
                           stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL, check=True, timeout=120)
        finally:
            Path(grant_name).unlink(missing_ok=True)

        phase = "verify runtime login, TLS, and read/write grants"
        from dotenv import dotenv_values
        runtime_url = dotenv_values(CONFIG / "azure-runtime.env").get("DATABASE_URL")
        if not runtime_url:
            raise RuntimeError("runtime_connection_config_missing")
        runtime_options = conninfo_to_dict(runtime_url)
        runtime_options["dbname"] = DEST_DB
        with psycopg.connect(make_conninfo(**runtime_options), connect_timeout=12) as c:
            who, database, tls = c.execute(
                "SELECT current_user,current_database(),"
                "(SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid())").fetchone()
            can_read = c.execute("SELECT has_table_privilege(current_user,'tasks','SELECT')").fetchone()[0]
            can_insert = c.execute("SELECT has_table_privilege(current_user,'tasks','INSERT')").fetchone()[0]
            if (who, database, tls, can_read, can_insert) != (RUNTIME, DEST_DB, True, True, True):
                raise RuntimeError("runtime_login_or_privilege_verification_failed")

        report = {
            "passed": True,
            "snapshot_sha256": EXPECTED_SHA256,
            "snapshot_bytes": EXPECTED_BYTES,
            "snapshot_time_utc": "2026-10-24T09:40:44Z",
            "destination_database": DEST_DB,
            "schema_version": actual["schema_version"],
            "table_count": len(actual["tables"]),
            "tasks": actual["tables"].get("tasks", {}).get("rows"),
            "attempts": actual["tables"].get("attempts", {}).get("rows"),
            "batches": actual["tables"].get("batches", {}).get("rows"),
            "all_table_digests_match_verified_snapshot": True,
            "runtime_login_tls_and_basic_privileges_verified": True,
            "neon_source_modified": False,
            "serving_apis_changed": False,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "limitations": ["Point-in-time database snapshot; Neon could not be queried for later writes.",
                            "External object-store payloads are not included in this PostgreSQL dump.",
                            "Database import is staged; serving API cutover is a separate step."],
        }
        report_dir = CONFIG / "migration-reports"
        report_dir.mkdir(mode=0o700, exist_ok=True)
        report_path = report_dir / "azure-snapshot-import-20261025.json"
        fd, tmp_name = tempfile.mkstemp(prefix=".report-", dir=report_dir)
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
            f.write("\n")
        os.replace(tmp_name, report_path)
        print(json.dumps(report, indent=2))
    except Exception as exc:
        # Never print driver/process error text; it may contain credentials or data.
        message = str(exc).lower()
        state = getattr(exc, "sqlstate", None)
        category = "transport_or_tls" if isinstance(exc, psycopg.OperationalError) else type(exc).__name__
        if state == "28P01" or "password authentication failed" in message:
            category = "admin_authentication_rejected"
        elif "timeout" in message or "could not connect" in message:
            category = "azure_connection_failed"
        safe_markers = {
            "schema_migration_ledger_differs",
            "restored_invariant_mismatch",
            "restored_function_security_differs",
            "restored_admission_ledger_mismatch",
            "Restored table row-count/digest verification failed",
            "Restored migration ledger differs from verified snapshot",
            "resume_destination_missing",
            "runtime_connection_config_missing",
            "runtime_login_or_privilege_verification_failed",
        }
        if str(exc) in safe_markers:
            category = str(exc)
        code = f", SQLSTATE={state}" if state else ""
        print(f"Snapshot import failed during {phase} ({category}{code}); credentials and connection details suppressed.")
        raise SystemExit(1) from None
    finally:
        pgpass.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
