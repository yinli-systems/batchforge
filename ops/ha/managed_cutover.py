"""Guarded offline copy to an EMPTY managed PostgreSQL database.

Connection strings are read from owner-only env files, never CLI arguments or
logs. Dry-run is the default. Stop every source writer before --apply: this is
an offline maintenance cutover, not CDC or zero-downtime migration. The tool
requires a drained source, exact schema compatibility, an empty destination,
one transaction for copied destination data, and matching ordered COPY hashes.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import stat
import sys

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import dict_row

# Direct script execution should resolve only this repository's backend.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from backend.migrate import migrate_database, validate_schema_rows

TABLES = ("tenants", "api_keys", "workers", "files", "batches", "tasks", "attempts", "idempotency", "tenant_queues")
ORDER = {"tenants": "id", "api_keys": "key_hash", "workers": "id", "files": "id", "batches": "id", "tasks": "id", "attempts": "token", "idempotency": "tenant_id,scope,key", "tenant_queues": "tenant_id,model"}


def load_url(path: Path) -> str:
    if stat.S_IMODE(path.stat().st_mode) & 0o077:
        raise ValueError("Connection environment files must be owner-only")
    from dotenv import dotenv_values
    values = dotenv_values(path, interpolate=False)
    value = values.get("DATABASE_URL")
    if not value:
        raise ValueError("Connection environment file must define DATABASE_URL")
    return value


def validate_target_url(value: str):
    options = conninfo_to_dict(value)
    host = options.get("host", "")
    if "-pooler" in host:
        raise ValueError("Use a direct PostgreSQL endpoint for migration")
    local = not host or host.startswith("/") or host in {"127.0.0.1", "localhost", "::1"}
    if not local and (options.get("sslmode") != "verify-full" or not options.get("sslrootcert")):
        raise ValueError("Remote destination requires sslmode=verify-full and sslrootcert")


def copy_query(table):
    return sql.SQL("COPY (SELECT * FROM {} ORDER BY {}) TO STDOUT").format(sql.Identifier(table), sql.SQL(ORDER[table]))


def digest(conn, table):
    result = hashlib.sha256()
    with conn.cursor().copy(copy_query(table)) as stream:
        for block in stream:
            result.update(block)
    return result.hexdigest()


def transfer(source_url: str, target_url: str, *, apply=False, writers_stopped=False):
    validate_target_url(target_url)
    source_values, target_values = conninfo_to_dict(source_url), conninfo_to_dict(target_url)
    identity = lambda values: tuple(values.get(key, "") for key in ("host", "port", "dbname"))
    if identity(source_values) == identity(target_values):
        raise ValueError("Source and destination must be different databases")
    if apply and not writers_stopped:
        raise ValueError("Stop source APIs, workers, recovery jobs and operator writers, then assert writers_stopped")
    with psycopg.connect(source_url, autocommit=False, row_factory=dict_row, connect_timeout=10) as source:
        source.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        source.execute("SET LOCAL timezone='UTC'")
        source.execute("SET LOCAL datestyle='ISO, MDY'")
        validate_schema_rows(source.execute("SELECT version,checksum FROM batchforge_schema_migrations ORDER BY version").fetchall())
        source_counts = {table: source.execute(sql.SQL("SELECT count(*) AS n FROM {}").format(sql.Identifier(table))).fetchone()["n"] for table in TABLES}
        active = source.execute("SELECT count(*) AS n FROM tasks WHERE status IN ('pending','running')").fetchone()["n"]
        if active:
            raise ValueError("Drain or explicitly cancel pending/running source tasks before cutover")
        inconsistent = source.execute("""SELECT count(*) AS n FROM batches b LEFT JOIN (
            SELECT batch_id,count(*) AS total,
                   count(*) FILTER(WHERE status='pending') AS pending,
                   count(*) FILTER(WHERE status='running') AS running,
                   count(*) FILTER(WHERE status='completed') AS completed,
                   count(*) FILTER(WHERE status='failed') AS failed,
                   count(*) FILTER(WHERE status='cancelled') AS cancelled,
                   sum(prompt_tokens) AS prompt_tokens,sum(completion_tokens) AS completion_tokens
              FROM tasks GROUP BY batch_id) t ON t.batch_id=b.id
            WHERE b.total_count IS DISTINCT FROM t.total
               OR b.pending_count IS DISTINCT FROM t.pending
               OR b.running_count IS DISTINCT FROM t.running
               OR b.completed_count IS DISTINCT FROM t.completed
               OR b.failed_count IS DISTINCT FROM t.failed
               OR b.cancelled_count IS DISTINCT FROM t.cancelled
               OR b.prompt_tokens IS DISTINCT FROM t.prompt_tokens
               OR b.completion_tokens IS DISTINCT FROM t.completion_tokens""").fetchone()["n"]
        queue_mismatches = source.execute("SELECT count(*) AS n FROM tenant_queues WHERE pending_count<>0").fetchone()["n"]
        if inconsistent or queue_mismatches:
            raise ValueError("Source task/usage/queue counters are inconsistent; repair before migration")
        with psycopg.connect(target_url, autocommit=True, row_factory=dict_row, connect_timeout=10) as target:
            existing = target.execute("SELECT to_regclass('public.batchforge_schema_migrations') AS relation").fetchone()["relation"]
            if existing:
                validate_schema_rows(target.execute("SELECT version,checksum FROM batchforge_schema_migrations ORDER BY version").fetchall())
                if any(target.execute(sql.SQL("SELECT count(*) AS n FROM {}").format(sql.Identifier(table))).fetchone()["n"] for table in TABLES):
                    raise ValueError("Destination application tables must be empty; no overwrite is permitted")
            elif target.execute("SELECT count(*) AS n FROM pg_tables WHERE schemaname='public'").fetchone()["n"]:
                raise ValueError("Unversioned destination public schema must be empty")
        report = {"observed_at": datetime.now(timezone.utc).isoformat(), "mode": "apply" if apply else "dry-run", "source_task_count": source_counts["tasks"], "source_counts": source_counts, "source_drained": True,
                  "source_writers_stopped_operator_assertion": writers_stopped, "source_counters_consistent": True, "destination_required_empty": True,
                  "copied": False, "online_migration": False}
        if not apply:
            return report
        migrate_database(target_url)
        # A concurrent source writer after this point invalidates a maintenance
        # migration. The caller must keep all former writers stopped/fenced.
        with psycopg.connect(target_url, autocommit=False, row_factory=dict_row, connect_timeout=10) as target:
            target.execute("SET LOCAL timezone='UTC'")
            target.execute("SET LOCAL datestyle='ISO, MDY'")
            # Lock/check again inside the atomic copy transaction, rather than
            # relying solely on the earlier preflight's empty-table observation.
            for table in TABLES:
                target.execute(sql.SQL("LOCK TABLE {} IN ACCESS EXCLUSIVE MODE").format(sql.Identifier(table)))
                if target.execute(sql.SQL("SELECT count(*) AS n FROM {}").format(sql.Identifier(table))).fetchone()["n"]:
                    raise ValueError("Destination was modified after preflight; aborting copy")
            hashes = {}
            for table in TABLES:
                source_columns = source.execute("SELECT column_name,data_type FROM information_schema.columns WHERE table_schema='public' AND table_name=%s ORDER BY ordinal_position", (table,)).fetchall()
                target_columns = target.execute("SELECT column_name,data_type FROM information_schema.columns WHERE table_schema='public' AND table_name=%s ORDER BY ordinal_position", (table,)).fetchall()
                if source_columns != target_columns:
                    raise ValueError("Source/destination column definitions differ")
                copied = hashlib.sha256()
                with source.cursor().copy(copy_query(table)) as reader, target.cursor().copy(sql.SQL("COPY {} FROM STDIN").format(sql.Identifier(table))) as writer:
                    for block in reader:
                        copied.update(block)
                        writer.write(block)
                if digest(target, table) != copied.hexdigest():
                    raise ValueError("Destination COPY hash differs; destination transaction will roll back")
                if target.execute(sql.SQL("SELECT count(*) AS n FROM {}").format(sql.Identifier(table))).fetchone()["n"] != source_counts[table]:
                    raise ValueError("Destination count differs; destination transaction will roll back")
                hashes[table] = copied.hexdigest()
            report.update({"copied": True, "ordered_table_sha256": hashes, "all_counts_and_hashes_match": True,
                           "destination_data_committed_atomically": True})
        return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-env-file", type=Path, required=True)
    parser.add_argument("--target-env-file", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--assert-writers-stopped", action="store_true")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    try:
        report = transfer(load_url(args.source_env_file), load_url(args.target_env_file), apply=args.apply, writers_stopped=args.assert_writers_stopped)
    except Exception as exc:
        # Driver messages may contain DSNs or table contents. Keep diagnostics
        # operator-safe; investigate privately rather than printing exceptions.
        print(f"Cutover failed ({type(exc).__name__}); no connection strings or row contents logged", file=sys.stderr)
        raise SystemExit(1) from None
    rendered = json.dumps(report, indent=2) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        with args.report.open("x") as handle:
            os.chmod(args.report, 0o600)
            handle.write(rendered)
    print(rendered, end="")


if __name__ == "__main__":
    main()
