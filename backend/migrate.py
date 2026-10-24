"""Explicit, serialized PostgreSQL migrations. API replicas only verify schema.

Run with the schema owner's DATABASE_URL before rolling out API replicas. A
legacy unversioned v0.1 database can be adopted: its original idempotent schema
is executed once, then recorded. Never edit schema.sql after version 1 ships;
future migrations must be new immutable steps.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

from .config import get_settings

SCHEMA_VERSION = 8
MIGRATION_LOCK = 1936150117
SCHEMA_SQL = Path(__file__).with_name("schema.sql").read_text()
SCHEMA_CHECKSUM = hashlib.sha256(SCHEMA_SQL.encode()).hexdigest()
SQL_COMPLETION_SQL = Path(__file__).with_name("migrations").joinpath("0002_sql_completion.sql").read_text()
SQL_CLAIM_SQL = Path(__file__).with_name("migrations").joinpath("0003_sql_claim.sql").read_text()
MIGRATIONS = ((1, SCHEMA_SQL, SCHEMA_CHECKSUM),
              (2, SQL_COMPLETION_SQL, hashlib.sha256(SQL_COMPLETION_SQL.encode()).hexdigest()),
              (3, SQL_CLAIM_SQL, hashlib.sha256(SQL_CLAIM_SQL.encode()).hexdigest()))
for version, filename in ((4, '0004_sql_failure_recovery.sql'),
                          (5, '0005_blob_storage.sql'), (6, '0006_cost_scheduler.sql'),
                          (7, '0007_runtime_registration_guard.sql'),
                          (8, '0008_result_archival.sql')):
    statement = Path(__file__).with_name('migrations').joinpath(filename).read_text()
    MIGRATIONS += ((version, statement, hashlib.sha256(statement.encode()).hexdigest()),)


class SchemaMismatch(RuntimeError):
    pass


def validate_schema_rows(rows):
    # Known immutable prefixes are understood, enabling staged additive rollout.
    # Gaps, checksum drift and unknown future versions are never accepted.
    if not rows or len(rows) > len(MIGRATIONS) or any(row["version"] != expected[0] or row["checksum"] != expected[2] for row, expected in zip(rows, MIGRATIONS)):
        raise SchemaMismatch("Database schema does not match this release; run python -m backend.migrate with the migration owner before rollout")
    return rows[-1]["version"]


async def verify_schema(conn, minimum_version=1):
    try:
        rows = await (await conn.execute("SELECT version,checksum FROM batchforge_schema_migrations ORDER BY version")).fetchall()
    except psycopg.errors.UndefinedTable:
        raise SchemaMismatch("Database is not versioned; run python -m backend.migrate before API startup") from None
    version = validate_schema_rows(rows)
    if version < minimum_version:
        raise SchemaMismatch(f"Enabled SQL feature requires schema v{minimum_version}; apply migrations before enabling its feature flag")
    return version


def migrate_database(database_url: str, bootstrap_api_key: str | None = None, target_version: int = SCHEMA_VERSION) -> list[int]:
    if target_version not in {migration[0] for migration in MIGRATIONS}:
        raise ValueError("Unknown migration target version")
    if bootstrap_api_key is not None and not 16 <= len(bootstrap_api_key) <= 512:
        raise ValueError("BATCHFORGE_API_KEY must contain 16 to 512 characters for bootstrap")
    with psycopg.connect(database_url, row_factory=dict_row, connect_timeout=10,
                         options="-c client_encoding=UTF8 -c lock_timeout=30000 -c statement_timeout=60000") as conn:
        conn.execute("SELECT pg_advisory_xact_lock(%s)", (MIGRATION_LOCK,))
        conn.execute("CREATE TABLE IF NOT EXISTS batchforge_schema_migrations(version integer PRIMARY KEY,checksum text NOT NULL,applied_at timestamptz NOT NULL DEFAULT clock_timestamp())")
        rows = conn.execute("SELECT version,checksum FROM batchforge_schema_migrations ORDER BY version").fetchall()
        applied = []
        current = validate_schema_rows(rows) if rows else 0
        if current > target_version:
            raise SchemaMismatch("Downgrade is not supported; feature flags may be disabled without reverting additive schema")
        for version, statement, checksum in MIGRATIONS:
            if current < version <= target_version:
                conn.execute(statement)
                conn.execute("INSERT INTO batchforge_schema_migrations(version,checksum) VALUES(%s,%s)", (version, checksum))
                applied.append(version)
        if bootstrap_api_key is not None:
            conn.execute("INSERT INTO tenants(id,name) VALUES('default','Default tenant') ON CONFLICT DO NOTHING")
            conn.execute("INSERT INTO api_keys(key_hash,tenant_id) VALUES(%s,'default') ON CONFLICT DO NOTHING", (hashlib.sha256(bootstrap_api_key.encode()).hexdigest(),))
        return applied


def main():
    parser = argparse.ArgumentParser(description="Apply BatchForge schema once before API rollout")
    parser.add_argument("--bootstrap", action="store_true", help="Also add the default tenant/key from BATCHFORGE_API_KEY; does not re-enable revoked keys")
    parser.add_argument("--target-version", type=int, choices=[migration[0] for migration in MIGRATIONS], default=SCHEMA_VERSION)
    args = parser.parse_args()
    settings = get_settings()
    applied = migrate_database(settings.database_url, settings.batchforge_api_key if args.bootstrap else None, args.target_version)
    print(json.dumps({"schema_version": args.target_version, "applied_versions": applied, "bootstrapped": args.bootstrap}))


if __name__ == "__main__":
    main()
