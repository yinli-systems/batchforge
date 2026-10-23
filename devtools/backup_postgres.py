"""Create a private, version-compatible PostgreSQL custom-format snapshot.

Does not stop services or restore/overwrite a database. Keep snapshots outside Git.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import uuid


class BackupError(RuntimeError):
    pass


def select_bin_dir(explicit: Path | None, env: dict[str, str]) -> Path:
    selected = explicit or (Path(env["PG_BIN_DIR"]) if env.get("PG_BIN_DIR") else None)
    if selected is None:
        # Legacy local fixtures remain available, but version compatibility is
        # checked before creating an archive or connecting pg_dump to a server.
        from pgserver._commands import POSTGRES_BIN_PATH
        selected = Path(POSTGRES_BIN_PATH)
    directory = selected.expanduser().resolve()
    for name in ("pg_dump", "pg_restore"):
        binary = directory / name
        if not binary.is_file() or not os.access(binary, os.X_OK):
            raise BackupError(f"Missing executable {name} in selected PostgreSQL bin directory")
    return directory


def tool_version(binary: Path, env: dict[str, str]) -> tuple[int, str]:
    version_env = {key: value for key, value in env.items()
                   if key not in {"DATABASE_URL", "PGPASSWORD", "PGPASSFILE"}}
    result = subprocess.run([str(binary), "--version"], env=version_env, text=True,
                            capture_output=True, check=True, timeout=15)
    version = result.stdout.strip()
    match = re.search(r"\(PostgreSQL\)\s+(\d+)(?:\.\d+)*", version)
    if not match or int(match.group(1)) < 10:
        raise BackupError(f"Unable to identify a supported PostgreSQL10+ version for {binary.name}")
    return int(match.group(1)), version


def preflight(database_url: str, bin_dir: Path, env: dict[str, str]) -> dict:
    import psycopg
    from psycopg.conninfo import conninfo_to_dict

    dump_major, dump_version = tool_version(bin_dir / "pg_dump", env)
    restore_major, restore_version = tool_version(bin_dir / "pg_restore", env)
    if restore_major != dump_major:
        raise BackupError("pg_dump and pg_restore must come from the same major-version toolchain")
    with psycopg.connect(database_url, connect_timeout=10, application_name="batchforge-backup-preflight") as conn:
        server_version, server_version_num = conn.execute(
            "SELECT current_setting('server_version'),current_setting('server_version_num')::int"
        ).fetchone()
        server_major = server_version_num // 10000
        ssl_in_use = bool(conn.pgconn.ssl_in_use)
    if server_major > dump_major:
        raise BackupError(f"pg_dump{dump_major} cannot back up PostgreSQL{server_major}; select matching or newer tools with --bin-dir or PG_BIN_DIR")
    if server_major < 10:
        raise BackupError("This helper supports PostgreSQL10+ servers")
    return {"server_version": server_version, "server_major": server_major,
            "pg_dump_version": dump_version, "pg_dump_major": dump_major,
            "pg_restore_version": restore_version, "ssl_in_use": ssl_in_use,
            "configured_sslmode": conninfo_to_dict(database_url).get("sslmode", "libpq-default"),
            "compatible": True}


def pgpass_escape(value: str) -> str:
    if "\n" in value or "\r" in value:
        raise BackupError("Multiline passwords are not supported by the temporary pgpass file")
    return value.replace("\\", "\\\\").replace(":", "\\:")


@contextmanager
def connection_environment(database_url: str, source_env: dict[str, str], private_dir: Path):
    """Keep connection secrets out of argv and remove them from child environment."""
    from psycopg.conninfo import conninfo_to_dict

    options = conninfo_to_dict(database_url)
    env = dict(source_env)
    names = {"host": "PGHOST", "hostaddr": "PGHOSTADDR", "port": "PGPORT", "user": "PGUSER",
             "dbname": "PGDATABASE", "sslmode": "PGSSLMODE", "sslcert": "PGSSLCERT",
             "sslkey": "PGSSLKEY", "sslrootcert": "PGSSLROOTCERT", "sslcrl": "PGSSLCRL",
             "sslcrldir": "PGSSLCRLDIR", "connect_timeout": "PGCONNECT_TIMEOUT",
             "options": "PGOPTIONS", "channel_binding": "PGCHANNELBINDING",
             "target_session_attrs": "PGTARGETSESSIONATTRS", "gssencmode": "PGGSSENCMODE",
             "application_name": "PGAPPNAME", "sslnegotiation": "PGSSLNEGOTIATION"}
    for name in (*names.values(), "DATABASE_URL", "PGPASSWORD", "PGPASSFILE", "PGSERVICE", "PGSERVICEFILE"):
        env.pop(name, None)
    env.update({names[key]: value for key, value in options.items() if key in names})
    env.setdefault("PGCONNECT_TIMEOUT", "10")
    env.setdefault("PGAPPNAME", "batchforge-private-backup")
    password = options.get("password")
    with tempfile.TemporaryDirectory(prefix=".pgpass-", dir=private_dir) as temporary:
        if password is not None:
            path = Path(temporary) / "pgpass"
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            with os.fdopen(fd, "w") as file:
                file.write("*:*:*:*:" + pgpass_escape(password) + "\n")
            env["PGPASSFILE"] = str(path)
        yield env


def write_private_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name("." + path.name + "." + uuid.uuid4().hex + ".partial")
    fd = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(fd, "w") as file:
            json.dump(value, file, indent=2)
            file.write("\n")
            file.flush()
            os.fsync(file.fileno())
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def create_backup(database_url: str, bin_dir: Path, output: Path, env: dict[str, str],
                  metadata: dict, report_path: Path | None = None) -> tuple[Path, dict]:
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=True, mode=0o700)
    output.chmod(0o700)
    name = "batchforge-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8] + ".dump"
    destination = output / name
    partial = output / ("." + name + ".partial")
    fd = os.open(partial, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    os.close(fd)
    try:
        with connection_environment(database_url, env, output) as child_env:
            subprocess.run([str(bin_dir / "pg_dump"), "--format=custom", "--no-password",
                            "--lock-wait-timeout=10s", "--file", str(partial)], env=child_env, check=True)
            listing = subprocess.run([str(bin_dir / "pg_restore"), "--list", str(partial)],
                                     env=child_env, check=True, text=True, capture_output=True)
        with partial.open("rb") as file:
            digest = hashlib.file_digest(file, "sha256").hexdigest()
            os.fsync(file.fileno())
        partial.replace(destination)
        report = {"schema_version": 1, "observed_at": datetime.now(timezone.utc).isoformat(),
            **metadata, "snapshot_path": str(destination), "snapshot_sha256": digest,
            "snapshot_bytes": destination.stat().st_size, "snapshot_mode": oct(destination.stat().st_mode & 0o777),
            "archive_format": "custom", "archive_list_verified": True,
            "archive_toc_entries": sum(bool(line.strip()) and not line.lstrip().startswith(";") for line in listing.stdout.splitlines()),
            "restore_verified": False, "database_modified": False,
            "credentials_in_argv": False, "password_delivery": "temporary mode-0600 pgpass file when URI contains a password",
            "limitations": ["Archive listing is not a restore test", "Cluster-global roles and external infrastructure require separate backup policies"]}
        write_private_json(report_path or destination.with_suffix(".json"), report)
        return destination, report
    finally:
        partial.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--output-dir", type=Path, default=Path(".runtime/backups"))
    parser.add_argument("--bin-dir", type=Path, help="Directory containing pg_dump and pg_restore; overrides PG_BIN_DIR")
    parser.add_argument("--report", type=Path, help="Optional private JSON metadata destination")
    parser.add_argument("--check-only", action="store_true", help="Validate tool/server compatibility without creating a snapshot")
    args = parser.parse_args()
    env = dict(os.environ)
    if args.env_file:
        from dotenv import dotenv_values
        env.update({key: value for key, value in dotenv_values(args.env_file, interpolate=False).items() if value is not None})
    database_url = env.get("DATABASE_URL")
    if not database_url:
        parser.error("DATABASE_URL is required")
    try:
        bin_dir = select_bin_dir(args.bin_dir, env)
        metadata = preflight(database_url, bin_dir, env)
        if args.check_only:
            print(json.dumps(metadata, indent=2))
            return
        destination, _ = create_backup(database_url, bin_dir, args.output_dir, env, metadata, args.report)
        print(destination)
    except BackupError as exc:
        parser.exit(1, str(exc) + "\n")


if __name__ == "__main__":
    main()
