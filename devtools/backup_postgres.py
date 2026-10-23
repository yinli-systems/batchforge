"""Create and verify a private PostgreSQL custom-format snapshot.

Does not stop services or restore/overwrite any database. Keep output outside Git.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import os
from pathlib import Path
import subprocess
import uuid


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--output-dir", type=Path, default=Path(".runtime/backups"))
    args = parser.parse_args()
    env = dict(os.environ)
    if args.env_file:
        from dotenv import dotenv_values
        env.update({key: value for key, value in dotenv_values(args.env_file, interpolate=False).items() if value is not None})
    if not env.get("DATABASE_URL"):
        parser.error("DATABASE_URL is required")
    from pgserver._commands import POSTGRES_BIN_PATH

    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True, mode=0o700)
    output.chmod(0o700)
    name = "batchforge-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8] + ".dump"
    destination = output / name
    fd = os.open(destination, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    os.close(fd)
    # pg_dump does not expand a URI supplied through PGDATABASE. Parse it into
    # libpq's environment options, keeping passwords out of command arguments.
    from psycopg.conninfo import conninfo_to_dict
    options = conninfo_to_dict(env["DATABASE_URL"])
    names = {"host": "PGHOST", "port": "PGPORT", "user": "PGUSER", "password": "PGPASSWORD", "dbname": "PGDATABASE", "sslmode": "PGSSLMODE", "sslcert": "PGSSLCERT", "sslkey": "PGSSLKEY", "sslrootcert": "PGSSLROOTCERT", "connect_timeout": "PGCONNECT_TIMEOUT", "options": "PGOPTIONS"}
    for name in names.values():
        env.pop(name, None)
    env.update({names[key]: value for key, value in options.items() if key in names})
    try:
        subprocess.run([str(POSTGRES_BIN_PATH / "pg_dump"), "--format=custom", "--file", str(destination)], env=env, check=True)
        subprocess.run([str(POSTGRES_BIN_PATH / "pg_restore"), "--list", str(destination)], env=env, check=True, stdout=subprocess.DEVNULL)
    except BaseException:
        destination.unlink(missing_ok=True)
        raise
    print(destination)


if __name__ == "__main__":
    main()
