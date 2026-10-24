"""Open an interactive psql session to the private Azure BatchForge database.

Run as root on the configured RunPod. Reads the mode-0600 runtime URL, puts its
password in a short-lived mode-0600 pgpass file, and never puts it in argv or
stdout. The temporary password file is removed when psql exits.
"""
from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
import tempfile
from urllib.parse import parse_qs, unquote, urlsplit

from psycopg.conninfo import make_conninfo

ROOT = Path("/root/batchforge-runtime")
RUNTIME_ENV = ROOT / "azure-runtime.env"
PSQL = ROOT / "postgres-client-17/bin/psql"
EXPECTED_HOST = "batchforge-pg-frc-20261025.postgres.database.azure.com"


def escape_pgpass(value: str) -> str:
    return value.replace("\\", "\\\\").replace(":", "\\:").replace("\n", "")


def main() -> int:
    if os.geteuid() != 0:
        raise SystemExit("Run through the configured root SSH account")
    if not sys.stdin.isatty():
        raise SystemExit("An interactive SSH terminal is required")
    if RUNTIME_ENV.stat().st_mode & 0o077:
        raise SystemExit("Runtime config permissions are too broad")
    if not PSQL.is_file() or not PSQL.stat().st_mode & 0o111:
        raise SystemExit("The PostgreSQL client is unavailable")

    values = {}
    for line in RUNTIME_ENV.read_text(encoding="utf-8").splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip().strip(chr(34) + chr(39))
    parsed = urlsplit(values.get("DATABASE_URL", ""))
    query = parse_qs(parsed.query)
    host, port = parsed.hostname or "", parsed.port or 5432
    database = unquote(parsed.path.lstrip("/"))
    user = unquote(parsed.username or "")
    password = unquote(parsed.password or "")
    sslmode = query.get("sslmode", [""])[0]
    ca = query.get("sslrootcert", [""])[0]
    if (host != EXPECTED_HOST or database != "batchforge_import_20261025" or user != "bf_runtime"
            or not password or sslmode != "verify-full" or not ca):
        raise SystemExit("Azure runtime config did not match the expected endpoint and TLS policy")

    fd, name = tempfile.mkstemp(prefix="batchforge-azure-", suffix=".pgpass", dir="/tmp")
    path = Path(name)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(":".join(escape_pgpass(v) for v in (host, str(port), database, user, password)) + "\n")
        env = os.environ.copy()
        env["PGPASSFILE"] = str(path)
        conninfo = make_conninfo(host=host, port=port, dbname=database, user=user,
                                 sslmode=sslmode, sslrootcert=ca)
        print("Opening BatchForge Azure PostgreSQL with the restricted runtime role. Type \\q to exit.", flush=True)
        return subprocess.run(
            [str(PSQL), "-X", "-w", "-v", "ON_ERROR_STOP=1", conninfo],
            env=env,
            check=False,
        ).returncode
    finally:
        path.unlink(missing_ok=True)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"Could not open Azure PostgreSQL ({type(exc).__name__}); no credentials were displayed.", file=sys.stderr)
        raise SystemExit(1) from None
