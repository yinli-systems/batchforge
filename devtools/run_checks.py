"""Run correctness checks against a real, private local PostgreSQL instance.

Requires the optional pgserver development utility. Never used by production API.
"""
from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
import shlex
import tempfile


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    if os.environ.get("TEST_DATABASE_URL"):
        return subprocess.call([sys.executable, "-m", "pytest", "tests", "worker/tests", "-q"], cwd=root)
    # Use the packaged binaries directly. Do not use pgserver's discovery and
    # cleanup manager, which enumerates unrelated system processes.
    from pgserver._commands import POSTGRES_BIN_PATH

    with tempfile.TemporaryDirectory(prefix="batchforge-checks-", dir="/private/tmp" if sys.platform == "darwin" else None) as temporary:
        runtime = Path(temporary)
        data, sockets = runtime / "data", runtime / "socket"
        sockets.mkdir(mode=0o700)
        postgres_env = dict(os.environ, LC_ALL="C", LANG="C")
        subprocess.run([str(POSTGRES_BIN_PATH / "initdb"), "-D", str(data), "-U", "batchforge", "-A", "trust", "--no-locale", "--encoding=UTF8"], env=postgres_env, check=True, stdout=subprocess.DEVNULL)
        pg_ctl = str(POSTGRES_BIN_PATH / "pg_ctl")
        subprocess.run([pg_ctl, "-D", str(data), "-l", str(runtime / "postgres.log"), "-o", f"-h '' -k {shlex.quote(str(sockets))}", "-w", "start"], env=postgres_env, check=True, stdout=subprocess.DEVNULL)
        try:
            uri = "postgresql://batchforge@/postgres?host=" + str(sockets)
            env = dict(os.environ, TEST_DATABASE_URL=uri, DATABASE_URL=uri)
            return subprocess.call([sys.executable, "-m", "pytest", "tests", "worker/tests", "-q"], env=env, cwd=root)
        finally:
            subprocess.run([pg_ctl, "-D", str(data), "-m", "fast", "-w", "stop"], env=postgres_env, check=True, stdout=subprocess.DEVNULL)


if __name__ == "__main__":
    raise SystemExit(main())
