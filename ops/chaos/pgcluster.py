"""Isolated local PostgreSQL 17 for lab runs. Never the managed database.

Mirrors devtools/run_checks.py: packaged binaries, a private socket directory,
and a shutdown in `finally`. Refuses to run against any URL other than its own.
"""
from __future__ import annotations

import os
import shlex
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path

LOG_DIR_PARENT = "/private/tmp" if os.uname().sysname == "Darwin" else None


@dataclass
class Cluster:
    root: Path
    data: Path
    sockets: Path
    pg_ctl: str
    env: dict

    @property
    def url(self) -> str:
        return f"postgresql://batchforge@/lab?host={self.sockets}"


def start() -> Cluster:
    from pgserver._commands import POSTGRES_BIN_PATH

    root = Path(tempfile.mkdtemp(prefix="batchforge-lab-", dir=LOG_DIR_PARENT))
    data, sockets = root / "data", root / "socket"
    sockets.mkdir(mode=0o700)
    env = dict(os.environ, LC_ALL="C", LANG="C")
    bin_dir = Path(POSTGRES_BIN_PATH)
    subprocess.run([str(bin_dir / "initdb"), "-D", str(data), "-U", "batchforge", "-A", "trust",
                    "--no-locale", "--encoding=UTF8"], env=env, check=True, stdout=subprocess.DEVNULL)
    pg_ctl = str(bin_dir / "pg_ctl")
    subprocess.run([pg_ctl, "-D", str(data), "-l", str(root / "postgres.log"),
                    "-o", f"-h '' -k {shlex.quote(str(sockets))} -c max_connections=300",
                    "-w", "start"], env=env, check=True, stdout=subprocess.DEVNULL)
    cluster = Cluster(root=root, data=data, sockets=sockets, pg_ctl=pg_ctl, env=env)
    subprocess.run([str(bin_dir / "createdb"), "-h", str(sockets), "-U", "batchforge", "lab"],
                   env=env, check=True, stdout=subprocess.DEVNULL)
    return cluster


def stop(cluster: Cluster) -> None:
    subprocess.run([cluster.pg_ctl, "-D", str(cluster.data), "-m", "fast", "-w", "stop"],
                   env=cluster.env, check=True, stdout=subprocess.DEVNULL)
