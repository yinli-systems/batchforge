"""Manage only BatchForge's private PostgreSQL cluster on a Linux GPU pod.

The pgserver wheel supplies binaries only: its system-wide process discovery is
never used. PostgreSQL listens on an owner-only Unix socket, never TCP.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import pwd
import shlex
import subprocess


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["start", "stop", "status"])
    parser.add_argument("--base", type=Path, default=Path("/opt/batchforge-pg"))
    args = parser.parse_args()
    if os.geteuid() != 0:
        parser.error("Pod bootstrap requires root to create a dedicated database OS user")
    from pgserver._commands import POSTGRES_BIN_PATH

    base = args.base.resolve()
    if base != Path("/opt/batchforge-pg"):
        parser.error("This bootstrap is scoped to /opt/batchforge-pg")
    base.mkdir(exist_ok=True, mode=0o755)
    try:
        user = pwd.getpwnam("batchforgepg")
    except KeyError:
        subprocess.run(["useradd", "--system", "--no-create-home", "--home-dir", str(base), "--shell", "/usr/sbin/nologin", "batchforgepg"], check=True)
        user = pwd.getpwnam("batchforgepg")
    data, sockets = base / "data", base / "socket"
    for path in (data, sockets):
        path.mkdir(mode=0o700, exist_ok=True)
        os.chown(path, user.pw_uid, user.pw_gid)
        path.chmod(0o700)
    # All these paths belong exclusively to the newly installed PG environment.
    for path in (base, base / "venv", POSTGRES_BIN_PATH.parent, POSTGRES_BIN_PATH):
        path.chmod(path.stat().st_mode | 0o055)

    def run(command: list[str], **kwargs):
        return subprocess.run(["runuser", "-u", "batchforgepg", "--", *command], cwd=base, env=dict(os.environ, LC_ALL="C", LANG="C"), **kwargs)

    ctl = str(POSTGRES_BIN_PATH / "pg_ctl")
    if args.action == "start":
        if not (data / "PG_VERSION").exists():
            run([str(POSTGRES_BIN_PATH / "initdb"), "-D", str(data), "-U", "batchforge", "-A", "trust", "--no-locale", "--encoding=UTF8"], check=True, stdout=subprocess.DEVNULL)
        status = run([ctl, "-D", str(data), "status"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if status.returncode:
            run([ctl, "-D", str(data), "-l", str(data / "postgres.log"), "-o", f"-h '' -k {shlex.quote(str(sockets))}", "-w", "start"], check=True)
        print("postgresql://batchforge@/postgres?host=" + str(sockets))
    elif args.action == "stop":
        run([ctl, "-D", str(data), "-m", "fast", "-w", "stop"], check=True)
    else:
        run([ctl, "-D", str(data), "status"], check=True)


if __name__ == "__main__":
    main()
