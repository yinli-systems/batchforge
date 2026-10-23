"""Restore a snapshot into a NEW temporary PostgreSQL cluster and check counters.

Never accepts a live destination database and never modifies the source backup.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("snapshot", type=Path)
    parser.add_argument("--report", type=Path, default=Path(".runtime/restore-check.json"))
    args = parser.parse_args()
    if os.geteuid() == 0:
        parser.error("Run the isolated restore check as a non-root development user")
    snapshot = args.snapshot.resolve(strict=True)
    from pgserver._commands import POSTGRES_BIN_PATH
    import psycopg

    env = dict(os.environ, LC_ALL="C", LANG="C")
    with tempfile.TemporaryDirectory(prefix="bf-restore-", dir="/private/tmp" if sys.platform == "darwin" else None) as temporary:
        root = Path(temporary)
        data, sockets = root / "data", root / "socket"
        sockets.mkdir(mode=0o700)
        ctl = str(POSTGRES_BIN_PATH / "pg_ctl")
        subprocess.run([str(POSTGRES_BIN_PATH / "initdb"), "-D", str(data), "-U", "batchforge", "-A", "trust", "--no-locale", "--encoding=UTF8"], env=env, check=True, stdout=subprocess.DEVNULL)
        subprocess.run([ctl, "-D", str(data), "-l", str(root / "postgres.log"), "-o", f"-h '' -k {shlex.quote(str(sockets))}", "-w", "start"], env=env, check=True, stdout=subprocess.DEVNULL)
        uri = "postgresql://batchforge@/postgres?host=" + str(sockets)
        try:
            subprocess.run([str(POSTGRES_BIN_PATH / "pg_restore"), "--no-owner", "--exit-on-error", "--dbname", uri, str(snapshot)], env=env, check=True, stdout=subprocess.DEVNULL)
            with psycopg.connect(uri) as connection:
                batches, tasks, tokens = connection.execute("SELECT (SELECT count(*) FROM batches),(SELECT count(*) FROM tasks),(SELECT COALESCE(sum(prompt_tokens+completion_tokens),0) FROM tasks)").fetchone()
                mismatches = connection.execute("SELECT count(*) FROM batches b WHERE b.total_count != (SELECT count(*) FROM tasks t WHERE t.batch_id=b.id)").fetchone()[0]
                batch_tokens = connection.execute("SELECT COALESCE(sum(prompt_tokens+completion_tokens),0) FROM batches").fetchone()[0]
                if mismatches or tokens != batch_tokens:
                    raise RuntimeError("Restored task counts or usage counters do not match")
            report = {"observed_at": datetime.now(timezone.utc).isoformat(), "passed": True, "snapshot_sha256": hashlib.sha256(snapshot.read_bytes()).hexdigest(), "restored_batches": batches, "restored_tasks": tasks, "restored_tokens": int(tokens), "batch_task_count_mismatches": mismatches, "usage_matches_batch_totals": True, "destination": "new temporary local PostgreSQL cluster", "live_database_modified": False}
        finally:
            subprocess.run([ctl, "-D", str(data), "-m", "fast", "-w", "stop"], env=env, check=True, stdout=subprocess.DEVNULL)
    report["temporary_cluster_stopped_and_removed"] = True
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
