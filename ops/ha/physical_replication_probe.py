"""Destructive only to its own temporary PostgreSQL cluster pair.

Uses optional pgserver development binaries directly. Creates physical streaming
replication over owner-only Unix sockets, requires synchronous remote_apply,
crashes its own primary, verifies the standby stays read-only, then performs an
explicit promotion only after the former primary is confirmed stopped. This is
a local database correctness probe, not a cross-host HA claim or a DCS substitute.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import time

import psycopg
from psycopg.conninfo import make_conninfo


def run_probe() -> dict:
    from pgserver._commands import POSTGRES_BIN_PATH

    binaries = Path(POSTGRES_BIN_PATH)
    environment = {**os.environ, "LC_ALL": "C", "LANG": "C"}

    def command(binary, *arguments, check=True):
        return subprocess.run([str(binaries / binary), *map(str, arguments)], env=environment,
                              check=check, capture_output=True, text=True, timeout=45)

    def wait_until(callback, timeout=15):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            value = callback()
            if value:
                return value
            time.sleep(0.05)
        raise RuntimeError("Private replication probe did not converge before timeout")

    with tempfile.TemporaryDirectory(prefix="bf-repl-", dir="/private/tmp" if sys.platform == "darwin" else None) as temporary:
        root = Path(temporary)
        primary, standby = root / "primary", root / "standby"
        primary_socket, standby_socket = root / "ps", root / "ss"
        primary_socket.mkdir(mode=0o700)
        standby_socket.mkdir(mode=0o700)
        started = set()
        connection = dict(user="batchforge", dbname="postgres", connect_timeout=2)
        primary_dsn = make_conninfo(**connection, host=str(primary_socket), port="54321")
        standby_dsn = make_conninfo(**connection, host=str(standby_socket), port="54322")
        command("initdb", "-D", primary, "-U", "batchforge", "-A", "trust", "--no-locale", "--encoding=UTF8")
        with (primary / "postgresql.conf").open("a") as handle:
            handle.write("\nwal_level=replica\nmax_wal_senders=4\nmax_replication_slots=2\nfsync=on\nfull_page_writes=on\n")
        try:
            command("pg_ctl", "-D", primary, "-l", root / "primary.log", "-o", f"-h '' -k {shlex.quote(str(primary_socket))} -p 54321", "-w", "start")
            started.add(primary)
            clone_dsn = make_conninfo(**connection, host=str(primary_socket), port="54321", application_name="bf_probe_standby")
            command("pg_basebackup", "--dbname", clone_dsn, "--pgdata", standby, "--write-recovery-conf", "--wal-method=stream", "--checkpoint=fast", "--create-slot", "--slot=bf_probe_slot")
            command("pg_ctl", "-D", standby, "-l", root / "standby.log", "-o", f"-h '' -k {shlex.quote(str(standby_socket))} -p 54322", "-w", "start")
            started.add(standby)
            with psycopg.connect(primary_dsn, autocommit=True) as conn:
                conn.execute("ALTER SYSTEM SET synchronous_standby_names='FIRST 1 (bf_probe_standby)'")
                conn.execute("ALTER SYSTEM SET synchronous_commit='remote_apply'")
                conn.execute("SELECT pg_reload_conf()")

                def synchronous():
                    row = conn.execute("SELECT sync_state FROM pg_stat_replication WHERE application_name='bf_probe_standby' AND state='streaming'").fetchone()
                    return row and row[0] == "sync"

                wait_until(synchronous)
            # New session must pick up synchronous_commit=remote_apply. The
            # acknowledged writes are already replayed, not merely sent to WAL.
            with psycopg.connect(primary_dsn, autocommit=True) as conn:
                assert conn.execute("SHOW synchronous_commit").fetchone()[0] == "remote_apply"
                conn.execute("CREATE TABLE accepted_commits(id integer PRIMARY KEY,payload text NOT NULL)")
                conn.execute("INSERT INTO accepted_commits SELECT id,'commit-'||id FROM generate_series(1,64) AS id")
                primary_lsn = conn.execute("SELECT pg_current_wal_lsn()::text").fetchone()[0]
            with psycopg.connect(standby_dsn, autocommit=True) as conn:
                before = conn.execute("SELECT count(*) FROM accepted_commits").fetchone()[0]
                assert before == 64
                assert conn.execute("SELECT pg_is_in_recovery()").fetchone()[0] is True
                try:
                    conn.execute("INSERT INTO accepted_commits VALUES(65,'must-fail-before-promotion')")
                except psycopg.errors.ReadOnlySqlTransaction:
                    pass
                else:
                    raise AssertionError("Standby accepted writes before fencing/promotion")
            crashed_at = time.monotonic()
            command("pg_ctl", "-D", primary, "-m", "immediate", "-w", "stop")
            started.remove(primary)
            # This check proves the *app-owned* former primary is stopped before
            # promotion. Production additionally needs external STONITH/watchdog.
            assert command("pg_ctl", "-D", primary, "status", check=False).returncode == 3
            with psycopg.connect(standby_dsn, autocommit=True) as conn:
                assert conn.execute("SELECT pg_is_in_recovery()").fetchone()[0] is True
                assert conn.execute("SELECT count(*) FROM accepted_commits").fetchone()[0] == 64
            promoted_at = time.monotonic()
            command("pg_ctl", "-D", standby, "-w", "promote")
            # The old primary's synchronous config is inherited by the clone;
            # disable it only on this isolated promoted node to permit the probe
            # write without a replacement synchronous standby. Production
            # strict synchronous mode intentionally blocks here instead.
            with psycopg.connect(standby_dsn, autocommit=True) as conn:
                conn.execute("SET synchronous_commit=local")
                conn.execute("INSERT INTO accepted_commits VALUES(65,'post-fencing-promotion')")
                count = conn.execute("SELECT count(*) FROM accepted_commits").fetchone()[0]
                assert count == 65
                assert conn.execute("SELECT pg_is_in_recovery()").fetchone()[0] is False
                server_version = conn.info.server_version
            return {"observed_at": datetime.now(timezone.utc).isoformat(), "probe": "local-private-physical-postgres-replication",
                    "postgres_server_version": server_version, "transport": "owner-only Unix sockets", "synchronous_commit": "remote_apply",
                    "synchronous_standby_count": 1, "accepted_commits_before_crash": 64, "retained_commits_after_crash": 64,
                    "standby_rejected_writes_before_promotion": True, "former_primary_confirmed_stopped_before_promotion": True,
                    "automatic_failover": False, "operator_promotion": True, "promoted_write_requires_synchronous_commit_local_in_probe": True,
                    "count_after_promoted_write": count, "primary_acknowledged_wal_lsn": primary_lsn,
                    "crash_stop_to_fencing_verified_seconds": round(promoted_at - crashed_at, 6),
                    "promotion_and_write_seconds": round(time.monotonic() - promoted_at, 6),
                    "limitations": ["Two local processes share one host and disk failure domain", "No network partition or independent quorum tested",
                                    "Production strict synchronous failover requires another synchronous standby before writes resume",
                                    "No existing database or application workload was modified"]}
        finally:
            for data in (standby, primary):
                if data in started:
                    command("pg_ctl", "-D", data, "-m", "immediate", "-w", "stop", check=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, help="Write nonsecret JSON evidence")
    args = parser.parse_args()
    report = run_probe()
    rendered = json.dumps(report, indent=2) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(rendered)
    print(rendered, end="")


if __name__ == "__main__":
    main()
