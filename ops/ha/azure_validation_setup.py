"""Provision a least-privilege, empty Azure PostgreSQL validation target.

This deliberately does not switch any serving API or copy production data.
Run on the primary RunPod over an interactive SSH terminal. Passwords are read
without echo, never accepted as command-line arguments, and never logged.
"""
from __future__ import annotations

import getpass
import os
from pathlib import Path
import pty
import select
import secrets
import subprocess
import sys
import tempfile
import termios
import time
from urllib.parse import quote

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row

ROOT = Path(__file__).resolve().parents[2]
HOST = "batchforge-pg-frc-20261025.postgres.database.azure.com"
ADMIN = "bfadmin"
DATABASE = "batchforge"
RUNTIME = "bf_runtime"
PORT = 5432
CA = "/etc/ssl/certs/ca-certificates.crt"
CONFIG_DIR = Path("/root/batchforge-runtime")
RUNTIME_ENV = CONFIG_DIR / "azure-runtime.env"
PSQL = Path("/root/batchforge-runtime/postgres-client-17/bin/psql")
PHASE = "startup checks"


def pgpass_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace(":", "\\:").replace("\n", "")


def connect_info(database: str, user: str) -> str:
    return make_conninfo(
        host=HOST,
        port=PORT,
        dbname=database,
        user=user,
        sslmode="verify-full",
        sslrootcert=CA,
        connect_timeout=10,
        options="-c client_encoding=UTF8 -c lock_timeout=30000 -c statement_timeout=60000",
    )


def database_is_empty(conn: psycopg.Connection) -> None:
    marker = conn.execute("SELECT to_regclass('public.batchforge_schema_migrations')").fetchone()[0]
    if marker is None:
        count = conn.execute(
            "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname='public' AND c.relkind IN ('r','p','v','m','S','f')"
        ).fetchone()[0]
        if count:
            raise RuntimeError("Azure target has public objects without the BatchForge migration ledger")
        return

    tables = conn.execute(
        "SELECT tablename FROM pg_tables "
        "WHERE schemaname='public' AND tablename<>'batchforge_schema_migrations'"
    ).fetchall()
    for row in tables:
        table = row[0]
        if conn.execute(
            sql.SQL("SELECT EXISTS(SELECT 1 FROM {} LIMIT 1)").format(sql.Identifier("public", table))
        ).fetchone()[0]:
            raise RuntimeError("Azure target contains application data; refusing to modify it")


def set_runtime_password(password: str) -> None:
    r"""Use psql's hidden \password prompt without exposing the generated secret."""
    command = [
        str(PSQL), "-X", "-w", "-v", "ON_ERROR_STOP=1", "-c",
        f"\\password {RUNTIME}", connect_info("postgres", ADMIN),
    ]
    child_pid, terminal_fd = pty.fork()
    if child_pid == 0:
        try:
            os.execv(command[0], command)
        except Exception:
            os._exit(127)

    output = bytearray()
    new_prompt = b'Enter new password for user "bf_runtime":'
    confirm_prompt = b"Enter it again:"
    sent_new = False
    sent_confirm = False
    child_reaped = False
    deadline = time.monotonic() + 30
    try:
        while time.monotonic() < deadline:
            ready, _, _ = select.select([terminal_fd], [], [], 0.5)
            if ready:
                try:
                    chunk = os.read(terminal_fd, 4096)
                except OSError:
                    break
                if not chunk:
                    break
                output.extend(chunk)
                if len(output) > 8192:
                    del output[:-4096]
                echo_enabled = bool(termios.tcgetattr(terminal_fd)[3] & termios.ECHO)
                if not sent_new and new_prompt in output and not echo_enabled:
                    os.write(terminal_fd, password.encode("utf-8") + b"\n")
                    sent_new = True
                if sent_new and not sent_confirm and confirm_prompt in output and not echo_enabled:
                    os.write(terminal_fd, password.encode("utf-8") + b"\n")
                    sent_confirm = True
            waited, status = os.waitpid(child_pid, os.WNOHANG)
            if waited:
                child_reaped = True
                if os.waitstatus_to_exitcode(status) != 0:
                    raise RuntimeError("psql could not set the runtime password")
                if not sent_new or not sent_confirm:
                    raise RuntimeError("psql password prompts did not complete")
                return
        else:
            raise RuntimeError("psql password update timed out")
        _, status = os.waitpid(child_pid, 0)
        child_reaped = True
        if os.waitstatus_to_exitcode(status) != 0 or not sent_new or not sent_confirm:
            raise RuntimeError("psql password update did not complete")
    finally:
        os.close(terminal_fd)
        if not child_reaped:
            try:
                os.kill(child_pid, 15)
            except ProcessLookupError:
                pass
            try:
                os.waitpid(child_pid, 0)
            except ChildProcessError:
                pass


def main() -> int:
    global PHASE
    if os.geteuid() != 0:
        raise SystemExit("Run this on the primary RunPod as root")
    if not sys.stdin.isatty() or not Path("/dev/tty").exists():
        raise SystemExit("An interactive SSH terminal is required for hidden password prompts")
    if RUNTIME_ENV.exists():
        raise SystemExit("Azure runtime config already exists; refusing to overwrite it")
    if not PSQL.is_file() or not PSQL.stat().st_mode & 0o111:
        raise SystemExit("The verified PostgreSQL 17 client is missing on this Pod")

    admin_password = getpass.getpass("Azure bfadmin password (hidden): ")
    if not admin_password:
        raise SystemExit("An empty admin password is not accepted")

    CONFIG_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(CONFIG_DIR, 0o700)
    fd, pgpass_name = tempfile.mkstemp(prefix="batchforge-azure-", suffix=".pgpass", dir="/tmp")
    pgpass = Path(pgpass_name)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(":".join(pgpass_escape(v) for v in (HOST, str(PORT), "*", ADMIN, admin_password)) + "\n")
        os.environ["PGPASSFILE"] = str(pgpass)

        PHASE = "admin authentication and role-capability check"
        with psycopg.connect(connect_info("postgres", ADMIN), autocommit=True, row_factory=dict_row) as conn:
            role = conn.execute(
                "SELECT rolcreatedb, rolcreaterole FROM pg_roles WHERE rolname=current_user"
            ).fetchone()
            if not role or not role["rolcreatedb"] or not role["rolcreaterole"]:
                raise RuntimeError("Azure admin lacks CREATEDB or CREATEROLE required for safe setup")
            exists = conn.execute("SELECT 1 FROM pg_database WHERE datname=%s", (DATABASE,)).fetchone()
            if not exists:
                conn.execute(sql.SQL("CREATE DATABASE {} OWNER {}").format(sql.Identifier(DATABASE), sql.Identifier(ADMIN)))

            runtime_role = conn.execute("SELECT oid, rolsuper FROM pg_roles WHERE rolname=%s", (RUNTIME,)).fetchone()
            if runtime_role and runtime_role["rolsuper"]:
                raise RuntimeError("Existing bf_runtime is privileged; refusing to alter it")
            memberships = conn.execute(
                "SELECT 1 FROM pg_auth_members WHERE member=(SELECT oid FROM pg_roles WHERE rolname=%s) LIMIT 1",
                (RUNTIME,),
            ).fetchone()
            if memberships:
                raise RuntimeError("Existing bf_runtime has role memberships; refusing to alter it")
            if not runtime_role:
                conn.execute(sql.SQL("CREATE ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS").format(sql.Identifier(RUNTIME)))
            else:
                conn.execute(sql.SQL("ALTER ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS").format(sql.Identifier(RUNTIME)))

        PHASE = "empty-target verification"
        with psycopg.connect(connect_info(DATABASE, ADMIN), autocommit=True) as conn:
            database_is_empty(conn)

        sys.path.insert(0, str(ROOT))
        from backend.migrate import migrate_database

        PHASE = "BatchForge schema migration"
        applied = migrate_database(connect_info(DATABASE, ADMIN))

        PHASE = "least-privilege SQL grants"
        grant_file = ROOT / "ops" / "ha" / "runtime_grants.sql"
        subprocess.run(
            [str(PSQL), "-X", "-w", "-v", "ON_ERROR_STOP=1", connect_info(DATABASE, ADMIN), "-f", str(grant_file)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=os.environ.copy(),
            check=True,
        )

        # Generate a high-entropy secret and let psql's \password hash it
        # client-side. The secret never enters SQL text, argv, logs or stdout.
        PHASE = "runtime password setup"
        runtime_password = secrets.token_urlsafe(48)
        set_runtime_password(runtime_password)

        PHASE = "runtime login, TLS and privilege verification"
        runtime_url = (
            f"postgresql://{RUNTIME}:{quote(runtime_password, safe='')}@{HOST}:{PORT}/{DATABASE}"
            f"?sslmode=verify-full&sslrootcert={quote(CA, safe='')}"
        )
        PHASE = "runtime login, TLS and privilege verification"
        fd, tmp_name = tempfile.mkstemp(prefix=".azure-runtime-", dir=CONFIG_DIR, text=True)
        tmp = Path(tmp_name)
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                stream.write(f"DATABASE_URL={runtime_url}\n")
            with psycopg.connect(runtime_url, connect_timeout=10, row_factory=dict_row) as conn:
                row = conn.execute(
                    "SELECT current_user, current_database(), "
                    "(SELECT max(version) FROM batchforge_schema_migrations) AS schema_version, "
                    "has_table_privilege(current_user,'tasks','SELECT') AS can_read_tasks, "
                    "has_table_privilege(current_user,'tasks','DELETE') AS can_delete_tasks"
                ).fetchone()
                if row != {
                    "current_user": RUNTIME,
                    "current_database": DATABASE,
                    "schema_version": 11,
                    "can_read_tasks": True,
                    "can_delete_tasks": False,
                }:
                    raise RuntimeError("Runtime role verification did not match the expected least-privilege policy")
            PHASE = "private runtime config publication"
            os.chmod(tmp, 0o600)
            os.replace(tmp, RUNTIME_ENV)
        finally:
            if tmp.exists():
                tmp.unlink()

        print(f"Azure validation database ready; schema v11; applied migrations: {applied or 'already current'}.")
        print("Runtime login, TLS verification and least-privilege checks passed.")
        print("A root-only azure-runtime.env was written for validation only; serving APIs were not changed.")
        return 0
    finally:
        os.environ.pop("PGPASSFILE", None)
        try:
            pgpass.unlink()
        except FileNotFoundError:
            pass


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"Azure validation setup failed during {PHASE} ({type(exc).__name__}); secrets and connection details were suppressed.", file=sys.stderr)
        raise SystemExit(1) from None
