#!/usr/bin/env python3
"""Actual HTTP + subprocess crash test on a newly created disposable PostgreSQL DB.

Requires TEST_DATABASE_URL with CREATEDB privileges. Does not truncate an existing
database. Kills only subprocesses this invocation creates. No GPU is used.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import uuid

import httpx
import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo

ROOT = Path(__file__).resolve().parent.parent


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_for(check, deadline=30):
    end = time.monotonic() + deadline
    while time.monotonic() < end:
        try:
            result = check()
            if result:
                return result
        except (httpx.HTTPError, ValueError, KeyError):
            pass
        time.sleep(.1)
    raise RuntimeError("Timed out waiting for HTTP chaos-test condition")


def run(args):
    admin_dsn = os.environ.get("TEST_DATABASE_URL")
    if not admin_dsn:
        raise SystemExit("TEST_DATABASE_URL to a disposable PostgreSQL server is required")
    dbname = "batchforge_chaos_" + uuid.uuid4().hex[:16]
    dsn = make_conninfo(admin_dsn, dbname=dbname, options="-c client_encoding=UTF8")
    with psycopg.connect(admin_dsn, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
    processes = []
    started = time.monotonic()
    api_port, engine_port, proxy_port, metrics_port = port(), port(), port(), port()
    url = f"http://127.0.0.1:{api_port}"
    worker_key, public_key = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
    env = dict(os.environ, DATABASE_URL=dsn, BATCHFORGE_API_KEY=public_key,
        BATCHFORGE_WORKER_TOKEN=worker_key, MODEL_NAME="test-model", LEASE_SECONDS="5",
        RETRY_BASE_SECONDS="1", MAX_ATTEMPTS="3", BATCHFORGE_TEST_ENGINE="1",
        TEST_ENGINE_DELAY_SECONDS="1", CONTROL_PLANE_URL=url, INFERENCE_BASE_URLS=f"http://127.0.0.1:{engine_port}",
        WORKER_MAX_INFLIGHT="2", ENGINE_MAX_MODEL_LEN="2048", WORKER_TOKEN_BUDGET="4096",
        WORKER_METRICS_PORT=str(metrics_port), WORKER_POLL_SECONDS="0.05", PYTHONUNBUFFERED="1")
    try:
        logs = Path(tempfile.mkdtemp(prefix="batchforge-http-chaos-"))

        def start(name, module, extra=(), overrides=None):
            output = (logs / f"{name}.log").open("wb")
            child = subprocess.Popen([sys.executable, "-m", module, *extra], cwd=ROOT,
                env={**env, **(overrides or {})}, stdin=subprocess.DEVNULL,
                stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
            output.close()
            processes.append(child)
            return child

        def stop(child, sig=signal.SIGTERM):
            if child.poll() is None:
                os.killpg(child.pid, sig)
                try:
                    child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(child.pid, signal.SIGKILL)
                    child.wait(timeout=5)

        subprocess.run([sys.executable, "-m", "backend.migrate", "--bootstrap"], cwd=ROOT,
                       env=env, check=True, stdout=subprocess.DEVNULL)
        start("api", "uvicorn", ("backend.app:app", "--host", "127.0.0.1", "--port", str(api_port), "--no-access-log"))
        start("engine", "uvicorn", ("worker.fake_engine:app", "--host", "127.0.0.1", "--port", str(engine_port), "--no-access-log"))
        public = httpx.Client(base_url=url, headers={"Authorization": f"Bearer {public_key}"}, timeout=5)
        internal = httpx.Client(base_url=url, headers={"Authorization": f"Bearer {worker_key}"}, timeout=5)
        wait_for(lambda: public.get("/readyz").status_code == 200)
        wait_for(lambda: httpx.get(f"http://127.0.0.1:{engine_port}/health").status_code == 200)

        def submit(label, count):
            rows = [{"custom_id": f"{label}-{i}", "method": "POST", "url": "/v1/chat/completions", "body": {
                "model": "test-model", "messages": [{"role": "user", "content": f"TEST {label} {i}"}], "max_tokens": 16}} for i in range(count)]
            upload = public.post("/v1/files", data={"purpose": "batch"}, files={"file": ("test.jsonl", "\n".join(json.dumps(row) for row in rows) + "\n", "application/jsonl")})
            upload.raise_for_status()
            result = public.post("/v1/batches", json={"input_file_id": upload.json()["id"], "endpoint": "/v1/chat/completions", "completion_window": "24h", "metadata": {"test_only": "true"}})
            result.raise_for_status()
            return result.json()["id"]

        def get_batch(bid):
            response = public.get(f"/v1/batches/{bid}")
            response.raise_for_status()
            return response.json()

        def terminal(bid):
            batch = get_batch(bid)
            return batch if batch["status"] in {"completed", "failed", "cancelled"} else None

        def assert_outputs(bid, expected):
            batch = wait_for(lambda: terminal(bid), deadline=40)
            assert batch["status"] == "completed", batch
            assert batch["request_counts"]["completed"] == expected, batch
            content = public.get(f"/v1/files/{batch['output_file_id']}/content")
            content.raise_for_status()
            rows = [json.loads(line) for line in content.text.splitlines()]
            assert len(rows) == len({row["custom_id"] for row in rows}) == expected
            assert all(row["response"]["body"].get("batchforge_test_only") for row in rows)

        worker_one = start("worker-one", "worker", overrides={"WORKER_ID": "chaos-worker-one"})
        wait_for(lambda: httpx.get(f"http://127.0.0.1:{metrics_port}/readyz").status_code == 200)
        crash_batch = submit("crash", 6)
        wait_for(lambda: get_batch(crash_batch)["request_counts"]["running"] > 0)
        wait_for(lambda: httpx.get(f"http://127.0.0.1:{engine_port}/test/state").json()["started"] > 0)
        with psycopg.connect(dsn) as conn:
            old_id, old_token = conn.execute("SELECT id,attempt_token FROM tasks WHERE batch_id=%s AND status='running' LIMIT 1", (crash_batch,)).fetchone()
        stop(worker_one, signal.SIGKILL)
        worker_two = start("worker-two", "worker", overrides={"WORKER_ID": "chaos-worker-two"})
        assert_outputs(crash_batch, 6)
        stale = internal.post(f"/internal/tasks/{old_id}/complete", json={"worker_id": "chaos-worker-one", "attempt_token": old_token,
            "response": {"choices": [{"message": {"role": "assistant", "content": "STALE TEST"}}], "usage": {"prompt_tokens": 1, "completion_tokens": 1}, "batchforge_test_only": True}})
        assert stale.status_code == 409, stale.text
        stop(worker_two)

        start("proxy", "uvicorn", ("worker.test_proxy:app", "--host", "127.0.0.1", "--port", str(proxy_port), "--no-access-log"),
              {"TEST_PROXY_UPSTREAM": url})
        proxy_url = f"http://127.0.0.1:{proxy_port}"
        wait_for(lambda: httpx.get(proxy_url + "/test/state").status_code == 200)
        start("worker-lost-ack", "worker", overrides={"WORKER_ID": "chaos-worker-lost-ack", "CONTROL_PLANE_URL": proxy_url})
        ack_batch = submit("lost-ack", 4)
        wait_for(lambda: httpx.get(proxy_url + "/test/state").json()["dropped_claims"] == 1)
        assert_outputs(ack_batch, 4)
        with psycopg.connect(dsn) as conn:
            recovered = conn.execute("SELECT count(*) FROM attempts WHERE status='expired'").fetchone()[0]
            max_attempts = conn.execute("SELECT max(attempts) FROM tasks").fetchone()[0]
            task_count, completed_count = conn.execute("SELECT count(*),count(*) FILTER(WHERE status='completed') FROM tasks").fetchone()
        assert recovered >= 3 and task_count == completed_count == 10 and max_attempts == 2
        public.close()
        internal.close()
        return {"schema_version": 1, "test_only": True, "observed_at": datetime.now(timezone.utc).isoformat(),
            "elapsed_seconds": round(time.monotonic() - started, 3), "transport": "real localhost HTTP",
            "worker_crash": "SIGKILL after engine received a request", "worker_restart_new_identity": True,
            "lost_claim_acknowledgement": "upstream committed; proxy discarded response and returned503",
            "stale_crashed_worker_completion_rejected": True, "expired_attempts_recovered": recovered,
            "maximum_attempts_per_task": max_attempts, "completed_tasks": completed_count,
            "unique_result_rows": 10, "test_database_created_and_dropped": True,
            "limitations": ["Synthetic test engine; no GPU used or throughput claim", "A503 proxy simulates lost claim acknowledgement after upstream commit"]}
    except Exception:
        if 'logs' in locals():
            for logfile in logs.glob("*.log"):
                print(f"--- {logfile.name} ---", file=sys.stderr)
                print("\n".join(logfile.read_text(errors="replace").splitlines()[-30:]), file=sys.stderr)
        raise
    finally:
        for child in reversed(processes):
            if child.poll() is None:
                os.killpg(child.pid, signal.SIGTERM)
                try:
                    child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(child.pid, signal.SIGKILL)
                    child.wait(timeout=5)
        with psycopg.connect(admin_dsn, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(dbname)))
        if "logs" in locals():
            shutil.rmtree(logs, ignore_errors=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, default=Path(".runtime/http-chaos.json"))
    args = parser.parse_args()
    report = run(args)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
