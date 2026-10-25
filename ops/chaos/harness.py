"""Scale and chaos lab harness (R0 happy path; faults are added in later rungs).

    python -m ops.chaos.harness --workers 10 --tasks 10000 --out docs/evidence/scale-chaos/R0

Runs against an isolated PostgreSQL, applies the real migrations, starts the real
API as a subprocess, drives simulated workers through the worker contract, then
checks the invariants in SQL and writes an evidence pack.
"""
from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import hashlib
import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import psycopg

from . import pgcluster
from .worker_sim import Ledger, run_worker

MODEL = "lab-model"
API_KEY = "lab-tenant-key-0123456789abcdef"
WORKER_TOKEN = "lab-worker-token-0123456789abcdef"
REPO = Path(__file__).resolve().parents[2]


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def tasks_jsonl(count: int) -> bytes:
    lines = []
    for index in range(count):
        lines.append(json.dumps({"custom_id": f"t{index:07d}", "method": "POST", "url": "/v1/chat/completions",
                                 "body": {"model": MODEL, "messages": [{"role": "user", "content": f"task {index}"}],
                                          "max_tokens": 64}}, separators=(",", ":")))
    return ("\n".join(lines) + "\n").encode()


def start_api(db_url: str, port: int, log: Path) -> subprocess.Popen:
    env = dict(os.environ,
               DATABASE_URL=db_url, BATCHFORGE_API_KEY=API_KEY, BATCHFORGE_WORKER_TOKEN=WORKER_TOKEN,
               MODEL_NAME=MODEL, SQL_CLAIM_ENABLED="true", SQL_COMPLETION_ENABLED="true",
               SQL_RECOVERY_ENABLED="true", MAX_TASKS_PER_BATCH="10000", LEASE_SECONDS="60",
               DB_POOL_MAX="20")
    handle = open(log, "w")
    return subprocess.Popen([sys.executable, "-m", "uvicorn", "backend.app:app", "--host", "127.0.0.1",
                             "--port", str(port), "--log-level", "warning"],
                            cwd=REPO, env=env, stdout=handle, stderr=subprocess.STDOUT)


def wait_ready(base: str, timeout: float = 60) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if httpx.get(base + "/readyz", timeout=2).status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.2)
    raise RuntimeError("API did not become ready")


async def drive(base: str, batch_id: str, workers: int, seed: int, service_ms, ledger: Ledger,
                deadline_s: float) -> dict:
    stop = asyncio.Event()
    started = time.monotonic()
    sims = [asyncio.create_task(run_worker(i, base_url=base, token=WORKER_TOKEN, model=MODEL, slots=4,
                                           service_ms=service_ms, seed=seed, ledger=ledger, stop=stop))
            for i in range(workers)]
    samples = []
    async with httpx.AsyncClient(base_url=base, timeout=30,
                                 headers={"Authorization": f"Bearer {API_KEY}"}) as http:
        while True:
            batch = (await http.get(f"/v1/batches/{batch_id}")).json()
            samples.append({"t": round(time.monotonic() - started, 2), "status": batch["status"],
                            "counts": batch["request_counts"]})
            if batch["status"] != "in_progress":
                break
            if time.monotonic() - started > deadline_s:
                raise RuntimeError("batch did not drain before the deadline")
            await asyncio.sleep(1.0)
    stop.set()
    await asyncio.gather(*sims)
    return {"wall_s": round(time.monotonic() - started, 2), "samples": samples}


def check_invariants(db_url: str, batch_id: str, ledger: Ledger, expected: int) -> list[dict]:
    results = []

    def check(name: str, ok: bool, detail) -> None:
        results.append({"invariant": name, "ok": bool(ok), "detail": detail})

    with psycopg.connect(db_url, row_factory=psycopg.rows.dict_row) as conn:
        batch = conn.execute("SELECT * FROM batches WHERE id=%s", (batch_id,)).fetchone()
        states = conn.execute("SELECT status, count(*) AS n, sum(prompt_tokens) AS p, sum(completion_tokens) AS c "
                              "FROM tasks WHERE batch_id=%s GROUP BY status", (batch_id,)).fetchall()
        by_state = {row["status"]: row for row in states}
        completed = by_state.get("completed", {"n": 0, "p": 0, "c": 0})
        live = sum(by_state.get(s, {"n": 0})["n"] for s in ("pending", "running"))
        multi = conn.execute("SELECT count(*) FROM (SELECT task_id FROM attempts WHERE batch_id=%s AND status='completed' "
                             "GROUP BY task_id HAVING count(*) > 1) x", (batch_id,)).fetchone()["count"]
        db_ids = {row["id"] for row in conn.execute(
            "SELECT id FROM tasks WHERE batch_id=%s AND status='completed'", (batch_id,)).fetchall()}

    accepted_ids = set(ledger.accepted)
    check("every submitted task ends terminal", live == 0 and sum(v["n"] for v in states) == expected,
          {"live": live, "total": sum(v["n"] for v in states), "expected": expected})
    check("exactly-once acceptance (no task with two completed attempts)", multi == 0, {"tasks_with_two": multi})
    check("accepted set equals completed set", accepted_ids == db_ids,
          {"workers_accepted": len(accepted_ids), "db_completed": len(db_ids),
           "only_worker": len(accepted_ids - db_ids), "only_db": len(db_ids - accepted_ids)})
    check("batch counters reconcile", batch["completed_count"] == completed["n"] and batch["pending_count"] == 0
          and batch["running_count"] == 0, {"batch_completed": batch["completed_count"], "tasks_completed": completed["n"]})
    worker_completion = sum(v["completion_tokens"] for v in ledger.accepted.values())
    worker_prompt = sum(v["prompt_tokens"] for v in ledger.accepted.values())
    check("token totals match worker-side usage",
          batch["completion_tokens"] == worker_completion == (completed["c"] or 0)
          and batch["prompt_tokens"] == worker_prompt == (completed["p"] or 0),
          {"batch_completion": batch["completion_tokens"], "worker_completion": worker_completion,
           "batch_prompt": batch["prompt_tokens"], "worker_prompt": worker_prompt})
    check("no stale or duplicate acceptance in fault-free run", ledger.stale == 0 and ledger.duplicates == 0,
          {"stale": ledger.stale, "duplicates": ledger.duplicates})
    check("worker-side errors", ledger.errors == 0, {"errors": ledger.errors})
    return results


def submit(http: httpx.Client, payload: bytes, tasks: int) -> dict:
    uploaded = http.post("/v1/files", data={"purpose": "batch"},
                         files={"file": ("tasks.jsonl", payload, "application/jsonl")})
    uploaded.raise_for_status()
    created = http.post("/v1/batches", json={"input_file_id": uploaded.json()["id"], "endpoint": "/v1/chat/completions",
                                             "completion_window": "24h", "metadata": {"lab": "R0"}})
    if created.status_code == 200:
        return {"status": 200, "batch_id": created.json()["id"], "tasks": tasks}
    body = created.json().get("error", {})
    return {"status": created.status_code, "batch_id": None, "tasks": tasks,
            "code": body.get("code"), "message": body.get("message")}


def raise_retention_limits(db_url: str, payload_gib: int) -> dict:
    """Lab-only: the lab operator raises the tenant payload budget so R0 can run. Recorded as a deviation."""
    budget = payload_gib * 1024**3
    with psycopg.connect(db_url, autocommit=True) as conn:
        conn.execute("UPDATE tenant_retention_limits SET max_payload_bytes=%s, max_retained_tasks=%s "
                     "WHERE tenant_id='default'", (budget, 1_000_000))
    return {"max_payload_bytes": budget, "max_retained_tasks": 1_000_000, "deviation_from_default": True}


def replay_check(base: str, ledger: Ledger, sample: int = 50) -> dict:
    """Replaying an accepted completion with its own token must be idempotent and change nothing."""
    items = list(ledger.accepted.items())[:sample]
    duplicate = 0
    other = 0
    with httpx.Client(base_url=base, headers={"Authorization": f"Bearer {WORKER_TOKEN}"}, timeout=30) as http:
        for task_id, info in items:
            reply = http.post(f"/internal/tasks/{task_id}/complete", json={
                "worker_id": info["worker_id"], "attempt_token": info["attempt_token"],
                "response": {"choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}}],
                             "usage": {"prompt_tokens": info["prompt_tokens"], "completion_tokens": info["completion_tokens"]}}})
            if reply.status_code == 200 and reply.json().get("duplicate"):
                duplicate += 1
            else:
                other += 1
    return {"replayed": len(items), "idempotent": duplicate, "unexpected": other}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--tasks", type=int, default=10000)
    parser.add_argument("--seed", type=int, default=20261025)
    parser.add_argument("--service-ms", type=float, nargs=2, default=(5.0, 40.0))
    parser.add_argument("--deadline-s", type=float, default=1800.0)
    parser.add_argument("--retention-payload-gib", type=int, default=16)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)
    out = args.out
    out.mkdir(parents=True, exist_ok=True)

    cluster = pgcluster.start()
    api = None
    try:
        from backend.migrate import SCHEMA_VERSION, migrate_database
        db_url = cluster.url
        applied = migrate_database(db_url, bootstrap_api_key=API_KEY)
        port = free_port()
        base = f"http://127.0.0.1:{port}"
        api = start_api(db_url, port, out / "api.log")
        wait_ready(base)
        with httpx.Client(base_url=base, timeout=120, headers={"Authorization": f"Bearer {API_KEY}"}) as http:
            payload = tasks_jsonl(args.tasks)
            # Probe: production-default retention limits, as shipped in migration 0009.
            probe = submit(http, payload, args.tasks)
            raised = raise_retention_limits(db_url, args.retention_payload_gib)
            batch_id = probe["batch_id"] or submit(http, payload, args.tasks)["batch_id"]
            probe_record = {k: v for k, v in probe.items() if k != "batch_id"}
            probe_record["retried_after_raise"] = probe["batch_id"] is None

        ledger = Ledger()
        timing = asyncio.run(drive(base, batch_id, args.workers, args.seed, tuple(args.service_ms), ledger,
                                   args.deadline_s))
        replay = replay_check(base, ledger)
        invariants = check_invariants(db_url, batch_id, ledger, args.tasks)
        passed = all(item["ok"] for item in invariants) and replay["unexpected"] == 0 \
            and replay["idempotent"] == replay["replayed"]
        summary = {"default_limit_probe": probe_record, "retention_limits_raised": raised, "rung": "R0", "workers": args.workers, "tasks": args.tasks,
                   "wall_s": timing["wall_s"], "throughput_tasks_per_s": round(args.tasks / timing["wall_s"], 1),
                   "claims": ledger.claims, "replay": replay, "invariants": invariants, "passed": passed,
                   "migrations_applied": applied, "schema_version": SCHEMA_VERSION}
        config = {"simulated": ["workers", "inference"], "real": ["api", "postgresql", "sql_paths"],
                  "workers": args.workers, "tasks": args.tasks, "seed": args.seed,
                  "service_ms": list(args.service_ms), "slots_per_worker": 4,
                  "sql_claim": True, "sql_completion": True, "sql_recovery": True,
                  "lease_seconds": 60, "git_sha": subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO,
                                                                 capture_output=True, text=True).stdout.strip(),
                  "started_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")}
        (out / "config.json").write_text(json.dumps(config, indent=1))
        (out / "summary.json").write_text(json.dumps(summary, indent=1))
        (out / "observer.jsonl").write_text("\n".join(json.dumps(s) for s in timing["samples"]) + "\n")
        print(json.dumps({"passed": passed, "wall_s": timing["wall_s"],
                          "throughput_tasks_per_s": summary["throughput_tasks_per_s"]}))
        return 0 if passed else 1
    finally:
        if api is not None:
            api.terminate()
            try:
                api.wait(timeout=20)
            except subprocess.TimeoutExpired:
                api.kill()
        pgcluster.stop(cluster)


if __name__ == "__main__":
    raise SystemExit(main())
