"""Scale and chaos lab harness (control-plane only; inference and workers are simulated).

    python -m ops.chaos.harness --workers 10 --tasks 10000 --out docs/evidence/scale-chaos/R0

Runs against an isolated PostgreSQL, applies the real migrations, starts the real
API as a subprocess, drives simulated workers through the worker contract, then
checks invariants in SQL and writes an evidence pack.

Facts about the run are recorded when they happen: start and finish times, the
hash of the code that ran, whether the git tree was dirty, and every worker-side
error with the operation and status or exception that produced it. Lab-only
deviations from production defaults are written next to the results. Nothing is
turned into a pass by changing a threshold; a failed invariant fails the run.
"""
from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import hashlib
import json
import os
import socket
import statistics
import subprocess
import sys
import time
from pathlib import Path

import httpx
import psycopg
from psycopg.rows import dict_row

from . import pgcluster
from .worker_sim import Ledger, merge, run_worker, run_worker_group

MODEL = "lab-model"
API_KEY = "lab-tenant-key-0123456789abcdef"
WORKER_TOKEN = "lab-worker-token-0123456789abcdef"
REPO = Path(__file__).resolve().parents[2]
PRODUCTION_MAX_TASKS_PER_BATCH = 10000
PRODUCTION_MAX_QUEUED_TASKS = 100000
PRODUCTION_RETAINED_TASKS = 100000
PRODUCTION_PAYLOAD_BYTES = 1024**3
LAB_TASK_LIMIT = 1_000_000  # lab-only queue and retained-task budget so a 100k-task run is not refused by the queue
CODE_PATHS = ("ops/chaos", "backend")
CLIENT_LAG_LIMIT_MS = 50.0


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds")


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def code_fingerprint() -> dict:
    """Hash of every tracked source file the run depends on, plus git state."""
    digest = hashlib.sha256()
    files = sorted(p for rel in CODE_PATHS for p in (REPO / rel).rglob("*")
                   if p.is_file() and p.suffix in {".py", ".sql"} and "__pycache__" not in p.parts)
    for path in files:
        digest.update(str(path.relative_to(REPO)).encode())
        digest.update(path.read_bytes())
    git = lambda *args: subprocess.run(["git", *args], cwd=REPO, capture_output=True, text=True).stdout.strip()
    dirty = git("status", "--porcelain", "--", *CODE_PATHS)
    return {"code_sha256": digest.hexdigest(), "files_hashed": len(files), "git_head": git("rev-parse", "HEAD"),
            "git_tree_dirty_in_code_paths": bool(dirty), "dirty_paths": dirty.splitlines()[:50]}


def environment() -> dict:
    from pgserver._commands import POSTGRES_BIN_PATH

    pg = subprocess.run([str(Path(POSTGRES_BIN_PATH) / "postgres"), "--version"], capture_output=True, text=True)
    return {"python": sys.version.split()[0], "platform": sys.platform, "cpu_count": os.cpu_count(),
            "host_type": "local Mac, isolated PostgreSQL (not GPU hardware, not managed Neon)",
            "postgres": pg.stdout.strip() or "unknown"}


def tasks_jsonl(start: int, count: int) -> bytes:
    lines = []
    for index in range(start, start + count):
        lines.append(json.dumps({"custom_id": f"t{index:07d}", "method": "POST", "url": "/v1/chat/completions",
                                 "body": {"model": MODEL, "messages": [{"role": "user", "content": f"task {index}"}],
                                          "max_tokens": 64}}, separators=(",", ":")))
    return ("\n".join(lines) + "\n").encode()


def start_api(db_url: str, port: int, log: Path, max_queued: int, lease_seconds: int, pool: int) -> subprocess.Popen:
    env = dict(os.environ,
               DATABASE_URL=db_url, BATCHFORGE_API_KEY=API_KEY, BATCHFORGE_WORKER_TOKEN=WORKER_TOKEN,
               MODEL_NAME=MODEL, SQL_CLAIM_ENABLED="true", SQL_COMPLETION_ENABLED="true",
               SQL_RECOVERY_ENABLED="true", MAX_TASKS_PER_BATCH=str(PRODUCTION_MAX_TASKS_PER_BATCH),
               MAX_QUEUED_TASKS_PER_TENANT=str(max_queued), LEASE_SECONDS=str(lease_seconds),
               DB_POOL_MAX=str(pool))
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


def submit(http: httpx.Client, payload: bytes, tasks: int, label: str) -> dict:
    uploaded = http.post("/v1/files", data={"purpose": "batch"},
                         files={"file": ("tasks.jsonl", payload, "application/jsonl")})
    uploaded.raise_for_status()
    created = http.post("/v1/batches", json={"input_file_id": uploaded.json()["id"], "endpoint": "/v1/chat/completions",
                                             "completion_window": "24h", "metadata": {"lab": label}})
    if created.status_code == 200:
        return {"status": 200, "batch_id": created.json()["id"], "tasks": tasks}
    body = created.json().get("error", {})
    return {"status": created.status_code, "batch_id": None, "tasks": tasks,
            "code": body.get("code"), "message": body.get("message")}


def raise_retention(db_url: str, payload_bytes: int, retained: int) -> dict:
    """Lab-only: the operator raises the tenant retention budget. Recorded as a deviation."""
    with psycopg.connect(db_url, autocommit=True) as conn:
        conn.execute("UPDATE tenant_retention_limits SET max_payload_bytes=%s, max_retained_tasks=%s "
                     "WHERE tenant_id='default'", (payload_bytes, retained))
    return {"max_payload_bytes": payload_bytes, "max_retained_tasks": retained, "deviation_from_default": True}


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round(pct / 100 * (len(ordered) - 1))))
    return round(ordered[index], 3)


def latency_summary(samples: dict[str, list[float]]) -> dict:
    return {op: {"n": len(values), "p50_ms": percentile(values, 50), "p95_ms": percentile(values, 95),
                 "p99_ms": percentile(values, 99), "max_ms": round(max(values), 3), "mean_ms": round(statistics.fmean(values), 3)}
            for op, values in samples.items() if values}


async def observe(base: str, batch_ids: list[str], deadline_s: float) -> list[dict]:
    """Poll batch state once a second until every batch has drained."""
    started = time.monotonic()
    samples = []
    async with httpx.AsyncClient(base_url=base, timeout=30,
                                 headers={"Authorization": f"Bearer {API_KEY}"}) as http:
        while True:
            tick = time.monotonic()
            states = [(await http.get(f"/v1/batches/{bid}")).json() for bid in batch_ids]
            samples.append({"t": round(tick - started, 2),
                            "in_progress": sum(1 for s in states if s["status"] == "in_progress"),
                            "completed_tasks": sum(s["request_counts"]["completed"] for s in states),
                            "failed_tasks": sum(s["request_counts"]["failed"] for s in states)})
            if all(s["status"] != "in_progress" for s in states):
                return samples
            if time.monotonic() - started > deadline_s:
                raise RuntimeError("batches did not drain before the deadline")
            await asyncio.sleep(1.0)


def drive(base: str, batch_ids: list[str], workers: int, processes: int, options: dict, deadline_s: float) -> dict:
    """Run the simulated fleet in `processes` OS processes and observe from this process."""
    import multiprocessing

    context = multiprocessing.get_context("spawn")
    stop_flag = context.Event()
    results = context.Queue()
    shares = [len(range(i, workers, processes)) for i in range(processes)]
    groups = []
    first = 0
    for share in shares:
        groups.append(context.Process(target=run_worker_group, args=(first, share, options, stop_flag, results)))
        first += share
    started = time.monotonic()
    for group in groups:
        group.start()
    try:
        samples = asyncio.run(observe(base, batch_ids, deadline_s))
    finally:
        stop_flag.set()
    collected = [results.get(timeout=120) for _ in groups]
    for group in groups:
        group.join(timeout=60)
    wall = round(time.monotonic() - started, 2)
    ledger = merge([item["ledger"] for item in collected])
    overrun = [value for item in collected for value in item["loop_overrun_ms"]]
    return {"wall_s": wall, "samples": samples, "ledger": ledger, "loop_overrun_ms": overrun,
            "processes": [{"workers": item["workers"], "cpu_s": item["cpu_s"],
                           "loop_overrun_p99_ms": percentile(item["loop_overrun_ms"], 99)} for item in collected]}


def check_invariants(db_url: str, batch_ids: list[str], ledger: Ledger, expected: int) -> tuple[list[dict], dict]:
    results = []
    facts: dict = {}

    def check(name: str, ok: bool, detail) -> None:
        results.append({"invariant": name, "ok": bool(ok), "detail": detail})

    with psycopg.connect(db_url, row_factory=dict_row) as conn:
        batches = conn.execute("SELECT * FROM batches WHERE id = ANY(%s)", (batch_ids,)).fetchall()
        states = conn.execute("SELECT status, count(*) AS n, sum(prompt_tokens) AS p, sum(completion_tokens) AS c "
                              "FROM tasks WHERE batch_id = ANY(%s) GROUP BY status", (batch_ids,)).fetchall()
        multi = conn.execute("SELECT count(*) FROM (SELECT task_id FROM attempts WHERE batch_id = ANY(%s) "
                             "AND status='completed' GROUP BY task_id HAVING count(*) > 1) x",
                             (batch_ids,)).fetchone()["count"]
        db_ids = {row["id"] for row in conn.execute(
            "SELECT id FROM tasks WHERE batch_id = ANY(%s) AND status='completed'", (batch_ids,)).fetchall()}
        attempts = {row["status"]: row["n"] for row in conn.execute(
            "SELECT status, count(*) AS n FROM attempts WHERE batch_id = ANY(%s) GROUP BY status", (batch_ids,)).fetchall()}

    by_state = {row["status"]: row for row in states}
    total = sum(row["n"] for row in states)
    live = sum(by_state.get(s, {"n": 0})["n"] for s in ("pending", "running"))
    completed_p = sum((row["p"] or 0) for row in states if row["status"] == "completed")
    completed_c = sum((row["c"] or 0) for row in states if row["status"] == "completed")
    completed_n = by_state.get("completed", {"n": 0})["n"]
    batch_completed = sum(b["completed_count"] for b in batches)
    batch_pending = sum(b["pending_count"] for b in batches)
    batch_running = sum(b["running_count"] for b in batches)
    batch_prompt = sum(b["prompt_tokens"] for b in batches)
    batch_completion = sum(b["completion_tokens"] for b in batches)
    accepted_ids = set(ledger.accepted)
    worker_completion = sum(v["completion_tokens"] for v in ledger.accepted.values())
    worker_prompt = sum(v["prompt_tokens"] for v in ledger.accepted.values())

    facts.update({"task_states": {k: v["n"] for k, v in by_state.items()}, "attempt_states": attempts,
                  "claims_worker_side": ledger.claims, "tasks_submitted": expected,
                  "reclaims_beyond_one_per_task": ledger.claims - expected,
                  "expired_attempts_db": attempts.get("expired", 0)})

    check("every submitted task ends terminal", live == 0 and total == expected,
          {"live": live, "total": total, "expected": expected})
    check("exactly-once acceptance (no task with two completed attempts)", multi == 0, {"tasks_with_two": multi})
    check("accepted set equals completed set", accepted_ids == db_ids,
          {"workers_accepted": len(accepted_ids), "db_completed": len(db_ids),
           "only_worker": len(accepted_ids - db_ids), "only_db": len(db_ids - accepted_ids)})
    check("batch counters reconcile", batch_completed == completed_n and batch_pending == 0 and batch_running == 0,
          {"batch_completed": batch_completed, "tasks_completed": completed_n, "batch_pending": batch_pending,
           "batch_running": batch_running})
    check("token totals match worker-side usage",
          batch_completion == worker_completion == completed_c and batch_prompt == worker_prompt == completed_p,
          {"batch_completion": batch_completion, "worker_completion": worker_completion,
           "batch_prompt": batch_prompt, "worker_prompt": worker_prompt})
    check("no stale or duplicate acceptance", ledger.stale == 0 and ledger.duplicates == 0,
          {"stale": ledger.stale, "duplicates": ledger.duplicates, "stale_by_code": dict(ledger.stale_by_code)})
    check("worker-side errors", ledger.errors == 0,
          {"errors": ledger.errors, "errors_by_kind": dict(ledger.errors_by_kind)})
    return results, facts


class SubmissionRefused(RuntimeError):
    """The API refused a batch submission; the run is recorded as NOT_RUN with the refusal, not retried around."""


def retention_usage(db_url: str) -> dict:
    with psycopg.connect(db_url, row_factory=dict_row) as conn:
        row = conn.execute("SELECT retained_tasks, charged_payload_bytes FROM tenant_retention_usage "
                           "WHERE tenant_id='default'").fetchone()
    return dict(row) if row else {}


def submit_all(http: httpx.Client, tasks: int, chunk: int, label: str, start: int = 0,
               first_chunk: int = 0) -> tuple[list[dict], list[str]]:
    submissions, batch_ids = [], []
    for offset in range(0, tasks, chunk):
        size = min(chunk, tasks - offset)
        record = submit(http, tasks_jsonl(start + offset, size), size, label)
        submissions.append({"chunk": first_chunk + offset // chunk, "tasks": size, "status": record["status"],
                            "batch_id": record["batch_id"], "code": record.get("code"), "at": "lab_budget"})
        if record["batch_id"] is None:
            raise SubmissionRefused(record)
        batch_ids.append(record["batch_id"])
    return submissions, batch_ids


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


def submit_phase(base: str, db_url: str, args, retained: int, deviations: dict):
    """Submit every chunk. The first chunk is tried at production retention defaults first."""
    with httpx.Client(base_url=base, timeout=120, headers={"Authorization": f"Bearer {API_KEY}"}) as http:
        # First chunk is submitted at production retention defaults. If the budget refuses it, the
        # refusal is recorded, the lab raises the budget, and the chunk is submitted again.
        first = min(args.chunk, args.tasks)
        probe = submit(http, tasks_jsonl(0, first), first, args.rung)
        submissions = [{"chunk": 0, "tasks": first, "status": probe["status"], "batch_id": probe["batch_id"],
                        "code": probe.get("code"), "at": "production_defaults"}]
        raised = raise_retention(db_url, args.retention_payload_gib * 1024**3, retained)
        deviations["retention_payload_bytes"]["lab"] = raised["max_payload_bytes"]
        batch_ids = [probe["batch_id"]] if probe["batch_id"] else []
        if not probe["batch_id"]:
            retry = submit(http, tasks_jsonl(0, first), first, args.rung)
            submissions[0].update({"retry_status": retry["status"], "batch_id": retry["batch_id"]})
            if retry["batch_id"] is None:
                raise SubmissionRefused(retry)
            batch_ids.append(retry["batch_id"])
        if args.tasks > first:
            more, more_ids = submit_all(http, args.tasks - first, args.chunk, args.rung, start=first,
                                        first_chunk=1)
            submissions += more
            batch_ids += more_ids
    return probe, submissions, batch_ids, raised


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rung", default="R0")
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--slots", type=int, default=4)
    parser.add_argument("--tasks", type=int, default=10000)
    parser.add_argument("--chunk", type=int, default=PRODUCTION_MAX_TASKS_PER_BATCH)
    parser.add_argument("--seed", type=int, default=20261025)
    parser.add_argument("--lease-seconds", type=int, default=60)
    parser.add_argument("--service-ms", type=float, nargs=2, default=(5.0, 40.0))
    parser.add_argument("--deadline-s", type=float, default=1800.0)
    parser.add_argument("--api-pool", type=int, default=20)
    parser.add_argument("--retention-payload-gib", type=int, default=16)
    parser.add_argument("--processes", type=int, default=4, help="OS processes that host the simulated workers")
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)
    out = args.out
    out.mkdir(parents=True, exist_ok=True)

    started_utc = utc_now()
    fingerprint = code_fingerprint()
    retained = LAB_TASK_LIMIT
    max_queued = LAB_TASK_LIMIT
    deviations = {
        "max_queued_tasks_per_tenant": {"production": PRODUCTION_MAX_QUEUED_TASKS, "lab": max_queued},
        "retention_payload_bytes": {"production": PRODUCTION_PAYLOAD_BYTES, "lab": None},
        "retention_max_retained_tasks": {"production": PRODUCTION_RETAINED_TASKS, "lab": retained},
    }

    cluster = pgcluster.start()
    api = None
    try:
        from backend.migrate import SCHEMA_VERSION, migrate_database
        db_url = cluster.url
        applied = migrate_database(db_url, bootstrap_api_key=API_KEY)
        port = free_port()
        base = f"http://127.0.0.1:{port}"
        api = start_api(db_url, port, out / "api.log", max_queued, args.lease_seconds, args.api_pool)
        wait_ready(base)
        try:
            probe, submissions, batch_ids, raised = submit_phase(base, db_url, args, retained, deviations)
        except SubmissionRefused as refusal:
            record = {"rung": args.rung, "verdict": "NOT_RUN: submission refused by the API",
                      "refusal": refusal.args[0], "retention_usage": retention_usage(db_url),
                      "started_utc": started_utc, "finished_utc": utc_now(), "lab_deviations": deviations}
            (out / "summary.json").write_text(json.dumps(record, indent=1) + "\n")
            print(json.dumps(record["verdict"]))
            return 2
        usage_after_submit = retention_usage(db_url)
        probe_record = {k: v for k, v in probe.items() if k != "batch_id"}
        probe_record["retried_after_raise"] = not probe["batch_id"]

        options = {"base_url": base, "token": WORKER_TOKEN, "model": MODEL, "slots": args.slots,
                   "service_ms": tuple(args.service_ms), "seed": args.seed}
        timing = drive(base, batch_ids, args.workers, args.processes, options, args.deadline_s)
        ledger = timing["ledger"]
        replay = replay_check(base, ledger)
        invariants, facts = check_invariants(db_url, batch_ids, ledger, args.tasks)
        passed = all(item["ok"] for item in invariants) and replay["unexpected"] == 0 \
            and replay["idempotent"] == replay["replayed"]
        finished_utc = utc_now()
        lag_p99 = percentile(timing["loop_overrun_ms"], 99)
        # Design stop rule (docs/design/scale-chaos-lab.md section 8): a run whose load generator
        # (the simulated fleet) cannot keep up is not a throughput result, whatever the invariants say.
        measurement_valid = lag_p99 is not None and lag_p99 < CLIENT_LAG_LIMIT_MS
        if not passed:
            verdict = "FAIL"
        elif not measurement_valid:
            verdict = f"INVALID: generator loop overrun p99 {lag_p99} ms >= {CLIENT_LAG_LIMIT_MS} ms"
        else:
            verdict = "PASS"
        passed = passed and measurement_valid
        summary = {
            "rung": args.rung, "verdict": verdict, "measurement_valid": measurement_valid,
            "workers": args.workers, "slots_per_worker": args.slots, "tasks": args.tasks,
            "batches": len(batch_ids), "wall_s": timing["wall_s"],
            "throughput_tasks_per_s": round(args.tasks / timing["wall_s"], 1),
            "started_utc": started_utc, "finished_utc": finished_utc,
            "default_limit_probe": probe_record, "lab_deviations": deviations,
            "tenant_retention_usage_after_submit": usage_after_submit,
            "retention_limits_raised": raised,
            "facts": facts, "simulator_processes": timing["processes"], "worker_latency_ms": latency_summary(ledger.latency_ms),
            "worker_errors_by_kind": dict(ledger.errors_by_kind),
            "heartbeats_ok": ledger.heartbeats_ok, "heartbeats_failed": ledger.heartbeats_failed,
            "generator_loop_overrun_p99_ms": lag_p99, "generator_loop_overrun_limit_ms": CLIENT_LAG_LIMIT_MS,
            "replay": replay, "invariants": invariants,
            "migrations_applied": applied, "schema_version": SCHEMA_VERSION,
        }
        config = {
            "rung": args.rung, "simulated": ["workers", "inference", "engine"],
            "real": ["api", "postgresql", "sql_paths", "migrations"],
            "workers": args.workers, "slots_per_worker": args.slots, "tasks": args.tasks,
            "chunk_per_batch": args.chunk, "seed": args.seed, "service_ms": list(args.service_ms),
            "heartbeat": "advertised interval (lease_seconds // 3), per in-flight task",
            "lease_seconds": args.lease_seconds, "api_db_pool_max": args.api_pool,
            "sql_claim": True, "sql_completion": True, "sql_recovery": True,
            "submissions": submissions, "environment": environment(), **fingerprint,
            "started_utc": started_utc,
        }
        (out / "config.json").write_text(json.dumps(config, indent=1) + "\n")
        (out / "summary.json").write_text(json.dumps(summary, indent=1) + "\n")
        (out / "observer.jsonl").write_text("\n".join(json.dumps(s) for s in timing["samples"]) + "\n")
        print(json.dumps({"verdict": summary["verdict"], "wall_s": timing["wall_s"],
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
