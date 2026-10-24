"""Synthetic lease-expiry comparison; creates/drops only uniquely named databases.

No inference, application HTTP, live application table mutation, existing-table
TRUNCATE, or GPU operation. Supply an administrative DSN through an environment
variable and explicitly allow creation of disposable databases. An isolated
database still shares its server's compute; coordinate remote measurements.
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from backend.config import Settings
from backend.store import Store
from scripts.benchmark import distribution

SOURCES = ("backend/schema.sql", "backend/migrations/0002_sql_completion.sql",
           "backend/migrations/0003_sql_claim.sql", "backend/migrations/0004_sql_failure_recovery.sql",
           "backend/store.py", "backend/config.py", "ops/ha/recovery_storm_benchmark.py")


def hashes():
    return {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in SOURCES}


def safe_errors(exc):
    if isinstance(exc, BaseExceptionGroup):
        return [item for child in exc.exceptions for item in safe_errors(child)]
    return [{"error_type": type(exc).__name__, "sqlstate": getattr(exc, "sqlstate", None)}]


def connection(dsn):
    return psycopg.connect(dsn, autocommit=True, row_factory=dict_row,
                           connect_timeout=10, options="-c statement_timeout=30000 -c lock_timeout=10000")


def seed(dsn, args):
    """Identical fixtures in each empty database; setup is outside the timer."""
    synthetic = Jsonb({"batchforge_test_only": True, "content": "SYNTHETIC; no inference"})
    with connection(dsn) as conn:
        for name in SOURCES[:4]:
            conn.execute((ROOT / name).read_text())
        for index in range(args.tenants):
            tenant = f"synthetic-tenant-{index:04}"
            conn.execute("INSERT INTO tenants(id,name) VALUES(%s,'SYNTHETIC ONLY')", (tenant,))
            conn.execute("INSERT INTO tenant_queues(tenant_id,model) VALUES(%s,'synthetic-recovery')", (tenant,))
        offset = 0
        for batch in range(args.batches):
            count = args.tasks // args.batches + int(batch < args.tasks % args.batches)
            tenant, bid, fid, wid = (f"synthetic-tenant-{batch % args.tenants:04}",
                f"batch_{batch:04}", f"file_{batch:04}", f"worker_{batch:04}")
            conn.execute("INSERT INTO workers(id,name,model,concurrency) VALUES(%s,'SYNTHETIC ONLY','synthetic-recovery',4096)", (wid,))
            conn.execute("INSERT INTO files(id,tenant_id,filename,kind,content) VALUES(%s,%s,'synthetic.jsonl','input',''::bytea)", (fid, tenant))
            # One already accepted result per batch proves recovery leaves usage
            # intact and exercises terminal output plus error-file metadata.
            conn.execute("INSERT INTO batches(id,tenant_id,input_file_id,model,status,total_count,running_count,completed_count,prompt_tokens,completion_tokens) VALUES(%s,%s,%s,'synthetic-recovery','in_progress',%s,%s,1,7,2)", (bid, tenant, fid, count + 1, count))
            rows, attempts = [], []
            for index in range(count + 1):
                accepted = index == count
                tid, token = f"task_{batch:04}_{index:06}", f"attempt_{batch:04}_{index:06}"
                status = "completed" if accepted else "running"
                rows.append((tid, tenant, bid, str(index), status, wid, token,
                             status, 7 if accepted else 0, 2 if accepted else 0,
                             synthetic if accepted else None))
                attempts.append((token, tid, bid, tenant, wid, status,
                                 7 if accepted else 0, 2 if accepted else 0))
            with conn.cursor() as cursor:
                cursor.executemany("INSERT INTO tasks(id,tenant_id,batch_id,custom_id,model,body,status,attempts,max_attempts,worker_id,attempt_token,lease_expires_at,prompt_tokens,completion_tokens,response) VALUES(%s,%s,%s,%s,'synthetic-recovery','{\"batchforge_test_only\":true}',%s,1,1,%s,%s,CASE WHEN %s='running' THEN clock_timestamp()-interval '10 seconds' END,%s,%s,%s)", rows)
                cursor.executemany("INSERT INTO attempts(token,task_id,batch_id,tenant_id,worker_id,number,status,prompt_tokens,completion_tokens) VALUES(%s,%s,%s,%s,%s,1,%s,%s,%s)", attempts)
            offset += count
        assert offset == args.tasks


def verify(dsn, args, *, terminal):
    with connection(dsn) as conn:
        state = conn.execute("SELECT count(*)::integer AS total,count(*) FILTER(WHERE status='running')::integer AS running,count(*) FILTER(WHERE status='failed')::integer AS failed,count(*) FILTER(WHERE status='completed')::integer AS completed,sum(prompt_tokens)::bigint AS prompt_tokens,sum(completion_tokens)::bigint AS completion_tokens FROM tasks").fetchone()
        attempts = conn.execute("SELECT count(*)::integer AS total,count(*) FILTER(WHERE status='failed' AND error->>'code'='lease_expired')::integer AS exhausted_expired,count(*) FILTER(WHERE status='completed')::integer AS accepted,sum(prompt_tokens)::bigint AS prompt_tokens,sum(completion_tokens)::bigint AS completion_tokens FROM attempts").fetchone()
        batches = conn.execute("SELECT count(*)::integer AS total,count(*) FILTER(WHERE status='completed')::integer AS terminal,count(*) FILTER(WHERE output_file_id IS NOT NULL AND error_file_id IS NOT NULL)::integer AS output_metadata,sum(prompt_tokens)::bigint AS prompt_tokens,sum(completion_tokens)::bigint AS completion_tokens FROM batches").fetchone()
        mismatch = conn.execute("SELECT count(*) AS n FROM batches b CROSS JOIN LATERAL(SELECT count(*) AS total,count(*) FILTER(WHERE status='pending') AS pending,count(*) FILTER(WHERE status='running') AS running,count(*) FILTER(WHERE status='completed') AS completed,count(*) FILTER(WHERE status='failed') AS failed,count(*) FILTER(WHERE status='cancelled') AS cancelled,sum(prompt_tokens) AS prompt,sum(completion_tokens) AS completion FROM tasks WHERE batch_id=b.id) t WHERE (b.total_count,b.pending_count,b.running_count,b.completed_count,b.failed_count,b.cancelled_count,b.prompt_tokens,b.completion_tokens) IS DISTINCT FROM(t.total,t.pending,t.running,t.completed,t.failed,t.cancelled,t.prompt,t.completion)").fetchone()["n"]
        queues = conn.execute("SELECT count(*) AS n FROM tenant_queues q WHERE pending_count<>(SELECT count(*) FROM tasks t WHERE t.tenant_id=q.tenant_id AND t.model=q.model AND t.status='pending')").fetchone()["n"]
        duplicates = conn.execute("SELECT count(*) AS n FROM(SELECT task_id FROM attempts WHERE status='completed' GROUP BY task_id HAVING count(*)<>1) d").fetchone()["n"]
        accepted = conn.execute("SELECT id,attempt_token,worker_id,response,prompt_tokens,completion_tokens FROM tasks WHERE status='completed' ORDER BY id").fetchall()
        digest = hashlib.sha256(json.dumps(accepted, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    expected_usage = (args.batches * 7, args.batches * 2)
    assert not mismatch and not queues and not duplicates
    assert state["total"] == attempts["total"] == args.tasks + args.batches
    assert state["completed"] == attempts["accepted"] == args.batches
    assert all((row["prompt_tokens"], row["completion_tokens"]) == expected_usage for row in (state, attempts, batches))
    if terminal:
        assert state["failed"] == attempts["exhausted_expired"] == args.tasks and state["running"] == 0
        assert batches["terminal"] == batches["output_metadata"] == args.batches
    else:
        assert state["running"] == args.tasks and state["failed"] == 0
    return {"tasks": state, "attempts": attempts, "batches": batches,
            "batch_counter_mismatches": int(mismatch), "queue_counter_mismatches": int(queues),
            "duplicate_accepted_tasks": int(duplicates), "accepted_rows_sha256": digest}


async def drain(dsn, args, optimized):
    settings = Settings(database_url=dsn, model_name="synthetic-recovery",
        sql_recovery_enabled=optimized, recovery_task_limit=args.task_limit,
        recovery_batch_limit=args.batch_limit, retry_base_seconds=1,
        blob_storage_backend="inline", scheduler_policy="legacy")
    recovered, calls, latency = 0, 0, []
    async with AsyncConnectionPool(dsn, min_size=args.sweepers, max_size=args.sweepers,
            kwargs={"autocommit": True, "row_factory": dict_row,
                    "options": "-c statement_timeout=30000 -c lock_timeout=10000"}, timeout=10) as pool:
        store = Store(pool, settings)
        start_at, began = datetime.now(timezone.utc).isoformat(), time.monotonic()

        async def sweeper():
            nonlocal recovered, calls
            while recovered < args.tasks:
                if time.monotonic() - began > args.deadline:
                    raise TimeoutError("Synthetic recovery deadline exceeded")
                started = time.monotonic()
                value = await store.recover()
                latency.append(time.monotonic() - started)
                calls += 1
                recovered += value
                if recovered > args.tasks:
                    raise AssertionError("Recovery counted a task more than once")
                if value == 0:
                    await asyncio.sleep(.002)

        async with asyncio.TaskGroup() as group:
            for _ in range(args.sweepers):
                group.create_task(sweeper())
        elapsed = time.monotonic() - began
    return {"started_at": start_at, "finished_at": datetime.now(timezone.utc).isoformat(),
            "elapsed_seconds": elapsed, "expired_attempts_recovered": recovered,
            "synthetic_recoveries_per_second": recovered / elapsed,
            "recovery_calls": calls, "call_latency_seconds": distribution(latency)}


def mode(owner_dsn, args, optimized):
    name = "bf_recovery_bench_" + uuid.uuid4().hex
    result = {"mode": "sql_v4" if optimized else "python_legacy", "created_database": name,
              "database_removed": False, "passed": False}
    created = False
    try:
        with connection(owner_dsn) as owner:
            owner.execute(sql.SQL("CREATE DATABASE {} TEMPLATE template0 ENCODING 'UTF8'").format(sql.Identifier(name)))
            created = True
        dsn = make_conninfo(**{**conninfo_to_dict(owner_dsn), "dbname": name})
        with connection(dsn) as conn:
            assert conn.info.dbname == name and name.startswith("bf_recovery_bench_")
        seed(dsn, args)
        result["before"] = verify(dsn, args, terminal=False)
        result.update(asyncio.run(drain(dsn, args, optimized)))
        result["after"] = verify(dsn, args, terminal=True)
        assert result["before"]["accepted_rows_sha256"] == result["after"]["accepted_rows_sha256"]
        result["accepted_results_and_usage_unchanged"] = True
        result["passed"] = True
    except BaseException as exc:
        result["errors"] = safe_errors(exc)
    finally:
        if created:
            try:
                with connection(owner_dsn) as owner:
                    owner.execute(sql.SQL("DROP DATABASE {} WITH(FORCE)").format(sql.Identifier(name)))
                    result["database_removed"] = not owner.execute("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=%s) AS present", (name,)).fetchone()["present"]
            except BaseException as exc:
                result["cleanup_errors"] = safe_errors(exc)
        result["passed"] = result["passed"] and result["database_removed"]
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn-env", default="TEST_DATABASE_URL")
    parser.add_argument("--allow-disposable-database-create", action="store_true")
    parser.add_argument("--mode", choices=("both", "python", "sql"), default="both")
    parser.add_argument("--tasks", type=int, default=1000)
    parser.add_argument("--batches", type=int, default=16)
    parser.add_argument("--tenants", type=int, default=4)
    parser.add_argument("--sweepers", type=int, default=4)
    parser.add_argument("--task-limit", type=int, default=100)
    parser.add_argument("--batch-limit", type=int, default=8)
    parser.add_argument("--deadline", type=int, default=300)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    if not args.allow_disposable_database_create:
        parser.error("Explicit --allow-disposable-database-create is required")
    if not re.fullmatch(r"[A-Z_][A-Z_0-9]*", args.dsn_env) or not os.environ.get(args.dsn_env):
        parser.error("Provide a DSN through the named environment variable; never a command-line credential")
    if not (1 <= args.tasks <= 100000 and 1 <= args.batches <= min(64, args.tasks)
            and 1 <= args.tenants <= args.batches and 1 <= args.sweepers <= 16
            and 1 <= args.task_limit <= 1000 and 1 <= args.batch_limit <= 64 and 1 <= args.deadline <= 1800):
        parser.error("Task, batch, tenant, sweeper or deadline bounds are invalid")
    if args.report.exists():
        parser.error("Refusing to overwrite an experiment report")
    dsn = os.environ[args.dsn_env]
    source = hashes()
    report = {"schema_version": 1, "synthetic_state_machine_only": True,
        "physical_inference_performed": False, "http_throughput_measured": False,
        "tasks_per_mode": args.tasks, "preaccepted_results_per_mode": args.batches,
        "batches": args.batches, "tenants": args.tenants, "concurrent_sweepers": args.sweepers,
        "sql_global_task_limit": args.task_limit, "sql_batch_limit": args.batch_limit,
        "legacy_budget": "100 lease candidates then up to100 tasks per candidate batch; no global transition cap",
        "max_attempts": 1, "transport": "unix_socket" if conninfo_to_dict(dsn).get("host", "").startswith("/") else "tcp",
        "source_sha256": source, "modes": [],
        "limitations": ["Synthetic expired attempts, not inference tasks or accepted AI completions",
            "Sequential modes share server compute; local Unix-socket results do not predict WAN performance",
            "Administrative database connection is identical for both modes; this is not a privilege-recovery test",
            "Setup, seeding, invariant verification and database cleanup are outside the recovery timer",
            "Expired attempts are exhausted; retry-jitter distribution is separately tested, not measured here",
            "Only newly created uniquely named databases are mutated; no existing table is truncated"]}
    selected_modes = (False, True) if args.mode == "both" else (args.mode == "sql",)
    for optimized in selected_modes:
        report["modes"].append(mode(dsn, args, optimized))
    report["source_changed_during_run"] = hashes() != source
    report["passed"] = all(r["passed"] for r in report["modes"]) and not report["source_changed_during_run"]
    if report["passed"] and args.mode == "both":
        report["observed_sql_to_python_rate_ratio"] = (report["modes"][1]["synthetic_recoveries_per_second"] /
            report["modes"][0]["synthetic_recoveries_per_second"])
    args.report.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd = os.open(args.report, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as handle:
        json.dump(report, handle, indent=2)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    print(json.dumps({"passed": report["passed"], "modes": [{k: r.get(k) for k in
        ("mode", "passed", "elapsed_seconds", "expired_attempts_recovered", "database_removed", "errors")}
        for r in report["modes"]], "source_changed_during_run": report["source_changed_during_run"]}))
    raise SystemExit(0 if report["passed"] else 1)


if __name__ == "__main__":
    main()
