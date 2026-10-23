"""Synthetic state-machine benchmark against an isolated managed PostgreSQL DB.

No inference and no HTTP API performance claim. Creates a uniquely named empty
database/role and removes only those exact resources. Production data is never
used. The synthetic result marker is mandatory and survives validation.
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import secrets
import sys
import time
import uuid

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from backend.config import Settings
from backend.migrate import migrate_database
from backend.store import Store
from ops.ha.managed_cutover import load_url, validate_target_url
from scripts.benchmark import distribution


class BenchmarkRunError(RuntimeError):
    def __init__(self, report):
        super().__init__("Synthetic state-machine benchmark failed")
        self.report = report


def error_details(error):
    """Return diagnostic types/codes without exception text or connection URLs."""
    if isinstance(error, BaseExceptionGroup):
        return [item for child in error.exceptions for item in error_details(child)]
    return [{"error_type": type(error).__name__, "sqlstate": getattr(error, "sqlstate", None)}]


async def exercise(dsn, args, tenants):
    settings = Settings(database_url=dsn, batchforge_worker_token="synthetic-controlplane-only-credential",
        model_name="test-only-controlplane", lease_seconds=30, db_pool_min=2, db_pool_max=args.pool,
        sql_completion_enabled=args.sql, sql_claim_enabled=args.sql_claim)
    if getattr(settings, "sql_completion_enabled", False) != args.sql:
        raise RuntimeError("Requested SQL mode is not implemented in this checkout")
    if getattr(settings, "sql_claim_enabled", False) != args.sql_claim:
        raise RuntimeError("Requested SQL claim mode is not implemented in this checkout")
    async with AsyncConnectionPool(dsn, min_size=2, max_size=args.pool,
            kwargs={"autocommit": True, "row_factory": dict_row, "options": "-c statement_timeout=10000 -c lock_timeout=2000"}, timeout=10) as pool:
        store = Store(pool, settings)
        for index, tenant in enumerate(tenants):
            count = args.tasks // len(tenants) + (index < args.tasks % len(tenants))
            payload = ("\n".join(json.dumps({"custom_id": f"fixture-{index}-{n}","method":"POST","url":"/v1/chat/completions",
                "body":{"model":settings.model_name,"messages":[{"role":"user","content":"SYNTHETIC CONTROLPLANE ONLY; no model invocation"}],"max_tokens":1}}) for n in range(count))+"\n").encode()
            file = await store.upload(tenant, "synthetic.jsonl", payload)
            await store.create(tenant, {"input_file_id":file["id"],"endpoint":"/v1/chat/completions","completion_window":"24h","metadata":{"synthetic_controlplane_only":"true"}}, f"fixture-{index}")
        workers = [f"synthetic-worker-{n}" for n in range(args.workers)]
        for worker in workers:
            await store.register({"worker_id":worker,"name":"SYNTHETIC; no GPU","model":settings.model_name,"concurrency":8})
        completed, claims, completion_latency, errors = 0, [], [], {}
        operation_errors = {"claim": {}, "complete": {}}
        completion_attempts = claim_calls = 0
        start_utc, start = datetime.now(timezone.utc).isoformat(), time.monotonic()
        response = {"batchforge_test_only":True,"choices":[{"message":{"role":"assistant","content":"SYNTHETIC"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1}}

        def record_error(operation, error):
            name = type(error).__name__
            counts = operation_errors[operation]
            counts[name] = counts.get(name, 0) + 1

        def result(passed):
            elapsed = time.monotonic() - start
            return {"test_only":True,"physical_inference_performed":False,"passed":passed,"model":settings.model_name,
                "measurement_started_at":start_utc,"terminal_observation_ended_at":datetime.now(timezone.utc).isoformat(),
                "tasks":args.tasks,"tenants":args.tenants,"workers":args.workers,"pool_max":args.pool,"sql_completion_enabled":args.sql,
                "sql_claim_enabled":args.sql_claim,
                "lease_seconds":settings.lease_seconds,"completion_retry_budget_seconds":25,"heartbeat_performed":False,
                "elapsed_seconds":elapsed,"synthetic_accepted_results_per_second":completed/elapsed,"completed":completed,
                "claim_calls":claim_calls,"completion_attempts":completion_attempts,
                "claim_latency_seconds":distribution(claims),"complete_with_explicit_same_fence_retry_seconds":distribution(completion_latency),
                "retry_errors":errors,"operation_errors":operation_errors,"unique_results_and_exact_usage_verified":passed,
                "limitations":["Synthetic state-machine benchmark; no GPU or HTTP/API throughput measurement","Ingestion and output validation excluded from timed section","Same worker/fence retained on completion retry","Temporary database shares managed compute capacity with the application; run after real inference tests","Direct runtime-role database client; not a daily AI capacity forecast"]}

        async def complete(task, worker):
            nonlocal completed, completion_attempts
            began = time.monotonic()
            delay = .01
            while time.monotonic() - began < 25:
                try:
                    completion_attempts += 1
                    await store.complete(task["id"], worker, task["attempt_token"], response)
                    completion_latency.append(time.monotonic()-began)
                    completed += 1
                    return
                except (psycopg.OperationalError, psycopg.errors.LockNotAvailable, psycopg.errors.QueryCanceled, psycopg.errors.DeadlockDetected) as exc:
                    record_error("complete", exc)
                    name=type(exc).__name__;errors[name]=errors.get(name,0)+1
                    await asyncio.sleep(delay)
                    delay=min(1,delay*2)
                except Exception as exc:
                    record_error("complete", exc)
                    raise
            raise RuntimeError("Synthetic completion could not be confirmed within lease")

        async def loop(worker):
            nonlocal claim_calls
            while completed < args.tasks:
                if time.monotonic()-start > args.deadline:
                    raise RuntimeError("Synthetic control-plane deadline exceeded")
                began=time.monotonic()
                claim_calls += 1
                try:
                    claimed=await store.claim(worker, settings.model_name, 8)
                except Exception as exc:
                    record_error("claim", exc)
                    raise
                claims.append(time.monotonic()-began)
                if claimed["tasks"]:
                    async with asyncio.TaskGroup() as group:
                        for task in claimed["tasks"]:
                            group.create_task(complete(task, worker))
                else:
                    await asyncio.sleep(.01)

        try:
            async with asyncio.TaskGroup() as group:
                for worker in workers:
                    group.create_task(loop(worker))
            timed_result = result(False)
            async with pool.connection() as conn:
                row=await (await conn.execute("SELECT count(*) AS n,count(DISTINCT id) AS unique_tasks,count(*) FILTER(WHERE status='completed' AND response->>'batchforge_test_only'='true') AS synthetic_completed,sum(prompt_tokens) AS input_tokens,sum(completion_tokens) AS output_tokens FROM tasks")).fetchone()
                batch=await (await conn.execute("SELECT sum(completed_count) AS n,sum(prompt_tokens) AS input_tokens,sum(completion_tokens) AS output_tokens FROM batches")).fetchone()
            passed=(row["n"]==row["unique_tasks"]==row["synthetic_completed"]==batch["n"]==args.tasks
                and row["input_tokens"]==row["output_tokens"]==batch["input_tokens"]==batch["output_tokens"]==args.tasks)
            timed_result.update(passed=passed, unique_results_and_exact_usage_verified=passed)
            return timed_result
        except Exception as exc:
            failed = result(False)
            failed["errors"] = error_details(exc)
            failed["credentials_omitted"] = True
            raise BenchmarkRunError(failed) from exc


def run(args):
    owner=load_url(args.owner_env_file)
    validate_target_url(owner)
    suffix=uuid.uuid4().hex[:16]
    database,role="bf_cp_"+suffix,"bf_cp_role_"+suffix
    dbdsn=make_conninfo(owner,dbname=database)
    created=role_created=False
    result = None
    try:
        with psycopg.connect(owner,autocommit=True) as conn:
            conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)));created=True
        migrate_database(dbdsn)
        password=secrets.token_urlsafe(32)
        tenants=[f"fixture-tenant-{n}" for n in range(args.tenants)]
        with psycopg.connect(dbdsn,autocommit=True) as conn:
            conn.execute(sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(sql.Identifier(role),sql.Literal(password)));role_created=True
            conn.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(sql.Identifier(database),sql.Identifier(role)))
            conn.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(sql.Identifier(role)))
            conn.execute(sql.SQL("GRANT SELECT,INSERT,UPDATE ON ALL TABLES IN SCHEMA public TO {}").format(sql.Identifier(role)))
            conn.execute(sql.SQL("GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO {}").format(sql.Identifier(role)))
            for tenant in tenants:conn.execute("INSERT INTO tenants(id,name) VALUES(%s,%s)",(tenant,"SYNTHETIC CONTROLPLANE ONLY"))
        result=asyncio.run(exercise(make_conninfo(dbdsn,user=role,password=password),args,tenants))
        result["isolated_database_created"]=True
        return result
    finally:
        original_error = sys.exception()
        cleanup_errors = []
        if created or role_created:
            try:
                with psycopg.connect(owner, autocommit=True) as conn:
                    for enabled, statement in [
                        (created, sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(database))),
                        (role_created, sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))]:
                        if enabled:
                            try:
                                conn.execute(statement)
                            except psycopg.Error as exc:
                                cleanup_errors.append(type(exc).__name__)
                    cleanup_remaining = conn.execute(
                        "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=%s),"
                        "EXISTS(SELECT 1 FROM pg_roles WHERE rolname=%s)", (database, role)
                    ).fetchone()
                    if any(cleanup_remaining):
                        cleanup_errors.append("TemporaryResourceStillPresent")
            except psycopg.Error as exc:
                cleanup_errors.append(type(exc).__name__)
        report = original_error.report if isinstance(original_error, BenchmarkRunError) else result
        if report is not None:
            report.update(isolated_database_created=created, isolated_role_created=role_created,
                temporary_database=database, temporary_role=role,
                temporary_database_and_role_removed=not cleanup_errors,
                cleanup_errors=cleanup_errors,
                source_sha256={name:hashlib.sha256((Path(__file__).resolve().parents[1]/name).read_bytes()).hexdigest()
                    for name in ["scripts/controlplane_benchmark.py","backend/store.py","backend/config.py","backend/migrate.py","backend/migrations/0002_sql_completion.sql","backend/migrations/0003_sql_claim.sql"]
                    if (Path(__file__).resolve().parents[1]/name).is_file()})
        if cleanup_errors:
            if original_error is not None:
                original_error.add_note("Temporary test resource cleanup also failed; inspect the explicitly created bf_cp resource names")
            else:
                raise RuntimeError("Temporary test resources could not all be removed") from None


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--owner-env-file",type=Path,required=True)
    parser.add_argument("--tasks",type=int,default=1024)
    parser.add_argument("--tenants",type=int,default=1)
    parser.add_argument("--workers",type=int,default=16)
    parser.add_argument("--pool",type=int,default=16)
    parser.add_argument("--deadline",type=float,default=600)
    parser.add_argument("--sql",action="store_true",help="Use SQL completion/heartbeat; claim remains independently controlled")
    parser.add_argument("--sql-claim",action="store_true",help="Use SQL claim; requires compatible schema-v3 runtime code")
    parser.add_argument("--synthetic-controlplane-only",action="store_true",required=True)
    parser.add_argument("--report",type=Path,required=True)
    args=parser.parse_args()
    if not 1 <= args.tenants <= args.tasks <= 100000 or not 1<=args.workers<=64 or not 2<=args.pool<=32:parser.error("Invalid bounded test size")
    if not math.isfinite(args.deadline) or not 1 <= args.deadline <= 3600:
        parser.error("Use a finite deadline between 1 and 3600 seconds")
    try:result=run(args)
    except BenchmarkRunError as exc:
        result=exc.report
    except Exception as exc:
        result={"test_only":True,"physical_inference_performed":False,"passed":False,"error_type":type(exc).__name__,"sqlstate":getattr(exc,"sqlstate",None),"credentials_omitted":True}
    args.report.parent.mkdir(parents=True,exist_ok=True)
    args.report.write_text(json.dumps(result,indent=2)+"\n")
    print(json.dumps(result,indent=2))
    if not result["passed"]:raise SystemExit(1)


if __name__=="__main__":main()
