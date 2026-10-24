"""Actual PostgreSQL failure/recovery races in isolated, disposable databases.

No API background reaper or shared fixture participates in these tests. The
caller must supply TEST_DATABASE_URL for a development owner able to create a
temporary database; no production database is a mutation target.
"""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import json
import os
from pathlib import Path
import threading
import time
import uuid

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool
import pytest

from backend.config import Settings
from backend.store import Store

DSN = os.environ.get("TEST_DATABASE_URL")
ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.skipif(not DSN, reason="Set TEST_DATABASE_URL for real PostgreSQL recovery tests")
ERROR = {"code": "engine_busy", "message": "retry this isolated test"}


@contextmanager
def connect(dsn):
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row,
                         options="-c statement_timeout=8000 -c lock_timeout=5000") as conn:
        yield conn


@pytest.fixture
def database():
    name = "bf_recovery_" + uuid.uuid4().hex
    with connect(DSN) as owner:
        owner.execute(sql.SQL("CREATE DATABASE {} TEMPLATE template0 ENCODING 'UTF8'").format(sql.Identifier(name)))
    dsn = make_conninfo(**{**conninfo_to_dict(DSN), "dbname": name})
    try:
        with connect(dsn) as conn:
            for path in (ROOT / "backend/schema.sql",
                         ROOT / "backend/migrations/0002_sql_completion.sql",
                         ROOT / "backend/migrations/0003_sql_claim.sql",
                         ROOT / "backend/migrations/0004_sql_failure_recovery.sql"):
                conn.execute(path.read_text())
        yield dsn
    finally:
        with connect(DSN) as owner:
            owner.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))


def seed(dsn, count=1, *, tenant="tenant", states=None, expired=True,
         deadline=False, max_attempts=3, worker=None):
    suffix = uuid.uuid4().hex[:12]
    bid, fid, wid = "batch_" + suffix, "file_" + suffix, worker or "worker_" + suffix
    states = states or ["running"] * count
    pending, running, completed = (states.count(value) for value in ("pending", "running", "completed"))
    with connect(dsn) as conn:
        conn.execute("INSERT INTO tenants(id,name) VALUES(%s,%s) ON CONFLICT DO NOTHING", (tenant, tenant))
        conn.execute("INSERT INTO workers(id,name,model,concurrency) VALUES(%s,%s,'test-model',4096) ON CONFLICT DO NOTHING", (wid, wid))
        conn.execute("INSERT INTO files(id,tenant_id,filename,kind,content) VALUES(%s,%s,'test.jsonl','input',''::bytea)", (fid, tenant))
        conn.execute("INSERT INTO batches(id,tenant_id,input_file_id,model,status,total_count,pending_count,running_count,completed_count,prompt_tokens,completion_tokens,expires_at) VALUES(%s,%s,%s,'test-model',%s,%s,%s,%s,%s,%s,%s,clock_timestamp()+(%s*interval '1 second'))", (bid, tenant, fid, "completed" if completed == len(states) else "in_progress", len(states), pending, running, completed, completed * 7, completed * 2, -1 if deadline else 3600))
        conn.execute("INSERT INTO tenant_queues(tenant_id,model,pending_count) VALUES(%s,'test-model',%s) ON CONFLICT(tenant_id,model) DO UPDATE SET pending_count=tenant_queues.pending_count+excluded.pending_count", (tenant, pending))
        tasks = []
        for index, status in enumerate(states):
            tid, token = f"task_{suffix}_{index:04}", f"attempt_{suffix}_{index:04}"
            attempts = int(status != "pending")
            conn.execute("INSERT INTO tasks(id,tenant_id,batch_id,custom_id,model,body,status,attempts,max_attempts,worker_id,attempt_token,lease_expires_at,prompt_tokens,completion_tokens,response) VALUES(%s,%s,%s,%s,'test-model','{}',%s,%s,%s,%s,%s,CASE WHEN %s='running' THEN clock_timestamp()+(%s*interval '1 second') END,%s,%s,%s)", (tid, tenant, bid, str(index), status, attempts, max_attempts, wid if attempts else None, token if attempts else None, status, -1 if expired else 90, 7 if status == "completed" else 0, 2 if status == "completed" else 0, Jsonb({"content": "previously accepted"}) if status == "completed" else None))
            if attempts:
                conn.execute("INSERT INTO attempts(token,task_id,batch_id,tenant_id,worker_id,number,status,prompt_tokens,completion_tokens) VALUES(%s,%s,%s,%s,%s,1,%s,%s,%s)", (token, tid, bid, tenant, wid, status, 7 if status == "completed" else 0, 2 if status == "completed" else 0))
            tasks.append({"id": tid, "attempt_token": token, "worker_id": wid, "batch_id": bid})
    return tasks


def recover(dsn, limit=100, batches=8, base=1):
    with connect(dsn) as conn:
        return conn.execute("SELECT public.batchforge_recover(%s,%s,%s) AS r", (limit, batches, base)).fetchone()["r"]


def fail(dsn, task, retry=True, base=1):
    with connect(dsn) as conn:
        return conn.execute("SELECT public.batchforge_fail(%s,%s,%s,%s,%s,%s) AS r", (task["id"], task["worker_id"], task["attempt_token"], Jsonb(ERROR), retry, base)).fetchone()["r"]


def complete(dsn, task):
    with connect(dsn) as conn:
        return conn.execute("SELECT public.batchforge_complete(%s,%s,%s,%s,7,2) AS r", (task["id"], task["worker_id"], task["attempt_token"], Jsonb({"answer": "accepted"}))).fetchone()["r"]


def invariants(dsn):
    with connect(dsn) as conn:
        rows = conn.execute("SELECT b.id,b.total_count,b.pending_count,b.running_count,b.completed_count,b.failed_count,b.cancelled_count,b.prompt_tokens,b.completion_tokens, count(t.*) AS tasks,count(*) FILTER(WHERE t.status='pending') AS pending,count(*) FILTER(WHERE t.status='running') AS running,count(*) FILTER(WHERE t.status='completed') AS completed,count(*) FILTER(WHERE t.status='failed') AS failed,count(*) FILTER(WHERE t.status='cancelled') AS cancelled,coalesce(sum(t.prompt_tokens),0) AS task_prompt,coalesce(sum(t.completion_tokens),0) AS task_completion FROM batches b JOIN tasks t ON t.batch_id=b.id GROUP BY b.id").fetchall()
        for row in rows:
            assert row["total_count"] == row["tasks"]
            for status in ("pending", "running", "completed", "failed", "cancelled"):
                assert row[status + "_count"] == row[status]
            assert row["prompt_tokens"] == row["task_prompt"]
            assert row["completion_tokens"] == row["task_completion"]
        assert conn.execute("SELECT count(*) AS n FROM tenant_queues q WHERE q.pending_count<>(SELECT count(*) FROM tasks t WHERE t.tenant_id=q.tenant_id AND t.model=q.model AND t.status='pending')").fetchone()["n"] == 0
        assert conn.execute("SELECT count(*) AS n FROM (SELECT task_id FROM attempts WHERE status='completed' GROUP BY task_id HAVING count(*)>1) d").fetchone()["n"] == 0


def test_global_task_and_batch_budget_across_many_batches(database):
    for index in range(9):
        seed(database, 11, tenant=f"tenant-{index}")
    total = 0
    for _ in range(100):
        result = recover(database, limit=7, batches=2)
        assert result["processed"] == result["recovered"] <= 7
        assert result["deadline_cancelled"] == 0 and result["batches"] <= 2
        total += result["processed"]
        invariants(database)
        if not result["processed"]:
            break
    assert total == 99
    with connect(database) as conn:
        assert conn.execute("SELECT count(*) AS n FROM attempts WHERE status='expired' AND error->>'code'='lease_expired'").fetchone()["n"] == 99
        assert conn.execute("SELECT count(*) AS n FROM tasks WHERE status='running'").fetchone()["n"] == 0


def test_concurrent_sweepers_transition_each_attempt_once(database):
    tasks = [task for index in range(6) for task in seed(database, 13, tenant=f"tenant-{index}")]
    barrier = threading.Barrier(2)

    def sweep():
        barrier.wait(timeout=5)
        return recover(database, 17, 3)

    total = 0
    with ThreadPoolExecutor(max_workers=2) as pool:
        for _ in range(40):
            results = list(pool.map(lambda _: sweep(), range(2)))
            assert all(r["processed"] <= 17 and r["batches"] <= 3 for r in results)
            total += sum(r["processed"] for r in results)
            if total == len(tasks):
                break
    assert total == len(tasks)
    invariants(database)
    with connect(database) as conn:
        assert conn.execute("SELECT count(*) AS n FROM attempts WHERE status='expired'").fetchone()["n"] == len(tasks)
        assert conn.execute("SELECT count(*) AS n FROM attempts").fetchone()["n"] == len(tasks)


@pytest.mark.parametrize("locked", ["worker", "queue", "batch", "task"])
def test_recovery_skips_each_held_lock_without_waiting(database, locked):
    task = seed(database)[0]
    statements = {"worker": ("SELECT id FROM workers WHERE id=%s FOR UPDATE", (task["worker_id"],)),
                  "queue": ("SELECT tenant_id FROM tenant_queues WHERE tenant_id='tenant' FOR UPDATE", ()),
                  "batch": ("SELECT id FROM batches WHERE id=%s FOR UPDATE", (task["batch_id"],)),
                  "task": ("SELECT id FROM tasks WHERE id=%s FOR UPDATE", (task["id"],))}
    with psycopg.connect(database) as blocker:
        blocker.execute(*statements[locked])
        started = time.monotonic()
        assert recover(database)["processed"] == 0
        assert time.monotonic() - started < 2
    assert recover(database)["processed"] == 1
    invariants(database)


def test_chunked_deadline_cancellation_preserves_previously_accepted_usage(database):
    tasks = seed(database, states=["completed", "pending", "pending", "running", "running", "running", "pending"], deadline=True)
    bid = tasks[0]["batch_id"]
    results = []
    for _ in range(3):
        result = recover(database, 2, 1)
        results.append(result)
        assert result == {"processed": 2, "deadline_cancelled": 2, "recovered": 0, "batches": 1}
        invariants(database)
        with connect(database) as conn:
            batch = conn.execute("SELECT * FROM batches WHERE id=%s", (bid,)).fetchone()
            assert batch["status"] == ("failed" if len(results) == 3 else "in_progress")
            assert (batch["prompt_tokens"], batch["completion_tokens"]) == (7, 2)
    assert recover(database)["processed"] == 0
    with connect(database) as conn:
        assert conn.execute("SELECT count(*) AS n FROM tasks WHERE status='cancelled' AND error->>'code'='batch_expired'").fetchone()["n"] == 6
        assert conn.execute("SELECT count(*) AS n FROM attempts WHERE status='cancelled'").fetchone()["n"] == 3
        batch = conn.execute("SELECT * FROM batches WHERE id=%s", (bid,)).fetchone()
        assert batch["output_file_id"] and batch["error_file_id"]
    # Deadline fencing does not wait for the bounded cleanup to touch a task.
    with pytest.raises(psycopg.Error) as stale:
        complete(database, tasks[3])
    assert stale.value.sqlstate == "BF409"


def test_valid_lease_and_completed_result_are_untouched(database):
    old = seed(database, 3)
    live = seed(database, 2, expired=False)
    accepted = seed(database, states=["completed"])
    with connect(database) as conn:
        before = conn.execute("SELECT to_jsonb(t) AS row FROM tasks t WHERE id=ANY(%s) ORDER BY id", ([t["id"] for t in live + accepted],)).fetchall()
    assert recover(database)["recovered"] == len(old)
    with connect(database) as conn:
        assert conn.execute("SELECT to_jsonb(t) AS row FROM tasks t WHERE id=ANY(%s) ORDER BY id", ([t["id"] for t in live + accepted],)).fetchall() == before
    invariants(database)


def test_fail_retry_jitter_exhaustion_and_duplicate_fences(database):
    tasks = seed(database, 24, expired=False)
    for task in tasks:
        assert fail(database, task) == {"ok": True, "status": "pending"}
    with connect(database) as conn:
        delays = conn.execute("SELECT extract(epoch FROM(t.ready_at-a.finished_at))::float8 AS delay FROM tasks t JOIN attempts a ON a.token=t.attempt_token").fetchall()
        assert all(.5 <= row["delay"] <= 1 for row in delays)
        assert len({round(row["delay"], 3) for row in delays}) >= 4
    with pytest.raises(psycopg.Error) as stale:
        fail(database, tasks[0])
    assert stale.value.sqlstate == "BF409"
    exhausted = seed(database, max_attempts=1, expired=False)[0]
    assert fail(database, exhausted) == {"ok": True, "status": "failed"}
    with connect(database) as conn:
        batch = conn.execute("SELECT * FROM batches WHERE id=%s", (exhausted["batch_id"],)).fetchone()
        assert batch["status"] == "failed" and batch["error_file_id"]
    invariants(database)


def test_expired_retry_exhaustion_records_cause_and_keeps_zero_usage(database):
    task = seed(database, max_attempts=1)[0]
    assert recover(database)["recovered"] == 1
    with connect(database) as conn:
        attempt = conn.execute("SELECT status,error,prompt_tokens,completion_tokens FROM attempts WHERE token=%s", (task["attempt_token"],)).fetchone()
        assert attempt["status"] == "failed" and attempt["error"]["code"] == "lease_expired"
        assert attempt["prompt_tokens"] == attempt["completion_tokens"] == 0
    invariants(database)


def test_recovered_task_new_token_accepts_once_and_old_reports_stay_fenced(database):
    old = seed(database)[0]
    assert recover(database)["recovered"] == 1
    with connect(database) as conn:
        conn.execute("UPDATE tasks SET ready_at=clock_timestamp() WHERE id=%s", (old["id"],))
        conn.execute("UPDATE tenant_queues SET next_ready_at=clock_timestamp()")
        claimed = conn.execute("SELECT public.batchforge_claim(%s,'test-model',1,90) AS r", (old["worker_id"],)).fetchone()["r"]["tasks"]
    assert len(claimed) == 1 and claimed[0]["id"] == old["id"]
    fresh = {**old, "attempt_token": claimed[0]["attempt_token"]}
    assert fresh["attempt_token"] != old["attempt_token"] and claimed[0]["attempt_number"] == 2
    assert complete(database, fresh) == {"ok": True}
    assert complete(database, fresh) == {"ok": True, "duplicate": True}
    for operation in (lambda: fail(database, old), lambda: complete(database, old)):
        with pytest.raises(psycopg.Error) as stale:
            operation()
        assert stale.value.sqlstate == "BF409"
    invariants(database)
    with connect(database) as conn:
        assert conn.execute("SELECT status FROM attempts WHERE token=%s", (old["attempt_token"],)).fetchone()["status"] == "expired"
        batch = conn.execute("SELECT * FROM batches WHERE id=%s", (old["batch_id"],)).fetchone()
        assert batch["status"] == "completed" and (batch["prompt_tokens"], batch["completion_tokens"]) == (7, 2)


def test_deadline_fences_valid_lease_before_chunk_touches_task(database):
    tasks = seed(database, 4, deadline=True, expired=False)
    with connect(database) as conn:
        before = conn.execute("SELECT last_seen_at FROM workers WHERE id=%s", (tasks[0]["worker_id"],)).fetchone()
    for task in tasks:
        with pytest.raises(psycopg.Error) as stale:
            fail(database, task)
        assert stale.value.sqlstate == "BF409"
    with connect(database) as conn:
        assert conn.execute("SELECT last_seen_at FROM workers WHERE id=%s", (tasks[0]["worker_id"],)).fetchone() == before
    assert recover(database, 1, 1)["deadline_cancelled"] == 1
    with connect(database) as conn:
        assert conn.execute("SELECT status FROM batches WHERE id=%s", (tasks[0]["batch_id"],)).fetchone()["status"] == "in_progress"
        assert conn.execute("SELECT count(*) AS n FROM tasks WHERE status='running'").fetchone()["n"] == 3
    invariants(database)


def test_fail_checks_server_clock_after_queue_lock_wait(database):
    task = seed(database, expired=False)[0]
    with psycopg.connect(database) as blocker, ThreadPoolExecutor(max_workers=1) as pool:
        blocker.execute("SELECT * FROM tenant_queues FOR UPDATE")
        future = pool.submit(fail, database, task)
        waiting = False
        try:
            deadline = time.monotonic() + 2
            with connect(database) as observer:
                while time.monotonic() < deadline and not future.done():
                    waiting = observer.execute("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE %s=ANY(pg_blocking_pids(pid))) AS waiting", (blocker.info.backend_pid,)).fetchone()["waiting"]
                    if waiting:
                        break
                    time.sleep(.01)
            blocker.execute("UPDATE tasks SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=%s", (task["id"],))
        finally:
            blocker.commit()
        with pytest.raises(psycopg.Error) as stale:
            future.result(timeout=5)
    assert waiting and stale.value.sqlstate == "BF409"
    assert recover(database)["recovered"] == 1
    invariants(database)


def outcome(operation):
    try:
        operation()
        return "accepted"
    except psycopg.Error as exc:
        return exc.sqlstate


def test_fail_and_completion_race_accepts_only_one_transition(database):
    tasks = seed(database, 12, expired=False)
    with ThreadPoolExecutor(max_workers=2) as pool:
        for task in tasks:
            barrier = threading.Barrier(2)

            def report(operation):
                barrier.wait(timeout=5)
                return outcome(operation)

            a = pool.submit(report, lambda: complete(database, task))
            z = pool.submit(report, lambda: fail(database, task, retry=False))
            assert sorted([a.result(timeout=8), z.result(timeout=8)]) == ["BF409", "accepted"]
    invariants(database)


def test_fail_and_real_store_cancellation_race_preserves_partial_output(database):
    task = seed(database, expired=False)[0]

    async def cancel():
        async with AsyncConnectionPool(database, min_size=1, max_size=2,
                                       kwargs={"autocommit": True, "row_factory": dict_row}) as pool:
            return await Store(pool, Settings(database_url=database)).cancel("tenant", task["batch_id"])

    barrier = threading.Barrier(2)
    with ThreadPoolExecutor(max_workers=2) as pool:
        def reporting():
            barrier.wait(timeout=5)
            return outcome(lambda: fail(database, task))

        def cancelling():
            barrier.wait(timeout=5)
            return asyncio.run(cancel())

        a, z = pool.submit(reporting), pool.submit(cancelling)
        assert a.result(timeout=8) in {"accepted", "BF409"}
        assert z.result(timeout=8)["status"] == "cancelled"
    invariants(database)
    with connect(database) as conn:
        assert conn.execute("SELECT status FROM tasks WHERE id=%s", (task["id"],)).fetchone()["status"] == "cancelled"


@pytest.mark.parametrize("operation", ["fail", "recover"])
def test_terminal_metadata_failure_rolls_back_the_entire_statement(database, operation):
    tasks = seed(database, 5, expired=operation == "recover", max_attempts=1)
    with connect(database) as conn:
        conn.execute("CREATE FUNCTION public.recovery_test_output_failure() RETURNS trigger LANGUAGE plpgsql AS $f$ BEGIN RAISE EXCEPTION USING ERRCODE='PT004',MESSAGE='isolated output failure'; END $f$")
        conn.execute("CREATE TRIGGER recovery_output_failure BEFORE INSERT ON files FOR EACH ROW WHEN(NEW.kind='error') EXECUTE FUNCTION public.recovery_test_output_failure()")
    # Fail needs the final task to finish its batch. Give each task a separate
    # batch in that case, so the first report reaches the metadata trigger.
    if operation == "fail":
        tasks = seed(database, expired=False, max_attempts=1)
    with pytest.raises(psycopg.Error) as injected:
        fail(database, tasks[0]) if operation == "fail" else recover(database)
    assert injected.value.sqlstate == "PT004"
    invariants(database)
    with connect(database) as conn:
        assert conn.execute("SELECT count(*) AS n FROM tasks WHERE status<>'running'").fetchone()["n"] == 0
        assert conn.execute("SELECT count(*) AS n FROM attempts WHERE status<>'running'").fetchone()["n"] == 0
        conn.execute("DROP TRIGGER recovery_output_failure ON files")
        conn.execute("DROP FUNCTION public.recovery_test_output_failure()")
    assert (fail(database, tasks[0])["status"] == "failed") if operation == "fail" else recover(database)["processed"] == 5


def test_low_privilege_function_execution_cannot_gain_owner_powers(database):
    task = seed(database, expired=False, max_attempts=1)[0]
    role = "recovery_role_" + uuid.uuid4().hex[:12]
    with connect(database) as owner:
        owner.execute(sql.SQL("CREATE ROLE {} LOGIN").format(sql.Identifier(role)))
        owner.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(sql.Identifier(owner.info.dbname), sql.Identifier(role)))
        owner.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {} ").format(sql.Identifier(role)))
        owner.execute(sql.SQL("GRANT SELECT,INSERT,UPDATE ON workers,files,batches,tasks,attempts,tenant_queues TO {}").format(sql.Identifier(role)))
        funcs = owner.execute("SELECT prosecdef,proconfig FROM pg_proc WHERE pronamespace='public'::regnamespace AND proname IN ('batchforge_fail','batchforge_recover','batchforge_finish_batch_v4')").fetchall()
        assert len(funcs) == 3 and all(r == {"prosecdef": False, "proconfig": ["search_path=pg_catalog, public"]} for r in funcs)
    runtime = make_conninfo(**{**conninfo_to_dict(database), "user": role})
    try:
        assert fail(runtime, task)["status"] == "failed"
        seed(database, 3)
        assert recover(runtime)["processed"] == 3
        with connect(runtime) as conn:
            for statement in ("CREATE TABLE forbidden(id integer)", "UPDATE api_keys SET revoked=true", "INSERT INTO tenants(id,name) VALUES('forbidden','forbidden')"):
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    conn.execute(statement)
    finally:
        with connect(database) as conn:
            conn.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role)))
            conn.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))
    invariants(database)


def test_invalid_limits_and_error_shapes_are_rejected_without_mutation(database):
    task = seed(database, expired=False)[0]
    with connect(database) as conn:
        for limit, batches, base in [(0, 1, 1), (1025, 1, 1), (1, 0, 1), (1, 65, 1), (1, 1, 0), (1, 1, 61)]:
            with pytest.raises(psycopg.Error) as invalid:
                conn.execute("SELECT public.batchforge_recover(%s,%s,%s)", (limit, batches, base))
            assert invalid.value.sqlstate == "BF400"
        for error in [None, [], {}, {"code": "x", "message": 1}, {"code": "", "message": "x"}]:
            with pytest.raises(psycopg.Error) as invalid:
                conn.execute("SELECT public.batchforge_fail(%s,%s,%s,%s,true,1)", (task["id"], task["worker_id"], task["attempt_token"], Jsonb(error)))
            assert invalid.value.sqlstate == "BF400"
    invariants(database)
