"""Real PostgreSQL scheduling invariants; logical profiles are NOT GPU benchmarks."""
from concurrent.futures import ThreadPoolExecutor
import json
import os
import time
from pathlib import Path
import uuid

import psycopg
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
import pytest

from backend.scheduling import estimate_work, predict, profile_from_metadata, runtime_fingerprint, validate_profile

ROOT = Path(__file__).resolve().parents[1]


def profile(**changes):
    return {"profile_id": "logical-test", "runtime_fingerprint": "runtime-v1",
            "prefill_tokens_per_second": 1000, "decode_tokens_per_second": 100,
            "hourly_cost_usd": 1, "calibration_label": "synthetic-unit-test-not-hardware",
            "cost_source": "test-arithmetic-not-a-quote", **changes}


@pytest.mark.parametrize("field,value", [("prefill_tokens_per_second", 0), ("decode_tokens_per_second", -1),
    ("hourly_cost_usd", float("nan")), ("hourly_cost_usd", float("inf")),
    ("prefill_tokens_per_second", 1e-300),
    ("prefill_tokens_per_second", True), ("runtime_fingerprint", ""), ("cost_source", "")])
def test_profile_rejects_invalid_or_unattributed_values(field, value):
    with pytest.raises(ValueError):
        validate_profile(profile(**{field: value}))


def test_work_estimate_and_prediction_do_not_claim_exact_tokenization():
    work = estimate_work({"messages": [{"role": "user", "content": "你好世界"}], "max_tokens": 32})
    assert work["estimated_input_tokens"] > 1
    assert work["estimated_output_tokens"] == 32
    assert "utf8" in work["estimate_source"]
    result = predict({"estimated_input_tokens": 1000, "estimated_output_tokens": 100}, profile())
    assert result == {"predicted_prefill_seconds": 1, "predicted_decode_seconds": 1,
                      "predicted_service_seconds": 2, "predicted_cost_usd": 2 / 3600}
    assert profile_from_metadata({}) is None
    assert estimate_work({"messages": [], "max_completion_tokens": 2048})["estimated_output_tokens"] == 2048
    assert estimate_work({"messages": []})["estimated_output_tokens"] == 256
    assert runtime_fingerprint({"revision": "abc", "quant": "fp8"}) == runtime_fingerprint({"quant": "fp8", "revision": "abc"})
    assert runtime_fingerprint({"revision": "abc"}) != runtime_fingerprint({"revision": "def"})


def test_worker_profile_is_explicit_operator_configuration(monkeypatch):
    from worker.config import Settings
    monkeypatch.setenv("BATCHFORGE_WORKER_TOKEN", "test-worker-token-not-a-real-secret")
    monkeypatch.setenv("SCHEDULER_PROFILE_JSON", json.dumps(profile()))
    settings = Settings.from_env()
    assert settings.scheduling_profile == validate_profile(profile())
    assert settings.slots == min(settings.max_inflight, settings.token_budget // settings.max_model_len)
    monkeypatch.setenv("SCHEDULER_PROFILE_JSON", json.dumps(profile(decode_tokens_per_second=0)))
    with pytest.raises(ValueError):
        Settings.from_env()


@pytest.fixture(scope="module")
def scheduler_dsn():
    source = os.environ.get("TEST_DATABASE_URL")
    if not source:
        pytest.skip("Set TEST_DATABASE_URL to run real PostgreSQL scheduler tests")
    name = "bf_scheduler_" + uuid.uuid4().hex
    with psycopg.connect(source, autocommit=True) as admin:
        admin.execute(psycopg.sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(psycopg.sql.Identifier(name)))
    dsn = make_conninfo(source, dbname=name)
    try:
        with psycopg.connect(dsn, autocommit=True) as conn:
            for source_file in ("backend/schema.sql", "backend/migrations/0002_sql_completion.sql",
                                "backend/migrations/0003_sql_claim.sql", "backend/migrations/0006_cost_scheduler.sql"):
                conn.execute((ROOT / source_file).read_text())
        yield dsn
    finally:
        with psycopg.connect(source, autocommit=True) as admin:
            admin.execute(psycopg.sql.SQL("DROP DATABASE {}").format(psycopg.sql.Identifier(name)))


@pytest.fixture
def db(scheduler_dsn):
    with psycopg.connect(scheduler_dsn, autocommit=True, row_factory=dict_row) as conn:
        conn.execute("TRUNCATE idempotency,attempts,tasks,batches,files,workers,tenant_queues,api_keys,tenants CASCADE")
        yield conn


def worker(db, wid, *, prefill=1000, decode=100, price=1, fingerprint="runtime-v1", slots=1, profiled=True):
    values = profile(profile_id=wid, runtime_fingerprint=fingerprint, prefill_tokens_per_second=prefill,
                     decode_tokens_per_second=decode, hourly_cost_usd=price)
    db.execute("""INSERT INTO workers(id,name,model,concurrency,metadata,scheduler_profile_id,runtime_fingerprint,
        prefill_tokens_per_second,decode_tokens_per_second,hourly_cost_usd)
        VALUES(%s,%s,'model',%s,%s,%s,%s,%s,%s,%s)""",
        (wid, wid, slots, Jsonb({"ready_engine_count": 1, "scheduling_profile": values}),
         wid if profiled else None, fingerprint, prefill if profiled else None,
         decode if profiled else None, price if profiled else None))


def batch(db, bid="batch", tenant="tenant", tasks=(('task', 100, 10),), age=0, deadline=3600):
    db.execute("INSERT INTO tenants(id,name) VALUES(%s,%s) ON CONFLICT DO NOTHING", (tenant, tenant))
    db.execute("INSERT INTO files(id,tenant_id,filename,kind) VALUES(%s,%s,'test.jsonl','input')", ("file-"+bid, tenant))
    db.execute("""INSERT INTO batches(id,tenant_id,input_file_id,model,status,total_count,pending_count,expires_at)
        VALUES(%s,%s,%s,'model','in_progress',%s,%s,clock_timestamp()+%s*interval '1 second')""",
        (bid, tenant, "file-"+bid, len(tasks), len(tasks), deadline))
    for tid, inputs, outputs in tasks:
        db.execute("""INSERT INTO tasks(id,tenant_id,batch_id,custom_id,model,body,status,max_attempts,
            estimated_input_tokens,estimated_output_tokens,estimate_source,created_at)
            VALUES(%s,%s,%s,%s,'model',%s,'pending',3,%s,%s,'logical_test',clock_timestamp()-%s*interval '1 second')""",
            (tid, tenant, bid, tid, Jsonb({"messages": [{"role": "user", "content": "test"}], "max_tokens": outputs}), inputs, outputs, age))
    db.execute("""INSERT INTO tenant_queues(tenant_id,model,pending_count) VALUES(%s,'model',%s)
        ON CONFLICT(tenant_id,model) DO UPDATE SET pending_count=tenant_queues.pending_count+excluded.pending_count""", (tenant, len(tasks)))


def claim(db, wid, *, policy="cost_aware", limit=1, aging=60):
    return db.execute("SELECT batchforge_claim_scheduled(%s,'model',%s,30,%s,%s,60) AS result",
                      (wid, limit, policy, aging)).fetchone()["result"]["tasks"]


def test_prefill_and_decode_work_choose_different_logical_profiles(db):
    worker(db, "prefill", prefill=1000, decode=10)
    worker(db, "decode", prefill=100, decode=100)
    batch(db, tasks=(("decode-heavy", 10, 1000), ("prefill-heavy", 10000, 10)))
    first = claim(db, "prefill")
    assert [t["id"] for t in first] == ["prefill-heavy"]
    assert [t["id"] for t in claim(db, "decode")] == ["decode-heavy"]
    decision = first[0]["scheduling_decision"]
    assert decision["predicted_service_seconds"] == 11
    assert decision["predicted_cost_usd"] == pytest.approx(11/3600)
    assert decision["calibration_label"] == "synthetic-unit-test-not-hardware"
    assert db.execute("SELECT scheduling_decision FROM attempts WHERE task_id='prefill-heavy'").fetchone()["scheduling_decision"] == decision


def test_cheaper_idle_peer_deferral_is_bounded_by_age(db):
    worker(db, "expensive", price=10)
    worker(db, "cheap", price=1)
    batch(db)
    assert claim(db, "expensive") == []
    db.execute("UPDATE tasks SET created_at=clock_timestamp()-interval '61 seconds'")
    assert len(claim(db, "expensive")) == 1


@pytest.mark.parametrize("peer_state", ["stale", "not-ready", "busy"])
def test_unavailable_cheaper_peer_does_not_strand_capacity(db, peer_state):
    worker(db, "expensive", price=10)
    worker(db, "cheap", price=1)
    batch(db)
    if peer_state == "stale":
        db.execute("UPDATE workers SET last_seen_at=clock_timestamp()-interval '61 seconds' WHERE id='cheap'")
    elif peer_state == "not-ready":
        db.execute("UPDATE workers SET metadata=jsonb_set(metadata,'{ready_engine_count}','0') WHERE id='cheap'")
    else:
        batch(db, "busy-batch", "busy-tenant", (("busy-task", 100, 10),))
        db.execute("UPDATE tasks SET status='running',worker_id='cheap',lease_expires_at=clock_timestamp()+interval '30 seconds' WHERE id='busy-task'")
        db.execute("UPDATE batches SET pending_count=0,running_count=1 WHERE id='busy-batch'")
        db.execute("UPDATE tenant_queues SET pending_count=0 WHERE tenant_id='busy-tenant'")
    assert len(claim(db, "expensive")) == 1


def test_deadline_risk_overrides_cheaper_worker_deferral(db):
    worker(db, "fast", prefill=1000, decode=100, price=100)
    worker(db, "cheap-slow", prefill=10, decode=1, price=.01)
    batch(db, tasks=(("deadline-task", 1000, 100),), deadline=3)
    assert len(claim(db, "fast")) == 1


def test_fair_policy_preserves_fifo_and_tenant_rotation(db):
    worker(db, "worker", slots=4)
    batch(db, "a", "a", (("a-long", 10000, 1000), ("a-short", 1, 1)))
    batch(db, "b", "b", (("b-task", 1, 1),))
    assert [t["id"] for t in claim(db, "worker", policy="fair")] == ["a-long"]
    assert [t["id"] for t in claim(db, "worker", policy="fair")] == ["b-task"]


def test_aged_long_task_precedes_new_short_tasks(db):
    worker(db, "worker", slots=4)
    batch(db, tasks=(("long", 100000, 1000), ("short", 1, 1)))
    db.execute("UPDATE tasks SET created_at=clock_timestamp()-interval '61 seconds' WHERE id='long'")
    assert [t["id"] for t in claim(db, "worker")] == ["long"]


@pytest.mark.parametrize("policy", ["fair", "cost_aware"])
def test_fingerprint_pins_first_attempt_and_blocks_incompatible_retry(db, policy):
    worker(db, "one", fingerprint="runtime-v1")
    worker(db, "incompatible", fingerprint="runtime-v2")
    worker(db, "replacement", fingerprint="runtime-v1")
    batch(db, age=61)
    first = claim(db, "one", policy=policy)[0]
    assert db.execute("SELECT runtime_fingerprint FROM tasks").fetchone()["runtime_fingerprint"] == "runtime-v1"
    db.execute("UPDATE attempts SET status='expired',finished_at=clock_timestamp()")
    db.execute("UPDATE tasks SET status='pending',lease_expires_at=NULL,ready_at=clock_timestamp()")
    db.execute("UPDATE batches SET pending_count=1,running_count=0")
    db.execute("UPDATE tenant_queues SET pending_count=1,next_ready_at=clock_timestamp()")
    assert claim(db, "incompatible", policy=policy) == []
    second = claim(db, "replacement", policy=policy)[0]
    assert first["attempt_token"] != second["attempt_token"]
    assert second["attempt_number"] == 2
    with pytest.raises(psycopg.DatabaseError) as rejected:
        db.execute("SELECT batchforge_heartbeat(%s,%s,%s,30)", (first["id"], "one", first["attempt_token"]))
    assert rejected.value.sqlstate == "BF409"


def test_expired_batch_does_not_head_of_line_block_live_batch(db):
    worker(db, "worker")
    batch(db, "expired", tasks=(("expired-task", 1, 1),), deadline=-1)
    batch(db, "live", tasks=(("live-task", 1, 1),))
    assert [t["id"] for t in claim(db, "worker")] == ["live-task"]


@pytest.mark.parametrize("policy", ["fair", "cost_aware"])
def test_first_dispatch_pins_entire_batch_numerical_runtime(db, policy):
    worker(db, "first", fingerprint="runtime-v1")
    worker(db, "different", fingerprint="runtime-v2")
    worker(db, "compatible", fingerprint="runtime-v1")
    batch(db, tasks=(("one", 100, 10), ("two", 100, 10)), age=61)
    assert len(claim(db, "first", policy=policy)) == 1
    assert db.execute("SELECT runtime_fingerprint,runtime_compatibility_scope FROM batches").fetchone() == {
        "runtime_fingerprint": "runtime-v1", "runtime_compatibility_scope": "first_dispatch"}
    assert claim(db, "different", policy=policy) == []
    assert len(claim(db, "compatible", policy=policy)) == 1


def test_waiting_for_worker_lock_does_not_dispatch_expired_batch(db, scheduler_dsn):
    worker(db, "worker")
    batch(db, deadline=.5)
    def contender():
        with psycopg.connect(scheduler_dsn, row_factory=dict_row) as conn:
            return claim(conn, "worker")
    with psycopg.connect(scheduler_dsn) as blocker, ThreadPoolExecutor(max_workers=1) as pool:
        blocker.execute("SELECT id FROM workers WHERE id='worker' FOR UPDATE")
        future = pool.submit(contender)
        time.sleep(.65)
        blocker.commit()
        assert future.result(timeout=3) == []
    assert db.execute("SELECT count(*) AS n FROM attempts").fetchone()["n"] == 0
    assert db.execute("SELECT last_seen_at>clock_timestamp()-interval '.3 seconds' AS fresh FROM workers").fetchone()["fresh"]


def test_incompatible_tenants_cannot_permanently_hide_compatible_queue(db):
    worker(db, "worker")
    for index in range(8):
        batch(db, f"a-{index}", f"a-{index}", ((f"task-{index}", 100, 10),))
    db.execute("UPDATE batches SET runtime_fingerprint='incompatible'")
    batch(db, "z-live", "z-live", (("compatible-task", 100, 10),))
    assert [t["id"] for t in claim(db, "worker")] == ["compatible-task"]


def test_same_worker_concurrent_claims_preserve_capacity_and_disjointness(db, scheduler_dsn):
    worker(db, "worker", slots=3)
    batch(db, tasks=tuple((f"task-{i}", 100, 10) for i in range(10)))
    def contender(_):
        with psycopg.connect(scheduler_dsn, row_factory=dict_row) as conn:
            return claim(conn, "worker", limit=3)
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(contender, range(4)))
    ids = [t["id"] for items in results for t in items]
    assert len(ids) == len(set(ids)) == 3
    counts = db.execute("SELECT pending_count,running_count FROM batches").fetchone()
    assert counts == {"pending_count": 7, "running_count": 3}


def test_claim_transaction_rolls_back_all_state_on_attempt_failure(db):
    worker(db, "worker")
    batch(db)
    db.execute("ALTER TABLE attempts ADD CONSTRAINT test_reject CHECK(worker_id<>'worker')")
    try:
        with pytest.raises(psycopg.errors.CheckViolation):
            claim(db, "worker")
        assert db.execute("SELECT status,attempts,runtime_fingerprint FROM tasks").fetchone() == {
            "status": "pending", "attempts": 0, "runtime_fingerprint": None}
        assert db.execute("SELECT pending_count,running_count FROM batches").fetchone() == {"pending_count": 1, "running_count": 0}
    finally:
        db.execute("ALTER TABLE attempts DROP CONSTRAINT test_reject")


def test_cost_policy_requires_profile_and_work_estimates(db):
    worker(db, "legacy", profiled=False, fingerprint=None)
    batch(db)
    with pytest.raises(psycopg.DatabaseError) as rejected:
        claim(db, "legacy")
    assert rejected.value.sqlstate == "BF400"
    assert len(claim(db, "legacy", policy="fair")) == 1
    assert db.execute("SELECT runtime_compatibility_scope FROM batches").fetchone()["runtime_compatibility_scope"] == "legacy_unverified"


def test_legacy_api_insert_after_migration_is_schedulable(db):
    worker(db, "worker", slots=2)
    batch(db)
    db.execute("""INSERT INTO tasks(id,tenant_id,batch_id,custom_id,model,body,status,max_attempts)
        VALUES('legacy-insert','tenant','batch','legacy-insert','model',%s,'pending',3)""",
        (Jsonb({"messages": [{"role": "user", "content": "hello"}], "max_completion_tokens": 512}),))
    db.execute("UPDATE batches SET total_count=2,pending_count=2")
    db.execute("UPDATE tenant_queues SET pending_count=2")
    row = db.execute("SELECT estimated_input_tokens,estimated_output_tokens,estimate_source FROM tasks WHERE id='legacy-insert'").fetchone()
    assert row["estimated_input_tokens"] > 0
    assert row["estimated_output_tokens"] == 512
    assert row["estimate_source"] == "postgres_jsonb_utf8_bytes_div4_and_output_cap_v1"
    assert len(claim(db, "worker", limit=2)) == 2


@pytest.mark.parametrize("legacy_path", ["sql-function", "direct-python-update"])
def test_legacy_claim_cannot_bypass_bound_runtime(db, legacy_path):
    worker(db, "one", fingerprint="runtime-v1")
    worker(db, "incompatible", fingerprint="runtime-v2")
    batch(db, age=61)
    first = claim(db, "one")[0]
    db.execute("UPDATE attempts SET status='expired',finished_at=clock_timestamp()")
    db.execute("UPDATE tasks SET status='pending',lease_expires_at=NULL,ready_at=clock_timestamp()")
    db.execute("UPDATE batches SET pending_count=1,running_count=0")
    db.execute("UPDATE tenant_queues SET pending_count=1,next_ready_at=clock_timestamp()")
    with pytest.raises(psycopg.DatabaseError) as rejected:
        if legacy_path == "sql-function":
            db.execute("SELECT batchforge_claim('incompatible','model',1,30)")
        else:
            db.execute("UPDATE tasks SET status='running',worker_id='incompatible',attempt_token='different' WHERE id=%s", (first["id"],))
    assert rejected.value.sqlstate == "BF409"
    assert db.execute("SELECT status,runtime_fingerprint FROM tasks").fetchone() == {
        "status": "pending", "runtime_fingerprint": "runtime-v1"}
    with pytest.raises(psycopg.DatabaseError) as immutable:
        db.execute("UPDATE tasks SET runtime_fingerprint=NULL")
    assert immutable.value.sqlstate == "BF409"


@pytest.mark.parametrize("column,value", [("prefill_tokens_per_second", float("nan")),
    ("decode_tokens_per_second", float("inf")), ("hourly_cost_usd", -1)])
def test_database_rejects_invalid_profile_even_without_api_validation(db, column, value):
    worker(db, "worker")
    with pytest.raises(psycopg.errors.CheckViolation):
        db.execute(psycopg.sql.SQL("UPDATE workers SET {}=%s WHERE id='worker'").format(psycopg.sql.Identifier(column)), (value,))
