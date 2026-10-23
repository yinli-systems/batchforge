"""Real PostgreSQL state-machine tests.

Set TEST_DATABASE_URL to a disposable database. Tables are truncated per test.
Tests never substitute SQLite or an in-memory lease implementation.
"""
import hashlib
import json
import os
from concurrent.futures import ThreadPoolExecutor

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg.rows import dict_row

from backend.app import create_app
from backend.config import Settings
from backend.migrate import migrate_database

DSN = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(not DSN, reason="Set TEST_DATABASE_URL to run actual PostgreSQL integration tests")
API_KEY = "test-api-key-not-a-real-secret"
OTHER_KEY = "test-other-key-not-a-real-secret"
WORKER_TOKEN = "test-worker-token-not-a-real-secret"
MODEL = "test-model"
PUBLIC = {"Authorization": f"Bearer {API_KEY}"}
OTHER = {"Authorization": f"Bearer {OTHER_KEY}"}
INTERNAL = {"Authorization": f"Bearer {WORKER_TOKEN}"}


@pytest.fixture(params=[(False, False), (True, False), (True, True), (False, True)], ids=["python-both", "sql-complete", "sql-both", "sql-claim"])
def client(request):
    migrate_database(DSN, API_KEY)
    # Destructive test reset happens before recovery loops start. TRUNCATE's
    # ACCESS EXCLUSIVE locks can deadlock with a live sweep's multi-table reads.
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute("TRUNCATE idempotency,attempts,tasks,batches,files,workers,tenant_queues CASCADE")
        conn.execute("INSERT INTO tenants(id,name) VALUES('other','Other tenant') ON CONFLICT DO NOTHING")
        conn.execute("INSERT INTO api_keys(key_hash,tenant_id) VALUES(%s,'other') ON CONFLICT DO NOTHING", (hashlib.sha256(OTHER_KEY.encode()).hexdigest(),))
    app = create_app(Settings(database_url=DSN, batchforge_api_key=API_KEY, batchforge_worker_token=WORKER_TOKEN, model_name=MODEL, lease_seconds=30, retry_base_seconds=1, max_attempts=2, sql_completion_enabled=request.param[0], sql_claim_enabled=request.param[1]))
    with TestClient(app) as client:
        yield client


def sql(statement, values=()):
    with psycopg.connect(DSN, autocommit=True, row_factory=dict_row) as conn:
        return conn.execute(statement, values).fetchall() if statement.lstrip().upper().startswith("SELECT") else conn.execute(statement, values)


def make_batch(client, count=1, headers=PUBLIC, key=None):
    content = "\n".join(json.dumps({"custom_id": f"item-{index}", "method": "POST", "url": "/v1/chat/completions", "body": {"model": MODEL, "messages": [{"role": "user", "content": f"hello {index}"}], "max_tokens": 16}}) for index in range(count))
    uploaded = client.post("/v1/files", headers=headers, files={"file": ("input.jsonl", content, "application/jsonl")}, data={"purpose": "batch"})
    assert uploaded.status_code == 200, uploaded.text
    body = {"input_file_id": uploaded.json()["id"], "endpoint": "/v1/chat/completions", "completion_window": "24h"}
    response = client.post("/v1/batches", json=body, headers={**headers, **({"Idempotency-Key": key} if key else {})})
    assert response.status_code == 200, response.text
    return response.json(), body


def register(client, wid="worker-one", concurrency=16):
    response = client.post("/internal/workers/register", headers=INTERNAL, json={"worker_id": wid, "name": wid, "model": MODEL, "concurrency": concurrency})
    assert response.status_code == 200, response.text


def claim(client, wid="worker-one", limit=16):
    response = client.post("/internal/tasks/claim", headers=INTERNAL, json={"worker_id": wid, "model": MODEL, "limit": limit})
    assert response.status_code == 200, response.text
    return response.json()["tasks"]


def response_body(task):
    return {"worker_id": "worker-one", "attempt_token": task["attempt_token"], "response": {"id": "chat-test", "object": "chat.completion", "model": MODEL, "choices": [{"index": 0, "message": {"role": "assistant", "content": "hello"}, "finish_reason": "stop"}], "usage": {"prompt_tokens": 7, "completion_tokens": 2, "total_tokens": 9}}}


def complete(client, task, **changes):
    body = response_body(task)
    body.update(changes)
    return client.post(f"/internal/tasks/{task['id']}/complete", headers=INTERNAL, json=body)


def make_ready():
    sql("UPDATE tasks SET ready_at=clock_timestamp() WHERE status='pending'")
    sql("UPDATE tenant_queues SET next_ready_at=clock_timestamp()")


def test_idempotency_and_tenant_isolation(client):
    batch, body = make_batch(client, 2, key="same-request")
    replay = client.post("/v1/batches", headers={**PUBLIC, "Idempotency-Key": "same-request"}, json=body)
    assert replay.json()["id"] == batch["id"]
    changed = {**body, "metadata": {"different": "yes"}}
    assert client.post("/v1/batches", headers={**PUBLIC, "Idempotency-Key": "same-request"}, json=changed).status_code == 409
    assert client.get(f"/v1/batches/{batch['id']}", headers=OTHER).status_code == 404
    assert client.get(f"/v1/files/{body['input_file_id']}/content", headers=OTHER).status_code == 404
    assert client.post(f"/v1/batches/{batch['id']}/cancel", headers=OTHER).status_code == 404
    assert client.get("/v1/batches", headers=OTHER).json()["data"] == []
    assert client.get("/v1/overview", headers=OTHER).json()["stats"]["total_tasks"] == 0
    assert client.post("/internal/tasks/claim", headers=PUBLIC, json={"worker_id": "one", "model": MODEL}).status_code == 401
    assert client.get("/metrics", headers=PUBLIC).status_code == 401


def test_lease_fencing_duplicate_commit_and_metering(client):
    batch, _ = make_batch(client)
    register(client)
    old = claim(client)[0]
    sql("UPDATE tasks SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=%s", (old["id"],))
    assert complete(client, old).status_code == 409
    client.portal.call(client.app.state.store.recover)
    make_ready()
    new = claim(client)[0]
    assert new["attempt_token"] != old["attempt_token"]
    assert new["attempt_number"] == 2
    assert complete(client, old).status_code == 409
    assert complete(client, new).status_code == 200
    assert complete(client, new).json()["duplicate"] is True
    done = client.get(f"/v1/batches/{batch['id']}", headers=PUBLIC).json()
    assert done["status"] == "completed"
    assert done["request_counts"]["completed"] == 1
    assert done["usage"] == {"prompt_tokens": 7, "completion_tokens": 2}
    result = client.get(f"/v1/files/{done['output_file_id']}/content", headers=PUBLIC)
    assert json.loads(result.text)["custom_id"] == "item-0"
    assert "batchforge_durable_completed 1" in client.get("/metrics", headers=INTERNAL).text


def test_cancellation_fences_inflight_results_and_keeps_partial_output(client):
    batch, _ = make_batch(client, 3)
    register(client)
    tasks = claim(client)
    assert complete(client, tasks[0]).status_code == 200
    cancelled = client.post(f"/v1/batches/{batch['id']}/cancel", headers=PUBLIC).json()
    assert cancelled["status"] == "cancelled"
    assert cancelled["request_counts"]["completed"] == 1
    assert cancelled["request_counts"]["cancelled"] == 2
    assert complete(client, tasks[1]).status_code == 409
    assert claim(client) == []
    output = client.get(f"/v1/files/{cancelled['output_file_id']}/content", headers=PUBLIC).text
    errors = client.get(f"/v1/files/{cancelled['error_file_id']}/content", headers=PUBLIC).text
    assert len(output.splitlines()) == 1 and len(errors.splitlines()) == 2
    assert client.post(f"/v1/batches/{batch['id']}/cancel", headers=PUBLIC).json()["request_counts"] == cancelled["request_counts"]


def test_cancel_completion_race_preserves_counter_invariant(client):
    batch, _ = make_batch(client)
    register(client)
    task = claim(client)[0]
    with ThreadPoolExecutor(max_workers=2) as pool:
        complete_future = pool.submit(complete, client, task)
        cancel_future = pool.submit(client.post, f"/v1/batches/{batch['id']}/cancel", headers=PUBLIC)
        completed, cancelled = complete_future.result(), cancel_future.result()
    assert completed.status_code in (200, 409)
    assert cancelled.status_code == 200
    final = client.get(f"/v1/batches/{batch['id']}", headers=PUBLIC).json()
    counts = final["request_counts"]
    assert counts["completed"] + counts["cancelled"] == 1
    assert counts["pending"] == counts["running"] == 0
    assert sum(counts[key] for key in ("pending", "running", "completed", "failed", "cancelled")) == counts["total"]


def test_concurrent_claims_are_disjoint_and_worker_capacity_enforced(client):
    make_batch(client, 10)
    register(client, "one", concurrency=4)
    register(client, "two", concurrency=4)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first, second = list(pool.map(lambda wid: claim(client, wid, 10), ["one", "two"]))
    ids = [task["id"] for task in first + second]
    assert len(ids) == len(set(ids))
    # A busy batch can be skipped by one claimant; a subsequent claim must fill it.
    for wid in ("one", "two"):
        claim(client, wid, 10)
        assert claim(client, wid, 10) == []
    assert sql("SELECT count(*) AS n FROM tasks WHERE status='running'")[0]["n"] == 8


def test_retry_backoff_exhaustion_and_failed_only_retry(client):
    batch, _ = make_batch(client, 2)
    register(client)
    tasks = claim(client)
    assert complete(client, tasks[0]).status_code == 200
    failure = {"worker_id": "worker-one", "attempt_token": tasks[1]["attempt_token"], "error": {"code": "engine_busy", "message": "retry me"}, "retryable": True}
    assert client.post(f"/internal/tasks/{tasks[1]['id']}/fail", headers=INTERNAL, json=failure).json()["status"] == "pending"
    assert claim(client) == []
    make_ready()
    task = claim(client)[0]
    failure["attempt_token"] = task["attempt_token"]
    assert client.post(f"/internal/tasks/{task['id']}/fail", headers=INTERNAL, json=failure).json()["status"] == "failed"
    final = client.get(f"/v1/batches/{batch['id']}", headers=PUBLIC).json()
    assert final["request_counts"]["completed"] == final["request_counts"]["failed"] == 1
    retried = client.post(f"/v1/batches/{batch['id']}/retry", headers={**PUBLIC, "Idempotency-Key": "retry-once"})
    assert retried.status_code == 200, retried.text
    assert retried.json()["request_counts"]["total"] == 1
    assert client.post(f"/v1/batches/{batch['id']}/retry", headers={**PUBLIC, "Idempotency-Key": "retry-once"}).json()["id"] == retried.json()["id"]


def test_tenant_dispatch_fairness_and_expiration(client):
    first, _ = make_batch(client, 10)
    second, _ = make_batch(client, 2, headers=OTHER)
    register(client)
    claimed = claim(client, limit=1)
    next_claimed = claim(client, limit=1)
    assert {claimed[0]["batch_id"], next_claimed[0]["batch_id"]} == {first["id"], second["id"]}
    sql("UPDATE batches SET expires_at=clock_timestamp()-interval '1 second' WHERE id=%s", (first["id"],))
    client.portal.call(client.app.state.store.recover)
    expired = client.get(f"/v1/batches/{first['id']}", headers=PUBLIC).json()
    assert expired["status"] == "failed"
    assert expired["request_counts"]["cancelled"] == 10


def test_api_routes_do_not_fall_back_to_html(client):
    assert client.get("/v1/missing").status_code == 404
    assert client.get("/internal/missing").status_code == 404
    assert client.get("/healthz").json() == {"status": "ok"}
    assert client.get("/readyz").status_code == 200


def test_malformed_json_is_400_and_deadline_fences_without_reaper(client):
    for malformed in (b'{"metadata":{"x":NaN}}', b'{"metadata":{"x":"\\ud800"}}'):
        result = client.post("/v1/batches", content=malformed, headers={**PUBLIC, "Content-Type": "application/json"})
        assert result.status_code == 400
    invalid = client.post("/v1/files", headers=PUBLIC, files={"file": ("bad.jsonl", b'{"custom_id":"x","x":NaN}')})
    assert invalid.status_code == 400
    batch, _ = make_batch(client)
    register(client)
    task = claim(client)[0]
    sql("UPDATE batches SET expires_at=clock_timestamp()-interval '1 second' WHERE id=%s", (batch["id"],))
    assert complete(client, task).status_code == 409
    assert client.post(f"/internal/tasks/{task['id']}/heartbeat", headers=INTERNAL, json={"worker_id": "worker-one", "attempt_token": task["attempt_token"]}).status_code == 409


def test_operator_tenant_key_rotation_and_revocation(client, monkeypatch, capsys):
    from backend import admin
    monkeypatch.setattr(admin, "get_settings", lambda: Settings(database_url=DSN))
    monkeypatch.setattr("sys.argv", ["admin", "create-tenant", "--name", "New customer"])
    admin.main()
    issued = json.loads(capsys.readouterr().out)
    headers = {"Authorization": f"Bearer {issued['api_key']}"}
    assert client.get("/v1/models", headers=headers).status_code == 200
    assert issued["key_hash"] == hashlib.sha256(issued["api_key"].encode()).hexdigest()
    monkeypatch.setattr("sys.argv", ["admin", "add-key", "--tenant-id", issued["tenant_id"]])
    admin.main()
    rotated = json.loads(capsys.readouterr().out)
    monkeypatch.setattr("sys.argv", ["admin", "revoke-key", "--key-hash", issued["key_hash"]])
    admin.main()
    capsys.readouterr()
    assert client.get("/v1/models", headers=headers).status_code == 401
    assert client.get("/v1/models", headers={"Authorization": f"Bearer {rotated['api_key']}"}).status_code == 200


def test_concurrent_idempotent_batch_creation_has_no_fk_lock_upgrade_deadlock(client):
    # Same flow as scripts/benchmark.py: upload once, then submit independent
    # batches concurrently, each with its own idempotency key.
    initial, body = make_batch(client, 8)
    client.post(f"/v1/batches/{initial['id']}/cancel", headers=PUBLIC)
    def submit(index):
        return client.post("/v1/batches", headers={**PUBLIC, "Idempotency-Key": f"concurrent-{index}"}, json=body)
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(submit, range(4)))
    assert [result.status_code for result in results] == [200] * 4
    assert len({result.json()["id"] for result in results}) == 4
    overview = client.get("/v1/overview", headers=PUBLIC).json()
    assert overview["stats"]["queued_tasks"] == 32
    assert sql("SELECT pending_count FROM tenant_queues WHERE tenant_id='default'")[0]["pending_count"] == 32
    # Replay all keys concurrently must not admit another task.
    with ThreadPoolExecutor(max_workers=4) as pool:
        replays = list(pool.map(submit, range(4)))
    assert [result.json()["id"] for result in replays] == [result.json()["id"] for result in results]
    assert client.get("/v1/overview", headers=PUBLIC).json()["stats"]["queued_tasks"] == 32


def test_concurrent_admission_respects_tenant_queue_cap(client):
    initial, body = make_batch(client, 6)
    client.post(f"/v1/batches/{initial['id']}/cancel", headers=PUBLIC)
    client.app.state.settings.max_queued_tasks_per_tenant = 10
    def submit(index):
        return client.post("/v1/batches", headers={**PUBLIC, "Idempotency-Key": f"capped-{index}"}, json=body)
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(submit, range(4)))
    assert sorted(result.status_code for result in results) == [200, 429, 429, 429]
    assert client.get("/v1/overview", headers=PUBLIC).json()["stats"]["queued_tasks"] == 6
    assert sql("SELECT pending_count FROM tenant_queues WHERE tenant_id='default'")[0]["pending_count"] == 6


def test_real_http_four_concurrent_batches_32_tasks(client, tmp_path):
    """Exercise the deployed benchmark's submission pattern over actual TCP HTTP."""
    import socket
    import subprocess
    import sys
    import time

    import httpx

    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    env = {**os.environ, "DATABASE_URL": DSN, "BATCHFORGE_API_KEY": API_KEY, "BATCHFORGE_WORKER_TOKEN": WORKER_TOKEN, "MODEL_NAME": MODEL}
    log_path = tmp_path / "api.log"
    with log_path.open("w+") as log:
        process = subprocess.Popen([sys.executable, "-m", "uvicorn", "backend.app:app", "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"], env=env, stdout=log, stderr=subprocess.STDOUT)
        try:
            with httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=10) as http:
                deadline = time.monotonic() + 15
                while True:
                    try:
                        if http.get("/readyz").status_code == 200:
                            break
                    except httpx.TransportError:
                        pass
                    if process.poll() is not None or time.monotonic() > deadline:
                        log.flush()
                        pytest.fail(f"HTTP API failed to start: {log_path.read_text()}")
                    time.sleep(0.05)
                content = "\n".join(json.dumps({"custom_id": f"task-{index}", "method": "POST", "url": "/v1/chat/completions", "body": {"model": MODEL, "messages": [{"role": "user", "content": "hello"}], "max_tokens": 16}}) for index in range(8))
                uploaded = http.post("/v1/files", headers=PUBLIC, files={"file": ("benchmark.jsonl", content)}, data={"purpose": "batch"})
                assert uploaded.status_code == 200, uploaded.text
                body = {"input_file_id": uploaded.json()["id"], "endpoint": "/v1/chat/completions", "completion_window": "24h"}
                def submit(index):
                    return http.post("/v1/batches", headers={**PUBLIC, "Idempotency-Key": f"http-benchmark-{index}"}, json=body)
                with ThreadPoolExecutor(max_workers=4) as pool:
                    responses = list(pool.map(submit, range(4)))
                assert [response.status_code for response in responses] == [200] * 4, [response.text for response in responses]
                assert http.get("/v1/overview", headers=PUBLIC).json()["stats"]["queued_tasks"] == 32
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)


def test_worker_liveness_readiness_counts_and_private_metadata_isolation(client):
    body = {"worker_id": "readiness-worker", "name": "Readiness worker", "model": MODEL, "concurrency": 8, "metadata": {"engine_count": 2, "ready_engine_count": 0, "engine_urls": ["http://private-engine-secret-marker:8000"], "credential": "private-token-secret-marker"}}
    response = client.post("/internal/workers/register", headers=INTERNAL, json=body)
    assert response.status_code == 200
    snapshot = client.get("/v1/overview", headers=PUBLIC)
    worker = next(worker for worker in snapshot.json()["workers"] if worker["id"] == body["worker_id"])
    assert worker["status"] == "online"
    assert worker["engine_count"] == 2 and worker["ready_engine_count"] == 0
    assert "metadata" not in worker and "engine_urls" not in worker and "credential" not in worker
    assert "secret-marker" not in snapshot.text
    body["metadata"]["ready_engine_count"] = 2
    assert client.post("/internal/workers/register", headers=INTERNAL, json=body).status_code == 200
    worker = client.get("/v1/overview", headers=PUBLIC).json()["workers"][0]
    assert worker["status"] == "online" and worker["ready_engine_count"] == 2
    for engine_count, ready_engine_count in ((True, -1), (4097, "2"), (None, 2.5)):
        body["metadata"].update(engine_count=engine_count, ready_engine_count=ready_engine_count)
        assert client.post("/internal/workers/register", headers=INTERNAL, json=body).status_code == 200
        worker = client.get("/v1/overview", headers=PUBLIC).json()["workers"][0]
        assert worker["engine_count"] is None and worker["ready_engine_count"] is None


def test_model_upgrade_rejects_retry_of_unserved_model_without_admitting_work(client):
    batch, _ = make_batch(client)
    register(client)
    task = claim(client)[0]
    failed = client.post(f"/internal/tasks/{task['id']}/fail", headers=INTERNAL, json={"worker_id": "worker-one", "attempt_token": task["attempt_token"], "error": {"code": "test", "message": "failed before model upgrade"}, "retryable": False})
    assert failed.status_code == 200
    client.app.state.settings.model_name = "new-larger-model"
    result = client.post(f"/v1/batches/{batch['id']}/retry", headers={**PUBLIC, "Idempotency-Key": "upgrade-retry"})
    assert result.status_code == 409
    assert result.json()["error"]["code"] == "model_unavailable"
    assert sql("SELECT count(*) AS n FROM batches")[0]["n"] == 1
    assert sql("SELECT count(*) AS n FROM tasks")[0]["n"] == 1


def test_model_upgrade_cancellation_locks_the_historical_batch_queue(client):
    import time

    batch, _ = make_batch(client, 2)
    client.app.state.settings.model_name = "new-larger-model"
    with psycopg.connect(DSN) as blocker, ThreadPoolExecutor(max_workers=1) as pool:
        blocker.execute("SELECT * FROM tenant_queues WHERE tenant_id='default' AND model=%s FOR UPDATE", (MODEL,))
        future = pool.submit(client.post, f"/v1/batches/{batch['id']}/cancel", headers=PUBLIC)
        observed_block = False
        try:
            deadline = time.monotonic() + 1.5
            while time.monotonic() < deadline and not future.done():
                observed_block = bool(sql("SELECT count(*) AS n FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE %s", ("%tenant_queues%",))[0]["n"])
                if observed_block:
                    break
                time.sleep(0.01)
        finally:
            blocker.commit()
        result = future.result(timeout=5)
    assert observed_block, "Cancellation bypassed the batch's original model queue lock"
    assert result.status_code == 200, result.text
    assert result.json()["request_counts"]["cancelled"] == 2
    assert sql("SELECT pending_count FROM tenant_queues WHERE model=%s", (MODEL,))[0]["pending_count"] == 0


def test_mixed_old_new_completion_race_accepts_one_result_and_rolls_back_stale_worker_touch(client):
    batch, _ = make_batch(client)
    register(client)
    task = claim(client)[0]
    other_settings = client.app.state.settings.model_copy(update={"sql_completion_enabled": not client.app.state.settings.sql_completion_enabled})
    with TestClient(create_app(other_settings)) as other:
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = [future.result() for future in (pool.submit(complete, client, task), pool.submit(complete, other, task))]
    assert [result.status_code for result in results] == [200, 200]
    assert sum(result.json().get("duplicate", False) for result in results) == 1
    final = client.get(f"/v1/batches/{batch['id']}", headers=PUBLIC).json()
    assert final["usage"] == {"prompt_tokens": 7, "completion_tokens": 2}
    before = sql("SELECT last_seen_at FROM workers WHERE id='worker-one'")[0]["last_seen_at"]
    stale = client.post(f"/internal/tasks/{task['id']}/heartbeat", headers=INTERNAL, json={"worker_id": "worker-one", "attempt_token": task["attempt_token"]})
    assert stale.status_code == 409
    # Both a Python transaction and a failing SQL function must roll back their
    # initial worker timestamp mutation, not commit a partial heartbeat.
    assert sql("SELECT last_seen_at FROM workers WHERE id='worker-one'")[0]["last_seen_at"] == before


def test_fencing_clock_is_checked_after_waiting_for_queue_lock(client):
    import time

    make_batch(client)
    register(client)
    task = claim(client)[0]
    with psycopg.connect(DSN) as blocker, ThreadPoolExecutor(max_workers=1) as pool:
        blocker.execute("SELECT * FROM tenant_queues WHERE tenant_id='default' AND model=%s FOR UPDATE", (MODEL,))
        future = pool.submit(client.post, f"/internal/tasks/{task['id']}/heartbeat", headers=INTERNAL, json={"worker_id": "worker-one", "attempt_token": task["attempt_token"]})
        waiting = False
        try:
            deadline = time.monotonic() + 1.5
            while time.monotonic() < deadline and not future.done():
                waiting = bool(sql("SELECT count(*) AS n FROM pg_stat_activity WHERE %s=ANY(pg_blocking_pids(pid))", (blocker.info.backend_pid,))[0]["n"])
                if waiting:
                    break
                time.sleep(.01)
            blocker.execute("UPDATE tasks SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=%s", (task["id"],))
        finally:
            blocker.commit()
        result = future.result(timeout=5)
    assert waiting
    assert result.status_code == 409, result.text
    assert result.json()["error"]["code"] == "stale_attempt"


def test_terminal_metadata_failure_rolls_back_response_attempt_and_usage_atomically(client):
    batch, _ = make_batch(client)
    register(client)
    task = claim(client)[0]
    sql("CREATE FUNCTION public.bf_test_fail_output() RETURNS trigger LANGUAGE plpgsql AS $fn$ BEGIN RAISE EXCEPTION USING ERRCODE='PT001',MESSAGE='intentional test-only output failure'; END $fn$")
    sql("CREATE TRIGGER bf_test_output_failure BEFORE INSERT ON files FOR EACH ROW WHEN (NEW.kind='output') EXECUTE FUNCTION public.bf_test_fail_output()")
    try:
        with pytest.raises(psycopg.Error) as failed:
            client.portal.call(client.app.state.store.complete, task["id"], "worker-one", task["attempt_token"], response_body(task)["response"])
        assert failed.value.sqlstate == "PT001"
        row = sql("SELECT status,response,prompt_tokens,completion_tokens FROM tasks WHERE id=%s", (task["id"],))[0]
        assert row == {"status": "running", "response": None, "prompt_tokens": 0, "completion_tokens": 0}
        assert sql("SELECT status FROM attempts WHERE token=%s", (task["attempt_token"],))[0]["status"] == "running"
        current = client.get(f"/v1/batches/{batch['id']}", headers=PUBLIC).json()
        assert current["request_counts"]["running"] == 1
        assert current["request_counts"]["completed"] == 0
        assert current["usage"] == {"prompt_tokens": 0, "completion_tokens": 0}
    finally:
        sql("DROP TRIGGER bf_test_output_failure ON files")
        sql("DROP FUNCTION public.bf_test_fail_output()")
    assert complete(client, task).status_code == 200


def test_mixed_claim_modes_serialize_the_same_workers_capacity(client):
    make_batch(client, 12)
    register(client, concurrency=4)
    other_settings = client.app.state.settings.model_copy(update={"sql_claim_enabled": not client.app.state.settings.sql_claim_enabled})
    with TestClient(create_app(other_settings)) as other, ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(claim, target, "worker-one", 10) for target in (client, other)]
        results = [future.result() for future in futures]
    identifiers = [task["id"] for tasks in results for task in tasks]
    assert len(identifiers) == len(set(identifiers)) == 4
    assert sql("SELECT count(*) AS n FROM attempts")[0]["n"] == 4
    assert sql("SELECT pending_count FROM tenant_queues WHERE tenant_id='default'")[0]["pending_count"] == 8
    assert claim(client) == []


def test_claim_skips_locked_task_and_preserves_remaining_queue_count(client):
    batch, _ = make_batch(client, 3)
    register(client)
    with psycopg.connect(DSN) as blocker:
        locked = blocker.execute("SELECT id FROM tasks WHERE batch_id=%s ORDER BY ready_at,id LIMIT 1 FOR UPDATE", (batch["id"],)).fetchone()[0]
        tasks = claim(client)
        assert len(tasks) == 2
        assert locked not in {task["id"] for task in tasks}
        assert sql("SELECT pending_count FROM tenant_queues WHERE tenant_id='default'")[0]["pending_count"] == 1
        blocker.commit()
    remaining = claim(client)
    assert len(remaining) == 1 and remaining[0]["id"] == locked
    assert sql("SELECT count(*) AS n FROM attempts")[0]["n"] == 3


def test_claim_considers_only_eight_ready_tenants_before_recovery(client):
    tenants = [f"bounded-{index:02d}" for index in range(9)]
    for index, tenant in enumerate(tenants):
        key = f"bounded-tenant-test-credential-{index}"
        sql("INSERT INTO tenants(id,name) VALUES(%s,%s) ON CONFLICT DO NOTHING", (tenant, tenant))
        sql("INSERT INTO api_keys(key_hash,tenant_id) VALUES(%s,%s) ON CONFLICT DO NOTHING", (hashlib.sha256(key.encode()).hexdigest(), tenant))
        make_batch(client, headers={"Authorization": f"Bearer {key}"})
        sql("UPDATE tenant_queues SET last_dispatched_at='2000-01-01'::timestamptz+%s*interval '1 second' WHERE tenant_id=%s", (index, tenant))
    register(client)
    sql("UPDATE batches SET expires_at=clock_timestamp()-interval '1 second' WHERE tenant_id=ANY(%s)", (tenants[:8],))
    # No widening past the eight locked candidates when all eight batches are
    # expired. The bounded recovery sweep makes the ninth eligible next time.
    assert claim(client) == []
    client.portal.call(client.app.state.store.recover)
    task = claim(client)
    assert len(task) == 1
    assert sql("SELECT tenant_id FROM tasks WHERE id=%s", (task[0]["id"],))[0]["tenant_id"] == tenants[8]


def test_attempt_insert_failure_rolls_back_claim_capacity_and_queue_atomically(client):
    batch, _ = make_batch(client, 3)
    register(client)
    before = sql("SELECT last_seen_at FROM workers WHERE id='worker-one'")[0]["last_seen_at"]
    sql("CREATE FUNCTION public.bf_test_fail_attempt() RETURNS trigger LANGUAGE plpgsql AS $fn$ BEGIN RAISE EXCEPTION USING ERRCODE='PT002',MESSAGE='intentional test-only attempt failure'; END $fn$")
    sql("CREATE TRIGGER bf_test_attempt_failure BEFORE INSERT ON attempts FOR EACH ROW EXECUTE FUNCTION public.bf_test_fail_attempt()")
    try:
        with pytest.raises(psycopg.Error) as failed:
            client.portal.call(client.app.state.store.claim, "worker-one", MODEL, 3)
        assert failed.value.sqlstate == "PT002"
        assert sql("SELECT count(*) AS n FROM tasks WHERE status='pending'")[0]["n"] == 3
        assert sql("SELECT count(*) AS n FROM attempts")[0]["n"] == 0
        assert sql("SELECT last_seen_at FROM workers WHERE id='worker-one'")[0]["last_seen_at"] == before
        current = client.get(f"/v1/batches/{batch['id']}", headers=PUBLIC).json()
        assert current["request_counts"]["pending"] == 3 and current["request_counts"]["running"] == 0
        assert sql("SELECT pending_count FROM tenant_queues WHERE tenant_id='default'")[0]["pending_count"] == 3
    finally:
        sql("DROP TRIGGER bf_test_attempt_failure ON attempts")
        sql("DROP FUNCTION public.bf_test_fail_attempt()")
    assert len(claim(client)) == 3
