"""Real PostgreSQL + TCP multi-replica availability and migration regressions."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
import uuid

from fastapi.testclient import TestClient
import httpx
import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
import pytest

from backend.app import create_app
from backend.config import Settings
from backend.migrate import SchemaMismatch, migrate_database

DSN = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(not DSN, reason="Set TEST_DATABASE_URL for real PostgreSQL HA tests")
API_KEY = "ha-test-api-credential-not-real"
OLD_TOKEN = "ha-test-worker-old-credential"
NEW_TOKEN = "ha-test-worker-new-credential"
MODEL = "ha-test-model"
PUBLIC = {"Authorization": f"Bearer {API_KEY}"}
INTERNAL = {"Authorization": f"Bearer {OLD_TOKEN}"}


@contextmanager
def temporary_database():
    """Never change server defaults or the development database's schema."""
    name = "bf_ha_" + uuid.uuid4().hex
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE DATABASE {} TEMPLATE template0 ENCODING 'UTF8'").format(sql.Identifier(name)))
    values = conninfo_to_dict(DSN)
    values["dbname"] = name
    uri = make_conninfo(**values)
    try:
        yield uri
    finally:
        with psycopg.connect(DSN, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))


@pytest.fixture
def database():
    with temporary_database() as uri:
        yield uri


def settings(database, **extra):
    return Settings(database_url=database, batchforge_worker_token=OLD_TOKEN, model_name=MODEL, **extra)


def test_serialized_migration_and_verify_only_api_startup(database):
    with ThreadPoolExecutor(max_workers=2) as pool:
        applied = list(pool.map(lambda _: migrate_database(database, API_KEY), range(2)))
    assert sorted(applied) == [[], [1, 2, 3]]
    # API replicas need no tenant bootstrap secret and do no DDL or key writes.
    with TestClient(create_app(settings(database))) as first, TestClient(create_app(settings(database))) as second:
        assert first.get("/readyz").status_code == second.get("/readyz").status_code == 200
        assert second.get("/v1/models", headers=PUBLIC).status_code == 200
    with psycopg.connect(database) as conn:
        assert conn.execute("SELECT count(*) FROM batchforge_schema_migrations").fetchone()[0] == 3
        assert conn.execute("SELECT count(*) FROM api_keys").fetchone()[0] == 1


def test_unmigrated_startup_fails_without_creating_tables(database):
    with pytest.raises(SchemaMismatch, match="not versioned"):
        with TestClient(create_app(settings(database))):
            pass
    with psycopg.connect(database) as conn:
        assert conn.execute("SELECT count(*) FROM pg_tables WHERE schemaname='public'").fetchone()[0] == 0


def test_additive_v2_two_phase_rollout_and_unknown_version_rejection(database):
    assert migrate_database(database, API_KEY, target_version=1) == [1]
    # The compatibility release starts and remains ready against schema v1.
    with TestClient(create_app(settings(database))) as compatible:
        assert compatible.get("/readyz").status_code == 200
        with pytest.raises(SchemaMismatch, match="requires schema v2"):
            with TestClient(create_app(settings(database, sql_completion_enabled=True))):
                pass
        assert migrate_database(database, target_version=2) == [2]
        # Adding functions does not make the already-running compatible API
        # unready, and the new feature can now be enabled on another replica.
        assert compatible.get("/readyz").status_code == 200
        with TestClient(create_app(settings(database, sql_completion_enabled=True))) as optimized:
            assert optimized.get("/readyz").status_code == 200
        with psycopg.connect(database, autocommit=True) as conn:
            rows = conn.execute("SELECT prosecdef,proconfig FROM pg_proc WHERE oid IN ('public.batchforge_complete(text,text,text,jsonb,bigint,bigint)'::regprocedure,'public.batchforge_heartbeat(text,text,text,integer)'::regprocedure)").fetchall()
            assert len(rows) == 2
            assert all(row[0] is False and row[1] == ['search_path=pg_catalog, public'] for row in rows)
            conn.execute("INSERT INTO batchforge_schema_migrations(version,checksum) VALUES(4,'unknown-future-version')")
        assert compatible.get("/readyz").status_code == 503
    with pytest.raises(SchemaMismatch):
        with TestClient(create_app(settings(database))):
            pass


def test_additive_v3_claim_flag_requires_compatible_rollout_before_enable(database):
    assert migrate_database(database, API_KEY, target_version=2) == [1, 2]
    with TestClient(create_app(settings(database, sql_completion_enabled=True))) as compatible:
        assert compatible.get("/readyz").status_code == 200
        with pytest.raises(SchemaMismatch, match="requires schema v3"):
            with TestClient(create_app(settings(database, sql_completion_enabled=True, sql_claim_enabled=True))):
                pass
        assert migrate_database(database, target_version=3) == [3]
        assert compatible.get("/readyz").status_code == 200
        with TestClient(create_app(settings(database, sql_completion_enabled=True, sql_claim_enabled=True))) as optimized:
            assert optimized.get("/readyz").status_code == 200
        with psycopg.connect(database) as conn:
            row = conn.execute("SELECT prosecdef,proconfig FROM pg_proc WHERE oid='public.batchforge_claim(text,text,integer,integer)'::regprocedure").fetchone()
            assert row == (False, ['search_path=pg_catalog, public'])


def test_readiness_rejects_readonly_database_and_schema_drift(database):
    migrate_database(database, API_KEY)
    with TestClient(create_app(settings(database))) as client:
        assert client.get("/readyz").status_code == 200
        with psycopg.connect(database, autocommit=True) as conn:
            conn.execute("UPDATE batchforge_schema_migrations SET checksum='unexpected-release'")
        assert client.get("/readyz").status_code == 503
        assert client.get("/healthz").status_code == 200
    # Check actual PostgreSQL read-only sessions, not a mocked probe result.
    with psycopg.connect(database, autocommit=True) as conn:
        conn.execute(sql.SQL("ALTER DATABASE {} SET default_transaction_read_only=on").format(sql.Identifier(conn.info.dbname)))
        from backend.migrate import MIGRATIONS
        for version, _, checksum in MIGRATIONS:
            conn.execute("UPDATE batchforge_schema_migrations SET checksum=%s WHERE version=%s", (checksum, version))
    with TestClient(create_app(settings(database))) as client:
        assert client.get("/readyz").status_code == 503
        assert client.get("/healthz").status_code == 200
        result = client.post("/internal/workers/register", headers=INTERNAL, json={"worker_id": "test-worker", "name": "test-worker", "model": MODEL, "concurrency": 1})
        assert result.status_code == 503
        assert result.headers["Retry-After"] == "1"
        assert "read-only" not in result.text and database not in result.text


def test_worker_rotation_overlap_then_retirement(database):
    migrate_database(database, API_KEY)
    register = {"worker_id": "rotating-worker", "name": "rotating-worker", "model": MODEL, "concurrency": 1}
    overlap = settings(database, batchforge_worker_tokens_json=json.dumps([NEW_TOKEN]))
    with TestClient(create_app(overlap)) as client:
        for token in (OLD_TOKEN, NEW_TOKEN):
            assert client.post("/internal/workers/register", headers={"Authorization": f"Bearer {token}"}, json=register).status_code == 200
        assert client.post("/internal/workers/register", headers=PUBLIC, json=register).status_code == 401
    retired = Settings(database_url=database, model_name=MODEL, batchforge_worker_tokens_json=json.dumps([NEW_TOKEN]))
    with TestClient(create_app(retired)) as client:
        assert client.post("/internal/workers/register", headers=INTERNAL, json=register).status_code == 401
        assert client.post("/internal/workers/register", headers={"Authorization": f"Bearer {NEW_TOKEN}"}, json=register).status_code == 200


@contextmanager
def replicas(database, directory: Path, sql_completion_enabled=False):
    processes, clients, handles = [], [], []
    try:
        for index in range(2):
            with socket.socket() as listener:
                listener.bind(("127.0.0.1", 0))
                port = listener.getsockname()[1]
            log_path = directory / f"api-{index}.log"
            handle = log_path.open("w+")
            handles.append(handle)
            env = {**os.environ, "DATABASE_URL": database, "BATCHFORGE_API_KEY": "", "BATCHFORGE_WORKER_TOKEN": OLD_TOKEN,
                   "BATCHFORGE_WORKER_TOKENS_JSON": "", "MODEL_NAME": MODEL, "LEASE_SECONDS": "5", "RETRY_BASE_SECONDS": "1", "SQL_COMPLETION_ENABLED": str(sql_completion_enabled)}
            process = subprocess.Popen([sys.executable, "-m", "uvicorn", "backend.app:app", "--host", "127.0.0.1", "--port", str(port), "--no-access-log"], env=env, stdout=handle, stderr=subprocess.STDOUT)
            processes.append(process)
            client = httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=10)
            clients.append(client)
            deadline = time.monotonic() + 20
            while True:
                try:
                    if client.get("/readyz").status_code == 200:
                        break
                except httpx.TransportError:
                    pass
                if process.poll() is not None or time.monotonic() > deadline:
                    pytest.fail(f"Replica {index} failed to start: {log_path.read_text()}")
                time.sleep(0.05)
        yield processes, clients
    finally:
        for client in clients:
            client.close()
        for process in processes:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
        for handle in handles:
            handle.close()


@pytest.mark.parametrize("sql_completion_enabled", [False, True])
def test_tcp_replica_death_recovers_leases_and_replays_committed_results_once(database, tmp_path, sql_completion_enabled):
    migrate_database(database, API_KEY)
    with replicas(database, tmp_path, sql_completion_enabled) as (processes, clients):
        first, second = clients
        content = "\n".join(json.dumps({"custom_id": f"ha-{index}", "method": "POST", "url": "/v1/chat/completions", "body": {"model": MODEL, "messages": [{"role": "user", "content": "ha workload"}], "max_tokens": 16}}) for index in range(2))
        uploaded = first.post("/v1/files", headers=PUBLIC, files={"file": ("ha.jsonl", content)}).json()
        create_body = {"input_file_id": uploaded["id"]}
        created = first.post("/v1/batches", headers={**PUBLIC, "Idempotency-Key": "ha-create"}, json=create_body)
        assert created.status_code == 200, created.text
        bid = created.json()["id"]
        register_body = {"worker_id": "ha-worker", "name": "ha-worker", "model": MODEL, "concurrency": 2}
        assert first.post("/internal/workers/register", headers=INTERNAL, json=register_body).status_code == 200
        claim_body = {"worker_id": "ha-worker", "model": MODEL, "limit": 2}
        tasks = second.post("/internal/tasks/claim", headers=INTERNAL, json=claim_body).json()["tasks"]
        assert len(tasks) == 2
        completion = {"worker_id": "ha-worker", "attempt_token": tasks[0]["attempt_token"], "response": {"choices": [{"message": {"role": "assistant", "content": "done"}}], "usage": {"prompt_tokens": 7, "completion_tokens": 3}}}
        complete_path = f"/internal/tasks/{tasks[0]['id']}/complete"
        assert first.post(complete_path, headers=INTERNAL, json=completion).status_code == 200
        # Kill one API process outright. There is no process-local queue state
        # or accepted-result state to transfer to the surviving replica.
        processes[0].kill()
        processes[0].wait(timeout=5)
        assert second.post(complete_path, headers=INTERNAL, json=completion).json()["duplicate"] is True
        assert second.post("/v1/batches", headers={**PUBLIC, "Idempotency-Key": "ha-create"}, json=create_body).json()["id"] == bid
        with psycopg.connect(database, autocommit=True) as conn:
            conn.execute("UPDATE tasks SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=%s", (tasks[1]["id"],))
        deadline = time.monotonic() + 12
        recovered = []
        while not recovered and time.monotonic() < deadline:
            recovered = second.post("/internal/tasks/claim", headers=INTERNAL, json=claim_body).json()["tasks"]
            if not recovered:
                time.sleep(0.1)
        assert len(recovered) == 1
        assert recovered[0]["id"] == tasks[1]["id"]
        assert recovered[0]["attempt_token"] != tasks[1]["attempt_token"]
        stale = {**completion, "attempt_token": tasks[1]["attempt_token"]}
        recovered_path = f"/internal/tasks/{tasks[1]['id']}/complete"
        assert second.post(recovered_path, headers=INTERNAL, json=stale).status_code == 409
        assert second.post(recovered_path, headers=INTERNAL, json={**completion, "attempt_token": recovered[0]["attempt_token"]}).status_code == 200
        final = second.get(f"/v1/batches/{bid}", headers=PUBLIC).json()
        assert final["status"] == "completed"
        assert final["request_counts"]["completed"] == 2
        assert final["usage"] == {"prompt_tokens": 14, "completion_tokens": 6}
        output = second.get(f"/v1/files/{final['output_file_id']}/content", headers=PUBLIC).text.splitlines()
        assert len(output) == len({json.loads(line)["id"] for line in output}) == 2


def test_managed_cutover_atomic_copy_hashes_and_refusal_to_overwrite(database, tmp_path):
    from ops.ha.managed_cutover import transfer

    migrate_database(database, API_KEY)
    with TestClient(create_app(settings(database))) as client:
        content = json.dumps({"custom_id": "managed-migration", "method": "POST", "url": "/v1/chat/completions", "body": {"model": MODEL, "messages": [{"role": "user", "content": "migration fixture"}], "max_tokens": 16}})
        uploaded = client.post("/v1/files", headers=PUBLIC, files={"file": ("migration.jsonl", content)}).json()
        batch = client.post("/v1/batches", headers={**PUBLIC, "Idempotency-Key": "migration-create"}, json={"input_file_id": uploaded["id"]}).json()
        with temporary_database() as target:
            with pytest.raises(ValueError, match="Drain"):
                transfer(database, target)
            assert client.post(f"/v1/batches/{batch['id']}/cancel", headers=PUBLIC).status_code == 200
    # All source app lifespans and their recovery loops have now stopped.
    with temporary_database() as target:
        preflight = transfer(database, target)
        assert preflight["mode"] == "dry-run" and preflight["copied"] is False
        with pytest.raises(ValueError, match="Stop source APIs"):
            transfer(database, target, apply=True)
        report = transfer(database, target, apply=True, writers_stopped=True)
        assert report["all_counts_and_hashes_match"] is True
        assert report["source_counts"]["tasks"] == 1
        with TestClient(create_app(settings(target))) as moved:
            assert moved.get(f"/v1/batches/{batch['id']}", headers=PUBLIC).json()["status"] == "cancelled"
            replay = moved.post("/v1/batches", headers={**PUBLIC, "Idempotency-Key": "migration-create"}, json={"input_file_id": uploaded["id"]})
            assert replay.json()["id"] == batch["id"]
        with pytest.raises(ValueError, match="empty"):
            transfer(database, target, apply=True, writers_stopped=True)


@pytest.mark.parametrize("sql_completion_enabled,sql_claim_enabled", [(False, False), (True, False), (True, True), (False, True)])
def test_runtime_role_can_execute_batch_lifecycle_without_schema_or_key_admin(database, sql_completion_enabled, sql_claim_enabled):
    migrate_database(database, API_KEY)
    role = "bf_runtime_" + uuid.uuid4().hex[:12]
    root = Path(__file__).resolve().parents[1]
    values = conninfo_to_dict(database)
    with psycopg.connect(database, autocommit=True) as owner:
        owner.execute(sql.SQL("CREATE ROLE {} LOGIN").format(sql.Identifier(role)))
        grants = (root / "ops/ha/runtime_grants.sql").read_text().replace("bf_runtime", role).replace("ON DATABASE batchforge", "ON DATABASE " + sql.Identifier(owner.info.dbname).as_string(owner))
        owner.execute(grants)
    runtime_url = make_conninfo(**{**values, "user": role})
    try:
        with TestClient(create_app(settings(runtime_url, sql_completion_enabled=sql_completion_enabled, sql_claim_enabled=sql_claim_enabled))) as client:
            assert client.get("/readyz").status_code == 200
            content = json.dumps({"custom_id": "least-privilege", "method": "POST", "url": "/v1/chat/completions", "body": {"model": MODEL, "messages": [{"role": "user", "content": "minimum privileges"}], "max_tokens": 16}})
            uploaded = client.post("/v1/files", headers=PUBLIC, files={"file": ("role.jsonl", content)})
            assert uploaded.status_code == 200, uploaded.text
            created = client.post("/v1/batches", headers={**PUBLIC, "Idempotency-Key": "low-role"}, json={"input_file_id": uploaded.json()["id"]})
            assert created.status_code == 200, created.text
            bid = created.json()["id"]
            assert client.post("/internal/workers/register", headers=INTERNAL, json={"worker_id": "low-role-worker", "name": "low-role-worker", "model": MODEL, "concurrency": 1}).status_code == 200
            task = client.post("/internal/tasks/claim", headers=INTERNAL, json={"worker_id": "low-role-worker", "model": MODEL, "limit": 1}).json()["tasks"][0]
            heartbeat = client.post(f"/internal/tasks/{task['id']}/heartbeat", headers=INTERNAL, json={"worker_id": "low-role-worker", "attempt_token": task["attempt_token"]})
            assert heartbeat.status_code == 200, heartbeat.text
            completed = client.post(f"/internal/tasks/{task['id']}/complete", headers=INTERNAL, json={"worker_id": "low-role-worker", "attempt_token": task["attempt_token"], "response": {"choices": [{"message": {"role": "assistant", "content": "done"}}], "usage": {"prompt_tokens": 3, "completion_tokens": 1}}})
            assert completed.status_code == 200, completed.text
            assert client.get(f"/v1/batches/{bid}", headers=PUBLIC).json()["status"] == "completed"
        with psycopg.connect(runtime_url, autocommit=True) as conn:
            for statement in ("CREATE TABLE forbidden_schema_change(id integer)", "INSERT INTO tenants(id,name) VALUES('forbidden','forbidden')", "UPDATE api_keys SET revoked=true"):
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    conn.execute(statement)
    finally:
        with psycopg.connect(database, autocommit=True) as owner:
            owner.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role)))
            owner.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))
