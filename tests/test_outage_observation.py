"""Outage observer SQL against an isolated real PostgreSQL state machine."""
from __future__ import annotations

import json
import os
import uuid

from fastapi.testclient import TestClient
import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
import pytest

from backend.app import create_app
from backend.config import Settings
from backend.migrate import migrate_database
from ops.ha.controlplane_outage_probe import ProbeFailure, Progress
from ops.ha.engine_outage_probe import EngineProgress


def test_readonly_run_snapshot_captured_lease_and_accepted_usage_invariant():
    test_dsn = os.environ.get("TEST_DATABASE_URL")
    if not test_dsn:
        pytest.skip("Set TEST_DATABASE_URL for the actual PostgreSQL observer test")
    name = "bf_outage_" + uuid.uuid4().hex
    with psycopg.connect(test_dsn, autocommit=True) as owner:
        owner.execute(sql.SQL("CREATE DATABASE {} TEMPLATE template0 ENCODING 'UTF8'").format(sql.Identifier(name)))
    connection = conninfo_to_dict(test_dsn)
    connection["dbname"] = name
    dsn = make_conninfo(**connection)
    progress = None
    try:
        key, token, model = "outage-test-public-credential", "outage-test-worker-credential", "test-model"
        migrate_database(dsn, key)
        app = create_app(Settings(database_url=dsn, batchforge_worker_token=token, model_name=model,
                                  lease_seconds=5, retry_base_seconds=1, max_attempts=2,
                                  sql_completion_enabled=True, sql_claim_enabled=True))
        public, internal = {"Authorization": "Bearer " + key}, {"Authorization": "Bearer " + token}
        with TestClient(app) as client:
            data = "\n".join(json.dumps({"custom_id": str(index), "method": "POST", "url": "/v1/chat/completions",
                "body": {"model": model, "messages": [{"role": "user", "content": "public test input"}], "max_tokens": 16}}) for index in range(3))
            uploaded = client.post("/v1/files", headers=public, files={"file": ("input.jsonl", data)}, data={"purpose": "batch"})
            assert uploaded.status_code == 200
            created = client.post("/v1/batches", headers=public, json={"input_file_id": uploaded.json()["id"], "endpoint": "/v1/chat/completions",
                "completion_window": "24h", "metadata": {"benchmark_run": "bench-observer-test"}})
            assert created.status_code == 200
            for worker in ("secondary-one", "primary-one"):
                assert client.post("/internal/workers/register", headers=internal,
                    json={"worker_id": worker, "name": worker, "model": model, "concurrency": 1}).status_code == 200
            secondary = client.post("/internal/tasks/claim", headers=internal, json={"worker_id": "secondary-one", "model": model, "limit": 1}).json()["tasks"][0]
            response = {"id": "chat-test", "object": "chat.completion", "model": model, "choices": [{"index": 0,
                "message": {"role": "assistant", "content": "public result"}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 7, "completion_tokens": 2, "total_tokens": 9}}
            assert client.post(f"/internal/tasks/{secondary['id']}/complete", headers=internal,
                json={"worker_id": "secondary-one", "attempt_token": secondary["attempt_token"], "response": response}).status_code == 200
            claimed = client.post("/internal/tasks/claim", headers=internal, json={"worker_id": "primary-one", "model": model, "limit": 1}).json()["tasks"][0]
            progress = Progress(dsn, "bench-observer-test", "secondary-")
            assert progress.submission_counts() == {"total": 3, "unfinished": 2}
            observed = progress.snapshot()
            assert (observed["total"], observed["pending"], observed["running"], observed["completed"], observed["secondary_completed"]) == (3, 1, 1, 1, 1)
            assert observed["task_prompt_tokens"] == observed["accepted_prompt_tokens"] == observed["batch_prompt_tokens"] == 7
            captured = progress.inflight("primary-")
            assert len(captured) == 1 and captured[0]["attempt_token"] == claimed["attempt_token"]
            assert client.post(f"/internal/tasks/{claimed['id']}/fail", headers=internal,
                json={"worker_id": "primary-one", "attempt_token": claimed["attempt_token"], "retryable": False,
                      "error": {"code": "permanent_model_error", "message": "public test failure"}}).status_code == 200
            # A normal failed attempt also gets stale-heartbeat 409. It must
            # never be mistaken for evidence of a lease-expiry transition.
            assert progress.expired(captured) == ([], {"failed": 1})
            claimed = client.post("/internal/tasks/claim", headers=internal, json={"worker_id": "primary-one", "model": model, "limit": 1}).json()["tasks"][0]
            captured = progress.inflight("primary-")
            with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
                progress.connection.execute("UPDATE workers SET name=name")
            with psycopg.connect(dsn, autocommit=True) as owner:
                owner.execute("UPDATE tasks SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=%s", (claimed["id"],))
            assert progress.inflight("primary-") == []  # Already stale before an outage is not valid evidence.
            client.portal.call(client.app.state.store.recover)
            expired, statuses = progress.expired(captured)
            assert expired == captured and statuses == {"expired": 1}
            assert progress.snapshot()["expired_attempts"] == 1
            assert client.post(f"/internal/tasks/{claimed['id']}/heartbeat", headers=internal,
                json={"worker_id": "primary-one", "attempt_token": claimed["attempt_token"]}).status_code == 409
            # The engine-loss observer must prove this exact captured task was
            # accepted by the exact other worker, not unrelated fleet progress.
            engine_observer = EngineProgress(dsn, "bench-observer-test", model, "secondary-one")
            wrong_worker = EngineProgress(dsn, "bench-observer-test", model, "secondary")
            wrong_model = EngineProgress(dsn, "bench-observer-test", "other-model", "secondary-one")
            try:
                engine_observer.scope(3)
                assert engine_observer.secondary_ready() and not wrong_worker.secondary_ready()
                with pytest.raises(ProbeFailure, match="scope_mismatch"):
                    wrong_model.scope(3)
                assert wrong_model.capture("primary-one") == []
                assert engine_observer.capture("primary-one") == []  # Already-expired leases cannot arm a new fault.
                evidence = engine_observer.recovery(captured)
                assert evidence["lease_expired"] == 1 and evidence["reclaimed_by_exact_secondary"] == 0
                with psycopg.connect(dsn, autocommit=True) as owner:
                    owner.execute("UPDATE tasks SET ready_at=clock_timestamp() WHERE id=%s", (claimed["id"],))
                    owner.execute("UPDATE tenant_queues SET next_ready_at=clock_timestamp()")
                reclaimed = client.post("/internal/tasks/claim", headers=internal,
                    json={"worker_id": "secondary-one", "model": model, "limit": 1}).json()["tasks"][0]
                assert reclaimed["id"] == claimed["id"] and reclaimed["attempt_token"] != claimed["attempt_token"]
                assert client.post(f"/internal/tasks/{reclaimed['id']}/complete", headers=internal,
                    json={"worker_id": "secondary-one", "attempt_token": reclaimed["attempt_token"], "response": response}).status_code == 200
                evidence = engine_observer.recovery(captured)
                assert evidence["reclaimed_by_exact_secondary"] == 1
                assert evidence["reclaimed_tasks"][0]["accepted_count"] == 1
                assert evidence["reclaimed_tasks"][0]["accepted_worker"] == "secondary-one"
                assert wrong_worker.recovery(captured)["reclaimed_by_exact_secondary"] == 0
                assert wrong_worker.snapshot()["secondary_completed"] == 0
                assert engine_observer.snapshot()["secondary_completed"] == 2
                assert "attempt_token" not in json.dumps(evidence)
            finally:
                engine_observer.close()
                wrong_worker.close()
                wrong_model.close()
            # Simulate an externally corrupted accepted-attempt ledger in this
            # disposable DB: the observer must detect it, not declare success.
            with psycopg.connect(dsn, autocommit=True) as owner:
                owner.execute("INSERT INTO attempts(token,task_id,batch_id,tenant_id,worker_id,number,status,prompt_tokens,completion_tokens,finished_at) SELECT 'corruption-test-token',task_id,batch_id,tenant_id,worker_id,999,'completed',prompt_tokens,completion_tokens,clock_timestamp() FROM attempts WHERE token=%s", (secondary["attempt_token"],))
            with pytest.raises(ProbeFailure, match="duplicate_or_missing_accepted_usage"):
                progress.snapshot()
    finally:
        if progress:
            progress.close()
        with psycopg.connect(test_dsn, autocommit=True) as owner:
            owner.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
