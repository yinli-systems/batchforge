"""Arrival load must remain bounded and recover the same durable identities."""
import asyncio
from decimal import Decimal
import json
from pathlib import Path
import time
import os
import secrets

import httpx
import pytest

from scripts import sustained_benchmark as soak


def configuration(**changes):
    value = dict(job_id="soak-test", seed=20261024, tasks=40, tasks_per_batch=20,
                 arrival_rate=.4, hourly_cost=16.1, fixed_output=True)
    value.update(changes)
    return value


def fixture_rows():
    return [{"custom_id": f"fixture-{index}", "method": "POST", "url": "/v1/chat/completions",
             "body": {"model": "test-model", "messages": [{"role": "user", "content": "private prompt"}],
                      "max_tokens": maximum, "fixture_index": index}}
            for index, maximum in enumerate([32, 128, 512, 2048] * 5)]


def test_balanced_seeded_plan_and_exact_output_do_not_mutate_source():
    rows = fixture_rows()
    plan = soak.make_plan(configuration(), rows)
    assert [row["arrival_offset_seconds"] for row in plan] == [0, 50]
    for item in plan:
        submitted = [json.loads(line) for line in item["data"].splitlines()]
        assert sorted(row["body"]["fixture_index"] for row in submitted) == list(range(20))
        assert all(row["body"]["min_tokens"] == row["body"]["max_tokens"] for row in submitted)
        assert all(row["custom_id"].startswith("soak-test-") for row in submitted)
    assert plan == soak.make_plan(configuration(), rows)
    assert plan != soak.make_plan(configuration(seed=4), rows)
    assert all("min_tokens" not in row["body"] for row in rows)


def test_journal_exclusive_replay_config_binding_and_private_modes(tmp_path):
    root = tmp_path / "state"
    journal = soak.Journal(root, configuration())
    journal.commit("batch", index=0, value={"file_id": "file-safe", "stage": "uploaded"})
    with pytest.raises(soak.SafetyError, match="live controller"):
        soak.Journal(root, configuration())
    assert (root.stat().st_mode & 0o777) == 0o700
    assert ((root / "journal.jsonl").stat().st_mode & 0o777) == 0o600
    journal.close()
    # Checkpoint is not trusted as authoritative; durable journal wins.
    (root / "checkpoint.json").write_text('{"sequence": 0}')
    resumed = soak.Journal(root, configuration())
    assert resumed.state["batches"]["0"]["file_id"] == "file-safe"
    resumed.close()
    with pytest.raises(soak.SafetyError, match="identical"):
        soak.Journal(root, configuration(seed=9))


def test_torn_journal_is_preserved_and_fails_closed(tmp_path):
    journal = soak.Journal(tmp_path, configuration())
    journal.close()
    with (tmp_path / "journal.jsonl").open("ab") as handle:
        handle.write(b'{"sequence":')
    before = (tmp_path / "journal.jsonl").read_bytes()
    with pytest.raises(soak.SafetyError, match="Torn journal"):
        soak.Journal(tmp_path, configuration())
    assert (tmp_path / "journal.jsonl").read_bytes() == before


def test_decimal_telemetry_serializes_exact_counts_and_rejects_nonfinite():
    assert json.loads(soak.render({"sum": Decimal("12345678901234567890"), "fraction": Decimal("1.5")})) == {
        "sum": 12345678901234567890, "fraction": 1.5}
    with pytest.raises(soak.SafetyError):
        soak.render(float("nan"))


def test_atomic_checkpoint_recovers_with_stale_crash_temporary_and_preserves_it(tmp_path):
    stale = tmp_path / "checkpoint.json.tmp"
    stale.write_text("partial pre-crash checkpoint")
    target = tmp_path / "checkpoint.json"
    soak.atomic_json(target, {"sequence": 4})
    soak.atomic_json(target, {"sequence": 5})
    assert json.loads(target.read_text()) == {"sequence": 5}
    assert stale.read_text() == "partial pre-crash checkpoint"
    assert target.stat().st_mode & 0o777 == 0o600


def test_scheduled_latency_includes_admission_wait_and_sampling_gaps(tmp_path):
    config = configuration(tasks=1, tasks_per_batch=1, fixed_output=False, sample_seconds=10)
    journal = soak.Journal(tmp_path, config)
    start = journal.state["started_epoch"]
    at = lambda offset: soak.datetime.fromtimestamp(start + offset, soak.timezone.utc).isoformat()
    journal.commit("batch", index=0, value={"batch_id": "batch-one", "tasks": 1, "index": 0, "created_at": at(290)})
    task = {"custom_id": "soak-test-00000000", "status": "completed", "accepted_attempts": 1,
        "total_attempt_records": 1, "prompt_tokens": 128, "completion_tokens": 32,
        "accepted_prompt_tokens": 128, "accepted_completion_tokens": 32,
        "completion_seconds": 10, "first_queue_seconds": 3, "finished_at": at(300)}
    report = soak.summarize(journal, [task], [{"at": at(offset)} for offset in (0, 10, 100, 110)], 120)
    assert report["task_completion_seconds"]["p95"] == 10
    assert report["scheduled_arrival_to_finished_seconds"]["p95"] == 300
    assert report["scheduled_arrival_to_durable_admission_seconds"]["p95"] == 290
    assert report["sampling_coverage"]["maximum_sample_or_boundary_gap_seconds"] == 90
    assert report["sampling_coverage"]["continuous_sampling_segment_span_seconds"] == 20
    assert report["sampling_coverage"]["wall_seconds_outside_sampling_segments"] == 100
    journal.close()


def test_backoff_is_deterministic_finite_and_server_hint_bounded():
    assert soak.backoff_seconds(5, 7) == soak.backoff_seconds(5, 7)
    assert 0 <= soak.backoff_seconds(100000, 7) <= 30
    assert soak.backoff_seconds(1, 7, retry_after="99999999") == 30
    assert 0 <= soak.backoff_seconds(1, 7, retry_after="nan") <= 30
    assert 0 <= soak.backoff_seconds(1, 7, retry_after="untrusted text") <= 30


def batch_value():
    return {"id": "batch-real", "status": "in_progress", "request_counts":
        {"total": 20, "pending": 20, "running": 0, "completed": 0, "failed": 0, "cancelled": 0}}


@pytest.mark.asyncio
async def test_429_and_uncertain_create_reuse_one_file_and_same_idempotency_key(tmp_path, monkeypatch):
    journal = soak.Journal(tmp_path, configuration())
    item = soak.make_plan(configuration(), fixture_rows())[0]
    requests = []
    creates = 0

    async def handler(request):
        nonlocal creates
        requests.append(request)
        if request.url.path == "/v1/files":
            return httpx.Response(200, json={"id": "file-one"})
        creates += 1
        if creates == 1:
            return httpx.Response(429, headers={"Retry-After": "0"}, json={"error": "secret remote diagnostic"})
        if creates == 2:
            raise httpx.ReadTimeout("secret network detail", request=request)
        return httpx.Response(200, json=batch_value())

    monkeypatch.setattr(soak, "backoff_seconds", lambda *args, **kwargs: 0)
    async with httpx.AsyncClient(base_url="http://127.0.0.1:8080", transport=httpx.MockTransport(handler)) as client:
        await soak.submit(client, journal, item, configuration(), time.time() + 10)
    assert len([r for r in requests if r.url.path == "/v1/files"]) == 1
    creates = [r for r in requests if r.url.path == "/v1/batches"]
    assert len(creates) == 3
    assert len({r.headers["Idempotency-Key"] for r in creates}) == 1
    assert len({r.content for r in creates}) == 1
    assert creates[0].headers["Idempotency-Key"].startswith("bf1.")
    state = journal.state["batches"]["0"]
    assert state["create_429"] == 1 and state["uncertain_create_responses"] == 1
    assert state["batch_id"] == "batch-real"
    assert b"secret" not in (tmp_path / "journal.jsonl").read_bytes()
    assert b"private prompt" not in (tmp_path / "checkpoint.json").read_bytes()
    journal.close()


@pytest.mark.asyncio
async def test_upload_uncertainty_is_not_automatically_retried_on_resume(tmp_path):
    journal = soak.Journal(tmp_path, configuration())
    item = soak.make_plan(configuration(), fixture_rows())[0]
    requests = []

    async def handler(request):
        requests.append(request)
        raise httpx.ReadTimeout("private diagnostic", request=request)

    async with httpx.AsyncClient(base_url="http://127.0.0.1:8080", transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(soak.SafetyError, match="uncertain"):
            await soak.submit(client, journal, item, configuration(), time.time() + 10)
        with pytest.raises(soak.SafetyError, match="uncertain"):
            await soak.submit(client, journal, item, configuration(), time.time() + 10)
    assert len(requests) == 1
    journal.close()


def server_row(plan, index=0):
    return {"id": "batch-real", "input_file_id": "file-one", "status": "completed", "total_count": 20,
            "metadata": {"sustained_index": str(index), "sustained_input_sha256": plan[index]["input_sha256"]},
            "created_at": "2026-10-24T00:00:00Z", "finished_at": "2026-10-24T00:01:00Z",
            "pending_count": 0, "running_count": 0, "completed_count": 20, "failed_count": 0, "cancelled_count": 0}


def test_reconcile_recovers_unacknowledged_batch_and_rejects_duplicates(tmp_path):
    journal = soak.Journal(tmp_path, configuration())
    plan = soak.make_plan(configuration(), fixture_rows())
    row = server_row(plan)
    soak.reconcile(journal, plan, [row])
    assert journal.state["batches"]["0"]["batch_id"] == "batch-real"
    with pytest.raises(soak.SafetyError, match="Duplicate"):
        soak.reconcile(journal, plan, [row, {**row, "id": "batch-duplicate"}])
    with pytest.raises(soak.SafetyError, match="missing"):
        soak.reconcile(journal, plan, [])
    with pytest.raises(soak.SafetyError, match="identity"):
        soak.reconcile(journal, plan, [{**row, "input_file_id": "file-other"}])
    journal.close()


def test_create_finishing_during_db_snapshot_is_reserved_once_not_declared_missing(tmp_path):
    journal = soak.Journal(tmp_path, configuration(tasks=60))
    plan = soak.make_plan(configuration(tasks=60), fixture_rows())
    visible = server_row(plan)
    visible.update(status="in_progress", pending_count=20, completed_count=0)
    # Batch1 was acknowledged after a query began; batch2 is still creating.
    journal.commit("batch", index=1, value={"batch_id": "batch-newer-than-snapshot", "tasks": 20})
    soak.reconcile(journal, plan, [visible], known_before_query=set())
    active, reserved = soak.inflight_reservations(journal, plan, [visible], {1: object(), 2: object()})
    assert active == 20 and reserved == 40
    with pytest.raises(soak.SafetyError, match="missing"):
        soak.reconcile(journal, plan, [visible], known_before_query={"batch-newer-than-snapshot"})
    journal.close()


def test_summary_distinguishes_retries_duplicate_acceptance_and_partial_plan(tmp_path):
    config = configuration(tasks=1, tasks_per_batch=1)
    journal = soak.Journal(tmp_path, config)
    journal.commit("batch", index=0, value={"batch_id": "batch-real", "tasks": 1})
    plan = soak.make_plan(config, fixture_rows())
    task = {"id": "task-one", "custom_id": "soak-test-00000000", "status": "completed", "accepted_attempts": 1,
        "total_attempt_records": 2, "prompt_tokens": 128, "completion_tokens": plan[0]["output_caps"][0],
        "accepted_prompt_tokens": 128, "accepted_completion_tokens": plan[0]["output_caps"][0],
        "completion_seconds": 10, "first_queue_seconds": 3}
    report = soak.summarize(journal, [task], [], 3600, plan=plan)
    assert report["passed"] is True
    assert report["tasks_with_retries"] == 1 and report["duplicate_accepted_attempts"] == 0
    assert report["task_completion_seconds"]["p99"] == 10
    assert report["cost"]["allocation_usd"] == 16.1
    assert report["fixed_output_lengths_exact"] is True
    duplicate = soak.summarize(journal, [{**task, "accepted_attempts": 2}], [], 3600, plan=plan)
    assert duplicate["passed"] is False and duplicate["duplicate_accepted_attempts"] == 1
    wrong_usage = soak.summarize(journal, [{**task, "accepted_prompt_tokens": 999}], [], 3600, plan=plan)
    assert wrong_usage["passed"] is False
    journal.close()


def test_engine_stage_metrics_exclude_labels_and_unrelated_series():
    text = '''vllm:num_requests_running{model_name="private-model"} 7
vllm:prompt_tokens_total{model_name="private-model"} 1000
vllm:generation_tokens_total{model_name="private-model"} 200
private_api_key{value="secret"} 1
'''
    result = soak.engine_metrics(text)
    assert result == {"vllm:num_requests_running": 7, "vllm:prompt_tokens_total": 1000, "vllm:generation_tokens_total": 200}
    assert "private" not in json.dumps(result)


@pytest.mark.parametrize("url", ["https://example.com", "http://127.0.0.1/path", "http://token@localhost", "http://localhost?secret=x"])
def test_endpoints_cannot_send_credentials_to_remote_hosts(url):
    with pytest.raises(soak.SafetyError):
        soak.local_url(url)


def test_nonfinite_and_unsafe_state_bounds_are_rejected(tmp_path):
    args = soak.parser().parse_args(["--job-id", "bounded", "--env-file", "private.env", "--input-jsonl", "input.jsonl",
                                    "--model", "test", "--hourly-cost", "16.1"])
    args.arrival_rate = float("nan")
    with pytest.raises(soak.SafetyError):
        soak.validate_args(args)
    args.arrival_rate = .4
    args.state_dir = tmp_path / "unsafe-unignored-state"
    with pytest.raises(soak.SafetyError, match=".runtime"):
        soak.validate_args(args)


@pytest.mark.asyncio
async def test_complete_runner_respects_global_inflight_limit_and_restarts_same_job(tmp_path, monkeypatch):
    root = tmp_path / ".runtime"
    root.mkdir(mode=0o700)
    fixture = root / "input.jsonl"
    fixture.write_bytes(b"\n".join(soak.render(row) for row in fixture_rows()) + b"\n")
    env = root / "private.env"
    env.write_text("DATABASE_URL=unused-private-dsn\nBATCHFORGE_API_KEY=test-not-a-secret\n")
    env.chmod(0o600)
    args = soak.validate_args(soak.parser().parse_args(["--job-id", "loop-check", "--env-file", str(env),
        "--input-jsonl", str(fixture), "--model", "test-model", "--hourly-cost", "16.1", "--tasks", "4",
        "--tasks-per-batch", "1", "--arrival-rate", "1000", "--duration-seconds", "2", "--drain-seconds", "1",
        "--max-inflight-tasks", "2", "--poll-seconds", ".1", "--fixed-output", "--state-dir", str(root / "state")]))
    plan = soak.make_plan({**configuration(tasks=4, tasks_per_batch=1, job_id="loop-check"), "seed": args.seed}, fixture_rows())
    server = {}
    active_peak = 0
    posts = 0

    class FakeDatabase:
        def batches(self):
            result = []
            for row in server.values():
                row["polls"] += 1
                if row["polls"] >= 2:
                    row.update(status="completed", pending_count=0, completed_count=1, finished_at=soak.timestamp())
                result.append({key: value for key, value in row.items() if key != "polls"})
            return result

        def sample(self):
            return {"retention": {"retained_tasks": len(server)}, "queue": {"pending": 0}, "cleanup": {}}

        def tasks(self):
            return [{"id": f"task-{index}", "custom_id": f"loop-check-{index:08d}", "batch_id": row["id"],
                "status": "completed", "attempts": 1, "prompt_tokens": 128, "completion_tokens": plan[index]["output_caps"][0],
                "accepted_prompt_tokens": 128, "accepted_completion_tokens": plan[index]["output_caps"][0],
                "accepted_attempts": 1, "total_attempt_records": 1, "completion_seconds": .1, "first_queue_seconds": .01,
                "finished_at": row["finished_at"]} for index, row in server.items()]

        def close(self):
            pass

    async def handler(request):
        nonlocal active_peak, posts
        if request.url.path == "/metrics":
            return httpx.Response(200, text="vllm:num_requests_running 2\n")
        posts += 1
        if request.url.path == "/v1/files":
            return httpx.Response(200, json={"id": f"file-{posts}"})
        body = json.loads(request.content)
        index = int(body["metadata"]["sustained_index"])
        assert index not in server
        row = server_row(plan, index)
        row.update(id=f"batch-{index}", input_file_id=body["input_file_id"], status="in_progress", total_count=1,
                   pending_count=1, completed_count=0, polls=0, metadata=body["metadata"], finished_at=None)
        server[index] = row
        active_peak = max(active_peak, sum(row["pending_count"] for row in server.values()))
        value = batch_value()
        value.update(id=f"batch-{index}")
        value["request_counts"].update(total=1, pending=1)
        return httpx.Response(200, json=value)

    monkeypatch.setattr(soak.Sampler, "gpu", lambda: {"devices": [{"index": str(index), "utilization_percent": 80} for index in range(4)]})
    report = await soak.run(args, transport=httpx.MockTransport(handler), database=FakeDatabase())
    assert report["passed"] and report["accepted_tasks"] == report["completed_tasks"] == 4
    assert active_peak <= 2
    count_before = posts
    resumed = await soak.run(args, transport=httpx.MockTransport(handler), database=FakeDatabase())
    assert resumed["passed"] and posts == count_before
    assert len(list((root / "state").glob("report-*.json"))) == 2


def test_actual_schema10_readonly_queries_and_no_mutations():
    """A disposable database catches SQL/schema drift; never production tables."""
    source = os.environ.get("TEST_DATABASE_URL")
    if not source:
        pytest.skip("Set TEST_DATABASE_URL to a disposable PostgreSQL cluster")
    import hashlib
    import psycopg
    from psycopg import sql
    from psycopg.conninfo import make_conninfo
    from psycopg.types.json import Jsonb

    name = "bf_sustained_" + secrets.token_hex(8)
    with psycopg.connect(source, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(sql.Identifier(name)))
    dsn = make_conninfo(source, dbname=name)
    key = "sustained-isolated-not-a-real-secret"
    observer = None
    try:
        with psycopg.connect(dsn) as conn:
            conn.execute(Path("backend/schema.sql").read_text())
            for version in range(2, 11):
                conn.execute(next(Path("backend/migrations").glob(f"{version:04d}_*.sql")).read_text())
            conn.execute("INSERT INTO tenants(id,name) VALUES('soak','Isolated test')")
            conn.execute("INSERT INTO api_keys(key_hash,tenant_id) VALUES(%s,'soak')", (hashlib.sha256(key.encode()).hexdigest(),))
            conn.execute("SELECT batchforge_auxiliary_lock('soak')")
            conn.execute("INSERT INTO files(id,tenant_id,filename,kind) VALUES('file-test','soak','input','input')")
            conn.execute("""INSERT INTO batches(id,tenant_id,input_file_id,model,status,total_count,pending_count,metadata)
                VALUES('batch-test','soak','file-test','test-model','in_progress',1,1,%s)""",
                (Jsonb({"sustained_job": "read-query-test", "sustained_index": "0", "sustained_input_sha256": "0" * 64}),))
            conn.execute("""INSERT INTO tasks(id,tenant_id,batch_id,custom_id,model,body,status,max_attempts)
                VALUES('task-test','soak','batch-test','read-query-test-00000000','test-model','{}','pending',3)""")
        observer = soak.Database(dsn, key, "read-query-test")
        assert observer.batches()[0]["total_count"] == 1
        assert observer.sample()["retention"]["retained_tasks"] == 1
        assert observer.tasks()[0]["accepted_attempts"] == 0
        with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
            observer.conn.execute("UPDATE tasks SET status='cancelled'")
        assert observer.tasks()[0]["status"] == "pending"
    finally:
        if observer:
            observer.close()
        with psycopg.connect(source, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP DATABASE {} WITH(FORCE)").format(sql.Identifier(name)))
