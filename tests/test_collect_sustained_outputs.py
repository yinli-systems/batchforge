import copy
import json
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from scripts import collect_sustained_outputs as module


def private(path, value):
    path.write_bytes(json.dumps(value).encode() + b"\n")
    path.chmod(0o600)


def cohort(tmp_path, *, partial=False):
    root = tmp_path / ".runtime"; root.mkdir(mode=0o700)
    state = root / "runner"; state.mkdir(mode=0o700)
    (state / "controller.lock").touch(mode=0o600)
    job = "collect-test"
    task = {"id": "task-one", "custom_id": job + "-00000000", "batch_id": "batch-one", "status": "completed",
        "accepted_attempts": 1, "prompt_tokens": 128, "completion_tokens": 32,
        "accepted_prompt_tokens": 128, "accepted_completion_tokens": 32}
    config = {"job_id": job, "tasks": 2 if partial else 1, "fixed_output": True}
    report = {"job_id": job, "configuration": config, "journal_sequence": 19, "passed": not partial,
        "accepted_tasks": 1, "completed_tasks": 1, "terminal_tasks": 1, "failed_tasks": 0,
        "cancelled_tasks": 0, "unfinished_tasks": 0, "duplicate_accepted_attempts": 0,
        "accepted_exactly_once": True, "accepted_usage_matches_task_usage": True,
        "fixed_output_lengths_exact": True, "tasks": [task]}
    source_sha = "a" * 64
    key = "bf1.1792800000." + module.digest(f"{job}:0:{source_sha}".encode())[:32]
    checkpoint = {"configuration": config, "sequence": 20, "events": [{"kind": "finished", "value": {"passed": not partial, "completed_tasks": 1}}],
        "batches": {"0": {"index": 0, "batch_id": "batch-one", "file_id": "input-one", "tasks": 1,
            "status": "completed", "idempotency_key": key,
            "counts": {"pending_count": 0, "running_count": 0, "completed_count": 1, "failed_count": 0, "cancelled_count": 0}}}}
    private(state / "checkpoint.json", checkpoint); private(state / "report.json", report)
    env = root / "access.env"; env.write_text("DATABASE_URL=not-used\nBATCHFORGE_API_KEY=private-test-key\n"); env.chmod(0o600)
    args = SimpleNamespace(run_id=job, checkpoint=state / "checkpoint.json", report=state / "report.json",
        checkpoint_sha256=module.digest((state / "checkpoint.json").read_bytes()), report_sha256=module.digest((state / "report.json").read_bytes()),
        expected_completed=1, require_full_plan=False, env_file=env, output_dir=root / "collected", url="http://127.0.0.1:8080")
    database = [{"id": "batch-one", "input_file_id": "input-one", "output_file_id": "output-one", "status": "completed",
        "total_count": 1, "pending_count": 0, "running_count": 0, "completed_count": 1, "failed_count": 0,
        "cancelled_count": 0, "prompt_tokens": 128, "completion_tokens": 32,
        "metadata": {"sustained_job": job, "sustained_index": "0", "sustained_input_sha256": source_sha}}]
    raw = json.dumps({"id": "task-one", "custom_id": task["custom_id"], "error": None, "response": {"status_code": 200,
        "body": {"choices": [{"message": {"content": "private answer\u2028not metadata"}}], "usage": {"prompt_tokens": 128, "completion_tokens": 32}}}}, ensure_ascii=False).encode() + b"\n"
    return args, database, raw


def run(args, database, raw, requests):
    def handler(request):
        requests.append(request)
        assert request.method == "GET"
        assert request.url.path == "/v1/files/output-one/content"
        return httpx.Response(200, content=raw)
    return module.collect(args, transport=httpx.MockTransport(handler), database_reader=lambda *values: database)


def test_complete_collection_is_private_metadata_only_and_resume_does_not_download_again(tmp_path):
    args, database, raw = cohort(tmp_path)
    requests = []
    state = run(args, database, raw, requests)
    final = args.output_dir / "batch-000000-output.jsonl"
    assert final.read_bytes() == raw and final.stat().st_mode & 0o777 == 0o600
    assert state["phase"] == "complete" and state["completed_count"] == 1
    assert "private answer" not in json.dumps(state)
    assert "private-test-key" not in json.dumps(state)
    assert state["files"]["0"]["sha256"] == module.digest(raw)
    assert len(requests) == 1
    assert run(args, database, raw, requests)["phase"] == "complete"
    assert len(requests) == 1 and len(list(args.output_dir.glob("*-output.jsonl"))) == 1


def test_partial_plan_preserves_original_failure_and_strict_option_refuses(tmp_path):
    args, database, raw = cohort(tmp_path, partial=True)
    state = run(args, database, raw, [])
    assert state["original_report_passed"] is False and state["partial_plan"] is True
    args.require_full_plan = True
    with pytest.raises(module.CollectionError, match="not_proven_terminal"):
        run(args, database, raw, [])


@pytest.mark.parametrize("changed", ["sha", "finished", "sequence", "usage", "active_controller"])
def test_unproven_source_or_active_runner_refuses_before_http(tmp_path, changed):
    args, database, raw = cohort(tmp_path)
    if changed == "active_controller":
        with module.lock_file(args.checkpoint.parent / "controller.lock"):
            with pytest.raises(module.CollectionError): run(args, database, raw, [])
        return
    if changed == "sha": args.report_sha256 = "0" * 64
    else:
        checkpoint = json.loads(args.checkpoint.read_bytes())
        if changed == "finished": checkpoint["events"][-1]["kind"] = "failed"
        elif changed == "sequence": checkpoint["sequence"] += 1
        else: checkpoint["batches"]["0"]["counts"]["running_count"] = 1
        private(args.checkpoint, checkpoint); args.checkpoint_sha256 = module.digest(args.checkpoint.read_bytes())
    requests = []
    with pytest.raises(module.CollectionError): run(args, database, raw, requests)
    assert requests == []


@pytest.mark.parametrize("changed", ["extra", "file", "index", "hash", "tokens"])
def test_database_identity_and_metering_must_match_bound_terminal_cohort(tmp_path, changed):
    args, database, raw = cohort(tmp_path)
    if changed == "extra": database.append({**database[0], "id": "unowned-batch"})
    elif changed == "file": database[0]["input_file_id"] = "wrong-input"
    elif changed == "index": database[0]["metadata"]["sustained_index"] = "1"
    elif changed == "hash": database[0]["metadata"]["sustained_input_sha256"] = "b" * 64
    else: database[0]["completion_tokens"] = 99
    requests = []
    with pytest.raises(module.CollectionError): run(args, database, raw, requests)
    assert requests == []


def test_bad_download_is_retained_and_next_attempt_resumes_without_duplicate_final(tmp_path):
    args, database, raw = cohort(tmp_path)
    with pytest.raises(module.CollectionError): run(args, database, b"private partial not JSONL", [])
    first = list(args.output_dir.glob("*.part"))
    assert len(first) == 1 and first[0].read_bytes() == b"private partial not JSONL"
    state = run(args, database, raw, [])
    assert state["phase"] == "complete" and len(list(args.output_dir.glob("*-output.jsonl"))) == 1
    assert first[0].exists() and len(state["files"]["0"]["attempts"]) == 1


def test_crash_after_link_before_final_marker_recovers_verified_existing_file(tmp_path):
    args, database, raw = cohort(tmp_path)
    state = run(args, database, raw, [])
    state["files"]["0"]["stage"] = "download_verified"
    module.atomic_state(args.output_dir / "collection.json", state)
    requests = []
    assert run(args, database, raw, requests)["phase"] == "complete" and requests == []


def test_existing_final_or_marker_symlinks_and_modified_outputs_fail_closed(tmp_path):
    args, database, raw = cohort(tmp_path)
    run(args, database, raw, [])
    final = args.output_dir / "batch-000000-output.jsonl"
    final.write_bytes(raw.replace(b"private answer", b"changed answer"))
    with pytest.raises(module.CollectionError, match="completed_output_changed"):
        run(args, database, raw, [])
    final.unlink(); final.symlink_to(args.env_file)
    with pytest.raises(module.CollectionError, match="unknown_collection_artifact"):
        run(args, database, raw, [])


@pytest.mark.parametrize("url", ["https://example.com", "http://127.0.0.1/path", "http://token@localhost", "http://localhost?key=secret"])
def test_only_local_tenant_get_endpoints_are_supported(url):
    with pytest.raises(module.CollectionError): module.local_url(url)


def test_literal_escaped_nul_is_valid_but_actual_nul_is_refused():
    assert module.strict_json(b'{"value":"\\\\u0000"}') == {"value": "\\u0000"}
    with pytest.raises(module.CollectionError): module.strict_json(b'{"value":"\\u0000"}')
    for raw in ('{"value":"text"}'.encode("utf-16"), b'\xef\xbb\xbf{"value":"text"}'):
        with pytest.raises(module.CollectionError): module.strict_json(raw)


def test_false_full_plan_result_is_not_misclassified_as_safe_partial_plan(tmp_path):
    args, _, _ = cohort(tmp_path)
    report = json.loads(args.report.read_bytes()); report["passed"] = False
    checkpoint = json.loads(args.checkpoint.read_bytes()); checkpoint["events"][-1]["value"]["passed"] = False
    private(args.report, report); private(args.checkpoint, checkpoint)
    args.report_sha256, args.checkpoint_sha256 = module.digest(args.report.read_bytes()), module.digest(args.checkpoint.read_bytes())
    with pytest.raises(module.CollectionError, match="cohort_not_proven_terminal"):
        module.terminal_binding(args)


def test_actual_runtime_role_database_query_is_read_only(tmp_path):
    import os
    source = os.environ.get("TEST_DATABASE_URL")
    if not source: pytest.skip("Requires isolated disposable PostgreSQL")
    import psycopg
    from psycopg import sql
    from psycopg.conninfo import make_conninfo
    from psycopg.types.json import Jsonb
    name = "bf_collector_" + module.secrets.token_hex(6)
    args, rows, _ = cohort(tmp_path)
    with psycopg.connect(source, autocommit=True) as conn:
        conn.execute("CREATE ROLE bf_runtime LOGIN")
        conn.execute(sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(sql.Identifier(name)))
    dsn = make_conninfo(source, dbname=name)
    try:
        with psycopg.connect(dsn) as conn:
            conn.execute(Path("backend/schema.sql").read_text())
            conn.execute("INSERT INTO tenants(id,name) VALUES('test','Test')")
            conn.execute("INSERT INTO api_keys(key_hash,tenant_id) VALUES(%s,'test')", (module.digest(b"private-test-key"),))
            conn.execute("INSERT INTO files(id,tenant_id,filename,kind) VALUES('input-one','test','input','input'),('output-one','test','output','output')")
            conn.execute("""INSERT INTO batches(id,tenant_id,input_file_id,output_file_id,model,status,total_count,completed_count,
                prompt_tokens,completion_tokens,metadata) VALUES('batch-one','test','input-one','output-one','model','completed',1,1,128,32,%s)""", (Jsonb(rows[0]["metadata"]),))
            conn.execute("GRANT USAGE ON SCHEMA public TO bf_runtime")
            conn.execute("GRANT SELECT ON api_keys,batches TO bf_runtime")
        result = module.database_batches(make_conninfo(dsn, user="bf_runtime"), "private-test-key", args.run_id)
        checkpoint, report, accepted = module.terminal_binding(args)
        assert module.checked_batches(result, accepted, report)[0]["file_id"] == "output-one"
        with pytest.raises(module.CollectionError, match="runtime_database_role_required"):
            module.database_batches(dsn, "private-test-key", args.run_id)
        with pytest.raises(module.CollectionError, match="tenant_binding_failed"):
            module.database_batches(make_conninfo(dsn, user="bf_runtime"), "wrong-key", args.run_id)
    finally:
        with psycopg.connect(source, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP DATABASE {} WITH(FORCE)").format(sql.Identifier(name)))
            conn.execute("DROP ROLE bf_runtime")
