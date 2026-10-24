"""Observed response sizing is exact only for a validated integer JSON subset."""
import hashlib
import json
import os
from pathlib import Path
import secrets

import pytest

from scripts import audit_response_reservation as module


def response(identifier="audit-run-0001", *, text="private answer", completion=32):
    return {"custom_id": identifier, "error": None, "response": {"status_code": 200, "body": {
        "choices": [{"message": {"content": text}, "finish_reason": "length"}],
        "usage": {"prompt_tokens": 128, "completion_tokens": completion}}}}


def output(tmp_path, rows, name="cohort-output.jsonl"):
    root = tmp_path / ".runtime"
    root.mkdir(mode=0o700, exist_ok=True)
    path = root / name
    path.write_bytes(b"\n".join(json.dumps(row, ensure_ascii=False).encode() for row in rows) + b"\n")
    path.chmod(0o600)
    return path


def test_metadata_contains_sizes_hashes_groups_and_no_text_or_ids(tmp_path):
    one = response(text="回答😀\nquoted \"answer\"", completion=32)
    two = response("audit-run-0002", text="private response text", completion=2048)
    path = output(tmp_path, [one, two])
    report = module.audit([path], expected_count=2, run_id="audit-run", expected_output_caps={32, 2048})
    sizes = [module.canonical_bytes(row["response"]["body"]) for row in (one, two)]
    assert report["response_jsonb_text_bytes"]["max"] == max(sizes)
    assert report["response_jsonb_text_bytes"]["p50"] == sum(sizes) / 2
    assert report["candidate"]["all_observed_responses_fit"]
    assert report["candidate"]["minimum_headroom_bytes"] == 65536 - max(sizes)
    assert report["original"]["response_reservation_bytes_for_observed_count"] == 2 * 1048576
    assert report["reservation_comparison"]["response_reservation_reduction_fraction"] == .9375
    assert report["input_files"][0]["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    assert report["input_files"][0]["count"] == 2
    assert report["completion_token_groups"]["32"]["count"] == report["completion_token_groups"]["2048"]["count"] == 1
    serialized = json.dumps(report, ensure_ascii=False)
    for forbidden in ("回答", "private response text", "audit-run-0001", "audit-run-0002"):
        assert forbidden not in serialized
    assert report["policy_changed"] is False and report["database_read"] is False and report["http_requests"] is False


def test_uncovered_response_is_reported_without_policy_change(tmp_path):
    path = output(tmp_path, [response(text="x" * 70000)])
    report = module.audit([path], expected_count=1, run_id="audit-run")
    assert report["candidate"]["coverage_fraction"] == 0
    assert report["candidate"]["minimum_headroom_bytes"] < 0
    assert report["original"]["covered_count"] == 1
    assert report["policy_changed"] is False


@pytest.mark.parametrize("value", [1.0, float("nan"), float("inf"), {"nested": [1.1]}, {"bad": "\x00"}, "\ud800"])
def test_float_nonfinite_nul_and_surrogate_refuse_canonical_equivalence(value):
    with pytest.raises(module.AuditError): module.canonical_bytes(value)


def test_raw_float_extensions_and_duplicate_keys_are_refused(tmp_path):
    path = output(tmp_path, [response()])
    for raw, code in [(b'{"custom_id":"audit-run-1","value":1.0}', "unsupported_float_or_nonfinite"),
                      (b'{"custom_id":"audit-run-1","value":NaN}', "unsupported_float_or_nonfinite"),
                      (b'{"custom_id":"audit-run-1","value":1,"value":2}', "duplicate_json_key")]:
        path.write_bytes(raw + b"\n")
        with pytest.raises(module.AuditError) as error: module.audit([path], expected_count=1, run_id="audit-run")
        assert error.value.code == code


def test_unicode_line_and_paragraph_separators_are_inside_one_jsonl_record(tmp_path):
    path = output(tmp_path, [response(text="first\u2028second\u2029third")])
    assert module.audit([path], expected_count=1, run_id="audit-run")["observed_count"] == 1


@pytest.mark.parametrize("case", ["count", "prefix", "duplicate", "same_file", "cap"])
def test_cohort_identity_and_expected_usage_are_bound(tmp_path, case):
    rows = [response()]
    if case == "prefix": rows[0]["custom_id"] = "audit-runs-other-cohort"
    if case == "duplicate": rows.append(response())
    path = output(tmp_path, rows)
    paths = [path, path] if case == "same_file" else [path]
    with pytest.raises(module.AuditError):
        module.audit(paths, expected_count=2 if case in {"count", "duplicate", "same_file"} else 1,
                     run_id="audit-run", expected_output_caps={128} if case == "cap" else None)


def test_owner_only_regular_path_and_no_symlink_inputs(tmp_path):
    path = output(tmp_path, [response()])
    path.chmod(0o644)
    with pytest.raises(module.AuditError, match="private_regular"): module.audit([path], expected_count=1, run_id="audit-run")
    path.chmod(0o600)
    link = path.with_name("link-output.jsonl"); link.symlink_to(path)
    with pytest.raises(module.AuditError): module.audit([link], expected_count=1, run_id="audit-run")
    fifo = path.with_name("pipe-output.jsonl"); os.mkfifo(fifo, 0o600)
    with pytest.raises(module.AuditError): module.audit([fifo], expected_count=1, run_id="audit-run")
    outside = tmp_path / "outside-output.jsonl"; outside.write_bytes(path.read_bytes()); outside.chmod(0o600)
    with pytest.raises(module.AuditError, match="unsafe_input_path"): module.audit([outside], expected_count=1, run_id="audit-run")


def test_per_file_and_total_bounds_are_hard_limits(tmp_path, monkeypatch):
    first = output(tmp_path, [response()])
    size = first.stat().st_size
    monkeypatch.setattr(module, "MAX_FILE_BYTES", size - 1)
    with pytest.raises(module.AuditError, match="input_size_limit"): module.private_input(first)
    monkeypatch.setattr(module, "MAX_FILE_BYTES", 16 * 1024 * 1024)
    second = output(tmp_path, [response("audit-run-0002")], name="second-output.jsonl")
    monkeypatch.setattr(module, "MAX_TOTAL_BYTES", size)
    with pytest.raises(module.AuditError, match="total_input_size_limit"):
        module.audit([first, second], expected_count=2, run_id="audit-run")


def test_default_cli_preview_does_not_write_and_errors_never_echo_text(tmp_path, capsys):
    path = output(tmp_path, [response(text="answer must remain private")])
    before = set(path.parent.iterdir())
    argv = ["--input", str(path), "--expected-count", "1", "--run-id", "audit-run"]
    assert module.main(argv) == 0
    stdout = capsys.readouterr().out
    assert "answer must remain private" not in stdout
    assert set(path.parent.iterdir()) == before
    assert module.main([*argv, "--expected-output-caps", "private-invalid-value"]) == 1
    assert json.loads(capsys.readouterr().out) == {"ok": False, "error_code": "invalid_output_cap_set"}
    with pytest.raises(SystemExit): module.main(["--unknown-sensitive-value"])
    assert json.loads(capsys.readouterr().out) == {"ok": False, "error_code": "invalid_arguments"}


def test_report_is_new_exclusive_private_metadata_and_existing_bytes_survive(tmp_path):
    path = output(tmp_path, [response()])
    report = module.audit([path], expected_count=1, run_id="audit-run")
    destination = path.parent / "audit.json"
    module.write_report(destination, report)
    raw = destination.read_bytes()
    assert destination.stat().st_mode & 0o777 == 0o600
    assert b"private answer" not in raw
    with pytest.raises(module.AuditError, match="report_already_exists"): module.write_report(destination, report)
    assert destination.read_bytes() == raw
    path.parent.chmod(0o755)
    with pytest.raises(module.AuditError, match="report_parent_not_private"):
        module.write_report(path.parent / "unsafe.json", report)


@pytest.mark.parametrize("value", [
    {"Unicode": "中文、العربية、é、😀、\u2028、\u2029"},
    {"escape": "\"\\/\b\f\n\r\t\x01\x1f", "nested": [True, False, None, {"a": 1, "z": -7}]},
    {"z": 10 ** 120 + 7, "a": -(10 ** 300 + 11), "zero": 0},
    [0, -1, True, False, None, "text", {"long key": [], "a": {}}],
])
def test_actual_postgres_canonical_jsonb_byte_length_matches_integer_subset(value):
    source = os.environ.get("TEST_DATABASE_URL")
    if not source:
        pytest.skip("Requires an isolated disposable PostgreSQL cluster")
    import psycopg
    from psycopg import sql
    from psycopg.conninfo import make_conninfo
    name = "bf_response_audit_" + secrets.token_hex(6)
    with psycopg.connect(source, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(sql.Identifier(name)))
    try:
        dsn = make_conninfo(source, dbname=name)
        with psycopg.connect(dsn) as conn:
            # Use the exact installed migration definition, in an isolated DB;
            # no owner credential or production snapshot is read by the tool.
            migration = Path("backend/migrations/0009_retention_admission.sql").read_text()
            begin = migration.index("CREATE FUNCTION public.batchforge_json_bytes")
            end = migration.index("$$;", begin) + len("$$;")
            conn.execute(migration[begin:end])
            encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
            actual = conn.execute("SELECT batchforge_json_bytes(%s::jsonb)", (encoded,)).fetchone()[0]
            assert module.canonical_bytes(value) == actual
            # Key ordering changes PostgreSQL's text representation but not
            # the exact byte count for this accepted numeric/string subset.
            if isinstance(value, dict):
                reversed_value = dict(reversed(list(value.items())))
                assert module.canonical_bytes(reversed_value) == actual
    finally:
        with psycopg.connect(source, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP DATABASE {} WITH(FORCE)").format(sql.Identifier(name)))
