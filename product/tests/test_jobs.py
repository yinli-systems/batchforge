import csv
import io
import json

import pytest

from product.jobs import InvalidJob, build_jsonl, parse_inputs, parse_items, parse_results, to_csv
from product.templates import TEMPLATES

MODEL = "Qwen/Qwen3-235B-A22B-Instruct-2507-FP8"


def test_parse_items_drops_blank_lines_and_strips():
    assert parse_items(["  one ", "", "   ", "two"]) == ["one", "two"]


def test_parse_items_rejects_empty_and_oversized_input():
    with pytest.raises(InvalidJob):
        parse_items(["", "  "])
    with pytest.raises(InvalidJob):
        parse_items(["x"] * 501)
    with pytest.raises(InvalidJob):
        parse_items(["x" * 8001])


def test_build_jsonl_emits_one_chat_request_per_item():
    lines = build_jsonl(TEMPLATES["sentiment"], ["good", "bad"], MODEL).decode().splitlines()
    first, second = (json.loads(line) for line in lines)
    assert first["custom_id"] == "item-0001" and second["custom_id"] == "item-0002"
    assert first["url"] == "/v1/chat/completions" and first["method"] == "POST"
    assert first["body"]["model"] == MODEL
    assert first["body"]["messages"][1] == {"role": "user", "content": "good"}
    assert "response_format" not in first["body"]


def test_extract_template_requests_json_object():
    body = json.loads(build_jsonl(TEMPLATES["extract"], ["Invoice 1"], MODEL))["body"]
    assert body["response_format"] == {"type": "json_object"}


def test_inputs_round_trip_through_stored_file():
    stored = build_jsonl(TEMPLATES["summary"], ["alpha", "beta"], MODEL)
    assert parse_inputs(stored) == [("item-0001", "alpha"), ("item-0002", "beta")]


def _result(cid, text, prompt=10, completion=3):
    return json.dumps({
        "id": "req_" + cid, "custom_id": cid,
        "response": {"status_code": 200, "request_id": "req_" + cid, "body": {
            "choices": [{"message": {"content": text}}],
            "usage": {"prompt_tokens": prompt, "completion_tokens": completion},
        }},
        "error": None,
    })


def _error(cid, message):
    return json.dumps({"id": "req_" + cid, "custom_id": cid, "response": None,
                       "error": {"code": "engine_http_500", "message": message}})


def test_results_are_ordered_by_input_and_missing_rows_are_pending():
    inputs = [("item-0001", "a"), ("item-0002", "b"), ("item-0003", "c")]
    output = (_result("item-0003", "negative") + "\n" + _result("item-0001", "positive") + "\n").encode()
    errors = (_error("item-0002", "boom") + "\n").encode()
    rows = parse_results(TEMPLATES["sentiment"], output, errors, inputs)
    assert [r["input"] for r in rows] == ["a", "b", "c"]
    assert rows[0]["status"] == "completed" and rows[0]["text"] == "positive"
    assert rows[1]["status"] == "failed" and rows[1]["error"] == "boom"
    assert rows[2]["text"] == "negative"


def test_missing_result_is_reported_as_pending():
    rows = parse_results(TEMPLATES["sentiment"], b"", b"", [("item-0001", "a")])
    assert rows[0]["status"] == "pending"


def test_invalid_json_output_is_flagged_for_extract_template():
    output = (_result("item-0001", "not json") + "\n" + _result("item-0002", '{"title": "X"}') + "\n").encode()
    rows = parse_results(TEMPLATES["extract"], output, b"",
                         [("item-0001", "a"), ("item-0002", "b")])
    assert rows[0]["status"] == "invalid_json"
    assert rows[1]["status"] == "completed"


def test_csv_expands_extract_fields_and_keeps_failed_rows():
    inputs = [("item-0001", "Invoice 4471"), ("item-0002", "broken")]
    output = (_result("item-0001", '{"title": "Invoice", "date": null, "amount": "$1,250"}') + "\n").encode()
    errors = (_error("item-0002", "engine down") + "\n").encode()
    rows = parse_results(TEMPLATES["extract"], output, errors, inputs)
    parsed = list(csv.DictReader(io.StringIO(to_csv(TEMPLATES["extract"], rows))))
    assert parsed[0]["title"] == "Invoice" and parsed[0]["amount"] == "$1,250"
    assert parsed[1]["status"] == "failed" and parsed[1]["error"] == "engine down"
