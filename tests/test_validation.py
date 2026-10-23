import json

import pytest

from backend.config import Settings
from backend.validation import InvalidInput, body_hash, validate_rows, validate_usage


def row(**changes):
    body = {"model": "model", "messages": [{"role": "user", "content": "hello"}]}
    body.update(changes)
    return {"custom_id": "one", "method": "POST", "url": "/v1/chat/completions", "body": body}


def encoded(*rows):
    return b"\n".join(json.dumps(value).encode() for value in rows)


def test_validation_bounds_and_explicit_default():
    settings = Settings(model_name="model", max_output_tokens=512)
    assert validate_rows(encoded(row()), settings)[0]["body"]["max_tokens"] == 256
    for data in (encoded(row(max_tokens=513)), encoded(row(max_tokens=True)), encoded(row(stream=True)), encoded(row(), row()), b"\xff", b"[]", encoded(row(tools=[]))):
        with pytest.raises(InvalidInput):
            validate_rows(data, settings)


def test_usage_is_required_and_boolean_is_not_token_count():
    with pytest.raises(InvalidInput):
        validate_usage({"choices": [{}]})
    with pytest.raises(InvalidInput):
        validate_usage({"choices": [{}], "usage": {"prompt_tokens": True, "completion_tokens": 1}})
    assert validate_usage({"choices": [{}], "usage": {"prompt_tokens": 2, "completion_tokens": 3}}) == (2, 3)


def test_idempotency_hash_is_canonical():
    assert body_hash({"a": 1, "b": 2}) == body_hash({"b": 2, "a": 1})
    assert body_hash({"a": 1}) != body_hash({"a": 2})


def test_nonfinite_surrogates_and_nesting_fail_before_database():
    settings = Settings(model_name="model")
    for content in (b'{"custom_id":"x","x":NaN}', b'{"x":Infinity}', b'{"x":"\\u0000"}', b'{"x":-Infinity}', b'{"x":"\\ud800"}', b'[' * 1100 + b'0' + b']' * 1100):
        with pytest.raises(InvalidInput):
            validate_rows(content, settings)
    for bad_options in ({"stop": {"invalid": True}}, {"stop": [""]}, {"seed": True}, {"response_format": {"type": "json_schema"}}, {"n": True}):
        with pytest.raises(InvalidInput):
            validate_rows(encoded(row(**bad_options)), settings)
