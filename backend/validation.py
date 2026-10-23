"""Pure validation: no model execution and no tokenizer-dependent estimates."""
import hashlib
import json
from typing import Any


class InvalidInput(ValueError):
    pass


def reject_constant(value):
    raise ValueError(f"Nonfinite JSON number {value} is not supported")


def reject_nul(value):
    if isinstance(value, str):
        if "\x00" in value:
            raise ValueError("NUL characters cannot be stored in PostgreSQL JSONB")
    elif isinstance(value, dict):
        for key, child in value.items():
            reject_nul(key)
            reject_nul(child)
    elif isinstance(value, list):
        for child in value:
            reject_nul(child)


def strict_json(data: str | bytes):
    """Reject JSON extensions PostgreSQL jsonb cannot represent safely."""
    try:
        value = json.loads(data, parse_constant=reject_constant)
        reject_nul(value)
        json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8", errors="strict")
        return value
    except (ValueError, TypeError, UnicodeError, RecursionError) as exc:
        raise InvalidInput("Expected valid UTF-8 JSON without nonfinite numbers, lone surrogates, or excessive nesting") from exc


def body_hash(body: Any) -> str:
    return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()


def validate_rows(data: bytes, settings) -> list[dict]:
    if not data or len(data) > settings.max_upload_bytes:
        raise InvalidInput("File must be nonempty and within MAX_UPLOAD_BYTES")
    try:
        lines = data.decode("utf-8").splitlines()
    except UnicodeDecodeError as exc:
        raise InvalidInput("File must be UTF-8 JSONL") from exc
    if len(lines) > settings.max_tasks_per_batch:
        raise InvalidInput("Too many rows in input file")
    result, ids = [], set()
    for number, line in enumerate(lines, 1):
        if not line.strip():
            raise InvalidInput(f"Line {number}: empty JSONL rows are not allowed")
        if len(line.encode()) > settings.max_request_bytes:
            raise InvalidInput(f"Line {number}: request exceeds MAX_REQUEST_BYTES")
        try:
            row = strict_json(line)
        except (ValueError, TypeError, RecursionError) as exc:
            raise InvalidInput(f"Line {number}: invalid JSON") from exc
        if not isinstance(row, dict):
            raise InvalidInput(f"Line {number}: expected JSON object")
        custom_id = row.get("custom_id")
        if not isinstance(custom_id, str) or not 1 <= len(custom_id) <= 256 or custom_id in ids:
            raise InvalidInput(f"Line {number}: custom_id must be unique, nonempty, at most 256 characters")
        ids.add(custom_id)
        if row.get("method") != "POST" or row.get("url") != "/v1/chat/completions":
            raise InvalidInput(f"Line {number}: only POST /v1/chat/completions is supported")
        body = row.get("body")
        if not isinstance(body, dict) or body.get("model") not in settings.served_models():
            raise InvalidInput(f"Line {number}: model is not in the configured model catalog")
        if result and body["model"] != result[0]["body"]["model"]:
            raise InvalidInput(f"Line {number}: every request in a batch must use the same model")
        messages = body.get("messages")
        if not isinstance(messages, list) or not 1 <= len(messages) <= 128:
            raise InvalidInput(f"Line {number}: messages must contain 1 to 128 messages")
        for message in messages:
            if not isinstance(message, dict) or message.get("role") not in {"system", "user", "assistant"} or not isinstance(message.get("content"), str):
                raise InvalidInput(f"Line {number}: only text system/user/assistant messages are supported")
        if body.get("stream", False) is not False:
            raise InvalidInput(f"Line {number}: streaming is not supported for batch requests")
        if type(body.get("n", 1)) is not int or body.get("n", 1) != 1:
            raise InvalidInput(f"Line {number}: n must be 1")
        if "max_completion_tokens" in body and "max_tokens" in body:
            raise InvalidInput(f"Line {number}: use one output token limit")
        tokens = body.get("max_tokens", body.get("max_completion_tokens", min(256, settings.max_output_tokens)))
        if type(tokens) is not int or not 1 <= tokens <= settings.max_output_tokens:
            raise InvalidInput(f"Line {number}: output tokens must be 1 to {settings.max_output_tokens}")
        allowed = {"model", "messages", "max_tokens", "max_completion_tokens", "temperature", "top_p", "seed", "stop", "response_format", "stream", "n", "reasoning_effort"}
        unknown = set(body) - allowed
        if unknown:
            raise InvalidInput(f"Line {number}: unsupported request fields: {', '.join(sorted(unknown))}")
        if "reasoning_effort" in body and (not isinstance(body["reasoning_effort"], str) or body["reasoning_effort"] not in {"low", "medium", "high"}):
            raise InvalidInput(f"Line {number}: reasoning_effort must be low, medium, or high")
        if "temperature" in body and (type(body["temperature"]) not in (int, float) or not 0 <= body["temperature"] <= 2):
            raise InvalidInput(f"Line {number}: temperature must be 0 to 2")
        if "top_p" in body and (type(body["top_p"]) not in (int, float) or not 0 < body["top_p"] <= 1):
            raise InvalidInput(f"Line {number}: top_p must be greater than 0 and at most 1")
        if "seed" in body and (type(body["seed"]) is not int or not -(2**63) <= body["seed"] < 2**63):
            raise InvalidInput(f"Line {number}: seed must be a signed 64-bit integer")
        if "stop" in body and body["stop"] is not None:
            stops = [body["stop"]] if isinstance(body["stop"], str) else body["stop"]
            if not isinstance(stops, list) or not 1 <= len(stops) <= 4 or any(not isinstance(value, str) or not 1 <= len(value) <= 256 for value in stops):
                raise InvalidInput(f"Line {number}: stop must be a string or 1 to 4 nonempty strings of at most 256 characters")
        if "response_format" in body and body["response_format"] not in ({"type": "text"}, {"type": "json_object"}):
            raise InvalidInput(f"Line {number}: response_format supports only type=text or type=json_object")
        body = dict(body)
        body.setdefault("max_tokens", tokens) if "max_completion_tokens" not in body else None
        result.append({"custom_id": custom_id, "body": body})
    if not result:
        raise InvalidInput("At least one task is required")
    return result


def validate_usage(response: dict) -> tuple[int, int]:
    try:
        reject_nul(response)
        json.dumps(response, ensure_ascii=False, allow_nan=False).encode("utf-8", errors="strict")
    except (ValueError, TypeError, UnicodeError, RecursionError) as exc:
        raise InvalidInput("Response must contain valid finite UTF-8 JSON") from exc
    usage = response.get("usage")
    if not isinstance(usage, dict):
        raise InvalidInput("response.usage is required for transactional token metering")
    prompt, completion = usage.get("prompt_tokens"), usage.get("completion_tokens")
    if any(type(value) is not int or value < 0 or value > 1_000_000_000 for value in (prompt, completion)):
        raise InvalidInput("usage token counts must be nonnegative bounded integers")
    choices = response.get("choices")
    if not isinstance(choices, list) or not choices:
        raise InvalidInput("response.choices must be nonempty")
    return prompt, completion
