"""Pure request/result conversion between product jobs and BatchForge JSONL.

No network and no framework imports here, so every rule is unit-testable.
"""
from __future__ import annotations

import csv
import io
import json
from typing import Iterable

from .templates import EXTRACT_FIELDS, Template

MAX_ITEMS = 500
MAX_ITEM_CHARS = 8000


class InvalidJob(ValueError):
    pass


def parse_items(raw: Iterable[str]) -> list[str]:
    """Each non-blank line is one item. Blank lines are dropped, not errors."""
    items = [line.strip() for line in raw if line and line.strip()]
    if not items:
        raise InvalidJob("Provide at least one item")
    if len(items) > MAX_ITEMS:
        raise InvalidJob(f"A job takes at most {MAX_ITEMS} items")
    for number, item in enumerate(items, 1):
        if len(item) > MAX_ITEM_CHARS:
            raise InvalidJob(f"Item {number} exceeds {MAX_ITEM_CHARS} characters")
    return items


def custom_id(index: int) -> str:
    return f"item-{index:04d}"


def build_jsonl(template: Template, items: list[str], model: str) -> bytes:
    lines = []
    for index, text in enumerate(items, 1):
        body = {
            "model": model,
            "messages": [
                {"role": "system", "content": template.system},
                {"role": "user", "content": text},
            ],
            "max_tokens": template.max_tokens,
            "temperature": 0,
        }
        if template.json_object:
            body["response_format"] = {"type": "json_object"}
        lines.append(json.dumps(
            {"custom_id": custom_id(index), "method": "POST", "url": "/v1/chat/completions", "body": body},
            ensure_ascii=False,
        ))
    return ("\n".join(lines) + "\n").encode("utf-8")


def _json_ok(text: str) -> bool:
    try:
        return isinstance(json.loads(text), dict)
    except ValueError:
        return False


def parse_inputs(data: bytes) -> list[tuple[str, str]]:
    """Recover (custom_id, user text) pairs from a stored input JSONL file, in order."""
    pairs = []
    for line in data.decode("utf-8").splitlines():
        if not line.strip():
            continue
        record = json.loads(line)
        pairs.append((record["custom_id"], record["body"]["messages"][-1]["content"]))
    return pairs


def parse_results(template: Template, output: bytes, errors: bytes, inputs: list[tuple[str, str]]) -> list[dict]:
    """Join result and error JSONL into one row per input, in input order."""
    rows = {}
    for line in output.decode("utf-8").splitlines():
        if not line.strip():
            continue
        record = json.loads(line)
        body = record["response"]["body"]
        message = body["choices"][0]["message"]["content"] or ""
        row = {
            "custom_id": record["custom_id"],
            "status": "completed",
            "text": message.strip(),
            "prompt_tokens": body["usage"]["prompt_tokens"],
            "completion_tokens": body["usage"]["completion_tokens"],
            "error": None,
        }
        if template.json_object and not _json_ok(row["text"]):
            row["status"] = "invalid_json"
        rows[record["custom_id"]] = row
    for line in errors.decode("utf-8").splitlines():
        if not line.strip():
            continue
        record = json.loads(line)
        error = record.get("error") or {}
        rows[record["custom_id"]] = {
            "custom_id": record["custom_id"],
            "status": "failed",
            "text": "",
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "error": error.get("message") or error.get("code") or "failed",
        }
    ordered = []
    for cid, text in inputs:
        ordered.append(rows.get(cid, {
            "custom_id": cid, "status": "pending", "text": "",
            "prompt_tokens": 0, "completion_tokens": 0, "error": None,
        }) | {"input": text})
    return ordered


def to_csv(template: Template, rows: list[dict]) -> str:
    buffer = io.StringIO()
    columns = ["custom_id", "input", "status", "error"]
    if template.json_object:
        columns += list(EXTRACT_FIELDS)
    columns += ["text", "prompt_tokens", "completion_tokens"]
    writer = csv.DictWriter(buffer, fieldnames=columns, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        record = dict(row)
        if template.json_object and row["status"] == "completed":
            fields = json.loads(row["text"])
            for name in EXTRACT_FIELDS:
                record[name] = fields.get(name)
        writer.writerow(record)
    return buffer.getvalue()
