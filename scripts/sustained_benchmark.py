#!/usr/bin/env python3
"""Bounded, resumable real arrival load. No prompt, response or secret is logged.

Only the operator launches inference. This program never changes policy, cancels
tasks, removes history, changes quotas, or starts/restarts a service.
"""
from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import random
import re
import secrets
import stat
import sys
import time
from urllib.parse import urlsplit

import httpx
import psycopg
from psycopg.rows import dict_row

from ops.manage import load_env
from scripts.benchmark import checked_batch, distribution
from scripts.observe_runtime import Sampler, summarize as summarize_runtime


SAFETY_REASONS = {
    "Nonfinite telemetry refused": "nonfinite_telemetry",
    "Configuration and state files must be owner-only regular files": "unsafe_private_file",
    "State directory must be owner-only and must not be a symlink": "unsafe_state_directory",
    "This job already has a live controller": "controller_already_running",
    "Torn journal; preserve files for operator inspection": "journal_torn",
    "Journal sequence mismatch": "journal_sequence_mismatch",
    "Invalid journal; preserve files for operator inspection": "journal_invalid",
    "Resume requires identical job, source, and immutable configuration": "resume_configuration_changed",
    "Unknown journal event": "journal_unknown_event",
    "Only local HTTP service endpoints are supported": "endpoint_not_local",
    "Fixture exceeds 32 MiB bound": "fixture_size_exceeded",
    "Fixture row count out of bounds": "fixture_row_count_invalid",
    "Fixture must contain bounded chat tasks for the selected model": "fixture_invalid",
    "API key is not active in the selected database": "api_key_not_active",
    "Unexpected server batch in this job": "unexpected_server_batch",
    "Duplicate batch or immutable workload identity mismatch": "batch_workload_identity_mismatch",
    "Durable batch/file identity changed": "batch_file_identity_changed",
    "Previously accepted batch missing from durable database": "accepted_batch_missing",
    "Upload outcome uncertain; inspect journal and recover file ID before resuming": "upload_resume_outcome_uncertain",
    "Upload outcome uncertain; no automatic re-upload": "upload_transport_outcome_uncertain",
    "Upload server outcome uncertain; no automatic re-upload": "upload_server_outcome_uncertain",
    "Upload rejected; remote response text withheld": "upload_http_rejected",
    "Upload response invalid; no automatic re-upload": "upload_acknowledgement_invalid",
    "Unconfirmed create exceeds 24-hour recovery horizon; inspect durable state": "create_recovery_horizon_exceeded",
    "Create rejected; remote response text withheld": "create_http_rejected",
    "Invalid create acknowledgement; resume reconciles durable metadata": "create_acknowledgement_invalid",
    "Private environment must supply database and tenant API credentials": "credentials_missing",
    "Engine metrics exceed bound": "engine_metrics_size_exceeded",
    "Observation file reached 64 MiB bound": "observation_size_exceeded",
    "Job ID must be 1-64 alphanumeric, underscore or hyphen characters": "job_id_invalid",
    "Task, cohort, in-flight or submission bound invalid": "submission_bounds_invalid",
    "Nonfinite or out-of-range bounded configuration": "numeric_bounds_invalid",
    "State must be under an ignored .runtime directory": "state_not_private_runtime",
    "Invalid metrics endpoint list": "metrics_endpoints_invalid",
}


class SafetyError(RuntimeError):
    """Public diagnostics are fixed codes, never arbitrary exception text."""
    def __init__(self, message, *, stage=None, http_status=None):
        super().__init__(message)
        self.reason_code = SAFETY_REASONS.get(message, "safety_check_failed")
        self.stage = stage if stage in {"upload", "create"} else None
        self.http_status = http_status if type(http_status) is int and 100 <= http_status <= 599 else None


def timestamp():
    return datetime.now(timezone.utc).isoformat()


def safe_error(exc):
    sqlstate = getattr(exc, "sqlstate", None)
    output = {"type": type(exc).__name__, "sqlstate": sqlstate if isinstance(sqlstate, str) and re.fullmatch(r"[A-Z0-9]{5}", sqlstate) else None}
    if isinstance(exc, SafetyError):
        output.update(reason_code=exc.reason_code, stage=exc.stage, http_status=exc.http_status)
    return output


def normalize(value):
    if isinstance(value, Decimal):
        value = int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): normalize(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [normalize(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        raise SafetyError("Nonfinite telemetry refused")
    return value


def render(value):
    return json.dumps(normalize(value), sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(render(value)).hexdigest()


def private_file(path):
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) & 0o077:
        raise SafetyError("Configuration and state files must be owner-only regular files")


def atomic_json(path, value):
    temporary = path.with_name(path.name + f".{os.getpid()}.{secrets.token_hex(8)}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(render(value) + b"\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        temporary.unlink(missing_ok=True)


class Journal:
    """One writer; fsynced JSONL precedes atomic checkpoint publication.

    A torn/invalid journal fails closed. It is never truncated or silently
    repaired. The checkpoint is a convenience; replay is the source of truth.
    """
    def __init__(self, root: Path, configuration: dict):
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        info = root.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) & 0o077:
            raise SafetyError("State directory must be owner-only and must not be a symlink")
        self.root = root
        self.lock = os.open(root / "controller.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(self.lock)
            self.lock = None
            raise SafetyError("This job already has a live controller") from None
        self.path = root / "journal.jsonl"
        self.state = {"configuration": configuration, "started_epoch": time.time(), "started_at": timestamp(),
                      "sequence": 0, "batches": {}, "events": [], "admission_closed": False}
        if self.path.exists():
            private_file(self.path)
            expected = 1
            try:
                with self.path.open("rb") as handle:
                    for line in handle:
                        if not line.endswith(b"\n"):
                            raise SafetyError("Torn journal; preserve files for operator inspection")
                        event = json.loads(line)
                        if event["sequence"] != expected:
                            raise SafetyError("Journal sequence mismatch")
                        expected += 1
                        self._apply(event)
            except (KeyError, TypeError, ValueError):
                raise SafetyError("Invalid journal; preserve files for operator inspection") from None
            if self.state["configuration"] != configuration:
                raise SafetyError("Resume requires identical job, source, and immutable configuration")
        else:
            self.commit("initialize", value={key: self.state[key] for key in ("configuration", "started_epoch", "started_at")})

    def _apply(self, event):
        self.state["sequence"] = event["sequence"]
        kind = event["kind"]
        if kind == "initialize":
            self.state.update(event["value"])
        elif kind == "batch":
            self.state["batches"][str(event["index"])] = event["value"]
        elif kind == "admission_closed":
            self.state["admission_closed"] = True
        elif kind in {"interrupted", "failed", "finished", "resumed"}:
            self.state["events"].append({key: event[key] for key in ("kind", "at", "value")})
        else:
            raise SafetyError("Unknown journal event")

    def commit(self, kind, *, index=None, value=None):
        event = {"sequence": self.state["sequence"] + 1, "at": timestamp(), "kind": kind, "value": value}
        if index is not None:
            event["index"] = index
        descriptor = os.open(self.path, os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, "ab") as handle:
            handle.write(render(event) + b"\n")
            handle.flush()
            os.fsync(handle.fileno())
        self._apply(event)
        atomic_json(self.root / "checkpoint.json", self.state)

    def close(self):
        if getattr(self, "lock", None) is not None:
            os.close(self.lock)
            self.lock = None

    def __del__(self):
        self.close()


def local_url(value):
    parsed = urlsplit(value)
    if (parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "localhost", "::1"}
            or parsed.username or parsed.password or parsed.path not in {"", "/"} or parsed.query or parsed.fragment):
        raise SafetyError("Only local HTTP service endpoints are supported")
    return value.rstrip("/")


def load_fixture(path, model):
    source = path.read_bytes()
    if len(source) > 32 * 1024 * 1024:
        raise SafetyError("Fixture exceeds 32 MiB bound")
    rows = [json.loads(line) for line in source.splitlines() if line.strip()]
    if not 1 <= len(rows) <= 10000:
        raise SafetyError("Fixture row count out of bounds")
    for row in rows:
        body = row.get("body", {})
        maximum = body.get("max_tokens", body.get("max_completion_tokens"))
        if (row.get("method") != "POST" or row.get("url") != "/v1/chat/completions"
                or body.get("model") != model or not isinstance(body.get("messages"), list)
                or type(maximum) is not int or not 1 <= maximum <= 16384):
            raise SafetyError("Fixture must contain bounded chat tasks for the selected model")
    return source, rows


def make_plan(configuration, rows):
    """Balanced shuffled cycles preserve the fixture mixture without token claims."""
    rng = random.Random(configuration["seed"])
    selected = []
    while len(selected) < configuration["tasks"]:
        cycle = list(range(len(rows)))
        rng.shuffle(cycle)
        selected.extend(cycle)
    selected = selected[:configuration["tasks"]]
    output = []
    offset = 0
    for index in range(math.ceil(len(selected) / configuration["tasks_per_batch"])):
        indices = selected[offset:offset + configuration["tasks_per_batch"]]
        encoded = []
        for number, source_index in enumerate(indices):
            row = dict(rows[source_index])
            row["body"] = dict(row["body"])
            if configuration.get("fixed_output"):
                row["body"]["min_tokens"] = row["body"].get("max_tokens", row["body"].get("max_completion_tokens"))
            row["custom_id"] = f"{configuration['job_id']}-{offset + number:08d}"
            encoded.append(render(row))
        data = b"\n".join(encoded) + b"\n"
        output.append({"index": index, "tasks": len(indices), "task_offset": offset,
                       "arrival_offset_seconds": offset / configuration["arrival_rate"],
                       "input_sha256": hashlib.sha256(data).hexdigest(), "input_bytes": len(data), "data": data,
                       "output_caps": [rows[source_index]["body"].get("max_tokens", rows[source_index]["body"].get("max_completion_tokens")) for source_index in indices]})
        offset += len(indices)
    return output


def backoff_seconds(attempt, seed, *, retry_after=None, ceiling=30):
    # Stable full jitter, bounded even when an untrusted Retry-After is huge.
    delay = random.Random(f"{seed}:{attempt}").uniform(0, min(ceiling, .5 * 2 ** min(attempt, 12)))
    if retry_after is not None:
        try:
            value = float(retry_after)
            if math.isfinite(value) and value >= 0:
                delay = max(delay, min(ceiling, value))
        except ValueError:
            pass
    return delay


def engine_metrics(text):
    names = {"vllm:num_requests_running", "vllm:num_requests_waiting", "vllm:gpu_cache_usage_perc",
             "vllm:kv_cache_usage_perc", "vllm:prompt_tokens_total", "vllm:generation_tokens_total"}
    output = Counter()
    # Labels can contain model/operator data; report only allowlisted names.
    from prometheus_client.parser import text_string_to_metric_families
    for family in text_string_to_metric_families(text):
        for sample in family.samples:
            if sample.name in names and math.isfinite(sample.value):
                output[sample.name] += sample.value
    return dict(output)


class Database:
    def __init__(self, dsn, key, job_id):
        self.conn = psycopg.connect(dsn, row_factory=dict_row, autocommit=True, connect_timeout=5,
            application_name="batchforge-sustained-readonly", options="-c default_transaction_read_only=on -c statement_timeout=5000 -c lock_timeout=500")
        row = self.conn.execute("SELECT tenant_id FROM api_keys WHERE key_hash=%s AND NOT revoked", (hashlib.sha256(key.encode()).hexdigest(),)).fetchone()
        if row is None:
            raise SafetyError("API key is not active in the selected database")
        self.tenant, self.job_id = row["tenant_id"], job_id

    def batches(self):
        return normalize(self.conn.execute("""SELECT id,input_file_id,status,total_count,pending_count,running_count,
            completed_count,failed_count,cancelled_count,metadata,created_at,
            coalesce(completed_at,failed_at,cancelled_at) AS finished_at FROM batches
            WHERE tenant_id=%s AND metadata->>'sustained_job'=%s ORDER BY created_at,id""", (self.tenant, self.job_id)).fetchall())

    def sample(self):
        retention = self.conn.execute("""SELECT l.max_retained_tasks,l.max_payload_bytes,u.retained_tasks,u.charged_payload_bytes
            FROM tenant_retention_limits l JOIN tenant_retention_usage u USING(tenant_id) WHERE l.tenant_id=%s""", (self.tenant,)).fetchone()
        auxiliary = self.conn.execute("""SELECT l.resource,l.max_rows,l.max_bytes,u.rows,u.bytes
            FROM tenant_auxiliary_limits l JOIN tenant_auxiliary_usage u USING(tenant_id,resource)
            WHERE l.tenant_id=%s ORDER BY l.resource""", (self.tenant,)).fetchall()
        queue = self.conn.execute("""SELECT coalesce(sum(pending_count),0) AS pending,coalesce(sum(running_count),0) AS running,
            coalesce(sum(completed_count),0) AS completed,coalesce(sum(failed_count),0) AS failed,
            coalesce(sum(cancelled_count),0) AS cancelled FROM batches WHERE tenant_id=%s""", (self.tenant,)).fetchone()
        cleanup = self.conn.execute("""SELECT count(*) AS terminal_batches_pending_purge,
            max(extract(epoch FROM clock_timestamp()-coalesce(b.completed_at,b.failed_at,b.cancelled_at)))::float8 AS oldest_terminal_age_seconds
            FROM batches b WHERE b.tenant_id=%s AND b.metadata->>'sustained_job'=%s
            AND b.status IN ('completed','failed','cancelled') AND NOT EXISTS
            (SELECT 1 FROM result_archive_runs r WHERE r.tenant_id=b.tenant_id AND r.batch_id=b.id AND r.purged_at IS NOT NULL)""", (self.tenant, self.job_id)).fetchone()
        return normalize({"retention": retention, "auxiliary": auxiliary, "queue": queue, "cleanup": cleanup})

    def tasks(self):
        return normalize(self.conn.execute("""SELECT t.id,t.custom_id,t.batch_id,t.status,t.attempts,t.prompt_tokens,t.completion_tokens,t.finished_at,
            extract(epoch FROM (t.finished_at-b.created_at))::float8 AS completion_seconds,
            extract(epoch FROM (min(a.started_at)-b.created_at))::float8 AS first_queue_seconds,
            count(a.token) FILTER(WHERE a.status='completed') AS accepted_attempts,
            count(a.token) AS total_attempt_records,
            coalesce(sum(a.prompt_tokens) FILTER(WHERE a.status='completed'),0) AS accepted_prompt_tokens,
            coalesce(sum(a.completion_tokens) FILTER(WHERE a.status='completed'),0) AS accepted_completion_tokens,
            coalesce(bool_or(t.response->>'batchforge_test_only'='true'),false) AS simulated
            FROM tasks t JOIN batches b ON b.id=t.batch_id LEFT JOIN attempts a ON a.task_id=t.id
            WHERE b.tenant_id=%s AND b.metadata->>'sustained_job'=%s
            GROUP BY t.id,b.created_at ORDER BY t.custom_id""", (self.tenant, self.job_id)).fetchall())

    def close(self):
        self.conn.close()


def reconcile(journal, plan, found, *, known_before_query=None):
    by_index = {}
    for row in found:
        metadata = row["metadata"]
        try:
            index = int(metadata["sustained_index"])
            expected = plan[index]
        except (ValueError, KeyError, IndexError):
            raise SafetyError("Unexpected server batch in this job") from None
        if (index < 0 or index in by_index or metadata.get("sustained_input_sha256") != expected["input_sha256"]
                or row["total_count"] != expected["tasks"]):
            raise SafetyError("Duplicate batch or immutable workload identity mismatch")
        by_index[index] = row
        old = journal.state["batches"].get(str(index), {})
        if old.get("batch_id") not in {None, row["id"]} or old.get("file_id") not in {None, row["input_file_id"]}:
            raise SafetyError("Durable batch/file identity changed")
        new = {**old, "index": index, "tasks": expected["tasks"], "file_id": row["input_file_id"], "batch_id": row["id"],
               "status": row["status"], "counts": {key: row[key] for key in ("pending_count", "running_count", "completed_count", "failed_count", "cancelled_count")},
               "created_at": row["created_at"], "finished_at": row["finished_at"], "stage": "accepted"}
        if new != old:
            journal.commit("batch", index=index, value=new)
    for index, old in journal.state["batches"].items():
        if (old.get("batch_id") and int(index) not in by_index
                and (known_before_query is None or old["batch_id"] in known_before_query)):
            raise SafetyError("Previously accepted batch missing from durable database")
    return by_index


def inflight_reservations(journal, plan, found, pending):
    """Count each task once across a DB snapshot and locally newer accepts."""
    active = sum(row["pending_count"] + row["running_count"] for row in found)
    visible = {int(row["metadata"]["sustained_index"]) for row in found}
    outside_snapshot = ({index for index in pending if index not in visible}
                        | {int(index) for index, state in journal.state["batches"].items()
                           if state.get("batch_id") and int(index) not in visible})
    return active, sum(plan[index]["tasks"] for index in outside_snapshot)


def trusted_upload_not_forwarded(response, attempt):
    """Only the bound loopback edge can prove it never sent this request.

    The released gateway strips these reserved headers from every upstream
    response. A generic 503, transport exception or unknown nonce is ambiguous.
    """
    if (response.request.url != httpx.URL("http://127.0.0.1:8080/v1/files")
            or response.request.method != "POST" or response.status_code != 503
            or not re.fullmatch(r"[0-9a-f]{32}", attempt)
            or response.request.headers.get_list("X-BatchForge-Upload-Attempt") != [attempt]
            or response.headers.get_list("X-BatchForge-Outcome") != ["not-forwarded"]
            or response.headers.get_list("X-BatchForge-Outcome-Version") != ["1"]
            or response.headers.get_list("X-BatchForge-Upload-Attempt") != [attempt]
            or len(response.content) > 4096):
        return False
    try:
        def unique(pairs):
            value = {}
            for key, item in pairs:
                if key in value:
                    raise ValueError()
                value[key] = item
            return value
        def nonfinite(_):
            raise ValueError()
        value = json.loads(response.content.decode("utf-8"), object_pairs_hook=unique, parse_constant=nonfinite)
        error = value.get("error", {}) if isinstance(value, dict) else {}
        return (isinstance(error, dict) and error.get("code") == "gateway_not_forwarded"
                and error.get("stage") == "select_upstream" and error.get("reason") == "no_ready_upstream")
    except (ValueError, UnicodeError, RecursionError):
        return False


def submission_failure(journal, index, state, message, *, stage, http_status=None):
    error = SafetyError(message, stage=stage, http_status=http_status)
    state["safe_error"] = safe_error(error)
    if http_status is not None:
        state["last_" + stage + "_response"] = {"http_status": http_status, "stage": stage,
            "reason_code": error.reason_code, "request_number": state.get(stage + "_requests", 0)}
    journal.commit("batch", index=index, value=dict(state))
    return error


async def submit(client, journal, item, configuration, admission_deadline):
    index = item["index"]
    existing = dict(journal.state["batches"].get(str(index), {}))
    state = {"index": index, "tasks": item["tasks"], "stage": "planned", **existing}
    if state.get("batch_id"):
        return
    if state["stage"] == "uploading":
        raise submission_failure(journal, index, state,
            "Upload outcome uncertain; inspect journal and recover file ID before resuming", stage="upload")
    state.setdefault("first_submission_epoch", time.time())
    key_hash = hashlib.sha256(f"{configuration['job_id']}:{index}:{item['input_sha256']}".encode()).hexdigest()[:32]
    state.setdefault("idempotency_key", f"bf1.{int(journal.state['started_epoch']):010d}.{key_hash}")
    state.setdefault("arrival_delay_seconds", max(0, time.time() - (journal.state["started_epoch"] + item["arrival_offset_seconds"])))
    if not state.get("file_id"):
        while time.time() < admission_deadline:
            state["stage"] = "uploading"
            state["upload_requests"] = state.get("upload_requests", 0) + 1
            attempt = state["upload_attempt"] = secrets.token_hex(16)
            state.pop("last_upload_response", None)
            journal.commit("batch", index=index, value=dict(state))
            try:
                response = await client.post("/v1/files", data={"purpose": "batch"}, files={"file":
                    (f"{configuration['job_id']}-{index:06d}.jsonl", item["data"], "application/jsonl")},
                    headers={"X-BatchForge-Upload-Attempt": attempt})
            except httpx.HTTPError as exc:
                state["upload_transport_error"] = safe_error(exc)
                raise submission_failure(journal, index, state,
                    "Upload outcome uncertain; no automatic re-upload", stage="upload") from None
            state["last_upload_response"] = {"http_status": response.status_code, "stage": "upload",
                "reason_code": "upload_http_response", "request_number": state["upload_requests"]}
            if trusted_upload_not_forwarded(response, attempt):
                state["stage"] = "upload_not_forwarded"
                state["upload_not_forwarded"] = state.get("upload_not_forwarded", 0) + 1
                state["last_upload_response"]["reason_code"] = "gateway_not_forwarded"
                journal.commit("batch", index=index, value=dict(state))
                await asyncio.sleep(min(max(0, admission_deadline - time.time()), backoff_seconds(
                    state["upload_requests"], configuration["seed"] + index, retry_after=response.headers.get("Retry-After"))))
                continue
            if response.status_code == 429:
                state["stage"] = "upload_rejected"
                state["upload_429"] = state.get("upload_429", 0) + 1
                state["last_upload_response"]["reason_code"] = "upload_rate_limited"
                journal.commit("batch", index=index, value=dict(state))
                await asyncio.sleep(backoff_seconds(state["upload_429"], configuration["seed"] + index,
                                                   retry_after=response.headers.get("Retry-After")))
                continue
            if response.status_code >= 500:
                raise submission_failure(journal, index, state,
                    "Upload server outcome uncertain; no automatic re-upload", stage="upload", http_status=response.status_code)
            if not 200 <= response.status_code < 300:
                raise submission_failure(journal, index, state,
                    "Upload rejected; remote response text withheld", stage="upload", http_status=response.status_code)
            try:
                file_id = response.json()["id"]
                if not isinstance(file_id, str) or not re.fullmatch(r"[a-zA-Z0-9_-]{1,100}", file_id):
                    raise ValueError()
            except (ValueError, KeyError, TypeError):
                raise submission_failure(journal, index, state,
                    "Upload response invalid; no automatic re-upload", stage="upload", http_status=response.status_code) from None
            state.update(file_id=file_id, stage="uploaded")
            journal.commit("batch", index=index, value=dict(state))
            break
    if not state.get("file_id"):
        return
    key = state["idempotency_key"]
    if time.time() - journal.state["started_epoch"] >= 86400:
        raise submission_failure(journal, index, state,
            "Unconfirmed create exceeds 24-hour recovery horizon; inspect durable state", stage="create")
    while time.time() < admission_deadline:
        state["stage"] = "creating"
        state["create_requests"] = state.get("create_requests", 0) + 1
        journal.commit("batch", index=index, value=dict(state))
        body = {"input_file_id": state["file_id"], "endpoint": "/v1/chat/completions", "completion_window": "24h",
                "metadata": {"sustained_job": configuration["job_id"], "sustained_index": str(index),
                             "sustained_input_sha256": item["input_sha256"]}}
        try:
            response = await client.post("/v1/batches", json=body, headers={"Idempotency-Key": key})
        except httpx.HTTPError as exc:
            state["safe_error"] = {**safe_error(exc), "reason_code": "create_transport_outcome_uncertain",
                "stage": "create", "http_status": None}
            state["uncertain_create_responses"] = state.get("uncertain_create_responses", 0) + 1
            journal.commit("batch", index=index, value=dict(state))
            await asyncio.sleep(backoff_seconds(state["create_requests"], configuration["seed"] + index))
            continue
        if response.status_code == 429 or response.status_code >= 500:
            field = "create_429" if response.status_code == 429 else "uncertain_create_responses"
            state[field] = state.get(field, 0) + 1
            state["last_create_response"] = {"http_status": response.status_code, "stage": "create",
                "reason_code": "create_rate_limited" if response.status_code == 429 else "create_server_outcome_uncertain",
                "request_number": state["create_requests"]}
            journal.commit("batch", index=index, value=dict(state))
            await asyncio.sleep(backoff_seconds(state["create_requests"], configuration["seed"] + index,
                                               retry_after=response.headers.get("Retry-After")))
            continue
        if not 200 <= response.status_code < 300:
            raise submission_failure(journal, index, state,
                "Create rejected; remote response text withheld", stage="create", http_status=response.status_code)
        try:
            batch = checked_batch(response.json())
        except (ValueError, KeyError, TypeError):
            raise submission_failure(journal, index, state,
                "Invalid create acknowledgement; resume reconciles durable metadata", stage="create", http_status=response.status_code) from None
        state.update(batch_id=batch["id"], status=batch["status"], stage="accepted", acknowledged_at=timestamp())
        journal.commit("batch", index=index, value=dict(state))
        return


def summarize(journal, tasks, observations, elapsed, runtime_apis=None, plan=None):
    config = journal.state["configuration"]
    states = list(journal.state["batches"].values())
    accepted = [row for row in states if row.get("batch_id")]
    completed = [row for row in tasks if row["status"] == "completed"]
    terminal = [row for row in tasks if row["status"] in {"completed", "failed", "cancelled"}]
    ids = [row["custom_id"] for row in tasks]
    known = {f"{config['job_id']}-{index:08d}" for index in range(config["tasks"])}
    exact = (len(ids) == len(set(ids)) and set(ids) <= known
             and len(tasks) == sum(row["tasks"] for row in accepted)
             and all(row["accepted_attempts"] == int(row["status"] == "completed") for row in tasks))
    usage_matches = all(row["prompt_tokens"] == row["accepted_prompt_tokens"] and row["completion_tokens"] == row["accepted_completion_tokens"] for row in completed)
    caps = {f"{config['job_id']}-{item['task_offset'] + index:08d}": cap
            for item in plan or [] for index, cap in enumerate(item["output_caps"])}
    output_lengths_exact = (all(row["completion_tokens"] == caps.get(row["custom_id"]) for row in completed)
                            if config.get("fixed_output") else None)
    classes = {}
    for row in completed:
        cap = caps.get(row["custom_id"])
        if cap:
            ratio = row["prompt_tokens"] / cap
            label = "prefill_heavy" if ratio >= 16 else "decode_heavy" if ratio <= 4 else "balanced"
            classes.setdefault(label, []).append(row)
    windows = Counter()
    scheduled_latencies = []
    for row in completed:
        if row.get("finished_at"):
            offset = datetime.fromisoformat(row["finished_at"]).timestamp() - journal.state["started_epoch"]
            windows[str(max(0, int(offset // 60)))] += 1
            if row["custom_id"] in known:
                task_index = int(row["custom_id"].rsplit("-", 1)[1])
                cohort_offset = (task_index // config["tasks_per_batch"]) * config["tasks_per_batch"] / config["arrival_rate"]
                scheduled_latencies.append(offset - cohort_offset)
    admission_wait = []
    for row in accepted:
        if row.get("created_at"):
            scheduled = journal.state["started_epoch"] + row["index"] * config["tasks_per_batch"] / config["arrival_rate"]
            admission_wait.append(datetime.fromisoformat(row["created_at"]).timestamp() - scheduled)
    instants = sorted(datetime.fromisoformat(sample["at"]).timestamp() for sample in observations)
    gaps = [new - old for old, new in zip(instants, instants[1:])]
    segment_span = sum(gap for gap in gaps if gap <= 2 * config.get("sample_seconds", 10))
    all_gaps = ([max(0, instants[0] - journal.state["started_epoch"])] + gaps +
                [max(0, journal.state["started_epoch"] + elapsed - instants[-1])]) if instants else [elapsed]
    allocation = elapsed * config["hourly_cost"] / 3600 if config.get('hourly_cost') is not None else None
    snapshots = [sample["quota"] for sample in observations if "quota" in sample]
    gpu = {}
    for sample in observations:
        for row in sample.get("gpu", {}).get("devices", []):
            if row["utilization_percent"] is not None:
                gpu.setdefault(str(row["index"]), []).append(row["utilization_percent"])
    return normalize({"report_version": 1, "job_id": config["job_id"], "configuration": config,
        "started_at": journal.state["started_at"], "finished_at": timestamp(), "elapsed_seconds_including_disconnect_and_validation": elapsed,
        "accepted_batches": len(accepted), "accepted_tasks": sum(row["tasks"] for row in accepted),
        "completed_tasks": len(completed), "terminal_tasks": len(terminal), "unfinished_tasks": len(tasks) - len(terminal),
        "failed_tasks": sum(row["status"] == "failed" for row in tasks), "cancelled_tasks": sum(row["status"] == "cancelled" for row in tasks),
        "accepted_exactly_once": exact, "duplicate_accepted_attempts": sum(max(0, row["accepted_attempts"] - 1) for row in tasks),
        "fixed_output_lengths_exact": output_lengths_exact,
        "accepted_usage_matches_task_usage": usage_matches,
        "attempt_records": sum(row["total_attempt_records"] for row in tasks),
        "tasks_with_retries": sum(row["total_attempt_records"] > 1 for row in tasks),
        "prompt_tokens": sum(row["prompt_tokens"] for row in completed), "completion_tokens": sum(row["completion_tokens"] for row in completed),
        "completed_tasks_per_second": len(completed) / elapsed if elapsed else None,
        "task_completion_seconds": distribution([row["completion_seconds"] for row in completed if row["completion_seconds"] is not None]),
        "scheduled_arrival_to_finished_seconds": distribution(scheduled_latencies),
        "scheduled_arrival_to_durable_admission_seconds": distribution(admission_wait),
        "task_first_queue_seconds": distribution([row["first_queue_seconds"] for row in tasks if row["first_queue_seconds"] is not None]),
        "completion_counts_by_60_second_window": dict(sorted(windows.items(), key=lambda item: int(item[0]))),
        "by_class": {label: {"tasks": len(group), "completion_seconds": distribution([row["completion_seconds"] for row in group if row["completion_seconds"] is not None]),
                    "prompt_tokens": sum(row["prompt_tokens"] for row in group), "completion_tokens": sum(row["completion_tokens"] for row in group)} for label, group in classes.items()},
        "class_definition": "observed prompt tokens / requested output cap: >=16 prefill_heavy, <=4 decode_heavy, otherwise balanced",
        "arrival_to_submission_delay_seconds": distribution([row["arrival_delay_seconds"] for row in states if "arrival_delay_seconds" in row]),
        "percentile_method": "linear interpolation at (n-1)*p; completion population is completed tasks only",
        "latency_definition": "database batch.created_at to accepted task.finished_at; includes durable queue; excludes client upload/admission waiting",
        "scheduled_latency_definition": "accepted task.finished_at minus persisted job start plus deterministic cohort release offset; includes upload, 429 backoff, local concurrency and disconnect delay",
        "cost": {"assumed_whole_pod_usd_per_hour": config["hourly_cost"], "allocation_usd": allocation,
                 "usd_per_million_completed_tasks": allocation / len(completed) * 1e6 if completed and allocation is not None else None,
                 "measured_invoice":None,
                 "basis": "operator supplied whole-Pod allocation; includes idle, backoff, drain, validation and disconnect wall time; not invoice or marginal cost" if allocation is not None else "Current price unknown; allocation and invoice are unmeasured"},
        "http_rejections": {field: sum(row.get(field, 0) for row in states) for field in
            ("upload_429", "upload_not_forwarded", "create_429", "uncertain_create_responses")},
        "gpu_utilization_percent": {key: distribution(values) for key, values in sorted(gpu.items())},
        "observation_samples": len(observations), "observation_errors": sum("error" in sample for sample in observations),
        "sampling_coverage": {"first_sample_at": observations[0]["at"] if observations else None,
            "last_sample_at": observations[-1]["at"] if observations else None,
            "observed_wall_span_seconds": instants[-1] - instants[0] if instants else 0,
            "maximum_sample_or_boundary_gap_seconds": max(all_gaps),
            "long_gaps_over_twice_target_interval": sum(gap > 2 * config.get("sample_seconds", 10) for gap in all_gaps),
            "continuous_sampling_segment_span_seconds": segment_span,
            "wall_seconds_outside_sampling_segments": max(0, elapsed - segment_span),
            "basis": "segments join consecutive samples only when their gap is <=2x target interval; snapshots do not imply continuous observation; outages are never zero-filled"},
        "first_quota": snapshots[0] if snapshots else None, "last_quota": snapshots[-1] if snapshots else None,
        "runtime_http": summarize_runtime(runtime_apis or []), "tasks": tasks, "journal_sequence": journal.state["sequence"],
        "passed": (exact and usage_matches and output_lengths_exact is not False and len(completed) == config["tasks"] and not any(row.get("simulated") for row in tasks)
                   and not any(row["status"] != "completed" for row in tasks)),
        "limitations": ["Finite offered-load experiment; no daily or fleet capacity guarantee",
            "Tasks not admitted before the duration cap remain unsubmitted and make the full-plan check fail",
            "Client arrival delay/backpressure is reported separately from database completion latency",
            "GPU activity is device-wide NVML utilization, not Tensor Core utilization or exclusively this job",
            "Engine token counters are cumulative aggregate stage activity; no per-request prefill/decode timestamp is inferred",
            "Periodic queue/quota/cleanup samples cannot reconstruct behavior between samples",
            "No task cancellation, archive/purge call, quota change, history cleanup, or output quality scoring is performed"]})


async def run(args, *, transport=None, database=None):
    source, rows = load_fixture(args.input_jsonl, args.model)
    private_file(args.env_file)
    env = load_env(args.env_file)
    if not env.get("DATABASE_URL") or not env.get("BATCHFORGE_API_KEY"):
        raise SafetyError("Private environment must supply database and tenant API credentials")
    configuration = {name: getattr(args, name) for name in ("job_id", "model", "tasks", "tasks_per_batch", "arrival_rate", "seed",
        "duration_seconds", "drain_seconds", "max_inflight_tasks", "submission_concurrency", "hourly_cost", "poll_seconds", "sample_seconds", "request_timeout", "fixed_output")}
    configuration.update(source_sha256=hashlib.sha256(source).hexdigest(), url=local_url(args.url), engine_url=local_url(args.engine_url),
                         api=[list(item) for item in args.api], code_sha256={name: hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()
                            for name in ("sustained_benchmark.py", "benchmark.py", "observe_runtime.py")})
    plan = make_plan(configuration, rows)
    journal = Journal(args.state_dir, configuration)
    db = database or Database(env["DATABASE_URL"], env["BATCHFORGE_API_KEY"], args.job_id)
    observations_path = args.state_dir / "observations.jsonl"
    if observations_path.exists():
        private_file(observations_path)
    observations = [json.loads(line) for line in observations_path.read_bytes().splitlines()] if observations_path.exists() else []
    pending = {}
    admission_deadline = journal.state["started_epoch"] + args.duration_seconds
    end_deadline = admission_deadline + args.drain_seconds
    last_sample = 0.0
    key = env["BATCHFORGE_API_KEY"]
    metric_token = env.get("BATCHFORGE_WORKER_TOKEN")
    api_sampler = Sampler(env["DATABASE_URL"], metric_token or "", [])
    runtime_apis = [{"started_at": sample["at"], "finished_at": sample["at"], "apis": sample["apis"]}
                    for sample in observations if "apis" in sample]
    journal.commit("resumed", value={"controller_pid": os.getpid()})
    try:
        async with httpx.AsyncClient(base_url=configuration["url"], headers={"Authorization": "Bearer " + key},
            timeout=args.request_timeout, transport=transport, trust_env=False, follow_redirects=False) as client:
            while True:
                # All production database operations in this class are read-only.
                # The HTTP futures can finish while the read-only SQL query is
                # in flight. Require pre-query identities to remain visible;
                # newer journal accepts reserve capacity until the next read.
                known_before_query = {row["batch_id"] for row in journal.state["batches"].values() if row.get("batch_id")}
                found = await asyncio.to_thread(db.batches)
                by_index = reconcile(journal, plan, found, known_before_query=known_before_query)
                for index, future in list(pending.items()):
                    if future.done():
                        del pending[index]
                        await future
                now = time.time()
                if now - last_sample >= args.sample_seconds:
                    sample = {"at": timestamp()}
                    sample_started = time.monotonic()
                    try:
                        sample["quota"] = await asyncio.to_thread(db.sample)
                        sample["gpu"] = await asyncio.to_thread(Sampler.gpu)
                        reply = await client.get(configuration["engine_url"] + "/metrics", headers={"Authorization": ""}, timeout=3)
                        reply.raise_for_status()
                        if len(reply.content) > 2 * 1024 * 1024:
                            raise SafetyError("Engine metrics exceed bound")
                        sample["engine"] = engine_metrics(reply.text)
                        if metric_token:
                            apis = [await asyncio.to_thread(api_sampler.api, (name, url)) for name, url in args.api]
                            runtime_apis.append({"started_at": sample["at"], "finished_at": timestamp(), "apis": apis})
                            sample["apis"] = apis
                    except Exception as exc:
                        sample["error"] = safe_error(exc)
                    sample["finished_at"] = timestamp()
                    sample["elapsed_seconds"] = time.monotonic() - sample_started
                    observations.append(sample)
                    if observations_path.exists() and observations_path.stat().st_size > 64 * 1024 * 1024:
                        raise SafetyError("Observation file reached 64 MiB bound")
                    descriptor = os.open(observations_path, os.O_APPEND | os.O_CREAT | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
                    with os.fdopen(descriptor, "ab") as handle:
                        handle.write(render(sample) + b"\n")
                        handle.flush()
                        os.fsync(handle.fileno())
                    last_sample = now
                active, reserved = inflight_reservations(journal, plan, found, pending)
                for item in plan:
                    index = item["index"]
                    if now >= admission_deadline or journal.state["admission_closed"]:
                        break
                    if index in by_index or index in pending:
                        continue
                    if journal.state["batches"].get(str(index), {}).get("batch_id"):
                        continue
                    if journal.state["started_epoch"] + item["arrival_offset_seconds"] > now:
                        break
                    if len(pending) >= args.submission_concurrency or active + reserved + item["tasks"] > args.max_inflight_tasks:
                        break
                    pending[index] = asyncio.create_task(submit(client, journal, item, configuration, admission_deadline))
                    reserved += item["tasks"]
                if now >= admission_deadline and not journal.state["admission_closed"]:
                    journal.commit("admission_closed", value={"reason": "duration_cap"})
                accepted = sum(row["total_count"] for row in found)
                if (accepted == args.tasks and active + reserved == 0 and not pending) or now >= end_deadline:
                    break
                if journal.state["admission_closed"] and active + reserved == 0 and not pending:
                    break
                await asyncio.sleep(args.poll_seconds)
            # Await a bounded in-flight request before report generation. Never
            # send cancellations to production; only local HTTP futures stop.
            if pending:
                await asyncio.gather(*pending.values())
            reconcile(journal, plan, await asyncio.to_thread(db.batches))
            tasks = await asyncio.to_thread(db.tasks)
            report = summarize(journal, tasks, observations, time.time() - journal.state["started_epoch"], runtime_apis, plan)
            atomic_json(args.state_dir / f"report-{journal.state['sequence']:08d}.json", report)
            atomic_json(args.state_dir / "report.json", report)
            journal.commit("finished", value={"passed": report["passed"], "completed_tasks": report["completed_tasks"]})
            return report
    except BaseException as exc:
        journal.commit("interrupted" if isinstance(exc, (KeyboardInterrupt, asyncio.CancelledError)) else "failed", value=safe_error(exc))
        raise
    finally:
        for future in pending.values():
            if not future.done():
                future.cancel()
        if pending:
            await asyncio.gather(*pending.values(), return_exceptions=True)
        db.close()
        journal.close()


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--job-id", required=True)
    result.add_argument("--env-file", type=Path, required=True)
    result.add_argument("--input-jsonl", type=Path, required=True)
    result.add_argument("--model", required=True)
    result.add_argument("--state-dir", type=Path)
    result.add_argument("--tasks", type=int, default=720)
    result.add_argument("--tasks-per-batch", type=int, default=20)
    result.add_argument("--arrival-rate", type=float, default=.4, help="Offered tasks per second; cohorts arrive at deterministic intervals")
    result.add_argument("--seed", type=int, default=20261024)
    result.add_argument("--fixed-output", action="store_true", help="Set min_tokens to each output cap and verify actual lengths; synthetic compute load, not quality")
    result.add_argument("--duration-seconds", type=float, default=1800, help="Maximum admission wall time, persisted across resume")
    result.add_argument("--drain-seconds", type=float, default=1200)
    result.add_argument("--max-inflight-tasks", type=int, default=128)
    result.add_argument("--submission-concurrency", type=int, default=2)
    result.add_argument("--hourly-cost", type=float, default=None, help="Optional explicit whole-Pod allocation scenario; unknown stays null, never invoice")
    result.add_argument("--poll-seconds", type=float, default=2)
    result.add_argument("--sample-seconds", type=float, default=10)
    result.add_argument("--request-timeout", type=float, default=120)
    result.add_argument("--url", default="http://127.0.0.1:8080")
    result.add_argument("--engine-url", default="http://127.0.0.1:8300")
    result.add_argument("--api", action="append", help="Optional worker-authenticated metrics source, name=local URL")
    return result


def validate_args(args):
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}", args.job_id):
        raise SafetyError("Job ID must be 1-64 alphanumeric, underscore or hyphen characters")
    if (not 1 <= args.tasks <= 20000 or not 1 <= args.tasks_per_batch <= min(args.tasks, 1000)
            or not args.tasks_per_batch <= args.max_inflight_tasks <= 4096
            or not 1 <= args.submission_concurrency <= 16):
        raise SafetyError("Task, cohort, in-flight or submission bound invalid")
    ranges = {"arrival_rate": (.001, 1000), "duration_seconds": (1, 14400), "drain_seconds": (1, 7200),
              "poll_seconds": (.1, 30), "sample_seconds": (1, 60), "request_timeout": (1, 180), "hourly_cost": (0, 100000)}
    for name, (low, high) in ranges.items():
        value = getattr(args, name)
        if name=='hourly_cost' and value is None:continue
        if not math.isfinite(value) or not low <= value <= high:
            raise SafetyError("Nonfinite or out-of-range bounded configuration")
    args.state_dir = args.state_dir or Path(".runtime/sustained") / args.job_id
    if ".runtime" not in args.state_dir.resolve().parts:
        raise SafetyError("State must be under an ignored .runtime directory")
    args.api = [tuple(item.split("=", 1)) for item in args.api or []]
    if len(args.api) > 8 or any(len(item) != 2 or not re.fullmatch(r"[a-zA-Z0-9_-]{1,32}", item[0]) for item in args.api):
        raise SafetyError("Invalid metrics endpoint list")
    args.api = [(name, local_url(url)) for name, url in args.api]
    return args


def main():
    try:
        args = validate_args(parser().parse_args())
        report = asyncio.run(run(args))
        print(json.dumps({key: report[key] for key in ("job_id", "passed", "accepted_tasks", "completed_tasks", "accepted_exactly_once", "task_completion_seconds", "cost")}))
        raise SystemExit(0 if report["passed"] else 1)
    except Exception as exc:
        print(json.dumps({"runner_failed": safe_error(exc)}), file=sys.stderr)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
