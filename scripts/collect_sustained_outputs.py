"""Collect one terminal sustained cohort through tenant GETs on the same Pod.

Only metadata is printed. Raw output remains in a private .runtime directory.
No workload, cancellation, policy, archive or business-state mutation is sent.
GETs may acquire the platform's normal short-lived result reader leases.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shlex
import stat
import sys
from urllib.parse import urlsplit

import httpx
import psycopg
from psycopg.rows import dict_row

MAX_FILE = 16 * 1024 * 1024
MAX_TOTAL = 64 * 1024 * 1024
OPAQUE = re.compile(r"[A-Za-z0-9_-]{1,100}\Z")


class CollectionError(ValueError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def digest(raw): return hashlib.sha256(raw).hexdigest()


def private_read(path, maximum=MAX_FILE):
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            before = os.fstat(descriptor)
            if (not stat.S_ISREG(before.st_mode) or before.st_uid != os.geteuid()
                    or stat.S_IMODE(before.st_mode) & 0o077 or not 0 < before.st_size <= maximum):
                raise CollectionError("unsafe_private_file")
            with os.fdopen(descriptor, "rb", closefd=False) as handle:
                raw = handle.read(maximum + 1)
            after = os.fstat(descriptor)
            if len(raw) != before.st_size or any(getattr(before, name) != getattr(after, name) for name in ("st_dev", "st_ino", "st_mtime_ns", "st_ctime_ns", "st_size")):
                raise CollectionError("private_file_changed")
            return raw
        finally: os.close(descriptor)
    except CollectionError: raise
    except OSError: raise CollectionError("private_file_unavailable") from None


def strict_json(raw):
    def constant(_): raise CollectionError("invalid_jsonl")
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result: raise CollectionError("invalid_jsonl")
            result[key] = value
        return result
    try:
        text = raw.decode("utf-8", errors="strict") if isinstance(raw, bytes) else raw
        value = json.loads(text, parse_constant=constant, object_pairs_hook=unique)
        # Refuse overflowing floats, NUL and lone surrogates without printing
        # remote text. JSON floats are otherwise valid collector input; the
        # separate integer-only reservation audit can conservatively refuse it.
        json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8")
        def reject_nul(item, depth=0):
            if depth > 128: raise CollectionError("invalid_jsonl")
            if isinstance(item, str) and "\x00" in item: raise CollectionError("invalid_jsonl")
            if isinstance(item, dict):
                for key, child in item.items(): reject_nul(key, depth + 1); reject_nul(child, depth + 1)
            elif isinstance(item, list):
                for child in item: reject_nul(child, depth + 1)
        reject_nul(value)
        return value
    except CollectionError: raise
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise CollectionError("invalid_jsonl") from None


def atomic_state(path, value):
    temporary = path.with_name(path.name + "." + secrets.token_hex(8) + ".tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode() + b"\n")
            handle.flush(); os.fsync(handle.fileno())
        os.replace(temporary, path)
        sync_directory(path.parent)
    finally: temporary.unlink(missing_ok=True)


def sync_directory(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try: os.fsync(descriptor)
    finally: os.close(descriptor)


@contextmanager
def lock_file(path, *, create=False):
    try:
        descriptor = os.open(path, os.O_RDWR | os.O_NOFOLLOW | (os.O_CREAT if create else 0), 0o600)
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) & 0o077:
            os.close(descriptor); raise CollectionError("unsafe_lock")
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except CollectionError: raise
    except OSError:
        if "descriptor" in locals(): os.close(descriptor)
        raise CollectionError("controller_or_collector_still_active") from None
    try: yield
    finally: os.close(descriptor)


def terminal_binding(args):
    if (not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", args.run_id)
            or type(args.expected_completed) is not int or not 1 <= args.expected_completed <= 20000
            or any(not re.fullmatch("[0-9a-f]{64}", value) for value in (args.checkpoint_sha256, args.report_sha256))):
        raise CollectionError("invalid_binding")
    checkpoint_raw, report_raw = private_read(args.checkpoint), private_read(args.report)
    if digest(checkpoint_raw) != args.checkpoint_sha256 or digest(report_raw) != args.report_sha256:
        raise CollectionError("bound_source_sha_mismatch")
    checkpoint, report = strict_json(checkpoint_raw), strict_json(report_raw)
    config = report.get("configuration", {})
    expected = args.expected_completed
    events = checkpoint.get("events", [])
    if (config.get("job_id") != args.run_id or report.get("job_id") != args.run_id
            or checkpoint.get("configuration") != config or not events or events[-1].get("kind") != "finished"
            or checkpoint.get("sequence") != report.get("journal_sequence", -2) + 1
            or events[-1].get("value") != {"passed": report.get("passed"), "completed_tasks": expected}
            or any(report.get(key) != expected for key in ("accepted_tasks", "completed_tasks", "terminal_tasks"))
            or any(report.get(key) != 0 for key in ("failed_tasks", "cancelled_tasks", "unfinished_tasks", "duplicate_accepted_attempts"))
            or report.get("accepted_exactly_once") is not True or report.get("accepted_usage_matches_task_usage") is not True
            or type(report.get("passed")) is not bool or report["passed"] != (config.get("tasks") == expected)
            or config.get("fixed_output") and report.get("fixed_output_lengths_exact") is not True
            or args.require_full_plan and (report.get("passed") is not True or config.get("tasks") != expected)):
        raise CollectionError("cohort_not_proven_terminal")
    accepted = {}
    for index, state in checkpoint.get("batches", {}).items():
        if not state.get("batch_id"):
            if state.get("uncertain_create_responses", 0) or state.get("safe_error") or state.get("stage") == "uploading":
                raise CollectionError("unresolved_submission_outcome")
            if state.get("create_requests", 0) != state.get("create_429", 0):
                raise CollectionError("unresolved_submission_outcome")
            continue
        if (not str(index).isdigit() or state.get("index") != int(index) or state.get("status") != "completed"
                or not OPAQUE.fullmatch(state["batch_id"]) or not OPAQUE.fullmatch(state.get("file_id", ""))
                or type(state.get("tasks")) is not int or state["tasks"] < 1
                or state.get("counts") != {"pending_count": 0, "running_count": 0, "completed_count": state["tasks"], "failed_count": 0, "cancelled_count": 0}):
            raise CollectionError("checkpoint_batch_invalid")
        accepted[state["batch_id"]] = state
    tasks = report.get("tasks", [])
    if (sum(state["tasks"] for state in accepted.values()) != expected or len(tasks) != expected
            or len({row.get("custom_id") for row in tasks}) != expected):
        raise CollectionError("checkpoint_report_count_mismatch")
    for row in tasks:
        if (row.get("batch_id") not in accepted or row.get("status") != "completed"
                or row.get("simulated") is True
                or not re.fullmatch(re.escape(args.run_id) + r"-[0-9]{8}", row.get("custom_id", ""))
                or row.get("accepted_attempts") != 1 or row.get("prompt_tokens") != row.get("accepted_prompt_tokens")
                or row.get("completion_tokens") != row.get("accepted_completion_tokens")):
            raise CollectionError("report_task_invalid")
    return checkpoint, report, accepted


def environment(raw):
    result = {}
    try:
        for line in raw.decode().splitlines():
            line = line.strip()
            if not line or line.startswith("#"): continue
            key, separator, value = line.removeprefix("export ").partition("=")
            if not separator or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key) or key in result:
                raise CollectionError("invalid_private_configuration")
            result[key] = " ".join(shlex.split(value, comments=True))
    except (ValueError, UnicodeError): raise CollectionError("invalid_private_configuration") from None
    if not result.get("DATABASE_URL") or not result.get("BATCHFORGE_API_KEY"):
        raise CollectionError("invalid_private_configuration")
    return result


def database_batches(dsn, key, run_id):
    try:
        with psycopg.connect(dsn, row_factory=dict_row, connect_timeout=5,
                options="-c default_transaction_read_only=on -c statement_timeout=5000 -c lock_timeout=500") as conn:
            if conn.execute("SELECT current_user AS role").fetchone()["role"] != "bf_runtime":
                raise CollectionError("runtime_database_role_required")
            tenant = conn.execute("SELECT tenant_id FROM api_keys WHERE key_hash=%s AND NOT revoked", (digest(key.encode()),)).fetchone()
            if not tenant: raise CollectionError("tenant_binding_failed")
            return conn.execute("""SELECT id,input_file_id,output_file_id,status,total_count,pending_count,running_count,
                completed_count,failed_count,cancelled_count,prompt_tokens,completion_tokens,metadata FROM batches
                WHERE tenant_id=%s AND metadata->>'sustained_job'=%s ORDER BY created_at,id""", (tenant["tenant_id"], run_id)).fetchall()
    except CollectionError: raise
    except Exception: raise CollectionError("database_read_failed") from None


def checked_batches(rows, accepted, report):
    found = {}
    by_batch = {}
    for task in report["tasks"]: by_batch.setdefault(task["batch_id"], {})[task["custom_id"]] = task
    for row in rows:
        bid = row.get("id")
        state = accepted.get(bid)
        if (not state or bid in found or row.get("status") != "completed" or row.get("input_file_id") != state["file_id"]
                or not OPAQUE.fullmatch(row.get("output_file_id") or "") or row.get("total_count") != state["tasks"]
                or row.get("completed_count") != state["tasks"]
                or any(row.get(key) != 0 for key in ("pending_count", "running_count", "failed_count", "cancelled_count"))
                or row.get("metadata", {}).get("sustained_index") != str(state["index"])
                or not re.fullmatch("[0-9a-f]{64}", row.get("metadata", {}).get("sustained_input_sha256", ""))):
            raise CollectionError("database_cohort_mismatch")
        tasks = by_batch.get(bid, {})
        metadata_sha = row["metadata"]["sustained_input_sha256"]
        key_hash = digest(f"{report['job_id']}:{state['index']}:{metadata_sha}".encode())[:32]
        if not re.fullmatch(r"bf1\.[0-9]{10}\." + key_hash, state.get("idempotency_key", "")):
            raise CollectionError("database_workload_hash_mismatch")
        if (len(tasks) != state["tasks"] or sum(task["prompt_tokens"] for task in tasks.values()) != row["prompt_tokens"]
                or sum(task["completion_tokens"] for task in tasks.values()) != row["completion_tokens"]):
            raise CollectionError("database_metering_mismatch")
        found[bid] = {"index": state["index"], "batch_id": bid, "file_id": row["output_file_id"], "tasks": tasks}
    if set(found) != set(accepted): raise CollectionError("database_cohort_mismatch")
    return sorted(found.values(), key=lambda row: row["index"])


def verify_output(raw, expected):
    if not 0 < len(raw) <= MAX_FILE or not raw.endswith(b"\n"):
        raise CollectionError("output_size_or_framing_invalid")
    seen, prompt, completion = set(), 0, 0
    for line in raw.split(b"\n")[:-1]:
        if not line.strip(): raise CollectionError("invalid_jsonl")
        row = strict_json(line)
        identifier = row.get("custom_id") if isinstance(row, dict) else None
        task = expected.get(identifier)
        response = row.get("response") if isinstance(row, dict) else None
        if (not task or identifier in seen or row.get("error") is not None or not isinstance(response, dict)
                or response.get("status_code") != 200 or not isinstance(response.get("body"), dict)):
            raise CollectionError("output_identity_mismatch")
        usage = response["body"].get("usage")
        if (not isinstance(usage, dict) or type(usage.get("prompt_tokens")) is not int
                or type(usage.get("completion_tokens")) is not int or usage["prompt_tokens"] != task["prompt_tokens"]
                or usage["completion_tokens"] != task["completion_tokens"]):
            raise CollectionError("output_usage_mismatch")
        seen.add(identifier); prompt += usage["prompt_tokens"]; completion += usage["completion_tokens"]
    if seen != set(expected): raise CollectionError("output_count_mismatch")
    return {"sha256": digest(raw), "bytes": len(raw), "count": len(seen), "prompt_tokens": prompt, "completion_tokens": completion}


def local_url(url):
    parsed = urlsplit(url)
    if (parsed.scheme != "http" or parsed.hostname not in {"localhost", "127.0.0.1", "::1"}
            or parsed.username or parsed.password or parsed.path not in {"", "/"} or parsed.query or parsed.fragment):
        raise CollectionError("local_api_required")
    return url.rstrip("/")


def collect(args, *, transport=None, database_reader=database_batches):
    url = local_url(args.url)
    with lock_file(args.checkpoint.parent / "controller.lock"):
        checkpoint, report, accepted = terminal_binding(args)
        env_raw = private_read(args.env_file)
        env = environment(env_raw)
        batches = checked_batches(database_reader(env["DATABASE_URL"], env["BATCHFORGE_API_KEY"], args.run_id), accepted, report)
        root = args.output_dir
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        info = root.lstat()
        if (root.is_symlink() or ".runtime" not in root.resolve().parts or not stat.S_ISDIR(info.st_mode)
                or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) & 0o077):
            raise CollectionError("private_output_directory_required")
        with lock_file(root / "collector.lock", create=True):
            allowed = re.compile(r"(?:collector\.lock|collection\.json(?:\.[0-9a-f]{16}\.tmp)?|batch-[0-9]{6}-output\.jsonl|batch-[0-9]{6}-[0-9a-f]{16}\.part)\Z")
            storage_used, data_inodes = 0, set()
            for entry in root.iterdir():
                if not allowed.fullmatch(entry.name) or entry.is_symlink():
                    raise CollectionError("unknown_collection_artifact")
                info = entry.lstat()
                if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) & 0o077:
                    raise CollectionError("unsafe_collection_artifact")
                if entry.suffix in {".part", ".jsonl"} and (info.st_dev, info.st_ino) not in data_inodes:
                    data_inodes.add((info.st_dev, info.st_ino)); storage_used += info.st_size
            if storage_used > MAX_TOTAL: raise CollectionError("private_artifact_size_limit")
            binding = {"run_id": args.run_id, "checkpoint_sha256": args.checkpoint_sha256, "report_sha256": args.report_sha256,
                "expected_completed": args.expected_completed, "environment_sha256": digest(env_raw), "url": url,
                "collector_source_sha256": digest(Path(__file__).read_bytes()),
                "batch_files": [{"index": item["index"], "batch_id": item["batch_id"], "file_id": item["file_id"]} for item in batches]}
            marker = root / "collection.json"
            state = strict_json(private_read(marker)) if marker.exists() else {"binding": binding, "files": {}, "phase": "collecting"}
            if state.get("binding") != binding: raise CollectionError("collection_binding_changed")
            if (state.get("phase") not in {"collecting", "complete"} or not isinstance(state.get("files"), dict)
                    or not set(state["files"]).issubset({str(item["index"]) for item in batches})):
                raise CollectionError("unknown_collection_state")
            atomic_state(marker, state)
            total = 0
            with httpx.Client(base_url=url, headers={"Authorization": "Bearer " + env["BATCHFORGE_API_KEY"]},
                    timeout=120, trust_env=False, follow_redirects=False, transport=transport) as client:
                for batch in batches:
                    index = str(batch["index"])
                    final = root / f"batch-{batch['index']:06d}-output.jsonl"
                    previous = state["files"].get(index, {})
                    if previous and previous.get("stage") not in {"complete", "download_verified", "downloading", "incomplete"}:
                        raise CollectionError("unknown_collection_stage")
                    if final.exists() or final.is_symlink():
                        proof = verify_output(private_read(final), batch["tasks"])
                        if previous.get("stage") == "complete" and any(previous.get(key) != value for key, value in proof.items()):
                            raise CollectionError("completed_output_changed")
                        if previous.get("stage") not in {"complete", "download_verified", "incomplete"}:
                            raise CollectionError("unexpected_final_output")
                        if previous.get("stage") in {"download_verified", "incomplete"} and any(previous.get(key) != value for key, value in proof.items()):
                            raise CollectionError("verified_output_changed")
                        state["files"][index] = {**previous, **proof, "stage": "complete", "filename": final.name}
                    else:
                        if previous.get("stage") == "complete": raise CollectionError("completed_output_missing")
                        partial = root / f"batch-{batch['index']:06d}-{secrets.token_hex(8)}.part"
                        attempts = list(previous.get("attempts", []))
                        if previous:
                            attempts.append({"stage": previous.get("stage"), "partial": previous.get("partial"), "error_code": previous.get("error_code")})
                        state["files"][index] = {"stage": "downloading", "partial": partial.name, "attempts": attempts}
                        atomic_state(marker, state)
                        try:
                            descriptor = os.open(partial, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
                            with os.fdopen(descriptor, "wb") as handle:
                                size = 0
                                with client.stream("GET", f"/v1/files/{batch['file_id']}/content") as response:
                                    if response.status_code != 200: raise CollectionError("output_get_rejected")
                                    for chunk in response.iter_bytes(65536):
                                        size += len(chunk)
                                        if size > MAX_FILE or storage_used + size > MAX_TOTAL: raise CollectionError("output_size_limit")
                                        handle.write(chunk)
                                handle.flush(); os.fsync(handle.fileno())
                            proof = verify_output(private_read(partial), batch["tasks"])
                            storage_used += proof["bytes"]
                            state["files"][index].update(proof, stage="download_verified")
                            atomic_state(marker, state)
                            os.link(partial, final, follow_symlinks=False)
                            sync_directory(root)
                            state["files"][index].update(stage="complete", filename=final.name)
                        except Exception as error:
                            state["files"][index].update(stage="incomplete", error_code=error.code if isinstance(error, CollectionError) else "output_collection_failed")
                            atomic_state(marker, state)
                            raise CollectionError(state["files"][index]["error_code"]) from None
                    total += state["files"][index]["bytes"]
                    if total > MAX_TOTAL: raise CollectionError("total_output_size_limit")
                    atomic_state(marker, state)
            if digest(private_read(args.checkpoint)) != args.checkpoint_sha256 or digest(private_read(args.report)) != args.report_sha256:
                raise CollectionError("bound_source_changed_during_collection")
            state.update(phase="complete", raw_output_bytes=total, completed_count=args.expected_completed,
                original_report_passed=report["passed"], partial_plan=args.expected_completed != report["configuration"]["tasks"],
                business_state_modified=False, get_reader_leases_expected=True)
            atomic_state(marker, state)
            return state


def main():
    class SafeArgumentParser(argparse.ArgumentParser):
        def error(self, _message):
            print(json.dumps({"ok": False, "error_code": "invalid_arguments"}))
            raise SystemExit(2)
    parser = SafeArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--checkpoint", type=Path, required=True)
    parser.add_argument("--checkpoint-sha256", required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--report-sha256", required=True)
    parser.add_argument("--expected-completed", type=int, required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--url", default="http://127.0.0.1:8080")
    parser.add_argument("--require-full-plan", action="store_true")
    args = parser.parse_args()
    try:
        if sys.platform != "linux":
            raise CollectionError("same_pod_linux_execution_required")
        state = collect(args)
        print(json.dumps(state, sort_keys=True, separators=(",", ":"), allow_nan=False))
    except Exception as error:
        print(json.dumps({"ok": False, "error_code": error.code if isinstance(error, CollectionError) else "collection_failed"}))
        raise SystemExit(1) from None


if __name__ == "__main__": main()
