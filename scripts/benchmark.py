#!/usr/bin/env python3
"""Submit measured HTTP batches; preserve partial failures and verify actual outputs."""
from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import time
import uuid

import httpx

TERMINAL = {"completed", "failed", "cancelled"}


def distribution(values: list[float]) -> dict:
    """Linearly interpolated percentiles, including correct zero-sample output."""
    ordered = sorted(values)

    def percentile(p: float):
        if not ordered:
            return None
        position = (len(ordered) - 1) * p
        lower, upper = math.floor(position), math.ceil(position)
        return round(ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower), 6)

    return {"samples": len(values), "p50": percentile(.5), "p95": percentile(.95), "p99": percentile(.99),
            "max": round(max(ordered), 6) if ordered else None,
            "mean": round(sum(ordered) / len(ordered), 6) if ordered else None}


@dataclass
class Submission:
    index: int
    count: int
    data: bytes
    custom_ids: set[str]
    upload_seconds: float | None = None
    create_seconds: float | None = None
    started_at: float | None = None
    create_started_at: float | None = None
    accepted_at: float | None = None
    terminal_at: float | None = None
    file_id: str | None = None
    batch: dict | None = None
    error: dict | None = None
    poll_errors: list[dict] = field(default_factory=list)


def generate_workload(args, run_id: str) -> list[Submission]:
    input_path = getattr(args, "input_jsonl", None)
    source_rows = None
    if input_path:
        source_rows = [json.loads(line) for line in input_path.read_text().splitlines() if line.strip()]
        if len(source_rows) < args.tasks:
            raise ValueError("Input workload has fewer rows than requested tasks")
        source_rows = source_rows[:args.tasks]
        if any(row.get("body", {}).get("model") != args.model for row in source_rows):
            raise ValueError("Provided workload model must match --model")
        if len({row.get("custom_id") for row in source_rows}) != len(source_rows):
            raise ValueError("Provided workload custom IDs must be unique")
    workloads = []
    offset = 0
    for index in range(args.batches):
        count = args.tasks // args.batches + (index < args.tasks % args.batches)
        rows, identifiers = [], set()
        for number in range(count):
            if source_rows is not None:
                row = dict(source_rows[offset + number])
                row["custom_id"] = run_id + "-" + row["custom_id"]
                identifiers.add(row["custom_id"])
                rows.append(json.dumps(row, ensure_ascii=False))
                continue
            custom_id = f"{run_id}-{index}-{number}"
            identifiers.add(custom_id)
            rows.append(json.dumps({"custom_id": custom_id, "method": "POST", "url": "/v1/chat/completions",
                "body": {"model": args.model, "messages": [{"role": "user", "content": f"Request {custom_id}. " + args.prompt}],
                         "max_tokens": args.max_tokens, "temperature": args.temperature}}, ensure_ascii=False))
        workloads.append(Submission(index, count, ("\n".join(rows) + "\n").encode(), identifiers))
        offset += count
    return workloads


def request_error(exc: Exception, stage: str) -> dict:
    # Keep safe diagnosis fields; full server diagnostics belong in private logs.
    status = exc.response.status_code if isinstance(exc, httpx.HTTPStatusError) else None
    return {"stage": stage, "type": type(exc).__name__, "http_status": status,
            "create_outcome_uncertain": stage == "create" and (status is None or status >= 500)}


def checked_batch(value: dict, expected_id: str | None = None) -> dict:
    if not isinstance(value, dict) or not isinstance(value.get("id"), str):
        raise ValueError("Missing batch ID")
    if expected_id is not None and value["id"] != expected_id:
        raise ValueError("Batch ID changed")
    if value.get("status") not in TERMINAL | {"in_progress"}:
        raise ValueError("Invalid batch status")
    counts = value.get("request_counts", {})
    keys = ("total", "pending", "running", "completed", "failed", "cancelled")
    if any(type(counts.get(key)) is not int or counts[key] < 0 for key in keys):
        raise ValueError("Invalid batch counts")
    if sum(counts[key] for key in keys[1:]) != counts["total"]:
        raise ValueError("Batch count conservation failed")
    return value


def content_breakdown(body: dict) -> dict:
    """Presence/shape statistics, independent of durable acceptance or quality."""
    if not isinstance(body, dict):
        raise ValueError("Output body must be an object")
    choices = body.get("choices")
    if not isinstance(choices, list) or not choices:
        raise ValueError("Output choices must be a nonempty list")
    final = reasoning = 0
    length = False
    for choice in choices:
        if not isinstance(choice, dict) or not isinstance(choice.get("message"), dict):
            raise ValueError("Output choices/messages must be objects")
        message = choice["message"]
        text = message.get("content")
        if any(message.get(key) is not None and not isinstance(message[key], str) for key in ("content", "reasoning_content", "reasoning")):
            raise ValueError("Output text or reasoning must be a string or null")
        thought = message.get("reasoning_content") or message.get("reasoning")
        final += len(text or "")
        reasoning += len(thought or "")
        length |= choice.get("finish_reason") == "length"
    return {"rows_with_valid_content_shape": 1, "final_text_present": int(final > 0),
            "reasoning_only": int(final == 0 and reasoning > 0), "no_final_text": int(final == 0),
            "length_capped": int(length), "final_text_codepoints": final, "reasoning_text_codepoints": reasoning}


async def run(args, *, transport=None) -> dict:
    key = os.environ.get("BATCHFORGE_API_KEY", "")
    if not key:
        raise SystemExit("BATCHFORGE_API_KEY is required (never pass secrets on the command line)")
    run_id = "bench-" + uuid.uuid4().hex[:12]
    generation_started = time.monotonic()
    submissions = generate_workload(args, run_id)
    digest = hashlib.sha256()
    for item in submissions:
        digest.update(item.data)
    workload_hash = digest.hexdigest()
    generation_seconds = time.monotonic() - generation_started
    if args.workload_dir:
        args.workload_dir.mkdir(parents=True, exist_ok=True)
        for item in submissions:
            (args.workload_dir / f"{run_id}-{item.index}.jsonl").write_bytes(item.data)
    monitors: list[asyncio.Task] = []
    monitor_by_index = {}
    admission_blocked = asyncio.Event()
    attempted_requests = successful_requests = 0
    measurement_started_at = datetime.now(timezone.utc).isoformat()
    started = time.monotonic()
    run_seconds=getattr(args,'run_deadline',None)
    if run_seconds is not None and (type(run_seconds) not in (int,float) or not math.isfinite(run_seconds) or run_seconds<=0):
        raise ValueError('run_deadline must be a finite positive number')
    global_deadline=started+run_seconds if run_seconds is not None else float('inf')
    headers = {"Authorization": f"Bearer {key}"}
    async with httpx.AsyncClient(base_url=args.url.rstrip("/"), headers=headers,
                                timeout=args.request_timeout, transport=transport, trust_env=False) as client:
        async def monitor(item: Submission) -> None:
            deadline = min(item.accepted_at + args.deadline,global_deadline)
            while time.monotonic() < deadline:
                try:
                    response = await client.get(f"/v1/batches/{item.batch['id']}",
                        timeout=min(args.request_timeout, max(.1, deadline - time.monotonic())))
                    response.raise_for_status()
                    item.batch = checked_batch(response.json(), item.batch["id"])
                    if item.batch["status"] in TERMINAL:
                        item.terminal_at = time.monotonic()
                        return
                except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
                    error = request_error(exc, "poll")
                    if not item.poll_errors or any(item.poll_errors[-1][key] != error[key] for key in error):
                        item.poll_errors.append({**error, "count": 1})
                    else:
                        item.poll_errors[-1]["count"] += 1
                await asyncio.sleep(min(args.poll, max(0, deadline - time.monotonic())))

        async def submit(item: Submission) -> None:
            nonlocal attempted_requests, successful_requests
            item.started_at = time.monotonic()
            stage = "upload"
            request_started = item.started_at
            try:
                attempted_requests += 1
                try:
                    response = await client.post("/v1/files", data={"purpose": "batch"},
                        timeout=min(args.request_timeout,max(.1,global_deadline-time.monotonic())),
                        files={"file": (f"{run_id}-{item.index}.jsonl", item.data, "application/jsonl")})
                    response.raise_for_status()
                    item.file_id = response.json()["id"]
                    successful_requests += 1
                finally:
                    item.upload_seconds = time.monotonic() - request_started
                stage = "create"
                if time.monotonic()>=global_deadline:
                    item.error={'stage':'create','type':'RunDeadlineExceeded','http_status':None,'create_outcome_uncertain':False}
                    return
                request_started = item.create_started_at = time.monotonic()
                attempted_requests += 1
                try:
                    response = await client.post("/v1/batches", timeout=min(args.request_timeout,max(.1,global_deadline-time.monotonic())),json={"input_file_id": item.file_id,
                        "endpoint": "/v1/chat/completions", "completion_window": "24h",
                        "metadata": {**getattr(args,'metadata',{}), "benchmark_run": run_id, "engine_label": args.engine_label}},
                        headers={"Idempotency-Key": f"{run_id}-{item.index}"})
                    response.raise_for_status()
                    item.batch = checked_batch(response.json())
                    successful_requests += 1
                    item.accepted_at = time.monotonic()
                finally:
                    item.create_seconds = time.monotonic() - request_started
                # Observe each batch immediately: one slow create must not
                # inflate all successful batches' measured turnaround.
                task=asyncio.create_task(monitor(item));monitors.append(task);monitor_by_index[item.index]=task
            except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
                item.error = request_error(exc, stage)

        semaphore = asyncio.Semaphore(getattr(args, "submission_concurrency", 8))
        window_size=getattr(args,'max_inflight_batches',None)
        if window_size is not None and (type(window_size) is not int or window_size<1):
            raise ValueError('max_inflight_batches must be a positive integer')
        window=asyncio.Semaphore(window_size) if window_size is not None else None
        async def submit_if_within_budget(item):
            async with semaphore:
                if admission_blocked.is_set():
                    item.error={'stage':'admission','type':'UnresolvedBatchAwaitingRecovery','http_status':None,'create_outcome_uncertain':False}
                    return
                if time.monotonic()>=global_deadline:
                    item.error={'stage':'admission','type':'RunDeadlineExceeded','http_status':None,'create_outcome_uncertain':False}
                    return
                await submit(item)
        async def bounded_submit(item):
            if window is None:
                await submit_if_within_budget(item)
            else:
                async with window:
                    await submit_if_within_budget(item)
                    if item.index in monitor_by_index:
                        await monitor_by_index[item.index]
                        if item.terminal_at is None:admission_blocked.set()
                    elif item.error and item.error.get('create_outcome_uncertain'):
                        # An ambiguous create may still own server-side capacity.
                        # Do not spend another lifecycle credit by guessing.
                        admission_blocked.set()
        await asyncio.gather(*(bounded_submit(item) for item in submissions))
        submission_elapsed = time.monotonic() - started
        if monitors:
            await asyncio.gather(*monitors)
        elapsed = time.monotonic() - started
        terminal_observation_ended_at = datetime.now(timezone.utc).isoformat()
        validation_started = time.monotonic()
        completed = failed = cancelled = prompt_tokens = completion_tokens = 0
        output_ids: list[str] = []
        simulated = False
        validation_errors = []
        prompt_lengths, output_lengths, finish_reasons = [], [], {}
        output_content = {key: 0 for key in ("rows_with_valid_content_shape", "final_text_present", "reasoning_only",
            "no_final_text", "length_capped", "final_text_codepoints", "reasoning_text_codepoints")}
        output_dir = getattr(args, "output_dir", None)
        if output_dir:
            output_dir.mkdir(parents=True, exist_ok=True)
        for item in submissions:
            if item.terminal_at is None:
                continue
            batch = item.batch
            counts = batch["request_counts"]
            completed += counts["completed"]
            failed += counts["failed"]
            cancelled += counts["cancelled"]
            if batch.get("output_file_id"):
                try:
                    content = await client.get(f"/v1/files/{batch['output_file_id']}/content")
                    content.raise_for_status()
                    if output_dir:
                        path = output_dir / f"{run_id}-{item.index}-output.jsonl"
                        path.write_bytes(content.content)
                    for line in content.text.splitlines():
                        row = json.loads(line)
                        output_ids.append(row["custom_id"])
                        body = row["response"]["body"]
                        content_stats = content_breakdown(body)
                        for key, value in content_stats.items():
                            output_content[key] += value
                        simulated |= bool(body.get("batchforge_test_only"))
                        prompt_tokens += body["usage"]["prompt_tokens"]
                        completion_tokens += body["usage"]["completion_tokens"]
                        prompt_lengths.append(body["usage"]["prompt_tokens"])
                        output_lengths.append(body["usage"]["completion_tokens"])
                        for choice in body.get("choices", []):
                            reason = choice.get("finish_reason", "unknown")
                            finish_reasons[reason] = finish_reasons.get(reason, 0) + 1
                except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
                    validation_errors.append({"batch_id": batch["id"], **request_error(exc, "output_validation")})
        validation_seconds = time.monotonic() - validation_started
    all_ids = set().union(*(item.custom_ids for item in submissions))
    unique = len(output_ids) == len(set(output_ids))
    known = set(output_ids) <= all_ids
    unfinished = [item.batch["id"] for item in submissions if item.accepted_at is not None and item.terminal_at is None]
    errors = [{"batch_index": item.index, "input_file_id": item.file_id, "idempotency_key": f"{run_id}-{item.index}", **item.error}
              for item in submissions if item.error]
    accepted = [item for item in submissions if item.accepted_at is not None]
    terminal = [item for item in submissions if item.terminal_at is not None]
    batch_counts_match = all(item.batch["request_counts"]["total"] == item.count for item in accepted)
    integrity = unique and known and len(output_ids) == completed and batch_counts_match and not validation_errors
    passed = len(accepted) == args.batches and completed == args.tasks and not unfinished and not failed and not cancelled and integrity
    return {
        "schema_version": 2, "run_id": run_id, "observed_at": datetime.now(timezone.utc).isoformat(),
        "max_inflight_batches":window_size,
        "submission_rate_scope":"Includes completion-credit waits when max_inflight_batches is set",
        "measurement_started_at": measurement_started_at,
        "terminal_observation_ended_at": terminal_observation_ended_at,
        "passed": passed, "measurement": "client observed submission through terminal observation; output validation measured separately",
        "engine_label": args.engine_label, "test_fixture_detected": simulated,
        "model": args.model, "requested_tasks": args.tasks, "submitted_tasks": sum(item.count for item in accepted),
        "batches": args.batches, "accepted_batches": len(accepted), "terminal_batches": len(terminal),
        "requested_max_output_tokens": args.max_tokens, "elapsed_seconds": round(elapsed, 6),
        "completed": completed, "failed": failed, "cancelled": cancelled,
        "unfinished_batch_ids": sorted(unfinished), "submission_errors": errors,
        "output_validation_errors": validation_errors, "completed_tasks_per_second": round(completed / elapsed, 6),
        "observed_prompt_tokens": prompt_tokens, "observed_completion_tokens": completion_tokens,
        "prompt_tokens_per_task": distribution(prompt_lengths), "completion_tokens_per_task": distribution(output_lengths),
        "output_finish_reasons": finish_reasons,
        "output_content": {**output_content,
            "population": "Durable output rows with valid choice/message shapes; length_capped means any choice ended with length",
            "quality_evaluated": False, "durable_completion_distinct_from_final_answer": True},
        "output_custom_ids_unique": unique, "output_custom_ids_known": known,
        "output_rows_match_completed": len(output_ids) == completed,
        "batch_counts_match_submission": batch_counts_match,
        "batch_ids": [item.batch["id"] for item in accepted],
        "workload": {"sha256": workload_hash, "hash_definition": "concatenated exact JSONL bytes in batch_index order",
            "provided_source_sha256": hashlib.sha256(args.input_jsonl.read_bytes()).hexdigest() if getattr(args,"input_jsonl",None) else None,
            "bytes": sum(len(item.data) for item in submissions), "generation_seconds": round(generation_seconds, 6),
            "model": args.model, "prompt": None if getattr(args,"input_jsonl",None) else args.prompt,
            "generation_mode": "provided-jsonl" if getattr(args,"input_jsonl",None) else "generated-prompt",
            "message_template": "exact provided messages; run_id prepended to custom_id" if getattr(args,"input_jsonl",None) else "Request {custom_id}. {prompt}",
            "max_tokens": args.max_tokens, "temperature": args.temperature,
            "tasks_per_batch": [item.count for item in submissions]},
        "submission": {"elapsed_seconds": round(submission_elapsed, 6),
            "attempted_http_requests": attempted_requests, "successful_http_requests": successful_requests,
            "requests_per_second": round(attempted_requests / submission_elapsed, 6),
            "successful_requests_per_second": round(successful_requests / submission_elapsed, 6),
            "accepted_batches_per_second": round(len(accepted) / submission_elapsed, 6),
            "accepted_tasks_per_second": round(sum(item.count for item in accepted) / submission_elapsed, 6),
            "upload_latency_seconds": distribution([item.upload_seconds for item in submissions if item.upload_seconds is not None]),
            "create_latency_seconds": distribution([item.create_seconds for item in submissions if item.create_seconds is not None]),
            "latency_population": "all attempted requests, including failures",
            "request_population": "POST /v1/files and POST /v1/batches only; excludes polling and downloads"},
        "batch_turnaround_seconds": distribution([item.terminal_at - item.create_started_at for item in terminal]),
        "batch_after_acceptance_seconds": distribution([item.terminal_at - item.accepted_at for item in terminal]),
        "batch_upload_to_terminal_seconds": distribution([item.terminal_at - item.started_at for item in terminal]),
        "percentile_method": "linear interpolation at (n-1)*p",
        "output_validation_seconds": round(validation_seconds, 6),
        "poll_interval_seconds": args.poll, "per_batch_deadline_seconds": args.deadline,
        "batch_observations": [{"index": item.index, "tasks": item.count,
            "batch_id": item.batch["id"] if item.accepted_at is not None else None,
            "status": item.batch.get("status") if item.batch else None,
            "upload_seconds": round(item.upload_seconds, 6) if item.upload_seconds is not None else None,
            "create_seconds": round(item.create_seconds, 6) if item.create_seconds is not None else None,
            "turnaround_seconds": round(item.terminal_at - item.create_started_at, 6) if item.terminal_at else None,
            "request_counts": item.batch.get("request_counts") if item.batch else None,
            "poll_errors": item.poll_errors} for item in submissions],
        "limitations": ["Not a daily capacity forecast", "Token lengths are measured from outputs, not guaranteed by the prompt",
            "Batch turnaround starts before POST /v1/batches and ends at first client-observed terminal response; polling adds observation delay",
            "No per-task inference latency or quality evaluation; final text presence is counted separately from durable completion",
            "Reported generated tokens can include reasoning and control tokens; reasoning-only length-capped outputs remain durable completions",
            "Accepted counts require a received create acknowledgement; uncertain create errors may have committed server-side"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    parser.add_argument("--model", default=os.environ.get("MODEL_NAME", "Qwen/Qwen2.5-0.5B-Instruct"))
    parser.add_argument("--tasks", type=int, default=100)
    parser.add_argument("--batches", type=int, default=4)
    parser.add_argument("--max-tokens", type=int, default=64)
    parser.add_argument("--temperature", type=float, default=0)
    parser.add_argument("--prompt", default="Return a concise JSON object with label=positive for this review: the product works well.")
    parser.add_argument("--engine-label", required=True, help="Describe actual engine/hardware; use test-only for fake engine")
    parser.add_argument("--deadline", type=float, default=600, help="Seconds to observe each acknowledged batch")
    parser.add_argument("--request-timeout", type=float, default=60)
    parser.add_argument("--poll", type=float, default=.5)
    parser.add_argument("--workload-dir", type=Path, help="Optionally save exact generated JSONL for the report SHA256")
    parser.add_argument("--input-jsonl", type=Path, help="Replay a prepared real dataset workload; must match --model and contain at least --tasks rows")
    parser.add_argument("--output-dir", type=Path, help="Save downloaded outputs privately for independent quality evaluation")
    parser.add_argument("--submission-concurrency", type=int, default=8)
    parser.add_argument("--report", type=Path, default=Path(".runtime/benchmark.json"))
    args = parser.parse_args()
    if not 1 <= args.batches <= args.tasks or args.max_tokens <= 0 or args.submission_concurrency < 1:
        parser.error("Require 1 <= batches <= tasks and positive max tokens")
    if not all(math.isfinite(value) and value > 0 for value in (args.deadline, args.poll, args.request_timeout)):
        parser.error("Timeouts and poll interval must be finite and positive")
    if not math.isfinite(args.temperature) or not 0 <= args.temperature <= 2:
        parser.error("Temperature must be 0 to 2")
    report = asyncio.run(run(args))
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if not report["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
