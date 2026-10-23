#!/usr/bin/env python3
"""Submit real JSONL batches and write observed results; never estimates throughput."""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import time
import uuid

import httpx


async def run(args) -> dict:
    key = os.environ.get("BATCHFORGE_API_KEY", "")
    if not key:
        raise SystemExit("BATCHFORGE_API_KEY is required (never pass secrets on the command line)")
    run_id = "bench-" + uuid.uuid4().hex[:12]
    tasks_per_batch = [args.tasks // args.batches + (i < args.tasks % args.batches) for i in range(args.batches)]
    headers = {"Authorization": f"Bearer {key}"}
    submitted_ids: set[str] = set()
    started = time.monotonic()
    async with httpx.AsyncClient(base_url=args.url.rstrip("/"), headers=headers, timeout=60) as client:
        async def submit(index: int, count: int) -> dict:
            rows = []
            for i in range(count):
                custom_id = f"{run_id}-{index}-{i}"
                submitted_ids.add(custom_id)
                prompt = f"Request {custom_id}. " + args.prompt
                rows.append(json.dumps({"custom_id": custom_id, "method": "POST", "url": "/v1/chat/completions",
                    "body": {"model": args.model, "messages": [{"role": "user", "content": prompt}],
                             "max_tokens": args.max_tokens, "temperature": 0}}, ensure_ascii=False))
            data = ("\n".join(rows) + "\n").encode()
            upload = await client.post("/v1/files", data={"purpose": "batch"}, files={"file": (f"{run_id}-{index}.jsonl", data, "application/jsonl")})
            upload.raise_for_status()
            create = await client.post("/v1/batches", json={"input_file_id": upload.json()["id"], "endpoint": "/v1/chat/completions",
                "completion_window": "24h", "metadata": {"benchmark_run": run_id, "engine_label": args.engine_label}},
                headers={"Idempotency-Key": f"{run_id}-{index}"})
            create.raise_for_status()
            return create.json()

        batches = await asyncio.gather(*(submit(i, n) for i, n in enumerate(tasks_per_batch)))
        remaining = {batch["id"] for batch in batches}
        final: dict[str, dict] = {}
        deadline = time.monotonic() + args.deadline
        while remaining and time.monotonic() < deadline:
            for batch_id in list(remaining):
                result = await client.get(f"/v1/batches/{batch_id}")
                result.raise_for_status()
                batch = result.json()
                if batch["status"] in {"completed", "failed", "cancelled"}:
                    final[batch_id] = batch
                    remaining.remove(batch_id)
            if remaining:
                await asyncio.sleep(args.poll)
        elapsed = time.monotonic() - started
        completed = failed = cancelled = prompt_tokens = completion_tokens = 0
        output_ids: list[str] = []
        simulated = False
        for batch in final.values():
            counts = batch["request_counts"]
            completed += counts["completed"]
            failed += counts["failed"]
            cancelled += counts["cancelled"]
            if batch.get("output_file_id"):
                content = await client.get(f"/v1/files/{batch['output_file_id']}/content")
                content.raise_for_status()
                for line in content.text.splitlines():
                    row = json.loads(line)
                    output_ids.append(row["custom_id"])
                    body = row["response"]["body"]
                    simulated |= bool(body.get("batchforge_test_only"))
                    prompt_tokens += body["usage"]["prompt_tokens"]
                    completion_tokens += body["usage"]["completion_tokens"]
        unique = len(output_ids) == len(set(output_ids))
        known = set(output_ids) <= submitted_ids
        report = {
            "schema_version": 1, "run_id": run_id, "observed_at": datetime.now(timezone.utc).isoformat(),
            "measurement": "observed end-to-end; includes upload and queue wait",
            "engine_label": args.engine_label, "test_fixture_detected": simulated,
            "model": args.model, "submitted_tasks": args.tasks, "batches": args.batches,
            "requested_max_output_tokens": args.max_tokens, "elapsed_seconds": round(elapsed, 3),
            "completed": completed, "failed": failed, "cancelled": cancelled,
            "unfinished_batch_ids": sorted(remaining), "completed_tasks_per_second": round(completed / elapsed, 3),
            "observed_prompt_tokens": prompt_tokens, "observed_completion_tokens": completion_tokens,
            "output_custom_ids_unique": unique, "output_custom_ids_known": known,
            "output_rows_match_completed": len(output_ids) == completed,
            "batch_ids": [batch["id"] for batch in batches],
            "limitations": ["Not a daily capacity forecast", "Token lengths are measured from outputs, not guaranteed by the input prompt", "Does not measure per-task latency or quality"],
        }
        if not unique or not known or len(output_ids) != completed:
            raise RuntimeError("Result integrity check failed: " + json.dumps(report))
        return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    parser.add_argument("--model", default=os.environ.get("MODEL_NAME", "Qwen/Qwen2.5-0.5B-Instruct"))
    parser.add_argument("--tasks", type=int, default=100)
    parser.add_argument("--batches", type=int, default=4)
    parser.add_argument("--max-tokens", type=int, default=64)
    parser.add_argument("--prompt", default="Return a concise JSON object with label=positive for this review: the product works well.")
    parser.add_argument("--engine-label", required=True, help="Describe actual engine/hardware; use test-only for fake engine")
    parser.add_argument("--deadline", type=float, default=600)
    parser.add_argument("--poll", type=float, default=.5)
    parser.add_argument("--report", type=Path, default=Path(".runtime/benchmark.json"))
    args = parser.parse_args()
    if not 1 <= args.batches <= args.tasks or args.max_tokens <= 0 or min(args.deadline, args.poll) <= 0:
        parser.error("Require 1 <= batches <= tasks and positive timeout/token values")
    report = asyncio.run(run(args))
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if report["unfinished_batch_ids"] or report["failed"] or report["cancelled"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
