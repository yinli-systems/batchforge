#!/usr/bin/env python3
"""Destructive TEST-ONLY protocol probe for an EMPTY isolated control plane.

Exercises cancellation fences and lease recovery without running a model.
Every completion is explicitly marked test-only. Never use a customer database.
"""
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
    public_key = os.environ.get("BATCHFORGE_API_KEY")
    worker_key = os.environ.get("BATCHFORGE_WORKER_TOKEN")
    if not public_key or not worker_key:
        raise SystemExit("Both API and worker tokens must be set in the environment")
    run_id = "failure-test-" + uuid.uuid4().hex[:12]
    started = time.monotonic()
    async with httpx.AsyncClient(base_url=args.url.rstrip("/"), timeout=15,
                                headers={"Authorization": f"Bearer {public_key}"}) as public, \
               httpx.AsyncClient(base_url=args.url.rstrip("/"), timeout=15,
                                headers={"Authorization": f"Bearer {worker_key}"}) as internal:
        overview = await public.get("/v1/overview")
        overview.raise_for_status()
        if overview.json()["stats"]["total_batches"] != 0 or overview.json()["workers"]:
            raise SystemExit("Refusing: probe requires an EMPTY database with NO registered workers")

        async def post(client, path, body, expected=200):
            result = await client.post(path, json=body)
            if result.status_code != expected:
                raise RuntimeError(f"{path}: expected HTTP {expected}, got {result.status_code}")
            return result.json()

        worker_id = run_id
        registration = await post(internal, "/internal/workers/register", {
            "worker_id": worker_id, "name": "TEST ONLY failure probe", "model": args.model,
            "concurrency": 1, "metadata": {"test_only": "true"}})
        lease_seconds = registration["lease_seconds"]
        if lease_seconds + 10 > args.deadline:
            raise SystemExit("Probe deadline must exceed server lease duration by at least 10 seconds")

        async def batch(custom_id):
            row = {"custom_id": custom_id, "method": "POST", "url": "/v1/chat/completions", "body": {
                "model": args.model, "messages": [{"role": "user", "content": "TEST ONLY protocol probe"}], "max_tokens": 8}}
            upload = await public.post("/v1/files", data={"purpose": "batch"}, files={"file": ("probe.jsonl", json.dumps(row) + "\n", "application/jsonl")})
            upload.raise_for_status()
            return await post(public, "/v1/batches", {"input_file_id": upload.json()["id"], "endpoint": "/v1/chat/completions", "completion_window": "24h", "metadata": {"test_only": "true"}})

        async def claim(batch_id):
            result = await post(internal, "/internal/tasks/claim", {"worker_id": worker_id, "model": args.model, "limit": 1})
            if not result["tasks"]:
                return None
            task = result["tasks"][0]
            if task["batch_id"] != batch_id:
                raise RuntimeError("Unexpected task in isolated probe database")
            return task

        def identity(task):
            return {"worker_id": worker_id, "attempt_token": task["attempt_token"]}

        def completion(task):
            return {**identity(task), "response": {"id": "test-only", "object": "chat.completion", "model": args.model,
                "choices": [{"index": 0, "message": {"role": "assistant", "content": "TEST ONLY"}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": 4, "completion_tokens": 2, "total_tokens": 6}, "batchforge_test_only": True}}

        cancelled = await batch(run_id + "-cancel")
        attempt = await claim(cancelled["id"])
        if not attempt:
            raise RuntimeError("Probe task was not claimable")
        await post(public, f"/v1/batches/{cancelled['id']}/cancel", {})
        await post(internal, f"/internal/tasks/{attempt['id']}/heartbeat", identity(attempt), 409)
        await post(internal, f"/internal/tasks/{attempt['id']}/complete", completion(attempt), 409)

        recovered = await batch(run_id + "-recover")
        old = await claim(recovered["id"])
        if not old:
            raise RuntimeError("Recovery task was not claimable")
        deadline = time.monotonic() + args.deadline
        fresh = None
        while time.monotonic() < deadline:
            await asyncio.sleep(.5)
            fresh = await claim(recovered["id"])
            if fresh:
                break
        if fresh is None or fresh["attempt_token"] == old["attempt_token"]:
            raise RuntimeError("Lease did not recover to a fresh fenced attempt")
        await post(internal, f"/internal/tasks/{old['id']}/complete", completion(old), 409)
        await post(internal, f"/internal/tasks/{fresh['id']}/complete", completion(fresh))
        replay = await post(internal, f"/internal/tasks/{fresh['id']}/complete", completion(fresh))
        result = await public.get(f"/v1/batches/{recovered['id']}")
        result.raise_for_status()
        terminal = result.json()
        if terminal["status"] != "completed" or terminal["request_counts"]["completed"] != 1:
            raise RuntimeError("Recovery task did not finish exactly once in durable state")
        output = await public.get(f"/v1/files/{terminal['output_file_id']}/content")
        output.raise_for_status()
        rows = [json.loads(line) for line in output.text.splitlines()]
        if len(rows) != 1 or rows[0]["custom_id"] != run_id + "-recover":
            raise RuntimeError("Recovery output integrity failed")
        return {"schema_version": 1, "test_only": True, "run_id": run_id,
            "observed_at": datetime.now(timezone.utc).isoformat(), "lease_seconds": lease_seconds,
            "elapsed_seconds": round(time.monotonic() - started, 3),
            "cancellation_fences_heartbeat": True, "cancellation_fences_completion": True,
            "expired_lease_reclaimed": True, "stale_completion_rejected": True,
            "accepted_completion_replay_idempotent": True, "single_durable_output_row": True,
            "replay_response": replay, "limitations": ["Protocol test with synthetic response; no model or GPU throughput measured"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    parser.add_argument("--model", default=os.environ.get("MODEL_NAME", "Qwen/Qwen2.5-0.5B-Instruct"))
    parser.add_argument("--deadline", type=float, default=180)
    parser.add_argument("--report", type=Path, default=Path(".runtime/failure-probe.json"))
    parser.add_argument("--isolated-empty-test-database", action="store_true", required=True,
                        help="Explicit acknowledgement: this writes synthetic results into an isolated test database")
    args = parser.parse_args()
    report = asyncio.run(run(args))
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
