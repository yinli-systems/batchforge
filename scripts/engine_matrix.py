"""Measure real streaming inference and bounded client dispatch policies.

This is an engine-only experiment, separate from durable BatchForge completion.
The same offered burst can compare FIFO, shortest requested work, and class fair
dispatch. It does not change or claim to measure vLLM's internal scheduler.
"""
from __future__ import annotations

import argparse
import asyncio
from collections import deque
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import time

import httpx

from scripts.benchmark import distribution


def dispatch_order(rows, examples, policy):
    if policy == "fifo":
        return list(rows)
    if policy == "shortest":
        # Cost proxy is explicit, deterministic and never an observed runtime.
        return sorted(rows, key=lambda r: (examples[r["custom_id"]]["rendered_prompt_tokens"] +
            16 * examples[r["custom_id"]]["output_token_cap"]))
    if policy != "class-fair":
        raise ValueError("Unknown dispatch policy")
    queues = {}
    for row in rows:
        queues.setdefault(examples[row["custom_id"]]["class"], deque()).append(row)
    result = []
    while any(queues.values()):
        for key in sorted(queues):
            if queues[key]:
                result.append(queues[key].popleft())
    return result


async def sse_events(response):
    """SSE event framing: TCP chunks and data lines are not token boundaries."""
    data = []
    async for line in response.aiter_lines():
        if not line:
            if data:
                yield "\n".join(data)
                data = []
        elif line.startswith("data:"):
            data.append(line[5:].removeprefix(" "))
    if data:
        yield "\n".join(data)


async def invoke(client, row, example, *, offered, dispatch, force_length, request_timeout):
    start = time.monotonic()
    result = {"custom_id": row["custom_id"], **example, "status": "error",
        "dispatch_index": dispatch, "queue_seconds": start-offered,
        "dispatch_started_at": datetime.now(timezone.utc).isoformat()}
    body = {**row["body"], "stream": True, "stream_options": {"include_usage": True}}
    if force_length:
        # Engine-only vLLM controls. Never inserted into tenant API workloads.
        body.update(ignore_eos=True, min_tokens=body["max_tokens"])
    first = last = first_final = None
    final_codepoints = reasoning_codepoints = 0
    emitted_events = 0
    usage = None
    reason = None
    done = False
    response_hash = hashlib.sha256()
    try:
        async with asyncio.timeout(request_timeout), client.stream("POST", "/v1/chat/completions", json=body) as response:
            result["http_status"] = response.status_code
            response.raise_for_status()
            async for raw in sse_events(response):
                if raw == "[DONE]":
                    done = True
                    break
                event = json.loads(raw)
                if event.get("error"):
                    raise ValueError("Engine emitted a stream error")
                if event.get("usage"):
                    if usage is not None:
                        raise ValueError("Multiple nonempty usage events")
                    usage = event["usage"]
                for choice in event.get("choices", []):
                    if choice.get("index", 0) != 0:
                        raise ValueError("Unexpected multiple choices")
                    delta = choice.get("delta", {})
                    content = delta.get("content") or ""
                    reasoning = delta.get("reasoning_content") or delta.get("reasoning") or ""
                    if not isinstance(content, str) or not isinstance(reasoning, str):
                        raise ValueError("Unexpected non-text delta")
                    # Some servers expose both reasoning aliases. Count one
                    # text field, rather than doubling codepoints or hashes.
                    if delta.get("reasoning_content") and delta.get("reasoning") and delta["reasoning_content"] != delta["reasoning"]:
                        raise ValueError("Conflicting reasoning aliases")
                    text = content + reasoning
                    if text:
                        current = time.monotonic()
                        first = first if first is not None else current
                        last = current
                        if content and first_final is None:
                            first_final = current
                        final_codepoints += len(content)
                        reasoning_codepoints += len(reasoning)
                        emitted_events += 1
                        response_hash.update(text.encode())
                    if choice.get("finish_reason") is not None:
                        reason = choice["finish_reason"]
        if not done or usage is None or reason is None:
            raise ValueError("Incomplete stream or missing token usage/finish reason")
        for key in ("prompt_tokens", "completion_tokens"):
            if type(usage.get(key)) is not int or usage[key] < 0:
                raise ValueError("Invalid usage counters")
        result.update(reported_prompt_tokens=usage["prompt_tokens"], reported_completion_tokens=usage["completion_tokens"])
        if usage["prompt_tokens"] != example["rendered_prompt_tokens"]:
            raise ValueError("Server prompt count differs from pinned tokenizer manifest")
        if usage["completion_tokens"] > body["max_tokens"]:
            raise ValueError("Output exceeded requested token cap")
        if force_length and usage["completion_tokens"] != body["max_tokens"]:
            raise ValueError("Controlled output length not achieved")
        result.update(status="completed", prompt_tokens=usage["prompt_tokens"],
            completion_tokens=usage["completion_tokens"], finish_reason=reason,
            ttft_seconds=first-start if first is not None else None,
            stream_span_per_reported_output_token_proxy_seconds=(last-first)/(usage["completion_tokens"]-1)
                if first is not None and usage["completion_tokens"] > 1 else None,
            emitted_text_events=emitted_events, response_text_sha256=response_hash.hexdigest())
    except (httpx.HTTPError, ValueError, TypeError, KeyError, TimeoutError) as exc:
        # No response bodies/headers or arbitrary engine exception strings.
        result.update(error_type=type(exc).__name__)
    result.update(inference_success=result["status"] == "completed", final_text_present=final_codepoints > 0,
        first_final_content_seconds=first_final-start if first_final is not None else None,
        final_text_codepoints=final_codepoints, reasoning_text_codepoints=reasoning_codepoints,
        visible_text_codepoints=final_codepoints+reasoning_codepoints,
        service_seconds=time.monotonic()-start, completion_seconds=time.monotonic()-offered)
    return result


def summarize(records, elapsed, *, hourly_cost=None):
    complete = [r for r in records if r["status"] == "completed"]
    def stats(key):
        return distribution([r[key] for r in complete if r.get(key) is not None])
    inputs = sum(r["prompt_tokens"] for r in complete)
    outputs = sum(r["completion_tokens"] for r in complete)
    result = {"offered": len(records), "completed": len(complete), "errors": len(records)-len(complete),
        "elapsed_seconds": elapsed, "successful_inferences_per_second": len(complete)/elapsed,
        "final_text_present": sum(bool(r.get("final_text_present")) for r in complete),
        "no_final_text": sum(not r.get("final_text_present", False) for r in complete),
        "prompt_tokens": inputs, "generated_tokens": outputs,
        "prompt_tokens_per_second": inputs/elapsed, "generated_tokens_per_second": outputs/elapsed,
        "completion_seconds": stats("completion_seconds"), "queue_seconds": stats("queue_seconds"),
        "http_ttft_seconds": stats("ttft_seconds"), "first_final_content_seconds": stats("first_final_content_seconds"),
        "stream_span_per_reported_output_token_proxy_seconds": stats("stream_span_per_reported_output_token_proxy_seconds"),
        "visible_text_codepoints": sum(r.get("visible_text_codepoints", 0) for r in complete),
        "final_text_codepoints": sum(r.get("final_text_codepoints", 0) for r in complete),
        "reasoning_text_codepoints": sum(r.get("reasoning_text_codepoints", 0) for r in complete),
        "http_service_seconds": stats("service_seconds"),
        "all_terminal_completion_seconds": distribution([r["completion_seconds"] for r in records]),
        "cost": None}
    if hourly_cost is not None:
        cost = hourly_cost * elapsed / 3600
        result["cost"] = {"supplied_allocation_usd_per_hour": hourly_cost, "window_usd": cost,
            "usd_per_million_generated_tokens": cost*1e6/outputs if outputs else None,
            "usd_per_million_successful_inferences": cost*1e6/len(complete) if complete else None,
            "scope": "User supplied allocation price; includes idle/failed work during this experiment only"}
    return result


async def run(args, *, transport=None, event_sink=None):
    payload = args.workload.read_bytes()
    manifest = json.loads(args.manifest.read_text())
    if hashlib.sha256(payload).hexdigest() != manifest["workload_sha256"]:
        raise ValueError("Workload hash differs from token manifest")
    rows = [json.loads(line) for line in payload.splitlines() if line.strip()]
    examples = manifest["examples"]
    ids = [r["custom_id"] for r in rows]
    if not ids or len(ids) != len(set(ids)) or set(ids) != set(examples):
        raise ValueError("Manifest/workload IDs must match uniquely")
    for row in rows:
        example = examples[row["custom_id"]]
        if (row["body"]["model"] != manifest["model"] or row["body"]["max_tokens"] != example["output_token_cap"]
                or example["rendered_prompt_tokens"] + example["output_token_cap"] > args.context_limit):
            raise ValueError("Invalid model, output cap, or context limit")
    ordered = dispatch_order(rows, examples, args.policy)
    auth = os.environ.get("INFERENCE_API_KEY", "")
    headers = {"Authorization": "Bearer " + auth} if auth else {}
    records = []
    async with httpx.AsyncClient(base_url=args.url.rstrip("/"), headers=headers, trust_env=False,
            timeout=args.request_timeout, limits=httpx.Limits(max_connections=args.concurrency+1), transport=transport) as client:
        response = await client.get("/v1/models")
        response.raise_for_status()
        catalog = {r["id"]: r for r in response.json()["data"]}
        if manifest["model"] not in catalog:
            raise ValueError("Engine does not advertise requested model")
        server_context = catalog[manifest["model"]].get("max_model_len")
        if type(server_context) is int and args.context_limit > server_context:
            raise ValueError("Requested context exceeds engine's advertised capacity")
        started_at = datetime.now(timezone.utc).isoformat()
        offered = time.monotonic()
        pending = deque(enumerate(ordered))
        async def consumer():
            while pending:
                index, row = pending.popleft()
                # Exactly one request. An uncertain stream is never replayed.
                if event_sink is not None:
                    event_sink({"event": "intent", "custom_id": row["custom_id"], "dispatch_index": index})
                record = await invoke(client, row, examples[row["custom_id"]], offered=offered,
                    dispatch=index, force_length=args.force_length, request_timeout=args.request_timeout)
                if event_sink is not None:
                    event_sink({"event": "terminal", "record": record})
                records.append(record)
        await asyncio.gather(*(consumer() for _ in range(min(args.concurrency, len(rows)))))
        elapsed = time.monotonic()-offered
    summary = summarize(records, elapsed, hourly_cost=args.hourly_cost)
    return {"schema_version": 3, "scope": "engine-only streaming HTTP; no durable platform completions",
        "measurement_started_at": started_at, "finished_at": datetime.now(timezone.utc).isoformat(),
        "passed": summary["errors"] == 0, "engine_label": args.engine_label, "model": manifest["model"],
        "passed_scope": "Complete inference streams and usage only; final text presence and quality are independent",
        "tokenizer_revision": manifest["tokenizer_revision"], "workload_sha256": manifest["workload_sha256"],
        "prompt_counter": manifest.get("prompt_counter", {"type": "legacy-unrecorded"}),
        "policy": args.policy, "policy_scope": "bounded client dispatch of one offered burst; engine scheduler unchanged",
        "shortest_cost_proxy": "prompt_tokens + 16 * output_cap", "concurrency": args.concurrency,
        "controlled_fixed_output": args.force_length, "context_limit": args.context_limit,
        "server_advertised_context_limit": server_context,
        "summary": summary,
        "by_class": {key: summarize([r for r in records if r["class"] == key], elapsed)
            for key in sorted({r["class"] for r in records})},
        "records": sorted(records, key=lambda r: r["dispatch_index"]),
        "limitations": ["Burst experiment, not sustained daily throughput or production traffic",
            "TTFT is first visible text delta including reasoning, HTTP and engine queueing; first final content is reported separately",
            "Stream-span/output-token value is a proxy: transport can bundle text, and reported usage includes reasoning/control tokens without visible deltas",
            "Successful inference means a complete valid HTTP stream and usage; final text presence is separate and does not establish task quality",
            "Generated-token throughput includes all engine-reported output tokens, including reasoning and Harmony/control tokens",
            "Success-only percentile population is accompanied by all-terminal latency and explicit errors",
            "Fixed-output mode ignores EOS and forces minimum tokens: capacity experiment, not natural task quality",
            "Class token rates share the full run wall time; not isolated prefill/decode peaks",
            "No GPU utilization measurement is inferred from HTTP token throughput"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--workload", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--engine-label", required=True)
    parser.add_argument("--policy", choices=["fifo", "shortest", "class-fair"], default="fifo")
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--context-limit", type=int, required=True)
    parser.add_argument("--request-timeout", type=float, default=600)
    parser.add_argument("--hourly-cost", type=float)
    parser.add_argument("--force-length", action="store_true")
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    if args.concurrency < 1 or args.context_limit < 1 or not math.isfinite(args.request_timeout) or args.request_timeout <= 0:
        parser.error("Positive concurrency, context and timeout required")
    if args.hourly_cost is not None and (not math.isfinite(args.hourly_cost) or args.hourly_cost < 0):
        parser.error("Cost must be finite and nonnegative")
    report = asyncio.run(run(args))
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({k: v for k, v in report.items() if k != "records"}, indent=2))
    if not report["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
