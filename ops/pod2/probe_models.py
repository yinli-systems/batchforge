#!/usr/bin/env python3
"""Run auditable real model requests against private local 7B engine replicas.

This is an engine-only workload probe. Its generated incident documents contain
no customer data. It measures actual response latency and reported token usage,
not platform queue latency, task quality, or projected daily capacity.
"""
from __future__ import annotations

import argparse
import asyncio
import datetime
import hashlib
import json
from pathlib import Path
import time

import httpx

MODEL = "Qwen/Qwen2.5-7B-Instruct"
REVISION = "a09a35458c702b33eeacc393d103063234e8bc28"


def prompt(index: int, sections: int) -> str:
    rows = [f"Incident report {index}. Analyze this synthetic operations log. Report the largest reliability risks, "
            "which regions need intervention, and recommended actions. Cite concrete measurements. "
            "Write a useful concise executive summary followed by a technical action plan.\n"]
    for region in range(sections):
        rows.append(f"Region r{region:03d}, observation {index}: completed {12000 + region * 137 + index} tasks; "
                    f"p95 end-to-end latency {400 + ((region * 97 + index * 11) % 1800)} ms; "
                    f"transient retry rate {(region * 7 + index) % 43 / 10:.1f} percent; "
                    f"GPU occupancy {51 + (region * 13 + index) % 47} percent. "
                    f"The queue age is {(region * 19 + index) % 173} seconds. "
                    "Clients require durable outputs, fair capacity allocation and no silent task loss. "
                    "A completed attempt can be replayed after a lost acknowledgement.\n")
    return "".join(rows)


async def run(args) -> dict:
    started_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
    start = time.monotonic()
    results = []
    async with httpx.AsyncClient(timeout=httpx.Timeout(args.request_timeout, connect=10), trust_env=False,
                                 limits=httpx.Limits(max_connections=len(args.ports) * args.concurrency)) as client:
        async def engine(port: int) -> None:
            deadline = time.monotonic() + args.ready_timeout
            while True:
                try:
                    response = await client.get(f"http://127.0.0.1:{port}/v1/models", timeout=5)
                    response.raise_for_status()
                    expected_root = f"/root/batchforge-runtime/models/Qwen2.5-7B-Instruct/{REVISION}"
                    if any(item["id"] == MODEL and item.get("root") == expected_root
                           and item.get("max_model_len") == 4096 for item in response.json()["data"]):
                        break
                except (httpx.HTTPError, ValueError, KeyError):
                    pass
                if time.monotonic() >= deadline:
                    results.append({"port": port, "passed": False, "error": "engine_readiness_timeout"})
                    return
                await asyncio.sleep(1)
            capacity = asyncio.Semaphore(args.concurrency)
            async def request(index: int) -> None:
                document = prompt(port * 100000 + index, args.sections)
                async with capacity:
                    began = time.monotonic()
                    record = {"port": port, "index": index, "prompt_sha256": hashlib.sha256(document.encode()).hexdigest()}
                    try:
                        response = await client.post(f"http://127.0.0.1:{port}/v1/chat/completions", json={
                            "model": MODEL, "messages": [{"role": "user", "content": document}],
                            "max_tokens": args.max_tokens, "temperature": 0, "seed": 42})
                        response.raise_for_status()
                        value = response.json()
                        content = value["choices"][0]["message"]["content"]
                        usage = value["usage"]
                        passed = value["model"] == MODEL and bool(content) and usage["completion_tokens"] > 0
                        record.update({"passed": passed, "usage": usage,
                                       "finish_reason": value["choices"][0]["finish_reason"],
                                       "output_sha256": hashlib.sha256(content.encode()).hexdigest(),
                                       "output_sample": content[:256] if index == 0 else None})
                    except (httpx.HTTPError, ValueError, KeyError, TypeError) as error:
                        record.update({"passed": False, "error": type(error).__name__})
                    record["request_seconds"] = round(time.monotonic() - began, 6)
                    results.append(record)
            await asyncio.gather(*(request(i) for i in range(args.requests)))
        await asyncio.gather(*(engine(port) for port in args.ports))
    elapsed = time.monotonic() - start
    successful = [row for row in results if row.get("passed")]
    prompt_tokens = sum(row["usage"]["prompt_tokens"] for row in successful)
    output_tokens = sum(row["usage"]["completion_tokens"] for row in successful)
    return {"schema_version": 1, "measurement_type": "real_engine_only_workload", "started_at_utc": started_at,
            "model": MODEL, "revision": REVISION, "dtype": "bfloat16", "prefix_caching": False,
            "revision_evidence": "verified local model manifest, pinned engine arguments and /v1/models root path",
            "context_limit": 4096, "requested_ports": args.ports, "requests_per_engine": args.requests,
            "concurrency_per_engine": args.concurrency, "input_sections": args.sections,
            "output_limit_tokens": args.max_tokens, "temperature": 0, "seed": 42,
            "elapsed_seconds_including_readiness": round(elapsed, 6), "requested_count": len(args.ports) * args.requests,
            "successful_count": len(successful), "passed": len(successful) == len(args.ports) * args.requests,
            "prompt_tokens": prompt_tokens, "completion_tokens": output_tokens,
            "observed_requests_per_second_including_readiness": len(successful) / elapsed,
            "observed_output_tokens_per_second_including_readiness": output_tokens / elapsed,
            "quality_evaluated": False, "results": sorted(results, key=lambda r: (r["port"], r.get("index", -1)))}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ports", type=int, nargs="+", required=True)
    parser.add_argument("--requests", type=int, default=2, help="Requests per engine")
    parser.add_argument("--concurrency", type=int, default=2, help="Concurrent requests per engine")
    parser.add_argument("--sections", type=int, default=16)
    parser.add_argument("--max-tokens", type=int, default=256)
    parser.add_argument("--ready-timeout", type=float, default=600)
    parser.add_argument("--request-timeout", type=float, default=240)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    if any(not 8200 <= p <= 8215 for p in args.ports) or len(set(args.ports)) != len(args.ports) or min(args.requests, args.concurrency, args.sections, args.max_tokens) <= 0:
        parser.error("Use distinct owned ports 8200–8215 and positive workload values")
    report = asyncio.run(run(args))
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: value for key, value in report.items() if key != "results"}), flush=True)
    raise SystemExit(0 if report["passed"] else 1)


if __name__ == "__main__":
    main()
