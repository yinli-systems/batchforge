"""Command line for the open-loop engine load test.

    python -m ops.engine_loadtest.run dry-run --jobs JOBS.csv --docs DIR --base-url URL --model MODEL --out DIR
    python -m ops.engine_loadtest.run calibrate ... --out DIR
    python -m ops.engine_loadtest.run step --rate R ... --out DIR

`dry-run` builds the workload and schedule and checks sizes. It sends only
tokenization requests to the engine and generates no completions.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import sys
import time
from dataclasses import asdict
from pathlib import Path

import httpx

from .client import run_open_loop
from .corpus import load_jobs, load_paragraphs
from .metrics import sample_engine, stability, summarize_step
from .schedule import build_schedule, mix_report
from .workload import SPECS, build_pools, reuse_report

DEADLINES = {"short": 10.0, "medium": 60.0, "long": 180.0}  # proposals; see design §6


def engine_counter(base_url: str, model: str):
    """Exact prompt sizes from the engine's own tokenizer."""
    client = httpx.Client(base_url=base_url.rstrip("/"), timeout=60)

    def count(messages: list[dict]) -> int:
        response = client.post("/tokenize", json={"model": model, "messages": messages,
                                                  "add_generation_prompt": True,
                                                  "add_special_tokens": False})
        response.raise_for_status()
        return int(response.json()["count"])

    return count


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build_workload(args, count):
    jobs = load_jobs(Path(args.jobs))
    doc_files = sorted(Path(args.docs).glob("*.md"))
    paragraphs = load_paragraphs(doc_files)
    if len(jobs) < args.per_kind or len(paragraphs) < 50:
        raise SystemExit(f"Not enough real text: {len(jobs)} labelled jobs, {len(paragraphs)} prose paragraphs")
    pools = build_pools(jobs, paragraphs, seed=args.seed, per_kind=args.per_kind, count=count)
    return pools, {
        "jobs_file": {"path": str(args.jobs), "sha256": sha256(Path(args.jobs)), "labelled_jobs": len(jobs)},
        "docs": {"files": len(doc_files), "paragraphs": len(paragraphs)},
        "pools": {kind: len(items) for kind, items in pools.items()},
        "prompt_tokens": {kind: _range(items) for kind, items in pools.items()},
        "reuse": reuse_report(pools, len(paragraphs)),
    }


def _range(items):
    sizes = [item.prompt_tokens for item in items]
    return {"min": min(sizes), "max": max(sizes), "mean": round(sum(sizes) / len(sizes), 1)}


def write_jsonl(path: Path, rows) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


async def measure_step(args, pools, rate: float, out: Path) -> dict:
    total = args.warmup + args.measure
    schedule = build_schedule(pools, rate, total, seed=args.seed)
    labels = {item.id: item.label for kind in pools for item in pools[kind]}
    samples: list[dict] = []
    stop = asyncio.Event()
    fetch = lambda: httpx.get(args.base_url.rstrip("/") + "/metrics", timeout=5).text
    sampler = asyncio.create_task(sample_engine(fetch, samples, stop, clock=time.monotonic))
    started = time.monotonic()
    records = await run_open_loop(schedule, base_url=args.base_url, model=args.model,
                                  max_inflight=args.max_inflight, timeout=args.timeout)
    stop.set()
    await sampler
    measured = [r for r in records if args.warmup <= r.planned_at < total]
    window = [s for s in samples if started + args.warmup <= s["t"] < started + total]
    summary = summarize_step(measured, window, duration=args.measure, deadlines=DEADLINES,
                             labels=labels, window=min(600.0, args.measure), sample_seed=args.seed)
    stable, reasons = stability(summary, deadlines=DEADLINES)
    summary.update({"rate_req_per_s": rate, "stable": stable, "unstable_reasons": reasons,
                    "mix": mix_report(schedule, pools)})
    out.mkdir(parents=True, exist_ok=True)
    write_jsonl(out / "requests.jsonl", (r.as_json() for r in records))
    write_jsonl(out / "schedule.jsonl", ({"id": p.item.id, "kind": p.item.kind, "at": p.at} for p in schedule))
    write_jsonl(out / "metrics.jsonl", samples)
    (out / "summary.json").write_text(json.dumps(summary, indent=1))
    return summary


def common(parser):
    parser.add_argument("--jobs", required=True, help="Job-listing CSV with an early_career column")
    parser.add_argument("--docs", required=True, help="Directory of project Markdown docs (prose source)")
    parser.add_argument("--base-url", required=True, help="Engine base URL, e.g. http://127.0.0.1:8300")
    parser.add_argument("--model", required=True)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--seed", type=int, default=20261025)
    parser.add_argument("--per-kind", type=int, default=400, help="Distinct items per class")
    parser.add_argument("--max-inflight", type=int, default=512)
    parser.add_argument("--timeout", type=float, default=900.0)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    dry = sub.add_parser("dry-run", help="Build and check the workload; no completions are generated")
    common(dry)
    dry.add_argument("--rate", type=float, default=1.0)
    dry.add_argument("--duration", type=float, default=1800.0)

    step = sub.add_parser("step", help="One measured open-loop step")
    common(step)
    step.add_argument("--rate", type=float, required=True)
    step.add_argument("--warmup", type=float, default=300.0)
    step.add_argument("--measure", type=float, default=1500.0)

    cal = sub.add_parser("calibrate", help="Ramp the rate until the engine is saturated; prints capacity")
    common(cal)
    cal.add_argument("--start", type=float, default=0.5)
    cal.add_argument("--factor", type=float, default=1.5)
    cal.add_argument("--warmup", type=float, default=60.0)
    cal.add_argument("--measure", type=float, default=180.0)
    cal.add_argument("--max-steps", type=int, default=12)

    args = parser.parse_args(argv)
    args.out.mkdir(parents=True, exist_ok=True)
    count = engine_counter(args.base_url, args.model)
    pools, workload = build_workload(args, count)

    if args.command == "dry-run":
        schedule = build_schedule(pools, args.rate, args.duration, seed=args.seed)
        report = {"workload": workload, "schedule": mix_report(schedule, pools),
                  "classes": {k: asdict(v) for k, v in SPECS.items()}}
        (args.out / "dry_run.json").write_text(json.dumps(report, indent=1))
        print(json.dumps(report, indent=1))
        return 0

    (args.out / "workload.json").write_text(json.dumps(workload, indent=1))

    if args.command == "step":
        summary = asyncio.run(measure_step(args, pools, args.rate, args.out))
        print(json.dumps({k: summary[k] for k in ("rate_req_per_s", "stable", "unstable_reasons",
                                                  "completed_rate", "output_tokens_per_s")}, indent=1))
        return 0

    # calibrate: the last rate that is stable, times 0.9, is C.
    last_stable = None
    rate = args.start
    for index in range(args.max_steps):
        step_dir = args.out / f"calibrate-{index:02d}-{rate:.3f}"
        summary = asyncio.run(measure_step(args, pools, rate, step_dir))
        saturated = (summary["per_kind"]["short"]["p95_s"] or 0) > 10 or \
                    (summary["engine"]["vllm:num_requests_waiting_max"] or 0) >= 20
        print(json.dumps({"rate": rate, "stable": summary["stable"], "saturated": saturated,
                          "completed_rate": summary["completed_rate"]}), flush=True)
        if saturated:
            break
        if summary["stable"]:
            last_stable = rate
        rate *= args.factor
    capacity = None if last_stable is None else round(0.9 * last_stable, 3)
    result = {"capacity_req_per_s": capacity, "last_stable_rate": last_stable}
    (args.out / "calibration.json").write_text(json.dumps(result, indent=1))
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    sys.exit(main())
