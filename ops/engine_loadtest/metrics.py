"""Engine sampling, per-request statistics, quality checks and the stability rule."""
from __future__ import annotations

import asyncio
import json
import math
import random
import re
import time
from collections.abc import Callable, Sequence

from .client import Record
from .workload import LONG_SYSTEM

LONG_KEYS = {"title", "date", "amount"}


def parse_prometheus(text: str) -> dict[str, float]:
    """Sum every labelled series under its metric name (gauges and histogram _sum/_count)."""
    totals: dict[str, float] = {}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        match = re.match(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+(\S+)$", line)
        if not match:
            continue
        name, value = match.group(1), match.group(3)
        try:
            number = float(value)
        except ValueError:
            continue
        if math.isfinite(number):
            totals[name] = totals.get(name, 0.0) + number
    return totals


async def sample_engine(fetch: Callable[[], str], rows: list[dict], stop: asyncio.Event,
                        interval: float = 5.0, clock: Callable[[], float] = time.monotonic) -> None:
    """Append one row per interval until `stop` is set. A failed scrape is recorded, not fatal."""
    while not stop.is_set():
        row = {"t": clock()}
        try:
            row.update(parse_prometheus(await asyncio.to_thread(fetch)))
        except Exception as exc:  # the engine may be busy; keep the gap visible
            row["scrape_error"] = type(exc).__name__
        rows.append(row)
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass


def percentile(values: Sequence[float], p: float) -> float | None:
    """Nearest-rank percentile; None when there is no data."""
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, math.ceil(p / 100 * len(ordered)))
    return ordered[rank - 1]


def slope(points: Sequence[tuple[float, float]]) -> float | None:
    """Least-squares slope (y per second) of (t, y) points."""
    if len(points) < 2:
        return None
    n = len(points)
    mean_t = sum(t for t, _ in points) / n
    mean_y = sum(y for _, y in points) / n
    denom = sum((t - mean_t) ** 2 for t, _ in points)
    if denom == 0:
        return 0.0
    return sum((t - mean_t) * (y - mean_y) for t, y in points) / denom


def is_correct(record: Record, label: dict | None, kind: str) -> bool | None:
    """Scoring per class. None means no check applies to this class."""
    text = (record.text or "").strip()
    if kind == "short":
        if label is None:
            return None
        answer = text.lower().strip(" .!")
        return answer == label["answer"]
    if kind == "medium":
        sentences = [s for s in re.split(r"(?<=[.!?])\s+", text) if s]
        return 1 <= len(sentences) <= 2
    if kind == "long":
        try:
            value = json.loads(text)
        except ValueError:
            return False
        return isinstance(value, dict) and set(value) == LONG_KEYS
    return None


def quality_sample(records: list[Record], labels: dict[str, dict], rate: float = 0.05,
                   seed: int = 1) -> dict:
    """Score a seeded random sample of completed requests, per class."""
    rng = random.Random(seed)
    done = [r for r in records if completed(r)]
    chosen = [r for r in done if rng.random() < rate]
    by_kind: dict[str, list[bool]] = {}
    for record in chosen:
        verdict = is_correct(record, labels.get(record.id), record.kind)
        if verdict is not None:
            by_kind.setdefault(record.kind, []).append(verdict)
    return {kind: {"n": len(v), "pass_rate": round(sum(v) / len(v), 3)} for kind, v in by_kind.items() if v}


def engine_side(samples: list[dict]) -> dict:
    """Step-window engine figures: histogram means from counter deltas, gauge peaks."""
    out: dict[str, float | None] = {}
    if len(samples) >= 2:
        first, last = samples[0], samples[-1]
        for name in ("vllm:request_queue_time_seconds", "vllm:time_to_first_token_seconds",
                     "vllm:time_per_output_token_seconds"):
            count = last.get(name + "_count", 0) - first.get(name + "_count", 0)
            total = last.get(name + "_sum", 0) - first.get(name + "_sum", 0)
            out[name + "_mean_s"] = round(total / count, 4) if count > 0 else None
    for name in ("vllm:num_requests_waiting", "vllm:num_requests_running",
                 "vllm:gpu_cache_usage_perc", "vllm:kv_cache_usage_perc"):
        values = [s[name] for s in samples if name in s]
        out[name + "_max"] = max(values) if values else None
    return out


def completed(record: Record) -> bool:
    return record.status == 200 and record.error is None and record.latency is not None and record.prompt_tokens is not None


def summarize_step(records: list[Record], samples: list[dict], *, duration: float, deadlines: dict[str, float],
                   labels: dict[str, dict], window: float = 600.0, sample_seed: int = 1) -> dict:
    """Per-step numbers used for the stability decision and the report."""
    done = [r for r in records if completed(r)]
    per_kind = {}
    for kind in sorted({r.kind for r in records}):
        mine = [r for r in done if r.kind == kind]
        lat = [r.latency for r in mine]
        ttft = [r.ttft for r in mine if r.ttft is not None]
        within = sum(1 for r in mine if r.latency <= deadlines[kind])
        total = sum(1 for r in records if r.kind == kind)
        per_kind[kind] = {
            "offered": total,
            "completed": len(mine),
            "p50_s": _round(percentile(lat, 50)),
            "p95_s": _round(percentile(lat, 95)),
            "p99_s": _round(percentile(lat, 99)),
            "ttft_p50_s": _round(percentile(ttft, 50)),
            "ttft_p95_s": _round(percentile(ttft, 95)),
            "deadline_s": deadlines[kind],
            "goodput_share": round(within / total, 3) if total else None,
        }
    lags = [r.lag for r in records if r.lag is not None]
    errors = sum(1 for r in records if not r.dropped and not completed(r))
    dropped = sum(1 for r in records if r.dropped)
    last_t = samples[-1]["t"] if samples else 0.0
    waiting = [(s["t"], s["vllm:num_requests_waiting"]) for s in samples
               if "vllm:num_requests_waiting" in s and s["t"] >= last_t - window]
    queue_slope_per_min = None if slope(waiting) is None else slope(waiting) * 60
    return {
        "duration_s": duration,
        "offered_rate": round(len(records) / duration, 3),
        "completed_rate": round(len(done) / duration, 3),
        "dropped": dropped,
        "errors": errors,
        # Dropped requests are failures too: the engine did not serve them.
        "error_rate": round((errors + dropped) / len(records), 5) if records else None,
        "engine": engine_side(samples),
        "client_lag_p99_s": _round(percentile(lags, 99)),
        "output_tokens_per_s": round(sum(r.completion_tokens or 0 for r in done) / duration, 1),
        "queue_slope_per_min": None if queue_slope_per_min is None else round(queue_slope_per_min, 3),
        "per_kind": per_kind,
        "quality": quality_sample(records, labels, seed=sample_seed),
    }


def stability(summary: dict, *, deadlines: dict[str, float]) -> tuple[bool, list[str]]:
    """The five criteria from the design doc, §7. Returns (stable, reasons for failure)."""
    reasons = []
    slope_value = summary["queue_slope_per_min"]
    if slope_value is None or abs(slope_value) > 0.5:
        reasons.append(f"queue not flat (slope {slope_value} req/min)")
    offered = summary["offered_rate"]
    if offered and summary["completed_rate"] < 0.98 * offered:
        reasons.append("completed rate below 98% of offered")
    if summary["error_rate"] is None or summary["error_rate"] >= 0.001:
        reasons.append(f"error rate {summary['error_rate']} at or above 0.1%")
    lag = summary["client_lag_p99_s"]
    if lag is None or lag > 0.05:
        reasons.append(f"client lag p99 {lag}s above 50 ms; client is the bottleneck")
    for kind, stats in summary["per_kind"].items():
        if stats["p99_s"] is None or stats["p99_s"] > deadlines[kind]:
            reasons.append(f"{kind} p99 {stats['p99_s']}s above deadline {deadlines[kind]}s")
    return (not reasons, reasons)


def _round(value: float | None) -> float | None:
    return None if value is None else round(value, 3)
