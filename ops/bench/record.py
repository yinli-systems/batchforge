"""One benchmark record per (model, precision, hardware, shape). Refuses incomplete records.

Hardware and model facts are recorded as measured or as declared by the operator; nothing is inferred.
"""
from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field


def percentile(values: list[float], pct: float) -> float:
    """Nearest-rank percentile over the sorted samples."""
    if not values:
        raise ValueError("no samples")
    ordered = sorted(values)
    rank = max(1, math.ceil(pct / 100 * len(ordered)))
    return ordered[rank - 1]


@dataclass
class Record:
    model: str
    precision: str
    gpu_model: str
    gpu_count: int
    tensor_parallel: int
    input_tokens: int
    output_tokens: int
    requests: int
    failures: list[str] = field(default_factory=list)
    latency_ms: list[float] = field(default_factory=list)
    output_tokens_per_s: float | None = None
    peak_vram_mib: float | None = None          # measured by nvidia-smi during the run, or None
    hourly_cost_usd: float | None = None        # operator-declared rate, or None
    cost_source: str | None = None              # where the rate came from
    command: str = ""

    def summary(self) -> dict:
        out = asdict(self)
        if self.latency_ms:
            out["p50_ms"] = percentile(self.latency_ms, 50)
            out["p95_ms"] = percentile(self.latency_ms, 95)
            out["p99_ms"] = percentile(self.latency_ms, 99)
        out["success_rate"] = (self.requests - len(self.failures)) / self.requests if self.requests else None
        if self.hourly_cost_usd is not None and self.output_tokens_per_s:
            # cost per million output tokens, using the measured rate and the declared hourly price
            out["usd_per_million_output_tokens"] = round(
                self.hourly_cost_usd / 3600 / (self.output_tokens_per_s * self.gpu_count) * 1e6, 4)
        return out


def validate(record: Record) -> list[str]:
    problems = []
    if not record.command:
        problems.append("missing reproduction command")
    if record.requests < 1:
        problems.append("no requests")
    if record.latency_ms and len(record.latency_ms) + len(record.failures) != record.requests:
        problems.append("samples do not add up to requests")
    if record.hourly_cost_usd is not None and not record.cost_source:
        problems.append("cost given without a source")
    if record.tensor_parallel > record.gpu_count:
        problems.append("tensor parallel larger than gpu count")
    return problems
