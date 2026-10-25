"""Read-only GPU inventory snapshot for evidence packs.

Records only what the driver reports: device identity, memory, utilization,
power, temperature and which processes hold GPU memory. It infers nothing
about topology or interconnects, and it never changes GPU state.

    python -m ops.evidence.gpu_inventory > snapshot.json
"""
from __future__ import annotations

import csv
import datetime as dt
import io
import json
import math
import subprocess
import sys
from typing import Callable, Sequence

SCHEMA = "batchforge.gpu_inventory/1"

GPU_FIELDS = (
    "index", "name", "uuid", "memory.total", "memory.used",
    "utilization.gpu", "power.draw", "temperature.gpu", "pstate",
)
PROCESS_FIELDS = ("pid", "process_name", "gpu_uuid", "used_memory")

NUMERIC = {"memory.total", "memory.used", "utilization.gpu", "power.draw", "temperature.gpu", "used_memory"}
INTEGER = {"index", "pid"}


def _value(field: str, raw: str):
    text = (raw or "").strip()
    if text in {"", "[N/A]", "[Not Supported]", "N/A"}:
        return None
    if field in INTEGER:
        return int(text)
    if field in NUMERIC:
        number = float(text.split()[0])
        if not math.isfinite(number):
            return None
        return number
    return text


def parse_csv(text: str, fields: Sequence[str]) -> list[dict]:
    """Parse nvidia-smi `--format=csv,noheader,nounits` output into typed records."""
    rows = []
    for line in csv.reader(io.StringIO(text)):
        if not line or all(not cell.strip() for cell in line):
            continue
        if len(line) != len(fields):
            raise ValueError(f"expected {len(fields)} fields, got {len(line)}")
        rows.append({field: _value(field, cell) for field, cell in zip(fields, line)})
    return rows


def build_snapshot(gpu_text: str, process_text: str, *, host: str, collected_at: str) -> dict:
    gpus = parse_csv(gpu_text, GPU_FIELDS)
    processes = parse_csv(process_text, PROCESS_FIELDS)
    known = {gpu["uuid"] for gpu in gpus}
    for process in processes:
        process["gpu_known"] = process["gpu_uuid"] in known
    total = sum(gpu["memory.total"] or 0 for gpu in gpus)
    used = sum(gpu["memory.used"] or 0 for gpu in gpus)
    return {
        "schema": SCHEMA,
        "host": host,
        "collected_at": collected_at,
        "gpu_count": len(gpus),
        "gpu_models": sorted({gpu["name"] for gpu in gpus if gpu["name"]}),
        "memory_total_mib": total,
        "memory_used_mib": used,
        "gpus": gpus,
        "processes": processes,
    }


def run_nvidia_smi(query: Callable[[list[str]], str] | None = None) -> tuple[str, str]:
    def default(args: list[str]) -> str:
        return subprocess.run(["nvidia-smi", *args], check=True, capture_output=True, text=True, timeout=30).stdout

    run = query or default
    gpu = run(["--query-gpu=" + ",".join(GPU_FIELDS), "--format=csv,noheader,nounits"])
    procs = run(["--query-compute-apps=" + ",".join(PROCESS_FIELDS), "--format=csv,noheader,nounits"])
    return gpu, procs


def main(argv: Sequence[str] | None = None) -> int:
    import socket

    host = socket.gethostname()
    collected = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    gpu_text, process_text = run_nvidia_smi()
    json.dump(build_snapshot(gpu_text, process_text, host=host, collected_at=collected), sys.stdout, indent=1)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
