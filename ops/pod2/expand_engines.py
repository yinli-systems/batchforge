#!/usr/bin/env python3
"""Load and genuinely probe the additional ten MIG model replicas, two at a time.

This never stops a service or starts a worker. Existing engines return their
verified manager status, preventing duplicate starts after interrupted clients.
Run only after the operator releases the frozen six-engine inference baseline.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

try:
    from .cuda_devices import driver_metadata, select_mig_device
except ImportError:  # Direct script invocation on the Pod.
    from cuda_devices import driver_metadata, select_mig_device

ROOT = Path(__file__).resolve().parents[2]
PYTHON = str(ROOT.parent / "engine-venv/bin/python")
REPORT = ROOT / ".runtime/pod2-7b-expansion.json"


def timestamp() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def atomic_report(value: dict) -> None:
    temporary = REPORT.with_name(REPORT.name + f".{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "w") as handle:
        json.dump(value, handle, indent=2)
        handle.write("\n")
    temporary.replace(REPORT)


def expand(index: int, identity: dict) -> dict:
    began = time.monotonic()
    row = {"logical_device": index, "cuda_selector": identity["selector"], "started_at_utc": timestamp(),
           "port": 8200 + index, "passed": False}
    try:
        maximum = Path("/sys/fs/cgroup/memory.max").read_text().strip()
        if maximum != "max":
            available = int(maximum) - int(Path("/sys/fs/cgroup/memory.current").read_text())
            row["container_memory_available_before_bytes"] = available
            if available < 48 * 1024**3:
                row["error"] = "container_memory_headroom_below_48_gib"
                return row
        logfile = ROOT / f".runtime/pod2-7b-expansion-gpu{index}.log"
        descriptor = os.open(logfile, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        with os.fdopen(descriptor, "ab") as output:
            startup = subprocess.run([PYTHON, "ops/pod2/upgrade_engines.py", "--gpu", str(index),
                                      "--mig-uuid", identity["selector"]], cwd=ROOT,
                                     stdin=subprocess.DEVNULL, stdout=output, stderr=output,
                                     timeout=180)
            if startup.returncode:
                row.update(error="engine_start_preflight_failed", exit_code=startup.returncode)
                return row
            probe_path = ROOT / f".runtime/pod2-7b-probe-gpu{index}.json"
            generation = subprocess.run([PYTHON, "ops/pod2/probe_models.py", "--ports", str(8200 + index),
                "--requests", "1", "--concurrency", "1", "--sections", "16", "--max-tokens", "8",
                "--ready-timeout", "180", "--request-timeout", "180", "--report", str(probe_path)],
                cwd=ROOT, stdin=subprocess.DEVNULL, stdout=output, stderr=output, timeout=390)
        if generation.returncode:
            row.update(error="real_generation_probe_failed", exit_code=generation.returncode)
            return row
        probe = json.loads(probe_path.read_text())
        if probe.get("passed") is not True or probe.get("successful_count") != 1 or probe.get("completion_tokens", 0) <= 0:
            row["error"] = "real_generation_result_invalid"
            return row
        row.update(passed=True, prompt_tokens=probe["prompt_tokens"], completion_tokens=probe["completion_tokens"],
                   generation_probe=probe, probe_report_sha256=hashlib.sha256(probe_path.read_bytes()).hexdigest())
        return row
    except Exception as error:
        row["error"] = type(error).__name__
        return row
    finally:
        row.update(finished_at_utc=timestamp(), elapsed_seconds=round(time.monotonic() - began, 6))


def main() -> None:
    inventory = driver_metadata()
    identities = {index: select_mig_device(inventory, index) for index in range(6, 16)}
    report = {"started_at_utc": timestamp(), "status": "running", "parallel_startups": 2,
              "requested_logical_devices": list(identities), "model": "Qwen/Qwen2.5-7B-Instruct",
              "revision": "a09a35458c702b33eeacc393d103063234e8bc28", "dtype": "bfloat16",
              "context_limit": 4096, "max_num_seqs": 8, "prefix_caching": False,
              "workers_started": 0, "processes_stopped": 0, "devices": []}
    atomic_report(report)
    with ThreadPoolExecutor(max_workers=2) as pool:
        pending = {pool.submit(expand, index, identity): index for index, identity in identities.items()}
        for future in as_completed(pending):
            row = future.result()
            report["devices"].append(row)
            report["devices"].sort(key=lambda item: item["logical_device"])
            report["last_progress_at_utc"] = timestamp()
            atomic_report(report)
            print(json.dumps({"event": "engine_expansion_result", "logical_device": row["logical_device"],
                              "passed": row["passed"], "finished_count": len(report["devices"])}), flush=True)
    report.update(status="complete" if all(row["passed"] for row in report["devices"]) else "incomplete",
                  finished_at_utc=timestamp(), passed_count=sum(row["passed"] for row in report["devices"]))
    atomic_report(report)
    print(json.dumps({key: value for key, value in report.items() if key != "devices"}), flush=True)
    if report["status"] != "complete":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
