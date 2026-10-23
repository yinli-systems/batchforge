#!/usr/bin/env python3
"""Continue read-only observation after the existing app sampler exits.

The successor waits on only the verified predecessor identity, does not signal
it, and reuses the local low-privilege API configuration without logging it.
"""
from __future__ import annotations

import argparse
import datetime
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
PYTHON = str(ROOT.parent / "venv/bin/python")
RECORD = ROOT / ".runtime/sampler-secondary-ab.json"
PREDECESSOR = ROOT / ".runtime/sampler-secondary.json"
COMMAND = [PYTHON, "scripts/observe_runtime.py", "--env-file", ".env.api-a",
           "--api", "api-a=http://127.0.0.1:8082", "--api", "api-b=http://127.0.0.1:8083",
           "--interval-seconds", "2", "--duration-seconds", "3600",
           "--output", ".runtime/observability/controlplane-secondary-ab-20261024.jsonl"]


def manager_module():
    spec = importlib.util.spec_from_file_location("batchforge_manage", ROOT / "ops/manage.py")
    manager = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(manager)
    return manager


def private_json(path: Path, value: dict) -> None:
    temporary = path.with_name(path.name + f".{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "w") as handle:
        json.dump(value, handle)
        handle.write("\n")
    temporary.replace(path)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wait-and-exec", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    manager = manager_module()
    if args.wait_and_exec:
        deadline = time.monotonic() + 4000
        while manager.owned_record(PREDECESSOR):
            if time.monotonic() > deadline:
                raise RuntimeError("Predecessor exceeded the bounded continuity wait")
            time.sleep(1)
        private_json(ROOT / ".runtime/pod2-sampler-continuity.json", {
            "observed_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "status": "successor_starting", "pid": os.getpid(),
            "output": COMMAND[-1], "duration_seconds": 3600, "overlap_requested": False})
        os.execv(PYTHON, COMMAND)
    existing = manager.owned_record(RECORD)
    if existing:
        print(json.dumps({"status": "already_scheduled", "pid": existing["pid"]}))
        return
    predecessor = manager.owned_record(PREDECESSOR)
    descriptor = os.open(ROOT / ".runtime/sampler-secondary-ab.log", os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    with os.fdopen(descriptor, "ab") as logfile:
        child = subprocess.Popen([PYTHON, str(Path(__file__).resolve()), "--wait-and-exec"], cwd=ROOT,
                                 stdin=subprocess.DEVNULL, stdout=logfile, stderr=logfile,
                                 start_new_session=True)
    time.sleep(.25)
    identity = manager.process_identity(child.pid)
    if not identity or child.poll() is not None:
        raise RuntimeError("Observation successor exited before its identity was recorded")
    private_json(RECORD, identity)
    report = {"observed_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
              "status": "successor_waiting" if predecessor else "successor_starting",
              "pid": child.pid, "predecessor_pid": predecessor["pid"] if predecessor else None,
              "output": COMMAND[-1], "duration_seconds": 3600, "overlap_requested": False}
    private_json(ROOT / ".runtime/pod2-sampler-continuity.json", report)
    print(json.dumps(report), flush=True)


if __name__ == "__main__":
    main()
