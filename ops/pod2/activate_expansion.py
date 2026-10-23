#!/usr/bin/env python3
"""Finish the operator-released 16-MIG rollout with app-owned process records.

Only the watchdog is restarted. The original six engines keep their PIDs;
the additional ten are warmed by genuine requests before workers are activated.
"""
from __future__ import annotations

import datetime
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import stat
import time
from urllib.request import urlopen

try:
    from .cuda_devices import driver_metadata, verified_mig_devices
except ImportError:
    from cuda_devices import driver_metadata, verified_mig_devices

ROOT = Path(__file__).resolve().parents[2]
PYTHON = str(ROOT.parent / "venv/bin/python")
ENGINE_PYTHON = str(ROOT.parent / "engine-venv/bin/python")


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> None:
    manager = load_module("bf_manage", ROOT / "ops/manage.py")
    supervisor = load_module("bf_supervise", ROOT / "ops/supervise.py")
    before = {index: manager.owned_record(ROOT / f".runtime/engine-pod2-7b-gpu{index}.json")["pid"] for index in range(6)}
    # Existing processes keep their environment. Their future UUID-based
    # watchdog restarts need the same app-local platform selector as new ones.
    compatibility = {"BATCHFORGE_MIG_PLATFORM": "1", "VLLM_PLUGINS": "batchforge_mig",
                     "PYTHONPATH": str(ROOT / "ops/pod2/mig_platform"),
                     "VLLM_WORKER_MULTIPROC_METHOD": "spawn"}
    for index in range(6):
        path = ROOT / f".env.engine-pod2-7b-gpu{index}"
        if path.is_symlink() or stat.S_IMODE(path.stat().st_mode) != 0o600:
            raise RuntimeError("Future engine configuration must remain owner-only")
        lines = [line for line in path.read_text().splitlines()
                 if line.strip().partition("=")[0].removeprefix("export ").strip() not in compatibility]
        temporary = path.with_name(path.name + f".{os.getpid()}.tmp")
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, "w") as handle:
            handle.write("\n".join(lines) + "\n" + "".join(f"{key}={value!r}\n" for key, value in compatibility.items()))
        temporary.replace(path)

    def run(*arguments, timeout=None):
        subprocess.run(arguments, cwd=ROOT, check=True, timeout=timeout)

    run(ENGINE_PYTHON, "ops/pod2/plan_expansion.py", "--prepare-workers", timeout=120)
    run(ENGINE_PYTHON, "ops/pod2/expand_engines.py", timeout=1800)
    run(PYTHON, "ops/pod2/start_model_workers.py", "--devices", *map(str, range(6, 16)), timeout=180)
    deadline = time.monotonic() + 90
    while True:
        states = []
        for index in range(16):
            try:
                with urlopen(f"http://127.0.0.1:{9200 + index}/readyz", timeout=2) as response:
                    states.append(response.status == 200)
            except Exception:
                states.append(False)
        if all(states):
            break
        if time.monotonic() > deadline:
            raise RuntimeError("Not all sixteen prepared workers became ready")
        time.sleep(1)

    plan = ROOT / ".runtime/pod2-services-expanded.json"
    _, services, fingerprint = supervisor.load_manifest(plan, ROOT)
    if len(services) != 35:
        raise RuntimeError("Expanded service plan must contain exactly 35 services")
    for entry in services:
        if manager.owned_record(ROOT / f".runtime/{entry['role']}-{entry['name']}.json") is None:
            raise RuntimeError("Expanded service plan includes a process that is not running")
    run(PYTHON, "ops/manage.py", "stop", "supervisor", "--name", "main", "--grace-seconds", "20", timeout=30)
    try:
        active = ROOT / ".runtime/pod2-services.json"
        temporary = active.with_name(active.name + f".{os.getpid()}.tmp")
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(plan.read_bytes())
        temporary.replace(active)
    finally:
        run(PYTHON, "ops/manage.py", "start", "supervisor", "--name", "main", "--env-file", ".env.supervisor-pod2", "--python", PYTHON, timeout=30)

    deadline = time.monotonic() + 90
    while True:
        snapshot = json.loads((ROOT / ".runtime/supervision-pod2-services.json").read_text())
        if (snapshot.get("configuration_sha256") == fingerprint and snapshot.get("watchdog_running")
                and len(snapshot["services"]) == 35
                and all(state.get("running") and state.get("ready") is True for state in snapshot["services"].values())):
            break
        if time.monotonic() > deadline:
            raise RuntimeError("Expanded watchdog did not observe all thirty-five services ready")
        time.sleep(1)

    inventory = verified_mig_devices(driver_metadata(python=ENGINE_PYTHON))
    expansion = json.loads((ROOT / ".runtime/pod2-7b-expansion.json").read_text())
    rows = []
    for index, identity in enumerate(inventory):
        record = manager.owned_record(ROOT / f".runtime/engine-pod2-7b-gpu{index}.json")
        if not record:
            raise RuntimeError("An engine stopped during final identity verification")
        mask = None
        for item in Path(f"/proc/{record['pid']}/environ").read_bytes().split(b"\0"):
            if item.startswith(b"CUDA_VISIBLE_DEVICES="):
                mask = item.partition(b"=")[2].decode()
        selected = driver_metadata(mask, python=ENGINE_PYTHON)
        if selected.get("device_count") != 1 or selected["devices"][0]["uuid_v2"] != identity["uuid_v2"]:
            raise RuntimeError("Final engine mask differs from its unique selected MIG UUID")
        text = (ROOT / f".runtime/engine-pod2-7b-gpu{index}.log").read_text(errors="replace")
        bfloat16, cache_off = "dtype=torch.bfloat16" in text, "enable_prefix_caching=False" in text
        if not bfloat16 or not cache_off:
            raise RuntimeError("Engine logs do not confirm the required precision and cache configuration")
        rows.append({"logical_device": index, "cuda_selector": identity["selector"], "actual_process_mask": mask,
                     "pid": record["pid"], "engine_port": 8200 + index,
                     "worker_id": f"secondpod-7b-mig{index}", "worker_port": 9200 + index,
                     "slots": 8, "bfloat16_observed": bfloat16, "prefix_caching_disabled_observed": cache_off,
                     "generation_evidence": "six-engine 48-request probe and frozen platform workloads" if index < 6 else "expansion per-engine genuine generation probe"})
    unchanged = {index: manager.owned_record(ROOT / f".runtime/engine-pod2-7b-gpu{index}.json")["pid"] == pid for index, pid in before.items()}
    if not all(unchanged.values()):
        raise RuntimeError("An original six-engine process identity changed during expansion")
    report = {"observed_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(), "status": "ready",
              "engine_count": len(rows), "worker_count": len(rows), "worker_slots_total": 8 * len(rows),
              "model": "Qwen/Qwen2.5-7B-Instruct", "revision": "a09a35458c702b33eeacc393d103063234e8bc28",
              "dtype": "bfloat16", "context_limit": 4096, "max_num_seqs": 8, "prefix_caching": False,
              "supervised_service_count": 35, "all_supervised_services_ready": True,
              "original_engine_pids_unchanged": unchanged,
              "additional_engine_generation_probes_passed": expansion["passed_count"], "engines": rows}
    supervisor.private_write(ROOT / ".runtime/pod2-16mig-ready.json", report)
    print(json.dumps(report), flush=True)


if __name__ == "__main__":
    main()
