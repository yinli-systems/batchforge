#!/usr/bin/env python3
"""Collect only BatchForge-owned 7B engine records and CUDA device aggregates."""
from __future__ import annotations

import argparse
import datetime
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import urllib.request

from cuda_devices import driver_metadata, verified_mig_devices

ROOT = Path(__file__).resolve().parents[2]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    spec = importlib.util.spec_from_file_location("batchforge_manage", ROOT / "ops/manage.py")
    manager = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(manager)
    rows = []
    devices = verified_mig_devices(driver_metadata())
    for device, identity in enumerate(devices):
        name, port = f"pod2-7b-gpu{device}", 8200 + device
        record = manager.owned_record(ROOT / f".runtime/engine-{name}.json")
        row = {"device": device, "device_name": identity["name"], "cuda_selector": identity["selector"],
               "mig_uuid": identity["uuid_v2"], "running": bool(record),
               "port": port, "pid": record["pid"] if record else None}
        if record:
            probe = subprocess.run([sys.executable, "-c",
                "import json,torch; p=torch.cuda.get_device_properties(0); f,t=torch.cuda.mem_get_info(0); "
                "print(json.dumps({'free_bytes':f,'total_bytes':t,'multiprocessor_count':p.multi_processor_count,"
                "'compute_capability':[p.major,p.minor]}))"],
                env={**os.environ, "CUDA_VISIBLE_DEVICES": identity["selector"]},
                capture_output=True, text=True, check=True, timeout=30)
            row.update(json.loads(probe.stdout))
        log = ROOT / f".runtime/engine-{name}.log"
        if log.exists():
            text = log.read_text(errors="replace")
            row["engine_dtype_bfloat16_observed"] = "dtype=torch.bfloat16" in text
            row["prefix_caching_disabled_observed"] = "enable_prefix_caching=False" in text
            for key, pattern in (("reported_model_gib", r"Model loading took ([0-9.]+) GiB"),
                                 ("reported_kv_cache_gib", r"Available KV cache memory: ([0-9.]+) GiB")):
                values = re.findall(pattern, text)
                row[key] = float(values[-1]) if values else None
        if record:
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{port}/v1/models", timeout=5) as response:
                    catalog = json.load(response)
                row["advertised_models"] = [{key: model.get(key) for key in ("id", "root", "max_model_len")}
                                             for model in catalog["data"]]
            except Exception as error:
                row["catalog_error"] = type(error).__name__
        rows.append(row)
    report = {"observed_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
              "model_manifest": json.loads((ROOT / ".runtime/pod2-qwen7b-model.json").read_text()),
              "cuda_driver_visible_mig_count": len(devices),
              "owned_running_engine_count": sum(row["running"] for row in rows),
              "engines": rows}
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report), flush=True)


if __name__ == "__main__":
    main()
