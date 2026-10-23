#!/usr/bin/env python3
"""Bounded CUDA allocation proof for explicitly selected, visible MIG UUIDs.

Run only outside a frozen inference baseline. This loads no model, changes no
device geometry, and never stops a process. Each subprocess releases its tiny
test tensor and CUDA context on exit before the next device is checked.
"""
from __future__ import annotations

import argparse
import datetime
import json
import os
from pathlib import Path
import subprocess
import sys
import time

from cuda_devices import driver_metadata, select_mig_device

ROOT = Path(__file__).resolve().parents[2]
ALLOCATION_PROBE = r'''
import json,torch
if torch.cuda.device_count()!=1:
 raise RuntimeError("An explicit MIG UUID must expose exactly one CUDA device")
free,total=torch.cuda.mem_get_info(0)
if free < int(total*0.82)+512*1024**2:
 print(json.dumps({"status":"insufficient_free_memory","free_bytes":free,"total_bytes":total}))
else:
 tensor=torch.ones(1024,dtype=torch.float32,device="cuda:0")
 checksum=float(tensor.sum().item())
 torch.cuda.synchronize(0)
 if checksum!=1024:
  raise RuntimeError("Bounded CUDA operation failed its checksum")
 print(json.dumps({"status":"cuda_allocation_and_kernel_ok","free_bytes_before":free,
  "total_bytes":total,"tensor_bytes":tensor.numel()*tensor.element_size(),"checksum":checksum,
  "torch_visible_device_count":torch.cuda.device_count()}))
'''


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--devices", nargs="+", type=int, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    if not args.devices or len(set(args.devices)) != len(args.devices) or any(index not in range(16) for index in args.devices):
        parser.error("Select distinct logical MIG devices 0 through 15")
    inventory = driver_metadata()
    identities = [select_mig_device(inventory, index) for index in args.devices]
    started = time.monotonic()
    rows = []
    for index, identity in zip(args.devices, identities):
        selector = identity["selector"]
        selected = driver_metadata(selector)
        if selected.get("device_count") != 1 or selected["devices"][0]["uuid_v2"] != identity["uuid_v2"]:
            raise RuntimeError("Selected MIG identity changed before the bounded allocation")
        result = subprocess.run([sys.executable, "-c", ALLOCATION_PROBE],
                                env={**os.environ, "CUDA_VISIBLE_DEVICES": selector},
                                capture_output=True, text=True, timeout=60)
        if result.returncode:
            row = {"status": "cuda_probe_failed", "exit_code": result.returncode}
        else:
            row = json.loads(result.stdout)
        rows.append({"logical_device": index, "cuda_selector": selector, "driver_uuid_verified": True, **row})
    report = {"observed_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
              "duration_seconds": round(time.monotonic() - started, 6),
              "model_loaded": False, "processes_stopped": 0, "devices": rows}
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report), flush=True)
    if any(row["status"] != "cuda_allocation_and_kernel_ok" for row in rows):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
