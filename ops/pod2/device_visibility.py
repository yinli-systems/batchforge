#!/usr/bin/env python3
"""Read only container GPU visibility and verified BatchForge engine masks.

No GPU allocations, process enumeration, device reconfiguration, or environment
dumping. The two GPU visibility variables are the only environment values read
from PID 1 or the six application-owned engine processes.
"""
from __future__ import annotations

import datetime
import importlib.util
import json
import os
from pathlib import Path
import subprocess

import torch
from cuda_devices import driver_metadata, verified_mig_devices

ROOT = Path(__file__).resolve().parents[2]
KEYS = ("CUDA_VISIBLE_DEVICES", "NVIDIA_VISIBLE_DEVICES")


def visible_environment(pid: int) -> dict[str, str | None]:
    found = dict.fromkeys(KEYS)
    for item in Path(f"/proc/{pid}/environ").read_bytes().split(b"\0"):
        key, separator, value = item.partition(b"=")
        if separator and key.decode(errors="replace") in KEYS:
            found[key.decode()] = value.decode(errors="replace")
    return found


def command_output(command: list[str]) -> dict:
    result = subprocess.run(command, capture_output=True, text=True, timeout=20)
    return {"exit_code": result.returncode, "stdout": result.stdout.strip(),
            "stderr": result.stderr.strip()}


def main() -> None:
    spec = importlib.util.spec_from_file_location("batchforge_manage", ROOT / "ops/manage.py")
    manager = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(manager)
    torch_count_before_initialization = torch.cuda.device_count()
    torch_nvml_count = torch.cuda._device_count_nvml()
    devices = []
    for index in range(torch_count_before_initialization):
        properties = torch.cuda.get_device_properties(index)
        devices.append({"cuda_index": index, "uuid": str(properties.uuid),
                        "name": properties.name, "total_memory": properties.total_memory,
                        "multiprocessor_count": properties.multi_processor_count,
                        "compute_capability": [properties.major, properties.minor]})
    driver_inventory = driver_metadata(None)
    engines = []
    for index in range(len(verified_mig_devices(driver_inventory))):
        name = f"pod2-7b-gpu{index}"
        record = manager.owned_record(ROOT / f".runtime/engine-{name}.json")
        row = {"name": name, "owned_identity_verified": bool(record)}
        if record:
            mask = visible_environment(record["pid"])
            row.update({"pid": record["pid"], "visibility": mask})
            selector = mask["CUDA_VISIBLE_DEVICES"]
            if selector and selector.isdigit() and int(selector) < len(devices):
                row["torch_parent_uuid_from_unmasked_index"] = devices[int(selector)]["uuid"]
            row["selected_device_driver_metadata"] = driver_metadata(selector)
            row["mapping_method"] = "fresh_metadata_only_driver_process_with_same_owned_engine_mask"
        engines.append(row)
    report = {"observed_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
              "process_visibility": {key: os.environ.get(key) for key in KEYS},
              "pid1_visibility": visible_environment(1),
              "nvidia_smi_list": command_output(["nvidia-smi", "-L"]),
              "nvidia_smi_driver": command_output(["nvidia-smi", "--query-gpu=index,uuid,driver_version", "--format=csv,noheader"]),
              "torch_version": torch.__version__, "cuda_runtime": torch.version.cuda,
              "torch_device_count_before_initialization": torch_count_before_initialization,
              "torch_nvml_device_count": torch_nvml_count,
              "torch_device_count_after_initialization": torch.cuda.device_count(),
              "torch_runtime_device_count": torch._C._cuda_getDeviceCount(),
              "torch_property_uuid_semantics_observed": "physical_parent_gpu_uuid_not_unique_mig_uuid",
              "driver_default_metadata": driver_inventory,
              "cuda_visible_devices": devices, "engines": engines,
              "scope": "metadata_only_no_new_model_no_gpu_allocation_no_geometry_change"}
    target = ROOT / ".runtime/pod2-device-visibility.json"
    target.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report), flush=True)


if __name__ == "__main__":
    main()
