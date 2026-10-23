#!/usr/bin/env python3
"""Start one revision-pinned 7B replica on a verified CUDA-visible MIG device."""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
from pathlib import Path
import subprocess

from cuda_devices import driver_metadata, select_mig_device

ROOT = Path(__file__).resolve().parents[2]
RUNTIME = ROOT.parent
REVISION = "a09a35458c702b33eeacc393d103063234e8bc28"
MODEL = "Qwen/Qwen2.5-7B-Instruct"
PYTHON = str(RUNTIME / "venv/bin/python")
ENGINE_PYTHON = str(RUNTIME / "engine-venv/bin/python")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gpu", type=int, required=True, choices=range(16), help="Logical CUDA Driver MIG index, not physical NVML index")
    parser.add_argument("--mig-uuid", help="Optional exact MIG-UUID assertion against the Driver inventory")
    parser.add_argument("--retire-legacy", action="store_true", help="Drain only recorded BatchForge 0.5B worker/engine")
    args = parser.parse_args()
    manifest = json.loads((ROOT / ".runtime/pod2-qwen7b-model.json").read_text())
    if manifest.get("status") != "verified" or manifest.get("revision") != REVISION:
        raise RuntimeError("Pinned model download and SHA256 verification must finish first")
    spec = importlib.util.spec_from_file_location("batchforge_manage", ROOT / "ops/manage.py")
    manager = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(manager)

    def manage(*arguments: str) -> None:
        subprocess.run([PYTHON, "ops/manage.py", *arguments], cwd=ROOT, check=True)

    if args.retire_legacy:
        manage("stop", "worker", "--name", "pod2-gpu0")
        manage("stop", "engine", "--name", "gpu0")
    name = f"pod2-7b-gpu{args.gpu}"
    device = select_mig_device(driver_metadata(python=ENGINE_PYTHON), args.gpu, args.mig_uuid)
    if manager.owned_record(ROOT / f".runtime/engine-{name}.json"):
        manage("status", "engine", "--name", name)
        return
    selector = device["selector"]
    # MIG UUIDs force Torch's NVML discovery to fall back to CUDA Runtime. A
    # numeric index >= the physical GPU count can otherwise appear invisible.
    os.environ["CUDA_VISIBLE_DEVICES"] = selector
    import torch
    if torch.cuda.device_count() != 1:
        raise RuntimeError("The selected MIG UUID must expose exactly one CUDA device")
    selected = driver_metadata(selector, python=ENGINE_PYTHON)
    if selected.get("device_count") != 1 or selected["devices"][0]["uuid_v2"] != device["uuid_v2"]:
        raise RuntimeError("Selected MIG UUID mapping changed during the preflight")
    free, total = torch.cuda.mem_get_info(0)
    fraction = 0.82
    if free < int(total * fraction) + 512 * 1024**2:
        raise RuntimeError("Insufficient free GPU memory; no unrelated workload will be stopped")
    config = {
        "MODEL_NAME": MODEL, "ENGINE_MODEL_PATH": manifest["model_path"],
        "ENGINE_REVISION": REVISION, "ENGINE_DTYPE": "bfloat16",
        "ENGINE_MAX_MODEL_LEN": "4096", "ENGINE_MAX_NUM_SEQS": "8",
        "ENGINE_MAX_NUM_BATCHED_TOKENS": "4096", "ENGINE_ENABLE_PREFIX_CACHING": "0",
        "ENGINE_GPU_MEMORY_UTILIZATION": str(fraction), "HF_HOME": str(RUNTIME / "hf-home"),
        "HF_HUB_OFFLINE": "1", "VLLM_CACHE_ROOT": str(RUNTIME / f"vllm-cache-7b-gpu{args.gpu}"),
        "TORCHINDUCTOR_CACHE_DIR": str(RUNTIME / f"torchinductor-cache-7b-gpu{args.gpu}"),
        "NUMBA_CACHE_DIR": str(RUNTIME / f"numba-cache-7b-gpu{args.gpu}"),
        "BATCHFORGE_MIG_PLATFORM": "1", "VLLM_PLUGINS": "batchforge_mig",
        "PYTHONPATH": str(ROOT / "ops/pod2/mig_platform"),
        "VLLM_WORKER_MULTIPROC_METHOD": "spawn",
    }
    env_file = ROOT / f".env.engine-pod2-7b-gpu{args.gpu}"
    env_file.write_text("".join(f"{key}={value!r}\n" for key, value in config.items()))
    env_file.chmod(0o600)
    port = 8200 + args.gpu
    report = {"device": args.gpu, "device_name": torch.cuda.get_device_name(0), "cuda_selector": selector,
              "free_bytes_before": free, "total_bytes": total, "config": config,
              "port": port, "manager_name": name}
    (ROOT / f".runtime/pod2-7b-start-gpu{args.gpu}.json").write_text(json.dumps(report, indent=2) + "\n")
    manage("start", "engine", "--name", name, "--gpu", selector, "--port", str(port),
           "--env-file", str(env_file), "--python", ENGINE_PYTHON)
    print(json.dumps({"event": "engine_started", "gpu": args.gpu, "port": port,
                      "free_bytes_before": free, "model": MODEL, "revision": REVISION,
                      "dtype": "bfloat16", "max_model_len": 4096, "max_num_seqs": 8,
                      "cuda_selector": selector}), flush=True)


if __name__ == "__main__":
    main()
