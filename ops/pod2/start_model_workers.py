#!/usr/bin/env python3
"""Activate prepared per-MIG workers after the operator's control-plane cutover.

This command does not write or transmit new credentials. Each private worker
configuration must already exist. Run it only after the shared 7B control plane
and the server-side Pod link have passed their separate readiness checks.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path
import stat
import subprocess
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
MODEL = "Qwen/Qwen2.5-7B-Instruct"
REVISION = "a09a35458c702b33eeacc393d103063234e8bc28"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--devices", nargs="+", type=int, default=list(range(6)))
    parser.add_argument("--control-plane-url", default="http://127.0.0.1:8080",
                        choices=("http://127.0.0.1:8080", "http://127.0.0.1:18084"))
    args = parser.parse_args()
    if len(set(args.devices)) != len(args.devices) or any(device not in range(16) for device in args.devices):
        parser.error("Choose distinct prepared MIG devices 0 through 15")
    spec = importlib.util.spec_from_file_location("batchforge_manage", ROOT / "ops/manage.py")
    manager = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(manager)
    # Validate every engine/config before starting any worker.
    for device in args.devices:
        path = ROOT / f".env.worker-pod2-7b-mig{device}"
        if stat.S_IMODE(path.stat().st_mode) != 0o600:
            raise RuntimeError("Worker configuration must have mode 0600")
        env = manager.load_env(path)
        expected = {"MODEL_NAME": MODEL, "WORKER_ID": f"secondpod-7b-mig{device}",
                    "CONTROL_PLANE_URL": args.control_plane_url,
                    "INFERENCE_BASE_URLS": f"http://127.0.0.1:{8200 + device}",
                    "ENGINE_MAX_MODEL_LEN": "4096", "WORKER_TOKEN_BUDGET": "32768",
                    "WORKER_MAX_INFLIGHT": "8", "WORKER_METRICS_PORT": str(9200 + device)}
        if any(env.get(key) != value for key, value in expected.items()):
            raise RuntimeError("Prepared worker configuration differs from the verified model topology")
        with urllib.request.urlopen(f"http://127.0.0.1:{8200 + device}/v1/models", timeout=5) as response:
            models = json.load(response)["data"]
        expected_root = f"/root/batchforge-runtime/models/Qwen2.5-7B-Instruct/{REVISION}"
        if not any(model["id"] == MODEL and model.get("root") == expected_root
                   and model.get("max_model_len") == 4096 for model in models):
            raise RuntimeError("Engine does not advertise the verified local model/context")
    for device in args.devices:
        subprocess.run([str(ROOT.parent / "venv/bin/python"), "ops/manage.py", "start", "worker",
            "--name", f"pod2-7b-mig{device}", "--env-file", f".env.worker-pod2-7b-mig{device}",
            "--python", str(ROOT.parent / "venv/bin/python")], cwd=ROOT, check=True)
    print(json.dumps({"event": "model_workers_started", "devices": args.devices,
                      "worker_ids": [f"secondpod-7b-mig{i}" for i in args.devices]}), flush=True)


if __name__ == "__main__":
    main()
