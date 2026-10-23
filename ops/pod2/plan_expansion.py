#!/usr/bin/env python3
"""Prepare an inactive 16-MIG service manifest and optional worker configs.

No service is started, stopped, or signalled. No inference request or GPU memory
probe is performed. The existing application worker token is reused only inside
mode-600 files on this same authorized Pod and is never printed.
"""
from __future__ import annotations

import argparse
import datetime
import importlib.util
import json
import os
from pathlib import Path
import stat

from cuda_devices import driver_metadata, verified_mig_devices

ROOT = Path(__file__).resolve().parents[2]
RUNTIME = ROOT.parent
MODEL = "Qwen/Qwen2.5-7B-Instruct"


def private_create(path: Path, config: dict[str, str]) -> None:
    content = "".join(f"{key}={value!r}\n" for key, value in config.items())
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "w") as handle:
        handle.write(content)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepare-workers", action="store_true", help="Create only missing same-Pod private worker configurations")
    args = parser.parse_args()
    devices = verified_mig_devices(driver_metadata(python=str(RUNTIME / "engine-venv/bin/python")))
    if len(devices) != 16:
        raise RuntimeError("The proposed expansion requires exactly 16 verified visible MIG identities")
    spec = importlib.util.spec_from_file_location("batchforge_manage", ROOT / "ops/manage.py")
    manager = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(manager)
    template_path = ROOT / ".env.worker-pod2-7b-mig0"
    if template_path.is_symlink() or stat.S_IMODE(template_path.stat().st_mode) != 0o600:
        raise RuntimeError("Existing same-Pod worker token source must be an owner-only regular file")
    template = manager.load_env(template_path)
    token = template.get("BATCHFORGE_WORKER_TOKEN", "")
    if len(token) < 24 or template.get("MODEL_NAME") != MODEL or template.get("CONTROL_PLANE_URL") != "http://127.0.0.1:8080":
        raise RuntimeError("Worker template must use the current local 7B platform and authorized worker token")
    base = {"role": "api", "python": str(RUNTIME / "venv/bin/python")}
    services = [
        {**base, "name": "a", "env_file": str(ROOT / ".env.api-a"), "port": 8082},
        {**base, "name": "b", "env_file": str(ROOT / ".env.api-b"), "port": 8083},
        {**base, "role": "gateway", "name": "edge", "env_file": str(ROOT / ".env.gateway-pod2"), "port": 8080},
    ]
    rows = []
    for index, device in enumerate(devices):
        worker_path = ROOT / f".env.worker-pod2-7b-mig{index}"
        config = {
            "BATCHFORGE_WORKER_TOKEN": token, "MODEL_NAME": MODEL,
            "WORKER_ID": f"secondpod-7b-mig{index}", "WORKER_NAME": f"POD2/Blackwell-7B-MIG{index}",
            "CONTROL_PLANE_URL": "http://127.0.0.1:8080", "INFERENCE_BASE_URLS": f"http://127.0.0.1:{8200 + index}",
            "CONTROL_PLANE_URLS": "http://127.0.0.1:8080,http://127.0.0.1:8082,http://127.0.0.1:8083",
            "WORKER_MAX_INFLIGHT": "8", "WORKER_TOKEN_BUDGET": "32768", "ENGINE_MAX_MODEL_LEN": "4096",
            "INFERENCE_TIMEOUT_SECONDS": "240", "WORKER_SHUTDOWN_SECONDS": "300",
            "WORKER_METRICS_HOST": "127.0.0.1", "WORKER_METRICS_PORT": str(9200 + index),
        }
        if args.prepare_workers and not worker_path.exists():
            private_create(worker_path, config)
        if worker_path.exists():
            if worker_path.is_symlink() or stat.S_IMODE(worker_path.stat().st_mode) != 0o600:
                raise RuntimeError("Prepared worker configuration has unsafe permissions")
            actual = manager.load_env(worker_path)
            if args.prepare_workers and not actual.get("CONTROL_PLANE_URLS"):
                temporary = worker_path.with_name(worker_path.name + ".origins-tmp")
                descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
                with os.fdopen(descriptor, "w") as handle:
                    handle.write(worker_path.read_text().rstrip() + f"\nCONTROL_PLANE_URLS={config['CONTROL_PLANE_URLS']!r}\n")
                temporary.replace(worker_path)
                actual = manager.load_env(worker_path)
            for key in ("BATCHFORGE_WORKER_TOKEN", "MODEL_NAME", "WORKER_ID", "CONTROL_PLANE_URL", "CONTROL_PLANE_URLS", "INFERENCE_BASE_URLS", "WORKER_MAX_INFLIGHT", "WORKER_TOKEN_BUDGET", "ENGINE_MAX_MODEL_LEN", "WORKER_METRICS_PORT"):
                if actual.get(key) != config[key]:
                    raise RuntimeError("Existing worker configuration differs from the expansion plan")
        engine_path = ROOT / f".env.engine-pod2-7b-gpu{index}"
        services.extend([
            {"role": "engine", "name": f"pod2-7b-gpu{index}", "env_file": str(engine_path),
             "python": str(RUNTIME / "engine-venv/bin/python"), "port": 8200 + index, "gpu": device["selector"]},
            {"role": "worker", "name": f"pod2-7b-mig{index}", "env_file": str(worker_path),
             "python": str(RUNTIME / "venv/bin/python"), "port": 9200 + index},
        ])
        rows.append({"logical_device": index, "cuda_selector": device["selector"],
                     "engine_port": 8200 + index, "worker_id": config["WORKER_ID"],
                     "metrics_port": 9200 + index, "worker_config_exists": worker_path.exists(),
                     "engine_config_exists": engine_path.exists()})
    target = ROOT / ".runtime/pod2-services-expanded.json"
    descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "w") as handle:
        json.dump({"services": services}, handle, indent=2)
        handle.write("\n")
    report = {"observed_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
              "status": "inactive_prepared_plan", "service_count": len(services), "devices": rows,
              "model": MODEL, "dtype": "bfloat16", "max_model_len": 4096, "max_num_seqs": 8,
              "prefix_caching": False, "gpu_memory_utilization": 0.82,
              "minimum_free_memory": "total * 0.82 + 512 MiB per selected MIG",
              "services_started": 0, "services_stopped": 0, "inference_requests": 0}
    (ROOT / ".runtime/pod2-expansion-plan.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report), flush=True)


if __name__ == "__main__":
    main()
