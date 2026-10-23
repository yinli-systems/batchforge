"""Read only loopback worker readiness and nonsecret Prometheus measurements."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
from pathlib import Path
from urllib.request import ProxyHandler, build_opener

from prometheus_client.parser import text_string_to_metric_families


def inspect(port):
    http = build_opener(ProxyHandler({}))
    result = {"port": port}
    try:
        with http.open(f"http://127.0.0.1:{port}/readyz", timeout=3) as response:
            result["ready"] = response.status == 200
    except Exception as exc:
        result["ready"] = False
        result["readiness_error_type"] = type(exc).__name__
    try:
        with http.open(f"http://127.0.0.1:{port}/metrics", timeout=3) as response:
            metrics = response.read(1024 * 1024).decode()
        result["metrics"] = [{"name": sample.name, "labels": sample.labels, "value": sample.value}
            for family in text_string_to_metric_families(metrics) for sample in family.samples
            if sample.name.startswith("batchforge_worker_") or sample.name == "process_start_time_seconds"]
    except Exception as exc:
        result["metrics_error_type"] = type(exc).__name__
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ports", nargs="+", type=int, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    if len(args.ports) > 64 or len(set(args.ports)) != len(args.ports) or not all(1 <= p <= 65535 for p in args.ports):
        parser.error("Use at most 64 distinct loopback ports")
    with ThreadPoolExecutor(max_workers=min(8, len(args.ports))) as pool:
        result = {"observed_at": datetime.now(timezone.utc).isoformat(), "workers": list(pool.map(inspect, args.ports))}
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"observed_at": result["observed_at"], "ready": sum(worker["ready"] for worker in result["workers"]), "total": len(args.ports)}))


if __name__ == "__main__":
    main()
