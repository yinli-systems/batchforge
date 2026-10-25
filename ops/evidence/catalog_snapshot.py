"""Build a capability-catalog view of one measured host from committed evidence.

    python -m ops.evidence.catalog_snapshot --inventory docs/evidence/gpu-snapshot-2026-10-25.json \
        --topology ops/reports/primary-qwen235b-gpu-topology-2026-10-24.json --now 2026-10-25T12:00:00+00:00

No heartbeat is read from any file here, so liveness is reported as unknown and the node is not eligible.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path

from backend.placement import Job, Node, Policy, describe, place_check
from .topology import nvlink_pairs, parse_topo_matrix


def topo_text(report: dict) -> tuple[str, str]:
    for observation in report["observations"]:
        if observation["argv"][:2] == ["nvidia-smi", "topo"]:
            return observation["stdout"], report["observed_at"]
    raise ValueError("no `nvidia-smi topo -m` observation in report")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--topology", type=Path, required=True)
    parser.add_argument("--now", required=True)
    parser.add_argument("--node-id", default="four-h100-host")
    args = parser.parse_args(argv)
    inventory = json.loads(args.inventory.read_text())
    text, topo_at = topo_text(json.loads(args.topology.read_text()))
    topo = parse_topo_matrix(text)
    node = Node(args.node_id, gpu_count=inventory["gpu_count"], gpu_models=tuple(inventory["gpu_models"]),
                nvlink_pairs=frozenset(nvlink_pairs(topo)), inventory_at=inventory["collected_at"],
                topology_at=topo_at, heartbeat_at=None)
    now = dt.datetime.fromisoformat(args.now)
    result = {
        "schema": "batchforge.capability_catalog/1",
        "now": args.now,
        "policy": {"stale_after_s": Policy().stale_after_s, "expired_after_s": Policy().expired_after_s},
        "node": describe(node, now, Policy()),
        "placement": {
            name: place_check(Job(name, gpus=gpus, require_nvlink=nv), [node], now, Policy()).__dict__
            for name, gpus, nv in [("plain_2gpu", 2, False), ("nvlink_2gpu", 2, True), ("nvlink_4gpu", 4, True)]
        },
        "measured_inputs": {"inventory": str(args.inventory), "topology": str(args.topology),
                            "topology_observed_at": topo_at},
        "not_measured": ["heartbeat for this node (no heartbeat recorded)", "NCCL capability",
                         "network bandwidth", "node availability for scheduling"],
    }
    print(json.dumps(result, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
