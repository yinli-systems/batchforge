"""Node capability catalog with explicit provenance and freshness.

Every fact carries its status: `measured` (from a read-only collector, with a time),
or absent, which means unknown. Nothing is inferred from a GPU model name.
A node's liveness is decided only by its heartbeat age, not by its inventory age.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field

FRESH, STALE, EXPIRED, UNKNOWN = "fresh", "stale", "expired", "unknown"


@dataclass(frozen=True)
class Node:
    node_id: str
    gpu_count: int | None = None          # measured by the inventory collector, or None
    gpu_models: tuple[str, ...] = ()      # measured
    nvlink_pairs: frozenset[str] | None = None   # measured topology; None = never measured
    inventory_at: str | None = None       # when the inventory was measured
    topology_at: str | None = None        # when the topology was measured
    heartbeat_at: str | None = None       # last heartbeat received


@dataclass
class Policy:
    stale_after_s: float = 30.0
    expired_after_s: float = 120.0


def _age(now: dt.datetime, stamp: str | None) -> float | None:
    if stamp is None:
        return None
    return (now - dt.datetime.fromisoformat(stamp)).total_seconds()


def liveness(node: Node, now: dt.datetime, policy: Policy) -> str:
    age = _age(now, node.heartbeat_at)
    if age is None:
        return UNKNOWN
    if age > policy.expired_after_s:
        return EXPIRED
    if age > policy.stale_after_s:
        return STALE
    return FRESH


def describe(node: Node, now: dt.datetime, policy: Policy) -> dict:
    """Read-only view of one node with every fact labelled by status."""
    return {
        "node_id": node.node_id,
        "liveness": liveness(node, now, policy),
        "gpu_count": {"status": "measured" if node.gpu_count is not None else UNKNOWN,
                      "value": node.gpu_count, "measured_at": node.inventory_at},
        "nvlink": {"status": "measured" if node.nvlink_pairs is not None else UNKNOWN,
                   "value": sorted(node.nvlink_pairs) if node.nvlink_pairs is not None else None,
                   "measured_at": node.topology_at},
    }


@dataclass
class Job:
    job_id: str
    gpus: int
    require_nvlink: bool = False


@dataclass
class Decision:
    job_id: str
    eligible: list[str] = field(default_factory=list)
    rejected: dict[str, str] = field(default_factory=dict)


def place_check(job: Job, nodes: list[Node], now: dt.datetime, policy: Policy) -> Decision:
    """Which nodes may run this job, with the reason each other node is excluded. Deterministic order."""
    decision = Decision(job_id=job.job_id)
    for node in sorted(nodes, key=lambda n: n.node_id):
        live = liveness(node, now, policy)
        if live != FRESH:
            decision.rejected[node.node_id] = f"heartbeat {live}"
        elif node.gpu_count is None:
            decision.rejected[node.node_id] = "gpu count unknown"
        elif node.gpu_count < job.gpus:
            decision.rejected[node.node_id] = f"{node.gpu_count} gpus measured, {job.gpus} required"
        elif job.require_nvlink and node.nvlink_pairs is None:
            decision.rejected[node.node_id] = "nvlink unknown (topology never measured)"
        elif job.require_nvlink and not has_nvlink_clique(node.gpu_count, node.nvlink_pairs, job.gpus):
            decision.rejected[node.node_id] = "no measured fully NVLink-connected group of required size"
        else:
            decision.eligible.append(node.node_id)
    return decision


def has_nvlink_clique(gpu_count: int, pairs: frozenset[str], size: int) -> bool:
    """True if some `size` GPUs on the node are pairwise connected by measured NVLink pairs."""
    from itertools import combinations

    for group in combinations(range(gpu_count), size):
        if all("-".join(map(str, sorted(pair))) in pairs for pair in combinations(group, 2)):
            return True
    return False


DTYPE_BYTES = {"fp16": 2.0, "bf16": 2.0, "fp8": 1.0, "int8": 1.0, "int4": 0.5}
RUNTIME_OVERHEAD = 1.10  # multiplier for activations and allocator slack; a fixed, documented assumption


def required_vram_bytes(*, params_billion: float, dtype: str, layers: int, kv_heads: int, head_dim: int,
                        max_seq_len: int, batch: int, kv_dtype: str = "fp16") -> int:
    """Estimate memory for weights plus KV cache. Every input is required; nothing is inferred from a model name.

    This is a planning estimate. Actual peak memory must be measured by the benchmark before a model is trusted.
    """
    if dtype not in DTYPE_BYTES or kv_dtype not in DTYPE_BYTES:
        raise ValueError("unknown dtype")
    if min(params_billion, layers, kv_heads, head_dim, max_seq_len, batch) <= 0:
        raise ValueError("all sizes must be positive")
    weights = params_billion * 1e9 * DTYPE_BYTES[dtype]
    kv_cache = 2 * layers * kv_heads * head_dim * max_seq_len * batch * DTYPE_BYTES[kv_dtype]
    return int((weights + kv_cache) * RUNTIME_OVERHEAD)


def fits_on_gpus(*, required_bytes: int, tensor_parallel: int, free_mib_per_gpu: list[float | None]) -> dict:
    """Reject unless each of `tensor_parallel` GPUs has measured free memory for its shard.

    Unknown (None) free memory is never treated as available.
    """
    if tensor_parallel < 1 or len(free_mib_per_gpu) < tensor_parallel:
        return {"fits": False, "reason": "not enough measured GPUs for tensor parallelism"}
    shard_mib = required_bytes / tensor_parallel / 1024**2
    short = [index for index, free in enumerate(free_mib_per_gpu[:tensor_parallel])
             if free is None or free < shard_mib]
    if short:
        return {"fits": False, "reason": f"measured free memory below {shard_mib:.0f} MiB shard on GPUs {short}",
                "shard_mib": round(shard_mib, 1)}
    return {"fits": True, "shard_mib": round(shard_mib, 1)}
