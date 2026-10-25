import pytest
import datetime as dt

from ops.evidence.capability_catalog import Job, Node, Policy, describe, liveness, place_check
from ops.evidence.topology import nvlink_pairs, parse_topo_matrix

NOW = dt.datetime(2026, 10, 25, 12, 0, 0, tzinfo=dt.timezone.utc)
FRESH = (NOW - dt.timedelta(seconds=5)).isoformat()
STALE = (NOW - dt.timedelta(seconds=60)).isoformat()
EXPIRED = (NOW - dt.timedelta(seconds=600)).isoformat()
ALL_NVLINK = frozenset({"0-1", "0-2", "0-3", "1-2", "1-3", "2-3"})
MISSING_ONE = frozenset({"0-1", "0-2", "1-2", "0-3", "1-3"})

# Shape of the `nvidia-smi topo -m` GPU block captured on the four-H100 host (ops/reports, 2026-10-24).
TOPO = (
    "\t\u001b[4mGPU0\tGPU1\tGPU2\tGPU3\tNIC0\tNIC1\tCPU Affinity\tNUMA Affinity\tGPU NUMA ID\u001b[0m\n"
    "GPU0\t X \tNV18\tNV18\tNV18\tSYS\tSYS\t0-51\t0\t\tN/A\n"
    "GPU1\tNV18\t X \tNV18\tNV18\tSYS\tSYS\t0-51\t0\t\tN/A\n"
    "GPU2\tNV18\tNV18\t X \tNV18\tNODE\tNODE\t52-103\t1\t\tN/A\n"
    "GPU3\tNV18\tNV18\tNV18\t X \tPXB\tPXB\t52-103\t1\t\tN/A\n"
)


def test_parses_measured_nvlink_pairs_from_topology_matrix():
    topo = parse_topo_matrix(TOPO)
    assert topo["gpus"] == [0, 1, 2, 3]
    assert nvlink_pairs(topo) == ALL_NVLINK
    assert topo["links"]["0-2"] == "NV18"


def test_non_nvlink_link_classes_are_not_nvlink():
    pcie = TOPO.replace("GPU0\t X \tNV18\tNV18", "GPU0\t X \tNODE\tNV18", 1).replace(
        "GPU1\tNV18\t X ", "GPU1\tNODE\t X ", 1)
    topo = parse_topo_matrix(pcie)
    assert topo["links"]["0-1"] == "NODE"
    assert "0-1" not in nvlink_pairs(topo)
    assert "0-2" in nvlink_pairs(topo)


def test_asymmetric_matrix_is_rejected_not_silently_overwritten():
    broken = TOPO.replace("GPU0\t X \tNV18\tNV18", "GPU0\t X \tNODE\tNV18", 1)
    with pytest.raises(ValueError):
        parse_topo_matrix(broken)


def test_unknown_topology_is_never_eligible_for_nvlink_jobs_but_fine_for_plain_jobs():
    node = Node("a", gpu_count=4, inventory_at=FRESH, heartbeat_at=FRESH)
    nvlink = place_check(Job("j1", gpus=2, require_nvlink=True), [node], NOW, Policy())
    plain = place_check(Job("j2", gpus=2), [node], NOW, Policy())
    assert nvlink.eligible == [] and "unknown" in nvlink.rejected["a"]
    assert plain.eligible == ["a"]


def test_stale_and_expired_heartbeats_exclude_a_node_even_with_full_facts():
    stale = Node("s", gpu_count=4, nvlink_pairs=ALL_NVLINK, heartbeat_at=STALE)
    expired = Node("e", gpu_count=4, nvlink_pairs=ALL_NVLINK, heartbeat_at=EXPIRED)
    decision = place_check(Job("j", gpus=4, require_nvlink=True), [stale, expired], NOW, Policy())
    assert decision.eligible == []
    assert decision.rejected == {"e": "heartbeat expired", "s": "heartbeat stale"}
    assert liveness(Node("n"), NOW, Policy()) == "unknown"


def test_full_clique_is_required_not_a_pair_count():
    node = Node("p", gpu_count=4, nvlink_pairs=MISSING_ONE, heartbeat_at=FRESH)
    four = place_check(Job("j4", gpus=4, require_nvlink=True), [node], NOW, Policy())
    two = place_check(Job("j2", gpus=2, require_nvlink=True), [node], NOW, Policy())
    assert four.eligible == []
    assert two.eligible == ["p"]


def test_gpu_count_unknown_is_not_eligible_and_order_is_deterministic():
    nodes = [Node("z", gpu_count=4, heartbeat_at=FRESH), Node("b", gpu_count=None, heartbeat_at=FRESH),
             Node("a", gpu_count=8, heartbeat_at=FRESH)]
    first = place_check(Job("j", gpus=2), nodes, NOW, Policy())
    second = place_check(Job("j", gpus=2), list(reversed(nodes)), NOW, Policy())
    assert first.eligible == ["a", "z"] and first == second
    assert first.rejected == {"b": "gpu count unknown"}


def test_describe_labels_every_fact_with_status():
    view = describe(Node("n", gpu_count=4, heartbeat_at=FRESH, inventory_at=FRESH), NOW, Policy())
    assert view["gpu_count"]["status"] == "measured"
    assert view["nvlink"] == {"status": "unknown", "value": None, "measured_at": None}
