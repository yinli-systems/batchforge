import json

import pytest

from ops.evidence.gpu_inventory import build_snapshot, parse_csv, run_nvidia_smi, GPU_FIELDS, PROCESS_FIELDS

GPU_CSV = (
    "0, NVIDIA H100 80GB HBM3, GPU-aaa, 81559, 74119, 97, 618.50, 61, P0\n"
    "1, NVIDIA H100 80GB HBM3, GPU-bbb, 81559, 74119, [N/A], [N/A], 58, P0\n"
)
PROCESS_CSV = "481798, python, GPU-aaa, 74000\n"


def test_parses_typed_gpu_rows_and_marks_unavailable_fields_as_none():
    rows = parse_csv(GPU_CSV, GPU_FIELDS)
    assert rows[0]["index"] == 0 and rows[0]["name"] == "NVIDIA H100 80GB HBM3"
    assert rows[0]["memory.total"] == 81559.0 and rows[0]["power.draw"] == 618.5
    assert rows[1]["utilization.gpu"] is None and rows[1]["power.draw"] is None


def test_rejects_rows_with_the_wrong_number_of_fields():
    with pytest.raises(ValueError):
        parse_csv("0, only-two", GPU_FIELDS)


def test_snapshot_totals_and_flags_processes_on_known_gpus():
    snap = build_snapshot(GPU_CSV, PROCESS_CSV, host="pod", collected_at="2026-10-25T00:00:00Z")
    assert snap["schema"] == "batchforge.gpu_inventory/1"
    assert snap["gpu_count"] == 2
    assert snap["gpu_models"] == ["NVIDIA H100 80GB HBM3"]
    assert snap["memory_total_mib"] == 163118.0
    assert snap["memory_used_mib"] == 148238.0
    assert snap["processes"][0]["gpu_known"] is True
    json.dumps(snap)  # must serialize cleanly


def test_unknown_gpu_uuid_is_reported_not_hidden():
    snap = build_snapshot(GPU_CSV, "1, orphan, GPU-zzz, 10\n", host="pod", collected_at="t")
    assert snap["processes"][0]["gpu_known"] is False


def test_collection_uses_only_read_only_queries():
    calls = []

    def fake(args):
        calls.append(args)
        return GPU_CSV if args[0].startswith("--query-gpu=") else PROCESS_CSV

    gpu, procs = run_nvidia_smi(fake)
    assert gpu == GPU_CSV and procs == PROCESS_CSV
    assert all(arg.startswith("--query-") or arg.startswith("--format=") for call in calls for arg in call)
    assert calls[0][0] == "--query-gpu=" + ",".join(GPU_FIELDS)
    assert calls[1][0] == "--query-compute-apps=" + ",".join(PROCESS_FIELDS)
