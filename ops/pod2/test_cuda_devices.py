"""MIG inventory regressions observed on the 16-instance Runpod deployment."""
from __future__ import annotations

import copy
import uuid
import threading
import time

import pytest

from ops.pod2.cuda_devices import select_mig_device, verified_mig_devices
from ops.pod2 import expand_engines


def inventory(count=16):
    return {"device_count": count, "devices": [
        {"cuda_index": index, "uuid_v2": str(uuid.UUID(int=index + 1)),
         "name": "NVIDIA RTX PRO 6000 Blackwell Server Edition MIG 1g.24gb"}
        for index in range(count)]}


def test_all_16_driver_instances_are_selected_by_uuid_despite_six_physical_gpus():
    source = inventory()
    devices = verified_mig_devices(source)
    assert len(devices) == 16
    # In the affected Torch NVML path a numerical selector >=6 returns zero.
    # Every planned engine instead gets a complete MIG UUID, including 6..15.
    for index in range(16):
        selected = select_mig_device(source, index)
        assert selected["selector"] == f"MIG-{source['devices'][index]['uuid_v2']}"
    assert len({device["selector"] for device in devices}) == 16


def test_duplicate_mig_identities_and_noncontiguous_indices_are_rejected():
    duplicate = inventory()
    duplicate["devices"][8]["uuid_v2"] = duplicate["devices"][0]["uuid_v2"]
    with pytest.raises(ValueError, match="duplicate"):
        verified_mig_devices(duplicate)
    noncontiguous = inventory()
    noncontiguous["devices"][8]["cuda_index"] = 99
    with pytest.raises(ValueError, match="contiguous"):
        verified_mig_devices(noncontiguous)


def test_count_mismatch_non_mig_devices_and_wrong_uuid_assertion_are_rejected():
    source = inventory()
    wrong_count = copy.deepcopy(source)
    wrong_count["device_count"] = 6
    with pytest.raises(ValueError, match="count"):
        verified_mig_devices(wrong_count)
    non_mig = inventory()
    non_mig["devices"][0]["name"] = "NVIDIA RTX PRO 6000 Blackwell Server Edition"
    with pytest.raises(ValueError, match="MIG"):
        verified_mig_devices(non_mig)
    with pytest.raises(ValueError, match="differs"):
        select_mig_device(source, 15, explicit_uuid="MIG-00000000-0000-0000-0000-000000000001")
    for index in (-1, 16, True):
        with pytest.raises(ValueError, match="not CUDA-visible"):
            select_mig_device(source, index)


@pytest.mark.parametrize("failed_device", [None, 12])
def test_expansion_preserves_existing_six_and_bounds_model_startups_to_two(monkeypatch, failed_device):
    monkeypatch.setattr(expand_engines, "driver_metadata", inventory)
    snapshots = []
    monkeypatch.setattr(expand_engines, "atomic_report", lambda value: snapshots.append(copy.deepcopy(value)))
    lock = threading.Lock()
    state = {"active": 0, "peak": 0, "indices": []}

    def fake_expand(index, identity):
        with lock:
            state["active"] += 1
            state["peak"] = max(state["peak"], state["active"])
            state["indices"].append(index)
        time.sleep(.005)
        with lock:
            state["active"] -= 1
        return {"logical_device": index, "cuda_selector": identity["selector"], "passed": index != failed_device}

    monkeypatch.setattr(expand_engines, "expand", fake_expand)
    if failed_device is None:
        expand_engines.main()
        assert snapshots[-1]["status"] == "complete"
        assert snapshots[-1]["passed_count"] == 10
    else:
        with pytest.raises(SystemExit) as exception:
            expand_engines.main()
        assert exception.value.code == 1
        assert snapshots[-1]["status"] == "incomplete"
        assert snapshots[-1]["passed_count"] == 9
    assert sorted(state["indices"]) == list(range(6, 16))
    assert state["peak"] == 2
    assert snapshots[-1]["workers_started"] == snapshots[-1]["processes_stopped"] == 0
