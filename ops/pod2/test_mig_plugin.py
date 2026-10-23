"""The compatibility selector is local, explicit, pinned, and UUID-only."""
from __future__ import annotations

import pytest

from ops.pod2.mig_platform import batchforge_mig_plugin as plugin


def test_plugin_is_inactive_without_application_optin(monkeypatch):
    monkeypatch.delenv("BATCHFORGE_MIG_PLATFORM", raising=False)
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "0")
    assert plugin.register() is None


def test_single_mig_uuid_selects_supported_official_non_nvml_platform(monkeypatch):
    monkeypatch.setenv("BATCHFORGE_MIG_PLATFORM", "1")
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "MIG-22019961-7f1b-58a7-89a2-eee1fd1a72d4")
    monkeypatch.setattr(plugin, "version", lambda _: "0.10.2")
    assert plugin.register() == "vllm.platforms.cuda.NonNvmlCudaPlatform"


@pytest.mark.parametrize("mask", ["0", "6", "MIG-broken", "MIG-22019961-7f1b-58a7-89a2-eee1fd1a72d4,0"])
def test_unscoped_or_multiple_devices_are_rejected(monkeypatch, mask):
    monkeypatch.setenv("BATCHFORGE_MIG_PLATFORM", "1")
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", mask)
    with pytest.raises(RuntimeError, match="exactly one"):
        plugin.register()


def test_unreviewed_vllm_version_is_rejected(monkeypatch):
    monkeypatch.setenv("BATCHFORGE_MIG_PLATFORM", "1")
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "MIG-22019961-7f1b-58a7-89a2-eee1fd1a72d4")
    monkeypatch.setattr(plugin, "version", lambda _: "0.11.0")
    with pytest.raises(RuntimeError, match="0.10.2"):
        plugin.register()
