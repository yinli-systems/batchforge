"""Opt-in vLLM 0.10.2 platform discovery for a single explicit MIG UUID.

Uses vLLM's supported out-of-tree platform entry point and its built-in
NonNvmlCudaPlatform implementation. This overlay is put on PYTHONPATH only for
the selected application engines; no installed library is modified.
"""
from __future__ import annotations

from importlib.metadata import version
import os
import re

MIG_UUID = re.compile(r"MIG-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z")


def register() -> str | None:
    if os.environ.get("BATCHFORGE_MIG_PLATFORM") != "1":
        return None
    if not MIG_UUID.fullmatch(os.environ.get("CUDA_VISIBLE_DEVICES", "")):
        raise RuntimeError("BatchForge MIG platform requires exactly one complete MIG UUID")
    if version("vllm") != "0.10.2":
        raise RuntimeError("BatchForge MIG compatibility is verified only for vLLM 0.10.2")
    return "vllm.platforms.cuda.NonNvmlCudaPlatform"
