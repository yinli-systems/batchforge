#!/usr/bin/env python3
"""MIG discovery via the CUDA Driver API, independent of Torch's NVML count.

This is metadata-only discovery: no context creation, GPU allocation, model
loading, host process discovery or device reconfiguration is requested.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid

DRIVER_PROBE = r'''
import ctypes,json,uuid
lib=ctypes.CDLL("libcuda.so.1")
def checked(name,*args):
 code=getattr(lib,name)(*args)
 if code: raise RuntimeError(name+" returned "+str(code))
checked("cuInit",0)
count=ctypes.c_int()
checked("cuDeviceGetCount",ctypes.byref(count))
rows=[]
for index in range(count.value):
 device=ctypes.c_int()
 checked("cuDeviceGet",ctypes.byref(device),index)
 identifier=(ctypes.c_ubyte*16)()
 checked("cuDeviceGetUuid_v2",ctypes.byref(identifier),device)
 name=ctypes.create_string_buffer(256)
 checked("cuDeviceGetName",name,len(name),device)
 rows.append({"cuda_index":index,"uuid_v2":str(uuid.UUID(bytes=bytes(identifier))),"name":name.value.decode()})
print(json.dumps({"device_count":count.value,"devices":rows}))
'''


def driver_metadata(selector: str | None = None, *, python: str | None = None) -> dict:
    environment = os.environ.copy()
    if selector is not None:
        environment["CUDA_VISIBLE_DEVICES"] = selector
    result = subprocess.run([python or sys.executable, "-c", DRIVER_PROBE], env=environment,
                            capture_output=True, text=True, timeout=30)
    if result.returncode:
        raise RuntimeError(f"CUDA Driver metadata failed with exit status {result.returncode}")
    return json.loads(result.stdout)


def verified_mig_devices(metadata: dict) -> list[dict]:
    devices = metadata.get("devices")
    count = metadata.get("device_count")
    if not isinstance(devices, list) or type(count) is not int or count != len(devices) or not 1 <= count <= 64:
        raise ValueError("CUDA Driver inventory count is inconsistent")
    seen = set()
    verified = []
    for index, device in enumerate(devices):
        if not isinstance(device, dict) or device.get("cuda_index") != index or " MIG " not in device.get("name", ""):
            raise ValueError("Inventory must contain contiguous CUDA MIG device identities")
        identifier = str(uuid.UUID(device["uuid_v2"]))
        if identifier in seen:
            raise ValueError("Inventory contains duplicate MIG UUIDs")
        seen.add(identifier)
        verified.append({**device, "uuid_v2": identifier, "selector": f"MIG-{identifier}"})
    return verified


def select_mig_device(metadata: dict, logical_index: int, explicit_uuid: str | None = None) -> dict:
    devices = verified_mig_devices(metadata)
    if type(logical_index) is not int or not 0 <= logical_index < len(devices):
        raise ValueError("Requested logical MIG index is not CUDA-visible")
    device = devices[logical_index]
    if explicit_uuid is not None and explicit_uuid != device["selector"]:
        raise ValueError("Explicit MIG UUID differs from the visible logical device identity")
    return device
