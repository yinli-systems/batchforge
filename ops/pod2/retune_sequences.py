#!/usr/bin/env python3
"""Operator-released 16-MIG sequence tuning with per-device 8-slot rollback.

Touches only verified BatchForge process records and owner-only app configs.
Two pipelines maximum; each worker stops before its engine. No GPU repartition,
memory-fraction increase, external process discovery, or credential printing.
"""
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shlex
import stat
import subprocess
import time
from urllib.request import urlopen

import httpx

try:
    from .cuda_devices import driver_metadata, verified_mig_devices
    from .probe_models import MODEL, REVISION, prompt
except ImportError:
    from cuda_devices import driver_metadata, verified_mig_devices
    from probe_models import MODEL, REVISION, prompt

ROOT = Path(__file__).resolve().parents[2]
PYTHON = str(ROOT.parent / "venv/bin/python")
ENGINE_PYTHON = str(ROOT.parent / "engine-venv/bin/python")
REPORT = ROOT / ".runtime/pod2-16seq-tuning.json"
ARCHIVE_SHA = "a2a3ac2f6f5be7593f13a52cbf9cc3167031c9751c8f9df2e9c1ae47323c428f"


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def private_write(path: Path, data: bytes):
    temporary = path.with_name(path.name + f".{os.getpid()}.{time.monotonic_ns()}.tmp")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as handle:
        handle.write(data)
    temporary.replace(path)


def private_read(path: Path) -> bytes:
    if path.is_symlink() or stat.S_IMODE(path.stat().st_mode) != 0o600:
        raise RuntimeError("App configuration must be a regular owner-only file")
    return path.read_bytes()


def replace_env(raw: bytes, changes: dict[str, str]) -> bytes:
    lines = [line for line in raw.decode().splitlines()
             if line.strip().removeprefix("export ").partition("=")[0].strip() not in changes]
    return ("\n".join(lines) + "\n" + "".join(f"{key}={shlex.quote(value)}\n" for key, value in changes.items())).encode()


def kv_capacity(log: str) -> int:
    matches = re.findall(r"GPU KV cache size:\s*([\d,]+)\s*tokens", log)
    if not matches:
        raise RuntimeError("Fresh startup logs do not establish KV token capacity")
    return int(matches[-1].replace(",", ""))


def worker_capacity(port: int, expected: int):
    with urlopen(f'http://127.0.0.1:{port}/metrics', timeout=5) as response:
        metrics = response.read().decode()
    for name, value in (('batchforge_worker_capacity', expected), ('batchforge_worker_ready_engines', 1)):
        found = re.search(r'^' + name + r'\s+([\d.eE+-]+)$', metrics, re.M)
        if not found or float(found.group(1)) != value:
            raise RuntimeError('Worker readiness does not establish reserved capacity and one ready engine')


def load_manager():
    spec = importlib.util.spec_from_file_location("bf_tune_manage", ROOT / "ops/manage.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def manage(*args):
    subprocess.run([PYTHON, "ops/manage.py", *args], cwd=ROOT, stdin=subprocess.DEVNULL,
                   stdout=subprocess.DEVNULL, check=True, timeout=340)


def wait_ready(port: int, timeout=180):
    deadline = time.monotonic() + timeout
    path = "/v1/models" if 8200 <= port <= 8215 else "/readyz"
    while time.monotonic() < deadline:
        try:
            with urlopen(f"http://127.0.0.1:{port}{path}", timeout=3) as response:
                if response.status == 200:
                    if path == "/v1/models":
                        catalog = json.load(response)
                        expected = str(ROOT.parent / "models/Qwen2.5-7B-Instruct" / REVISION)
                        if not any(row["id"] == MODEL and row.get("root") == expected
                                   and row.get("max_model_len") == 4096 for row in catalog["data"]):
                            raise RuntimeError("Unexpected model catalog")
                    return
        except Exception:
            pass
        time.sleep(.5)
    raise RuntimeError("Owned service readiness deadline exceeded")


def free_memory(selector: str) -> dict:
    code = ("import json,torch; assert torch.cuda.device_count()==1; "
            "free,total=torch.cuda.mem_get_info(0); print(json.dumps({'free':free,'total':total}))")
    result = subprocess.run([ENGINE_PYTHON, "-c", code], cwd=ROOT,
                            env={**os.environ, "CUDA_VISIBLE_DEVICES": selector},
                            capture_output=True, text=True, check=True, timeout=45)
    memory = json.loads(result.stdout)
    if memory["free"] < int(memory["total"] * .82) + 512 * 1024**2:
        raise RuntimeError("Selected MIG lacks .82 total plus 512 MiB free; no other process will be stopped")
    return memory


async def concurrent_probe(port: int, count=16) -> dict:
    results = []
    peak = 0
    running = True
    barrier = asyncio.Event()
    async with httpx.AsyncClient(timeout=180, trust_env=False,
                                 limits=httpx.Limits(max_connections=count + 2)) as client:
        async def sample():
            nonlocal peak
            while running:
                try:
                    response = await client.get(f"http://127.0.0.1:{port}/metrics", timeout=3)
                    matches = re.findall(r'^vllm:num_requests_running(?:\{[^\n]*\})?\s+([\d.eE+-]+)$', response.text, re.M)
                    peak = max(peak, int(sum(map(float, matches))))
                except httpx.HTTPError:
                    pass
                await asyncio.sleep(.05)
        async def request(index):
            await barrier.wait()
            response = await client.post(f"http://127.0.0.1:{port}/v1/chat/completions", json={
                "model": MODEL, "messages": [{"role": "user", "content": prompt(port * 100000 + index, 16)}],
                "temperature": 0, "seed": 20261024, "max_tokens": 256})
            response.raise_for_status()
            value = response.json()
            content = value["choices"][0]["message"]["content"]
            if value["model"] != MODEL or not content or value["usage"]["completion_tokens"] <= 0:
                raise RuntimeError("Generation did not produce real model output")
            results.append({"index": index, "usage": value["usage"],
                            "output_sha256": hashlib.sha256(content.encode()).hexdigest(),
                            "finish_reason": value["choices"][0]["finish_reason"]})
        sampler = asyncio.create_task(sample())
        tasks = [asyncio.create_task(request(index)) for index in range(count)]
        began = time.monotonic()
        barrier.set()
        try:
            await asyncio.gather(*tasks)
        finally:
            running = False
            await sampler
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
    if len(results) != count or peak < count:
        raise RuntimeError(f"Concurrent generation gate failed: successful={len(results)} peak_running={peak}")
    return {"requested": count, "successful": len(results), "peak_running_observed": peak,
            "elapsed_seconds": time.monotonic() - began, "quality_evaluated": False,
            "results": sorted(results, key=lambda row: row["index"])}


def tune(index: int, device: dict, backups: Path) -> dict:
    manager = load_manager()
    engine_name, worker_name = f"pod2-7b-gpu{index}", f"pod2-7b-mig{index}"
    engine_env, worker_env = ROOT / f".env.engine-{engine_name}", ROOT / f".env.worker-{worker_name}"
    engine_old, worker_old = private_read(backups / engine_env.name), private_read(backups / worker_env.name)
    row = {"logical_device": index, "cuda_selector": device["selector"], "started_at": now(),
           "passed": False, "effective_slots": 8}
    before_engine = manager.owned_record(ROOT / f".runtime/engine-{engine_name}.json")
    before_worker = manager.owned_record(ROOT / f".runtime/worker-{worker_name}.json")
    if not before_engine or not before_worker:
        raise RuntimeError("Tuning requires both exact owned process records")
    row["before_engine_pid"], row["before_worker_pid"] = before_engine["pid"], before_worker["pid"]
    logpath = ROOT / f".runtime/engine-{engine_name}.log"
    offset = logpath.stat().st_size
    try:
        maximum = Path('/sys/fs/cgroup/memory.max').read_text().strip()
        if maximum != 'max':
            available = int(maximum) - int(Path('/sys/fs/cgroup/memory.current').read_text())
            row['container_memory_available_before_bytes'] = available
            if available < 48 * 1024**3:
                raise RuntimeError('Container memory headroom is below 48 GiB')
        manage("stop", "worker", "--name", worker_name, "--grace-seconds", "300")
        manage("stop", "engine", "--name", engine_name, "--grace-seconds", "25")
        row["free_memory_before"] = free_memory(device["selector"])
        updates = {"ENGINE_MAX_NUM_SEQS": "16", "BATCHFORGE_MIG_PLATFORM": "1",
                   "VLLM_PLUGINS": "batchforge_mig", "PYTHONPATH": str(ROOT / "ops/pod2/mig_platform"),
                   "VLLM_WORKER_MULTIPROC_METHOD": "spawn"}
        private_write(engine_env, replace_env(engine_old, updates))
        manage("start", "engine", "--name", engine_name, "--env-file", str(engine_env),
               "--python", ENGINE_PYTHON, "--gpu", device["selector"], "--port", str(8200 + index))
        wait_ready(8200 + index)
        with logpath.open("rb") as handle:
            handle.seek(offset)
            fresh = handle.read().decode(errors="replace")
        row["kv_token_capacity"] = kv_capacity(fresh)
        if row["kv_token_capacity"] < 65536:
            raise RuntimeError("KV capacity is below the 16 x 4096 reservation")
        command = manager.owned_record(ROOT / f".runtime/engine-{engine_name}.json")['command']
        if command[command.index('--max-num-seqs') + 1] != '16':
            raise RuntimeError('Owned engine command does not request 16 sequences')
        if not all(marker in fresh for marker in ("dtype=torch.bfloat16", "enable_prefix_caching=False")):
            raise RuntimeError("Fresh logs do not establish unchanged precision/cache and 16 sequences")
        row["generation_probe"] = asyncio.run(concurrent_probe(8200 + index))
        private_write(worker_env, replace_env(worker_old, {"WORKER_MAX_INFLIGHT": "16", "WORKER_TOKEN_BUDGET": "65536"}))
        manage("start", "worker", "--name", worker_name, "--env-file", str(worker_env), "--python", PYTHON)
        wait_ready(9200 + index, 90)
        worker_capacity(9200 + index, 16)
        record = manager.owned_record(ROOT / f".runtime/engine-{engine_name}.json")
        mask = next(item.partition(b"=")[2].decode() for item in Path(f"/proc/{record['pid']}/environ").read_bytes().split(b"\0")
                    if item.startswith(b"CUDA_VISIBLE_DEVICES="))
        if mask != device["selector"]:
            raise RuntimeError("Restarted engine is not pinned to its unique MIG UUID")
        row.update(passed=True, effective_slots=16, engine_pid=record["pid"],
                   worker_pid=manager.owned_record(ROOT / f".runtime/worker-{worker_name}.json")["pid"],
                   actual_cuda_mask=mask, bfloat16=True, prefix_caching=False, app_owned_mig_plugin=True)
    except Exception as error:
        row["failure"] = str(error) if isinstance(error, RuntimeError) else type(error).__name__
        manage("stop", "worker", "--name", worker_name, "--grace-seconds", "30")
        manage("stop", "engine", "--name", engine_name, "--grace-seconds", "25")
        private_write(engine_env, engine_old)
        private_write(worker_env, worker_old)
        free_memory(device["selector"])
        manage("start", "engine", "--name", engine_name, "--env-file", str(engine_env),
               "--python", ENGINE_PYTHON, "--gpu", device["selector"], "--port", str(8200 + index))
        wait_ready(8200 + index)
        manage("start", "worker", "--name", worker_name, "--env-file", str(worker_env), "--python", PYTHON)
        wait_ready(9200 + index, 90)
        worker_capacity(9200 + index, 8)
        row["rollback_8_ready"] = True
    row["finished_at"] = now()
    return row


def main():
    manager = load_manager()
    manifest = json.loads((ROOT / "ops/pod2/json-worker-manifest.json").read_text())
    if manifest["archive_sha256"] != ARCHIVE_SHA:
        raise RuntimeError("Worker manifest is not the operator-released frozen revision")
    for entry in manifest["files"]:
        path = Path(entry["path"])
        if not path.parts or path.parts[0] != "worker" or ".." in path.parts:
            raise RuntimeError("Worker manifest escaped its application directory")
        data = (ROOT / path).read_bytes()
        if len(data) != entry["bytes"] or hashlib.sha256(data).hexdigest() != entry["sha256"]:
            raise RuntimeError("Worker source differs from released per-file hashes")
    identities = verified_mig_devices(driver_metadata(python=ENGINE_PYTHON))
    if len(identities) != 16:
        raise RuntimeError("Expected exactly sixteen authorized visible MIG UUIDs")
    actual_uuids = []
    for index, device in enumerate(identities):
        record = manager.owned_record(ROOT / f'.runtime/engine-pod2-7b-gpu{index}.json')
        if not record:
            raise RuntimeError('All preexisting engines must have exact live process ownership')
        mask = next(item.partition(b'=')[2].decode() for item in Path(f"/proc/{record['pid']}/environ").read_bytes().split(b'\0')
                    if item.startswith(b'CUDA_VISIBLE_DEVICES='))
        selected = driver_metadata(mask, python=ENGINE_PYTHON)
        if selected.get('device_count') != 1 or selected['devices'][0]['uuid_v2'] != device['uuid_v2']:
            raise RuntimeError('Existing engine CUDA mask differs from the planned unique MIG UUID')
        actual_uuids.append(selected['devices'][0]['uuid_v2'])
    if len(set(actual_uuids)) != 16:
        raise RuntimeError('Preexisting engines do not occupy sixteen distinct authorized MIG instances')
    backups = ROOT / ".runtime/rollback-8seq-20261024"
    backups.mkdir(mode=0o700, exist_ok=False)
    for index in range(16):
        for role, name in (("engine", f"pod2-7b-gpu{index}"), ("worker", f"pod2-7b-mig{index}")):
            if not manager.owned_record(ROOT / f".runtime/{role}-{name}.json"):
                raise RuntimeError("An expected application process is not running")
            source = ROOT / f".env.{role}-{name}"
            private_write(backups / source.name, private_read(source))
            env = manager.load_env(source)
            expected = ({'ENGINE_MAX_NUM_SEQS': '8', 'ENGINE_DTYPE': 'bfloat16',
                         'ENGINE_MAX_MODEL_LEN': '4096', 'ENGINE_MAX_NUM_BATCHED_TOKENS': '4096',
                         'ENGINE_ENABLE_PREFIX_CACHING': '0', 'ENGINE_GPU_MEMORY_UTILIZATION': '0.82'}
                        if role == 'engine' else {'WORKER_MAX_INFLIGHT': '8', 'WORKER_TOKEN_BUDGET': '32768',
                                                   'ENGINE_MAX_MODEL_LEN': '4096'})
            if any(env.get(key) != value for key, value in expected.items()):
                raise RuntimeError('Private source configuration differs from the approved 8-slot baseline')
    protected = {name: manager.owned_record(ROOT / f".runtime/{name}.json") for name in ("api-a", "api-b", "gateway-edge")}
    if not all(protected.values()):
        raise RuntimeError("Independent control plane must be healthy before tuning")
    report = {"started_at": now(), "status": "running", "parallel_pipelines": 2,
              "max_num_seqs_target": 16, "worker_token_budget_target": 65536,
              "memory_fraction": .82, "dtype": "bfloat16", "context": 4096,
              "max_num_batched_tokens": 4096, "prefix_caching": False,
              "worker_archive_sha256": ARCHIVE_SHA, "private_8seq_config_backup": str(backups), "devices": []}
    private_write(REPORT, (json.dumps(report, indent=2) + "\n").encode())
    manage("stop", "supervisor", "--name", "main", "--grace-seconds", "20")
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            pending = {pool.submit(tune, index, identity, backups): index for index, identity in enumerate(identities)}
            for future in as_completed(pending):
                try:
                    row = future.result()
                except Exception as error:
                    row = {"logical_device": pending[future], "passed": False, "fatal_error": type(error).__name__,
                           "manual_recovery_required": True, "effective_slots": 0,
                           "rollback_8_ready": False}
                report["devices"].append(row)
                report["last_progress_at"] = now()
                private_write(REPORT, (json.dumps(report, indent=2) + "\n").encode())
                print(json.dumps({"event": "sequence_tuning_result", "finished": len(report["devices"]),
                                  "device": row.get("logical_device"), "passed": row["passed"],
                                  "effective_slots": row.get("effective_slots"), "failure": row.get("failure")}), flush=True)
    finally:
        manage("start", "supervisor", "--name", "main", "--env-file", ".env.supervisor-pod2", "--python", PYTHON)
    report.update(finished_at=now(), passed_count=sum(row["passed"] for row in report["devices"]),
                  control_plane_processes_unchanged=all(manager.owned_record(ROOT / f".runtime/{name}.json") == record for name, record in protected.items()))
    report["status"] = "complete" if report["passed_count"] == 16 else "partial_rollback"
    private_write(REPORT, (json.dumps(report, indent=2) + "\n").encode())
    print(json.dumps({key: value for key, value in report.items() if key != "devices"}), flush=True)


if __name__ == "__main__":
    main()
