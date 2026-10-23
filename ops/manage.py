#!/usr/bin/env python3
"""Linux app-local process manager. Never discovers/kills unrelated processes."""
from __future__ import annotations

import argparse
import fcntl
import json
import os
from pathlib import Path
import re
import shlex
import signal
import subprocess
import sys
import time

try:
    from ops import engine_members, llama_backend
except ModuleNotFoundError:
    import engine_members
    import llama_backend

ROOT = Path(__file__).resolve().parent.parent
RUNTIME = ROOT / ".runtime"


def load_env(path: Path) -> dict[str, str]:
    env = dict(os.environ)
    if path.exists():
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[7:]
            key, separator, value = line.partition("=")
            if not separator or not key.replace("_", "").isalnum():
                raise ValueError("Invalid env assignment (values are never evaluated as shell code)")
            parts = shlex.split(value, comments=True)
            env[key] = " ".join(parts)
    return env


def process_identity(pid: int) -> dict | None:
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
        # The process name can include spaces and parentheses.
        fields = stat[stat.rfind(")") + 2:].split()
        if fields[0] == "Z":
            return None
        return {"pid": pid, "start_ticks": fields[19], "cwd": str(Path(f"/proc/{pid}/cwd").resolve())}
    except (FileNotFoundError, ProcessLookupError, PermissionError):
        return None


def owned_record(path: Path) -> dict | None:
    if not path.exists():
        return None
    record = json.loads(path.read_text())
    if not isinstance(record, dict) or any(key not in record for key in ("pid", "start_ticks", "cwd")) or type(record["pid"]) is not int:
        raise RuntimeError("This file is not a managed process identity record; choose a different configuration filename")
    current = process_identity(record["pid"])
    if not current:
        return None
    if current != {key: record[key] for key in ("pid", "start_ticks", "cwd")} or current["cwd"] != str(ROOT):
        raise RuntimeError("PID identity mismatch; refusing to signal a process that may belong to somebody else")
    return record


def engine_tensor_parallel_size(args, env: dict) -> str:
    value = env.get("ENGINE_TENSOR_PARALLEL_SIZE", "1")
    if not isinstance(value, str) or not re.fullmatch(r"[1-9][0-9]*", value):
        raise ValueError("ENGINE_TENSOR_PARALLEL_SIZE must be a positive integer")
    size = int(value)
    if size == 1:
        return "1"
    # Multi-GPU use is opt-in and must name the exact devices. In particular,
    # the existing MIG plugin is deliberately restricted to TP1/DP1.
    plugins = env.get("VLLM_PLUGINS", "").split(",")
    if env.get("BATCHFORGE_MIG_PLATFORM") == "1" or "batchforge_mig" in {p.strip() for p in plugins}:
        raise ValueError("The application MIG platform supports tensor parallel size 1 only")
    mask = getattr(args, "gpu", None)
    if mask is None:
        mask = env.get("CUDA_VISIBLE_DEVICES", "")
    selectors = [part.strip() for part in mask.split(",")]
    uuid = r"GPU-[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}"
    if len(selectors) != size or any(not re.fullmatch(r"[0-9]+|" + uuid, part) for part in selectors):
        raise ValueError("Tensor parallel engines require exactly that many explicit non-MIG GPU indices or full GPU UUIDs")
    kinds = {"index" if part.isdecimal() else "uuid" for part in selectors}
    identities = [str(int(part)) if part.isdecimal() else part.lower() for part in selectors]
    if len(kinds) != 1 or len(set(identities)) != size:
        raise ValueError("Tensor parallel GPU selectors must be unique and use one identifier format")
    return str(size)


def command(args, env: dict) -> list[str]:
    python = args.python or str(ROOT / ".venv" / "bin" / "python")
    if args.role == "api":
        return [python, "-m", "uvicorn", "backend.app:app", "--host", "127.0.0.1", "--port", str(args.port or 8000), "--no-access-log"]
    if args.role == "worker":
        return [python, "-m", "worker"]
    if args.role == "gateway":
        return [python, "-m", "uvicorn", "ops.gateway.worker_gateway:app", "--host", env.get("GATEWAY_HOST", "127.0.0.1"), "--port", str(args.port or 8081), "--no-access-log"]
    if args.role == "link":
        return [python, "-m", "ops.link.broker"]
    if args.role == "supervisor":
        return [python, "ops/supervise.py"]
    native = llama_backend.command(args, env, ROOT)
    if native is not None:
        return native
    tensor_parallel_size = engine_tensor_parallel_size(args, env)
    chunked_prefill = env.get("ENGINE_ENABLE_CHUNKED_PREFILL", "0")
    if chunked_prefill not in ("0", "1"):
        raise ValueError("ENGINE_ENABLE_CHUNKED_PREFILL must be 0 or 1")
    # vLLM allocates only the configured fraction of *total* memory. The default
    # is deliberately small for shared Runpod hosts; inspect free memory first.
    result = [python, "-m", "vllm.entrypoints.openai.api_server",
            "--model", env.get("ENGINE_MODEL_PATH", env.get("MODEL_NAME", "Qwen/Qwen2.5-0.5B-Instruct")),
            "--served-model-name", env.get("MODEL_NAME", "Qwen/Qwen2.5-0.5B-Instruct"),
            "--host", "127.0.0.1", "--port", str(args.port or 8001),
            "--tensor-parallel-size", tensor_parallel_size, "--dtype", env.get("ENGINE_DTYPE", "half"),
            "--max-model-len", env.get("ENGINE_MAX_MODEL_LEN", "2048"),
            "--max-num-seqs", env.get("ENGINE_MAX_NUM_SEQS", "8"),
            "--gpu-memory-utilization", env.get("ENGINE_GPU_MEMORY_UTILIZATION", "0.06"),
            "--enforce-eager", "--disable-log-requests"]
    if env.get("ENGINE_REVISION"):
        result.extend(["--revision", env["ENGINE_REVISION"]])
    if env.get("ENGINE_MAX_NUM_BATCHED_TOKENS"):
        result.extend(["--max-num-batched-tokens", env["ENGINE_MAX_NUM_BATCHED_TOKENS"]])
    if env.get("ENGINE_GENERATION_CONFIG"):
        result.extend(["--generation-config", env["ENGINE_GENERATION_CONFIG"]])
    if chunked_prefill == "1":
        result.append("--enable-chunked-prefill")
    if env.get("ENGINE_ENABLE_PREFIX_CACHING", "0") == "1":
        result.append("--enable-prefix-caching")
    else:
        result.append("--no-enable-prefix-caching")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("start", "stop", "status", "logs", "reap"))
    parser.add_argument("role", choices=("api", "worker", "engine", "gateway", "link", "supervisor"))
    parser.add_argument("--name", help="Unique instance suffix, e.g. gpu0")
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env")
    parser.add_argument("--python", help="Interpreter from an app-owned virtual environment")
    parser.add_argument("--port", type=int)
    parser.add_argument("--gpu", help="CUDA_VISIBLE_DEVICES for this managed engine only")
    parser.add_argument("--grace-seconds", type=float, default=250)
    args = parser.parse_args()
    name = args.role + ("-" + args.name if args.name else "")
    if not all(c.isalnum() or c in "-_" for c in name):
        parser.error("name must contain only letters, digits, hyphen, underscore")
    if not Path("/proc/self/stat").exists():
        parser.error("This supervisor intentionally requires Linux /proc for safe PID identity checks")
    RUNTIME.mkdir(mode=0o700, exist_ok=True)
    os.chmod(RUNTIME, 0o700)
    pidfile, logfile = RUNTIME / f"{name}.json", RUNTIME / f"{name}.log"
    with (RUNTIME / f"{name}.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        record = owned_record(pidfile)
        if args.action == "logs":
            if logfile.exists():
                print("\n".join(logfile.read_text(errors="replace").splitlines()[-80:]))
            return
        if args.action == "status":
            if record and record.get("track_members"):
                engine_members.capture(pidfile.with_suffix(".members.json"), record)
            print(json.dumps({"name": name, "running": record is not None, "pid": record["pid"] if record else None}))
            return
        if args.action == "reap":
            if args.role != "engine":
                raise RuntimeError("Only engine members can be reaped")
            if record:
                print(json.dumps({"status": "leader_running", "remaining": 0}))
            elif pidfile.exists():
                previous = engine_members.read(pidfile)
                if previous.get("track_members"):
                    print(json.dumps(engine_members.reap(pidfile.with_suffix(".members.json"), previous)))
                else:
                    print(json.dumps({"status": "nothing_to_reap", "remaining": 0}))
            else:
                print(json.dumps({"status": "nothing_to_reap", "remaining": 0}))
            return
        if args.action == "start":
            if record:
                print(json.dumps({"name": name, "status": "already_running", "pid": record["pid"]}))
                return
            env = load_env(args.env_file)
            if args.gpu is not None:
                env["CUDA_VISIBLE_DEVICES"] = args.gpu
            env["PYTHONUNBUFFERED"] = "1"
            cmd = command(args, env)
            fd = os.open(logfile, os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o600)
            with os.fdopen(fd, "ab") as output:
                child = subprocess.Popen(cmd, cwd=ROOT, env=env, stdin=subprocess.DEVNULL,
                                         stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
            time.sleep(.25)
            identity = process_identity(child.pid)
            if not identity or child.poll() is not None:
                raise RuntimeError(f"Process exited at startup; inspect {logfile}")
            tmp = pidfile.with_suffix(".tmp")
            tmp.write_text(json.dumps({**identity, "command": cmd, "started_at": time.time(),
                "track_members": args.role == "engine" and int(env.get("ENGINE_TENSOR_PARALLEL_SIZE", "1")) > 1}))
            os.chmod(tmp, 0o600)
            tmp.replace(pidfile)
            print(json.dumps({"name": name, "status": "started", "pid": child.pid, "log": str(logfile)}))
            return
        if not record:
            if args.role == "engine" and pidfile.exists():
                previous = engine_members.read(pidfile)
                if previous.get("track_members"):
                    outcome = engine_members.reap(pidfile.with_suffix(".members.json"), previous)
                    if outcome["remaining"]:
                        raise RuntimeError("Engine children are still exiting; retry after memory release")
            pidfile.unlink(missing_ok=True)
            print(json.dumps({"name": name, "status": "not_running"}))
            return
        if record.get("track_members"):
            engine_members.capture(pidfile.with_suffix(".members.json"), record)
        os.killpg(record["pid"], signal.SIGTERM)
        deadline = time.monotonic() + args.grace_seconds
        while time.monotonic() < deadline and process_identity(record["pid"]):
            time.sleep(.2)
        if process_identity(record["pid"]):
            # Revalidate before escalation: PID reuse cannot target another app.
            owned_record(pidfile)
            os.killpg(record["pid"], signal.SIGKILL)
        if record.get("track_members"):
            outcome = engine_members.reap(pidfile.with_suffix(".members.json"), record)
            if outcome["remaining"] or outcome["status"] == "leader_running":
                raise RuntimeError("Engine processes are still exiting; retry after memory release")
        pidfile.unlink(missing_ok=True)
        print(json.dumps({"name": name, "status": "stopped"}))


if __name__ == "__main__":
    main()
