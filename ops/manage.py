#!/usr/bin/env python3
"""Linux app-local process manager. Never discovers/kills unrelated processes."""
from __future__ import annotations

import argparse
import fcntl
import json
import os
from pathlib import Path
import shlex
import signal
import subprocess
import sys
import time

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
    current = process_identity(record["pid"])
    if not current:
        return None
    if current != {key: record[key] for key in ("pid", "start_ticks", "cwd")} or current["cwd"] != str(ROOT):
        raise RuntimeError("PID identity mismatch; refusing to signal a process that may belong to somebody else")
    return record


def command(args, env: dict) -> list[str]:
    python = args.python or str(ROOT / ".venv" / "bin" / "python")
    if args.role == "api":
        return [python, "-m", "uvicorn", "backend.app:app", "--host", "127.0.0.1", "--port", str(args.port or 8000), "--no-access-log"]
    if args.role == "worker":
        return [python, "-m", "worker"]
    # vLLM allocates only the configured fraction of *total* memory. The default
    # is deliberately small for shared Runpod hosts; inspect free memory first.
    return [python, "-m", "vllm.entrypoints.openai.api_server",
            "--model", env.get("MODEL_NAME", "Qwen/Qwen2.5-0.5B-Instruct"),
            "--host", "127.0.0.1", "--port", str(args.port or 8001),
            "--tensor-parallel-size", "1", "--dtype", "half",
            "--max-model-len", env.get("ENGINE_MAX_MODEL_LEN", "2048"),
            "--max-num-seqs", env.get("ENGINE_MAX_NUM_SEQS", "8"),
            "--gpu-memory-utilization", env.get("ENGINE_GPU_MEMORY_UTILIZATION", "0.06"),
            "--enforce-eager", "--disable-log-requests"]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("start", "stop", "status", "logs"))
    parser.add_argument("role", choices=("api", "worker", "engine"))
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
            print(json.dumps({"name": name, "running": record is not None, "pid": record["pid"] if record else None}))
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
            tmp.write_text(json.dumps({**identity, "command": cmd, "started_at": time.time()}))
            os.chmod(tmp, 0o600)
            tmp.replace(pidfile)
            print(json.dumps({"name": name, "status": "started", "pid": child.pid, "log": str(logfile)}))
            return
        if not record:
            pidfile.unlink(missing_ok=True)
            print(json.dumps({"name": name, "status": "not_running"}))
            return
        os.killpg(record["pid"], signal.SIGTERM)
        deadline = time.monotonic() + args.grace_seconds
        while time.monotonic() < deadline and process_identity(record["pid"]):
            time.sleep(.2)
        if process_identity(record["pid"]):
            # Revalidate before escalation: PID reuse cannot target another app.
            owned_record(pidfile)
            os.killpg(record["pid"], signal.SIGKILL)
        pidfile.unlink(missing_ok=True)
        print(json.dumps({"name": name, "status": "stopped"}))


if __name__ == "__main__":
    main()
