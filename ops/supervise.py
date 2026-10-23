#!/usr/bin/env python3
"""Restart only exact app-owned processes via ops/manage.py, without shell eval.

SIGTERM/SIGINT stop this watchdog and leave services alive for a rolling watchdog
restart. Liveness determines restarts; unavailable readiness never kills an API.
This is a process watchdog, not Pod boot orchestration or a database failover agent.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shlex
import signal
import stat
import subprocess
import sys
import threading
import time
from urllib.request import ProxyHandler, build_opener

ROOT = Path(__file__).resolve().parent.parent
ROLES = {"api", "worker", "engine", "gateway", "link"}
NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}\Z")
MAX_MANIFEST_BYTES = 65536
MAX_SERVICES = 64


class ConfigError(ValueError):
    pass


def private_read(path: Path, limit=MAX_MANIFEST_BYTES):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "rb") as handle:
        info = os.fstat(handle.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) & 0o077:
            raise ConfigError("Configuration must be an owner-only regular file")
        content = handle.read(limit + 1)
        if len(content) > limit:
            raise ConfigError("Configuration exceeds bounded size")
        return content


def local_path(value, root: Path):
    if not isinstance(value, str) or not value or "\x00" in value:
        raise ConfigError("Invalid path")
    path = Path(value)
    return Path(os.path.abspath(path if path.is_absolute() else root / path))


def load_manifest(path: Path, root: Path = ROOT):
    root = root.resolve()
    path = local_path(str(path), root)
    runtime = root / ".runtime"
    if path.parent != runtime or path.suffix != ".json" or not NAME.fullmatch(path.stem) or path.resolve() != path:
        raise ConfigError("Manifest must be a JSON file directly inside app/.runtime")
    raw = private_read(path)
    try:
        document = json.loads(raw)
    except (ValueError, UnicodeError):
        raise ConfigError("Invalid JSON manifest") from None
    if not isinstance(document, dict) or set(document) != {"services"} or not isinstance(document["services"], list) or not 1 <= len(document["services"]) <= MAX_SERVICES:
        raise ConfigError(f"Manifest requires one to {MAX_SERVICES} services")
    services, names, endpoints = [], set(), set()
    for entry in document["services"]:
        if not isinstance(entry, dict) or set(entry) - {"role", "name", "env_file", "python", "port", "gpu"}:
            raise ConfigError("Unknown service fields")
        role, name = entry.get("role"), entry.get("name")
        if role not in ROLES or not isinstance(name, str) or not NAME.fullmatch(name):
            raise ConfigError("Invalid service role/name")
        identity = f"{role}-{name}"
        if identity in names:
            raise ConfigError("Duplicate service identity")
        names.add(identity)
        environment = local_path(entry.get("env_file"), root)
        inside = environment.is_relative_to(root)
        # Legacy app-owned access.env/gpuN.env live directly beside app/.
        sibling = (root.name == "app" and root.parent.name == "batchforge-runtime" and environment.parent == root.parent and environment.suffix == ".env")
        if not (inside or sibling) or environment.resolve() != environment:
            raise ConfigError("Environment path is outside the application configuration roots")
        private_read(environment)
        interpreter = local_path(entry.get("python"), root)
        binary_roots = {root / ".venv" / "bin"}
        if root.name == "app" and root.parent.name == "batchforge-runtime":
            binary_roots |= {root.parent / "venv" / "bin", root.parent / "engine-venv" / "bin"}
        if interpreter.parent not in binary_roots or not re.fullmatch(r"python(?:3(?:\.\d+)?)?", interpreter.name):
            raise ConfigError("Interpreter must belong to an application virtual environment")
        info = interpreter.stat()
        if not stat.S_ISREG(info.st_mode) or info.st_uid not in {0, os.geteuid()} or info.st_mode & 0o022 or not os.access(interpreter, os.X_OK):
            raise ConfigError("Interpreter ownership or executable permissions are unsafe")
        port = entry.get("port")
        if port is not None and (type(port) is not int or not 1 <= port <= 65535):
            raise ConfigError("Invalid port")
        if role in {"api", "engine", "gateway"} and port is None:
            raise ConfigError("HTTP service requires an explicit port")
        if port is not None:
            if port in endpoints:
                raise ConfigError("Duplicate local HTTP readiness port")
            endpoints.add(port)
        gpu = entry.get("gpu")
        if gpu is not None and (role != "engine" or not isinstance(gpu, str) or not re.fullmatch(r"[A-Za-z0-9_,./-]{1,128}", gpu)):
            raise ConfigError("Invalid engine GPU selector")
        if role == "engine" and (gpu is None or "," in gpu):
            raise ConfigError("Each managed engine requires one explicit GPU selector")
        services.append({"role": role, "name": name, "env_file": str(environment), "python": str(interpreter), "port": port, "gpu": gpu})
    digest = hashlib.sha256(json.dumps(services, sort_keys=True).encode()).hexdigest()
    return path, services, digest


def private_write(path: Path, value):
    temporary = path.with_name(path.name + f".{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(descriptor, "w") as handle:
            json.dump(value, handle, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


class Manager:
    def __init__(self, root=ROOT, timeout=10):
        self.root, self.timeout = root, timeout
        # Disable ambient HTTP_PROXY/ALL_PROXY for loopback readiness probes.
        self.http = build_opener(ProxyHandler({}))

    def invoke(self, service, action):
        arguments = [sys.executable, str(self.root / "ops" / "manage.py"), action, service["role"], "--name", service["name"]]
        if action == "start":
            arguments += ["--env-file", service["env_file"], "--python", service["python"]]
            for field in ("port", "gpu"):
                if service.get(field) is not None:
                    arguments += [f"--{field}", str(service[field])]
        try:
            result = subprocess.run(arguments, cwd=self.root, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=self.timeout, check=False)
            if result.returncode or len(result.stdout) > 4096:
                return None, "manager_failed"
            payload = json.loads(result.stdout)
            if not isinstance(payload, dict):
                return None, "manager_protocol_error"
            if action == "status" and (type(payload.get("running")) is not bool or (payload["running"] and type(payload.get("pid")) is not int)):
                return None, "manager_protocol_error"
            if action == "start" and payload.get("status") not in {"started", "already_running"}:
                return None, "manager_protocol_error"
            return payload, None
        except subprocess.TimeoutExpired:
            return None, "manager_timeout"
        except (OSError, ValueError):
            return None, "manager_protocol_error"

    def readiness(self, service):
        if service.get("port") is None:
            return None
        path = "/health" if service["role"] == "engine" else "/readyz"
        try:
            with self.http.open(f"http://127.0.0.1:{service['port']}{path}", timeout=1) as response:
                return response.status == 200
        except Exception:
            return False

    def start_gate(self, service):
        if service["role"] != "engine":
            return True, None
        # Model initialization must never race residual CUDA allocations from a
        # dead engine. Read only the nonsecret utilization option; no env values
        # or CUDA probe diagnostics are logged or included in status snapshots.
        try:
            fraction = os.environ.get("ENGINE_GPU_MEMORY_UTILIZATION", "0.06")
            for line in private_read(Path(service["env_file"])).decode().splitlines():
                line = line.strip().removeprefix("export ").strip()
                key, separator, value = line.partition("=")
                if separator and key.strip() == "ENGINE_GPU_MEMORY_UTILIZATION":
                    fraction = " ".join(shlex.split(value, comments=True))
            fraction = float(fraction)
            if not math.isfinite(fraction) or not 0 < fraction < 1:
                return False, "gpu_configuration_invalid"
            probe = subprocess.run([service["python"], "-c", "import json,torch; free,total=torch.cuda.mem_get_info(0); print(json.dumps({'free':free,'total':total}))"],
                                   env={**os.environ, "CUDA_VISIBLE_DEVICES": service["gpu"]}, cwd=self.root,
                                   stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=15, check=False)
            if probe.returncode or len(probe.stdout) > 4096:
                return False, "gpu_probe_unavailable"
            memory = json.loads(probe.stdout)
            if type(memory.get("free")) is not int or type(memory.get("total")) is not int or not 0 <= memory["free"] <= memory["total"]:
                return False, "gpu_probe_unavailable"
            return (True, None) if memory["free"] >= memory["total"] * fraction + 512 * 1024**2 else (False, "gpu_memory_not_released")
        except (OSError, ValueError, UnicodeError, subprocess.TimeoutExpired):
            return False, "gpu_probe_unavailable"


class Watchdog:
    def __init__(self, services, fingerprint, manager, *, initial_backoff=1, max_backoff=60, stable_seconds=60, previous=None, clock=time.monotonic, wall=time.time):
        self.services, self.fingerprint, self.manager = services, fingerprint, manager
        self.initial_backoff, self.max_backoff, self.stable_seconds = initial_backoff, max_backoff, stable_seconds
        self.clock, self.wall = clock, wall
        previous = previous if isinstance(previous, dict) and previous.get("configuration_sha256") == fingerprint else {}
        previous_services = previous.get("services", {})
        if not isinstance(previous_services, dict):
            previous_services = {}
        self.states = {}
        for service in services:
            identity = f"{service['role']}-{service['name']}"
            former = previous_services.get(identity, {})
            former = former if isinstance(former, dict) else {}
            state = {"role": service["role"], "name": service["name"], "pid": None, "running": False, "ready": None, "restart_count": 0, "start_count": 0, "failed_start_count": 0, "failure_streak": 0, "ever_running": False, "restart_blocked": False, "error": None, "next_retry_at": 0}
            for key in ("restart_count", "start_count", "failed_start_count", "failure_streak"):
                value = former.get(key)
                if type(value) is int and 0 <= value <= 1_000_000_000:
                    state[key] = value
            state["ever_running"] = former.get("ever_running") is True
            state["restart_blocked"] = former.get("restart_blocked") is True
            remaining = former.get("next_retry_at", 0)
            remaining = max(0, min(max_backoff, remaining - wall())) if type(remaining) in {int, float} and math.isfinite(remaining) else 0
            state["_next_retry"] = clock() + remaining
            state["_alive_since"] = None
            self.states[identity] = state

    def inspect(self, service):
        state = self.states[f"{service['role']}-{service['name']}"]
        now = self.clock()
        status, error = self.manager.invoke(service, "status")
        if error:
            # An identity mismatch is not equivalent to a dead owned process.
            # Never attempt a start (or signal) when status cannot be trusted.
            state.update(error=error, running=None, ready=None, pid=None, _alive_since=None)
            return
        if status["running"]:
            if state["_alive_since"] is None or state["pid"] != status["pid"]:
                state["_alive_since"] = now
            if now - state["_alive_since"] >= self.stable_seconds:
                state["failure_streak"] = 0
            state.update(running=True, pid=status["pid"], ready=self.manager.readiness(service), ever_running=True, restart_blocked=False, error=None)
            return
        state.update(running=False, pid=None, ready=False, _alive_since=None)
        if state["restart_blocked"]:
            state["error"] = "ambiguous_start_requires_operator"
            return
        if now < state["_next_retry"]:
            return
        state["failure_streak"] += 1
        delay = min(self.max_backoff, self.initial_backoff * 2 ** min(state["failure_streak"] - 1, 20))
        state["_next_retry"] = now + delay
        state["next_retry_at"] = self.wall() + delay
        allowed, reason = self.manager.start_gate(service)
        if not allowed:
            state["error"] = reason
            state["_next_retry"] = self.clock() + delay
            state["next_retry_at"] = self.wall() + delay
            return
        started, error = self.manager.invoke(service, "start")
        state["_next_retry"] = self.clock() + delay
        state["next_retry_at"] = self.wall() + delay
        if error:
            state["failed_start_count"] += 1
            state["error"] = error
            # A timed-out manager may have spawned a child but failed before
            # recording it. Never start a second GPU engine blindly.
            state["restart_blocked"] = error == "manager_timeout"
            return
        if started["status"] == "started":
            state["start_count"] += 1
            if state["ever_running"]:
                state["restart_count"] += 1
        state.update(running=True, pid=started.get("pid"), ready=None, ever_running=True, error=None, _alive_since=self.clock())

    def snapshot(self, stopping=False):
        return {"configuration_sha256": self.fingerprint, "updated_at": self.wall(), "watchdog_pid": os.getpid(), "watchdog_running": not stopping,
                "services_preserved_on_exit": True, "services": {name: {key: value for key, value in state.items() if not key.startswith("_")} for name, state in self.states.items()}}


def emit_event(path, event, maximum=262144):
    # Event fields are constructed here, never manager output/env/commands.
    if path.exists() and path.stat().st_size >= maximum:
        path.replace(path.with_suffix(".previous.jsonl"))
    descriptor = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "w") as handle:
        handle.write(json.dumps(event, separators=(",", ":")) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=os.getenv("SUPERVISOR_MANIFEST", str(ROOT / ".runtime" / "services.json")))
    parser.add_argument("--poll-seconds", type=float, default=5)
    parser.add_argument("--initial-backoff-seconds", type=float, default=1)
    parser.add_argument("--max-backoff-seconds", type=float, default=60)
    parser.add_argument("--stable-seconds", type=float, default=60)
    args = parser.parse_args()
    if not Path("/proc/self/stat").exists():
        parser.error("Linux /proc is required for owned process identity checks")
    values = (args.poll_seconds, args.initial_backoff_seconds, args.max_backoff_seconds, args.stable_seconds)
    if any(not math.isfinite(value) or value < 0.05 or value > 3600 for value in values) or args.initial_backoff_seconds > args.max_backoff_seconds:
        parser.error("Invalid bounded timing configuration")
    try:
        manifest, services, fingerprint = load_manifest(args.manifest)
        runtime = ROOT / ".runtime"
        if runtime.stat().st_uid != os.geteuid() or stat.S_IMODE(runtime.stat().st_mode) & 0o077:
            raise ConfigError("Runtime directory must be owner-only")
        state_path = runtime / f"supervision-{manifest.stem}.json"
        event_path = runtime / f"supervision-{manifest.stem}.jsonl"
        lock_fd = os.open(runtime / "supervisor.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        with os.fdopen(lock_fd, "w") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise ConfigError("Another application watchdog already holds the lock") from None
            previous = json.loads(private_read(state_path, 262144)) if state_path.exists() else None
            watchdog = Watchdog(services, fingerprint, Manager(), initial_backoff=args.initial_backoff_seconds, max_backoff=args.max_backoff_seconds, stable_seconds=args.stable_seconds, previous=previous)
            stopped = threading.Event()
            for signum in (signal.SIGTERM, signal.SIGINT):
                signal.signal(signum, lambda *_: stopped.set())
            old_signature = None
            with ThreadPoolExecutor(max_workers=min(8, len(services))) as executor:
                while not stopped.is_set():
                    list(executor.map(watchdog.inspect, services))
                    snapshot = watchdog.snapshot()
                    private_write(state_path, snapshot)
                    signature = [(name, state["running"], state["ready"], state["pid"], state["restart_count"], state["failed_start_count"], state["error"]) for name, state in snapshot["services"].items()]
                    if signature != old_signature:
                        emit_event(event_path, {"at": time.time(), "event": "service_state_changed", "services": snapshot["services"]})
                        old_signature = signature
                    stopped.wait(args.poll_seconds)
            private_write(state_path, watchdog.snapshot(stopping=True))
            emit_event(event_path, {"at": time.time(), "event": "watchdog_stopped_services_preserved"})
    except (OSError, ValueError, KeyError):
        # Do not dump configuration, interpreter arguments or environment values.
        print("Supervisor stopped: invalid or inaccessible application configuration", file=sys.stderr)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
