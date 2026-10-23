"""Watchdog control invariants plus a real Linux process crash/restart drill."""
from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import stat
import subprocess
import sys
import time
from types import SimpleNamespace

import pytest

from ops.supervise import ConfigError, Watchdog, load_manifest, private_write


SERVICE = {"role": "api", "name": "one", "env_file": "/unused.env", "python": "/unused/python", "port": 9999, "gpu": None}


def test_engine_restart_generation_config_is_opt_in_and_preserves_other_flags(tmp_path, monkeypatch):
    from ops.manage import command, load_env

    monkeypatch.delenv("ENGINE_GENERATION_CONFIG", raising=False)
    environment = tmp_path / "engine.env"
    original = "MODEL_NAME=Qwen/Qwen2.5-7B-Instruct\nENGINE_MAX_NUM_SEQS=16\nENGINE_MAX_MODEL_LEN=4096\nENGINE_GPU_MEMORY_UTILIZATION=0.24\n"
    environment.write_text(original)
    args = SimpleNamespace(role="engine", python="/owned/engine-venv/bin/python", port=8100)
    baseline = command(args, load_env(environment))
    assert "--generation-config" not in baseline

    # Reload the persisted env as a fresh manager/watchdog restart does. A path
    # containing spaces must remain one argv element, without shell evaluation.
    path = "/owned/generation configs/eos-151645"
    environment.write_text(original + f"ENGINE_GENERATION_CONFIG='{path}'\n")
    restarted = command(args, load_env(environment))
    index = restarted.index("--generation-config")
    assert restarted[index + 1] == path
    assert restarted[:index] + restarted[index + 2:] == baseline
    assert restarted.count("--generation-config") == 1

    environment.write_text(original + "ENGINE_GENERATION_CONFIG=\n")
    assert command(args, load_env(environment)) == baseline


class FakeManager:
    def __init__(self, *, running=False, ready=False, start_error=None, status_error=None):
        self.running, self.ready, self.start_error, self.status_error = running, ready, start_error, status_error
        self.calls = []

    def invoke(self, service, action):
        self.calls.append(action)
        if action == "status":
            return ({"running": self.running, "pid": 42 if self.running else None}, self.status_error)
        if self.start_error:
            return None, self.start_error
        self.running = True
        return {"status": "started", "pid": 42}, None

    def readiness(self, service):
        return self.ready

    def start_gate(self, service):
        return True, None


def test_readiness_loss_never_restarts_a_live_process_and_identity_errors_never_start():
    manager = FakeManager(running=True, ready=False)
    watchdog = Watchdog([SERVICE], "fingerprint", manager)
    for _ in range(4):
        watchdog.inspect(SERVICE)
    assert manager.calls == ["status"] * 4
    assert watchdog.snapshot()["services"]["api-one"]["running"] is True
    assert watchdog.snapshot()["services"]["api-one"]["ready"] is False
    manager.status_error = "manager_failed"
    manager.running = False
    watchdog.inspect(SERVICE)
    assert "start" not in manager.calls


def test_failed_starts_use_exponential_backoff_and_survive_watchdog_restart():
    clock = [0.0]
    manager = FakeManager(start_error="manager_failed")
    watchdog = Watchdog([SERVICE], "same", manager, initial_backoff=1, max_backoff=8, clock=lambda: clock[0], wall=lambda: 100 + clock[0])
    for moment in (0, 0.2, 0.9, 1, 1.5, 2.9, 3):
        clock[0] = moment
        watchdog.inspect(SERVICE)
    assert manager.calls.count("start") == 3
    snapshot = watchdog.snapshot()
    state = snapshot["services"]["api-one"]
    assert state["failed_start_count"] == state["failure_streak"] == 3
    assert state["next_retry_at"] == 107
    restarted = Watchdog([SERVICE], "same", manager, initial_backoff=1, max_backoff=8, previous=snapshot, clock=lambda: clock[0], wall=lambda: 100 + clock[0])
    clock[0] = 6.9
    restarted.inspect(SERVICE)
    assert manager.calls.count("start") == 3
    clock[0] = 7
    restarted.inspect(SERVICE)
    assert manager.calls.count("start") == 4
    assert restarted.snapshot()["services"]["api-one"]["failed_start_count"] == 4


def test_ambiguous_start_timeout_blocks_duplicate_launch_until_owned_status_returns():
    manager = FakeManager(start_error="manager_timeout")
    clock = [0.0]
    watchdog = Watchdog([SERVICE], "same", manager, clock=lambda: clock[0])
    watchdog.inspect(SERVICE)
    clock[0] = 500
    watchdog.inspect(SERVICE)
    assert manager.calls.count("start") == 1
    assert watchdog.snapshot()["services"]["api-one"]["restart_blocked"] is True
    manager.running = True
    watchdog.inspect(SERVICE)
    assert watchdog.snapshot()["services"]["api-one"]["restart_blocked"] is False


def test_gpu_memory_gate_defers_launch_with_backoff_without_stopping_any_process():
    manager = FakeManager()
    manager.start_gate = lambda _: (False, "gpu_memory_not_released")
    watchdog = Watchdog([SERVICE], "same", manager)
    watchdog.inspect(SERVICE)
    assert manager.calls == ["status"]
    state = watchdog.snapshot()["services"]["api-one"]
    assert state["error"] == "gpu_memory_not_released"
    assert state["start_count"] == state["failed_start_count"] == 0


def app_fixture(tmp_path):
    root = tmp_path / "application"
    runtime = root / ".runtime"
    runtime.mkdir(parents=True, mode=0o700)
    interpreter = root / ".venv/bin/python"
    interpreter.parent.mkdir(parents=True)
    interpreter.symlink_to(sys.executable)
    environment = runtime / "service.env"
    environment.write_text("PRIVATE_MARKER=never-print-this-value\n")
    environment.chmod(0o600)
    manifest = runtime / "supervisor.json"
    service = {"role": "worker", "name": "crasher", "env_file": str(environment), "python": str(interpreter)}
    private_write(manifest, {"services": [service]})
    return root, manifest, service


def test_manifest_paths_permissions_and_duplicate_names_are_rejected(tmp_path):
    root, manifest, service = app_fixture(tmp_path)
    assert len(load_manifest(manifest, root)[1]) == 1
    manifest.chmod(0o644)
    with pytest.raises(ConfigError):
        load_manifest(manifest, root)
    manifest.chmod(0o600)
    for altered in ({**service, "env_file": "/etc/passwd"}, {**service, "python": "/bin/sh"}, {**service, "name": "../escape"}, {**service, "role": "supervisor"}, {**service, "port": True}):
        private_write(manifest, {"services": [altered]})
        with pytest.raises(ConfigError):
            load_manifest(manifest, root)
    private_write(manifest, {"services": [service, service]})
    with pytest.raises(ConfigError, match="Duplicate"):
        load_manifest(manifest, root)
    private_write(manifest, {"services": [service]})
    outside = root / "outside.json"
    outside.symlink_to(manifest)
    with pytest.raises(ConfigError):
        load_manifest(outside, root)


def test_manifest_accepts_35_service_mig_topology_and_rejects_more_than_64(tmp_path):
    root, manifest, service = app_fixture(tmp_path)
    services = [
        {**service, "role": "api", "name": "a", "port": 8082},
        {**service, "role": "api", "name": "b", "port": 8083},
        {**service, "role": "gateway", "name": "edge", "port": 8080},
    ]
    for device in range(16):
        services.extend([
            {**service, "role": "engine", "name": f"mig{device}", "port": 8200 + device,
             "gpu": f"MIG-00000000-0000-0000-0000-{device:012x}"},
            {**service, "role": "worker", "name": f"mig{device}", "port": 9200 + device},
        ])
    private_write(manifest, {"services": services})
    loaded = load_manifest(manifest, root)[1]
    assert len(loaded) == 35
    assert all(entry["gpu"].startswith("MIG-") for entry in loaded if entry["role"] == "engine")
    assert [entry["port"] for entry in loaded if entry["role"] == "worker"] == list(range(9200, 9216))
    private_write(manifest, {"services": [{**service, "name": f"worker{index}"} for index in range(64)]})
    assert len(load_manifest(manifest, root)[1]) == 64
    private_write(manifest, {"services": [{**service, "name": f"worker{index}"} for index in range(65)]})
    with pytest.raises(ConfigError, match="one to 64"):
        load_manifest(manifest, root)


@pytest.mark.skipif(sys.platform != "linux", reason="Real crash/restart identity drill requires Linux /proc")
def test_linux_process_crash_restarts_and_watchdog_rolling_restart_preserves_child(tmp_path):
    root, manifest, service = app_fixture(tmp_path)
    repository = Path(__file__).resolve().parents[1]
    (root / "ops").mkdir()
    for name in ("manage.py", "supervise.py"):
        shutil.copyfile(repository / "ops" / name, root / "ops" / name)
    (root / "worker").mkdir()
    (root / "worker/__init__.py").write_text("")
    (root / "worker/__main__.py").write_text("import time\nwhile True: time.sleep(0.1)\n")
    # An intentionally closed metrics port proves readiness never causes kills.
    service["port"] = 1
    private_write(manifest, {"services": [service]})
    state_file = root / ".runtime/supervision-supervisor.json"
    command = [sys.executable, str(root / "ops/supervise.py"), "--manifest", str(manifest), "--poll-seconds", "0.1", "--initial-backoff-seconds", "0.1", "--max-backoff-seconds", "0.5", "--stable-seconds", "1"]
    processes = []

    def wait_state(predicate):
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if state_file.exists():
                snapshot = json.loads(state_file.read_text())
                state = snapshot["services"]["worker-crasher"]
                if predicate(snapshot, state):
                    return state
            time.sleep(0.03)
        pytest.fail("Watchdog subprocess did not reach expected state")

    def launch():
        child = subprocess.Popen(command, cwd=root, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        processes.append(child)
        return child

    try:
        watcher = launch()
        first = wait_state(lambda _, state: state["running"] is True and state["ready"] is False)
        first_pid = first["pid"]
        assert Path(f"/proc/{first_pid}/cwd").resolve() == root.resolve()
        os.kill(first_pid, signal.SIGKILL)
        second = wait_state(lambda _, state: state["running"] is True and state["pid"] != first_pid and state["restart_count"] == 1)
        second_pid = second["pid"]
        watcher.terminate()
        assert watcher.wait(timeout=10) == 0
        assert Path(f"/proc/{second_pid}/cwd").resolve() == root.resolve()
        assert json.loads(state_file.read_text())["watchdog_running"] is False
        watcher = launch()
        preserved = wait_state(lambda snapshot, state: snapshot["watchdog_pid"] == watcher.pid and snapshot["watchdog_running"] and state["running"] is True)
        assert preserved["pid"] == second_pid
        assert preserved["restart_count"] == 1
        assert stat.S_IMODE(state_file.stat().st_mode) == 0o600
        assert "never-print-this-value" not in state_file.read_text()
        assert "never-print-this-value" not in (root / ".runtime/supervision-supervisor.jsonl").read_text()
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=10)
        subprocess.run([sys.executable, str(root / "ops/manage.py"), "stop", "worker", "--name", "crasher", "--grace-seconds", "2"], cwd=root, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
