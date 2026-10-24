"""Watchdog control invariants plus a real Linux process crash/restart drill."""
from __future__ import annotations

import importlib.util
import json
import math
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


def test_engine_default_command_retains_the_existing_single_gpu_argv():
    from ops.manage import command

    args = SimpleNamespace(role="engine", python="/owned/python", port=8100, gpu="0")
    assert command(args, {}) == [
        "/owned/python", "-m", "vllm.entrypoints.openai.api_server",
        "--model", "Qwen/Qwen2.5-0.5B-Instruct", "--served-model-name", "Qwen/Qwen2.5-0.5B-Instruct",
        "--host", "127.0.0.1", "--port", "8100", "--tensor-parallel-size", "1", "--dtype", "half",
        "--max-model-len", "2048", "--max-num-seqs", "8", "--gpu-memory-utilization", "0.06",
        "--enforce-eager", "--disable-log-requests", "--no-enable-prefix-caching",
    ]


def test_persisted_tp4_and_chunked_prefill_preserve_the_released_memory_budget(tmp_path, monkeypatch):
    from ops.manage import command, load_env

    for key in ("ENGINE_TENSOR_PARALLEL_SIZE", "ENGINE_ENABLE_CHUNKED_PREFILL", "BATCHFORGE_MIG_PLATFORM", "VLLM_PLUGINS"):
        monkeypatch.delenv(key, raising=False)
    path = tmp_path / "engine.env"
    base = ("MODEL_NAME=openai/gpt-oss-120b\nENGINE_DTYPE=bfloat16\n"
            "ENGINE_MAX_MODEL_LEN=18440\nENGINE_MAX_NUM_SEQS=4\n"
            "ENGINE_GPU_MEMORY_UTILIZATION=0.24\nENGINE_MAX_NUM_BATCHED_TOKENS=2048\n")
    args = SimpleNamespace(role="engine", python="/owned/python", port=8300, gpu="0,1,2,3")
    path.write_text(base)
    before = command(args, load_env(path))
    path.write_text(base + "ENGINE_TENSOR_PARALLEL_SIZE=4\nENGINE_ENABLE_CHUNKED_PREFILL=1\n")
    after = command(args, load_env(path))
    assert after[after.index("--tensor-parallel-size") + 1] == "4"
    assert after.count("--enable-chunked-prefill") == 1
    stripped = [part for part in after if part != "--enable-chunked-prefill"]
    stripped[stripped.index("--tensor-parallel-size") + 1] = "1"
    assert stripped == before
    assert after[after.index("--gpu-memory-utilization") + 1] == "0.24"
    assert after[after.index("--max-num-batched-tokens") + 1] == "2048"
    assert "--cpu-offload-gb" not in after
    path.write_text(base + "ENGINE_ENABLE_CHUNKED_PREFILL=0\n")
    assert command(args, load_env(path)) == before


@pytest.mark.parametrize("value", ["0", "-1", "1.5", "true", "", " 4", "04"])
def test_invalid_tensor_parallel_configuration_fails_before_launch(value):
    from ops.manage import command

    args = SimpleNamespace(role="engine", python="/owned/python", port=8300, gpu="0,1,2,3")
    with pytest.raises(ValueError, match="positive integer"):
        command(args, {"ENGINE_TENSOR_PARALLEL_SIZE": value})


@pytest.mark.parametrize("mask", [None, "", "all", "0,1,2", "0,1,2,3,4", "0,1,2,2", "0,1,2,02", "0,1,2,", "MIG-a,MIG-b,MIG-c,MIG-d"])
def test_tensor_parallel_requires_the_exact_unique_non_mig_device_mask(mask):
    from ops.manage import command

    args = SimpleNamespace(role="engine", python="/owned/python", port=8300, gpu=mask)
    with pytest.raises(ValueError):
        command(args, {"ENGINE_TENSOR_PARALLEL_SIZE": "4"})


def test_tensor_parallel_uuid_masks_and_mig_plugin_guards():
    from ops.manage import command

    uuids = [f"GPU-00000000-0000-0000-0000-{index:012x}" for index in range(4)]
    args = SimpleNamespace(role="engine", python="/owned/python", port=8300, gpu=None)
    env = {"ENGINE_TENSOR_PARALLEL_SIZE": "4", "CUDA_VISIBLE_DEVICES": ",".join(uuids)}
    assert command(args, env)[command(args, env).index("--tensor-parallel-size") + 1] == "4"
    for change in ({"BATCHFORGE_MIG_PLATFORM": "1"}, {"VLLM_PLUGINS": "other,batchforge_mig"}):
        with pytest.raises(ValueError, match="MIG platform"):
            command(args, {**env, **change})
    for mask in (",".join([*uuids[:3], uuids[0].upper().replace("GPU", "GPU")]), ",".join([*uuids[:3], "3"])):
        args.gpu = mask
        with pytest.raises(ValueError, match="unique"):
            command(args, env)
    # An explicit --gpu takes precedence over a broader inherited environment.
    args.gpu = "0,1,2,3"
    env["CUDA_VISIBLE_DEVICES"] = "0"
    assert "4" == command(args, env)[command(args, env).index("--tensor-parallel-size") + 1]


def test_chunked_prefill_does_not_require_tensor_parallel_or_change_mig_tp1():
    from ops.manage import command

    args = SimpleNamespace(role="engine", python="/owned/python", port=8200, gpu="MIG-one")
    env = {"ENGINE_ENABLE_CHUNKED_PREFILL": "1", "BATCHFORGE_MIG_PLATFORM": "1"}
    cmd = command(args, env)
    assert cmd[cmd.index("--tensor-parallel-size") + 1] == "1"
    assert "--enable-chunked-prefill" in cmd
    for value in ("true", "-1", "", "2"):
        with pytest.raises(ValueError, match="0 or 1"):
            command(args, {"ENGINE_ENABLE_CHUNKED_PREFILL": value})


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


def test_custom_collective_switch_survives_restart_without_other_argv_changes(tmp_path, monkeypatch):
    from ops.manage import command, load_env

    monkeypatch.delenv("ENGINE_DISABLE_CUSTOM_ALL_REDUCE", raising=False)
    environment = tmp_path / "engine.env"
    original = "ENGINE_TENSOR_PARALLEL_SIZE=4\nENGINE_ENABLE_CHUNKED_PREFILL=1\n"
    args = SimpleNamespace(role="engine", python="/owned/python", port=8300, gpu="0,1,2,3")
    environment.write_text(original)
    baseline = command(args, load_env(environment))
    flag = "--disable-custom-all-reduce"
    assert flag not in baseline
    environment.write_text(original + "ENGINE_DISABLE_CUSTOM_ALL_REDUCE=1\n")
    restarted = command(args, load_env(environment))
    assert restarted.count(flag) == 1
    assert [part for part in restarted if part != flag] == baseline
    environment.write_text(original + "ENGINE_DISABLE_CUSTOM_ALL_REDUCE=0\n")
    assert command(args, load_env(environment)) == baseline
    for invalid in ("true", "false", "", "2", "1 --unsafe-extra-argument"):
        with pytest.raises(ValueError, match="ENGINE_DISABLE_CUSTOM_ALL_REDUCE"):
            command(args, {"ENGINE_DISABLE_CUSTOM_ALL_REDUCE": invalid})


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


def test_cpu_expert_restart_rechecks_container_memory_before_cuda(tmp_path, monkeypatch):
    import ops.supervise as supervise

    env = tmp_path / 'engine.env'
    env.write_text('ENGINE_BACKEND=llama_cpp\nENGINE_MODEL_PATH=/owned/model.gguf\n')
    env.chmod(0o600)
    service = {**SERVICE, 'role': 'engine', 'gpu': 'MIG-one', 'env_file': str(env)}
    monkeypatch.setattr(supervise, 'host_admission', lambda model: False)
    monkeypatch.setattr(supervise.subprocess, 'run', lambda *a, **kw: pytest.fail('GPU probe must not run without CPU memory'))
    assert supervise.Manager().start_gate(service) == (False, 'host_memory_insufficient')
    def unavailable(model):
        raise OSError('missing cgroup mount')
    monkeypatch.setattr(supervise, 'host_admission', unavailable)
    assert supervise.Manager().start_gate(service) == (False, 'host_memory_probe_unavailable')


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


def test_manifest_multi_gpu_requires_matching_persisted_tp_and_preserves_mig_tp1(tmp_path, monkeypatch):
    root, manifest, service = app_fixture(tmp_path)
    monkeypatch.delenv("ENGINE_TENSOR_PARALLEL_SIZE", raising=False)
    environment = Path(service["env_file"])
    engine = {**service, "role": "engine", "name": "tp4", "port": 8300, "gpu": "0,1,2,3"}
    private_write(manifest, {"services": [engine]})
    with pytest.raises(ConfigError, match="tensor parallel"):
        load_manifest(manifest, root)
    environment.write_text("ENGINE_TENSOR_PARALLEL_SIZE=4\n")
    assert load_manifest(manifest, root)[1][0]["gpu"] == "0,1,2,3"
    uuids = ",".join(f"GPU-00000000-0000-0000-0000-{index:012x}" for index in range(4))
    assert len(uuids) > 128
    private_write(manifest, {"services": [{**engine, "gpu": uuids}]})
    assert load_manifest(manifest, root)[1][0]["gpu"] == uuids
    for text in ("ENGINE_TENSOR_PARALLEL_SIZE=3\n", "ENGINE_TENSOR_PARALLEL_SIZE=4\nBATCHFORGE_MIG_PLATFORM=1\n", "ENGINE_TENSOR_PARALLEL_SIZE=0\n"):
        environment.write_text(text)
        with pytest.raises(ConfigError, match="tensor parallel"):
            load_manifest(manifest, root)
    environment.write_text("ENGINE_TENSOR_PARALLEL_SIZE=4\n")
    private_write(manifest, {"services": [{**engine, "gpu": "0"}]})
    with pytest.raises(ConfigError, match="tensor parallel"):
        load_manifest(manifest, root)
    environment.write_text("BATCHFORGE_MIG_PLATFORM=1\n")
    private_write(manifest, {"services": [{**engine, "gpu": "MIG-00000000-0000-0000-0000-000000000001"}]})
    assert load_manifest(manifest, root)[1][0]["gpu"].startswith("MIG-")


def gpu_gate_fixture(tmp_path, monkeypatch, devices, *, tp=4):
    from ops.supervise import Manager

    # A real interpreter runs the actual short probe program. This tiny CPU-only
    # torch facade exercises rank enumeration/environment isolation without a GPU.
    (tmp_path / "torch.py").write_text(
        "import json,os\n"
        "rows=json.loads(os.environ['BF_TEST_GPU_ROWS'])\n"
        "assert os.environ['CUDA_VISIBLE_DEVICES']==os.environ['BF_TEST_GPU_MASK']\n"
        "class cuda:\n"
        " @staticmethod\n"
        " def device_count():\n"
        "  if os.environ.get('BF_TEST_NO_COUNT')=='1': raise AssertionError('legacy MIG probe changed')\n"
        "  return len(rows)\n"
        " @staticmethod\n"
        " def mem_get_info(rank): return rows[rank]\n"
    )
    envfile = tmp_path / "engine.env"
    envfile.write_text(f"ENGINE_TENSOR_PARALLEL_SIZE={tp}\nENGINE_GPU_MEMORY_UTILIZATION=0.24\n")
    envfile.chmod(0o600)
    mask = ",".join(str(index) for index in range(tp)) if tp > 1 else "MIG-one"
    monkeypatch.setenv("BF_TEST_GPU_ROWS", json.dumps(devices))
    monkeypatch.setenv("BF_TEST_GPU_MASK", mask)
    for key in ("BATCHFORGE_MIG_PLATFORM", "VLLM_PLUGINS"):
        monkeypatch.delenv(key, raising=False)
    service = {"role": "engine", "name": "gpu-probe-only", "gpu": mask, "env_file": str(envfile), "python": sys.executable}
    manager = Manager(root=tmp_path)
    # Rank cleanup has separate identity/pidfd tests. Isolate that preceding
    # gate here so the actual CPU probe program remains testable on macOS.
    monkeypatch.setattr(manager, "invoke", lambda service, action: ({"status": "reaped", "remaining": 0}, None))
    return manager, service


def test_real_probe_requires_every_tp_rank_to_release_its_memory_budget(tmp_path, monkeypatch):
    gib = 1024**3
    manager, service = gpu_gate_fixture(tmp_path, monkeypatch, [[20 * gib, 80 * gib] for _ in range(4)])
    assert manager.start_gate(service) == (True, None)
    # Three free ranks cannot hide a single constrained rank, even when average
    # aggregate free memory exceeds the total requested memory.
    rows = [[30 * gib, 80 * gib] for _ in range(3)] + [[19 * gib, 80 * gib]]
    monkeypatch.setenv("BF_TEST_GPU_ROWS", json.dumps(rows))
    assert manager.start_gate(service) == (False, "gpu_memory_not_released")


def test_measured_tp4_resident_memory_requires_the_explicit_one_gib_margin(tmp_path, monkeypatch):
    mib = 1024**2
    total, observed_resident = 81559*mib, 21984*mib
    manager, service = gpu_gate_fixture(tmp_path, monkeypatch, [[observed_resident, total]]*4)
    environment = Path(service["env_file"])
    environment.write_text("ENGINE_TENSOR_PARALLEL_SIZE=4\nENGINE_GPU_MEMORY_UTILIZATION=0.26\n")
    # The legacy default admits this much free memory even though the measured
    # rank already uses it; the actual deployment therefore opts into 1 GiB.
    assert manager.start_gate(service) == (True, None)
    with environment.open("a") as file:
        file.write("ENGINE_GPU_MEMORY_MARGIN_MIB=1024\n")
    assert manager.start_gate(service) == (False, "gpu_memory_not_released")
    threshold = math.ceil(total*.26 +1024*mib)
    monkeypatch.setenv("BF_TEST_GPU_ROWS", json.dumps([[threshold,total]]*4))
    assert manager.start_gate(service) == (True, None)
    # One rank a byte below its own gate cannot be covered by the other three.
    monkeypatch.setenv("BF_TEST_GPU_ROWS", json.dumps([[threshold+10*mib,total]]*3+[[threshold-1,total]]))
    assert manager.start_gate(service) == (False, "gpu_memory_not_released")


@pytest.mark.parametrize("value", ["", "511", "16385", "-512", "+512", "1.5", "1e3", "True", "NaN"])
def test_invalid_memory_margin_fails_closed_before_any_cuda_probe(tmp_path, monkeypatch, value):
    import ops.supervise as supervise
    manager, service = gpu_gate_fixture(tmp_path, monkeypatch, [[40*1024**3,80*1024**3]]*4)
    with Path(service["env_file"]).open("a") as file:
        file.write("ENGINE_GPU_MEMORY_MARGIN_MIB="+value+"\n")
    monkeypatch.setattr(supervise.subprocess, "run", lambda *args, **kwargs: pytest.fail("Invalid margin must not start a probe"))
    assert manager.start_gate(service) == (False, "gpu_configuration_invalid")
    with pytest.raises(ValueError, match="ENGINE_GPU_MEMORY_MARGIN_MIB"):
        supervise.engine_settings(Path(service["env_file"]))


def test_upper_bounded_margin_also_applies_to_the_legacy_single_mig_probe(tmp_path, monkeypatch):
    gib = 1024**3
    manager, service = gpu_gate_fixture(tmp_path, monkeypatch, [[36*gib,80*gib]], tp=1)
    monkeypatch.setenv("BF_TEST_NO_COUNT", "1")
    with Path(service["env_file"]).open("a") as file:
        file.write("ENGINE_GPU_MEMORY_MARGIN_MIB=16384\n")
    assert manager.start_gate(service) == (True, None)
    monkeypatch.setenv("BF_TEST_GPU_ROWS", json.dumps([[35*gib,80*gib]]))
    assert manager.start_gate(service) == (False, "gpu_memory_not_released")


@pytest.mark.parametrize("devices", [
    [[20 * 1024**3, 80 * 1024**3]] * 3,
    [[20 * 1024**3, 80 * 1024**3]] * 5,
    [[True, 80 * 1024**3]] * 4,
    [[-1, 80 * 1024**3]] * 4,
    [[0, 0]] * 4,
])
def test_real_probe_device_count_and_malformed_memory_fail_closed(tmp_path, monkeypatch, devices):
    manager, service = gpu_gate_fixture(tmp_path, monkeypatch, devices)
    assert manager.start_gate(service) == (False, "gpu_probe_unavailable")


def test_single_mig_probe_keeps_legacy_mem_info_path(tmp_path, monkeypatch):
    manager, service = gpu_gate_fixture(tmp_path, monkeypatch, [[20 * 1024**3, 80 * 1024**3]], tp=1)
    monkeypatch.setenv("BF_TEST_NO_COUNT", "1")
    monkeypatch.setenv("BATCHFORGE_MIG_PLATFORM", "1")
    assert manager.start_gate(service) == (True, None)


@pytest.mark.skipif(sys.platform != "linux", reason="Real crash/restart identity drill requires Linux /proc")
def test_linux_process_crash_restarts_and_watchdog_rolling_restart_preserves_child(tmp_path):
    root, manifest, service = app_fixture(tmp_path)
    repository = Path(__file__).resolve().parents[1]
    (root / "ops").mkdir()
    for name in ("manage.py", "supervise.py", "engine_members.py", "host_memory.py", "llama_backend.py"):
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
