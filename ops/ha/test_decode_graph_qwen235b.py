import copy
import json
from pathlib import Path

import pytest

from ops.ha import decode_graph_qwen235b as module
from ops.ha.retune_full_gpu import file_env
from ops.ha.test_collective_qwen235b import prepared as baseline_fixture


CAPABILITY = {"vllm_version": "0.10.2", "flash_attention_version": 3, "cudagraph_support": "ALWAYS"}
GRAPH_LOG = (b"GPU KV cache size: 314,096 tokens\n"
    b"compilation_config={level:0,cudagraph_mode:FULL_DECODE_ONLY,cudagraph_capture_sizes:[64,48,32,24,16,8,4,2,1]}\n"
    b"Capturing CUDA graphs (decode, FULL): 100%\n"
    b"Graph capturing finished in 8 secs, took 0.75 GiB\n")


def prepared(tmp_path, *, mode="decode_graph", shared=280000):
    base, manager, kwargs = baseline_fixture(tmp_path)
    run = tmp_path / ".runtime/decode-experiment"
    run.mkdir(mode=0o700)
    support = {"source_sha256": base.support["source_sha256"], "capability": CAPABILITY}
    kwargs = {**kwargs, "run": run, "support": support, "mode": mode, "shared_token_budget": shared,
              "path_observer": lambda candidate: {"candidate": candidate, "argv_verified": True}}
    roll = module.DecodeGraphRoll(**kwargs)
    def log(service):
        if service["role"] == "engine":
            manager.cache_log = (GRAPH_LOG.decode() if service["env_file"] == roll.candidate_engine["env_file"] and mode == "decode_graph"
                                 else "GPU KV cache size: 314,096 tokens\n")
    manager.before_start = log
    return roll, manager, kwargs


@pytest.mark.parametrize("mode", ["eager", "decode_graph"])
def test_prepare_changes_only_execution_and_explicit_shared_budget(tmp_path, mode):
    roll, manager, _ = prepared(tmp_path, mode=mode)
    assert manager.calls == []
    engine = file_env(roll.run / "engine.env")
    assert {key: value for key, value in engine.items() if key not in {"ENGINE_ENFORCE_EAGER", "ENGINE_COMPILATION_CONFIG"}} == roll.engine_env
    worker = file_env(roll.run / "worker.env")
    assert worker["WORKER_TOKEN_BUDGET"] == "280000"
    assert worker["WORKER_MAX_INFLIGHT"] == "64"
    assert roll.profile["runtime_fingerprint"] != roll.original_profile["runtime_fingerprint"]
    for key in ("prefill_tokens_per_second", "decode_tokens_per_second", "hourly_cost_usd"):
        assert roll.profile[key] == roll.original_profile[key]
    assert "not-recalibrated" in roll.profile["calibration_label"]
    assert "worker-token-placeholder" not in json.dumps(roll.report)
    if mode == "decode_graph":
        assert json.loads(engine["ENGINE_COMPILATION_CONFIG"]) == module.COMPILATION
        assert engine["ENGINE_ENFORCE_EAGER"] == "0"
    else:
        assert "ENGINE_COMPILATION_CONFIG" not in engine and engine["ENGINE_ENFORCE_EAGER"] == "1"


@pytest.mark.parametrize("value", [0, 18440, 18448.0, True, 298400, 300000, 280001])
def test_shared_budget_is_not_rounded_or_silently_shrunk(value):
    with pytest.raises(ValueError): module.budget(value)


def test_capture_requires_fresh_success_mode_and_no_downgrade():
    result = module.capture_evidence(GRAPH_LOG, "decode_graph")
    assert result["measured_cache_tokens"] == 314096 and result["capture_completion_lines"] == 1
    for broken in (GRAPH_LOG.replace(b"FULL_DECODE_ONLY", b"FULL_AND_PIECEWISE"),
                   GRAPH_LOG.replace(b"level:0", b"level:3"),
                   GRAPH_LOG.replace(b"64,48,32,24", b"128,48,32,24"),
                   GRAPH_LOG.replace(b"Graph capturing finished", b"Graph capture pending"),
                   GRAPH_LOG + b"setting cudagraph_mode=NONE\n",
                   GRAPH_LOG + b"setting cudagraph_mode=FULL_AND_PIECEWISE\n",
                   GRAPH_LOG + b"Skipping CUDA graph capture. Please ensure cudagraph_mode is not NONE.\n",
                   GRAPH_LOG + b"Capturing CUDA graphs (mixed prefill-decode, FULL)\n"):
        with pytest.raises(RuntimeError): module.capture_evidence(broken, "decode_graph")
    with pytest.raises(RuntimeError): module.capture_evidence(GRAPH_LOG, "eager")


def test_capability_and_mode_budget_sources_change_fingerprint(tmp_path):
    roll, _, _ = prepared(tmp_path)
    same = module.candidate_profile(roll.original_profile, "decode_graph", 280000, roll.support)[0]
    assert same == roll.profile
    for mode, shared in (("eager", 280000), ("decode_graph", 280016)):
        assert module.candidate_profile(roll.original_profile, mode, shared, roll.support)[0]["runtime_fingerprint"] != same["runtime_fingerprint"]
    for change in ({"vllm_version": "0.11.0"}, {"flash_attention_version": 1}, {"cudagraph_support": "NEVER"}):
        with pytest.raises(RuntimeError): module.validate_capability({**CAPABILITY, **change})


def test_activation_verifies_capture_before_worker_and_protects_other_services(tmp_path):
    roll, manager, _ = prepared(tmp_path)
    observed = []
    def engine_probe():
        assert manager.identity(roll.worker) is None
        assert not any(service["role"] == "worker" for service in json.loads(roll.manifest_path.read_bytes())["services"])
        observed.append("engine")
        return {"engine_running_waiting_zero": True}
    roll.engine_observer = engine_probe
    report = roll.apply()
    assert report["phase"] == "complete" and report["watchdog_restored"]
    assert report["candidate_engine_preflight"]["preserved_budget_tokens"] == 280000
    assert observed == ["engine"]
    assert report["engine_startup_logs"][0]["sha256"] == module.sha(GRAPH_LOG)
    assert not any(key.startswith(("api-", "gateway-")) for key, _, _ in manager.calls)


def test_insufficient_cache_fails_shared_budget_and_restores_exact_original(tmp_path):
    roll, manager, _ = prepared(tmp_path)
    def log(service):
        if service["role"] == "engine":
            manager.cache_log = (GRAPH_LOG.replace(b"314,096", b"280,000").decode()
                if service["env_file"] == roll.candidate_engine["env_file"] else "GPU KV cache size: 314,096 tokens\n")
    manager.before_start = log
    with pytest.raises(RuntimeError, match="no automatic shrinking"): roll.apply()
    assert roll.report["phase"] == "rolled_back" and roll.report["watchdog_restored"]
    assert roll.manifest_path.read_bytes() == roll.original
    assert roll.report["rollback_engine_preflight"]["preserved_budget_tokens"] == 298384
    assert len(roll.report["engine_startup_logs"]) == 2
    assert not any(key == "worker-qwen235b" and action == "start" and path == roll.candidate_worker["env_file"] for key, action, path in manager.calls)


@pytest.mark.parametrize("changed", ["env", "pid", "source", "snapshot", "manifest"])
def test_drift_before_apply_never_mutates_services(tmp_path, changed):
    roll, manager, _ = prepared(tmp_path)
    if changed == "env": Path(roll.worker["env_file"]).write_bytes(b"CHANGED=1\n")
    elif changed == "pid": manager.identities["api-a"]["start_ticks"] = "replacement"
    elif changed == "source": Path(next(iter(roll.support["source_sha256"]))).write_text("changed")
    elif changed == "snapshot": (roll.run / "original-engine.env").write_bytes(b"CHANGED=1\n")
    else: roll.manifest_path.write_bytes(b'{"foreign":true}')
    with pytest.raises(RuntimeError): roll.apply()
    assert manager.calls == []


def test_reprepare_keeps_all_evidence_and_rejects_shared_budget_drift(tmp_path):
    roll, manager, kwargs = prepared(tmp_path)
    saved = {path.name: path.read_bytes() for path in roll.run.iterdir()}
    module.DecodeGraphRoll(**kwargs, previous=copy.deepcopy(roll.report))
    assert saved == {path.name: path.read_bytes() for path in roll.run.iterdir()}
    with pytest.raises(RuntimeError):
        module.DecodeGraphRoll(**{**kwargs, "shared_token_budget": 280016}, previous=copy.deepcopy(roll.report))
    assert manager.calls == []


def activated(tmp_path):
    activation, manager, _ = prepared(tmp_path)
    activation.apply()
    restore = tmp_path / ".runtime/restore-decode"; restore.mkdir(mode=0o700)
    manager.calls.clear()
    kwargs = dict(root=tmp_path, run=restore, manifest_path=activation.manifest_path, watchdog=activation.watchdog,
        support=activation.support, activation_run=activation.run,
        activation_sha256=module.sha((activation.run / "report.json").read_bytes()),
        manager=manager, identity_reader=manager.identity, assert_drained=lambda: None,
        engine_observer=lambda: {"engine_running_waiting_zero": True}, worker_observer=lambda profile: {"registration_verified": True},
        path_observer=lambda candidate: {"candidate": candidate, "argv_verified": True}, clock=activation.clock, sleep=activation.sleep)
    return activation, manager, kwargs


def test_explicit_restore_requires_complete_sha_and_restores_original_paths_profiles_budget(tmp_path):
    activation, manager, kwargs = activated(tmp_path)
    original_report = (activation.run / "report.json").read_bytes()
    current = activation.manifest_path.read_bytes()
    roll = module.DecodeGraphRoll(**kwargs)
    assert manager.calls == [] and activation.manifest_path.read_bytes() == current
    report = roll.apply()
    assert report["phase"] == "complete" and report["restored"] and report["watchdog_restored"]
    assert report["restored_engine_preflight"]["preserved_budget_tokens"] == 298384
    assert roll.manifest_path.read_bytes() == activation.original
    assert all(path.read_bytes() == raw for path, raw in roll.env_bytes.items())
    assert (activation.run / "report.json").read_bytes() == original_report
    assert not any(key.startswith(("api-", "gateway-")) for key, _, _ in manager.calls)


@pytest.mark.parametrize("changed", ["sha", "activation", "snapshot", "pid", "source", "manifest"])
def test_restore_rejects_drift_without_any_service_mutation(tmp_path, changed):
    activation, manager, kwargs = activated(tmp_path)
    if changed == "sha": kwargs["activation_sha256"] = "0" * 64
    elif changed == "activation": (activation.run / "report.json").write_text("{}")
    elif changed == "snapshot": (activation.run / "original-engine.env").write_text("CHANGED=1")
    elif changed == "pid": manager.identities["engine-qwen235b"]["start_ticks"] = "replacement"
    elif changed == "source": Path(next(iter(activation.support["source_sha256"]))).write_text("changed")
    else: activation.manifest_path.write_bytes(activation.original)
    with pytest.raises(RuntimeError): module.DecodeGraphRoll(**kwargs)
    assert manager.calls == []


def test_failed_capture_and_failed_restoration_exclude_worker(tmp_path):
    roll, manager, _ = prepared(tmp_path)
    manager.before_start = lambda service: setattr(manager, "cache_log", "GPU KV cache size: 200,000 tokens\n")
    with pytest.raises(RuntimeError): roll.apply()
    assert roll.report["phase"] == "manual_recovery_required"
    assert roll.report["worker_excluded_and_owned_worker_stopped"]
    assert manager.identity(roll.worker) is None
    assert not any(service["role"] == "worker" for service in json.loads(roll.manifest_path.read_bytes())["services"])


def test_nondrained_prepare_never_stops_services(tmp_path):
    roll, manager, kwargs = prepared(tmp_path)
    def busy(): raise RuntimeError("accepted tasks remain")
    with pytest.raises(RuntimeError): module.DecodeGraphRoll(**{**kwargs, "assert_drained": busy}, previous=copy.deepcopy(roll.report))
    assert manager.calls == []


def test_original_and_candidate_registration_probes_use_their_exact_budgets(tmp_path, monkeypatch):
    roll, _, _ = prepared(tmp_path)
    monkeypatch.setattr(module.CollectiveRoll, "_observe_profile", lambda current, profile: {"observed_budget": current.budget})
    assert roll._observe_profile(roll.original_profile)["observed_budget"] == 298384
    assert roll._observe_profile(roll.profile)["observed_budget"] == 280000
    assert roll.budget == 280000


def test_actual_argv_and_effective_environment_are_both_required(tmp_path, monkeypatch):
    roll, _, _ = prepared(tmp_path)
    expected = file_env(roll.run / "engine.env")
    args = module.SimpleNamespace(role="engine", python=roll.engine["python"], port=roll.engine["port"], gpu=roll.engine["gpu"])
    wanted = module.manage.command(args, expected)
    record = {"pid": 123, "start_ticks": "456", "cwd": str(tmp_path), "command": wanted}
    proc = {"/proc/123/cmdline": b"\0".join(item.encode() for item in wanted) + b"\0",
            "/proc/123/environ": b"\0".join(f"{key}={value}".encode() for key, value in expected.items()) + b"\0"}
    class ProcPath:
        def __init__(self, value): self.value = value
        def read_bytes(self): return proc[self.value]
    monkeypatch.setattr(module, "Path", ProcPath)
    monkeypatch.setattr(module.manage, "owned_record", lambda path: record)
    monkeypatch.setattr(module, "identity", lambda root, engine: {key: record[key] for key in ("pid", "start_ticks", "cwd")})
    monkeypatch.setattr(module, "running_collective", lambda root, engine, disabled: {"disable_custom_all_reduce": False})
    assert module.running_execution(tmp_path, roll.engine, expected)["actual_argv_and_environment_verified"]
    proc["/proc/123/environ"] = proc["/proc/123/environ"].replace(b"ENGINE_ENFORCE_EAGER=0", b"ENGINE_ENFORCE_EAGER=1")
    with pytest.raises(RuntimeError, match="environment"):
        module.running_execution(tmp_path, roll.engine, expected)
    proc["/proc/123/cmdline"] = proc["/proc/123/cmdline"].replace(b"--compilation-config", b"--unreviewed-config")
    with pytest.raises(RuntimeError, match="argv"):
        module.running_execution(tmp_path, roll.engine, expected)


def test_failed_explicit_restore_excludes_worker_and_preserves_activation(tmp_path):
    activation, manager, kwargs = activated(tmp_path)
    before = (activation.run / "report.json").read_bytes()
    roll = module.DecodeGraphRoll(**kwargs)
    def registry(profile):
        if profile == roll.original_profile: raise RuntimeError("original registration differs")
        return {"registration_verified": True}
    roll.profile_observer = registry
    with pytest.raises(RuntimeError, match="original registration"):
        roll.apply()
    assert roll.report["phase"] == "manual_recovery_required" and not roll.report["restored"]
    assert roll.report["worker_excluded_and_owned_worker_stopped"]
    assert manager.identity(roll.worker) is None
    assert (activation.run / "report.json").read_bytes() == before


def test_foreign_manifest_mid_activation_is_never_overwritten(tmp_path):
    roll, manager, _ = prepared(tmp_path)
    foreign = b'{"foreign":true}\n'
    def mutate(service):
        if service["role"] == "worker": roll.manifest_path.write_bytes(foreign)
    manager.after_stop = mutate
    with pytest.raises(RuntimeError): roll.apply()
    assert roll.report["phase"] == "manual_recovery_required"
    assert roll.report["watchdog_restored"] is False
    assert roll.manifest_path.read_bytes() == foreign


@pytest.mark.parametrize("relative", ["v1/cudagraph_dispatcher.py", "compilation/cuda_graph.py"])
def test_actual_capture_dispatch_implementations_are_bound_and_drift_refuses_mutation(tmp_path, relative):
    assert relative in module.VLLM_SOURCES
    roll, manager, _ = prepared(tmp_path)
    path = tmp_path / "installed-vllm" / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("# inspected capture/dispatch implementation\n")
    # The real source_evidence includes every VLLM_SOURCES member. Bind this
    # installation-shaped path before the preparation's source snapshot, then
    # show that a later change cannot reach any service mutation.
    roll.support["source_sha256"][str(path)] = module.sha(path.read_bytes())
    path.write_text("# changed capture/dispatch behavior\n")
    with pytest.raises(RuntimeError, match="execution source changed"):
        roll.apply()
    assert manager.calls == []
