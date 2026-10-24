"""Execution-mode boundary checks; no vLLM import, process launch or GPU use."""
from __future__ import annotations

import json
import shlex
from types import SimpleNamespace

import pytest

from ops.manage import command, engine_execution_configuration, load_env


CONFIG = {"level": 0, "cudagraph_mode": "FULL_DECODE_ONLY",
          "cudagraph_capture_sizes": [1, 2, 4, 8, 16, 24, 32, 48, 64]}
ARGS = SimpleNamespace(role="engine", python="/owned/python", port=8300, gpu="0,1,2,3")
BASE = {"MODEL_NAME": "Qwen/Qwen3-235B-A22B-Instruct-2507-FP8",
        "ENGINE_TENSOR_PARALLEL_SIZE": "4", "ENGINE_MAX_NUM_SEQS": "64",
        "ENGINE_MAX_NUM_BATCHED_TOKENS": "4096", "ENGINE_MAX_MODEL_LEN": "18440",
        "ENGINE_GPU_MEMORY_UTILIZATION": ".90", "ENGINE_ENABLE_CHUNKED_PREFILL": "1"}


def graph_env(config=CONFIG):
    return {**BASE, "ENGINE_ENFORCE_EAGER": "0", "ENGINE_COMPILATION_CONFIG": json.dumps(config)}


def test_default_and_explicit_eager_preserve_the_existing_command():
    baseline = command(ARGS, BASE)
    assert command(ARGS, {**BASE, "ENGINE_ENFORCE_EAGER": "1"}) == baseline
    assert baseline.count("--enforce-eager") == 1
    assert "--compilation-config" not in baseline
    assert engine_execution_configuration(BASE) == {"enforce_eager": True}


def test_graph_command_changes_only_explicit_execution_flags_and_is_canonical():
    baseline, candidate = command(ARGS, BASE), command(ARGS, graph_env())
    at = baseline.index("--enforce-eager")
    canonical = json.dumps(CONFIG, sort_keys=True, separators=(",", ":"))
    assert candidate == baseline[:at] + ["--compilation-config", canonical] + baseline[at + 1:]
    assert "--enforce-eager" not in candidate
    assert engine_execution_configuration(graph_env()) == {"enforce_eager": False, "compilation_config": CONFIG}
    reordered = {key: CONFIG[key] for key in reversed(CONFIG)}
    assert command(ARGS, graph_env(reordered)) == candidate


def test_persisted_json_survives_env_reload_as_one_argument_without_shell_evaluation(tmp_path, monkeypatch):
    for key in ("ENGINE_ENFORCE_EAGER", "ENGINE_COMPILATION_CONFIG"):
        monkeypatch.delenv(key, raising=False)
    path = tmp_path / "engine.env"
    path.write_text("\n".join(f"{key}={shlex.quote(value)}" for key, value in graph_env().items()) + "\n")
    cmd = command(ARGS, load_env(path))
    assert cmd == command(ARGS, graph_env())
    assert json.loads(cmd[cmd.index("--compilation-config") + 1]) == CONFIG


@pytest.mark.parametrize("value", ["", "true", "false", "2", " 0", "01", 0, 1, None, False])
def test_invalid_eager_values_fail_closed(value):
    with pytest.raises(ValueError, match="0 or 1"):
        command(ARGS, {**BASE, "ENGINE_ENFORCE_EAGER": value})


@pytest.mark.parametrize("value", [None, "", " ", "not json", "[]", "null", '"string"', "{" * 4097])
def test_graphs_require_explicit_bounded_json(value):
    env = {**BASE, "ENGINE_ENFORCE_EAGER": "0"}
    if value is not None:
        env["ENGINE_COMPILATION_CONFIG"] = value
    with pytest.raises(ValueError):
        command(ARGS, env)


@pytest.mark.parametrize("value", ["", json.dumps(CONFIG)])
def test_eager_does_not_silently_ignore_compilation_configuration(value):
    with pytest.raises(ValueError, match="requires"):
        command(ARGS, {**BASE, "ENGINE_COMPILATION_CONFIG": value})


@pytest.mark.parametrize("change", [
    {"level": 3}, {"level": False}, {"level": 0.0}, {"level": "0"},
    {"cudagraph_mode": "FULL"}, {"cudagraph_mode": "FULL_AND_PIECEWISE"},
    {"cudagraph_mode": "full_decode_only"}, {"cudagraph_mode": None},
    {"backend": "inductor"}, {"custom_ops": ["all"]}, {"pass_config": {}},
    {"cudagraph_capture_sizes": None}, {"cudagraph_capture_sizes": []},
    {"cudagraph_capture_sizes": [1, 64, 32]}, {"cudagraph_capture_sizes": [1, 32, 32, 64]},
    {"cudagraph_capture_sizes": [1, 64, 128]}, {"cudagraph_capture_sizes": [1, 32]},
    {"cudagraph_capture_sizes": [2, 64]}, {"cudagraph_capture_sizes": [0, 1, 64]},
    {"cudagraph_capture_sizes": [True, 64]}, {"cudagraph_capture_sizes": [1, 64.0]},
    {"cudagraph_capture_sizes": [1, "64"]}, {"cudagraph_capture_sizes": [1, {"size": 64}]},
])
def test_unreviewed_configuration_and_invalid_capture_shapes_fail_closed(change):
    with pytest.raises(ValueError):
        command(ARGS, graph_env({**CONFIG, **change}))


@pytest.mark.parametrize("key", CONFIG)
def test_no_configuration_field_is_implicitly_defaulted(key):
    with pytest.raises(ValueError, match="Only level=0"):
        command(ARGS, graph_env({name: value for name, value in CONFIG.items() if name != key}))


@pytest.mark.parametrize("size", ["0", "65", "128", "01", "-1", "1.0", " 64", "", 64, None])
def test_graph_sequence_limit_is_explicit_and_bounded(size):
    with pytest.raises(ValueError, match="ENGINE_MAX_NUM_SEQS"):
        command(ARGS, {**graph_env(), "ENGINE_MAX_NUM_SEQS": size})


def test_duplicate_json_keys_are_rejected_before_canonicalization():
    value = json.dumps(CONFIG)[:-1] + ',"level":3}'
    with pytest.raises(ValueError, match="Duplicate"):
        command(ARGS, {**graph_env(), "ENGINE_COMPILATION_CONFIG": value})


def test_one_sequence_graph_has_no_hidden_compilation_pass():
    env = graph_env({**CONFIG, "cudagraph_capture_sizes": [1]})
    env["ENGINE_MAX_NUM_SEQS"] = "1"
    assert engine_execution_configuration(env)["compilation_config"] == {
        "level": 0, "cudagraph_mode": "FULL_DECODE_ONLY", "cudagraph_capture_sizes": [1]}
