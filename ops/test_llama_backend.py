"""Native engine launch configuration; no process launch or GPU access."""
from pathlib import Path
from types import SimpleNamespace

import pytest

from ops.llama_backend import command


def setup(tmp_path):
    runtime = tmp_path/"batchforge-runtime"
    root = runtime/"app"
    root.mkdir(parents=True)
    binary, model = runtime/"llama-server", runtime/"model.gguf"
    binary.write_text("#!/bin/false\n")
    binary.chmod(0o700)
    model.write_bytes(b"synthetic test GGUF placeholder; never loaded")
    model.chmod(0o600)
    env = {"ENGINE_BACKEND": "llama_cpp", "ENGINE_LLAMA_BINARY": str(binary),
           "ENGINE_MODEL_PATH": str(model), "MODEL_NAME": "openai/gpt-oss-120b"}
    args = SimpleNamespace(role="engine", port=8314, gpu="MIG-00000000-0000-0000-0000-000000000014")
    return root, binary, model, args, env


def test_default_vllm_dispatch_does_not_require_or_inspect_native_files(tmp_path):
    assert command(SimpleNamespace(), {}, tmp_path) is None
    assert command(SimpleNamespace(), {"ENGINE_BACKEND": "vllm"}, tmp_path) is None
    with pytest.raises(ValueError, match="ENGINE_BACKEND"):
        command(SimpleNamespace(), {"ENGINE_BACKEND": "shell"}, tmp_path)


def test_exact_app_owned_native_command_and_reservations(tmp_path):
    root, binary, model, args, env = setup(tmp_path)
    argv = command(args, env, root)
    assert argv == [str(binary), "--model", str(model), "--alias", "openai/gpt-oss-120b",
        "--host", "127.0.0.1", "--port", "8314", "--device", "CUDA0", "--ctx-size", "18440",
        "--parallel", "1", "--n-gpu-layers", "99", "--n-cpu-moe", "35", "--threads", "16",
        "--threads-batch", "16", "--batch-size", "2048", "--ubatch-size", "512", "--fit", "off",
        "--no-context-shift", "--jinja", "--metrics", "--perf"]
    assert "--gpu-memory-utilization" not in argv  # A separate launch gate is not a native reservation.
    assert str(root/".venv/bin/python") not in argv


@pytest.mark.parametrize("key,value", [
    ("ENGINE_MAX_MODEL_LEN", "0"), ("ENGINE_MAX_MODEL_LEN", "131073"),
    ("ENGINE_MAX_NUM_SEQS", "2"), ("ENGINE_LLAMA_THREADS", "257"),
    ("ENGINE_LLAMA_THREADS_BATCH", "-1"), ("ENGINE_LLAMA_N_GPU_LAYERS", "1;touch unsafe"),
    ("ENGINE_LLAMA_N_CPU_MOE", "3.5"), ("ENGINE_LLAMA_UBATCH_SIZE", "2049"),
    ("ENGINE_TENSOR_PARALLEL_SIZE", "4"), ("MODEL_NAME", "one,two"),
])
def test_invalid_bounded_or_ambiguous_config_is_rejected(tmp_path, key, value):
    root, _, _, args, env = setup(tmp_path)
    with pytest.raises(ValueError):
        command(args, {**env, key: value}, root)


def test_binary_and_model_paths_cannot_escape_or_use_writable_executables(tmp_path):
    root, binary, model, args, env = setup(tmp_path)
    binary.chmod(0o777)
    with pytest.raises(ValueError, match="unsafe"):
        command(args, env, root)
    binary.chmod(0o600)
    with pytest.raises(ValueError, match="permissions"):
        command(args, env, root)
    binary.chmod(0o700)
    outside = tmp_path/"outside"
    outside.write_bytes(b"test")
    outside.chmod(0o700)
    with pytest.raises(ValueError, match="inside"):
        command(args, {**env, "ENGINE_LLAMA_BINARY": str(outside)}, root)
    link = binary.with_name("linked-server")
    link.symlink_to(binary)
    with pytest.raises(ValueError, match="symlinks"):
        command(args, {**env, "ENGINE_LLAMA_BINARY": str(link)}, root)
    model.chmod(0o666)
    with pytest.raises(ValueError, match="unsafe"):
        command(args, env, root)


def test_multiplexed_gpu_mask_nonengine_roles_and_bad_ports_are_rejected(tmp_path):
    root, _, _, args, env = setup(tmp_path)
    args.gpu = "0,1"
    with pytest.raises(ValueError, match="one explicit GPU"):
        command(args, env, root)
    args.gpu = "0"
    args.port = True
    with pytest.raises(ValueError, match="port"):
        command(args, env, root)
    args.port, args.role = 8314, "worker"
    with pytest.raises(ValueError, match="engine role"):
        command(args, env, root)
