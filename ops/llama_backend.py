"""Opt-in, app-owned llama.cpp command builder. Never launches a process."""
from __future__ import annotations

import os
from pathlib import Path
import re
import stat


def number(env, name, default, minimum, maximum):
    raw = env.get(name, str(default))
    if not isinstance(raw, str) or not re.fullmatch(r"[0-9]+", raw):
        raise ValueError("Invalid bounded llama.cpp numeric configuration")
    value = int(raw)
    if not minimum <= value <= maximum:
        raise ValueError("llama.cpp numeric configuration is outside its supported bounds")
    return value


def owned_file(value, runtime, *, executable=False):
    if not isinstance(value, str) or not value or "\x00" in value:
        raise ValueError("An explicit application-owned llama.cpp file is required")
    path = Path(value)
    if not path.is_absolute() or not path.is_relative_to(runtime) or path.resolve() != path:
        raise ValueError("llama.cpp files must be inside the application runtime without symlinks")
    try:
        info = path.stat()
    except OSError:
        raise ValueError("Required llama.cpp file is unavailable") from None
    if not stat.S_ISREG(info.st_mode) or info.st_uid not in {0, os.geteuid()} or info.st_mode & 0o022:
        raise ValueError("llama.cpp file ownership or write permissions are unsafe")
    if not os.access(path, os.X_OK if executable else os.R_OK):
        raise ValueError("Required llama.cpp file permissions are unavailable")
    return str(path)


def command(args, env: dict, root: Path) -> list[str] | None:
    """Return None for unchanged vLLM dispatch; one CPU-expert GPU lane otherwise.

    The interpreter remains in the service manifest for CUDA readiness probes;
    the actual inference command runs this verified native binary directly.
    CUDA_VISIBLE_DEVICES is applied by the existing process manager.
    """
    backend = env.get("ENGINE_BACKEND", "vllm")
    if backend not in {"vllm", "llama_cpp"}:
        raise ValueError("ENGINE_BACKEND must be vllm or llama_cpp")
    if backend == "vllm":
        return None
    if getattr(args, "role", "engine") != "engine":
        raise ValueError("llama.cpp backend is restricted to the engine role")
    if env.get("ENGINE_TENSOR_PARALLEL_SIZE", "1") != "1":
        raise ValueError("The CPU-expert llama.cpp lane requires tensor parallel size 1")
    mask = getattr(args, "gpu", None)
    if mask is None:
        mask = env.get("CUDA_VISIBLE_DEVICES", "")
    if not isinstance(mask, str) or not mask or "," in mask or not re.fullmatch(r"[A-Za-z0-9_./-]+", mask):
        raise ValueError("The CPU-expert llama.cpp lane requires one explicit GPU selector")
    runtime = Path(root).resolve().parent
    binary = owned_file(env.get("ENGINE_LLAMA_BINARY"), runtime, executable=True)
    model = owned_file(env.get("ENGINE_MODEL_PATH"), runtime)
    if not model.endswith(".gguf"):
        raise ValueError("The llama.cpp model must be an explicit GGUF file")
    alias = env.get("MODEL_NAME", "")
    if not isinstance(alias, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,255}", alias):
        raise ValueError("One explicit model alias is required")
    port = getattr(args, "port", None)
    if type(port) is not int or not 1 <= port <= 65535:
        raise ValueError("The llama.cpp engine requires an explicit valid port")
    context = number(env, "ENGINE_MAX_MODEL_LEN", 18440, 128, 131072)
    parallel = number(env, "ENGINE_MAX_NUM_SEQS", 1, 1, 16)
    if parallel != 1:
        raise ValueError("The validated llama.cpp CPU-expert lane has one slot; context is total across parallel slots")
    batch = number(env, "ENGINE_MAX_NUM_BATCHED_TOKENS", 2048, 1, 8192)
    ubatch = number(env, "ENGINE_LLAMA_UBATCH_SIZE", 512, 1, 8192)
    if ubatch > batch or batch > context:
        raise ValueError("llama.cpp requires ubatch <= batch <= configured context")
    threads = number(env, "ENGINE_LLAMA_THREADS", 16, 1, 256)
    threads_batch = number(env, "ENGINE_LLAMA_THREADS_BATCH", threads, 1, 256)
    gpu_layers = number(env, "ENGINE_LLAMA_N_GPU_LAYERS", 99, 0, 1024)
    cpu_moe = number(env, "ENGINE_LLAMA_N_CPU_MOE", 35, 0, 1024)
    return [binary, "--model", model, "--alias", alias, "--host", "127.0.0.1", "--port", str(port),
            "--device", "CUDA0", "--ctx-size", str(context), "--parallel", str(parallel),
            "--n-gpu-layers", str(gpu_layers), "--n-cpu-moe", str(cpu_moe),
            "--threads", str(threads), "--threads-batch", str(threads_batch),
            "--batch-size", str(batch), "--ubatch-size", str(ubatch),
            "--fit", "off", "--no-context-shift", "--jinja", "--metrics", "--perf"]
