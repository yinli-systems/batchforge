"""Guarded eager/shared-budget and level0 decode-graph experiments on TP4.

Preparation writes private evidence only. No inference or benchmark is sent.
Apply requires a separately prepared, drained, identity-bound operation. Restore
requires the exact SHA256 of a successful activation and keeps it immutable.
"""
from __future__ import annotations

import argparse
import copy
import fcntl
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from types import SimpleNamespace

from backend.scheduling import runtime_fingerprint, validate_profile
from ops import manage
from ops.ha.adaptive_qwen235b import (AdaptiveRoll, BLOCK, CONTEXT, MODEL, REVISION,
    WORKER_SOURCES, admission_budget, fresh_cache, identity, private_operation_directory, sha)
from ops.ha.collective_qwen235b import CollectiveRoll, env_bytes, running_collective
from ops.ha.finalize_api_policy import values
from ops.ha.restore_collective_qwen235b import RestoreCollective, original_services
from ops.ha.set_scheduler_policy import atomic_bytes, timestamp
from ops.supervise import Manager, ROOT, load_manifest, private_read

MAX_SHARED_BUDGET = 298384
CAPTURE_SIZES = [1, 2, 4, 8, 16, 24, 32, 48, 64]
COMPILATION = {"level": 0, "cudagraph_mode": "FULL_DECODE_ONLY", "cudagraph_capture_sizes": CAPTURE_SIZES}
SNAPSHOTS = {"original-manifest.json", "original-engine.env", "original-worker.env", "engine.env", "worker.env"}
VLLM_SOURCES = ("config/compilation.py", "config/__init__.py", "engine/arg_utils.py", "envs.py",
    "v1/worker/gpu_model_runner.py", "v1/cudagraph_dispatcher.py", "compilation/cuda_graph.py",
    "v1/attention/backends/flash_attn.py", "attention/utils/fa_utils.py",
    "distributed/device_communicators/cuda_communicator.py")
LOCAL_SOURCES = ("ops/manage.py", "ops/supervise.py", "ops/engine_members.py", "ops/host_memory.py",
    "ops/ha/adaptive_qwen235b.py", "ops/ha/collective_qwen235b.py", "ops/ha/restore_collective_qwen235b.py",
    "ops/ha/finalize_api_policy.py", "ops/ha/switch_qwen235b.py", "ops/ha/retune_full_gpu.py",
    "ops/ha/set_scheduler_policy.py", "backend/scheduling.py", *WORKER_SOURCES)


def budget(value):
    minimum = (CONTEXT + BLOCK - 1) // BLOCK * BLOCK
    if type(value) is not int or not minimum <= value <= MAX_SHARED_BUDGET or value % BLOCK:
        raise ValueError("Shared budget must hold a full context and be a whole16 block <=298384")
    return value


def execution_environment(original, mode):
    if mode not in {"eager", "decode_graph"}:
        raise ValueError("Only eager control or decode_graph are supported")
    output = dict(original)
    output["ENGINE_ENFORCE_EAGER"] = "1" if mode == "eager" else "0"
    output.pop("ENGINE_COMPILATION_CONFIG", None)
    if mode == "decode_graph":
        output["ENGINE_COMPILATION_CONFIG"] = json.dumps(COMPILATION, sort_keys=True, separators=(",", ":"))
    manage.engine_execution_configuration(output)
    return output


def candidate_profile(original, mode, shared_budget, support):
    result = validate_profile(original)
    descriptor = {"parent_runtime_fingerprint": result["runtime_fingerprint"],
        "engine_execution_configuration": manage.engine_execution_configuration(execution_environment({"ENGINE_MAX_NUM_SEQS": "64"}, mode)),
        "vllm_version": "0.10.2", "worker_budget_tokens": budget(shared_budget), "claim_limit": 64,
        "source_sha256": support["source_sha256"], "flash_attention_capability": support["capability"]}
    result.update(profile_id=f"primary-h100-tp4-qwen235b-fp8-{mode}-b{shared_budget}",
        runtime_fingerprint=runtime_fingerprint(descriptor),
        calibration_label="static16-rates-not-recalibrated;64-claim-slots-not-priced-execution-slots;source:" + result["profile_id"],
        cost_source=result["cost_source"] + ";execution-mode-and-shared-budget-not-recalibrated")
    return validate_profile(result), descriptor


def validate_capability(value):
    if (not isinstance(value, dict) or set(value) != {"vllm_version", "flash_attention_version", "cudagraph_support"}
            or value["vllm_version"] != "0.10.2" or value["flash_attention_version"] not in {2, 3}
            or value["cudagraph_support"] not in {"ALWAYS", "UNIFORM_BATCH", "UNIFORM_SINGLE_TOKEN_DECODE"}):
        raise RuntimeError("Installed vLLM/FlashAttention cannot prove the bounded decode graph capability")
    return value


def source_evidence(root, engine):
    candidates = list((Path(engine["python"]).parent.parent / "lib").glob("python*/site-packages/vllm"))
    if len(candidates) != 1:
        raise RuntimeError("Exactly one installed vLLM source tree is required")
    base = candidates[0]
    hashes = {str(base / name): sha((base / name).read_bytes()) for name in VLLM_SOURCES}
    hashes.update({str(root / name): sha((root / name).read_bytes()) for name in LOCAL_SOURCES})
    hashes[str(Path(__file__).resolve())] = sha(Path(__file__).read_bytes())
    # No model loads or requests: inspect the installed backend's actual FA
    # version/support enum rather than guessing it from a generic startup line.
    code = ("import json,vllm;from vllm.attention.utils.fa_utils import get_flash_attn_version;"
            "from vllm.v1.attention.backends.flash_attn import FlashAttentionMetadataBuilder as B;"
            "print('BATCHFORGE_CAPABILITY='+json.dumps({'vllm_version':vllm.__version__,"
            "'flash_attention_version':get_flash_attn_version(),'cudagraph_support':B.cudagraph_support.name}))")
    environment = manage.load_env(Path(engine["env_file"]))
    environment["CUDA_VISIBLE_DEVICES"] = engine["gpu"]
    probe = subprocess.run([engine["python"], "-c", code], env=environment, stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=60, check=False)
    if probe.returncode or len(probe.stdout) > 65536:
        raise RuntimeError("Installed attention capability probe failed; diagnostics withheld")
    lines = [line.split("=", 1)[1] for line in probe.stdout.decode().splitlines() if line.startswith("BATCHFORGE_CAPABILITY=")]
    if len(lines) != 1:
        raise RuntimeError("Installed capability probe result is ambiguous")
    capability = validate_capability(json.loads(lines[0]))
    source = (base / "v1/worker/gpu_model_runner.py").read_text()
    if not all(fragment in source for fragment in ("Graph capturing finished", "uniform_decode=True", "cudagraph_mode.decode_mode()")):
        raise RuntimeError("Installed graph capture implementation differs from the reviewed v0.10.2 path")
    return {"source_sha256": hashes, "capability": capability,
        "reference_sources": ["https://github.com/vllm-project/vllm/blob/v0.10.2/vllm/v1/worker/gpu_model_runner.py",
            "https://github.com/vllm-project/vllm/blob/v0.10.2/vllm/v1/attention/backends/flash_attn.py",
            "https://github.com/vllm-project/vllm/blob/v0.10.2/vllm/v1/cudagraph_dispatcher.py",
            "https://github.com/vllm-project/vllm/blob/v0.10.2/vllm/compilation/cuda_graph.py"]}


def capture_evidence(raw, mode):
    text = raw.decode(errors="replace")
    cache = fresh_cache(text)
    bad = ("setting cudagraph_mode=NONE", "setting cudagraph_mode=PIECEWISE", "CUDA out of memory",
           "CUDAGraphMode.FULL_DECODE_ONLY is not supported", "Traceback (most recent call last)")
    if any(fragment in text for fragment in bad):
        raise RuntimeError("Fresh engine startup contains a failure or graph downgrade")
    if mode == "decode_graph" and any(value != "FULL_DECODE_ONLY" for value in re.findall(r"setting cudagraph_mode=([A-Z_]+)", text)):
        raise RuntimeError("Fresh engine startup changed the requested graph mode")
    if mode == "decode_graph" and "Skipping CUDA graph capture" in text:
        raise RuntimeError("At least one fresh startup path skipped graph capture")
    captures = re.findall(r"Graph capturing finished in ([0-9.]+) secs, took ([0-9.]+) GiB", text)
    if mode == "decode_graph":
        if not captures or "FULL_DECODE_ONLY" not in text:
            raise RuntimeError("Fresh startup does not prove resolved decode-only mode and successful capture")
        config_lines = "\n".join(line for line in text.splitlines() if "compilation_config" in line)
        levels = re.findall(r"\blevel['\"]?\s*[:=]\s*([0-9]+)", config_lines)
        capture_lists = re.findall(r"\bcudagraph_capture_sizes['\"]?\s*[:=]\s*\[([0-9, ]+)\]", config_lines)
        if (not levels or any(int(value) != 0 for value in levels) or not capture_lists
                or any(sorted(int(value.strip()) for value in found.split(",")) != CAPTURE_SIZES for found in capture_lists)):
            raise RuntimeError("Fresh startup does not prove level0 and the exact bounded capture set")
        if "Capturing CUDA graphs (mixed prefill-decode" in text:
            raise RuntimeError("Mixed prefill-decode capture would violate the experiment")
    elif captures:
        raise RuntimeError("Eager control unexpectedly captured CUDA graphs")
    return {"measured_cache_tokens": cache, "fresh_log_sha256": sha(raw), "fresh_log_bytes": len(raw),
        "capture_completion_lines": len(captures), "capture_seconds_reported": [float(item[0]) for item in captures],
        "capture_gib_reported": [float(item[1]) for item in captures],
        "resolved_graph_configuration_verified": mode == "decode_graph",
        "capture_rank_coverage": "startup logging may deduplicate ranks; no per-rank replay claim", "mode": mode}


def running_execution(root, engine, expected):
    collective = running_collective(root, engine, False)
    record = manage.owned_record(root / ".runtime/engine-qwen235b.json")
    arguments = SimpleNamespace(role="engine", python=engine["python"], port=engine["port"], gpu=engine["gpu"])
    wanted = manage.command(arguments, expected)
    actual = [item.decode() for item in Path(f"/proc/{record['pid']}/cmdline").read_bytes().rstrip(b"\0").split(b"\0")]
    if actual != record["command"] or actual != wanted:
        raise RuntimeError("Actual owned argv differs from the exact reviewed execution configuration")
    effective = dict(item.split(b"=", 1) for item in Path(f"/proc/{record['pid']}/environ").read_bytes().split(b"\0") if b"=" in item)
    if any(effective.get(key.encode()) != value.encode() for key, value in expected.items()):
        raise RuntimeError("Actual owned environment differs from the reviewed engine file")
    if identity(root, engine) != {key: record[key] for key in ("pid", "start_ticks", "cwd")}:
        raise RuntimeError("Engine identity changed while inspecting execution")
    return {"actual_argv_and_environment_verified": True,
            "execution_configuration": manage.engine_execution_configuration(expected), "collective": collective}


class DecodeGraphRoll(CollectiveRoll):
    def __init__(self, *, root, run, manifest_path, watchdog, support, mode=None, shared_token_budget=None,
                 services=None, previous=None, activation_run=None, activation_sha256=None,
                 manager=None, identity_reader=None, assert_drained=None, engine_observer=None,
                 worker_observer=None, path_observer=None, clock=time.monotonic, sleep=time.sleep):
        self.root, self.output_run, self.manifest_path, self.watchdog = root, run, manifest_path, watchdog
        self.restoring = activation_run is not None
        self.activation_path = activation_run / "report.json" if self.restoring else None
        activation = None
        if self.restoring:
            if not re.fullmatch("[0-9a-f]{64}", activation_sha256 or ""):
                raise ValueError("Restore requires the exact completed activation SHA256")
            raw = private_read(self.activation_path)
            if sha(raw) != activation_sha256:
                raise RuntimeError("Completed activation SHA256 differs")
            activation = json.loads(raw)
            if (activation.get("phase") != "complete" or activation.get("watchdog_restored") is not True
                    or activation.get("operation") != "qwen235b-shared-budget-execution-experiment"):
                raise RuntimeError("Only a completed supervised decode/eager activation can be restored")
            mode, shared_token_budget = activation["mode"], activation["worker_budget_tokens"]
            self.run = activation_run
            self.original = private_read(activation_run / "original-manifest.json")
            services = original_services(root, manifest_path, self.original)
        else:
            self.run = run
            self.original = private_read(manifest_path)
        self.mode, self.budget = mode, budget(shared_token_budget)
        self.services, self.document, self.support = services, json.loads(self.original), support
        validate_capability(support["capability"])
        engines, workers = ([service for service in services if service["role"] == role] for role in ("engine", "worker"))
        if len(engines) != 1 or len(workers) != 1:
            raise ValueError("Exactly one owned engine and worker are required")
        self.engine, self.worker = engines[0], workers[0]
        self.protected = [service for service in services if service["role"] not in {"engine", "worker"}]
        if any(service["role"] not in {"api", "gateway"} for service in self.protected):
            raise ValueError("Unexpected protected service")
        self.env_bytes = {Path(service["env_file"]): private_read(Path(service["env_file"])) for service in [*services, watchdog]}
        if Path(values(self.env_bytes[Path(watchdog["env_file"])]).get("SUPERVISOR_MANIFEST", "")).resolve() != manifest_path.resolve():
            raise ValueError("Watchdog must follow the exact bound manifest")
        self.engine_env, self.worker_env = (values(self.env_bytes[Path(service["env_file"])]) for service in (self.engine, self.worker))
        self._validate_current()
        self.profile, self.runtime_descriptor = candidate_profile(self.original_profile, mode, self.budget, support)
        self.candidate_engine = {**self.engine, "env_file": str(self.run / "engine.env")}
        self.candidate_worker = {**self.worker, "env_file": str(self.run / "worker.env")}
        self.candidate_engine_bytes = env_bytes(execution_environment(self.engine_env, mode))
        self.candidate_worker_bytes = env_bytes({**self.worker_env, "WORKER_TOKEN_BUDGET": str(self.budget),
            "SCHEDULER_PROFILE_JSON": json.dumps(self.profile, separators=(",", ":"))})
        self.candidate = copy.deepcopy(self.document)
        for service in self.candidate["services"]:
            if self.key(service) == self.key(self.engine): service["env_file"] = self.candidate_engine["env_file"]
            if self.key(service) == self.key(self.worker): service["env_file"] = self.candidate_worker["env_file"]
        candidate_raw = json.dumps(self.candidate, separators=(",", ":")).encode()
        self.engine_only, self.original_engine_only = copy.deepcopy(self.candidate), copy.deepcopy(self.document)
        for document in (self.engine_only, self.original_engine_only):
            document["services"] = [service for service in document["services"] if service["role"] != "worker"]
        self.allowed_manifest_bytes = {self.original, candidate_raw} if self.restoring else {self.original}
        self.manager = manager or Manager(root=root, timeout=300)
        self.identity = identity_reader or (lambda service: identity(root, service))
        self.assert_drained = assert_drained or self._drained
        self.engine_observer = engine_observer or self._observe_engine
        self.profile_observer = worker_observer or self._observe_profile
        self.path_observer = path_observer or (lambda candidate: running_execution(root, self.engine,
            values(self.candidate_engine_bytes) if candidate else self.engine_env))
        self.clock, self.sleep = clock, sleep
        self.before = {self.key(service): self.identity(service) for service in [*services, watchdog]}
        if not all(self.before.values()):
            raise RuntimeError("All guarded owned services must be live")
        self.expected = copy.deepcopy(self.before)
        snapshot = {"original-manifest.json": self.original, "original-engine.env": self.env_bytes[Path(self.engine["env_file"])],
            "original-worker.env": self.env_bytes[Path(self.worker["env_file"])], "engine.env": self.candidate_engine_bytes,
            "worker.env": self.candidate_worker_bytes}
        binding = {"manifest_sha256": sha(self.original), "environment_sha256": {str(path): sha(raw) for path, raw in self.env_bytes.items()},
            "owned_identities": self.before, "support": support, "worker_budget_tokens": self.budget, "mode": mode,
            "snapshot_sha256": {name: sha(raw) for name, raw in snapshot.items()}, "controller_sha256": sha(Path(__file__).read_bytes())}
        if self.restoring:
            baseline = activation["prepared_binding"]
            binding["owned_identities"] = baseline["owned_identities"]
            if binding != baseline or private_read(manifest_path) != candidate_raw:
                raise RuntimeError("Activation source/configuration or live candidate manifest changed")
            expected = copy.deepcopy(baseline["owned_identities"])
            for service, field in ((self.engine, "engine_identity_after"), (self.worker, "worker_identity_after"), (watchdog, "watchdog_identity_after")):
                expected[self.key(service)] = activation[field]
            if self.before != expected:
                raise RuntimeError("Completed candidate PID/start/cwd identities changed")
        if self.restoring or previous is not None:
            if any(private_read(self.run / name) != raw for name, raw in snapshot.items()):
                raise RuntimeError("Bound candidate/rollback snapshots changed")
        restore_binding = {"activation_sha256": activation_sha256, "owned_candidate_identities": self.before,
                           "controller_sha256": sha(Path(__file__).read_bytes())} if self.restoring else None
        if previous is not None:
            if (previous.get("phase") != "prepared" or previous.get("prepared_binding") != binding
                    or previous.get("restore_binding") != restore_binding):
                raise RuntimeError("Prepared execution operation changed")
            self.report = previous
        else:
            if any(run.iterdir()):
                raise RuntimeError("Operation directory must be empty before first preparation")
            if not self.restoring:
                for name, raw in snapshot.items(): atomic_bytes(run / name, raw)
            self.report = {"phase": "prepared", "prepared_at": timestamp(), "operation":
                "restore-qwen235b-execution-experiment" if self.restoring else "qwen235b-shared-budget-execution-experiment",
                "prepared_binding": binding, "restore_binding": restore_binding, "mode": mode,
                "model": MODEL, "revision": REVISION, "original_profile": self.original_profile, "profile": self.profile,
                "runtime_descriptor": self.runtime_descriptor, "worker_budget_tokens": self.budget,
                "original_worker_budget_tokens": self.original_budget, "events": [], "restored": False,
                "limitations": ["No benchmark, model quality result or per-rank graph replay is established by startup",
                    "Rates/price retain the original static16 calibration;64 claimed tasks are not64 priced execution slots",
                    "Shared budget never shrinks automatically; failed capacity/capture gates require exact rollback",
                    "Ready capture is startup evidence; timed inference and untimed replay traces are separate operations"]}
            self.save()
        self.guard(initial=True)
        self.assert_drained()

    def save(self):
        atomic_bytes(self.output_run / "report.json", json.dumps(self.report, separators=(",", ":"), allow_nan=False).encode() + b"\n")

    def _validate_current(self):
        # Reuse the baseline invariants except its specific calibration ID.
        profile = validate_profile(json.loads(self.worker_env["SCHEDULER_PROFILE_JSON"]))
        if (profile["profile_id"] != "primary-h100-tp4-qwen235b-fp8-adaptive64"
                and not profile["profile_id"].startswith("primary-h100-tp4-qwen235b-fp8-eager-b")):
            raise ValueError("Requires original adaptive64 or an explicit eager shared-budget control")
        temporary = {**profile, "profile_id": "primary-h100-tp4-qwen235b-fp8-adaptive64"}
        original_worker = self.worker_env
        self.worker_env = {**original_worker, "SCHEDULER_PROFILE_JSON": json.dumps(temporary)}
        target = self.budget
        try:
            CollectiveRoll._validate_current(self)
            self.original_budget = self.budget
        finally:
            self.worker_env, self.budget = original_worker, target
        self.original_profile = profile
        if self.original_budget > MAX_SHARED_BUDGET or manage.engine_execution_configuration(self.engine_env) != {"enforce_eager": True}:
            raise ValueError("Original baseline must remain eager with a bounded exact-token budget")
        if self.engine_env.get("VLLM_ATTENTION_BACKEND") not in {None, "FLASH_ATTN"}:
            raise ValueError("No attention backend switch is permitted")

    def guard(self, *, initial=False):
        AdaptiveRoll.guard(self, initial=initial)
        binding = self.report["prepared_binding"]
        if any(sha(Path(path).read_bytes()) != checksum for path, checksum in self.support["source_sha256"].items()):
            raise RuntimeError("Reviewed execution source changed")
        if sha(Path(__file__).read_bytes()) != binding["controller_sha256"]:
            raise RuntimeError("Execution controller source changed")
        if set(binding["snapshot_sha256"]) != SNAPSHOTS or any(
                sha(private_read(self.run / name)) != checksum for name, checksum in binding["snapshot_sha256"].items()):
            raise RuntimeError("Private candidate/rollback snapshot changed")
        if self.restoring and sha(private_read(self.activation_path)) != self.report["restore_binding"]["activation_sha256"]:
            raise RuntimeError("Completed activation evidence changed")

    def _observe_profile(self, profile):
        # The parent probe checks exact gauges, token budget and DB registration.
        current = self.budget
        self.budget = self.original_budget if profile == self.original_profile else current
        try:
            return CollectiveRoll._observe_profile(self, profile)
        finally:
            self.budget = current

    def start_engine(self, service, candidate):
        path = self.root / ".runtime/engine-qwen235b.log"
        before = path.stat() if path.exists() else None
        offset = before.st_size if before else 0
        self.gate(service)
        self.invoke(service, "start")
        self.ready(service, 1200)
        after = path.stat()
        if after.st_size < offset or before and (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
            raise RuntimeError("Fresh engine log continuity changed")
        with path.open("rb") as handle:
            handle.seek(offset)
            raw = handle.read(16 * 1024**2 + 1)
        if len(raw) > 16 * 1024**2:
            raise RuntimeError("Fresh engine log exceeds bound")
        mode = self.mode if candidate else "eager"
        # Keep exact raw log bytes privately, not in reports or Git. No prompt
        # logging is enabled by this controller.
        evidence_path = self.output_run / f"fresh-engine-{len(self.report['events']):04d}.log"
        if evidence_path.exists():
            raise RuntimeError("Fresh engine evidence path already exists")
        atomic_bytes(evidence_path, raw)
        self.report.setdefault("engine_startup_logs", []).append({"path": str(evidence_path), "sha256": sha(raw),
            "bytes": len(raw), "mode": mode, "log_offset": offset, "owned_identity": self.identity(service)})
        self.save()
        evidence = capture_evidence(raw, mode)
        expected_budget = self.budget if candidate else self.original_budget
        if admission_budget(evidence["measured_cache_tokens"], expected_budget) != expected_budget:
            raise RuntimeError("Fresh95%cache cannot hold the exact requested shared budget; no automatic shrinking")
        return {**evidence, "preserved_budget_tokens": expected_budget,
                "execution": self.path_observer(candidate), "protocol": self.engine_observer()}

    def apply(self):
        if self.restoring:
            # The existing exact restoration state machine uses this class's
            # execution/cache/profile guards and private report destination.
            return RestoreCollective.apply(self)
        return CollectiveRoll.apply(self)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--operation", required=True)
    parser.add_argument("--mode", choices=("eager", "decode_graph"))
    parser.add_argument("--shared-token-budget", type=int)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--restore", action="store_true")
    parser.add_argument("--activation-operation")
    parser.add_argument("--activation-sha256")
    args = parser.parse_args()
    if (args.restore and (not args.activation_operation or not args.activation_sha256 or args.mode is not None or args.shared_token_budget is not None)
            or not args.restore and (args.mode is None or args.shared_token_budget is None or args.activation_operation or args.activation_sha256)):
        parser.error("Activation requires mode+shared budget; restore instead requires exact activation operation+SHA256")
    os.umask(0o077)
    run = private_operation_directory(ROOT, args.operation)
    activation = None
    if args.restore:
        if not re.fullmatch("[a-zA-Z0-9_-]{1,80}", args.activation_operation):
            raise ValueError("Invalid activation operation")
        activation = ROOT / ".runtime" / args.activation_operation
        if not activation.is_dir() or activation.is_symlink() or activation == run:
            raise ValueError("Restore requires a separate real completed activation directory")
    descriptor = os.open(ROOT / ".runtime/qwen235b-maintenance.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        previous = json.loads(private_read(run / "report.json")) if (run / "report.json").exists() else None
        if args.apply and previous is None:
            raise RuntimeError("Separate immutable preparation is required before apply")
        manifest_path = ROOT / ".runtime/services-primary.json"
        _, services, _ = load_manifest(manifest_path)
        engine = next(service for service in services if service["role"] == "engine")
        watchdog = {"role": "supervisor", "name": "primary", "env_file": str(ROOT.parent / "supervisor.env"),
                    "python": str(ROOT.parent / "venv/bin/python")}
        roll = DecodeGraphRoll(root=ROOT, run=run, manifest_path=manifest_path, services=services, watchdog=watchdog,
            support=source_evidence(ROOT, engine), mode=args.mode, shared_token_budget=args.shared_token_budget,
            previous=previous, activation_run=activation, activation_sha256=args.activation_sha256)
        if args.apply:
            roll.apply()
    print(json.dumps({"phase": roll.report["phase"], "restoring": args.restore, "report": str(run / "report.json")}))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        # Driver/HTTP exception text can include credentials or private URLs.
        print(json.dumps({"operation_failed": type(error).__name__, "sqlstate": getattr(error, "sqlstate", None)}), file=sys.stderr)
        raise SystemExit(1) from None
