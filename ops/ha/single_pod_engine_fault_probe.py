"""Bounded single-Pod owned engine/worker loss and reload; dry-run by default.

Use an explicitly submitted 32..100-task real benchmark with --batches 1. This is application
serving-capacity loss, not a GPU reset, peer failover, or exactly-once inference.
Old lease tokens, credentials, response bodies and process commands stay in RAM.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import signal
import stat
import sys
import threading
import time

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ops import engine_members, manage
from ops.ha.controlplane_outage_probe import ProbeFailure, require, timestamp
from ops.ha.engine_outage_probe import EngineProbe, selected_pair
from ops.ha.single_pod_fault_probe import SinglePodProbe, WorkerController, WorkerProgress
from ops.supervise import load_manifest, private_read, private_write
from scripts.observe_runtime import Sampler

MODEL = "Qwen/Qwen3-235B-A22B-Instruct-2507-FP8"
DEPENDENCIES = ("ops/ha/single_pod_engine_fault_probe.py", "ops/ha/single_pod_fault_probe.py",
    "ops/ha/engine_outage_probe.py", "ops/ha/controlplane_outage_probe.py", "ops/manage.py",
    "ops/supervise.py", "ops/engine_members.py", "ops/host_memory.py", "scripts/observe_runtime.py")


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def durable_write(path, value):
    private_write(path, value)
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def reserve(path):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "wb") as file:
        file.write(b'{"phase":"reserved","automatic_replay":false}\n')
        file.flush(); os.fsync(file.fileno())
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def source_hashes(root):
    paths = {root / name for name in DEPENDENCIES}
    for directory in ("backend", "worker", "ops", "scripts"):
        paths.update(path for path in (root / directory).rglob("*")
                     if path.is_file() and path.suffix in {".py", ".sql"})
    return {str(path.relative_to(root)): sha(path.read_bytes()) for path in sorted(paths)}


class EngineFaultProgress(WorkerProgress):
    def scope(self, expected):
        super().scope(expected)
        row = self.connection.execute("""SELECT count(*) AS total,count(DISTINCT b.id) AS batches,
            count(*) FILTER(WHERE t.model<>%s OR b.model<>%s) AS wrong_model
            FROM tasks t JOIN batches b ON b.id=t.batch_id WHERE b.metadata->>'benchmark_run'=%s""",
            (self.model, self.model, self.run_id)).fetchone()
        require(row["total"] == expected and row["wrong_model"] == 0, "run_task_count_or_model_scope_mismatch")
        require(row["batches"] == 1, "exact_run_requires_one_benchmark_batch_with_live_payloads")

    def other_unfinished_work(self):
        # No unselected model/cohort may be affected by this serving loss.
        return self.connection.execute("""SELECT count(*) AS n FROM tasks t JOIN batches b ON b.id=t.batch_id
            WHERE t.status IN ('pending','running') AND b.metadata->>'benchmark_run' IS DISTINCT FROM %s""",
            (self.run_id,)).fetchone()["n"]

    def final_proof(self, expected):
        row = self.connection.execute("""SELECT count(*) AS total,
            count(*) FILTER(WHERE t.status<>'completed' OR t.model<>%s OR b.model<>%s OR t.response IS NULL OR t.response->>'batchforge_test_only'='true') AS invalid,
            count(*) FILTER(WHERE (SELECT count(*) FROM attempts a WHERE a.task_id=t.id AND a.status='completed')<>1) AS nonunique,
            count(*) FILTER(WHERE t.prompt_tokens<>(SELECT coalesce(sum(a.prompt_tokens),0) FROM attempts a WHERE a.task_id=t.id AND a.status='completed')
              OR t.completion_tokens<>(SELECT coalesce(sum(a.completion_tokens),0) FROM attempts a WHERE a.task_id=t.id AND a.status='completed')) AS usage_mismatch
            FROM tasks t JOIN batches b ON b.id=t.batch_id WHERE b.metadata->>'benchmark_run'=%s""", (self.model, self.model, self.run_id)).fetchone()
        require(row["total"] == expected and not any(row[key] for key in ("invalid", "nonunique", "usage_mismatch")),
                "final_real_workload_acceptance_or_usage_proof_failed")
        return {"tasks": int(row["total"]), "all_completed": True, "accepted_once": True,
                "durable_usage_matches": True, "simulated_results": False}


class EngineFaultController(WorkerController):
    def capture_members(self, engine, expected):
        require(self.record(engine) == expected and expected.get("track_members") is True,
                "tp4_owned_engine_membership_required")
        path = self.root / ".runtime" / ("engine-" + engine["name"] + ".members.json")
        require(engine_members.capture(path, expected), "engine_membership_capture_failed")
        members = engine_members.read(path)
        require(self.record(engine) == expected and members.get("leader") == engine_members.leader_key(expected),
                "engine_membership_identity_changed")
        return {"observed_members": len(members["members"]), "snapshot_sha256": sha(private_read(path))}

    def gpu_memory(self):
        # Existing bounded two-second NVML query; no model/process enumeration.
        value = Sampler.gpu()
        rows = value.get("devices", [])
        valid = (len(rows) == 4 and {row.get("index") for row in rows} == {"0", "1", "2", "3"}
            and all(type(row.get(key)) in (int, float) and math.isfinite(row[key])
                for row in rows for key in ("memory_used_mib", "memory_total_mib"))
            and all(0 <= row["memory_used_mib"] <= row["memory_total_mib"] and row["memory_total_mib"] > 0 for row in rows))
        if not valid:
            return {"observed_at": timestamp(), "available": False, "reason": "exact_four_gpu_metadata_unavailable"}
        return {"observed_at": value["observed_at"], "available": True, "devices": rows,
                "device_wide_metadata_not_exclusive_job_attribution": True}


class SinglePodEngineProbe(SinglePodProbe):
    stop_released = EngineProbe.stop_released

    def __init__(self, *, journal, engine_ready_timeout=900, engine_quiescence_timeout=600, **kwargs):
        super().__init__(capacity=0, **kwargs)
        self.journal = journal
        self.engine_ready_timeout, self.engine_quiescence_timeout = engine_ready_timeout, engine_quiescence_timeout
        self.protected = [row for row in self.services if row not in (self.worker, self.engine)]
        self.configs = {row["env_file"]: sha(private_read(Path(row["env_file"])))
                        for row in [*self.services, self.supervisor]}
        self.sources = source_hashes(self.controller.root)
        self.expected_engine = self.expected_worker = None
        self.down_clock = None
        self.reload_clock = None
        self.report.update(scope="Single-Pod application engine/worker serving-capacity loss and model reload",
            measurement_bounds=["Not a hardware GPU reset, whole-Pod failure, peer HA or cross-node handover",
                "Qwen235B is a mixture-of-experts model with about22B active parameters, not dense235B",
                "Inference may execute multiple times; exactly one durable accepted result and usage is checked",
                "Only the explicitly owned worker is SIGKILLed; its engine is stopped by the existing managed stop",
                "API/gateway continuity and GPU memory are sampled, not a continuous hardware-health proof",
                "No new workload, cancellation, database mutation or configuration change is issued by this probe",
                "Exactly one benchmark batch is required so archive grace cannot purge earlier completed batches during reload",
                "The only business API writes are stale heartbeat/complete requests required to return409"],
            manifest_sha256=sha(self.original), configuration_sha256=self.configs, source_sha256=self.sources,
            restore_attempts=[], automatic_replay=False, engine_quiescence_gates=[])
        self.report["required_benchmark_batches"] = 1

    def save(self, phase=None):
        if phase:
            self.report["phase"] = phase
        durable_write(self.journal, self.report)

    def guard(self):
        require(private_read(self.manifest) == self.original, "active_manifest_changed")
        require(all(sha(private_read(Path(path))) == value for path, value in self.configs.items()),
                "protected_configuration_changed")
        require(source_hashes(self.controller.root) == self.sources, "protected_source_changed")

    def preserve(self):
        self.guard()
        super().preserve()

    def _start_owned(self, service, attribute):
        current = self.controller.record(service)
        expected = getattr(self, attribute)
        require(current is None or current == expected, "unexpected_pair_identity_during_restore")
        if current is None:
            self.save("starting_" + service["role"])
            value = self.controller.invoke(service, "start")
            current = self.controller.record(service)
            require(current is not None and value.get("status") == "started" and value.get("pid") == current["pid"],
                    "start_acknowledgement_or_identity_ambiguous")
            setattr(self, attribute, current)
        return current

    def _ready(self, service, identity, timeout):
        def ready():
            self.preserve()
            require(self.controller.record(service) == identity, "pair_identity_changed_during_readiness")
            return self.controller.responding(service, ready=True)
        self.wait(ready, timeout, "restored_" + service["role"] + "_not_ready", interruptible=False)

    def restore(self, *, start_watchdog=True):
        errors = []
        try:
            self.preserve()
            if self.controller.record(self.engine) is None:
                def gated():
                    self.preserve()
                    allowed, reason = self.controller.start_gate(self.engine)
                    self.report.setdefault("engine_restart_gates", []).append({"at": timestamp(), "allowed": allowed, "reason": reason})
                    self.save("engine_memory_member_gate")
                    return allowed
                self.wait(gated, 120, "engine_restart_memory_or_member_gate_not_ready", interruptible=False)
                if self.reload_clock is None:
                    self.reload_clock = self.clock()
                    self.report["engine_load_requested_at"] = timestamp()
            engine = self._start_owned(self.engine, "expected_engine")
            self._ready(self.engine, engine, self.engine_ready_timeout)
            if "engine_ready_after_loss_at" not in self.report:
                self.report.update(engine_ready_after_loss_at=timestamp(), engine_reload_seconds=self.clock()-self.reload_clock if self.reload_clock is not None else None,
                    observed_engine_unavailable_seconds=self.clock()-self.down_clock if self.down_clock is not None else None)
            def quiet():
                self.preserve()
                require(self.controller.record(self.engine) == engine, "engine_identity_changed_before_worker_restart")
                value = self.controller.engine_quiescence(self.engine)
                self.report["engine_quiescence_gates"].append({"at": timestamp(), **value})
                self.save("engine_quiescence_gate")
                return value.get("quiescent") is True
            # Never overlap replacement worker admissions with orphaned inference.
            if self.controller.record(self.worker) is None:
                self.wait(quiet, self.engine_quiescence_timeout, "engine_not_quiescent_before_worker_restart", interruptible=False)
            worker = self._start_owned(self.worker, "expected_worker")
            self._ready(self.worker, worker, 120)
            self.report.update(restored_engine_pid=engine["pid"], restored_worker_pid=worker["pid"])
            self.preserve()
        except Exception as error:
            errors.append(str(error) if isinstance(error, ProbeFailure) else "pair_restore_failed")
        if not errors and start_watchdog:
            try:
                current = self.controller.record(self.supervisor)
                require(current is None or current == self.expected_supervisor, "unexpected_watchdog_identity")
                if current is None:
                    value = self.controller.invoke(self.supervisor, "start")
                    current = self.controller.record(self.supervisor)
                    require(current is not None and value.get("pid") == current["pid"], "watchdog_start_identity_ambiguous")
                    self.expected_supervisor = current
                require(current is not None, "watchdog_not_running")
            except Exception:
                errors.append("watchdog_restore_failed")
        self.report["restore_attempts"].append({"at": timestamp(), "errors": errors})
        self.report["manual_recovery_required"] = bool(errors)
        if errors:
            self.report["manual_recovery_scope"] = "Inspect owned engine/worker readiness, exact identities, member/memory/quiescence gates before restoring watchdog; do not replay fault"
        self.report["watchdog_restored"] = not errors and start_watchdog
        self.save("restore_failed" if errors else "pair_and_watchdog_restored" if start_watchdog else "pair_restored_watchdog_paused")
        require(not errors, "restore_incomplete")

    def run(self):
        maintenance, restored = False, False
        try:
            self.before = {row["role"] + "-" + row["name"]: self.controller.record(row) for row in self.services}
            self.expected_supervisor = self.controller.record(self.supervisor)
            require(all(self.before.values()) and self.expected_supervisor, "owned_service_missing")
            self.expected_worker = self.before["worker-" + self.worker["name"]]
            self.expected_engine = self.before["engine-" + self.engine["name"]]
            self.preserve()
            require(all(self.controller.responding(row, ready=True) for row in (self.worker, self.engine)), "initial_pair_not_ready")
            self.progress.scope(self.expected)
            require(self.progress.other_unfinished_work() == 0, "another_cohort_or_model_has_unfinished_work")
            require(self.progress.other_fresh_workers(self.progress.secondary_worker_id) == 0, "another_fresh_worker_serves_this_model")
            before = self.sample("before")
            require(before["failed"] == before["cancelled"] == 0, "initial_terminal_failure")
            captured = self.progress.capture(self.progress.secondary_worker_id)
            require(before["running"] >= self.minimum and len(captured) >= self.minimum, "insufficient_actual_running_leases")
            self.report["owned_engine_membership"] = self.controller.capture_members(self.engine, self.expected_engine)
            self.report["gpu_memory_before"] = self.controller.gpu_memory()
            self.report["protected_process_pids"] = {key: value["pid"] for key, value in self.before.items()
                if key not in {"worker-" + self.worker["name"], "engine-" + self.engine["name"]}}
            require(not self.stop.is_set(), "interrupted_before_fault")
            self.report["manual_recovery_required"] = True
            self.save("fault_intent_durable")
            maintenance = True
            require(self.controller.record(self.supervisor) == self.expected_supervisor, "watchdog_identity_changed")
            self.stop_released(self.supervisor, self.expected_supervisor)
            require(self.controller.record(self.supervisor) is None, "watchdog_did_not_stop")
            self.controller.crash_exact(self.worker, self.expected_worker)
            self.report["worker_sigkill_sent_at"] = timestamp()
            self.wait(lambda: self.controller.record(self.worker) is None and not self.controller.responding(self.worker),
                      15, "worker_still_responds_after_sigkill")
            self.save("worker_crashed_engine_stop_intent")
            self.report["engine_stop_requested_at"] = timestamp()
            self.stop_released(self.engine, self.expected_engine)
            self.wait(lambda: self.controller.record(self.engine) is None and not self.controller.responding(self.engine),
                      30, "engine_still_responds_after_managed_stop")
            self.down_clock = self.clock()
            held = self.progress.held_after_loss(captured)
            require(held, "no_captured_valid_lease_after_pair_loss")
            self.report.update(pair_unavailable_at=timestamp(), captured_valid_leases=len(captured),
                held_valid_leases_after_loss=len(held),
                captured_at_db_clock=min(row["captured_at"] for row in held).isoformat(),
                lease_expiry_bounds=[min(row["lease_expires_at"] for row in held).isoformat(), max(row["lease_expires_at"] for row in held).isoformat()],
                gpu_memory_pair_down=self.controller.gpu_memory())
            self.save("pair_down_awaiting_lease_expiry")
            def expired():
                self.preserve()
                require(all(self.controller.record(row) is None and not self.controller.responding(row)
                            for row in (self.worker, self.engine)), "pair_restarted_before_expiry_proof")
                value = self.sample("pair_down")
                require(value["failed"] == value["cancelled"] == 0, "terminal_failure_during_pair_loss")
                rows, statuses = self.progress.expired(held)
                self.report["captured_attempt_status_counts"] = statuses
                return len(rows) == len(held) and self.progress.recovered_pending(held) == len(held)
            self.wait(expired, self.expiry_timeout, "captured_leases_not_retryable_pending")
            self.report.update(all_captured_retryable_pending_at=timestamp(), observed_lease_recovery_seconds=self.clock()-self.down_clock)
            self.save("all_captured_retryable_pending")
            self.restore(start_watchdog=False)
            def terminal():
                self.preserve()
                require(self.controller.record(self.engine) == self.expected_engine and self.controller.record(self.worker) == self.expected_worker,
                        "restored_pair_identity_changed")
                require(self.controller.responding(self.engine, ready=True) and self.controller.responding(self.worker, ready=True), "restored_pair_not_ready")
                value = self.sample("after_reload")
                require(value["failed"] == value["cancelled"] == 0, "workload_has_terminal_failure")
                return value["completed"] == self.expected and value["pending"] == value["running"] == 0
            self.wait(terminal, self.terminal_timeout, "real_workload_not_terminal_after_engine_reload")
            recovery = self.progress.recovery(held)
            require(recovery["reclaimed_by_exact_secondary"] == len(held), "captured_tasks_not_reaccepted_once_by_original_worker")
            candidate = held[0]
            state = self.progress.accepted_state(candidate["id"])
            self.report["stale_attempt_http_status"] = self.controller.fence_both(self.api, self.token, candidate)
            require(self.progress.accepted_state(candidate["id"]) == state, "stale_request_changed_accepted_state")
            self.report.update(final_acceptance=self.progress.final_proof(self.expected),
                captured_tasks_reaccepted_once=recovery["reclaimed_by_exact_secondary"], accepted_state_unchanged=True,
                gpu_memory_after=self.controller.gpu_memory(), status="passed", finished_at=timestamp())
            self.restore(); restored = True
            self.save("complete")
        except Exception as error:
            self.report.update(status="failed", error=str(error) if isinstance(error, ProbeFailure) else type(error).__name__, finished_at=timestamp())
        finally:
            if maintenance and not restored:
                try:
                    self.restore()
                except Exception:
                    self.report["manual_recovery_required"] = True
                    self.report["final_restore_failed"] = True
            self.save()
        return self.report


def main(argv=None):
    class SafeParser(argparse.ArgumentParser):
        def error(self, _message):
            print(json.dumps({"status": "refused", "error": "invalid_arguments"})); raise SystemExit(2)
    parser = SafeParser(description=__doc__)
    for name in ("manifest", "env-file", "supervisor-env", "report"):
        parser.add_argument("--" + name, type=Path, required=True)
    for name in ("worker-name", "engine-name", "run-id"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--model", default=MODEL)
    parser.add_argument("--supervisor-name", default="primary")
    for name, default in (("expected-tasks", None), ("minimum-running", 8), ("wait-for-submission", 300),
            ("expiry-timeout", 240), ("engine-ready-timeout", 900), ("engine-quiescence-timeout", 600), ("terminal-timeout", 1800)):
        parser.add_argument("--" + name, type=int, default=default, required=default is None)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    reserved = False
    try:
        require(sys.platform == "linux" and hasattr(os, "pidfd_open"), "linux_pidfd_required")
        require(bool(re.fullmatch(r"bench-[A-Za-z0-9_-]{1,64}", args.run_id)), "invalid_exact_run_id")
        require(args.model == MODEL and 32 <= args.expected_tasks <= 100 and 8 <= args.minimum_running <= args.expected_tasks,
                "requires_qwen235b_and_32_to_100_real_tasks_with_at_least8_valid_leases")
        require(0 <= args.wait_for_submission <= 600 and 1 <= args.expiry_timeout <= 600
            and 60 <= args.engine_ready_timeout <= 900 and 10 <= args.engine_quiescence_timeout <= 900
            and 1 <= args.terminal_timeout <= 3600, "invalid_bounds")
        manifest, services, fingerprint = load_manifest(args.manifest)
        original = private_read(manifest)
        worker, engine = selected_pair(services, args.worker_name, args.engine_name)
        require(len([row for row in services if row["role"] == "worker"]) == 1
                and len([row for row in services if row["role"] == "engine"]) == 1, "requires_single_owned_worker_engine_pair")
        apis = sorted((row for row in services if row["role"] == "api"), key=lambda row: row["name"])
        gateways = [row for row in services if row["role"] == "gateway"]
        require([(row["name"], row["port"]) for row in apis] == [("a", 8082), ("b", 8083)]
                and [(row["name"], row["port"]) for row in gateways] == [("edge", 8080)], "requires_existing_api_a_b_and_gateway")
        require(Path(apis[0]["env_file"]) == args.env_file, "must_use_manifest_api_a_runtime_config")
        for path in (args.env_file, args.supervisor_env):
            require(path.is_absolute() and path.resolve() == path and path.parent in {ROOT, ROOT.parent}, "configuration_path_outside_app_roots")
            private_read(path)
        require(args.report.parent == ROOT / ".runtime" and args.report.resolve() == args.report
                and args.report.suffix == ".json" and not args.report.exists(), "report_must_be_new_private_runtime_json")
        env, watch = manage.load_env(args.env_file), manage.load_env(args.supervisor_env)
        worker_env, engine_env = manage.load_env(Path(worker["env_file"])), manage.load_env(Path(engine["env_file"]))
        require(watch.get("SUPERVISOR_MANIFEST") == str(manifest), "supervisor_manifest_mismatch")
        require(env.get("SQL_RECOVERY_ENABLED", "").lower() in {"true", "1"}, "sql_recovery_required")
        require(worker_env.get("MODEL_NAME") == engine_env.get("MODEL_NAME") == args.model
                and engine_env.get("ENGINE_TENSOR_PARALLEL_SIZE") == "4" and engine.get("gpu") == "0,1,2,3", "exact_qwen235b_tp4_pair_required")
        require(worker_env.get("INFERENCE_BASE_URLS") == f"http://127.0.0.1:{engine['port']}" and worker_env.get("WORKER_ID"), "worker_must_use_exact_local_engine")
        supervisor = {"role": "supervisor", "name": args.supervisor_name, "env_file": str(args.supervisor_env), "python": apis[0]["python"]}
        journal = ROOT / ".runtime" / ("singlepod-engine-fault-" + args.run_id + ".journal.json")
        require(not journal.exists(), "existing_operation_no_automatic_replay")
        if not args.execute:
            print(json.dumps({"status": "dry_run", "run_id": args.run_id, "tasks": args.expected_tasks,
                "worker_sigkill": worker["name"], "managed_engine_stop": engine["name"], "watchdog_paused": True,
                "manifest_fingerprint": fingerprint, "required_benchmark_batches": 1, "requires_execute": True})); return 0
        stop = threading.Event()
        for signum in (signal.SIGTERM, signal.SIGINT):
            signal.signal(signum, lambda *_: stop.set())
        descriptor = os.open(ROOT / ".runtime/single-pod-worker-fault.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, "r+") as lock:
            info = os.fstat(lock.fileno())
            require(stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid() and not info.st_mode & 0o077, "unsafe_fault_operation_lock")
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            reserve(journal)
            reserved = True
            progress = EngineFaultProgress(env["DATABASE_URL"], args.run_id, args.model, worker_env["WORKER_ID"])
            try:
                require(progress.connection.execute("SELECT current_user AS role").fetchone()["role"] == "bf_runtime", "runtime_database_role_required")
                deadline = time.monotonic() + args.wait_for_submission
                while True:
                    counts = progress.submission_counts()
                    require(counts["total"] <= args.expected_tasks, "exact_run_contains_extra_tasks")
                    if counts["total"] == args.expected_tasks and len(progress.capture(worker_env["WORKER_ID"])) >= args.minimum_running:
                        break
                    require(not stop.is_set() and time.monotonic() < deadline, "exact_run_not_armed_with_enough_valid_leases")
                    stop.wait(.25)
                report = SinglePodEngineProbe(services=services, worker=worker, engine=engine, api=apis[0], supervisor=supervisor,
                    manifest=manifest, original=original, progress=progress, controller=EngineFaultController(),
                    token=env["BATCHFORGE_WORKER_TOKEN"], expected=args.expected_tasks, minimum_running=args.minimum_running,
                    expiry_timeout=args.expiry_timeout, terminal_timeout=args.terminal_timeout, journal=journal,
                    engine_ready_timeout=args.engine_ready_timeout, engine_quiescence_timeout=args.engine_quiescence_timeout, stop=stop).run()
                reserve(args.report); durable_write(args.report, report)
                print(json.dumps({"status": report["status"], "manual_recovery_required": report.get("manual_recovery_required", False)}))
                return 0 if report["status"] == "passed" else 1
            finally:
                progress.close()
    except Exception as error:
        failure = {"status": "refused", "error": str(error) if isinstance(error, ProbeFailure) else type(error).__name__}
        if reserved:
            # Preserve the operation record even if arming fails before Probe.run.
            try:
                saved = json.loads(private_read(journal))
                saved.update(controller_error=failure, automatic_replay=False)
                durable_write(journal, saved)
                if not args.report.exists():
                    reserve(args.report); durable_write(args.report, saved)
            except Exception:
                failure["private_failure_report_write_failed"] = True
        print(json.dumps(failure))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
