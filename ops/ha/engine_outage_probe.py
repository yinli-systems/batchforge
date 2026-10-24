#!/usr/bin/env python3
"""Exact-run application engine/worker loss; dry-run unless --execute is set.

No hardware reset, partition change, remote-node action, database mutation, or
unrelated-process discovery. Lease tokens stay in RAM. Finally restores the
released application pair and watchdog, including when observation fails.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import threading
import time
from urllib.error import HTTPError, URLError

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from ops import manage
from ops.ha.controlplane_outage_probe import (Controller, ProbeFailure, Progress,
    SNAPSHOT, require, timestamp, validate_snapshot)
from ops.supervise import ConfigError, Manager, load_manifest, private_read, private_write


# Exact worker identity, not a broad prefix that could include another worker.
ENGINE_SNAPSHOT = SNAPSHOT.replace("left(worker_id,length(%s))=%s", "worker_id=%s")


class EngineProgress(Progress):
    def __init__(self, database_url, run_id, model, secondary_worker_id):
        super().__init__(database_url, run_id, secondary_worker_id)
        self.model, self.secondary_worker_id = model, secondary_worker_id

    def snapshot(self):
        row = self.connection.execute(ENGINE_SNAPSHOT, (self.run_id, self.secondary_worker_id)).fetchone()
        value = {key: item.isoformat() if isinstance(item, datetime) else int(item) for key, item in row.items()}
        validate_snapshot(value)
        return value

    def scope(self, expected):
        row = self.connection.execute("SELECT COALESCE(sum(total_count),0) AS total,count(*) FILTER(WHERE model<>%s) AS wrong_model FROM batches WHERE metadata->>'benchmark_run'=%s", (self.model, self.run_id)).fetchone()
        require(row["total"] == expected and row["wrong_model"] == 0, "run_task_count_or_model_scope_mismatch")

    def secondary_ready(self):
        row = self.connection.execute("SELECT count(*) AS n FROM workers WHERE id=%s AND model=%s AND concurrency>0 AND last_seen_at>clock_timestamp()-interval '120 seconds'", (self.secondary_worker_id, self.model)).fetchone()
        return row["n"] == 1

    def capture(self, worker_id):
        return self.connection.execute("WITH captured AS MATERIALIZED (SELECT clock_timestamp() AS captured_at) SELECT t.id,t.worker_id,t.attempt_token,t.lease_expires_at,captured.captured_at FROM tasks t JOIN batches b ON b.id=t.batch_id CROSS JOIN captured WHERE b.metadata->>'benchmark_run'=%s AND t.model=%s AND b.model=%s AND t.worker_id=%s AND t.status='running' AND t.lease_expires_at>captured.captured_at ORDER BY t.id LIMIT 128", (self.run_id, self.model, self.model, worker_id)).fetchall()

    def held_after_loss(self, captured):
        rows = self.connection.execute("SELECT attempt_token FROM tasks WHERE attempt_token=ANY(%s) AND status='running' AND lease_expires_at>clock_timestamp()", ([item["attempt_token"] for item in captured],)).fetchall()
        tokens = {row["attempt_token"] for row in rows}
        return [item for item in captured if item["attempt_token"] in tokens]

    def recovery(self, captured):
        rows = self.connection.execute("""
SELECT old.task_id AS id,old.status AS old_status,old.error->>'code' AS old_error,
 t.status AS task_status,t.worker_id AS accepted_worker,
 (SELECT count(*) FROM attempts a WHERE a.task_id=t.id AND a.status='completed') AS accepted_count,
 EXISTS(SELECT 1 FROM attempts a WHERE a.task_id=t.id AND a.status='completed'
   AND a.worker_id=%s AND a.token<>old.token) AS accepted_by_secondary,
 t.prompt_tokens,t.completion_tokens
FROM attempts old JOIN tasks t ON t.id=old.task_id JOIN batches b ON b.id=t.batch_id
WHERE old.token=ANY(%s) AND b.metadata->>'benchmark_run'=%s AND t.model=%s AND b.model=%s
ORDER BY old.task_id
""", (self.secondary_worker_id, [item["attempt_token"] for item in captured], self.run_id, self.model, self.model)).fetchall()
        expired = [row for row in rows if row["old_status"] in {"expired", "failed"} and row["old_error"] == "lease_expired"]
        reclaimed = [row for row in expired if row["task_status"] == "completed" and row["accepted_worker"] == self.secondary_worker_id and row["accepted_by_secondary"] and row["accepted_count"] == 1]
        return {"observed_at": timestamp(), "captured": len(captured), "lease_expired": len(expired),
                "reclaimed_by_exact_secondary": len(reclaimed),
                "expired_task_ids": [row["id"] for row in expired],
                "reclaimed_tasks": [{key: row[key] for key in ("id", "accepted_worker", "accepted_count", "prompt_tokens", "completion_tokens")} for row in reclaimed]}


class EngineController(Controller):
    def start_gate(self, service):
        return Manager(root=self.root).start_gate(service)

    def invoke(self, service, action):
        arguments = [sys.executable, str(self.root / "ops/manage.py"), action, service["role"], "--name", service["name"]]
        if action == "stop":
            # Stop the worker before its engine, without a long inference drain:
            # this is an explicitly released crash/recovery experiment.
            arguments += ["--grace-seconds", "0" if service["role"] == "worker" else "3"]
        else:
            arguments += ["--env-file", service["env_file"], "--python", service["python"]]
            for key in ("port", "gpu"):
                if service.get(key) is not None:
                    arguments += ["--" + key, str(service[key])]
        result = subprocess.run(arguments, cwd=self.root, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                stderr=subprocess.DEVNULL, timeout=30, check=False)
        require(result.returncode == 0 and len(result.stdout) <= 4096, "manager_" + action + "_failed")
        value = json.loads(result.stdout)
        require(isinstance(value, dict), "manager_protocol_error")
        return value

    def responding(self, service, *, ready=False):
        path = "/health" if service["role"] == "engine" else "/readyz"
        try:
            with self.http.open(f"http://127.0.0.1:{service['port']}{path}", timeout=1) as response:
                return response.status == 200 if ready else True
        except HTTPError:
            return not ready
        except (URLError, TimeoutError, OSError):
            return False


def selected_pair(services, worker_name, engine_name):
    workers = [row for row in services if (row["role"], row["name"]) == ("worker", worker_name)]
    engines = [row for row in services if (row["role"], row["name"]) == ("engine", engine_name)]
    require(len(workers) == len(engines) == 1, "released_worker_engine_pair_not_in_manifest")
    return workers[0], engines[0]


class EngineProbe:
    def __init__(self, *, services, worker, engine, supervisor, api, manifest, original, controller,
                 progress, worker_id, token, expected_tasks, duration=120, recovery_timeout=1800,
                 minimum_reclaimed=1, stop=None, clock=time.monotonic, sleep=time.sleep):
        self.services, self.worker, self.engine, self.supervisor, self.api = services, worker, engine, supervisor, api
        self.manifest, self.original, self.controller, self.progress = manifest, original, controller, progress
        self.worker_id, self.token, self.expected = worker_id, token, expected_tasks
        self.duration, self.recovery_timeout, self.minimum = duration, recovery_timeout, minimum_reclaimed
        self.stop, self.clock, self.sleep = stop or threading.Event(), clock, sleep
        self.controls = [row for row in services if row["role"] in {"api", "gateway"}]
        self.protected = [row for row in services if row not in (worker, engine)]
        self.configs = {row["env_file"]: hashlib.sha256(private_read(Path(row["env_file"]))).hexdigest()
                        for row in (worker, engine, supervisor)}
        self.report = {"schema_version": 1, "status": "running", "started_at": timestamp(),
            "scope": "Application-owned inference engine and its worker loss; no GPU reset, whole-Pod or database outage",
            "model": progress.model, "run_id": progress.run_id, "expected_tasks": expected_tasks,
            "primary_worker_id": worker_id, "secondary_worker_id": progress.secondary_worker_id,
            "samples": [], "recovery_samples": [],
            "measurement_bounds": ["Only the explicitly selected application worker/engine and watchdog are stopped",
                "Reclaim proof requires a pre-loss valid lease, still valid after the pair is down, lease_expired cause, and exactly one accepted attempt on that same task by the exact secondary worker",
                "Database observations are read-only; the sole post-loss API write is an old-attempt heartbeat required to return stale_attempt 409",
                "Inference may execute more than once; only accepted durable usage is required to be unique",
                "Availability and process preservation are sampled, not a proof of continuous physical hardware health"]}

    def wait(self, predicate, timeout, reason, *, interruptible=True):
        deadline = self.clock() + timeout
        while not predicate():
            require(not interruptible or not self.stop.is_set(), "interrupted")
            require(self.clock() < deadline, reason)
            self.stop.wait(.25) if interruptible else self.sleep(.25)

    def sample(self, phase):
        value = {"phase": phase, **self.progress.snapshot()}
        self.report["samples"].append(value)
        return value

    def restore(self):
        errors = []
        engine_ready = False
        try:
            require(hashlib.sha256(private_read(Path(self.engine["env_file"]))).hexdigest() == self.configs[self.engine["env_file"]], "released_configuration_changed")
            if self.controller.record(self.engine) is None:
                deadline = self.clock() + 120
                while True:
                    allowed, reason = self.controller.start_gate(self.engine)
                    self.report.setdefault("engine_restart_gates", []).append({"at": timestamp(), "allowed": allowed, "reason": reason})
                    if allowed:
                        break
                    require(self.clock() < deadline, "engine_restart_memory_or_member_gate_not_ready")
                    self.sleep(2)
            self.controller.invoke(self.engine, "start")
            self.wait(lambda: self.controller.responding(self.engine, ready=True), 600, "restored_engine_not_ready", interruptible=False)
            engine_ready = True
        except Exception:
            errors.append("engine_restore_failed")
        if engine_ready:
            try:
                require(hashlib.sha256(private_read(Path(self.worker["env_file"]))).hexdigest() == self.configs[self.worker["env_file"]], "released_configuration_changed")
                self.controller.invoke(self.worker, "start")
                self.wait(lambda: self.controller.responding(self.worker, ready=True), 120, "restored_worker_not_ready", interruptible=False)
            except Exception:
                errors.append("worker_restore_failed")
        else:
            errors.append("worker_restore_deferred_until_engine_ready")
        try:
            require(private_read(self.manifest) == self.original, "active_manifest_changed")
            require(hashlib.sha256(private_read(Path(self.supervisor["env_file"]))).hexdigest() == self.configs[self.supervisor["env_file"]], "watchdog_configuration_changed")
            self.controller.invoke(self.supervisor, "start")
            require(self.controller.record(self.supervisor) is not None, "watchdog_not_running")
        except Exception:
            errors.append("watchdog_restore_failed")
        self.report.setdefault("restore_attempts", []).append({"at": timestamp(), "errors": errors})
        require(not errors, "restore_incomplete")

    def stop_released(self, row, expected):
        deadline = self.clock() + 30
        while True:
            current = self.controller.record(row)
            require(current is None or current == expected, "released_stop_target_identity_changed")
            try:
                self.controller.invoke(row, "stop")
                return
            except Exception:
                # Exact tracked members can take a moment to leave after a
                # leader SIGKILL. Retrying never broadens the PID/name scope.
                current = self.controller.record(row)
                if current is None and not self.controller.responding(row):
                    return
                require(self.clock() < deadline, "released_pair_stop_did_not_converge")
                self.sleep(1)

    def run(self):
        maintenance, restored = False, False
        try:
            before_records = {f"{row['role']}-{row['name']}": self.controller.record(row) for row in self.services}
            require(all(before_records.values()) and self.controller.record(self.supervisor), "expected_owned_process_not_running")
            require(all(self.controller.responding(row, ready=True) for row in [*self.controls, self.worker, self.engine]), "initial_service_not_ready")
            self.progress.scope(self.expected)
            require(self.progress.secondary_ready(), "exact_secondary_worker_not_fresh_for_model")
            before = self.sample("before")
            require(before["pending"] + before["running"] >= self.minimum + 1, "insufficient_real_work")
            captured = self.progress.capture(self.worker_id)
            require(captured, "no_valid_primary_lease_before_loss")
            require(private_read(self.manifest) == self.original, "active_manifest_changed")
            require(not self.stop.is_set(), "interrupted")
            maintenance = True
            self.controller.invoke(self.supervisor, "stop")
            require(self.controller.record(self.supervisor) is None, "watchdog_did_not_stop")
            self.stop_released(self.worker, before_records[f"worker-{self.worker['name']}"])
            self.stop_released(self.engine, before_records[f"engine-{self.engine['name']}"])
            unavailable = lambda: all(self.controller.record(row) is None and not self.controller.responding(row) for row in (self.worker, self.engine))
            self.wait(unavailable, 30, "released_pair_still_running")
            held = self.progress.held_after_loss(captured)
            require(held, "all_captured_tasks_finished_or_expired_before_pair_loss")
            self.report.update(pair_unavailable_at=timestamp(), captured_valid_leases=len(captured), held_valid_leases_after_loss=len(held),
                valid_lease_capture_at_db_clock=min(row["captured_at"] for row in held).isoformat(),
                captured_lease_expiry_bounds=[min(row["lease_expires_at"] for row in held).isoformat(), max(row["lease_expires_at"] for row in held).isoformat()])
            down_at = self.clock()
            while True:
                require(not self.stop.is_set(), "interrupted")
                require(unavailable(), "released_pair_restarted_before_reclaim_proof")
                require(all(self.controller.responding(row, ready=True) for row in self.controls), "controlplane_not_ready_during_engine_loss")
                for row in self.protected:
                    require(self.controller.record(row) == before_records[f"{row['role']}-{row['name']}"], "unreleased_process_identity_changed")
                self.sample("during_pair_loss")
                recovery = self.progress.recovery(held)
                self.report["recovery_samples"].append(recovery)
                elapsed = self.clock() - down_at
                if elapsed >= self.duration and recovery["reclaimed_by_exact_secondary"] >= self.minimum:
                    break
                require(elapsed < self.duration + self.recovery_timeout, "captured_task_not_reclaimed_by_exact_secondary_before_timeout")
                self.stop.wait(2)
            self.report.update(pair_unavailable_through=timestamp(), observed_pair_loss_seconds=self.clock()-down_at,
                               captured_tasks_reclaimed_by_secondary=recovery["reclaimed_by_exact_secondary"])
            reclaimed_ids = {row["id"] for row in recovery["reclaimed_tasks"]}
            candidate = next((row for row in held if row["id"] in reclaimed_ids), None)
            require(candidate is not None, "reclaimed_task_missing_from_captured_leases")
            self.controller.fence(self.api, self.token, candidate)
            self.report.update(expired_attempt_heartbeat_status=409, fenced_task_id=candidate["id"])
            self.restore()
            restored = True
            require(all(self.controller.record(row) == before_records[f"{row['role']}-{row['name']}"] for row in self.protected), "unreleased_process_identity_changed")
            self.report["preserved_process_pids"] = {f"{row['role']}-{row['name']}": before_records[f"{row['role']}-{row['name']}"]["pid"] for row in self.protected}
            self.sample("after_restore")
            self.report["status"] = "passed"
        except BaseException as error:
            self.report.update(status="failed", error=str(error) if isinstance(error, ProbeFailure) else type(error).__name__)
        finally:
            if maintenance and not restored:
                try:
                    self.restore()
                except BaseException:
                    self.report["final_restore_failed"] = True
            self.report["finished_at"] = timestamp()
        return self.report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--supervisor-env", type=Path, required=True)
    parser.add_argument("--supervisor-name", default="primary")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--expected-tasks", type=int, required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--worker-name", required=True)
    parser.add_argument("--engine-name", required=True)
    parser.add_argument("--secondary-worker-id", required=True)
    parser.add_argument("--duration", type=int, default=120)
    parser.add_argument("--recovery-timeout", type=int, default=1800)
    parser.add_argument("--wait-for-submission-seconds", type=int, default=300)
    parser.add_argument("--minimum-reclaimed", type=int, default=1)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    try:
        require(Path("/proc/self/stat").exists(), "linux_required")
        require(bool(re.fullmatch(r"bench-[a-zA-Z0-9_-]{1,64}", args.run_id)), "invalid_exact_run_id")
        require(1 <= args.expected_tasks <= 1_000_000 and 1 <= args.minimum_reclaimed <= 128, "invalid_task_bounds")
        require(105 <= args.duration <= 300 and 0 <= args.recovery_timeout <= 3600 and 0 <= args.wait_for_submission_seconds <= 600, "invalid_time_bounds")
        require(bool(re.fullmatch(r"[A-Za-z0-9_-]{1,100}", args.secondary_worker_id)), "invalid_secondary_identity")
        manifest, services, fingerprint = load_manifest(args.manifest)
        original = private_read(manifest)
        worker, engine = selected_pair(services, args.worker_name, args.engine_name)
        apis = [row for row in services if row["role"] == "api" and row["name"] == "a"]
        require(len(apis) == 1 and Path(apis[0]["env_file"]) == args.env_file, "must_use_manifest_api_a_runtime_config")
        for path in (args.env_file, args.supervisor_env):
            require(path.is_absolute() and path.parent in {ROOT, ROOT.parent} and path.resolve() == path, "configuration_path_outside_app_roots")
            private_read(path)
        require(args.report.parent == ROOT/".runtime" and args.report.suffix == ".json" and args.report.resolve() == args.report and not args.report.exists(), "new_report_must_be_inside_app_runtime")
        environment, watch = manage.load_env(args.env_file), manage.load_env(args.supervisor_env)
        require(watch.get("SUPERVISOR_MANIFEST") == str(manifest), "supervisor_manifest_mismatch")
        worker_env, engine_env = manage.load_env(Path(worker["env_file"])), manage.load_env(Path(engine["env_file"]))
        wid = worker_env.get("WORKER_ID", "")
        require(wid and wid != args.secondary_worker_id and worker_env.get("MODEL_NAME") == engine_env.get("MODEL_NAME") == args.model, "released_pair_model_or_worker_mismatch")
        require(worker_env.get("INFERENCE_BASE_URLS") == f"http://127.0.0.1:{engine['port']}", "worker_must_use_only_the_released_local_engine")
        require(args.duration >= int(environment.get("LEASE_SECONDS", "90")) + 15, "loss_window_must_exceed_lease_and_recovery_margin")
        supervisor = {"role": "supervisor", "name": args.supervisor_name, "python": apis[0]["python"], "env_file": str(args.supervisor_env)}
        if not args.execute:
            print(json.dumps({"status": "dry_run", "run_id": args.run_id, "model": args.model, "configuration_sha256": fingerprint,
                "stopped_services": ["supervisor-"+args.supervisor_name, "worker-"+worker["name"], "engine-"+engine["name"]],
                "minimum_same_task_secondary_reclaims": args.minimum_reclaimed, "duration_seconds": args.duration,
                "expected_tasks": args.expected_tasks, "requires_execute": True}))
            return 0
        stop = threading.Event()
        for signum in (signal.SIGTERM, signal.SIGINT):
            signal.signal(signum, lambda *_: stop.set())
        with (ROOT/".runtime/engine-outage.lock").open("w") as lock:
            os.chmod(lock.name, 0o600)
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            progress = EngineProgress(environment["DATABASE_URL"], args.run_id, args.model, args.secondary_worker_id)
            try:
                deadline = time.monotonic() + args.wait_for_submission_seconds
                while True:
                    counts = progress.submission_counts()
                    require(counts["total"] <= args.expected_tasks, "exact_run_contains_extra_tasks")
                    if counts["total"] == args.expected_tasks and progress.capture(wid):
                        break
                    require(not stop.is_set(), "interrupted_before_maintenance")
                    require(time.monotonic() < deadline, "exact_run_not_submitted_with_valid_primary_lease")
                    stop.wait(1)
                report = EngineProbe(services=services, worker=worker, engine=engine, supervisor=supervisor, api=apis[0],
                    manifest=manifest, original=original, controller=EngineController(), progress=progress, worker_id=wid,
                    token=environment["BATCHFORGE_WORKER_TOKEN"], expected_tasks=args.expected_tasks, duration=args.duration,
                    recovery_timeout=args.recovery_timeout, minimum_reclaimed=args.minimum_reclaimed, stop=stop).run()
                private_write(args.report, report)
                print(json.dumps({"status": report["status"], "report": str(args.report), "error": report.get("error"), "final_restore_failed": report.get("final_restore_failed", False)}))
                return 0 if report["status"] == "passed" else 1
            finally:
                progress.close()
    except Exception as error:
        print(json.dumps({"status": "refused", "error": str(error) if isinstance(error, (ProbeFailure, ConfigError)) else type(error).__name__}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
