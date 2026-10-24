"""Released, exact-run worker SIGKILL recovery on one Pod; dry-run by default.

Keeps every engine/API alive. Old attempt tokens stay in RAM. Optional reduced
capacity uses a new private worker env, never changes the released config, and
is restored in finally. Start the bounded real benchmark separately and supply
its explicit run ID; no 'latest batch' selection or synthetic accepted results.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import signal
import select
import sys
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from ops import manage
from ops.ha.controlplane_outage_probe import ProbeFailure, require, timestamp
from ops.ha.engine_outage_probe import EngineController, EngineProgress, selected_pair
from ops.supervise import ConfigError, load_manifest, private_read, private_write
from worker.kv_admission import engine_quiescent


class WorkerController(EngineController):
    def engine_quiescence(self, service):
        require(service.get('role')=='engine' and type(service.get('port')) is int
                and 1<=service['port']<=65535, 'invalid_private_engine_metrics_target')
        try:
            with self.http.open(f"http://127.0.0.1:{service['port']}/metrics", timeout=3) as response:
                if response.status!=200:
                    return {'quiescent':False,'error':'metrics_http_error','http_status':response.status}
                raw=response.read(2*1024*1024+1)
            if len(raw)>2*1024*1024:
                return {'quiescent':False,'error':'metrics_too_large'}
            try:metrics=raw.decode('utf-8')
            except UnicodeDecodeError:return {'quiescent':False,'error':'metrics_invalid_utf8'}
            return {'quiescent':engine_quiescent(metrics),'error':None}
        except HTTPError as error:
            code=error.code
            error.close()
            return {'quiescent':False,'error':'metrics_http_error','http_status':code}
        except (URLError, TimeoutError, OSError):
            return {'quiescent':False,'error':'metrics_unavailable'}

    def graceful_stop(self, service, expected, grace, check):
        # Share manage.py's exact service lock. A PID cannot be replaced by a
        # concurrent managed start between validation and the TERM operation.
        name = "worker-" + service["name"]
        pidfile = self.root / ".runtime" / (name + ".json")
        began = time.monotonic()
        with (self.root / ".runtime" / (name + ".lock")).open("w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            require(self.record(service) == expected, "drain_worker_identity_changed")
            descriptor = os.pidfd_open(expected["pid"])
            try:
                require(self.record(service) == expected, "drain_identity_changed_after_pidfd_open")
                signal.pidfd_send_signal(descriptor, signal.SIGTERM)
                poll = select.poll()
                poll.register(descriptor, select.POLLIN)
                while not poll.poll(500):
                    check()
                    require(time.monotonic() - began < grace, "worker_drain_grace_expired")
                # pidfd readiness confirms kernel exit even for an unreaped
                # zombie. Remove only this unchanged owned identity record.
                require(json.loads(private_read(pidfile)) == expected, "drained_worker_record_changed")
                pidfile.unlink()
            finally:
                os.close(descriptor)
        return time.monotonic() - began

    def staged_record(self, service, run_id):
        record = self.record(service)
        if record is None:
            return None
        # Inspect only this exact already-owned PID. Never print or persist its
        # environment; the one run marker identifies a partially acknowledged
        # start without guessing a PID from a port/process-name search.
        wanted = ("BATCHFORGE_FAULT_RUN_ID=" + run_id).encode()
        environment = Path(f"/proc/{record['pid']}/environ").read_bytes()
        require(wanted in environment.split(b"\0"), "replacement_worker_not_owned_by_this_probe")
        require(self.record(service) == record, "replacement_worker_identity_changed")
        return record

    def crash_exact(self, service, expected):
        require(hasattr(os, "pidfd_open") and hasattr(signal, "pidfd_send_signal"), "linux_pidfd_required")
        require(expected is not None and self.record(service) == expected, "worker_identity_changed_before_fault")
        descriptor = os.pidfd_open(expected["pid"])
        try:
            require(self.record(service) == expected, "worker_identity_changed_after_pidfd_open")
            signal.pidfd_send_signal(descriptor, signal.SIGKILL)
        finally:
            os.close(descriptor)

    def fence_both(self, api, token, task):
        results = {}
        for operation in ("heartbeat", "complete"):
            body = {"worker_id": task["worker_id"], "attempt_token": task["attempt_token"]}
            if operation == "complete":
                body["response"] = {"batchforge_probe": "stale_completion_must_be_rejected",
                                    "choices": [{"message": {"role": "assistant", "content": "STALE PROBE; must be rejected"}, "finish_reason": "stop"}],
                                    "usage": {"prompt_tokens": 0, "completion_tokens": 0}}
            request = Request(f"http://127.0.0.1:{api['port']}/internal/tasks/{task['id']}/{operation}",
                data=json.dumps(body).encode(), headers={"Authorization": "Bearer " + token,
                "Content-Type": "application/json"}, method="POST")
            try:
                with self.http.open(request, timeout=5):
                    raise ProbeFailure("stale_" + operation + "_was_accepted")
            except HTTPError as error:
                require(error.code == 409 and json.loads(error.read(4096)).get("error", {}).get("code") == "stale_attempt",
                        "stale_" + operation + "_not_fenced_409")
                results[operation] = 409
        return results


class WorkerProgress(EngineProgress):
    def accepted_token_count(self, captured):
        return self.connection.execute("SELECT count(*) AS n FROM attempts WHERE token=ANY(%s) AND status='completed'", ([r["attempt_token"] for r in captured],)).fetchone()["n"]

    def other_unfinished_work(self):
        return self.connection.execute("SELECT count(*) AS n FROM tasks t JOIN batches b ON b.id=t.batch_id WHERE t.model=%s AND t.status IN ('pending','running') AND b.metadata->>'benchmark_run' IS DISTINCT FROM %s", (self.model, self.run_id)).fetchone()["n"]

    def other_fresh_workers(self, wid):
        return self.connection.execute("SELECT count(*) AS n FROM workers WHERE model=%s AND id<>%s AND last_seen_at>clock_timestamp()-interval '120 seconds'", (self.model, wid)).fetchone()["n"]

    def capacity(self, wid):
        row = self.connection.execute("SELECT concurrency FROM workers WHERE id=%s AND model=%s", (wid, self.model)).fetchone()
        return row["concurrency"] if row else None

    def recovered_pending(self, captured):
        return self.connection.execute("SELECT count(*) AS n FROM tasks WHERE id=ANY(%s) AND status='pending' AND error->>'code'='lease_expired'", ([r["id"] for r in captured],)).fetchone()["n"]

    def accepted_state(self, tid):
        return self.connection.execute("SELECT t.id,t.status,t.worker_id,t.attempts,t.prompt_tokens,t.completion_tokens,md5(t.response::text) AS response_digest,(SELECT count(*) FROM attempts a WHERE a.task_id=t.id AND a.status='completed') AS accepted_count FROM tasks t WHERE t.id=%s", (tid,)).fetchone()


def worker_env_values(content):
    values = {}
    for line in content.decode().splitlines():
        text = line.strip()
        if text.startswith("export "):
            text = text[7:]
        key, separator, value = text.partition("=")
        if separator:
            values[key] = " ".join(shlex.split(value, comments=True))
    return values


def worker_adaptive_enabled(content):
    flag=worker_env_values(content).get("WORKER_ADAPTIVE_KV_ENABLED", "false").lower()
    require(flag in {"true", "false", "1", "0"}, "invalid_adaptive_capacity_flag")
    return flag in {"true", "1"}


def reduced_worker_env(content, capacity):
    values=worker_env_values(content)
    maximum = int(values.get("ENGINE_MAX_MODEL_LEN", "2048"))
    require(maximum > 0, "invalid_worker_context")
    adaptive = values.get("WORKER_ADAPTIVE_KV_ENABLED", "false").lower()
    require(adaptive in {"true", "false", "1", "0"}, "invalid_adaptive_capacity_flag")
    inflight = int(values.get("WORKER_MAX_INFLIGHT", "8"))
    token_budget = int(values.get("WORKER_TOKEN_BUDGET", "16384"))
    # Match worker.config.Settings.slots: adaptive admission advertises claim
    # slots separately from its exact-token KV budget. Dividing that budget by
    # full context would incorrectly restore the released64 worker as16 slots.
    original = inflight if adaptive in {"true", "1"} else min(inflight, token_budget // maximum)
    require(1 <= capacity < original and maximum > 0, "reduced_capacity_must_be_below_original_slots")
    reserved_context = ((maximum + 15) // 16) * 16 if adaptive in {"true", "1"} else maximum
    require(token_budget >= reserved_context, "original_budget_cannot_fit_context")
    changes = {"WORKER_MAX_INFLIGHT": str(capacity), "WORKER_TOKEN_BUDGET": str(capacity * reserved_context)}
    lines = []
    for line in content.decode().splitlines():
        key = line.strip().removeprefix("export ").partition("=")[0]
        if key not in changes:
            lines.append(line)
    lines.extend(key + "=" + shlex.quote(value) for key, value in changes.items())
    return ("\n".join(lines) + "\n").encode(), original


class SinglePodProbe:
    def __init__(self, *, services, worker, engine, api, supervisor, manifest, original,
                 progress, controller, token, expected, minimum_running=8, capacity=2,
                 expiry_timeout=240, terminal_timeout=1800, drain_seconds=180, engine_quiescence_timeout=600, stop=None, clock=time.monotonic,
                 sleep=time.sleep):
        self.services, self.worker, self.engine, self.api, self.supervisor = services, worker, engine, api, supervisor
        self.manifest, self.original, self.progress, self.controller = manifest, original, progress, controller
        self.token, self.expected, self.minimum, self.capacity = token, expected, minimum_running, capacity
        self.expiry_timeout, self.terminal_timeout = expiry_timeout, terminal_timeout
        self.drain_seconds = drain_seconds
        self.stop, self.clock, self.sleep = stop or threading.Event(), clock, sleep
        self.protected = [row for row in services if row != worker]
        self.worker_bytes = private_read(Path(worker["env_file"]))
        self.adaptive_kv=worker_adaptive_enabled(self.worker_bytes)
        self.engine_quiescence_timeout=engine_quiescence_timeout
        if self.adaptive_kv:
            require(type(engine_quiescence_timeout) is int and 10<=engine_quiescence_timeout<=900, "invalid_engine_quiescence_timeout")
        self.watch_hash = hashlib.sha256(private_read(Path(supervisor["env_file"]))).hexdigest()
        self.reduced, self.reduced_record, self.restored_record = None, None, None
        self.report = {"schema_version": 1, "status": "running", "started_at": timestamp(),
            "run_id": progress.run_id, "model": progress.model, "expected_tasks": expected,
            "scope": "One exact application worker SIGKILL; inference engine/API/database remain alive",
            "samples": [], "http_poll_errors": {}, "restore_attempts": [],
            "adaptive_engine_startup_requires_quiescence": self.adaptive_kv, "engine_quiescence_gates": [],
            "measurement_bounds": ["Real workload is submitted by the separately identified existing benchmark",
                "No GPU reset, engine stop, whole-Pod failure or database failover",
                "Expired tokens stay only in RAM; final report contains task IDs and accepted usage only",
                "API recovery is observed periodically; sampled deltas do not establish per-call SQL budgets",
                "Inference may execute more than once; accepted results and usage must remain unique",
                "Recovered pending leases do not prove the still-live engine has released orphaned inference/KV",
                "Capacity2 uses a temporary worker-only config; original engine/KV settings stay unchanged"]}

    def wait(self, predicate, timeout, reason, *, interruptible=True):
        deadline = self.clock() + timeout
        while not predicate():
            require(not interruptible or not self.stop.is_set(), "interrupted")
            require(self.clock() < deadline, reason)
            self.sleep(.5)

    def sample(self, phase):
        value = self.progress.snapshot()
        value["worker_completed"] = value.pop("secondary_completed", 0)
        self.report["samples"].append({"phase": phase, **value})
        return value

    def preserve(self):
        for row in self.protected:
            name = row["role"] + "-" + row["name"]
            require(self.controller.record(row) == self.before[name], "unreleased_process_identity_changed")
            if not self.controller.responding(row, ready=True):
                errors = self.report["http_poll_errors"]
                errors[name] = errors.get(name, 0) + 1
                raise ProbeFailure("unreleased_service_not_ready")

    def await_engine_quiescence(self, phase="before_worker_restart"):
        if not self.adaptive_kv:return
        began=self.clock()
        gate={'phase':phase,'started_at':timestamp(),'timeout_seconds':self.engine_quiescence_timeout,
              'status':'waiting','observations':[],'errors':{},'busy_observations':0}
        self.report['engine_quiescence_gates'].append(gate)
        passed=False
        def idle():
            require(not self.stop.is_set(), 'interrupted')
            self.preserve()
            value=self.controller.engine_quiescence(self.engine)
            gate['observations'].append({'at':timestamp(),**value})
            if value.get('error'):
                error=value['error'];gate['errors'][error]=gate['errors'].get(error,0)+1
            elif not value['quiescent']:
                gate['busy_observations']+=1
            return value['quiescent']
        try:
            self.wait(idle,self.engine_quiescence_timeout,'adaptive_engine_not_quiescent_before_worker_restart')
            passed=True
        finally:
            gate.update(status='passed' if passed else 'failed',finished_at=timestamp(),wait_seconds=self.clock()-began)

    def restore(self):
        errors = []
        try:
            require(private_read(Path(self.worker["env_file"])) == self.worker_bytes, "original_worker_configuration_changed")
            current = self.controller.record(self.worker)
            if current and self.reduced_record and current == self.reduced_record:
                deadline = self.clock() + 30
                while self.controller.record(self.worker) is not None:
                    require(self.clock() < deadline, "reduced_worker_stop_did_not_converge")
                    require(self.controller.record(self.worker) == self.reduced_record, "replacement_worker_identity_changed_during_stop")
                    try:
                        self.controller.invoke(self.reduced, "stop")
                    except Exception:
                        require(self.clock() < deadline, "reduced_worker_stop_did_not_converge")
                    self.sleep(.5)
            elif current:
                require(current in (self.before["worker-" + self.worker["name"]], self.restored_record), "unexpected_replacement_worker_identity")
            try:
                self.controller.invoke(self.worker, "start")
            finally:
                self.restored_record = self.controller.record(self.worker)
            self.wait(lambda: self.controller.responding(self.worker, ready=True), 120, "original_worker_not_ready", interruptible=False)
            self.report["restored_worker_pid"] = self.controller.record(self.worker)["pid"]
            if self.report.get("original_configured_slots"):
                original = self.report["original_configured_slots"]
                self.wait(lambda: self.progress.capacity(self.progress.secondary_worker_id) == original,
                          30, "original_worker_capacity_not_restored", interruptible=False)
                self.report["restored_server_capacity"] = original
        except Exception:
            errors.append("worker_restore_failed")
        try:
            require(private_read(self.manifest) == self.original, "active_manifest_changed")
            require(hashlib.sha256(private_read(Path(self.supervisor["env_file"]))).hexdigest() == self.watch_hash,
                    "watchdog_configuration_changed")
            self.controller.invoke(self.supervisor, "start")
            require(self.controller.record(self.supervisor) is not None, "watchdog_not_running")
        except Exception:
            errors.append("watchdog_restore_failed")
        if not errors and self.reduced:
            try:
                temporary = Path(self.reduced["env_file"])
                if temporary.exists():
                    require(hashlib.sha256(private_read(temporary)).hexdigest() == self.report["reduced_worker_env_sha256"],
                            "temporary_worker_configuration_changed")
                    temporary.unlink()
                self.report["temporary_worker_configuration_removed"] = True
            except Exception:
                self.report["temporary_worker_configuration_cleanup_failed"] = True
        self.report["restore_attempts"].append({"at": timestamp(), "errors": errors})
        require(not errors, "restore_incomplete")

    def run(self):
        maintenance, restored = False, False
        try:
            self.before = {r["role"] + "-" + r["name"]: self.controller.record(r) for r in self.services}
            require(all(self.before.values()) and self.controller.record(self.supervisor), "owned_service_missing")
            self.preserve()
            self.progress.scope(self.expected)
            require(self.progress.other_unfinished_work() == 0, "another_cohort_has_unfinished_work_on_released_model")
            require(self.progress.other_fresh_workers(self.progress.secondary_worker_id) == 0, "another_fresh_worker_serves_this_model")
            before = self.sample("before")
            captured = self.progress.capture(self.progress.secondary_worker_id)
            require(before["running"] >= self.minimum and len(captured) >= self.minimum, "insufficient_actual_running_leases")
            require(private_read(self.manifest) == self.original, "active_manifest_changed")
            maintenance = True
            self.controller.invoke(self.supervisor, "stop")
            require(self.controller.record(self.supervisor) is None, "watchdog_did_not_stop")
            self.controller.crash_exact(self.worker, self.before["worker-" + self.worker["name"]])
            self.wait(lambda: self.controller.record(self.worker) is None and not self.controller.responding(self.worker),
                      15, "worker_still_responds_after_sigkill")
            held = self.progress.held_after_loss(captured)
            require(held, "no_captured_lease_remained_valid_after_worker_loss")
            down = self.clock()
            self.report.update(worker_unavailable_at=timestamp(), crashed_worker_pid=self.before["worker-" + self.worker["name"]]["pid"],
                captured_leases=len(captured), held_valid_leases_after_loss=len(held),
                captured_at_db_clock=min(r["captured_at"] for r in held).isoformat(),
                lease_expiry_bounds=[min(r["lease_expires_at"] for r in held).isoformat(), max(r["lease_expires_at"] for r in held).isoformat()])

            def expired():
                self.preserve()
                require(self.controller.record(self.worker) is None, "worker_restarted_before_expiry_proof")
                self.sample("worker_down")
                rows, statuses = self.progress.expired(held)
                if rows and "first_expired_observed_at" not in self.report:
                    self.report["first_expired_observed_at"] = timestamp()
                self.report["captured_attempt_status_counts"] = statuses
                return len(rows) == len(held) and self.progress.recovered_pending(held) == len(held)

            self.wait(expired, self.expiry_timeout, "captured_leases_not_recovered_as_retryable_pending")
            self.report.update(all_captured_recovered_pending_at=timestamp(),
                               observed_recovery_seconds=self.clock() - down)
            selected = self.worker
            if self.capacity:
                data, original_slots = reduced_worker_env(self.worker_bytes, self.capacity)
                data += ("BATCHFORGE_FAULT_RUN_ID=" + shlex.quote(self.progress.run_id) + "\n").encode()
                path = self.manifest.parent / ("singlepod-fault-" + self.progress.run_id + "-worker.env")
                descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
                with os.fdopen(descriptor, "wb") as handle:
                    handle.write(data)
                    handle.flush()
                    os.fsync(handle.fileno())
                self.reduced = {**self.worker, "env_file": str(path)}
                selected = self.reduced
                self.report.update(original_configured_slots=original_slots, reduced_configured_slots=self.capacity,
                                   reduced_worker_env_sha256=hashlib.sha256(data).hexdigest())
            self.await_engine_quiescence("after_lease_recovery_before_worker_restart")
            try:
                self.controller.invoke(selected, "start")
            finally:
                if self.reduced:
                    self.reduced_record = self.controller.staged_record(selected, self.progress.run_id)
                else:
                    self.restored_record = self.controller.record(selected)
            self.wait(lambda: self.controller.responding(selected, ready=True), 120, "restarted_worker_not_ready")
            if self.capacity:
                require(self.progress.capacity(self.progress.secondary_worker_id) == self.capacity, "reduced_server_capacity_not_registered")
                self.report["server_capacity_after_restart"] = self.capacity
            self.report["restarted_worker_pid"] = self.controller.record(selected)["pid"]

            if self.capacity:
                observations = 0
                self.report["capacity_reduced_at"] = timestamp()

                def reduced_observed():
                    nonlocal observations
                    self.preserve()
                    value = self.sample("capacity_reduced")
                    require(value["failed"] == value["cancelled"] == 0, "workload_has_terminal_failure")
                    require(value["running"] <= self.capacity, "restarted_worker_exceeded_reduced_capacity")
                    require(self.progress.capacity(self.progress.secondary_worker_id) == self.capacity, "reduced_server_capacity_changed")
                    if value["pending"] > 0 and value["running"] > 0:
                        observations += 1
                        self.report["reduced_capacity_observed_with_pending_work"] = True
                    return observations >= 3 or value["completed"] == self.expected

                self.wait(reduced_observed, self.terminal_timeout, "reduced_capacity_not_observed_before_timeout")
                self.report.update(reduced_positive_samples=observations, capacity_reduction_proven=observations >= 3,
                                   capacity_drain_started_at=timestamp())
                draining = self.progress.capture(self.progress.secondary_worker_id)
                require(len(draining) <= self.capacity, "draining_attempts_exceeded_reduced_capacity")
                drain_elapsed = self.controller.graceful_stop(selected, self.reduced_record, self.drain_seconds, self.preserve)
                self.wait(lambda: self.progress.accepted_token_count(draining) == len(draining),
                          15, "capacity_drain_did_not_accept_its_inflight_attempts")
                self.report.update(capacity_drain_finished_at=timestamp(), drained_valid_attempts=len(draining),
                                   graceful_drain_attempts_accepted=True, capacity_drain_seconds=drain_elapsed)
                after_drain = self.sample("after_capacity_drain")
                self.report["pending_before_capacity_restore"] = after_drain["pending"]
                self.restore()
                restored = True
                self.report["capacity_restored_under_load_at"] = timestamp()
                self.report["capacity_reexpansion_proven"] = False
                if after_drain["pending"] == 0:
                    self.report["capacity_reexpansion_limitation"] = "no_pending_work_remained_after_graceful_drain"

            def terminal():
                self.preserve()
                value = self.sample("recovery_running")
                if self.capacity and value["running"] > self.capacity:
                    self.report["capacity_reexpansion_proven"] = True
                    self.report.setdefault("capacity_reexpansion_observed_at", timestamp())
                require(value["failed"] == value["cancelled"] == 0, "workload_has_terminal_failure")
                return value["completed"] == self.expected and value["pending"] == value["running"] == 0

            self.wait(terminal, self.terminal_timeout, "real_workload_not_terminal_after_recovery")
            if self.capacity and not self.report["capacity_reexpansion_proven"]:
                self.report.setdefault("capacity_reexpansion_limitation", "no_sample_observed_more_than_reduced_capacity")
            recovery = self.progress.recovery(held)
            require(recovery["reclaimed_by_exact_secondary"] == len(held), "not_all_captured_tasks_reaccepted_once")
            candidate = next(row for row in held if row["id"] in {r["id"] for r in recovery["reclaimed_tasks"]})
            before_fence = self.progress.accepted_state(candidate["id"])
            self.report["stale_attempt_http_status"] = self.controller.fence_both(self.api, self.token, candidate)
            require(self.progress.accepted_state(candidate["id"]) == before_fence, "stale_request_changed_accepted_state")
            self.report.update(fenced_task_id=candidate["id"], accepted_state_unchanged=True,
                captured_tasks_reclaimed_once=recovery["reclaimed_by_exact_secondary"], reclaimed_tasks=recovery["reclaimed_tasks"])
            restored = False
            self.restore()
            restored = True
            self.preserve()
            self.sample("after_restore")
            self.report.update(status="passed", finished_at=timestamp(),
                preserved_process_pids={k: v["pid"] for k, v in self.before.items() if k != "worker-" + self.worker["name"]})
        except Exception as error:
            self.report.update(status="failed", error=str(error) if isinstance(error, ProbeFailure) else type(error).__name__, finished_at=timestamp())
        finally:
            if maintenance and not restored:
                try:
                    self.restore()
                except Exception:
                    self.report["final_restore_failed"] = True
        return self.report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, required=True, help="Manifest API-a runtime env, never owner credentials")
    parser.add_argument("--supervisor-env", type=Path, required=True)
    parser.add_argument("--supervisor-name", default="primary")
    parser.add_argument("--worker-name", required=True)
    parser.add_argument("--engine-name", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--expected-tasks", type=int, required=True)
    parser.add_argument("--minimum-running", type=int, default=8)
    parser.add_argument("--reduced-capacity", type=int, default=2, help="0 skips temporary capacity reduction")
    parser.add_argument("--wait-for-submission", type=int, default=300)
    parser.add_argument("--expiry-timeout", type=int, default=240)
    parser.add_argument("--terminal-timeout", type=int, default=1800)
    parser.add_argument("--drain-seconds", type=int, default=180)
    parser.add_argument("--engine-quiescence-timeout", type=int, default=600)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    try:
        require(Path("/proc/self/stat").exists(), "linux_required")
        require(bool(re.fullmatch(r"bench-[A-Za-z0-9_-]{1,64}", args.run_id)), "invalid_exact_run_id")
        require(32 <= args.expected_tasks <= 100 and 8 <= args.minimum_running <= args.expected_tasks,
                "real_workload_must_have_32_to_100_tasks_and_at_least_8_running")
        require(0 <= args.reduced_capacity <= 2 and 1 <= args.expiry_timeout <= 600
                and 1 <= args.terminal_timeout <= 3600 and 0 <= args.wait_for_submission <= 600
                and 30 <= args.drain_seconds <= 600, "invalid_bounds")
        manifest, services, fingerprint = load_manifest(args.manifest)
        original = private_read(manifest)
        worker, engine = selected_pair(services, args.worker_name, args.engine_name)
        apis = [r for r in services if r["role"] == "api" and r["name"] == "a"]
        require(len(apis) == 1 and Path(apis[0]["env_file"]) == args.env_file, "must_use_manifest_api_a_runtime_config")
        for path in (args.env_file, args.supervisor_env):
            require(path.is_absolute() and path.resolve() == path and path.parent in {ROOT, ROOT.parent}, "configuration_path_outside_app_roots")
            private_read(path)
        require(args.report.parent == ROOT / ".runtime" and args.report.resolve() == args.report
                and args.report.suffix == ".json" and not args.report.exists(), "report_must_be_new_private_runtime_json")
        env, watch = manage.load_env(args.env_file), manage.load_env(args.supervisor_env)
        worker_env, engine_env = manage.load_env(Path(worker["env_file"])), manage.load_env(Path(engine["env_file"]))
        require(watch.get("SUPERVISOR_MANIFEST") == str(manifest), "supervisor_manifest_mismatch")
        require(env.get("SQL_RECOVERY_ENABLED", "").lower() in {"true", "1"}, "v4_recovery_must_be_enabled")
        require(worker_env.get("MODEL_NAME") == engine_env.get("MODEL_NAME") == args.model, "released_pair_model_mismatch")
        wid = worker_env.get("WORKER_ID", "")
        require(bool(wid), "worker_id_missing")
        require(worker_env.get("INFERENCE_BASE_URLS") == f"http://127.0.0.1:{engine['port']}", "worker_must_use_released_local_engine_only")
        if worker_adaptive_enabled(private_read(Path(worker["env_file"]))):
            require(10<=args.engine_quiescence_timeout<=900,"invalid_engine_quiescence_timeout")
        if args.reduced_capacity:
            reduced_worker_env(private_read(Path(worker["env_file"])), args.reduced_capacity)
        supervisor = {"role": "supervisor", "name": args.supervisor_name, "env_file": str(args.supervisor_env), "python": apis[0]["python"]}
        if not args.execute:
            print(json.dumps({"status": "dry_run", "run_id": args.run_id, "tasks": args.expected_tasks,
                "only_hard_fault": "worker-" + worker["name"], "engine_kept_alive": "engine-" + engine["name"],
                "watchdog_paused": True, "temporary_worker_capacity": args.reduced_capacity,
                "configuration_sha256": fingerprint, "requires_execute": True}))
            return 0
        stop = threading.Event()
        for signum in (signal.SIGTERM, signal.SIGINT):
            signal.signal(signum, lambda *_: stop.set())
        with (ROOT / ".runtime/single-pod-worker-fault.lock").open("w") as lock:
            os.chmod(lock.name, 0o600)
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            progress = WorkerProgress(env["DATABASE_URL"], args.run_id, args.model, wid)
            try:
                deadline = time.monotonic() + args.wait_for_submission
                while True:
                    counts = progress.submission_counts()
                    require(counts["total"] <= args.expected_tasks, "exact_run_contains_extra_tasks")
                    if counts["total"] == args.expected_tasks and len(progress.capture(wid)) >= args.minimum_running:
                        break
                    require(not stop.is_set() and time.monotonic() < deadline, "exact_run_not_armed_with_enough_valid_worker_leases")
                    stop.wait(.25)
                report = SinglePodProbe(services=services, worker=worker, engine=engine, api=apis[0], supervisor=supervisor,
                    manifest=manifest, original=original, progress=progress, controller=WorkerController(),
                    token=env["BATCHFORGE_WORKER_TOKEN"], expected=args.expected_tasks, minimum_running=args.minimum_running,
                    capacity=args.reduced_capacity, expiry_timeout=args.expiry_timeout, terminal_timeout=args.terminal_timeout,
                    drain_seconds=args.drain_seconds, engine_quiescence_timeout=args.engine_quiescence_timeout, stop=stop).run()
                report["executed_source_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
                report["dependency_sha256"] = {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in
                    ("ops/ha/engine_outage_probe.py", "ops/ha/controlplane_outage_probe.py", "ops/manage.py", "ops/supervise.py")}
                private_write(args.report, report)
                print(json.dumps({"status": report["status"], "report": str(args.report), "error": report.get("error"),
                    "final_restore_failed": report.get("final_restore_failed", False)}))
                return 0 if report["status"] == "passed" else 1
            finally:
                progress.close()
    except Exception as error:
        print(json.dumps({"status": "refused", "error": str(error) if isinstance(error, (ProbeFailure, ConfigError)) else type(error).__name__}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
