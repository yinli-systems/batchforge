#!/usr/bin/env python3
"""Controlled app-only control-plane outage; dry-run unless --execute is set.

Run locally on the primary Linux Pod during an operator-started real workload.
Never stops workers, inference engines, PostgreSQL, or unrelated processes.
Database observations use the existing runtime role in read-only transactions.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import fcntl
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
from urllib.request import ProxyHandler, Request, build_opener

import psycopg
from psycopg.rows import dict_row

# Allow both `python -m ...` and the documented direct-script invocation.
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from ops import manage
from ops.supervise import ConfigError, load_manifest, private_read, private_write


def timestamp():
    return datetime.now(timezone.utc).isoformat()


class ProbeFailure(RuntimeError):
    """A bounded public reason; never includes a driver/HTTP diagnostic."""


def require(condition, reason):
    if not condition:
        raise ProbeFailure(reason)


SNAPSHOT = """
WITH chosen AS MATERIALIZED (
 SELECT id,pending_count,running_count,completed_count,failed_count,cancelled_count,
        prompt_tokens,completion_tokens FROM batches WHERE metadata->>'benchmark_run'=%s
), t AS MATERIALIZED (SELECT t.status,t.prompt_tokens,t.completion_tokens,t.worker_id
 FROM tasks t JOIN chosen b ON b.id=t.batch_id), a AS MATERIALIZED (
 SELECT a.task_id,a.status,a.prompt_tokens,a.completion_tokens FROM attempts a JOIN chosen b ON b.id=a.batch_id
), counts AS (
 SELECT count(*) AS total,count(*) FILTER(WHERE status='pending') AS pending,
 count(*) FILTER(WHERE status='running') AS running,count(*) FILTER(WHERE status='completed') AS completed,
 count(*) FILTER(WHERE status='failed') AS failed,count(*) FILTER(WHERE status='cancelled') AS cancelled,
 count(*) FILTER(WHERE status='completed' AND left(worker_id,length(%s))=%s) AS secondary_completed,
 COALESCE(sum(prompt_tokens),0) AS task_prompt_tokens,COALESCE(sum(completion_tokens),0) AS task_completion_tokens FROM t
), usage AS (
 SELECT count(*) FILTER(WHERE status='completed') AS accepted_attempts,
 count(*) FILTER(WHERE status='expired') AS expired_attempts,
 count(*) FILTER(WHERE status='running') AS running_attempts,
 COALESCE(sum(prompt_tokens) FILTER(WHERE status='completed'),0) AS accepted_prompt_tokens,
 COALESCE(sum(completion_tokens) FILTER(WHERE status='completed'),0) AS accepted_completion_tokens FROM a
), duplicate AS (SELECT count(*) AS duplicate_accepted_tasks FROM (
 SELECT task_id FROM a WHERE status='completed' GROUP BY task_id HAVING count(*)>1) d),
 batches AS (SELECT count(*) AS batches,
 COALESCE(sum(pending_count),0) AS batch_pending,COALESCE(sum(running_count),0) AS batch_running,
 COALESCE(sum(completed_count),0) AS batch_completed,COALESCE(sum(failed_count),0) AS batch_failed,
 COALESCE(sum(cancelled_count),0) AS batch_cancelled,
 COALESCE(sum(prompt_tokens),0) AS batch_prompt_tokens,COALESCE(sum(completion_tokens),0) AS batch_completion_tokens FROM chosen)
SELECT clock_timestamp() AS observed_at,counts.*,usage.*,duplicate.*,batches.* FROM counts,usage,duplicate,batches
"""


def validate_snapshot(snapshot):
    require(snapshot["batches"] > 0, "run_not_found")
    for state in ("pending", "running", "completed", "failed", "cancelled"):
        require(snapshot[state] == snapshot["batch_" + state], "task_batch_counter_mismatch")
    require(snapshot["total"] == sum(snapshot[state] for state in ("pending", "running", "completed", "failed", "cancelled")), "task_total_mismatch")
    require(snapshot["completed"] == snapshot["accepted_attempts"] and not snapshot["duplicate_accepted_tasks"], "duplicate_or_missing_accepted_usage")
    for kind in ("prompt", "completion"):
        require(snapshot[f"task_{kind}_tokens"] == snapshot[f"accepted_{kind}_tokens"] == snapshot[f"batch_{kind}_tokens"], "accepted_token_usage_mismatch")


class Progress:
    def __init__(self, database_url, run_id, secondary_prefix):
        self.connection = psycopg.connect(database_url, autocommit=True, row_factory=dict_row, connect_timeout=3,
            application_name="batchforge-outage-probe", options="-c default_transaction_read_only=on -c statement_timeout=3000 -c lock_timeout=500")
        self.run_id, self.secondary_prefix = run_id, secondary_prefix

    def snapshot(self):
        row = self.connection.execute(SNAPSHOT, (self.run_id, self.secondary_prefix, self.secondary_prefix)).fetchone()
        snapshot = {key: value.isoformat() if isinstance(value, datetime) else int(value) for key, value in row.items()}
        validate_snapshot(snapshot)
        return snapshot

    def submission_counts(self):
        row = self.connection.execute("SELECT COALESCE(sum(total_count),0) AS total,COALESCE(sum(pending_count+running_count),0) AS unfinished FROM batches WHERE metadata->>'benchmark_run'=%s", (self.run_id,)).fetchone()
        return {key: int(value) for key, value in row.items()}

    def inflight(self, primary_prefix):
        # Actual lease tokens remain in RAM only: no report, log, or output.
        return self.connection.execute("WITH captured AS MATERIALIZED (SELECT clock_timestamp() AS captured_at) SELECT t.id,t.worker_id,t.attempt_token,t.lease_expires_at,captured.captured_at FROM tasks t JOIN batches b ON b.id=t.batch_id CROSS JOIN captured WHERE b.metadata->>'benchmark_run'=%s AND t.status='running' AND t.lease_expires_at>captured.captured_at AND left(t.worker_id,length(%s))=%s ORDER BY t.id LIMIT 256", (self.run_id, primary_prefix, primary_prefix)).fetchall()

    def expired(self, inflight):
        rows = self.connection.execute("SELECT token,status,error->>'code'='lease_expired' AS lease_expired FROM attempts WHERE token=ANY(%s)", ([item["attempt_token"] for item in inflight],)).fetchall()
        expired_tokens = {row["token"] for row in rows if row["status"] in {"expired", "failed"} and row["lease_expired"]}
        expired = [item for item in inflight if item["attempt_token"] in expired_tokens]
        return expired, {state: sum(row["status"] == state for row in rows) for state in sorted({row["status"] for row in rows})}

    def close(self):
        self.connection.close()


def wait_for_submission(progress, expected_tasks, primary_prefix, timeout, stop, *, clock=time.monotonic):
    """Only an explicitly selected run may arm; no 'latest run' inference."""
    deadline = clock() + timeout
    while True:
        require(not stop.is_set(), "interrupted_before_maintenance")
        counts = progress.submission_counts()
        require(counts["total"] <= expected_tasks, "run_contains_more_than_expected_tasks")
        if counts["total"] == expected_tasks:
            require(counts["unfinished"] >= 6, "real_workload_already_finished_or_too_small")
            if progress.inflight(primary_prefix):
                return counts
        require(clock() < deadline, "exact_run_not_fully_submitted_with_valid_primary_lease_before_timeout")
        stop.wait(min(2, max(0, deadline - clock())))


def make_plan(services, retire_link):
    apis = sorted((item for item in services if item["role"] == "api"), key=lambda item: item["name"])
    gateways = [item for item in services if item["role"] == "gateway"]
    require([(item["name"], item["port"]) for item in apis] == [("a", 8082), ("b", 8083)], "requires_primary_api_a_b_ports_8082_8083")
    require([(item["name"], item["port"]) for item in gateways] == [("edge", 8080)], "requires_primary_gateway_edge_8080")
    links = [item for item in services if item["role"] == "link" and item["name"] == retire_link]
    require(retire_link is None or len(links) == 1, "retired_link_not_in_active_manifest")
    return apis, gateways[0], links


class Controller:
    def __init__(self, root=ROOT):
        self.root = root
        self.http = build_opener(ProxyHandler({}))

    def record(self, service):
        return manage.owned_record(self.root / ".runtime" / f"{service['role']}-{service['name']}.json")

    def invoke(self, service, action):
        arguments = [sys.executable, str(self.root / "ops/manage.py"), action, service["role"], "--name", service["name"]]
        if action == "stop":
            arguments += ["--grace-seconds", "3"]
        if action == "start":
            arguments += ["--env-file", service["env_file"], "--python", service["python"]]
            for field in ("port", "gpu"):
                if service.get(field) is not None:
                    arguments += [f"--{field}", str(service[field])]
        # Never echo subprocess stderr: configuration/driver diagnostics can
        # contain secrets. manage.py independently verifies PID identity.
        result = subprocess.run(arguments, cwd=self.root, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=15, check=False)
        require(result.returncode == 0 and len(result.stdout) <= 4096, "manager_" + action + "_failed")
        document = json.loads(result.stdout)
        require(isinstance(document, dict), "manager_protocol_error")
        return document

    def responding(self, service, *, ready=False):
        try:
            with self.http.open(f"http://127.0.0.1:{service['port']}/readyz", timeout=1) as response:
                return response.status == 200 if ready else True
        except HTTPError:
            return not ready  # Any HTTP response proves the listener is alive.
        except (URLError, TimeoutError, OSError):
            return False

    def fence(self, service, token, task):
        request = Request(f"http://127.0.0.1:{service['port']}/internal/tasks/{task['id']}/heartbeat",
            data=json.dumps({"worker_id": task["worker_id"], "attempt_token": task["attempt_token"]}).encode(),
            headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"}, method="POST")
        try:
            with self.http.open(request, timeout=5):
                raise ProbeFailure("expired_attempt_heartbeat_was_accepted")
        except HTTPError as error:
            require(error.code == 409, "expired_attempt_not_fenced_409")
            # Do not log HTTP bodies; checking the bounded error code is enough.
            require(json.loads(error.read(4096)).get("error", {}).get("code") == "stale_attempt", "unexpected_409_error")

    def crash(self, service):
        require(hasattr(os, "pidfd_open") and hasattr(signal, "pidfd_send_signal"), "linux_pidfd_required")
        before = self.record(service)
        require(before is not None, "crash_target_not_running")
        descriptor = os.pidfd_open(before["pid"])
        try:
            # Pin the process first, then compare exact start-time/cwd identity.
            require(self.record(service) == before, "crash_target_identity_changed")
            signal.pidfd_send_signal(descriptor, signal.SIGKILL)
        finally:
            os.close(descriptor)
        return before["pid"]


class Probe:
    def __init__(self, *, services, supervisor, manifest, original, controller, progress, token, duration=120,
                 minimum_secondary=5, expected_tasks=None, primary_prefix="primary-qwen7b-", retire_link=None, stop=None, clock=time.monotonic):
        self.services, self.supervisor, self.manifest, self.original = services, supervisor, manifest, original
        self.controller, self.progress, self.token = controller, progress, token
        self.duration, self.minimum_secondary, self.primary_prefix = duration, minimum_secondary, primary_prefix
        self.expected_tasks = expected_tasks
        self.apis, self.gateway, self.links = make_plan(services, retire_link)
        self.retire_link, self.stop, self.clock = retire_link, stop or threading.Event(), clock
        self.report = {"schema_version": 1, "started_at": timestamp(), "status": "running", "samples": [],
            "scope": "app control-plane outage simulation; not whole-Pod power loss or managed-database failover",
            "measurement_bounds": ["Only this primary application's two API replicas, gateway and explicitly selected obsolete link are stopped",
                "Workers and inference engines stay alive; preserved app PID identities are checked before and after",
                "Secondary progress is accepted durable completions attributed by worker ID prefix in this benchmark run",
                "Database snapshots use one read-only SQL statement each; no synthetic tasks or completion writes are generated",
                "Readiness/listener checks are periodic observations; they cannot prove uninterrupted unavailability between samples",
                "The probe sends a heartbeat with one real expired attempt solely to verify its 409 fence; tokens never leave RAM/reports omit them"]}

    def wait(self, predicate, timeout, reason, *, interruptible=True):
        deadline = self.clock() + timeout
        while not predicate():
            if interruptible and self.stop.is_set():
                raise ProbeFailure("interrupted")
            require(self.clock() < deadline, reason)
            self.stop.wait(.25) if interruptible else time.sleep(.25)

    def sample(self, phase):
        value = {"phase": phase, **self.progress.snapshot()}
        self.report["samples"].append(value)
        return value

    def restore(self, *, retire=False):
        errors = []
        # If a previous partial restore started watchdog, pause it again before
        # restoring the original manifest/selected link; leave all apps alive.
        for service in [self.supervisor]:
            try:
                self.controller.invoke(service, "stop")
            except Exception:
                errors.append("watchdog_pause_failed")
        try:
            document = json.loads(self.original)
            if retire:
                document["services"] = [item for item in document["services"] if not (item["role"] == "link" and item["name"] == self.retire_link)]
            private_write(self.manifest, document)
        except Exception:
            errors.append("manifest_restore_failed")
        for service in [*self.apis, self.gateway, *([] if retire else self.links)]:
            try:
                self.controller.invoke(service, "start")
                if service.get("port") is not None:
                    self.wait(lambda: self.controller.responding(service, ready=True), 40, "service_not_ready", interruptible=False)
            except Exception:
                errors.append(f"{service['role']}_{service['name']}_restore_failed")
        # Attempt watchdog restoration even if another service failed. It can
        # subsequently recover an app after transient database unavailability.
        try:
            self.controller.invoke(self.supervisor, "start")
            require(self.controller.record(self.supervisor) is not None, "watchdog_not_running")
        except Exception:
            errors.append("watchdog_restore_failed")
        self.report.setdefault("restore_attempts", []).append({"at": timestamp(), "retired_link": self.retire_link if retire else None, "errors": errors})
        require(not errors, "restore_incomplete")

    def run(self):
        completed, maintenance_started = False, False
        preserved = {f"{item['role']}-{item['name']}": self.controller.record(item) for item in self.services if item["role"] in {"worker", "engine"}}
        try:
            require(all(preserved.values()), "worker_or_engine_not_running")
            require(self.controller.record(self.supervisor) is not None, "watchdog_not_running")
            require(all(self.controller.record(item) and self.controller.responding(item, ready=True) for item in [*self.apis, self.gateway]), "initial_controlplane_not_ready")
            before = self.sample("before")
            require(self.expected_tasks is None or before["total"] == self.expected_tasks, "expected_real_workload_not_fully_submitted")
            require(before["pending"] + before["running"] >= self.minimum_secondary + 1, "insufficient_remaining_real_work")
            require(private_read(self.manifest) == self.original, "active_manifest_changed_after_validation")
            require(not self.stop.is_set(), "interrupted")
            maintenance_started = True
            self.controller.invoke(self.supervisor, "stop")
            require(self.controller.record(self.supervisor) is None, "watchdog_did_not_stop")
            for service in [self.gateway, *self.apis, *self.links]:
                self.controller.invoke(service, "stop")
            controls = [self.gateway, *self.apis]
            unavailable = lambda: not any(self.controller.responding(item) for item in controls)
            self.wait(unavailable, 15, "controlplane_listener_still_available")
            self.report["all_primary_urls_unavailable_at"] = timestamp()
            outage_start = self.clock()
            inflight = self.progress.inflight(self.primary_prefix)
            require(inflight, "no_primary_inflight_leases_to_test")
            self.report["captured_primary_inflight_leases"] = len(inflight)
            expiries = [item["lease_expires_at"] for item in inflight if item.get("lease_expires_at") is not None]
            self.report["captured_primary_lease_expiry_bounds"] = [min(expiries).isoformat(), max(expiries).isoformat()] if expiries else None
            captured_at = [item["captured_at"] for item in inflight if item.get("captured_at") is not None]
            self.report["valid_leases_captured_at_db_clock"] = min(captured_at).isoformat() if captured_at else None
            first = self.sample("outage_start")
            while self.clock() - outage_start < self.duration:
                require(not self.stop.is_set(), "interrupted")
                require(unavailable(), "primary_controlplane_became_available_during_outage")
                self.stop.wait(min(2, max(0, self.duration - (self.clock() - outage_start))))
                self.sample("during")
            require(unavailable(), "primary_controlplane_available_at_outage_end")
            last = self.sample("outage_end")
            self.report["all_primary_urls_unavailable_through"] = timestamp()
            self.report["observed_outage_seconds"] = self.clock() - outage_start
            self.report["secondary_accepted_during_outage"] = last["secondary_completed"] - first["secondary_completed"]
            require(self.report["secondary_accepted_during_outage"] >= self.minimum_secondary, "insufficient_durable_secondary_progress")
            expired, states = self.progress.expired(inflight)
            self.report["captured_attempt_status_counts_at_outage_end"] = states
            require(expired, "captured_leases_did_not_expire")
            self.restore(retire=self.retire_link is not None)
            require(not self.stop.is_set(), "interrupted")
            self.controller.fence(self.apis[0], self.token, expired[0])
            self.report["expired_real_attempt_heartbeat_status"] = 409
            self.sample("restored")
            old_pid = self.controller.crash(self.apis[0])
            self.report["watchdog_crash_started_at"] = timestamp()

            def restarted():
                # The surviving API keeps the gateway ready throughout polling.
                require(self.controller.responding(self.gateway, ready=True), "gateway_lost_readiness_during_single_api_crash")
                current = self.controller.record(self.apis[0])
                return current and current["pid"] != old_pid and self.controller.responding(self.apis[0], ready=True)

            self.wait(restarted, 100, "watchdog_did_not_recover_crashed_api")
            self.report["watchdog_crash_recovered_at"] = timestamp()
            self.report["watchdog_crash_old_pid"] = old_pid
            self.report["watchdog_crash_new_pid"] = self.controller.record(self.apis[0])["pid"]
            after = {f"{item['role']}-{item['name']}": self.controller.record(item) for item in self.services if item["role"] in {"worker", "engine"}}
            require(after == preserved, "worker_or_engine_identity_changed")
            self.report["preserved_worker_engine_pids"] = {name: record["pid"] for name, record in after.items()}
            self.sample("after_watchdog_recovery")
            self.report["status"], completed = "passed", True
        except BaseException as error:
            self.report["status"] = "failed"
            self.report["error"] = str(error) if isinstance(error, ProbeFailure) else type(error).__name__
        finally:
            if maintenance_started and not completed:
                try:
                    self.restore(retire=False)
                except BaseException:
                    self.report["final_restore_failed"] = True
            self.report["finished_at"] = timestamp()
        return self.report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=ROOT / ".runtime/services-primary.json")
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--supervisor-env", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--expected-tasks", type=int, required=True, help="Reject stopping APIs until all expected real tasks are durably submitted")
    parser.add_argument("--secondary-worker-prefix", default="secondpod-")
    parser.add_argument("--primary-worker-prefix", default="primary-qwen7b-")
    parser.add_argument("--duration", type=int, default=120)
    parser.add_argument("--minimum-secondary-completions", type=int, default=5)
    parser.add_argument("--wait-for-submission-seconds", type=int, default=0, help="After explicit --execute, wait read-only for this exact run's task count and a valid primary lease before maintenance")
    parser.add_argument("--retire-link", help="Stop and remove only this obsolete app link after success; restore it on failure")
    parser.add_argument("--report", type=Path, default=ROOT / ".runtime/controlplane-outage-report.json")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    try:
        require(Path("/proc/self/stat").exists(), "linux_required")
        require(bool(re.fullmatch(r"bench-[a-zA-Z0-9_-]{1,64}", args.run_id)), "invalid_benchmark_run_id")
        require(105 <= args.duration <= 300 and 5 <= args.minimum_secondary_completions <= 1000, "invalid_probe_bounds")
        require(1 <= args.expected_tasks <= 1_000_000, "invalid_expected_tasks")
        require(0 <= args.wait_for_submission_seconds <= 600, "invalid_submission_wait_bound")
        require(all(re.fullmatch(r"[a-zA-Z0-9_-]{1,100}", value) for value in (args.secondary_worker_prefix, args.primary_worker_prefix)), "invalid_worker_prefix")
        require(not args.secondary_worker_prefix.startswith(args.primary_worker_prefix) and not args.primary_worker_prefix.startswith(args.secondary_worker_prefix), "worker_prefixes_overlap")
        manifest, services, fingerprint = load_manifest(args.manifest)
        original = private_read(manifest)
        make_plan(services, args.retire_link)
        # Reuse the manifest's strict path/permissions checks for both env files.
        allowed = {ROOT, ROOT.parent}
        for path in (args.env_file, args.supervisor_env):
            require(path.is_absolute() and path.parent in allowed and path.resolve() == path, "configuration_path_outside_app_roots")
            private_read(path)
        require(args.report.parent == ROOT / ".runtime" and args.report.suffix == ".json" and args.report.resolve() == args.report, "report_must_be_app_runtime_json")
        require(args.report != manifest and not args.report.exists(), "report_must_not_overwrite_existing_file")
        environment = manage.load_env(args.env_file)
        require(args.env_file == Path(next(item["env_file"] for item in services if item["role"] == "api" and item["name"] == "a")), "must_use_api_a_runtime_configuration")
        require("DATABASE_URL" in environment and "BATCHFORGE_WORKER_TOKEN" in environment, "runtime_credentials_missing")
        supervisor_environment = manage.load_env(args.supervisor_env)
        require(supervisor_environment.get("SUPERVISOR_MANIFEST") == str(manifest), "supervisor_manifest_mismatch")
        lease_seconds = int(environment.get("LEASE_SECONDS", "90"))
        require(args.duration >= lease_seconds + 15, "outage_must_exceed_lease_and_recovery_margin")
        supervisor = {"role": "supervisor", "name": "primary", "python": next(item["python"] for item in services if item["role"] == "api"), "env_file": str(args.supervisor_env)}
        if not args.execute:
            print(json.dumps({"status": "dry_run", "run_id": args.run_id, "configuration_sha256": fingerprint,
                "duration_seconds": args.duration, "stopped_services": ["supervisor-primary", "gateway-edge", "api-a", "api-b"] + (["link-" + args.retire_link] if args.retire_link else []),
                "expected_tasks": args.expected_tasks, "wait_for_submission_seconds": args.wait_for_submission_seconds,
                "preserved_services": sum(item["role"] in {"worker", "engine"} for item in services), "requires_execute": True}))
            return 0
        stop = threading.Event()
        for signum in (signal.SIGTERM, signal.SIGINT):
            signal.signal(signum, lambda *_: stop.set())
        with (ROOT / ".runtime/controlplane-outage.lock").open("w") as lock:
            os.chmod(lock.name, 0o600)
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            progress = Progress(environment["DATABASE_URL"], args.run_id, args.secondary_worker_prefix)
            try:
                if args.wait_for_submission_seconds:
                    wait_for_submission(progress, args.expected_tasks, args.primary_worker_prefix, args.wait_for_submission_seconds, stop)
                report = Probe(services=services, supervisor=supervisor, manifest=manifest, original=original,
                    controller=Controller(), progress=progress, token=environment["BATCHFORGE_WORKER_TOKEN"],
                    duration=args.duration, minimum_secondary=args.minimum_secondary_completions, expected_tasks=args.expected_tasks,
                    primary_prefix=args.primary_worker_prefix, retire_link=args.retire_link, stop=stop).run()
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
