"""Operator control-flow proof; real PostgreSQL state-machine tests are separate."""
from datetime import datetime, timedelta, timezone
import json
from io import BytesIO
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time
from urllib.error import HTTPError

import pytest

from ops.ha.controlplane_outage_probe import ProbeFailure
from ops.ha.single_pod_fault_probe import SinglePodProbe, WorkerController, reduced_worker_env
from ops.manage import process_identity
from backend.validation import validate_usage


class Clock:
    value = 0.0

    def __call__(self):
        return self.value

    def sleep(self, seconds):
        self.value += seconds


class Controller:
    def __init__(self, services, supervisor):
        self.services, self.supervisor = services, supervisor
        self.records = {self.key(row): {"pid": index + 10, "start_ticks": str(index + 100), "cwd": "/owned-app"}
                        for index, row in enumerate([*services, supervisor])}
        self.events, self.capacity, self.next_pid = [], 64, 100
        self.restarts, self.partial_start_error, self.stop_error = 0, False, False
        self.drained = False

    def key(self, row):
        return row["role"] + "-" + row["name"]

    def record(self, row):
        return self.records.get(self.key(row))

    def responding(self, row, ready=False):
        return self.record(row) is not None

    def invoke(self, row, action):
        name = self.key(row)
        self.events.append((name, action))
        if action == "stop":
            self.records.pop(name, None)
            if self.stop_error and row["role"] == "worker":
                self.stop_error = False
                raise RuntimeError("only-owned worker is still exiting")
        elif self.record(row) is None:
            self.next_pid += 1
            self.records[name] = {"pid": self.next_pid, "start_ticks": str(self.next_pid * 10), "cwd": "/owned-app"}
            if row["role"] == "worker":
                self.restarts += 1
                self.capacity = 2 if "singlepod-fault-" in row["env_file"] else 64
                if self.partial_start_error and self.capacity == 2:
                    self.partial_start_error = False
                    raise RuntimeError("start acknowledgement lost after identity record was written")
        return {"status": action}

    def crash_exact(self, row, expected):
        assert self.record(row) == expected
        self.events.append((self.key(row), "SIGKILL"))
        self.records.pop(self.key(row))

    def staged_record(self, row, run_id):
        return self.record(row)

    def graceful_stop(self, row, expected, grace, check):
        assert self.record(row) == expected
        check()
        self.drained = True
        self.invoke(row, "stop")
        return .25

    def fence_both(self, api, token, task):
        assert task["id"] == "task-0" and token == "private-worker-token"
        self.events.append((task["id"], "heartbeat409+complete409"))
        return {"heartbeat": 409, "complete": 409}


class Progress:
    run_id, model, secondary_worker_id = "bench-exact", "real-model", "owned-worker"

    def __init__(self, clock, controller):
        self.clock, self.controller, self.capacity_samples = clock, controller, 0
        self.never_expire, self.overrun, self.incomplete_reclaim = False, False, False
        self.high_samples, self.finish_at_low_capacity = 0, False
        now = datetime.now(timezone.utc)
        self.held = [{"id": f"task-{i}", "worker_id": self.secondary_worker_id,
                      "attempt_token": f"NEVER-PERSIST-token-{i}", "captured_at": now,
                      "lease_expires_at": now + timedelta(seconds=90)} for i in range(8)]

    def scope(self, expected):
        assert expected == 32

    def other_fresh_workers(self, wid):
        return 0

    def other_unfinished_work(self):
        return 0

    def capture(self, wid):
        if self.controller.restarts and self.controller.capacity == 2:
            return [] if self.finish_at_low_capacity else [{**r, "attempt_token": "NEW-" + r["attempt_token"]} for r in self.held[:2]]
        return self.held

    def held_after_loss(self, captured):
        return captured

    def expired(self, captured):
        done = self.clock() >= 1 and not self.never_expire
        return (captured if done else []), {"expired" if done else "running": len(captured)}

    def recovered_pending(self, captured):
        return len(captured) if self.expired(captured)[0] else 0

    def capacity(self, wid):
        return self.controller.capacity

    def accepted_token_count(self, tasks):
        return len(tasks) if self.controller.drained else 0

    def snapshot(self):
        if self.controller.restarts == 0:
            running, completed = (8, 0) if not self.expired(self.held)[0] else (0, 0)
        elif self.controller.capacity == 2:
            self.capacity_samples += 1
            if self.controller.drained:
                running, completed = 0, 32 if self.finish_at_low_capacity else 2
            elif self.finish_at_low_capacity and self.capacity_samples > 1:
                running, completed = 0, 32
            else:
                running, completed = (3 if self.overrun else 2), 0
        else:
            self.high_samples += 1
            running, completed = (8, 2) if self.controller.drained and not self.finish_at_low_capacity and self.high_samples == 1 else (0, 32)
        return {"total": 32, "pending": 32 - running - completed, "running": running,
                "completed": completed, "failed": 0, "cancelled": 0, "secondary_completed": completed}

    def recovery(self, held):
        count = len(held) - int(self.incomplete_reclaim)
        return {"reclaimed_by_exact_secondary": count,
                "reclaimed_tasks": [{"id": r["id"], "accepted_worker": self.secondary_worker_id,
                                     "accepted_count": 1, "prompt_tokens": 128, "completion_tokens": 64}
                                    for r in held[:count]]}

    def accepted_state(self, tid):
        return {"id": tid, "status": "completed", "accepted_count": 1, "prompt_tokens": 128, "completion_tokens": 64}


def setup(tmp_path, **extra):
    root = tmp_path.resolve()
    manifest = root / "services.json"
    manifest.write_text('{"services":[]}')
    worker_env, watch_env = root / "worker.env", root / "watch.env"
    worker_env.write_text("WORKER_ID=owned-worker\nENGINE_MAX_MODEL_LEN=18440\nWORKER_MAX_INFLIGHT=64\nWORKER_TOKEN_BUDGET=1180160\nBATCHFORGE_WORKER_TOKEN='private-worker-token'\n")
    watch_env.write_text("SUPERVISOR_MANIFEST=" + str(manifest) + "\n")
    for path in (manifest, worker_env, watch_env):
        path.chmod(0o600)
    services = [{"role": role, "name": name, "port": port, "env_file": str(worker_env), "python": sys.executable}
                for role, name, port in [("api", "a", 8082), ("api", "b", 8083),
                                         ("gateway", "edge", 8080), ("engine", "inference", 8300),
                                         ("worker", "inference", 9105)]]
    supervisor = {"role": "supervisor", "name": "primary", "env_file": str(watch_env), "python": sys.executable}
    controller, clock = Controller(services, supervisor), Clock()
    progress = Progress(clock, controller)
    probe = SinglePodProbe(services=services, worker=services[-1], engine=services[-2], api=services[0],
        supervisor=supervisor, manifest=manifest, original=manifest.read_bytes(), progress=progress,
        controller=controller, token="private-worker-token", expected=32, expiry_timeout=3,
        terminal_timeout=3, clock=clock, sleep=clock.sleep, **extra)
    return probe, progress, controller


def test_real_context_budget_is_preserved_in_reduced_worker_config():
    raw = b"ENGINE_MAX_MODEL_LEN=18440\nWORKER_MAX_INFLIGHT=64\nWORKER_TOKEN_BUDGET=1180160\nSECRET_TOKEN='literal value'\n"
    reduced, slots = reduced_worker_env(raw, 2)
    assert slots == 64 and b"WORKER_TOKEN_BUDGET=36880" in reduced
    assert b"SECRET_TOKEN='literal value'" in reduced and b"ENGINE_MAX_MODEL_LEN=18440" in reduced
    assert raw.count(b"WORKER_MAX_INFLIGHT=64") == 1
    with pytest.raises(ProbeFailure):
        reduced_worker_env(raw, 64)


def test_stale_completion_payload_passes_usage_validation_before_409_fence():
    calls = []
    class Opener:
        def open(self, request, timeout):
            body = json.loads(request.data)
            calls.append(request.full_url.rsplit("/", 1)[-1])
            if calls[-1] == "complete":
                assert validate_usage(body["response"]) == (0, 0)
                assert body["response"]["batchforge_probe"] == "stale_completion_must_be_rejected"
            raise HTTPError(request.full_url, 409, "Conflict", {},
                            BytesIO(b'{"error":{"code":"stale_attempt"}}'))
    controller = WorkerController()
    controller.http = Opener()
    result = controller.fence_both({"port": 8082}, "private", {"id": "t", "worker_id": "w", "attempt_token": "old"})
    assert result == {"heartbeat": 409, "complete": 409} and calls == ["heartbeat", "complete"]


def test_success_keeps_engine_and_controls_alive_restores_capacity_and_omits_tokens(tmp_path):
    probe, _, controller = setup(tmp_path)
    result = probe.run()
    assert result["status"] == "passed" and result["captured_tasks_reclaimed_once"] == 8
    assert result["stale_attempt_http_status"] == {"heartbeat": 409, "complete": 409}
    assert result["fenced_task_id"] == "task-0" and result["accepted_state_unchanged"]
    assert result["reduced_capacity_observed_with_pending_work"] and result["restored_server_capacity"] == 64
    assert result["reduced_positive_samples"] == 3 and result["capacity_reexpansion_proven"]
    assert result["drained_valid_attempts"] == 2 and result["graceful_drain_attempts_accepted"]
    assert "NEVER-PERSIST" not in json.dumps(result) and "private-worker-token" not in json.dumps(result)
    assert not any(name.startswith(("api-", "engine-", "gateway-")) for name, _ in controller.events)
    assert controller.record(probe.supervisor) and controller.record(probe.worker)


def test_expiry_timeout_still_restores_worker_and_watchdog(tmp_path):
    probe, progress, controller = setup(tmp_path)
    progress.never_expire = True
    result = probe.run()
    assert result["status"] == "failed" and result["error"] == "captured_leases_not_recovered_as_retryable_pending"
    assert not result.get("final_restore_failed") and controller.record(probe.worker) and controller.record(probe.supervisor)
    assert controller.capacity == 64


def test_partial_reduced_start_ack_failure_restores_original_worker(tmp_path):
    probe, _, controller = setup(tmp_path)
    controller.partial_start_error = True
    result = probe.run()
    assert result["status"] == "failed" and not result.get("final_restore_failed")
    assert controller.record(probe.worker) and controller.record(probe.supervisor) and controller.capacity == 64


def test_reduced_capacity_overrun_fails_and_restores_without_engine_restart(tmp_path):
    probe, progress, controller = setup(tmp_path)
    progress.overrun = True
    result = probe.run()
    assert result["status"] == "failed" and result["error"] == "restarted_worker_exceeded_reduced_capacity"
    assert not result.get("final_restore_failed") and controller.capacity == 64
    assert not any(name.startswith("engine-") for name, _ in controller.events)


def test_incomplete_exact_task_reclaim_cannot_pass_fencing_proof(tmp_path):
    probe, progress, controller = setup(tmp_path)
    progress.incomplete_reclaim = True
    result = probe.run()
    assert result["status"] == "failed" and result["error"] == "not_all_captured_tasks_reaccepted_once"
    assert not any(action == "heartbeat409+complete409" for _, action in controller.events)
    assert controller.record(probe.worker) and controller.record(probe.supervisor)


def test_optional_capacity_stage_can_be_disabled_and_restore_is_still_idempotent(tmp_path):
    probe, _, controller = setup(tmp_path, capacity=0)
    result = probe.run()
    assert result["status"] == "passed" and not result.get("final_restore_failed")
    assert controller.capacity == 64 and controller.record(probe.supervisor)


def test_partial_stop_error_retries_only_known_reduced_worker(tmp_path):
    probe, _, controller = setup(tmp_path)
    controller.stop_error = True
    result = probe.run()
    assert result["status"] == "failed" and controller.capacity == 64
    assert not result.get("final_restore_failed")


def test_exhausted_cohort_restores_but_does_not_claim_capacity_reexpansion(tmp_path):
    probe, progress, controller = setup(tmp_path)
    progress.finish_at_low_capacity = True
    result = probe.run()
    assert result["status"] == "passed" and controller.capacity == 64
    assert result["capacity_reduction_proven"] is False and result["capacity_reexpansion_proven"] is False
    assert result["capacity_reexpansion_limitation"] == "no_pending_work_remained_after_graceful_drain"


@pytest.mark.skipif(sys.platform != "linux", reason="Linux pidfd identity and SIGKILL proof")
def test_pidfd_kills_only_the_exact_owned_child():
    selected = subprocess.Popen([sys.executable, "-c", "import time;time.sleep(60)"])
    untouched = subprocess.Popen([sys.executable, "-c", "import time;time.sleep(60)"])
    try:
        expected = process_identity(selected.pid)
        class ExactController(WorkerController):
            def record(self, service):
                current = process_identity(selected.pid)
                return expected if current == expected else None
        controller = ExactController()
        with pytest.raises(ProbeFailure):
            controller.crash_exact({}, {**expected, "start_ticks": "wrong"})
        assert selected.poll() is None and untouched.poll() is None
        controller.crash_exact({}, expected)
        assert selected.wait(timeout=5) == -signal.SIGKILL and untouched.poll() is None
    finally:
        for child in (selected, untouched):
            if child.poll() is None:
                child.terminate()
            child.wait(timeout=5)


@pytest.mark.skipif(sys.platform != "linux", reason="Linux pidfd SIGTERM drain and timeout proof")
def test_pidfd_graceful_drain_waits_for_owned_exit_without_escalating(tmp_path):
    runtime = tmp_path / ".runtime"
    runtime.mkdir(mode=0o700)
    ready, drained = tmp_path / "ready", tmp_path / "drained"
    code = """
import signal, sys, time
from pathlib import Path
def drain(*args):
    time.sleep(.15)
    Path(sys.argv[2]).write_text('drained')
    raise SystemExit(0)
signal.signal(signal.SIGTERM, drain)
Path(sys.argv[1]).write_text('ready')
while True: time.sleep(.05)
"""
    child = subprocess.Popen([sys.executable, "-c", code, str(ready), str(drained)])
    untouched = subprocess.Popen([sys.executable, "-c", "import time;time.sleep(60)"])
    service = {"role": "worker", "name": "test"}
    try:
        deadline = time.monotonic() + 5
        while not ready.exists():
            assert time.monotonic() < deadline and child.poll() is None
            time.sleep(.01)
        expected = process_identity(child.pid)
        class ExactController(WorkerController):
            def record(self, service):
                current = process_identity(child.pid)
                return expected if current == expected else None
        controller = ExactController(root=tmp_path)
        pidfile = runtime / "worker-test.json"
        pidfile.write_text(json.dumps(expected))
        pidfile.chmod(0o600)
        with pytest.raises(ProbeFailure):
            controller.graceful_stop(service, {**expected, "start_ticks": "wrong"}, 5, lambda: None)
        elapsed = controller.graceful_stop(service, expected, 5, lambda: None)
        assert child.wait(timeout=5) == 0 and drained.read_text() == "drained"
        assert elapsed >= .15 and not pidfile.exists() and untouched.poll() is None
    finally:
        for process in (child, untouched):
            if process.poll() is None:
                process.kill()
            process.wait(timeout=5)

    # A worker which refuses TERM must remain alive when the bounded drain
    # times out. This primitive never escalates to SIGKILL on its own.
    ready.unlink()
    ignoring = subprocess.Popen([sys.executable, "-c",
        "import signal,sys,time;from pathlib import Path;signal.signal(signal.SIGTERM,signal.SIG_IGN);Path(sys.argv[1]).write_text('ready');time.sleep(60)", str(ready)])
    try:
        deadline = time.monotonic() + 5
        while not ready.exists():
            assert time.monotonic() < deadline and ignoring.poll() is None
            time.sleep(.01)
        child = ignoring
        expected = process_identity(ignoring.pid)
        pidfile.write_text(json.dumps(expected))
        pidfile.chmod(0o600)
        with pytest.raises(ProbeFailure, match="worker_drain_grace_expired"):
            controller.graceful_stop(service, expected, .05, lambda: None)
        assert ignoring.poll() is None and pidfile.exists()
    finally:
        if ignoring.poll() is None:
            ignoring.kill()
        ignoring.wait(timeout=5)
