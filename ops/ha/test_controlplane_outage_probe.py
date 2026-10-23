"""Restoration and scope invariants; no live production services are touched."""
from __future__ import annotations

import json

import pytest

from ops.ha.controlplane_outage_probe import Probe, ProbeFailure, make_plan, validate_snapshot, wait_for_submission


def services():
    return [{"role": role, "name": name, "port": port, "env_file": "/unused.env", "python": "/unused/python"}
            for role, name, port in [("api", "a", 8082), ("api", "b", 8083), ("gateway", "edge", 8080),
                                      ("link", "pod2", None), ("worker", "gpu0", 9101), ("engine", "gpu0", 8100)]]


class ClockEvent:
    def __init__(self):
        self.now, self.stopped = 0, False

    def is_set(self):
        return self.stopped

    def wait(self, duration):
        self.now += duration


def snapshot(completed=1):
    counts = {"pending": 100 - completed, "running": 0, "completed": completed, "failed": 0, "cancelled": 0}
    return {"observed_at": "2026-10-24T00:00:00+00:00", "batches": 1, "total": 100, **counts,
            **{"batch_" + key: value for key, value in counts.items()}, "secondary_completed": completed,
            "accepted_attempts": completed, "duplicate_accepted_tasks": 0,
            **{scope + "_" + kind + "_tokens": completed for scope in ("task", "accepted", "batch") for kind in ("prompt", "completion")}}


class Progress:
    def __init__(self, fail_on=None):
        self.samples, self.fail_on = 0, fail_on

    def snapshot(self):
        self.samples += 1
        if self.samples == self.fail_on:
            raise RuntimeError("private diagnostic that must never appear")
        return snapshot(self.samples)

    def inflight(self, prefix):
        return [{"id": "real-task-id", "worker_id": prefix + "gpu0", "attempt_token": "never-report-real-attempt-token"}]

    def expired(self, inflight):
        return inflight, {"expired": len(inflight)}


class Controller:
    def __init__(self, service_list, supervisor, fail_start_once=None):
        self.live = {self.key(item): {"pid": index + 100, "start_ticks": str(index), "cwd": "/application"}
                     for index, item in enumerate([*service_list, supervisor])}
        self.calls, self.fail_start_once, self.next_pid = [], fail_start_once, 500
        self.pending_crash = None

    @staticmethod
    def key(service):
        return service["role"] + "-" + service["name"]

    def record(self, service):
        key = self.key(service)
        if self.pending_crash == key and "supervisor-primary" in self.live:
            self.next_pid += 1
            self.live[key] = {"pid": self.next_pid, "start_ticks": "new", "cwd": "/application"}
            self.pending_crash = None
        return self.live.get(key)

    def invoke(self, service, action):
        key = self.key(service)
        self.calls.append((action, key))
        if action == "stop":
            self.live.pop(key, None)
        else:
            if self.fail_start_once == key:
                self.fail_start_once = None
                raise ProbeFailure("injected_start_failure")
            self.next_pid += 1
            self.live.setdefault(key, {"pid": self.next_pid, "start_ticks": "new", "cwd": "/application"})
        return {"status": "ok"}

    def responding(self, service, *, ready=False):
        if self.key(service) == "gateway-edge" and ready:
            return "gateway-edge" in self.live and any(key in self.live for key in ("api-a", "api-b"))
        return self.key(service) in self.live

    def fence(self, service, token, task):
        assert task["attempt_token"] == "never-report-real-attempt-token"
        assert token == "never-report-worker-token"
        self.calls.append(("fence", self.key(service)))

    def crash(self, service):
        key = self.key(service)
        self.calls.append(("crash", key))
        old = self.live.pop(key)
        self.pending_crash = key
        return old["pid"]


def probe(tmp_path, *, fail_on=None, fail_start_once=None):
    service_list = services()
    supervisor = {"role": "supervisor", "name": "primary", "env_file": "/unused.env", "python": "/unused/python"}
    original = json.dumps({"services": service_list}).encode()
    manifest = tmp_path / "services.json"
    manifest.write_bytes(original)
    manifest.chmod(0o600)
    manager = Controller(service_list, supervisor, fail_start_once)
    event = ClockEvent()
    instance = Probe(services=service_list, supervisor=supervisor, manifest=manifest, original=original,
        controller=manager, progress=Progress(fail_on), token="never-report-worker-token", duration=120,
        retire_link="pod2", stop=event, clock=lambda: event.now)
    return instance, manager, manifest


def test_success_keeps_gpu_processes_removes_only_explicit_link_and_proves_watchdog_restart(tmp_path):
    instance, manager, manifest = probe(tmp_path)
    original_gpu = {key: value for key, value in manager.live.items() if key.startswith(("worker-", "engine-"))}
    report = instance.run()
    assert report["status"] == "passed"
    assert report["observed_outage_seconds"] >= 120
    assert report["secondary_accepted_during_outage"] >= 5
    assert report["expired_real_attempt_heartbeat_status"] == 409
    assert report["watchdog_crash_old_pid"] != report["watchdog_crash_new_pid"]
    assert not any(item["role"] == "link" for item in json.loads(manifest.read_text())["services"])
    assert "link-pod2" not in manager.live
    assert all(manager.live[key] == value for key, value in original_gpu.items())
    assert not any(key.startswith(("engine-", "worker-")) for action, key in manager.calls)
    encoded = json.dumps(report)
    assert "never-report" not in encoded and "attempt_token" not in encoded


def test_database_failure_during_outage_always_restores_original_manifest_apps_and_watchdog(tmp_path):
    instance, manager, manifest = probe(tmp_path, fail_on=3)
    report = instance.run()
    assert report["status"] == "failed" and report["error"] == "RuntimeError"
    assert "private diagnostic" not in json.dumps(report)
    assert "supervisor-primary" in manager.live and "link-pod2" in manager.live
    assert all(manager.responding(item, ready=True) for item in instance.apis + [instance.gateway])
    assert json.loads(manifest.read_text()) == json.loads(instance.original)
    assert not any(action in {"fence", "crash"} for action, key in manager.calls)


def test_partial_restore_attempts_all_services_and_retries_original_topology_in_finally(tmp_path):
    instance, manager, manifest = probe(tmp_path, fail_start_once="api-b")
    report = instance.run()
    assert report["status"] == "failed" and report["error"] == "restore_incomplete"
    assert len(report["restore_attempts"]) == 2
    assert report["restore_attempts"][0]["errors"] == ["api_b_restore_failed"]
    assert not report.get("final_restore_failed")
    assert manager.calls.count(("start", "supervisor-primary")) == 2
    assert "link-pod2" in manager.live and "api-b" in manager.live
    assert json.loads(manifest.read_text()) == json.loads(instance.original)


def test_plan_refuses_wrong_api_topology_and_snapshot_rejects_duplicate_usage():
    altered = services()
    altered[0]["port"] = 9999
    with pytest.raises(ProbeFailure, match="requires_primary_api"):
        make_plan(altered, None)
    validate_snapshot(snapshot())
    for changes in ({"duplicate_accepted_tasks": 1}, {"batch_prompt_tokens": 2}, {"batch_pending": 999}, {"accepted_attempts": 2}):
        with pytest.raises(ProbeFailure):
            validate_snapshot({**snapshot(), **changes})


def test_unsubmitted_tasks_or_changed_manifest_refuse_before_stopping_any_service(tmp_path):
    instance, manager, manifest = probe(tmp_path)
    instance.expected_tasks = 8192
    assert instance.run()["error"] == "expected_real_workload_not_fully_submitted"
    assert not manager.calls
    instance.expected_tasks = 100
    manifest.write_text(json.dumps({"services": services()[:-1]}))
    assert instance.run()["error"] == "active_manifest_changed_after_validation"
    assert not manager.calls


def test_interrupted_outage_runs_finally_and_restores_original_services(tmp_path):
    instance, manager, manifest = probe(tmp_path)
    former_wait = instance.stop.wait

    def interrupted_wait(duration):
        former_wait(duration)
        instance.stop.stopped = True

    instance.stop.wait = interrupted_wait
    report = instance.run()
    assert report["status"] == "failed" and report["error"] == "interrupted"
    assert all(manager.responding(item, ready=True) for item in instance.apis + [instance.gateway])
    assert "link-pod2" in manager.live and "supervisor-primary" in manager.live
    assert json.loads(manifest.read_text()) == json.loads(instance.original)


def test_exact_run_submission_wait_is_readonly_and_never_arms_on_partial_or_finished_work():
    class SubmissionProgress:
        def __init__(self, counts):
            self.counts = iter(counts)
            self.primary_captures = 0

        def submission_counts(self):
            return next(self.counts)

        def inflight(self, _):
            self.primary_captures += 1
            return ["valid real lease"]

    event = ClockEvent()
    progress = SubmissionProgress([{"total": 64, "unfinished": 64}, {"total": 128, "unfinished": 100}])
    assert wait_for_submission(progress, 128, "primary-", 10, event, clock=lambda: event.now)["total"] == 128
    assert event.now == 2 and progress.primary_captures == 1
    for counts, reason in [({"total": 129, "unfinished": 100}, "more_than_expected"),
                           ({"total": 128, "unfinished": 0}, "already_finished"),
                           ({"total": 64, "unfinished": 64}, "before_timeout")]:
        with pytest.raises(ProbeFailure, match=reason):
            wait_for_submission(SubmissionProgress([counts]), 128, "primary-", 0, event, clock=lambda: event.now)
