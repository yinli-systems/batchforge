"""Released-pair scope and restoration, without touching any production process."""
from datetime import datetime, timedelta, timezone
import hashlib
import json

import pytest

from ops.ha.engine_outage_probe import EngineProbe, ProbeFailure, selected_pair
from ops.ha.test_controlplane_outage_probe import ClockEvent, Controller, snapshot
from ops.ha.prepare_gptoss_outage import prepare


class Progress:
    model, run_id, secondary_worker_id = "same-100b-alias", "bench-exact-test", "secondary-exact"

    def __init__(self, event, *, capture=True, fail_during=False):
        self.event, self.has_capture, self.fail_during, self.calls = event, capture, fail_during, 0

    def scope(self, expected):
        if expected != 100:
            raise ProbeFailure("run_task_count_or_model_scope_mismatch")

    def secondary_ready(self):
        return True

    def snapshot(self):
        self.calls += 1
        if self.fail_during and self.calls > 1:
            raise RuntimeError("never-report-private-diagnostic")
        return snapshot(self.calls)

    def capture(self, worker_id):
        now = datetime.now(timezone.utc)
        return [{"id": "real-task-id", "worker_id": worker_id,
                 "attempt_token": "never-report-real-attempt-token", "captured_at": now,
                 "lease_expires_at": now+timedelta(seconds=90)}] if self.has_capture else []

    def held_after_loss(self, captured):
        return captured

    def recovery(self, captured):
        count = int(self.event.now >= 120)
        return {"captured": 1, "lease_expired": count, "reclaimed_by_exact_secondary": count,
                "expired_task_ids": ["real-task-id"] if count else [],
                "reclaimed_tasks": [{"id": "real-task-id", "accepted_worker": self.secondary_worker_id,
                    "accepted_count": 1, "prompt_tokens": 7, "completion_tokens": 2}] if count else []}


def fixture(tmp_path, *, capture=True, fail_during=False, fail_start_once=None):
    event = ClockEvent()
    services = []
    for role, name, port in [("api", "a", 8082), ("api", "b", 8083), ("gateway", "edge", 8080),
                              ("engine", "tp4", 8300), ("worker", "tp4", 9300),
                              ("engine", "untouched", 8101), ("worker", "untouched", 9102)]:
        env = tmp_path/f"{role}-{name}.env"
        env.write_text("CONFIG=private\n")
        env.chmod(0o600)
        services.append({"role": role, "name": name, "port": port,
                         "env_file": str(env), "python": "/unused/python"})
    worker, engine = selected_pair(services, "tp4", "tp4")
    env = tmp_path/"supervisor.env"
    env.write_text("CONFIG=private\n")
    env.chmod(0o600)
    supervisor = {"role": "supervisor", "name": "primary", "env_file": str(env), "python": "/unused/python"}
    manager = Controller(services, supervisor, fail_start_once=fail_start_once)
    manager.start_gate = lambda service: (True, None)
    manifest = tmp_path/"services.json"
    original = (json.dumps({"services": services})+'\n').encode()
    manifest.write_bytes(original)
    manifest.chmod(0o600)
    probe = EngineProbe(services=services, worker=worker, engine=engine, supervisor=supervisor, api=services[0],
        manifest=manifest, original=original, controller=manager,
        progress=Progress(event, capture=capture, fail_during=fail_during), worker_id="primary-exact",
        token="never-report-worker-token", expected_tasks=100, stop=event, clock=lambda: event.now, sleep=event.wait)
    return probe, manager, event


def test_only_released_pair_stops_and_same_task_secondary_reclaim_precedes_restore(tmp_path):
    probe, manager, event = fixture(tmp_path)
    before = dict(manager.live)
    report = probe.run()
    assert report["status"] == "passed"
    assert report["observed_pair_loss_seconds"] >= 120
    assert report["captured_tasks_reclaimed_by_secondary"] == 1
    assert report["expired_attempt_heartbeat_status"] == 409
    assert report["fenced_task_id"] == "real-task-id"
    stopped = [key for action, key in manager.calls if action == "stop"]
    assert stopped == ["supervisor-primary", "worker-tp4", "engine-tp4"]
    assert manager.calls.index(("fence", "api-a")) < manager.calls.index(("start", "engine-tp4"))
    assert manager.calls.index(("start", "engine-tp4")) < manager.calls.index(("start", "worker-tp4"))
    assert manager.calls[-1] == ("start", "supervisor-primary")
    assert all(manager.live[key] == value for key, value in before.items() if key not in stopped)
    assert "never-report" not in json.dumps(report) and "attempt_token" not in json.dumps(report)
    assert probe.manifest.read_bytes() == probe.original


def test_no_claimed_lease_or_wrong_run_count_refuses_without_stopping_anything(tmp_path):
    probe, manager, _ = fixture(tmp_path, capture=False)
    assert probe.run()["error"] == "no_valid_primary_lease_before_loss"
    assert manager.calls == []
    probe.expected = 999
    assert probe.run()["error"] == "run_task_count_or_model_scope_mismatch"
    assert manager.calls == []


def test_fenced_old_token_belongs_to_the_reclaimed_task_not_first_expired_task(tmp_path):
    probe, manager, event = fixture(tmp_path)
    capture, recovery, fence = probe.progress.capture, probe.progress.recovery, manager.fence
    def two_captured(worker_id):
        original = capture(worker_id)[0]
        return [{**original, "id": "expired-unreclaimed-first", "attempt_token": "never-report-other-token"}, original]
    def one_reclaimed(captured):
        result = recovery(captured)
        if result["lease_expired"]:
            result["lease_expired"] = 2
            result["expired_task_ids"].insert(0, "expired-unreclaimed-first")
        return result
    fenced = []
    def record_fence(api, token, candidate):
        fenced.append(candidate["id"])
        return fence(api, token, candidate)
    probe.progress.capture, probe.progress.recovery, manager.fence = two_captured, one_reclaimed, record_fence
    report = probe.run()
    assert report["status"] == "passed" and report["held_valid_leases_after_loss"] == 2
    assert report["recovery_samples"][-1]["expired_task_ids"][0] == "expired-unreclaimed-first"
    assert report["fenced_task_id"] == "real-task-id" and fenced == ["real-task-id"]
    assert report["expired_attempt_heartbeat_status"] == 409
    assert "never-report" not in json.dumps(report)


def test_observation_failure_during_loss_restores_pair_then_watchdog(tmp_path):
    probe, manager, _ = fixture(tmp_path, fail_during=True)
    report = probe.run()
    assert report["status"] == "failed" and report["error"] == "RuntimeError"
    assert "never-report" not in json.dumps(report)
    assert "engine-tp4" in manager.live and "worker-tp4" in manager.live and "supervisor-primary" in manager.live
    assert manager.calls[-3:] == [("start", "engine-tp4"), ("start", "worker-tp4"), ("start", "supervisor-primary")]


def test_partial_restore_is_retried_in_finally_and_interruption_restores_apps(tmp_path):
    probe, manager, event = fixture(tmp_path, fail_start_once="engine-tp4")
    report = probe.run()
    assert report["status"] == "failed" and report["error"] == "restore_incomplete"
    assert len(report["restore_attempts"]) == 2 and not report.get("final_restore_failed")
    assert manager.calls.count(("start", "engine-tp4")) == 2
    probe.progress.event.now = 0
    probe.progress.fail_during = True
    assert probe.run()["status"] == "failed"
    assert "supervisor-primary" in manager.live


def test_unavailable_restart_gate_never_starts_worker_and_finally_retries(tmp_path):
    probe, manager, event = fixture(tmp_path)
    checks = [0]
    def gate(service):
        checks[0] += 1
        return (False, "gpu_memory_not_released") if checks[0] <= 61 else (True, None)
    manager.start_gate = gate
    report = probe.run()
    assert report["status"] == "failed" and report["error"] == "restore_incomplete"
    assert report["restore_attempts"][0]["errors"] == ["engine_restore_failed", "worker_restore_deferred_until_engine_ready"]
    assert len(report["restore_attempts"]) == 2 and not report.get("final_restore_failed")
    assert manager.calls.count(("start", "engine-tp4")) == 1
    assert manager.calls.count(("start", "worker-tp4")) == 1
    assert report["engine_restart_gates"][-1]["allowed"] is True


def test_interrupt_during_down_window_restores_and_transient_stop_failure_is_bounded(tmp_path):
    probe, manager, event = fixture(tmp_path)
    invoke = manager.invoke
    failures = [0]
    def stop_once(service, action):
        if action == "stop" and service["role"] == "engine" and failures[0] == 0:
            failures[0] += 1
            raise ProbeFailure("injected_member_still_leaving")
        return invoke(service, action)
    manager.invoke = stop_once
    previous = event.wait
    def interrupt(duration):
        previous(duration)
        if duration == 2:
            event.stopped = True
    event.wait = interrupt
    report = probe.run()
    assert report["status"] == "failed" and report["error"] == "interrupted"
    assert failures[0] == 1
    assert all(key in manager.live for key in ("engine-tp4", "worker-tp4", "supervisor-primary"))


def test_bounded_preparation_preserves_frozen_body_without_submitting(tmp_path):
    body = {"model": "openai/gpt-oss-120b", "messages": [{"role": "user", "content": "public text"}], "max_tokens": 512}
    row = {"custom_id": "mix-0-128-512", "method": "POST", "url": "/v1/chat/completions", "body": body}
    payload = (json.dumps(row)+"\n").encode()
    source, metadata = tmp_path/"source.jsonl", tmp_path/"manifest.json"
    source.write_bytes(payload)
    document = {"model": body["model"], "workload_sha256": hashlib.sha256(payload).hexdigest(),
        "examples": {row["custom_id"]: {"target_prompt_tokens": 128, "rendered_prompt_tokens": 128, "output_token_cap": 512}},
        "prompt_counter": {"type": "frozen-test-double"}}
    metadata.write_text(json.dumps(document))
    destination = tmp_path/"prepared"
    report = prepare(source, metadata, destination)
    rows = [json.loads(line) for line in (destination/"workload.jsonl").read_text().splitlines()]
    assert report["submitted"] is False and len(rows) == report["tasks"] == 8
    assert len({item["custom_id"] for item in rows}) == 8
    assert all(item["body"] == body for item in rows)
    assert report["maximum_requested_generated_tokens_without_retries"] == 4096
    assert destination.stat().st_mode & 0o777 == 0o700
    assert all(path.stat().st_mode & 0o777 == 0o600 for path in destination.iterdir())
    with pytest.raises(FileExistsError):
        prepare(source, metadata, destination)
    source.write_bytes(payload+b"\n")
    with pytest.raises(ValueError, match="digest"):
        prepare(source, metadata, tmp_path/"must-not-create")
    assert not (tmp_path/"must-not-create").exists()
