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


@pytest.mark.parametrize("flag", ["true", "1"])
def test_adaptive_reduced_capacity_restores_claim_slots_and_block_rounds_budget(flag):
    raw = ("ENGINE_MAX_MODEL_LEN=18440\nWORKER_MAX_INFLIGHT=64\n"
           "WORKER_TOKEN_BUDGET=298384\nWORKER_ADAPTIVE_KV_ENABLED=" + flag + "\n").encode()
    reduced, original = reduced_worker_env(raw, 2)
    assert original == 64  # Full-context static division would incorrectly say16.
    assert b"WORKER_MAX_INFLIGHT=2\n" in reduced
    assert b"WORKER_TOKEN_BUDGET=36896\n" in reduced
    assert ("WORKER_ADAPTIVE_KV_ENABLED=" + flag).encode() in reduced


def test_reduced_capacity_rejects_unknown_adaptive_mode():
    with pytest.raises(ProbeFailure, match="invalid_adaptive_capacity_flag"):
        reduced_worker_env(b"WORKER_ADAPTIVE_KV_ENABLED=maybe\n", 2)


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
        self.engine_idle_at = 0
        self.quiescence_errors = []
        self.adaptive_startup_requires_idle = False
        self.worker_start_times = []

    def key(self, row):
        return row["role"] + "-" + row["name"]

    def record(self, row):
        return self.records.get(self.key(row))

    def responding(self, row, ready=False):
        if ready and row['role']=='worker' and self.adaptive_startup_requires_idle and self.restarts:
            return self.record(row) is not None and self.clock()>=self.engine_idle_at
        return self.record(row) is not None

    def engine_quiescence(self, row):
        assert row['role']=='engine'
        if self.quiescence_errors:
            return {'quiescent':False,'error':self.quiescence_errors.pop(0)}
        return {'quiescent':self.clock()>=self.engine_idle_at,'error':None}

    def invoke(self, row, action):
        name = self.key(row)
        self.events.append((name, action))
        if action == "stop":
            self.records.pop(name, None)
            if self.stop_error and row["role"] == "worker":
                self.stop_error = False
                raise RuntimeError("only-owned worker is still exiting")
            return {'status': 'stopped'}
        elif self.record(row) is None:
            self.next_pid += 1
            self.records[name] = {"pid": self.next_pid, "start_ticks": str(self.next_pid * 10), "cwd": "/owned-app"}
            if row["role"] == "worker":
                self.worker_start_times.append(self.clock())
                self.restarts += 1
                self.capacity = 2 if "singlepod-fault-" in row["env_file"] else 64
                if self.partial_start_error and self.capacity == 2:
                    self.partial_start_error = False
                    raise RuntimeError("start acknowledgement lost after identity record was written")
            return {'status': 'started', 'pid': self.record(row)['pid']}
        return {'status': 'already_running', 'pid': self.record(row)['pid']}

    def crash_exact(self, row, expected):
        assert self.record(row) == expected
        self.events.append((self.key(row), "SIGKILL"))
        self.records.pop(self.key(row))

    def staged_record(self, row, run_id):
        return self.record(row)

    def graceful_stop(self, row, expected, grace, check):
        assert self.record(row) == expected
        check()
        if row['role'] == 'worker':
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


def setup(tmp_path, adaptive=False, worker_shutdown_seconds=None, **extra):
    root = tmp_path.resolve()
    (root / '.runtime').mkdir(mode=0o700)
    manifest = root / "services.json"
    manifest.write_text('{"services":[]}')
    worker_env, watch_env = root / "worker.env", root / "watch.env"
    worker_env.write_text("WORKER_ID=owned-worker\nENGINE_MAX_MODEL_LEN=18440\nWORKER_MAX_INFLIGHT=64\nWORKER_TOKEN_BUDGET=1180160\nBATCHFORGE_WORKER_TOKEN='private-worker-token'\n")
    if adaptive:
        worker_env.write_text(worker_env.read_text().replace("WORKER_TOKEN_BUDGET=1180160", "WORKER_TOKEN_BUDGET=298384")+"WORKER_ADAPTIVE_KV_ENABLED=true\n")
    if worker_shutdown_seconds is not None:
        worker_env.write_text(worker_env.read_text() + 'WORKER_SHUTDOWN_SECONDS=' + str(worker_shutdown_seconds) + '\n')
    watch_env.write_text("SUPERVISOR_MANIFEST=" + str(manifest) + "\n")
    for path in (manifest, worker_env, watch_env):
        path.chmod(0o600)
    services = [{"role": role, "name": name, "port": port, "env_file": str(worker_env), "python": sys.executable}
                for role, name, port in [("api", "a", 8082), ("api", "b", 8083),
                                         ("gateway", "edge", 8080), ("engine", "inference", 8300),
                                         ("worker", "inference", 9105)]]
    supervisor = {"role": "supervisor", "name": "primary", "env_file": str(watch_env), "python": sys.executable}
    controller, clock = Controller(services, supervisor), Clock()
    controller.root = root
    controller.clock = clock
    controller.adaptive_startup_requires_idle = adaptive
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


def test_partial_reduced_start_ack_failure_is_manual_without_watchdog_restore(tmp_path):
    probe, _, controller = setup(tmp_path)
    controller.partial_start_error = True
    result = probe.run()
    assert result['status'] == 'failed' and result['final_restore_failed']
    assert result['manual_recovery_required'] and result['pending_action']['stage'] == 'worker_start'
    assert controller.record(probe.worker) and controller.capacity == 2
    assert not controller.record(probe.supervisor)
    assert controller.restarts == 1


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


def test_partial_stop_error_is_not_replayed_or_followed_by_watchdog_restore(tmp_path):
    probe, _, controller = setup(tmp_path)
    controller.stop_error = True
    result = probe.run()
    assert result['status'] == 'failed' and result['manual_recovery_required']
    assert result['pending_action']['stage'] == 'reduced_worker_drain'
    assert controller.restarts == 1 and not controller.record(probe.supervisor)


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


def test_adaptive_probe_waits_for_engine_idle_and_retries_metrics_before_any_worker_restart(tmp_path):
    probe,_,controller=setup(tmp_path,adaptive=True,engine_quiescence_timeout=10)
    controller.engine_idle_at=4
    controller.quiescence_errors=['metrics_http_error','metrics_unavailable']
    result=probe.run()
    assert result['status']=='passed' and not result.get('final_restore_failed')
    gate=result['engine_quiescence_gates'][0]
    assert gate['status']=='passed' and gate['wait_seconds']>=3
    assert gate['errors']=={'metrics_http_error':1,'metrics_unavailable':1}
    assert gate['busy_observations']>0 and gate['observations'][-1]['quiescent'] is True
    assert controller.worker_start_times and min(controller.worker_start_times)>=controller.engine_idle_at
    assert result['original_configured_slots']==result['restored_server_capacity']==64
    assert result['adaptive_engine_startup_requires_quiescence']
    assert not any(name.startswith(('api-','engine-','gateway-')) for name,_ in controller.events)


@pytest.mark.parametrize('eventual_idle',[20,1000])
def test_adaptive_quiescence_timeout_reports_failed_probe_and_honest_restore_state(tmp_path,eventual_idle):
    probe,_,controller=setup(tmp_path,adaptive=True,engine_quiescence_timeout=10)
    controller.engine_idle_at=eventual_idle
    result=probe.run()
    assert result['status']=='failed'
    assert result['error']=='adaptive_engine_not_quiescent_before_worker_restart'
    gate=result['engine_quiescence_gates'][0]
    assert gate['status']=='failed' and gate['wait_seconds']>=10
    # Recovery itself waits for quiescence; a timeout never starts the worker
    # or watchdog merely because a worker-internal fence might later help.
    assert not result.get('restarted_worker_pid')
    assert result.get('final_restore_failed',False)==(eventual_idle==1000)
    assert controller.capacity == 64
    assert bool(controller.record(probe.supervisor)) == (eventual_idle == 20)
    assert not any(name.startswith(('api-','engine-','gateway-')) for name,_ in controller.events)


@pytest.mark.parametrize('payload,expected',[
    (b'vllm:num_requests_running{model_name="qwen"} 0\nvllm:num_requests_waiting 0\n',True),
    (b'vllm:num_requests_running 1\nvllm:num_requests_waiting 0\n',False),
    (b'vllm:num_requests_running 0\n',False),
    (b'vllm:num_requests_running NaN\nvllm:num_requests_waiting 0\n',False)])
def test_private_engine_quiescence_get_uses_same_fail_closed_worker_protocol(payload,expected):
    calls=[]
    class Response(BytesIO):status=200
    class Opener:
        def open(self,url,timeout):calls.append((url,timeout));return Response(payload)
    controller=WorkerController();controller.http=Opener()
    result=controller.engine_quiescence({'role':'engine','port':8300})
    assert result=={'quiescent':expected,'error':None}
    assert calls==[('http://127.0.0.1:8300/metrics',3)]


def test_private_engine_quiescence_transient_http_error_is_not_idle():
    class Opener:
        def open(self,url,timeout):raise HTTPError(url,503,'Unavailable',{},BytesIO(b'private diagnostics never recorded'))
    controller=WorkerController();controller.http=Opener()
    assert controller.engine_quiescence({'role':'engine','port':8300})=={
        'quiescent':False,'error':'metrics_http_error','http_status':503}


@pytest.mark.parametrize('timeout',[9,901])
def test_adaptive_quiescence_timeout_bounds_are_checked_before_fault(tmp_path,timeout):
    with pytest.raises(ProbeFailure,match='invalid_engine_quiescence_timeout'):
        setup(tmp_path,adaptive=True,engine_quiescence_timeout=timeout)


@pytest.mark.parametrize('payload,error',[(b'x'*(2*1024*1024+1),'metrics_too_large'),(b'\xff','metrics_invalid_utf8')])
def test_private_engine_quiescence_rejects_oversized_or_undecodable_metrics(payload,error):
    class Response(BytesIO):status=200
    class Opener:
        def open(self,url,timeout):return Response(payload)
    controller=WorkerController();controller.http=Opener()
    assert controller.engine_quiescence({'role':'engine','port':8300})=={'quiescent':False,'error':error}


def test_durable_intent_precedes_watchdog_stop_and_exact_kill(tmp_path):
    probe, _, controller = setup(tmp_path, capacity=0)
    original_invoke, original_crash = controller.invoke, controller.crash_exact
    observed = []
    def invoke(row, action):
        if row['role'] == 'supervisor' and action == 'stop':
            saved = json.loads(probe.journal.read_bytes())
            assert saved['pending_action']['stage'] == 'watchdog_stop'
            assert saved['manual_recovery_required'] and saved['automatic_replay'] is False
            observed.append('watchdog')
        return original_invoke(row, action)
    def crash(row, expected):
        saved = json.loads(probe.journal.read_bytes())
        assert saved['pending_action']['stage'] == 'worker_sigkill'
        assert saved['pending_action']['expected_identity']['pid'] == expected['pid']
        assert saved['source_sha256'] == probe._source_binding
        assert saved['manifest_sha256'] and saved['configuration_sha256']
        observed.append('kill')
        original_crash(row, expected)
    controller.invoke, controller.crash_exact = invoke, crash
    result = probe.run()
    assert result['status'] == 'passed' and observed == ['watchdog', 'kill']
    saved = json.loads(probe.journal.read_bytes())
    assert saved['phase'] == 'complete' and saved['pending_action'] is None
    assert saved['watchdog_restored'] and not saved['manual_recovery_required']
    assert not probe.journal.stat().st_mode & 0o077
    assert 'NEVER-PERSIST' not in probe.journal.read_text() and 'private-worker-token' not in probe.journal.read_text()


def test_same_probe_cannot_execute_twice_or_overwrite_journal(tmp_path):
    probe, _, controller = setup(tmp_path, capacity=0)
    assert probe.run()['status'] == 'passed'
    original = probe.journal.read_bytes(); events = list(controller.events)
    with pytest.raises(ProbeFailure, match='existing_operation_no_automatic_replay'):
        probe.run()
    assert probe.journal.read_bytes() == original and controller.events == events


def test_existing_journal_is_never_adopted_by_another_execution(tmp_path):
    probe, _, controller = setup(tmp_path, capacity=0)
    probe.journal.write_text('{"phase":"worker_sigkill_intent","private":"preserve"}')
    probe.journal.chmod(0o600); original = probe.journal.read_bytes()
    with pytest.raises(FileExistsError):
        probe.run()
    assert probe.journal.read_bytes() == original and not controller.events


@pytest.mark.parametrize('operation', ['watchdog_stop', 'worker_sigkill', 'worker_start', 'watchdog_start'])
def test_unknown_mutation_is_never_replayed_or_accepted_as_restored(tmp_path, operation):
    probe, _, controller = setup(tmp_path, capacity=0)
    original_invoke, original_crash = controller.invoke, controller.crash_exact
    def invoke(row, action):
        value = original_invoke(row, action)
        matched = (operation == 'watchdog_stop' and row['role'] == 'supervisor' and action == 'stop'
                   or operation == 'worker_start' and row['role'] == 'worker' and action == 'start'
                   or operation == 'watchdog_start' and row['role'] == 'supervisor' and action == 'start')
        if matched:
            raise OSError('NEVER-PERSIST-untrusted-driver-output')
        return value
    def crash(row, expected):
        original_crash(row, expected)
        if operation == 'worker_sigkill':
            raise OSError('NEVER-PERSIST-unknown-kill-ack')
    controller.invoke, controller.crash_exact = invoke, crash
    result = probe.run()
    assert result['status'] == 'failed' and result['manual_recovery_required'] and not result['watchdog_restored']
    assert result['pending_action']['stage'] == operation and result['pending_action']['outcome'] == 'unknown'
    expected_event = {'watchdog_stop': ('supervisor-primary', 'stop'), 'worker_sigkill': ('worker-inference', 'SIGKILL'),
                      'worker_start': ('worker-inference', 'start'), 'watchdog_start': ('supervisor-primary', 'start')}[operation]
    assert controller.events.count(expected_event) == 1
    if operation != 'watchdog_start':
        assert ('supervisor-primary', 'start') not in controller.events
    assert 'NEVER-PERSIST' not in json.dumps(result)


@pytest.mark.parametrize('phase', ['worker_sigkill_intent', 'worker_sigkill_confirmed'])
def test_journal_failure_blocks_following_mutation_and_watchdog(tmp_path, phase):
    probe, _, controller = setup(tmp_path, capacity=0)
    original = probe.save
    def save(value=None):
        if value == phase:
            raise OSError('synthetic journal fsync failure')
        original(value)
    probe.save = save
    result = probe.run()
    assert result['status'] == 'failed' and result['manual_recovery_required']
    assert ('supervisor-primary', 'start') not in controller.events
    assert ('worker-inference', 'start') not in controller.events
    assert (('worker-inference', 'SIGKILL') in controller.events) == (phase == 'worker_sigkill_confirmed')
    assert result['pending_action']['stage'] == 'worker_sigkill'


@pytest.mark.parametrize('changed', ['source', 'env', 'foreign_worker'])
def test_changed_binding_or_foreign_worker_prevents_automatic_recovery(tmp_path, changed):
    probe, _, controller = setup(tmp_path, capacity=0)
    original = controller.crash_exact
    source = tmp_path / 'ops' / 'managed_source.py'; source.parent.mkdir(); source.write_text('# original\n')
    probe._source_binding = __import__('ops.ha.single_pod_fault_probe', fromlist=['source_hashes']).source_hashes(tmp_path)
    probe.report['source_sha256'] = probe._source_binding
    def crash(row, expected):
        original(row, expected)
        if changed == 'source': source.write_text('# changed\n')
        elif changed == 'env': Path(probe.worker['env_file']).write_text('NEVER-PERSIST=changed-secret\n')
        else: controller.records[controller.key(probe.worker)] = {'pid': 9876, 'start_ticks': '999', 'cwd': '/foreign'}
    controller.crash_exact = crash
    result = probe.run()
    assert result['status'] == 'failed' and result['manual_recovery_required']
    assert ('supervisor-primary', 'start') not in controller.events and ('worker-inference', 'start') not in controller.events
    assert 'NEVER-PERSIST' not in json.dumps(result)


def test_failed_worker_readiness_does_not_restore_watchdog(tmp_path):
    probe, _, controller = setup(tmp_path, capacity=0)
    original = controller.responding
    controller.responding = lambda row, ready=False: False if row['role'] == 'worker' and controller.restarts else original(row, ready)
    result = probe.run()
    assert result['status'] == 'failed' and result['manual_recovery_required'] and result['final_restore_failed']
    assert controller.restarts == 1 and not controller.record(probe.supervisor)


def test_recovery_quiescence_is_uninterruptible_but_bounded(tmp_path):
    probe, _, controller = setup(tmp_path, adaptive=True, capacity=0, engine_quiescence_timeout=10)
    controller.engine_idle_at = 4
    original = controller.crash_exact
    def crash(row, expected):
        original(row, expected); probe.stop.set()
    controller.crash_exact = crash
    result = probe.run()
    assert result['status'] == 'failed' and result['error'] == 'interrupted'
    assert not result['manual_recovery_required'] and result['watchdog_restored']
    assert min(controller.worker_start_times) >= 4


def test_capacity600_drain_requires_worker_internal_shutdown_budget(tmp_path):
    with pytest.raises(ProbeFailure, match='worker_shutdown_drain_shorter_than_probe_bound'):
        setup(tmp_path, drain_seconds=600)
    assert not list((tmp_path / '.runtime').glob('*.journal.json'))


def test_capacity600_path_keeps_watchdog_paused_until_fence_and_real_restore(tmp_path):
    probe, _, controller = setup(tmp_path, drain_seconds=600, worker_shutdown_seconds=600)
    original = controller.graceful_stop; seen = []
    def drain(row, expected, grace, check):
        if row['role'] == 'worker':
            assert grace == 600 and not controller.record(probe.supervisor)
            saved = json.loads(probe.journal.read_bytes())
            assert saved['pending_action']['stage'] == 'reduced_worker_drain'
            seen.append(grace)
        return original(row, expected, grace, check)
    controller.graceful_stop = drain
    result = probe.run()
    assert result['status'] == 'passed' and seen == [600]
    assert result['reduced_positive_samples'] == 3 and result['capacity_reexpansion_proven']
    assert result['restored_server_capacity'] == 64 and result['watchdog_restored']
    assert controller.events.index(('task-0', 'heartbeat409+complete409')) < controller.events.index(('supervisor-primary', 'start'))
