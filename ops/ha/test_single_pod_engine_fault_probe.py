"""Safety/control-flow proof; inference is never started by these tests."""
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import sys

import pytest

from ops.ha import single_pod_engine_fault_probe as module
from ops.ha.controlplane_outage_probe import ProbeFailure


class Clock:
    def __init__(self): self.value = 0
    def __call__(self): return self.value
    def sleep(self, seconds): self.value += seconds


class Controller:
    def __init__(self, root, services, supervisor):
        self.root, self.events = root, []
        self.records = {self.key(row): {"pid": index+10, "start_ticks": str(index+100), "cwd": str(root),
            "track_members": row["role"] == "engine", "command": ["--api-key", "NEVER-PERSIST-command-token"]}
            for index, row in enumerate([*services, supervisor])}
        self.next_pid, self.starts = 100, {"worker": 0, "engine": 0}
        self.gate_allowed, self.quiet, self.ready = True, True, True
        self.ambiguous_engine_start, self.after_stop = False, None

    @staticmethod
    def key(row): return row["role"] + "-" + row["name"]
    def record(self, row): return self.records.get(self.key(row))
    def responding(self, row, ready=False):
        return self.record(row) is not None and (self.ready or row["role"] not in {"worker", "engine"})
    def invoke(self, row, action):
        self.events.append((self.key(row), action))
        if action == "stop":
            self.records.pop(self.key(row), None)
            if self.after_stop: self.after_stop(row)
            return {"status": "stopped"}
        if self.record(row): return {"status": "already_running", "pid": self.record(row)["pid"]}
        self.next_pid += 1
        self.records[self.key(row)] = {"pid": self.next_pid, "start_ticks": str(self.next_pid*10), "cwd": str(self.root), "track_members": row["role"] == "engine"}
        if row["role"] in self.starts: self.starts[row["role"]] += 1
        if row["role"] == "engine" and self.ambiguous_engine_start:
            raise OSError("NEVER-PERSIST-driver-secret")
        return {"status": "started", "pid": self.next_pid}
    def crash_exact(self, row, expected):
        assert self.record(row) == expected
        self.events.append((self.key(row), "SIGKILL")); self.records.pop(self.key(row))
    def start_gate(self, row):
        self.events.append(("engine", "physical_member_memory_gate"))
        return self.gate_allowed, None if self.gate_allowed else "gpu_memory_not_released"
    def engine_quiescence(self, row):
        self.events.append(("engine", "quiescence")); return {"quiescent": self.quiet, "error": None}
    def capture_members(self, row, expected):
        assert self.record(row) == expected
        return {"observed_members": 5, "snapshot_sha256": "a"*64}
    def gpu_memory(self):
        return {"available": True, "devices": [{"index": str(index), "memory_used_mib": 100} for index in range(4)]}
    def fence_both(self, api, token, task):
        assert token == "NEVER-PERSIST-worker-secret"
        self.events.append(("stale", "heartbeat409+complete409")); return {"heartbeat": 409, "complete": 409}


class Progress:
    run_id, model, secondary_worker_id = "bench-exact", module.MODEL, "owned-worker"
    def __init__(self, clock, controller):
        self.clock, self.controller = clock, controller
        self.never_expire, self.other, self.peer, self.reclaim, self.invalid_final = False, 0, 0, 8, False
        now = datetime.now(timezone.utc)
        self.held = [{"id": f"task-{index}", "worker_id": self.secondary_worker_id,
            "attempt_token": f"NEVER-PERSIST-lease-{index}", "captured_at": now,
            "lease_expires_at": now+timedelta(seconds=90)} for index in range(8)]
    def scope(self, expected): assert expected == 32
    def other_unfinished_work(self): return self.other
    def other_fresh_workers(self, _): return self.peer
    def capture(self, _): return self.held
    def held_after_loss(self, captured): return captured
    def expired(self, captured):
        done = self.clock() >= .5 and not self.never_expire
        return (captured if done else []), {"expired" if done else "running": len(captured)}
    def recovered_pending(self, captured): return len(captured) if self.expired(captured)[0] else 0
    def snapshot(self):
        running, completed = (0, 32) if self.controller.starts["worker"] else (0 if self.expired(self.held)[0] else 8, 0)
        return {"total": 32, "pending": 32-running-completed, "running": running,
            "completed": completed, "failed": 0, "cancelled": 0, "secondary_completed": completed}
    def recovery(self, captured):
        return {"reclaimed_by_exact_secondary": self.reclaim, "reclaimed_tasks": [{"id": row["id"]} for row in captured[:self.reclaim]]}
    def accepted_state(self, tid): return {"id": tid, "accepted_count": 1, "prompt_tokens": 128, "completion_tokens": 64}
    def final_proof(self, expected):
        if self.invalid_final: raise ProbeFailure("final_real_workload_acceptance_or_usage_proof_failed")
        return {"tasks": expected, "accepted_once": True, "durable_usage_matches": True}


def setup(tmp_path):
    root = tmp_path.resolve(); (root / ".runtime").mkdir(mode=0o700)
    for name in module.DEPENDENCIES:
        path = root / name; path.parent.mkdir(parents=True, exist_ok=True); path.write_text("# frozen source\n")
    manifest = root / "services.json"; manifest.write_text('{"services":[]}'); manifest.chmod(0o600)
    env = root / "worker.env"; env.write_text("WORKER_ADAPTIVE_KV_ENABLED=true\nWORKER_ID=owned-worker\n"); env.chmod(0o600)
    watch = root / "watch.env"; watch.write_text("SUPERVISOR_MANIFEST="+str(manifest)+"\n"); watch.chmod(0o600)
    services = [{"role": role, "name": name, "port": port, "env_file": str(env), "python": sys.executable}
        for role, name, port in [("api", "a", 8082), ("api", "b", 8083), ("gateway", "edge", 8080), ("engine", "inference", 8300), ("worker", "inference", 9105)]]
    supervisor = {"role": "supervisor", "name": "primary", "env_file": str(watch), "python": sys.executable}
    controller, clock = Controller(root, services, supervisor), Clock()
    progress = Progress(clock, controller)
    probe = module.SinglePodEngineProbe(services=services, worker=services[-1], engine=services[-2], api=services[0],
        supervisor=supervisor, manifest=manifest, original=manifest.read_bytes(), progress=progress, controller=controller,
        token="NEVER-PERSIST-worker-secret", expected=32, journal=root / ".runtime/probe.json", minimum_running=8,
        expiry_timeout=2, terminal_timeout=3, engine_ready_timeout=3, engine_quiescence_timeout=2, clock=clock, sleep=clock.sleep)
    return probe, progress, controller


def test_real_pair_loss_reload_order_and_durable_safe_metadata(tmp_path):
    probe, progress, controller = setup(tmp_path)
    result = probe.run()
    assert result["status"] == "passed" and result["captured_tasks_reaccepted_once"] == 8
    events = controller.events
    assert events.index(("supervisor-primary", "stop")) < events.index(("worker-inference", "SIGKILL")) < events.index(("engine-inference", "stop"))
    assert events.index(("engine", "physical_member_memory_gate")) < events.index(("engine-inference", "start")) < events.index(("engine", "quiescence")) < events.index(("worker-inference", "start"))
    assert events.index(("stale", "heartbeat409+complete409")) < events.index(("supervisor-primary", "start"))
    assert not any(name.startswith(("api-", "gateway-")) for name, action in events if action in {"stop", "SIGKILL", "start"})
    assert result["stale_attempt_http_status"] == {"heartbeat": 409, "complete": 409}
    assert result["engine_reload_seconds"] >= 0 and result["observed_engine_unavailable_seconds"] >= .5
    assert len(result["gpu_memory_after"]["devices"]) == 4
    assert "NEVER-PERSIST" not in json.dumps(result) and "attempt_token" not in json.dumps(result)
    assert json.loads(probe.journal.read_bytes())["status"] == "passed"


@pytest.mark.parametrize("reason", ["other", "peer", "short", "identity", "manifest"])
def test_invalid_scope_or_ownership_never_reaches_fault(tmp_path, reason):
    probe, progress, controller = setup(tmp_path)
    if reason == "other": progress.other = 1
    elif reason == "peer": progress.peer = 1
    elif reason == "short": progress.held = progress.held[:7]
    elif reason == "identity": controller.records.pop("api-a")
    else: probe.manifest.write_text("changed")
    result = probe.run()
    assert result["status"] == "failed" and not controller.events


@pytest.mark.parametrize("reason", ["memory", "quiescence", "readiness", "ambiguous"])
def test_recovery_gates_fail_closed_without_worker_or_watchdog_start(tmp_path, reason):
    probe, progress, controller = setup(tmp_path)
    if reason == "memory": controller.gate_allowed = False
    elif reason == "quiescence": controller.quiet = False
    elif reason == "readiness":
        controller.after_stop = lambda row: setattr(controller, "ready", False) if row["role"] == "engine" else None
    else: controller.ambiguous_engine_start = True
    result = probe.run()
    assert result["status"] == "failed" and result["manual_recovery_required"] is True
    assert ("worker-inference", "start") not in controller.events and ("supervisor-primary", "start") not in controller.events
    assert "NEVER-PERSIST" not in json.dumps(result)
    if reason == "ambiguous": assert controller.starts["engine"] == 1


def test_failed_lease_observation_restores_safe_pair_and_watchdog(tmp_path):
    probe, progress, controller = setup(tmp_path); progress.never_expire = True
    result = probe.run()
    assert result["status"] == "failed" and "not_retryable_pending" in result["error"]
    assert result["manual_recovery_required"] is False
    assert controller.record(probe.worker) and controller.record(probe.engine) and controller.record(probe.supervisor)


def test_interrupted_observation_still_uses_uninterruptible_recovery(tmp_path):
    probe, progress, controller = setup(tmp_path)
    controller.after_stop = lambda row: probe.stop.set() if row["role"] == "engine" else None
    result = probe.run()
    assert result["status"] == "failed" and result["error"] == "interrupted"
    assert result["manual_recovery_required"] is False and controller.record(probe.supervisor)


def test_changed_source_or_unexpected_pair_identity_never_adopted_in_restore(tmp_path):
    probe, progress, controller = setup(tmp_path)
    def changed(row):
        if row["role"] == "engine":
            (controller.root / "ops/manage.py").write_text("# unreviewed replacement\n")
    controller.after_stop = changed
    result = probe.run()
    assert result["status"] == "failed" and result["manual_recovery_required"] is True
    assert ("engine-inference", "start") not in controller.events and ("supervisor-primary", "start") not in controller.events


@pytest.mark.parametrize("reason", ["reclaim", "final"])
def test_false_terminal_success_never_passes_and_safe_watchdog_is_restored(tmp_path, reason):
    probe, progress, controller = setup(tmp_path)
    if reason == "reclaim": progress.reclaim = 7
    else: progress.invalid_final = True
    result = probe.run()
    assert result["status"] == "failed" and controller.record(probe.supervisor)
    assert result["manual_recovery_required"] is False


def test_new_operation_is_exclusive_durable_and_cannot_replay(tmp_path, monkeypatch):
    path = tmp_path / "job.json"; events = []
    real_fsync = os.fsync
    monkeypatch.setattr(os, "fsync", lambda fd: (events.append(fd), real_fsync(fd))[1])
    module.reserve(path)
    assert path.stat().st_mode & 0o777 == 0o600 and len(events) == 2
    with pytest.raises(FileExistsError): module.reserve(path)
    module.durable_write(path, {"phase": "fault_intent_durable"})
    assert len(events) == 4


def test_actual_engine_membership_capture_checks_exact_identity_before_capture(tmp_path, monkeypatch):
    controller = module.EngineFaultController(root=tmp_path)
    service = {"role": "engine", "name": "inference"}
    monkeypatch.setattr(controller, "record", lambda row: {"pid": 4})
    called = []; monkeypatch.setattr(module.engine_members, "capture", lambda *args: called.append(args))
    with pytest.raises(ProbeFailure, match="tp4_owned_engine_membership_required"):
        controller.capture_members(service, {"pid": 3, "track_members": True})
    assert not called


def test_gpu_metadata_requires_exact_four_devices(monkeypatch):
    controller = module.EngineFaultController()
    monkeypatch.setattr(module.Sampler, "gpu", lambda: {"observed_at": "now", "devices": [{"index": "0"}]})
    assert controller.gpu_memory()["available"] is False


def test_main_refuses_without_execute_on_unsupported_host_without_network(capsys):
    assert module.main(["--manifest", "/missing", "--env-file", "/missing", "--supervisor-env", "/missing", "--report", "/missing",
        "--worker-name", "worker", "--engine-name", "engine", "--run-id", "latest", "--expected-tasks", "32"]) == 1
    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "refused"


def cli_setup(tmp_path, monkeypatch):
    probe, progress, controller = setup(tmp_path)
    probe.engine["gpu"] = "0,1,2,3"
    monkeypatch.setattr(module, "ROOT", controller.root)
    monkeypatch.setattr(module.sys, "platform", "linux")
    monkeypatch.setattr(module.os, "pidfd_open", lambda *_: None, raising=False)
    monkeypatch.setattr(module, "load_manifest", lambda _: (probe.manifest, probe.services, "fingerprint"))
    def env(path):
        if path == Path(probe.supervisor["env_file"]): return {"SUPERVISOR_MANIFEST": str(probe.manifest)}
        return {"MODEL_NAME": module.MODEL, "ENGINE_TENSOR_PARALLEL_SIZE": "4", "SQL_RECOVERY_ENABLED": "true",
            "WORKER_ID": "owned-worker", "INFERENCE_BASE_URLS": "http://127.0.0.1:8300", "DATABASE_URL": "NEVER-PERSIST-dsn"}
    monkeypatch.setattr(module.manage, "load_env", env)
    args = ["--manifest", str(probe.manifest), "--env-file", probe.api["env_file"],
        "--supervisor-env", probe.supervisor["env_file"], "--report", str(controller.root / ".runtime/final.json"),
        "--worker-name", "inference", "--engine-name", "inference", "--run-id", "bench-exact", "--expected-tasks", "32"]
    return probe, args


def test_default_dry_run_does_not_connect_create_journal_or_signal(tmp_path, monkeypatch, capsys):
    probe, args = cli_setup(tmp_path, monkeypatch)
    monkeypatch.setattr(module, "EngineFaultProgress", lambda *_: pytest.fail("Dry-run opened database"))
    assert module.main(args) == 0
    value = json.loads(capsys.readouterr().out)
    assert value["status"] == "dry_run" and value["requires_execute"] is True
    assert not list((probe.controller.root / ".runtime").glob("*.journal.json"))


def test_stable_job_replay_refused_before_connection(tmp_path, monkeypatch, capsys):
    probe, args = cli_setup(tmp_path, monkeypatch)
    module.reserve(probe.controller.root / ".runtime/singlepod-engine-fault-bench-exact.journal.json")
    monkeypatch.setattr(module, "EngineFaultProgress", lambda *_: pytest.fail("Replay opened database"))
    assert module.main(args + ["--execute"]) == 1
    assert json.loads(capsys.readouterr().out)["error"] == "existing_operation_no_automatic_replay"


def test_failed_arming_preserves_private_failure_report_without_service_action(tmp_path, monkeypatch, capsys):
    probe, args = cli_setup(tmp_path, monkeypatch)
    class Unsubmitted:
        connection = None
        def __init__(self, *_): self.connection = self
        def execute(self, *_): return self
        def fetchone(self): return {"role": "bf_runtime"}
        def submission_counts(self): return {"total": 0}
        def close(self): pass
    monkeypatch.setattr(module, "EngineFaultProgress", Unsubmitted)
    monkeypatch.setattr(module.signal, "signal", lambda *_: None)
    assert module.main(args + ["--execute", "--wait-for-submission", "0"]) == 1
    value = json.loads(capsys.readouterr().out)
    assert value["error"] == "exact_run_not_armed_with_enough_valid_leases"
    report = json.loads((probe.controller.root / ".runtime/final.json").read_bytes())
    assert report["phase"] == "reserved" and report["controller_error"] == value
    assert "NEVER-PERSIST" not in json.dumps(report)
    assert not probe.controller.events


@pytest.fixture
def real_database():
    """A new disposable DB inside the caller's separate private test cluster."""
    source = os.environ.get("TEST_DATABASE_URL")
    if not source: pytest.skip("Requires isolated disposable PostgreSQL")
    import psycopg
    from psycopg import sql
    from psycopg.conninfo import make_conninfo
    import secrets
    name = "bf_enginefault_" + secrets.token_hex(6)
    with psycopg.connect(source, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(sql.Identifier(name)))
    dsn = make_conninfo(source, dbname=name)
    try:
        with psycopg.connect(dsn) as conn:
            conn.execute("CREATE TABLE batches(id text PRIMARY KEY,model text,total_count int,metadata jsonb)")
            conn.execute("CREATE TABLE tasks(id text PRIMARY KEY,batch_id text,model text,status text,response jsonb,prompt_tokens int,completion_tokens int)")
            conn.execute("CREATE TABLE attempts(token text PRIMARY KEY,task_id text,status text,prompt_tokens int,completion_tokens int)")
            conn.execute("INSERT INTO batches VALUES('batch',%s,32,'{\"benchmark_run\":\"bench-exact\"}')", (module.MODEL,))
            conn.execute("INSERT INTO tasks SELECT 'task-'||n,'batch',%s,'completed','{}',128,32 FROM generate_series(0,31)n", (module.MODEL,))
            conn.execute("INSERT INTO attempts SELECT 'token-'||n,'task-'||n,'completed',128,32 FROM generate_series(0,31)n")
        yield dsn
    finally:
        with psycopg.connect(source, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP DATABASE {} WITH(FORCE)").format(sql.Identifier(name)))


def test_real_scope_acceptance_and_readonly_connection(real_database):
    import psycopg
    progress = module.EngineFaultProgress(real_database, "bench-exact", module.MODEL, "worker")
    try:
        assert progress.connection.execute("SHOW transaction_read_only").fetchone()["transaction_read_only"] == "on"
        progress.scope(32)
        assert progress.other_unfinished_work() == 0
        assert progress.final_proof(32)["accepted_once"] is True
        with psycopg.connect(real_database) as conn:
            conn.execute("INSERT INTO batches VALUES('other','another-model',1,'{}')")
            conn.execute("INSERT INTO tasks VALUES('unselected','other','another-model','pending',NULL,0,0)")
        assert progress.other_unfinished_work() == 1
    finally: progress.close()


def test_real_multiple_benchmark_batches_refused_before_fault(real_database):
    import psycopg
    progress = module.EngineFaultProgress(real_database, "bench-exact", module.MODEL, "worker")
    try:
        with psycopg.connect(real_database) as conn:
            conn.execute("UPDATE batches SET total_count=16 WHERE id='batch'")
            conn.execute("INSERT INTO batches VALUES('batch2',%s,16,'{\"benchmark_run\":\"bench-exact\"}')", (module.MODEL,))
            conn.execute("UPDATE tasks SET batch_id='batch2' WHERE substring(id FROM 6)::integer>=16")
        with pytest.raises(ProbeFailure, match="exact_run_requires_one_benchmark_batch_with_live_payloads"):
            progress.scope(32)
    finally: progress.close()


@pytest.mark.parametrize("mutation", ["duplicate", "usage", "simulated", "wrong_model", "null_response", "pending"])
def test_real_final_proof_rejects_invalid_durable_acceptance(real_database, mutation):
    import psycopg
    progress = module.EngineFaultProgress(real_database, "bench-exact", module.MODEL, "worker")
    try:
        with psycopg.connect(real_database) as conn:
            if mutation == "duplicate": conn.execute("INSERT INTO attempts VALUES('duplicate','task-0','completed',128,32)")
            elif mutation == "usage": conn.execute("UPDATE attempts SET completion_tokens=31 WHERE task_id='task-0'")
            elif mutation == "simulated": conn.execute("UPDATE tasks SET response='{\"batchforge_test_only\":true}' WHERE id='task-0'")
            elif mutation == "wrong_model": conn.execute("UPDATE tasks SET model='other' WHERE id='task-0'")
            elif mutation == "null_response": conn.execute("UPDATE tasks SET response=NULL WHERE id='task-0'")
            else: conn.execute("UPDATE tasks SET status='pending' WHERE id='task-0'")
        with pytest.raises(ProbeFailure, match="final_real_workload_acceptance_or_usage_proof_failed"):
            progress.final_proof(32)
        if mutation == "wrong_model":
            with pytest.raises(ProbeFailure, match="model_scope_mismatch"): progress.scope(32)
    finally: progress.close()
