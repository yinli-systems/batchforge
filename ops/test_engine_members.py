"""Linux-only real child-process failure tests. No GPUs, DB or network."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

import pytest

from ops import engine_members as members

pytestmark = pytest.mark.skipif(sys.platform != "linux" or not hasattr(os, "pidfd_open"), reason="Requires Linux pidfd ownership semantics")


def await_dead(pid):
    deadline = time.monotonic() + 3
    while members.identity(pid) and time.monotonic() < deadline:
        time.sleep(.02)
    assert members.identity(pid) is None


def test_leader_death_reaps_captured_child_but_preserves_unrelated_process(tmp_path):
    child_source = "import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); print('ready',flush=True); time.sleep(120)"
    leader_source = ("import subprocess,sys,time; "
        f"p=subprocess.Popen([sys.executable,'-c',{child_source!r}],stdout=subprocess.PIPE,text=True); "
        "p.stdout.readline(); print(p.pid,flush=True); time.sleep(120)")
    leader = subprocess.Popen([sys.executable, "-c", leader_source], cwd=tmp_path, start_new_session=True, stdout=subprocess.PIPE, text=True)
    unrelated = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"], cwd=tmp_path, start_new_session=True)
    child = None
    try:
        child = int(leader.stdout.readline())
        record = members.identity(leader.pid)
        path = tmp_path / "engine.members.json"
        assert members.capture(path, record)
        snapshot = members.read(path)
        assert {row["pid"] for row in snapshot["members"]} == {child}
        assert members.reap(path, record)["status"] == "leader_running"
        leader.kill()
        leader.wait(timeout=3)
        assert members.identity(child) is not None
        outcome = members.reap(path, record, grace_seconds=.05)
        assert outcome["signalled"] == 1
        await_dead(child)
        assert unrelated.poll() is None
        assert members.reap(path, record)["signalled"] == 0
    finally:
        if leader.poll() is None:
            leader.kill()
        leader.wait(timeout=3)
        if child and members.identity(child):
            os.kill(child, signal.SIGKILL)
        unrelated.kill()
        unrelated.wait(timeout=3)


def test_changed_launch_or_child_identity_never_receives_a_signal(tmp_path):
    process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"], cwd=tmp_path, start_new_session=True)
    try:
        live = members.identity(process.pid)
        record = {"pid": 2**30, "start_ticks": "not-a-real-launch", "cwd": str(tmp_path)}
        child = {**live, "start_ticks": "different", "pgid": record["pid"], "session": record["pid"]}
        path = tmp_path / "snapshot.json"
        members.save(path, {"leader": record, "members": [child], "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip()})
        assert members.reap(path, record, grace_seconds=0)["signalled"] == 0
        assert process.poll() is None
        with pytest.raises(RuntimeError, match="different launch"):
            members.reap(path, {**record, "start_ticks": "other"})
        assert process.poll() is None
    finally:
        process.kill()
        process.wait(timeout=3)
