"""Remember observed engine children and reap only their exact Linux identities.

No process-name matching, no signalling a stale process group, and no discovery
of new ownership after the engine leader is dead. Unknown children remain for
the per-device memory gate to detect rather than being guessed at.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import stat
import time


def identity(pid):
    try:
        raw = Path(f"/proc/{pid}/stat").read_text()
        fields = raw[raw.rfind(")") + 2:].split()
        if fields[0] == "Z":
            return None
        return {"pid": pid, "start_ticks": fields[19], "pgid": int(fields[2]),
            "session": int(fields[3]), "cwd": str(Path(f"/proc/{pid}/cwd").resolve())}
    except (OSError, ValueError, IndexError):
        return None


def leader_key(record):
    return {key: record[key] for key in ("pid", "start_ticks", "cwd")}


def matches_leader(current, record):
    return current is not None and leader_key(current) == leader_key(record)


def save(path, value):
    temporary = path.with_name(path.name + f".{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(descriptor, "w") as file:
            json.dump(value, file)
            file.flush()
            os.fsync(file.fileno())
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def read(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "rb") as file:
        info = os.fstat(file.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
            raise RuntimeError("Unsafe engine membership record")
        raw = file.read(131073)
        if len(raw) > 131072:
            raise RuntimeError("Oversized engine membership record")
        return json.loads(raw)


def capture(path, record):
    leader = identity(record["pid"])
    if not matches_leader(leader, record) or leader["pgid"] != record["pid"] or leader["session"] != record["pid"]:
        return False
    members, pending, visited = [], [record["pid"]], {record["pid"]}
    while pending:
        parent = pending.pop()
        # Follow only kernel-reported children of already-proven owned PIDs.
        # Do not enumerate unrelated host processes on a shared GPU machine.
        try:
            tasks = list(Path(f"/proc/{parent}/task").iterdir())
        except OSError:
            continue
        for task in tasks:
            try:
                children = [int(pid) for pid in (task / "children").read_text().split()]
            except OSError:
                continue
            for pid in children:
                if pid in visited:
                    continue
                visited.add(pid)
                current = identity(pid)
                if current and current["pgid"] == record["pid"] and current["session"] == record["pid"] and current["cwd"] == record["cwd"]:
                    members.append(current)
                    pending.append(pid)
                if len(visited) > 256:
                    raise RuntimeError("Too many engine children to track safely")
    if identity(record["pid"]) != leader:
        return False
    save(path, {"leader": leader_key(record), "members": members, "observed_at": time.time(),
        "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip()})
    return True


def reap(path, record, *, grace_seconds=1):
    if matches_leader(identity(record["pid"]), record):
        return {"status": "leader_running", "signalled": 0, "remaining": 0}
    if not path.exists():
        return {"status": "nothing_to_reap", "signalled": 0, "remaining": 0}
    snapshot = read(path)
    if snapshot.get("boot_id") != Path("/proc/sys/kernel/random/boot_id").read_text().strip():
        raise RuntimeError("Engine membership belongs to a different kernel boot")
    if snapshot.get("leader") != leader_key(record):
        raise RuntimeError("Engine membership belongs to a different launch")
    members = snapshot.get("members")
    if not isinstance(members, list) or len(members) > 256:
        raise RuntimeError("Invalid engine membership list")
    handles = []
    try:
        for member in members:
            if (not isinstance(member, dict) or type(member.get("pid")) is not int or member["pid"] <= 1
                    or member.get("pgid") != record["pid"] or member.get("session") != record["pid"]
                    or member.get("cwd") != record["cwd"] or member["pid"] == record["pid"]):
                raise RuntimeError("Invalid captured engine child identity")
        # Validate the complete document before signalling anything. pidfds pin
        # the target even if it exits and its numeric PID is reused afterward.
        for member in members:
            if identity(member["pid"]) != member:
                continue
            try:
                handle = os.pidfd_open(member["pid"])
            except ProcessLookupError:
                continue
            if identity(member["pid"]) != member:
                os.close(handle)
                continue
            handles.append((handle, member))
        for handle, _ in handles:
            try:
                signal.pidfd_send_signal(handle, signal.SIGTERM)
            except ProcessLookupError:
                pass
        deadline = time.monotonic() + grace_seconds
        while time.monotonic() < deadline and any(identity(m["pid"]) == m for _, m in handles):
            time.sleep(.05)
        for handle, member in handles:
            if identity(member["pid"]) == member:
                try:
                    signal.pidfd_send_signal(handle, signal.SIGKILL)
                except ProcessLookupError:
                    pass
        # GPU memory release is checked separately by the watchdog on every rank.
        return {"status": "reaped", "signalled": len(handles),
            "remaining": sum(identity(m["pid"]) == m for _, m in handles)}
    finally:
        for handle, _ in handles:
            os.close(handle)
