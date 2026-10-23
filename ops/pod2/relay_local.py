#!/usr/bin/env python3
"""Keep an SSH-only, route-restricted worker relay alive for the second Pod.

The fixed destination is the primary Pod's existing localhost SSH tunnel. No
HTTP destination or arbitrary headers can be supplied by a remote request.
Only status/count logs are printed; prompts, responses, and credentials stay
inside the encrypted SSH framing channel. The laptop is a temporary dependency.
"""
from __future__ import annotations

import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import pty
import re
import select
import subprocess
import sys
import termios
import threading
import time
import urllib.error
import urllib.request

PREFIX = b"BATCHFORGE_RELAY_V1 "
MAX_BODY = 4 * 1024 * 1024
MAX_FRAME = 6 * 1024 * 1024
TASK_PATH = re.compile(r"^/internal/tasks/task_[0-9a-f]{32}/(heartbeat|complete|fail)$")


def allowed(path: str) -> bool:
    return path in {"/internal/workers/register", "/internal/tasks/claim"} or bool(TASK_PATH.fullmatch(path))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", default="t0256w11ortyw6-644123a2@ssh.runpod.io")
    parser.add_argument("--identity", default=str(Path.home() / ".ssh/id_ed25519"))
    parser.add_argument("--known-hosts", default="/private/tmp/batchforge_pod2_known_hosts")
    parser.add_argument("--port", type=int, default=18082)
    args = parser.parse_args()
    master, slave = pty.openpty()
    attrs = termios.tcgetattr(slave)
    attrs[3] &= ~(termios.ECHO | termios.ICANON)
    termios.tcsetattr(slave, termios.TCSANOW, attrs)
    child = subprocess.Popen([
        "ssh", "-tt", "-o", "BatchMode=yes", "-o", "ConnectTimeout=20",
        "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=3",
        "-o", "StrictHostKeyChecking=yes", "-o", f"UserKnownHostsFile={args.known_hosts}",
        "-i", args.identity, args.target,
    ], stdin=slave, stdout=slave, stderr=slave, start_new_session=True)
    os.close(slave)
    write_lock = threading.Lock()
    count_lock = threading.Lock()
    counts = {"forwarded": 0, "errors": 0}
    # Explicitly disable environment HTTP proxies for this localhost-only hop.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def write(data: bytes) -> None:
        with write_lock:
            view = memoryview(data)
            while view:
                _, writable, _ = select.select([], [master], [], 1)
                if writable:
                    n = os.write(master, view[:65536])
                    view = view[n:]

    def handle(value: dict) -> None:
        status, body = 503, b'{"detail":"relay upstream unavailable"}'
        try:
            path = value.get("path", "")
            if not allowed(path):
                status, body = 403, b'{"detail":"relay route denied"}'
            else:
                request_body = base64.b64decode(value.get("body", ""), validate=True)
                authorization = value.get("authorization", "")
                if len(request_body) > MAX_BODY or len(authorization) > 1024 or "\n" in authorization or "\r" in authorization:
                    raise ValueError("invalid relay request")
                request = urllib.request.Request("http://127.0.0.1:8787" + path, data=request_body,
                    headers={"Authorization": authorization, "Content-Type": "application/json"}, method="POST")
                try:
                    response = opener.open(request, timeout=8)
                except urllib.error.HTTPError as error:
                    response = error
                with response:
                    status, body = response.code, response.read(MAX_BODY + 1)
                if len(body) > MAX_BODY:
                    status, body = 502, b'{"detail":"relay upstream response too large"}'
        except (ValueError, OSError, urllib.error.URLError):
            pass
        with count_lock:
            counts["forwarded"] += 1
            counts["errors"] += int(status >= 500)
        frame = {"type": "response", "id": value.get("id"), "status": status,
                 "body": base64.b64encode(body).decode()}
        write(PREFIX + json.dumps(frame, separators=(",", ":")).encode() + b"\n")

    buffer = bytearray()
    ready = False
    last_log = time.monotonic()
    last_ping = time.monotonic()
    pool = ThreadPoolExecutor(max_workers=32)
    try:
        deadline = time.monotonic() + 30
        while b"# " not in buffer:
            if time.monotonic() > deadline:
                raise TimeoutError("SSH prompt timeout")
            readable, _, _ = select.select([master], [], [], 1)
            if readable:
                buffer.extend(os.read(master, 65536))
            if child.poll() is not None:
                raise RuntimeError("SSH gateway disconnected")
        buffer.clear()
        write(("stty raw -echo; export PS1='' PS2=''; set +o history; "
               "cd /root/batchforge-runtime/app; exec /root/batchforge-runtime/venv/bin/python "
               f"ops/pod2/relay_remote.py --port {args.port}\n").encode())
        while child.poll() is None:
            readable, _, _ = select.select([master], [], [], 1)
            if readable:
                data = os.read(master, 65536)
                if not data:
                    break
                buffer.extend(data)
                if len(buffer) > MAX_FRAME:
                    raise ValueError("relay frame exceeds bound")
                while b"\n" in buffer:
                    line, _, rest = buffer.partition(b"\n")
                    buffer[:] = rest
                    if not ready and PREFIX in line:
                        # Bash may emit a bracketed-paste escape before exec;
                        # discard only that startup preamble, never frame data.
                        line = line[line.index(PREFIX):]
                    if not line.startswith(PREFIX):
                        continue
                    value = json.loads(line[len(PREFIX):])
                    if value.get("type") == "ready":
                        ready = True
                        print(json.dumps({"event": "relay_ready", "remote_bind": "127.0.0.1", "remote_port": args.port}), flush=True)
                    elif value.get("type") == "request":
                        pool.submit(handle, value)
            if not ready and time.monotonic() > deadline:
                raise TimeoutError("Remote relay did not become ready")
            if ready and time.monotonic() - last_ping >= 5:
                write(PREFIX + b'{"type":"ping"}\n')
                last_ping = time.monotonic()
            if time.monotonic() - last_log >= 30:
                with count_lock:
                    print(json.dumps({"event": "relay_counts", **counts}), flush=True)
                last_log = time.monotonic()
        return 0 if ready else 1
    except KeyboardInterrupt:
        return 0
    finally:
        pool.shutdown(wait=False, cancel_futures=True)
        if ready and child.poll() is None:
            try:
                write(PREFIX + b'{"type":"close"}\n')
            except OSError:
                pass
        os.close(master)
        try:
            child.wait(timeout=10)
        except subprocess.TimeoutExpired:
            child.terminate()
        with count_lock:
            print(json.dumps({"event": "relay_closed", **counts}), flush=True)


if __name__ == "__main__":
    raise SystemExit(main())
