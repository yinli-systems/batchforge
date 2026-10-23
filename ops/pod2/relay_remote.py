#!/usr/bin/env python3
"""Loopback-only worker HTTP endpoint carried over an existing SSH terminal.

Only the worker protocol is accepted. stdout/stdin are an SSH-encrypted private
framing channel, not logs. Never tee this stream into an operator log file.
"""
from __future__ import annotations

import argparse
import base64
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import re
import sys
import threading
import time
import uuid

PREFIX = b"BATCHFORGE_RELAY_V1 "
MAX_BODY = 4 * 1024 * 1024
MAX_FRAME = 6 * 1024 * 1024
TASK_PATH = re.compile(r"^/internal/tasks/task_[0-9a-f]{32}/(heartbeat|complete|fail)$")


def allowed(path: str) -> bool:
    return path in {"/internal/workers/register", "/internal/tasks/claim"} or bool(TASK_PATH.fullmatch(path))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=18082)
    args = parser.parse_args()
    pending: dict[str, dict] = {}
    lock, output_lock = threading.Lock(), threading.Lock()
    capacity = threading.BoundedSemaphore(64)
    last_message = time.monotonic()

    def frame(value: dict) -> None:
        encoded = PREFIX + json.dumps(value, separators=(",", ":")).encode() + b"\n"
        if len(encoded) > MAX_FRAME:
            raise ValueError("relay frame too large")
        with output_lock:
            sys.stdout.buffer.write(encoded)
            sys.stdout.buffer.flush()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_args) -> None:
            pass

        def reply(self, status: int, body: bytes) -> None:
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Connection", "close")
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass
            self.close_connection = True

        def do_POST(self) -> None:
            self.connection.settimeout(10)
            if not allowed(self.path) or self.headers.get("Transfer-Encoding"):
                self.reply(403, b'{"detail":"relay route denied"}')
                return
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                length = -1
            if length < 0 or length > MAX_BODY:
                self.reply(413, b'{"detail":"relay request too large"}')
                return
            if not capacity.acquire(blocking=False):
                self.reply(503, b'{"detail":"relay capacity reached"}')
                return
            request_id = uuid.uuid4().hex
            entry = {"event": threading.Event()}
            try:
                body = self.rfile.read(length)
                if len(body) != length:
                    self.reply(400, b'{"detail":"truncated relay request"}')
                    return
                with lock:
                    pending[request_id] = entry
                frame({"type": "request", "id": request_id, "path": self.path,
                       "authorization": self.headers.get("Authorization", ""),
                       "body": base64.b64encode(body).decode()})
                if not entry["event"].wait(15):
                    self.reply(504, b'{"detail":"relay response timeout"}')
                    return
                value = entry.get("response", {})
                response = base64.b64decode(value.get("body", ""), validate=True)
                if len(response) > MAX_BODY:
                    raise ValueError("relay response too large")
                self.reply(int(value.get("status", 503)), response)
            except (ValueError, OSError, TimeoutError):
                self.reply(503, b'{"detail":"relay unavailable"}')
            finally:
                with lock:
                    pending.pop(request_id, None)
                capacity.release()

    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    server.daemon_threads = True
    record = Path(__file__).resolve().parents[2] / ".runtime" / f"relay-{args.port}.json"
    record.parent.mkdir(mode=0o700, exist_ok=True)
    proc_stat = Path('/proc/self/stat')
    stat = proc_stat.read_text() if proc_stat.exists() else ''
    fields = stat[stat.rfind(')') + 2:].split() if stat else []
    record.write_text(json.dumps({"pid": os.getpid(), "start_ticks": fields[19] if fields else None,
                                  "cwd": str(Path.cwd()), "port": args.port}))
    record.chmod(0o600)

    def receive() -> None:
        nonlocal last_message
        try:
            while True:
                line = sys.stdin.buffer.readline(MAX_FRAME + 1)
                if not line or len(line) > MAX_FRAME:
                    break
                if not line.startswith(PREFIX):
                    continue
                try:
                    value = json.loads(line[len(PREFIX):])
                except (ValueError, UnicodeDecodeError):
                    break
                last_message = time.monotonic()
                if value.get("type") == "close":
                    break
                if value.get("type") == "ping":
                    frame({"type": "pong"})
                    continue
                if value.get("type") != "response":
                    continue
                with lock:
                    entry = pending.get(value.get("id"))
                    if entry:
                        entry["response"] = value
                        entry["event"].set()
        finally:
            with lock:
                for entry in pending.values():
                    entry["event"].set()
            server.shutdown()

    def watchdog() -> None:
        while time.monotonic() - last_message < 45:
            time.sleep(2)
        # Some gateways keep their PTY open after the local client disappears.
        # Liveness frames make that case bounded without relying on a HUP.
        server.shutdown()

    threading.Thread(target=receive, daemon=True).start()
    threading.Thread(target=watchdog, daemon=True).start()
    frame({"type": "ready", "bind": "127.0.0.1", "port": args.port})
    try:
        server.serve_forever(poll_interval=0.2)
    finally:
        server.server_close()
        record.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
