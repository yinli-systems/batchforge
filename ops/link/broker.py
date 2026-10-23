"""Pod-to-Pod worker transport using a dedicated forced-command SSH identity.

Runs on the control-plane host, independent of the operator laptop. The remote
authorized key must allow only relay_remote.py and forbid PTY/forwarding. No
user SSH private key, tenant key or database credential crosses this channel.
"""
from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import os
from pathlib import Path
import random
import re
import signal
import stat
import time
from urllib.parse import urlsplit

import httpx

from ops.pod2.relay_remote import PREFIX, MAX_BODY, MAX_FRAME, allowed


class Broker:
    def __init__(self):
        self.host = os.environ["LINK_HOST"]
        self.port = int(os.environ["LINK_PORT"])
        self.user = os.environ.get("LINK_USER", "root")
        self.identity = Path(os.environ["LINK_IDENTITY"]).resolve()
        self.known_hosts = Path(os.environ["LINK_KNOWN_HOSTS"]).resolve()
        self.upstream = os.environ.get("LINK_UPSTREAM", "http://127.0.0.1:8080").rstrip("/")
        parsed = urlsplit(self.upstream)
        if parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "localhost"} or parsed.username or parsed.path or parsed.query or parsed.fragment:
            raise ValueError("Broker upstream must be a fixed loopback HTTP origin")
        if not 1 <= self.port <= 65535 or not re.fullmatch(r"[a-zA-Z0-9.-]+", self.host) or not re.fullmatch(r"[a-zA-Z0-9_-]+", self.user):
            raise ValueError("Invalid fixed SSH destination")
        for path in [self.identity, self.known_hosts]:
            info = path.stat()
            if info.st_uid != os.getuid() or not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077:
                raise ValueError("SSH identity and host keys must be owner-only regular files")
        self.stop = asyncio.Event()
        self.counts = {"forwarded": 0, "errors": 0, "connections": 0}
        self.state_path = Path(os.environ.get("LINK_STATE_PATH", ".runtime/link-state.json"))

    def state(self, ready: bool):
        value = {"ready": ready, "updated_at": time.time(), **self.counts,
                 "operator_host_required": False, "transport": "restricted-machine-ssh"}
        self.state_path.parent.mkdir(mode=0o700, exist_ok=True)
        temporary = self.state_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(value) + "\n")
        temporary.chmod(0o600)
        temporary.replace(self.state_path)

    async def session(self, client):
        process = await asyncio.create_subprocess_exec("ssh", "-T", "-o", "BatchMode=yes",
            "-o", "IdentitiesOnly=yes", "-o", "StrictHostKeyChecking=yes",
            "-o", f"UserKnownHostsFile={self.known_hosts}", "-o", "ConnectTimeout=10",
            "-o", "ServerAliveInterval=10", "-o", "ServerAliveCountMax=3",
            "-i", str(self.identity), "-p", str(self.port), f"{self.user}@{self.host}",
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL, limit=MAX_FRAME + 1)
        lock = asyncio.Lock()
        requests = set()
        ready = False

        async def write(value):
            data = PREFIX + json.dumps(value, separators=(",", ":")).encode() + b"\n"
            if len(data) > MAX_FRAME:
                raise ValueError("Frame too large")
            async with lock:
                process.stdin.write(data)
                await asyncio.wait_for(process.stdin.drain(), 10)

        async def handle(value):
            status, body = 503, b'{"error":"link upstream unavailable"}'
            try:
                path, auth = value.get("path", ""), value.get("authorization", "")
                data = base64.b64decode(value.get("body", ""), validate=True)
                if not allowed(path) or len(data) > MAX_BODY or not isinstance(auth, str) or len(auth) > 1024 or "\r" in auth or "\n" in auth:
                    status, body = 403, b'{"error":"link request denied"}'
                else:
                    # One upstream send only. A lost claim acknowledgement is
                    # resolved through lease expiry, never a hidden POST replay.
                    async with client.stream("POST", self.upstream + path, content=data,
                        headers={"Authorization": auth, "Content-Type": "application/json"}) as result:
                        status = result.status_code
                        chunks = bytearray()
                        async for chunk in result.aiter_bytes():
                            chunks.extend(chunk)
                            if len(chunks) > MAX_BODY:
                                raise ValueError("Upstream response too large")
                        body = bytes(chunks)
            except (ValueError, httpx.HTTPError):
                status, body = 503, b'{"error":"link request unavailable"}'
            self.counts["forwarded"] += 1
            self.counts["errors"] += int(status >= 500)
            await write({"type": "response", "id": value["id"], "status": status,
                         "body": base64.b64encode(body).decode()})

        async def heartbeat():
            while True:
                await asyncio.sleep(5)
                await write({"type": "ping"})
                self.state(ready)

        ping = asyncio.create_task(heartbeat())
        try:
            while not self.stop.is_set():
                line = await asyncio.wait_for(process.stdout.readline(), timeout=20)
                if not line:
                    break
                if len(line) > MAX_FRAME or not line.startswith(PREFIX):
                    raise ValueError("Invalid remote protocol")
                value = json.loads(line[len(PREFIX):])
                if value.get("type") == "ready":
                    ready = True
                    self.counts["connections"] += 1
                    self.state(True)
                    print(json.dumps({"event": "pod_link_ready", **self.counts}), flush=True)
                elif value.get("type") == "request":
                    if not re.fullmatch(r"[0-9a-f]{32}", value.get("id", "")):
                        raise ValueError("Invalid request ID")
                    if len(requests) >= 128:
                        await write({"type": "response", "id": value["id"], "status": 503,
                                     "body": base64.b64encode(b'{"error":"link capacity reached"}').decode()})
                    else:
                        task = asyncio.create_task(handle(value))
                        requests.add(task)
                        task.add_done_callback(requests.discard)
            return ready
        finally:
            ping.cancel()
            with contextlib.suppress(Exception, asyncio.CancelledError):
                await ping
            if ready:
                with contextlib.suppress(Exception):
                    await write({"type": "close"})
            self.state(False)
            for task in requests:
                task.cancel()
            await asyncio.gather(*requests, return_exceptions=True)
            if process.returncode is None:
                process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), 5)
                except asyncio.TimeoutError:
                    process.kill()
                    await process.wait()

    async def run(self):
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self.stop.set)
        delay = 1
        async with httpx.AsyncClient(timeout=httpx.Timeout(8, connect=1),
                limits=httpx.Limits(max_connections=128, max_keepalive_connections=64), trust_env=False) as client:
            while not self.stop.is_set():
                try:
                    ready = await self.session(client)
                    delay = 1 if ready else min(15, delay * 2)
                except (OSError, ValueError, asyncio.TimeoutError) as exc:
                    self.state(False)
                    print(json.dumps({"event": "pod_link_reconnect", "reason": type(exc).__name__}), flush=True)
                    delay = min(15, delay * 2)
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self.stop.wait(), delay + random.random())


if __name__ == "__main__":
    asyncio.run(Broker().run())
