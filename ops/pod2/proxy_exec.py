#!/usr/bin/env python3
"""Run an app-owned script through Runpod's interactive SSH gateway.

The gateway does not support SCP, remote exec, or TCP forwarding. This helper
uses the already-authorized SSH session, disables terminal echo/history, and
transfers an optional source archive without copying SSH or GitHub credentials.
It does not change sshd configuration or authorized_keys.
"""
from __future__ import annotations

import argparse
import base64
import os
from pathlib import Path
import pty
import re
import select
import subprocess
import sys
import termios
import time
import uuid


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("script", type=Path, help="Local shell script; never echoed")
    parser.add_argument("--archive", type=Path)
    parser.add_argument("--target", default="t0256w11ortyw6-644123a2@ssh.runpod.io")
    parser.add_argument("--identity", default=str(Path.home() / ".ssh/id_ed25519"))
    parser.add_argument("--known-hosts", default="/private/tmp/batchforge_pod2_known_hosts")
    parser.add_argument("--timeout", type=int, default=1800)
    args = parser.parse_args()
    script = args.script.read_bytes()
    archive = args.archive.read_bytes() if args.archive else b""
    marker = uuid.uuid4().hex
    ready, done = f"BF_{marker}_READY".encode(), f"BF_{marker}_DONE:".encode()
    master, slave = pty.openpty()
    attrs = termios.tcgetattr(slave)
    attrs[3] &= ~(termios.ECHO | termios.ICANON)
    termios.tcsetattr(slave, termios.TCSANOW, attrs)
    child = subprocess.Popen([
        "ssh", "-tt", "-o", "BatchMode=yes", "-o", "ConnectTimeout=20",
        "-o", "ServerAliveInterval=30", "-o", "ServerAliveCountMax=3",
        "-o", "StrictHostKeyChecking=yes", "-o", f"UserKnownHostsFile={args.known_hosts}",
        "-i", args.identity, args.target,
    ], stdin=slave, stdout=slave, stderr=slave, start_new_session=True)
    os.close(slave)
    buffer = bytearray()
    deadline = time.monotonic() + args.timeout

    def read_until(expected: bytes, emit: bool = False) -> bytes:
        while time.monotonic() < deadline:
            if expected in buffer:
                result, _, remainder = bytes(buffer).partition(expected)
                buffer[:] = remainder
                if emit:
                    clean = re.sub(rb"\x1b\[[0-?]*[ -/]*[@-~]", b"", result)
                    clean = b"\n".join(line.strip(b"\r") for line in clean.splitlines() if line.strip()) + b"\n"
                    sys.stdout.buffer.write(clean)
                    sys.stdout.buffer.flush()
                return result
            readable, _, _ = select.select([master], [], [], 1)
            if readable:
                try:
                    data = os.read(master, 65536)
                except OSError:
                    data = b""
                if not data:
                    raise RuntimeError("SSH gateway closed before the completion marker")
                buffer.extend(data)
            if child.poll() is not None:
                raise RuntimeError(f"SSH exited with status {child.returncode}")
        raise TimeoutError("Runpod interactive command timed out")

    def send(data: bytes) -> None:
        view = memoryview(data)
        while view:
            if time.monotonic() > deadline:
                raise TimeoutError("Runpod upload timed out")
            readable, writable, _ = select.select([master], [master], [], 1)
            if readable:
                data = os.read(master, 65536)
                if not data:
                    raise RuntimeError("SSH gateway closed during upload")
                buffer.extend(data)
            if writable:
                n = os.write(master, view[:2048])
                view = view[n:]

    try:
        read_until(b"# ")
        send(("stty -echo; export PS1='' PS2=''; set +o history; bind 'set enable-bracketed-paste off'; "
              f"printf 'BF_%s_READY\\n' '{marker}'\n").encode())
        read_until(ready)
        payload = (
            "import base64,io,os,pathlib,subprocess,tarfile\n"
            "root=pathlib.Path('/root/batchforge-runtime/app')\n"
            "root.mkdir(parents=True,exist_ok=True,mode=0o700)\n"
            f"archive=base64.b64decode({base64.b64encode(archive).decode()!r})\n"
            "if archive:\n"
            " with tarfile.open(fileobj=io.BytesIO(archive),mode='r:*') as tf:\n"
            "  tf.extractall(root,filter='data')\n"
            f"script=base64.b64decode({base64.b64encode(script).decode()!r})\n"
            f"p=root/'.runtime'/'proxy-command-{marker}.sh'\n"
            "p.parent.mkdir(exist_ok=True,mode=0o700)\n"
            "p.write_bytes(script);p.chmod(0o600)\n"
            "rc=subprocess.run(['bash',str(p)],cwd=root).returncode\n"
            "p.unlink(missing_ok=True)\n"
            f"print('BF_{marker}_DONE:'+str(rc),flush=True)\n"
        )
        # Keep every input line below the terminal's canonical line limit.
        encoded = base64.b64encode(payload.encode()).decode()
        chunks = "\n".join(encoded[i:i+128] for i in range(0, len(encoded), 128))
        wrapper = "python3 - <<'BF_PAYLOAD'\nimport base64\nexec(base64.b64decode('''\n" + chunks + "\n'''))\nBF_PAYLOAD\n"
        send(wrapper.encode())
        read_until(done, emit=True)
        status = read_until(b"\n").strip()
        send(b"exit\n")
        return int(status)
    finally:
        os.close(master)
        try:
            child.wait(timeout=10)
        except subprocess.TimeoutExpired:
            child.terminate()


if __name__ == "__main__":
    raise SystemExit(main())
