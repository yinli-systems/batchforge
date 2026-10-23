"""Exercise the actual stdio/HTTP boundary without an SSH server or GPU."""
from __future__ import annotations

import base64
from concurrent.futures import ThreadPoolExecutor
import importlib.util
import json
from pathlib import Path
import socket
import subprocess
import sys
import unittest
import urllib.error
import urllib.request

PREFIX = b"BATCHFORGE_RELAY_V1 "


class RelayBoundaryTest(unittest.TestCase):
    def test_local_allowlist_matches_worker_contract(self):
        spec = importlib.util.spec_from_file_location("batchforge_local_relay", Path(__file__).with_name("relay_local.py"))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        for path in ("/internal/workers/register", "/internal/tasks/claim",
                     *[f"/internal/tasks/task_{'a' * 32}/{operation}" for operation in ("heartbeat", "complete", "fail")]):
            self.assertTrue(module.allowed(path), path)
        for path in ("/v1/batches", "/internal/tasks/claim?other=1",
                     "/internal/tasks/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa/complete",
                     "/internal/tasks/task_" + "a" * 32 + "/delete"):
            self.assertFalse(module.allowed(path), path)

    def test_framing_auth_route_rejection_and_disconnect(self):
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        child = subprocess.Popen([sys.executable, str(Path(__file__).with_name("relay_remote.py")), "--port", str(port)],
                                 stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        try:
            ready = json.loads(child.stdout.readline().removeprefix(PREFIX))
            self.assertEqual(ready, {"type": "ready", "bind": "127.0.0.1", "port": port})
            task_id = "task_" + "a" * 32
            request = urllib.request.Request(f"http://127.0.0.1:{port}/internal/tasks/{task_id}/complete",
                data=b'{"worker_id":"test-worker"}', headers={"Authorization": "Bearer test-only-token"}, method="POST")
            with ThreadPoolExecutor(max_workers=1) as pool:
                response = pool.submit(opener.open, request, timeout=5)
                frame = json.loads(child.stdout.readline().removeprefix(PREFIX))
                self.assertEqual(frame["path"], f"/internal/tasks/{task_id}/complete")
                self.assertEqual(frame["authorization"], "Bearer test-only-token")
                self.assertEqual(base64.b64decode(frame["body"]), request.data)
                body = b'{"tasks":[],"test_only":true}'
                child.stdin.write(PREFIX + json.dumps({"type": "response", "id": frame["id"], "status": 200,
                    "body": base64.b64encode(body).decode()}).encode() + b"\n")
                child.stdin.flush()
                with response.result(timeout=5) as result:
                    self.assertEqual(result.read(), body)
            for path in ("/v1/batches", "/metrics", "/internal/tasks/../../v1/batches", "/internal/tasks/claim?url=https://example.com",
                         "/internal/tasks/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa/complete"):
                with self.assertRaises(urllib.error.HTTPError) as error:
                    opener.open(urllib.request.Request(f"http://127.0.0.1:{port}" + path, data=b"{}", method="POST"), timeout=5)
                self.assertEqual(error.exception.code, 403)
            child.stdin.close()
            self.assertEqual(child.wait(timeout=5), 0)
        finally:
            if child.poll() is None:
                child.terminate()
                child.wait(timeout=5)
            child.stdout.close()
            child.stderr.close()


if __name__ == "__main__":
    unittest.main()
