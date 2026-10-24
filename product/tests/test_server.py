import json

import httpx
from fastapi.testclient import TestClient

from product.server import create_app

MODEL = "Qwen/Qwen3-235B-A22B-Instruct-2507-FP8"


class FakeBatchForge:
    """Minimal stand-in for the public API: files, batches and their results."""

    def __init__(self):
        self.files = {}
        self.batches = {}
        self.seen_headers = []
        self.counter = 0

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.seen_headers.append(dict(request.headers))
        path = request.url.path
        if request.method == "POST" and path == "/v1/files":
            self.counter += 1
            fid = f"file_{self.counter}"
            self.files[fid] = _multipart_file(request)
            return httpx.Response(200, json={"id": fid, "object": "file"})
        if request.method == "POST" and path == "/v1/batches":
            body = json.loads(request.content)
            self.counter += 1
            bid = f"batch_{self.counter}"
            lines = self.files[body["input_file_id"]].decode().splitlines()
            batch = {
                "id": bid, "object": "batch", "status": "in_progress",
                "input_file_id": body["input_file_id"], "output_file_id": None, "error_file_id": None,
                "metadata": body["metadata"], "created_at": 1, "completed_at": None,
                "request_counts": {"total": len(lines), "completed": 0, "failed": 0, "cancelled": 0,
                                   "running": 0, "pending": len(lines)},
                "usage": {"prompt_tokens": 0, "completion_tokens": 0},
            }
            self.batches[bid] = batch
            return httpx.Response(200, json=batch)
        if request.method == "GET" and path.startswith("/v1/batches/"):
            bid = path.rsplit("/", 1)[-1]
            if bid not in self.batches:
                return httpx.Response(404, json={"error": {"message": "Batch not found", "code": "not_found"}})
            return httpx.Response(200, json=self.batches[bid])
        if request.method == "GET" and path.startswith("/v1/files/"):
            fid = path.split("/")[3]
            return httpx.Response(200, content=self.files[fid])
        return httpx.Response(404, json={"error": {"message": "unexpected", "code": "x"}})

    def finish(self, bid, output_lines, error_lines=()):
        batch = self.batches[bid]
        if output_lines:
            self.counter += 1
            self.files[f"file_{self.counter}"] = "\n".join(output_lines).encode()
            batch["output_file_id"] = f"file_{self.counter}"
        if error_lines:
            self.counter += 1
            self.files[f"file_{self.counter}"] = "\n".join(error_lines).encode()
            batch["error_file_id"] = f"file_{self.counter}"
        batch["status"] = "completed"
        batch["completed_at"] = 2
        batch["request_counts"].update(completed=len(output_lines), failed=len(error_lines), pending=0)
        batch["usage"] = {"prompt_tokens": 20, "completion_tokens": 5}


def _multipart_file(request: httpx.Request) -> bytes:
    """Return the body of the multipart part named "file" (not the purpose field)."""
    boundary = request.headers["content-type"].split("boundary=", 1)[1].encode()
    for part in request.read().split(b"--" + boundary):
        headers, separator, body = part.partition(b"\r\n\r\n")
        if separator and b'name="file"' in headers:
            return body[:-2] if body.endswith(b"\r\n") else body
    raise AssertionError("no file part in upload")


def _client(fake):
    upstream = httpx.AsyncClient(base_url="http://bf.test", transport=httpx.MockTransport(fake.handler),
                                 headers={"Authorization": "Bearer tenant-key"})
    return TestClient(create_app(upstream=upstream, model=MODEL))


def test_templates_are_listed():
    client = _client(FakeBatchForge())
    ids = [t["id"] for t in client.get("/api/templates").json()["data"]]
    assert ids == ["sentiment", "summary", "extract"]


def test_create_job_uploads_file_and_tags_template_without_leaking_key():
    fake = FakeBatchForge()
    client = _client(fake)
    response = client.post("/api/jobs", json={"template": "sentiment", "text": "great\nawful",
                                             "idempotency_key": "k-1"})
    assert response.status_code == 201
    job = response.json()
    assert job["template"] == "sentiment" and job["total"] == 2 and job["status"] == "in_progress"
    batch = fake.batches[job["id"]]
    assert batch["metadata"] == {"product_template": "sentiment"}
    stored = fake.files[batch["input_file_id"]].decode().splitlines()
    assert json.loads(stored[0])["body"]["model"] == MODEL
    create_headers = [h for h in fake.seen_headers if h.get("idempotency-key")]
    assert create_headers and create_headers[0]["idempotency-key"] == "k-1"
    assert all(h.get("authorization") == "Bearer tenant-key" for h in fake.seen_headers)


def test_running_job_has_no_rows_yet():
    fake = FakeBatchForge()
    client = _client(fake)
    job = client.post("/api/jobs", json={"template": "sentiment", "text": "x"}).json()
    body = client.get(f"/api/jobs/{job['id']}").json()
    assert body["status"] == "in_progress" and "rows" not in body
    assert client.get(f"/api/jobs/{job['id']}/export.csv").status_code == 409


def test_finished_job_returns_rows_and_csv():
    fake = FakeBatchForge()
    client = _client(fake)
    job = client.post("/api/jobs", json={"template": "sentiment", "text": "great\nawful"}).json()
    out = [json.dumps({"id": "r1", "custom_id": "item-0001", "error": None, "response": {
        "status_code": 200, "request_id": "r1", "body": {
            "choices": [{"message": {"content": "positive"}}],
            "usage": {"prompt_tokens": 10, "completion_tokens": 1}}}})]
    err = [json.dumps({"id": "r2", "custom_id": "item-0002", "response": None,
                       "error": {"code": "engine_http_500", "message": "engine error"}})]
    fake.finish(job["id"], out, err)

    body = client.get(f"/api/jobs/{job['id']}").json()
    assert body["status"] == "completed" and body["progress"] == 1.0
    assert [r["status"] for r in body["rows"]] == ["completed", "failed"]
    assert body["rows"][0]["input"] == "great" and body["rows"][0]["text"] == "positive"

    csv_response = client.get(f"/api/jobs/{job['id']}/export.csv")
    assert csv_response.headers["content-type"].startswith("text/csv")
    assert "engine error" in csv_response.text


def test_validation_errors_are_400_with_message():
    client = _client(FakeBatchForge())
    response = client.post("/api/jobs", json={"template": "nope", "text": "x"})
    assert response.status_code == 400 and "Unknown template" in response.json()["error"]["message"]
    response = client.post("/api/jobs", json={"template": "sentiment", "text": "   \n  "})
    assert response.status_code == 400


def test_non_product_batch_is_not_exposed_as_a_job():
    fake = FakeBatchForge()
    fake.batches["batch_x"] = {"id": "batch_x", "status": "completed", "input_file_id": "f",
                               "output_file_id": None, "error_file_id": None, "metadata": {},
                               "created_at": 1, "completed_at": 2,
                               "request_counts": {"total": 0, "completed": 0, "failed": 0, "cancelled": 0,
                                                  "running": 0, "pending": 0},
                               "usage": {"prompt_tokens": 0, "completion_tokens": 0}}
    fake.files["f"] = b""
    client = _client(fake)
    assert client.get("/api/jobs/batch_x").status_code == 404


def test_upstream_outage_is_reported_without_crashing():
    def down(request):
        raise httpx.ConnectError("refused", request=request)

    upstream = httpx.AsyncClient(base_url="http://bf.test", transport=httpx.MockTransport(down))
    client = TestClient(create_app(upstream=upstream, model=MODEL))
    response = client.post("/api/jobs", json={"template": "sentiment", "text": "x"})
    assert response.status_code == 503
    assert "unreachable" in response.json()["error"]["message"]
