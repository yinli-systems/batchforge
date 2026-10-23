"""Replica routing and ambiguous POST semantics exercised via ASGI HTTP."""
import httpx
from fastapi.testclient import TestClient

from ops.gateway.worker_gateway import create_app


class ResponseStream(httpx.AsyncByteStream):
    async def __aiter__(self):
        yield b'{"id":"batch-stable"}'


def test_failed_mutation_is_not_replayed_and_surviving_replica_handles_next_call():
    calls = []
    def upstream(request):
        if request.url.path == "/readyz":
            return httpx.Response(200)
        calls.append((request.url.host, request.method, bytes(request.content)))
        if request.url.host == "api-a":
            raise httpx.ReadError("lost response after commit", request=request)
        return httpx.Response(200, stream=ResponseStream(), headers={"content-type": "application/json"})
    app = create_app(["http://api-a", "http://api-b"], transport=httpx.MockTransport(upstream))
    with TestClient(app) as client:
        result = client.post("/v1/batches", content=b'{"id":"stable"}', headers={"Idempotency-Key": "stable"})
        assert result.status_code == 503
        assert calls == [("api-a", "POST", b'{"id":"stable"}')]
        result = client.post("/v1/batches", content=b'{"id":"stable"}', headers={"Idempotency-Key": "stable"})
        assert result.status_code == 200
        assert calls[-1][0] == "api-b" and len(calls) == 2


def test_worker_edge_denies_tenant_routes_invalid_auth_and_oversized_requests():
    calls = []
    def upstream(request):
        if request.url.path != "/readyz":
            calls.append(request.url.path)
        return httpx.Response(200, stream=ResponseStream())
    token = "test-only-worker-token-" + "x" * 24
    app = create_app(["http://api-a"], mode="worker", token=token, transport=httpx.MockTransport(upstream))
    with TestClient(app) as client:
        assert client.post("/v1/batches").status_code == 404
        assert client.post("/internal/tasks/claim").status_code == 401
        headers = {"Authorization": "Bearer " + token}
        assert client.post("/internal/tasks/claim?url=bad", headers=headers).status_code == 404
        assert client.post("/internal/tasks/claim", headers={**headers, "Content-Length": str(4*1024*1024+1)}).status_code == 413
        assert client.post("/internal/tasks/claim", content=b"{}", headers=headers).status_code == 200
        assert calls == ["/internal/tasks/claim"]
