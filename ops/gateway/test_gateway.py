"""Replica routing and ambiguous POST semantics exercised via ASGI HTTP."""
import httpx
import json
import time
import pytest
from fastapi.testclient import TestClient

from ops.gateway.worker_gateway import (create_app, OUTCOME_HEADER, OUTCOME_VERSION_HEADER,
    UPLOAD_ATTEMPT_HEADER, PROOF_HEADERS)


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
        assert all(name not in result.headers for name in PROOF_HEADERS)
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


TOKEN = "private-diagnostic-token-" + "x" * 32
NONCE = "0123456789abcdef" * 2


def unavailable_app():
    calls = []
    def upstream(request):
        if request.url.path == "/readyz": return httpx.Response(503)
        calls.append(request)
        raise AssertionError("No upstream request may be sent")
    return create_app(["http://api-a", "http://api-b"], token=TOKEN,
        transport=httpx.MockTransport(upstream)), calls


def diagnostics(client):
    result = client.get("/internal/gateway/status", headers={"Authorization": "Bearer " + TOKEN})
    assert result.status_code == 200 and result.headers["Cache-Control"] == "no-store"
    return result.json()


def test_only_unsent_upload_has_exact_current_attempt_proof_and_fixed_diagnostics():
    app, calls = unavailable_app()
    with TestClient(app) as client:
        response = client.post("/v1/files", content=b"private file contents",
            headers={UPLOAD_ATTEMPT_HEADER: NONCE, "Authorization": "Bearer private-tenant-key"})
        assert response.status_code == 503
        assert response.headers[OUTCOME_HEADER] == "not-forwarded"
        assert response.headers[OUTCOME_VERSION_HEADER] == "1"
        assert response.headers[UPLOAD_ATTEMPT_HEADER] == NONCE
        assert response.json()["error"] == {"code": "gateway_not_forwarded", "stage": "select_upstream",
            "reason": "no_ready_upstream", "message": "No writable API replica is ready"}
        assert response.headers["Retry-After"] == "1" and not calls
        report = diagnostics(client)
        assert report["ready_replicas"] == 0 and report["configured_replicas"] == 2
        assert report["mutation_replays"] == 0
        assert report["outcome_events"] == [{"method": "POST", "route_category": "files", "outcome": "not_forwarded",
            "reason": "no_ready_upstream", "stage": "select_upstream", "count": 1}]
        assert not any(secret in json.dumps(report) for secret in (TOKEN, NONCE, "private file contents", "private-tenant-key", "api-a", "api-b"))


@pytest.mark.parametrize("headers", [{}, {UPLOAD_ATTEMPT_HEADER: "invalid"},
    {UPLOAD_ATTEMPT_HEADER: NONCE.upper()}, {UPLOAD_ATTEMPT_HEADER: "a"*31},
    [(UPLOAD_ATTEMPT_HEADER, NONCE), (UPLOAD_ATTEMPT_HEADER, NONCE)]])
def test_missing_malformed_or_duplicate_nonce_does_not_produce_replay_proof(headers):
    app, calls = unavailable_app()
    with TestClient(app) as client:
        response = client.post("/v1/files", content=b"file", headers=headers)
        assert response.status_code == 503 and not calls
        assert all(name not in response.headers for name in PROOF_HEADERS)


@pytest.mark.parametrize("method,path", [("POST", "/v1/batches"), ("GET", "/v1/files"),
    ("POST", "/v1/files?untrusted=value"), ("POST", "/v1/files/other")])
def test_proof_is_restricted_to_exact_upload_operation(method, path):
    app, calls = unavailable_app()
    with TestClient(app) as client:
        response = client.request(method, path, content=b"file", headers={UPLOAD_ATTEMPT_HEADER: NONCE})
        assert response.status_code == 503 and not calls
        assert all(name not in response.headers for name in PROOF_HEADERS)


@pytest.mark.parametrize("status", [200, 400, 503])
def test_upstream_cannot_forge_proof_and_reserved_headers_are_not_forwarded(status):
    calls = []
    def upstream(request):
        if request.url.path == "/readyz": return httpx.Response(200)
        calls.append(request)
        assert all(name not in request.headers for name in PROOF_HEADERS)
        return httpx.Response(status, stream=ResponseStream(), headers={OUTCOME_HEADER: "not-forwarded",
            OUTCOME_VERSION_HEADER: "1", UPLOAD_ATTEMPT_HEADER: NONCE, "content-type": "application/json"})
    app = create_app(["http://api-a"], token=TOKEN, transport=httpx.MockTransport(upstream))
    with TestClient(app) as client:
        response = client.post("/v1/files", content=b"upload", headers={UPLOAD_ATTEMPT_HEADER: NONCE,
            OUTCOME_HEADER: "not-forwarded", OUTCOME_VERSION_HEADER: "1"})
        assert response.status_code == status and len(calls) == 1
        assert all(name not in response.headers for name in PROOF_HEADERS)
        events = diagnostics(client)["outcome_events"]
        assert events == [{"method": "POST", "route_category": "files", "outcome": "upstream_response",
            "reason": "status_"+str(status//100)+"xx", "stage": "upstream_headers", "count": 1}]


@pytest.mark.parametrize("error_type", [httpx.ConnectError, httpx.ReadError, httpx.ReadTimeout, httpx.PoolTimeout])
def test_any_send_error_is_uncertain_never_replayed_or_given_proof(error_type):
    calls = []
    def upstream(request):
        if request.url.path == "/readyz": return httpx.Response(200)
        calls.append(request)
        raise error_type("private upstream diagnostic", request=request)
    app = create_app(["http://api-a", "http://api-b"], token=TOKEN, transport=httpx.MockTransport(upstream))
    with TestClient(app) as client:
        response = client.post("/v1/files", content=b"upload", headers={UPLOAD_ATTEMPT_HEADER: NONCE})
        assert response.status_code == 503 and len(calls) == 1
        assert all(name not in response.headers for name in PROOF_HEADERS)
        report = diagnostics(client)
        assert report["outcome_events"] == [{"method": "POST", "route_category": "files", "outcome": "uncertain",
            "reason": "upstream_send_error", "stage": "upstream_send", "count": 1}]
        assert "private upstream diagnostic" not in response.text + json.dumps(report)


def test_status_requires_worker_token_in_private_and_worker_mode_and_never_proxies():
    for mode in ("private", "worker"):
        app = create_app(["http://api-a"], mode=mode, token=TOKEN,
            transport=httpx.MockTransport(lambda request: httpx.Response(200)))
        with TestClient(app) as client:
            assert client.get("/internal/gateway/status").status_code == 401
            assert client.get("/internal/gateway/status", headers={"Authorization": "Bearer wrong"}).status_code == 401
            assert diagnostics(client)["outcome_events"] == []
    app, _ = unavailable_app()
    # An absent/short private-mode token must never authenticate an empty bearer.
    app = create_app(["http://api-a"], token="", transport=httpx.MockTransport(lambda request: httpx.Response(503)))
    with TestClient(app) as client:
        assert client.get("/internal/gateway/status", headers={"Authorization": "Bearer "}).status_code == 401


def test_unbounded_paths_queries_and_tenant_headers_do_not_expand_labels():
    app, calls = unavailable_app()
    with TestClient(app) as client:
        for index in range(50):
            response = client.post(f"/untrusted-tenant-{index}/private-path?secret={index}", content=b"sensitive",
                headers={"Authorization": f"Bearer secret-{index}"})
            assert response.status_code == 503
        report = diagnostics(client)
        assert len(report["outcome_events"]) == 1 and report["outcome_events"][0]["count"] == 50
        assert report["outcome_events"][0]["route_category"] == "other"
        assert not any(value in json.dumps(report) for value in ("private-path", "untrusted-tenant", "sensitive", "secret"))
        assert not calls


def test_local_header_or_body_rejections_never_receive_no_ready_proof():
    app, calls = unavailable_app()
    with TestClient(app) as client:
        for length, expected in ((str(11*1024*1024+1), 413), ("malformed", 400)):
            response = client.post("/v1/files", content=b"data", headers={UPLOAD_ATTEMPT_HEADER: NONCE, "Content-Length": length})
            assert response.status_code == expected and all(name not in response.headers for name in PROOF_HEADERS)
        assert not calls
        assert {event["stage"] for event in diagnostics(client)["outcome_events"]} == {"request_headers"}


def test_transient_empty_ready_set_recovers_without_replaying_rejected_upload():
    ready, calls = [False], []
    def upstream(request):
        if request.url.path == "/readyz": return httpx.Response(200 if ready[0] else 503)
        calls.append(request)
        return httpx.Response(200, stream=ResponseStream())
    app = create_app(["http://api-a"], token=TOKEN, transport=httpx.MockTransport(upstream))
    with TestClient(app) as client:
        rejected = client.post("/v1/files", content=b"upload", headers={UPLOAD_ATTEMPT_HEADER: NONCE})
        assert rejected.headers[OUTCOME_HEADER] == "not-forwarded" and not calls
        assert client.get("/readyz").status_code == 503
        ready[0] = True
        deadline = time.monotonic() + 3
        while client.get("/readyz").status_code != 200:
            assert time.monotonic() < deadline
            time.sleep(.02)
        assert not calls  # Readiness recovery itself never repeats the POST.
        accepted = client.post("/v1/files", content=b"upload", headers={UPLOAD_ATTEMPT_HEADER: "f"*32})
        assert accepted.status_code == 200 and len(calls) == 1
        assert all(name not in accepted.headers for name in PROOF_HEADERS)
        report = diagnostics(client)
        assert report["readiness_observations"]["ready"] > 0 and report["readiness_observations"]["unavailable"] > 0


def test_stream_failure_is_diagnosed_without_adding_proof_or_replaying():
    calls = []
    class BrokenStream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b"partial response"
            raise httpx.ReadError("secret response diagnostic")
    def upstream(request):
        if request.url.path == "/readyz": return httpx.Response(200)
        calls.append(request)
        return httpx.Response(200, stream=BrokenStream(), headers={OUTCOME_HEADER: "not-forwarded",
            OUTCOME_VERSION_HEADER: "1", UPLOAD_ATTEMPT_HEADER: NONCE})
    app = create_app(["http://api-a"], token=TOKEN, transport=httpx.MockTransport(upstream))
    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.post("/v1/files", content=b"upload", headers={UPLOAD_ATTEMPT_HEADER: NONCE})
        assert all(name not in response.headers for name in PROOF_HEADERS) and len(calls) == 1
        events = diagnostics(client)["outcome_events"]
        assert any(event["outcome"] == "uncertain" and event["reason"] == "upstream_stream_error"
            and event["stage"] == "upstream_body" and event["count"] == 1 for event in events)
        assert "secret response diagnostic" not in json.dumps(events)
