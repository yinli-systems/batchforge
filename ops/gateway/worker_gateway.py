"""Private replica router. Never replays a mutation after an uncertain outcome.

This is an app-local edge, not a replacement for a redundant external load
balancer. Bind loopback unless a separately secured ingress is configured.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, suppress
from collections import Counter
import os
import re
import secrets
import time
from urllib.parse import urlsplit

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
import httpx
from starlette.background import BackgroundTask
from starlette.requests import ClientDisconnect

WORKER_PATH = re.compile(r"^/internal/tasks/task_[0-9a-f]{32}/(heartbeat|complete|fail)$")
HOP_HEADERS = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
               "te", "trailer", "transfer-encoding", "upgrade", "host"}
OUTCOME_HEADER = "X-BatchForge-Outcome"
OUTCOME_VERSION_HEADER = "X-BatchForge-Outcome-Version"
UPLOAD_ATTEMPT_HEADER = "X-BatchForge-Upload-Attempt"
PROOF_HEADERS = {name.lower() for name in (OUTCOME_HEADER, OUTCOME_VERSION_HEADER, UPLOAD_ATTEMPT_HEADER)}
UPLOAD_NONCE = re.compile(r"[0-9a-f]{32}\Z")
METHODS = {"GET", "HEAD", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"}


def route_category(path):
    if path == "v1/files":
        return "files"
    if path == "v1/batches":
        return "batches"
    return "worker" if worker_path("/" + path) else "other"


def worker_path(path: str) -> bool:
    return path in {"/internal/workers/register", "/internal/tasks/claim"} or bool(WORKER_PATH.fullmatch(path))


def create_app(upstreams: list[str] | None = None, *, mode: str | None = None,
               token: str | None = None, transport=None) -> FastAPI:
    upstreams = upstreams or os.environ.get("GATEWAY_UPSTREAMS", "http://127.0.0.1:8082,http://127.0.0.1:8083").split(",")
    upstreams = [value.strip().rstrip("/") for value in upstreams]
    for value in upstreams:
        parsed = urlsplit(value)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.query or parsed.fragment or parsed.path:
            raise ValueError("Gateway upstreams must be fixed HTTP(S) origins without credentials")
    if not upstreams or len(upstreams) != len(set(upstreams)):
        raise ValueError("Gateway needs distinct upstreams")
    mode = mode or os.environ.get("GATEWAY_MODE", "private")
    if mode not in {"private", "worker"}:
        raise ValueError("GATEWAY_MODE must be private or worker")
    token = token if token is not None else os.environ.get("BATCHFORGE_WORKER_TOKEN", "")
    if mode == "worker" and len(token) < 24:
        raise ValueError("Worker-only edge requires an independent worker token")

    @asynccontextmanager
    async def lifespan(app):
        upstream_timeout = float(os.environ.get('GATEWAY_UPSTREAM_TIMEOUT_SECONDS', '120'))
        if not 5 <= upstream_timeout <= 600:
            raise ValueError('GATEWAY_UPSTREAM_TIMEOUT_SECONDS must be between 5 and 600')
        app.state.client = httpx.AsyncClient(timeout=httpx.Timeout(upstream_timeout, connect=1, pool=2),
            limits=httpx.Limits(max_connections=256, max_keepalive_connections=64),
            trust_env=False, transport=transport, follow_redirects=False)
        app.state.healthy = set()
        app.state.failed_until = {}
        app.state.next = 0
        app.state.lock = asyncio.Lock()
        app.state.outcomes = Counter()
        app.state.probes = Counter({"ready": 0, "unavailable": 0, "http_error": 0, "cooldown": 0})

        async def probe_one(url):
            error = False
            try:
                response = await app.state.client.get(url + "/readyz", timeout=2)
                ready = response.status_code == 200
            except httpx.HTTPError:
                ready = False; error = True
            if ready and app.state.failed_until.get(url, 0) <= time.monotonic():
                app.state.healthy.add(url)
                app.state.probes["ready"] += 1
            else:
                app.state.healthy.discard(url)
                reason = "http_error" if error else "cooldown" if ready else "unavailable"
                app.state.probes[reason] += 1

        async def probe():
            await asyncio.gather(*(probe_one(url) for url in upstreams))

        async def monitor():
            while True:
                await probe()
                await asyncio.sleep(.5)

        await probe()
        task = asyncio.create_task(monitor(), name="replica-readiness")
        try:
            yield
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
            await app.state.client.aclose()

    app = FastAPI(title="BatchForge replica router", docs_url=None, redoc_url=None,
                  openapi_url=None, lifespan=lifespan)

    def observed(request, path, outcome, reason, stage):
        # Every label comes from this module's finite constants/categories.
        method = request.method if request.method in METHODS else "OTHER"
        app.state.outcomes[(method, route_category(path), outcome, reason, stage)] += 1

    def refused(request, path, message, status, reason, stage):
        observed(request, path, "not_forwarded", reason, stage)
        return JSONResponse({"error": message}, status_code=status)

    @app.get("/internal/gateway/status")
    async def status(request: Request):
        received = request.headers.get("authorization", "")
        if len(token) < 24 or not secrets.compare_digest(received.encode(), ("Bearer " + token).encode()):
            return JSONResponse({"error": "unauthorized"}, status_code=401, headers={"Cache-Control": "no-store"})
        return JSONResponse({"component": "replica-router", "diagnostic_version": 1,
            "ready_replicas": len(app.state.healthy), "configured_replicas": len(upstreams),
            "readiness_observations": dict(app.state.probes),
            "outcome_events": [{"method": key[0], "route_category": key[1], "outcome": key[2],
                "reason": key[3], "stage": key[4], "count": count}
                for key, count in sorted(app.state.outcomes.items())],
            "counter_semantics": "Process-cumulative events; an upstream response and later stream failure can describe the same request",
            "mutation_replays": 0}, headers={"Cache-Control": "no-store"})

    @app.get("/healthz")
    async def health():
        return {"status": "ok", "component": "replica-router"}

    @app.get("/readyz")
    async def ready():
        count = len(app.state.healthy)
        return JSONResponse({"status": "ready" if count else "unavailable",
                             "ready_replicas": count}, status_code=200 if count else 503,
                            headers={"Cache-Control": "no-store"})

    @app.api_route("/{path:path}", methods=["GET", "HEAD", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"])
    async def forward(path: str, request: Request):
        if mode == "worker":
            if request.method != "POST" or not worker_path("/" + path) or request.url.query:
                return refused(request, path, "route denied", 404, "route_denied", "policy")
            received = request.headers.get("authorization", "")
            if not secrets.compare_digest(received.encode(), ("Bearer " + token).encode()):
                return refused(request, path, "unauthorized", 401, "unauthorized", "policy")
        limit = 11 * 1024 * 1024 if path == "v1/files" else 4 * 1024 * 1024
        try:
            if int(request.headers.get("content-length", "0")) > limit:
                return refused(request, path, "request too large", 413, "request_too_large", "request_headers")
        except ValueError:
            return refused(request, path, "invalid content length", 400, "invalid_content_length", "request_headers")
        body = bytearray()
        try:
            async for chunk in request.stream():
                body.extend(chunk)
                if len(body) > limit:
                    return refused(request, path, "request too large", 413, "request_too_large", "request_body")
        except ClientDisconnect:
            return refused(request, path, "request body unavailable", 400, "client_disconnected", "request_body")
        except asyncio.CancelledError:
            observed(request, path, "not_forwarded", "request_body_cancelled", "request_body")
            raise
        async with app.state.lock:
            candidates = [url for url in upstreams if url in app.state.healthy]
            if not candidates:
                observed(request, path, "not_forwarded", "no_ready_upstream", "select_upstream")
                headers = {"Retry-After": "1", "Cache-Control": "no-store"}
                nonces = request.headers.getlist(UPLOAD_ATTEMPT_HEADER)
                # This is the sole replay-proof producer. No upstream send has
                # been attempted. Echo one exact per-attempt upload challenge.
                if (request.method == "POST" and path == "v1/files" and not request.url.query
                        and len(nonces) == 1 and UPLOAD_NONCE.fullmatch(nonces[0])):
                    headers.update({OUTCOME_HEADER: "not-forwarded", OUTCOME_VERSION_HEADER: "1",
                                    UPLOAD_ATTEMPT_HEADER: nonces[0]})
                return JSONResponse({"error": {"code": "gateway_not_forwarded", "stage": "select_upstream",
                    "reason": "no_ready_upstream", "message": "No writable API replica is ready"}},
                    status_code=503, headers=headers)
            target = candidates[app.state.next % len(candidates)]
            app.state.next += 1
        headers = {key: value for key, value in request.headers.items() if key.lower() not in HOP_HEADERS | PROOF_HEADERS}
        # Do not accept or forward client-supplied proxy trust metadata.
        headers = {key: value for key, value in headers.items()
                   if not key.lower().startswith(("x-forwarded-", "forwarded"))}
        suffix = request.url.path + ("?" + request.url.query if request.url.query else "")
        try:
            outbound = app.state.client.build_request(request.method, target + suffix,
                headers=headers, content=bytes(body))
        except (httpx.HTTPError, httpx.InvalidURL, ValueError):
            observed(request, path, "not_forwarded", "request_build_error", "build_request")
            return JSONResponse({"error": "upstream request could not be built"}, status_code=503)
        try:
            response = await app.state.client.send(outbound, stream=True)
        except asyncio.CancelledError:
            observed(request, path, "uncertain", "upstream_send_cancelled", "upstream_send")
            raise
        except httpx.HTTPError:
            observed(request, path, "uncertain", "upstream_send_error", "upstream_send")
            app.state.failed_until[target] = time.monotonic() + 1
            app.state.healthy.discard(target)
            # A claim/create/complete may already have committed. Only its
            # owner can decide how to retry with the correct idempotency/fence.
            return JSONResponse({"error": "upstream outcome uncertain; retry using the same operation identity"},
                                status_code=503, headers={"Retry-After": "1"})
        if response.status_code == 503:
            app.state.failed_until[target] = time.monotonic() + 1
            app.state.healthy.discard(target)
        status_class = str(response.status_code // 100) + "xx" if 100 <= response.status_code <= 599 else "other"
        observed(request, path, "upstream_response", "status_" + status_class, "upstream_headers")
        response_headers = {key: value for key, value in response.headers.items()
                            if key.lower() not in HOP_HEADERS | PROOF_HEADERS}
        async def stream():
            try:
                async for chunk in response.aiter_raw():
                    yield chunk
            except asyncio.CancelledError:
                observed(request, path, "uncertain", "downstream_stream_cancelled", "upstream_body")
                raise
            except Exception:
                observed(request, path, "uncertain", "upstream_stream_error", "upstream_body")
                raise
            finally:
                await response.aclose()
        return StreamingResponse(stream(), status_code=response.status_code,
            headers=response_headers, background=BackgroundTask(response.aclose))

    return app


app = create_app()
