"""Private replica router. Never replays a mutation after an uncertain outcome.

This is an app-local edge, not a replacement for a redundant external load
balancer. Bind loopback unless a separately secured ingress is configured.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, suppress
import os
import re
import secrets
import time
from urllib.parse import urlsplit

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
import httpx
from starlette.background import BackgroundTask

WORKER_PATH = re.compile(r"^/internal/tasks/task_[0-9a-f]{32}/(heartbeat|complete|fail)$")
HOP_HEADERS = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
               "te", "trailer", "transfer-encoding", "upgrade", "host"}


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

        async def probe_one(url):
            try:
                response = await app.state.client.get(url + "/readyz", timeout=2)
                ready = response.status_code == 200
            except httpx.HTTPError:
                ready = False
            if ready and app.state.failed_until.get(url, 0) <= time.monotonic():
                app.state.healthy.add(url)
            else:
                app.state.healthy.discard(url)

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
                return JSONResponse({"error": "route denied"}, status_code=404)
            received = request.headers.get("authorization", "")
            if not secrets.compare_digest(received.encode(), ("Bearer " + token).encode()):
                return JSONResponse({"error": "unauthorized"}, status_code=401)
        limit = 11 * 1024 * 1024 if path == "v1/files" else 4 * 1024 * 1024
        try:
            if int(request.headers.get("content-length", "0")) > limit:
                return JSONResponse({"error": "request too large"}, status_code=413)
        except ValueError:
            return JSONResponse({"error": "invalid content length"}, status_code=400)
        body = bytearray()
        async for chunk in request.stream():
            body.extend(chunk)
            if len(body) > limit:
                return JSONResponse({"error": "request too large"}, status_code=413)
        async with app.state.lock:
            candidates = [url for url in upstreams if url in app.state.healthy]
            if not candidates:
                return JSONResponse({"error": "no writable API replica is ready"}, status_code=503,
                                    headers={"Retry-After": "1"})
            target = candidates[app.state.next % len(candidates)]
            app.state.next += 1
        headers = {key: value for key, value in request.headers.items() if key.lower() not in HOP_HEADERS}
        # Do not accept or forward client-supplied proxy trust metadata.
        headers = {key: value for key, value in headers.items()
                   if not key.lower().startswith(("x-forwarded-", "forwarded"))}
        suffix = request.url.path + ("?" + request.url.query if request.url.query else "")
        try:
            outbound = app.state.client.build_request(request.method, target + suffix,
                headers=headers, content=bytes(body))
            response = await app.state.client.send(outbound, stream=True)
        except httpx.HTTPError:
            app.state.failed_until[target] = time.monotonic() + 1
            app.state.healthy.discard(target)
            # A claim/create/complete may already have committed. Only its
            # owner can decide how to retry with the correct idempotency/fence.
            return JSONResponse({"error": "upstream outcome uncertain; retry using the same operation identity"},
                                status_code=503, headers={"Retry-After": "1"})
        if response.status_code == 503:
            app.state.failed_until[target] = time.monotonic() + 1
            app.state.healthy.discard(target)
        response_headers = {key: value for key, value in response.headers.items()
                            if key.lower() not in HOP_HEADERS}
        return StreamingResponse(response.aiter_raw(), status_code=response.status_code,
            headers=response_headers, background=BackgroundTask(response.aclose))

    return app


app = create_app()
