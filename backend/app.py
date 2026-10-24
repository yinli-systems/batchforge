"""FastAPI HTTP boundary for the PostgreSQL batch state machine."""
import asyncio
import contextlib
import logging
import random
import secrets
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Literal

from fastapi import Depends, FastAPI, File, Form, Header, Query, Request, UploadFile
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse, StreamingResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool, PoolTimeout
from pydantic import BaseModel, ConfigDict, Field

from .config import Settings, get_settings
from .migrate import verify_schema
from .store import APIError, Store, one
from .validation import InvalidInput, strict_json
from .blob_storage import BlobStorageError, BlobQuotaExceeded, BlobNotFound, BlobExpired

logger = logging.getLogger("batchforge")
HTTP_REQUESTS = Counter("batchforge_http_requests_total", "HTTP requests", ["method", "route", "status"])
HTTP_DURATION = Histogram("batchforge_http_request_duration_seconds", "HTTP request duration", ["method", "route"])
RECOVERED_LEASES = Counter("batchforge_recovered_leases_total", "Expired leases recovered by this API process")
RECOVERY_ERRORS = Counter("batchforge_recovery_errors_total", "Recovery sweep errors")
STORAGE_ERRORS = Counter('batchforge_storage_maintenance_errors_total', 'Storage retention/GC sweep errors')


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class CreateBatch(StrictModel):
    input_file_id: str = Field(min_length=1, max_length=100)
    endpoint: Literal["/v1/chat/completions"] = "/v1/chat/completions"
    completion_window: Literal["24h"] = "24h"
    metadata: dict[str, str] = Field(default_factory=dict, max_length=16)


class Registration(StrictModel):
    worker_id: str | None = Field(default=None, min_length=1, max_length=128)
    name: str = Field(min_length=1, max_length=128)
    model: str = Field(min_length=1, max_length=256)
    concurrency: int = Field(ge=1, le=4096)
    metadata: dict[str, Any] = Field(default_factory=dict)


class Claim(StrictModel):
    worker_id: str = Field(min_length=1, max_length=128)
    model: str = Field(min_length=1, max_length=256)
    limit: int = Field(default=1, ge=1, le=256)


class Attempt(StrictModel):
    worker_id: str = Field(min_length=1, max_length=128)
    attempt_token: str = Field(min_length=1, max_length=128)


class Completion(Attempt):
    response: dict[str, Any]


class TaskError(StrictModel):
    code: str = Field(min_length=1, max_length=128)
    message: str = Field(min_length=1, max_length=4096)


class Failure(Attempt):
    error: TaskError
    retryable: bool = True


def error_response(status, message, code="invalid_request"):
    return JSONResponse(status_code=status, content={"error": {"message": message, "type": "invalid_request_error", "code": code}})


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()

    async def recovery_loop(store):
        while True:
            try:
                RECOVERED_LEASES.inc(await store.recover())
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                RECOVERY_ERRORS.inc()
                # Database diagnostics can contain query parameters or DSNs.
                logger.warning("Lease recovery unavailable: %s", type(exc).__name__)
            # Independent replicas should not synchronize database recovery work.
            await asyncio.sleep(min(5, max(1, settings.lease_seconds // 3)) * random.uniform(.8, 1.2))

    async def storage_loop(store):
        while True:
            try:
                await store.blobs.expire_inputs(limit=100)
                await store.blobs.collect_orphans(limit=100, grace_seconds=300)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                STORAGE_ERRORS.inc()
                logger.warning('Storage maintenance unavailable: %s', type(exc).__name__)
            await asyncio.sleep(random.uniform(25, 35))

    @asynccontextmanager
    async def lifespan(app):
        worker_tokens = settings.worker_tokens()
        options = (f"-c client_encoding=UTF8 -c statement_timeout={settings.db_statement_timeout_ms} "
                   f"-c lock_timeout={settings.db_lock_timeout_ms} -c idle_in_transaction_session_timeout=15000")
        pool = AsyncConnectionPool(settings.database_url, min_size=settings.db_pool_min, max_size=settings.db_pool_max, open=False, kwargs={"autocommit": True, "row_factory": dict_row, "options": options, "connect_timeout": 5}, timeout=settings.db_pool_timeout_seconds)
        await pool.open(wait=True, timeout=30)
        try:
            async with pool.connection() as conn:
                schema_version = await verify_schema(conn, minimum_version=settings.minimum_schema_version())
            app.state.pool, app.state.store, app.state.settings = pool, Store(pool, settings, schema_version=schema_version), settings
            app.state.worker_tokens = worker_tokens
            reaper = asyncio.create_task(recovery_loop(app.state.store), name="lease-recovery")
            collector = (asyncio.create_task(storage_loop(app.state.store), name='storage-maintenance')
                         if settings.blob_gc_enabled and app.state.store.blobs else None)
            try:
                yield
            finally:
                reaper.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await reaper
                if collector:
                    collector.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await collector
        finally:
            await pool.close()

    app = FastAPI(title="BatchForge", version="0.1.0", lifespan=lifespan)

    @app.exception_handler(APIError)
    async def api_error(request, exc):
        return error_response(exc.status, exc.message, exc.code)

    @app.exception_handler(BlobStorageError)
    async def storage_error(request, exc):
        if isinstance(exc, BlobQuotaExceeded):
            return error_response(429, str(exc), exc.code)
        if isinstance(exc, BlobExpired):
            return error_response(410, 'Input retention expired', 'input_expired')
        if isinstance(exc, BlobNotFound):
            return error_response(404, 'Input file not found', 'not_found')
        response = error_response(503, 'Artifact storage temporarily unavailable', 'storage_unavailable')
        response.headers['Retry-After'] = '1'
        return response

    @app.exception_handler(psycopg.Error)
    async def fenced_database_error(request, exc):
        # v6 dispatch guards also fence legacy Python claim transactions during
        # rolling upgrades. Match the status of the SQL-call translation.
        if exc.sqlstate == 'BF409':
            return error_response(409, 'Lease or runtime identity changed; discard this attempt', 'stale_attempt')
        logger.error('Database operation failed: %s', type(exc).__name__)
        return error_response(500, 'Database operation failed', 'internal_error')

    async def database_unavailable(request, exc):
        logger.warning("Database request unavailable: %s", type(exc).__name__)
        response = error_response(503, "Control plane is temporarily unavailable; retry with the same idempotency key", "service_unavailable")
        response.headers["Retry-After"] = "1"
        return response

    for exception in (psycopg.OperationalError, psycopg.InterfaceError, PoolTimeout,
                      psycopg.errors.ReadOnlySqlTransaction, psycopg.errors.LockNotAvailable,
                      psycopg.errors.QueryCanceled, psycopg.errors.SerializationFailure,
                      psycopg.errors.DeadlockDetected):
        app.add_exception_handler(exception, database_unavailable)

    @app.exception_handler(InvalidInput)
    async def invalid_input(request, exc):
        return error_response(400, str(exc))

    @app.exception_handler(RequestValidationError)
    async def validation_error(request, exc):
        return error_response(422, "Invalid request: " + "; ".join(f"{'.'.join(map(str, item['loc']))}: {item['msg']}" for item in exc.errors()[:5]))

    @app.middleware("http")
    async def bounded_requests(request, call_next):
        started = time.monotonic()
        if request.method in {"POST", "PUT", "PATCH"}:
            limit = settings.max_upload_bytes + 65536 if request.url.path == "/v1/files" else settings.max_request_bytes * 2 + 65536
            try:
                if int(request.headers.get("content-length", "0")) > limit:
                    return error_response(413, "Request body exceeds configured limit", "payload_too_large")
            except ValueError:
                return error_response(400, "Invalid Content-Length")
            chunks, size = [], 0
            async for chunk in request.stream():
                size += len(chunk)
                if size > limit:
                    return error_response(413, "Request body exceeds configured limit", "payload_too_large")
                chunks.append(chunk)
            request._body = b"".join(chunks)
            if request._body and "application/json" in request.headers.get("content-type", ""):
                try:
                    strict_json(request._body)
                except InvalidInput as exc:
                    return error_response(400, str(exc))
        response = await call_next(request)
        route = getattr(request.scope.get("route"), "path", "unmatched")
        HTTP_REQUESTS.labels(request.method, route, response.status_code).inc()
        HTTP_DURATION.labels(request.method, route).observe(time.monotonic() - started)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Cache-Control"] = "no-store"
        return response

    async def tenant(request: Request, authorization: str | None = Header(default=None)):
        if not authorization or not authorization.startswith("Bearer "):
            raise APIError(401, "Bearer API key required", "invalid_api_key")
        return await request.app.state.store.tenant(authorization[7:])

    async def worker(request: Request, authorization: str | None = Header(default=None)):
        supplied = authorization[7:] if authorization and authorization.startswith("Bearer ") else ""
        accepted = False
        # Check every active credential, independent of which generation matched.
        for token in request.app.state.worker_tokens:
            accepted |= secrets.compare_digest(supplied.encode(), token.encode())
        if not supplied or not accepted:
            raise APIError(401, "Valid worker Bearer token required", "invalid_worker_token")

    @app.get("/healthz")
    async def health():
        return {"status": "ok"}

    @app.get("/readyz")
    async def ready(request: Request):
        try:
            async with asyncio.timeout(settings.readiness_timeout_seconds):
                async with request.app.state.pool.connection() as conn:
                    await verify_schema(conn, minimum_version=settings.minimum_schema_version())
                    row = await one(conn, "SELECT NOT pg_is_in_recovery() AND current_setting('transaction_read_only')='off' AS writable")
                    if not row["writable"]:
                        raise RuntimeError("Primary database is not writable")
        except Exception:
            return JSONResponse(status_code=503, content={"status": "unavailable"})
        return {"status": "ready"}

    @app.get("/metrics", dependencies=[Depends(worker)])
    async def metrics(request: Request):
        async with request.app.state.pool.connection() as conn:
            row = await one(conn, "SELECT COALESCE(sum(pending_count),0) AS queued,COALESCE(sum(running_count),0) AS running,COALESCE(sum(completed_count),0) AS completed,COALESCE(sum(failed_count),0) AS failed,COALESCE(sum(cancelled_count),0) AS cancelled,COALESCE(sum(prompt_tokens),0) AS prompt_tokens,COALESCE(sum(completion_tokens),0) AS completion_tokens FROM batches")
        output = generate_latest().decode()
        for name, value in row.items():
            metric = f"batchforge_durable_{name}"
            output += f"# HELP {metric} Durable database {name} count\n# TYPE {metric} gauge\n{metric} {int(value)}\n"
        return PlainTextResponse(output, media_type=CONTENT_TYPE_LATEST)

    @app.post("/v1/files")
    async def upload(request: Request, file: UploadFile = File(...), purpose: str = Form(default="batch"), tenant_id: str = Depends(tenant)):
        if purpose != "batch":
            raise APIError(400, "Only purpose=batch is supported")
        content = await file.read(settings.max_upload_bytes + 1)
        return await request.app.state.store.upload(tenant_id, file.filename or "input.jsonl", content)

    @app.get("/v1/files/{fid}/content")
    async def file_content(fid: str, request: Request, tenant_id: str = Depends(tenant)):
        store = request.app.state.store
        file = await store.file(tenant_id, fid)
        chunks = store.file_chunks(tenant_id, file)
        sentinel = object()
        try:
            first = await anext(chunks, sentinel)
        except BaseException:
            await chunks.aclose()
            raise

        async def stream():
            try:
                if first is not sentinel:
                    yield first
                async for chunk in chunks:
                    yield chunk
            finally:
                await chunks.aclose()
        # Never interpolate the untrusted upload filename into response headers.
        return StreamingResponse(stream(), media_type="application/x-ndjson", headers={"Content-Disposition": f'attachment; filename="{fid}.jsonl"'})

    @app.post("/v1/batches")
    async def create_batch(body: CreateBatch, request: Request, tenant_id: str = Depends(tenant), idempotency_key: str | None = Header(default=None)):
        if any(len(key) > 64 or len(value) > 512 for key, value in body.metadata.items()):
            raise APIError(400, "Metadata keys/values exceed limits")
        return await request.app.state.store.create(tenant_id, body.model_dump(), idempotency_key)

    @app.get("/v1/batches")
    async def list_batches(request: Request, limit: int = Query(default=20, ge=1, le=100), after: str | None = None, tenant_id: str = Depends(tenant)):
        return await request.app.state.store.list_batches(tenant_id, limit, after)

    @app.get("/v1/batches/{bid}")
    async def get_batch(bid: str, request: Request, tenant_id: str = Depends(tenant)):
        return await request.app.state.store.get_batch(tenant_id, bid)

    @app.post("/v1/batches/{bid}/cancel")
    async def cancel(bid: str, request: Request, tenant_id: str = Depends(tenant)):
        return await request.app.state.store.cancel(tenant_id, bid)

    @app.post("/v1/batches/{bid}/retry")
    async def retry(bid: str, request: Request, tenant_id: str = Depends(tenant), idempotency_key: str | None = Header(default=None)):
        return await request.app.state.store.retry(tenant_id, bid, idempotency_key)

    @app.get("/v1/models")
    async def models(tenant_id: str = Depends(tenant)):
        return {"object": "list", "data": [{"id": name, "object": "model", "owned_by": "batchforge"} for name in settings.served_models()]}

    @app.get("/v1/overview")
    async def overview(request: Request, tenant_id: str = Depends(tenant)):
        return await request.app.state.store.overview(tenant_id)

    @app.post("/internal/workers/register", dependencies=[Depends(worker)])
    async def register(body: Registration, request: Request):
        if body.model not in settings.served_models():
            raise APIError(400, "Worker model is not in the configured model catalog")
        return await request.app.state.store.register(body.model_dump())

    @app.post("/internal/tasks/claim", dependencies=[Depends(worker)])
    async def claim(body: Claim, request: Request):
        return await request.app.state.store.claim(body.worker_id, body.model, body.limit)

    @app.post("/internal/tasks/{tid}/heartbeat", dependencies=[Depends(worker)])
    async def heartbeat(tid: str, body: Attempt, request: Request):
        return await request.app.state.store.heartbeat(tid, body.worker_id, body.attempt_token)

    @app.post("/internal/tasks/{tid}/complete", dependencies=[Depends(worker)])
    async def complete(tid: str, body: Completion, request: Request):
        return await request.app.state.store.complete(tid, body.worker_id, body.attempt_token, body.response)

    @app.post("/internal/tasks/{tid}/fail", dependencies=[Depends(worker)])
    async def fail(tid: str, body: Failure, request: Request):
        return await request.app.state.store.fail(tid, body.worker_id, body.attempt_token, body.error.model_dump(), body.retryable)

    @app.get("/{path:path}", include_in_schema=False)
    async def frontend(path: str):
        if path.split("/", 1)[0] in {"v1", "internal", "metrics", "healthz", "readyz"}:
            raise APIError(404, "Endpoint not found", "not_found")
        root = Path(settings.frontend_dir).resolve()
        target = (root / path).resolve()
        if target != root and root not in target.parents:
            raise APIError(404, "Not found", "not_found")
        if target.is_file():
            return FileResponse(target)
        index = root / "index.html"
        if index.is_file():
            return FileResponse(index)
        return JSONResponse({"service": "BatchForge", "docs": "/docs"})

    return app


app = create_app()
