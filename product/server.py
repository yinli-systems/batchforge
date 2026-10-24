"""User-facing job app on top of the BatchForge public API.

The tenant API key stays on this server. Browsers never receive it. Jobs are
stateless here: the template id travels in batch metadata and the inputs are
read back from the stored input file, so a restarted server loses nothing.

Run: BATCHFORGE_API_KEY=... python -m product
"""
from __future__ import annotations

import os
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .jobs import InvalidJob, build_jsonl, parse_inputs, parse_items, parse_results, to_csv
from .templates import TEMPLATES

STATIC = Path(__file__).parent / "static"
TERMINAL = {"completed", "failed", "cancelled"}
DEFAULT_MODEL = "Qwen/Qwen3-235B-A22B-Instruct-2507-FP8"


class CreateJob(BaseModel):
    template: str = Field(min_length=1, max_length=64)
    text: str = Field(min_length=1, max_length=400_000)
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=128)


class UpstreamError(Exception):
    def __init__(self, status: int, message: str) -> None:
        self.status, self.message = status, message


def create_app(*, upstream: httpx.AsyncClient | None = None, model: str | None = None) -> FastAPI:
    base_url = os.environ.get("BATCHFORGE_URL", "http://127.0.0.1:8000")
    api_key = os.environ.get("BATCHFORGE_API_KEY", "")
    model = model or os.environ.get("PRODUCT_MODEL", DEFAULT_MODEL)
    client = upstream or httpx.AsyncClient(
        base_url=base_url,
        headers={"Authorization": f"Bearer {api_key}"},
        timeout=httpx.Timeout(30, connect=5),
    )
    app = FastAPI(title="BatchForge product", version="0.1.0")
    app.state.client, app.state.model = client, model

    @app.exception_handler(UpstreamError)
    async def upstream_error(_: Request, exc: UpstreamError):
        return JSONResponse(status_code=exc.status, content={"error": {"message": exc.message}})

    @app.exception_handler(InvalidJob)
    async def invalid_job(_: Request, exc: InvalidJob):
        return JSONResponse(status_code=400, content={"error": {"message": str(exc)}})

    async def call(method: str, path: str, **kwargs) -> httpx.Response:
        try:
            response = await client.request(method, path, **kwargs)
        except httpx.HTTPError:
            raise UpstreamError(503, "BatchForge is unreachable; try again shortly") from None
        if response.status_code >= 400:
            try:
                message = response.json()["error"]["message"]
            except (ValueError, KeyError, TypeError):
                message = "BatchForge request failed"
            raise UpstreamError(response.status_code if response.status_code < 500 else 503, message)
        return response

    @app.get("/api/health")
    async def health():
        return {"status": "ok", "model": app.state.model}

    @app.get("/api/templates")
    async def templates():
        return {"data": [
            {"id": t.id, "label": t.label, "description": t.description}
            for t in TEMPLATES.values()
        ]}

    @app.post("/api/jobs", status_code=201)
    async def create_job(body: CreateJob):
        template = TEMPLATES.get(body.template)
        if template is None:
            raise InvalidJob(f"Unknown template: {body.template}")
        items = parse_items(body.text.splitlines())
        content = build_jsonl(template, items, app.state.model)
        uploaded = (await call("POST", "/v1/files", data={"purpose": "batch"},
                               files={"file": ("items.jsonl", content, "application/jsonl")})).json()
        headers = {"Idempotency-Key": body.idempotency_key} if body.idempotency_key else {}
        batch = (await call("POST", "/v1/batches", headers=headers, json={
            "input_file_id": uploaded["id"],
            "endpoint": "/v1/chat/completions",
            "completion_window": "24h",
            "metadata": {"product_template": template.id},
        })).json()
        return summary(batch)

    @app.get("/api/jobs/{batch_id}")
    async def get_job(batch_id: str):
        batch = (await call("GET", f"/v1/batches/{batch_id}")).json()
        result = summary(batch)
        if batch["status"] in TERMINAL:
            result["rows"] = await rows_for(batch)
        return result

    @app.get("/api/jobs/{batch_id}/export.csv")
    async def export_job(batch_id: str):
        batch = (await call("GET", f"/v1/batches/{batch_id}")).json()
        if batch["status"] not in TERMINAL:
            raise HTTPException(409, "Results are available when the job finishes")
        rows = await rows_for(batch)
        return PlainTextResponse(
            to_csv(product_template(batch), rows),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{batch_id}.csv"'},
        )

    def product_template(batch: dict):
        template = TEMPLATES.get(batch["metadata"].get("product_template", ""))
        if template is None:
            raise UpstreamError(404, "Not a product job")
        return template

    async def rows_for(batch: dict) -> list[dict]:
        template = product_template(batch)
        inputs = parse_inputs((await call("GET", f"/v1/files/{batch['input_file_id']}/content")).content)
        output = await optional_file(batch.get("output_file_id"))
        errors = await optional_file(batch.get("error_file_id"))
        return parse_results(template, output, errors, inputs)

    async def optional_file(file_id: str | None) -> bytes:
        if not file_id:
            return b""
        return (await call("GET", f"/v1/files/{file_id}/content")).content

    app.mount("/", StaticFiles(directory=STATIC, html=True), name="static")
    return app


def summary(batch: dict) -> dict:
    counts = batch["request_counts"]
    total = counts["total"] or 1
    return {
        "id": batch["id"],
        "template": batch["metadata"].get("product_template"),
        "status": batch["status"],
        "total": counts["total"],
        "completed": counts["completed"],
        "failed": counts["failed"],
        "pending": counts["pending"] + counts["running"],
        "progress": round((counts["completed"] + counts["failed"] + counts["cancelled"]) / total, 3),
        "usage": batch["usage"],
        "created_at": batch["created_at"],
        "completed_at": batch["completed_at"],
    }

