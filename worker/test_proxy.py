"""TEST ONLY: commit one claim upstream, then discard its acknowledgement."""
import os
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request, Response


@asynccontextmanager
async def lifespan(app):
    if os.environ.get("BATCHFORGE_TEST_ENGINE") != "1":
        raise RuntimeError("Lost-ack proxy is test-only")
    app.state.client = httpx.AsyncClient(base_url=os.environ["TEST_PROXY_UPSTREAM"], timeout=10)
    app.state.dropped_claims = 0
    yield
    await app.state.client.aclose()


app = FastAPI(lifespan=lifespan)


@app.get("/test/state")
def state():
    return {"test_only": True, "dropped_claims": app.state.dropped_claims}


@app.post("/internal/{path:path}")
async def forward(path: str, request: Request):
    response = await app.state.client.post("/internal/" + path, content=await request.body(),
        headers={"Authorization": request.headers.get("Authorization", ""), "Content-Type": "application/json"})
    if path == "tasks/claim" and response.status_code == 200 and app.state.dropped_claims == 0 and response.json().get("tasks"):
        app.state.dropped_claims += 1
        return Response('{"test_only":true,"error":"claim committed but acknowledgement deliberately discarded"}',
                        status_code=503, media_type="application/json")
    return Response(response.content, status_code=response.status_code, media_type="application/json")
