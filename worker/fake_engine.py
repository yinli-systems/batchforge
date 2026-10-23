"""TEST ONLY: deterministic HTTP fixture, never used as an inference fallback."""
from __future__ import annotations

import asyncio
import os
import time
import uuid
from collections import defaultdict
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request


def create_app(*, delay: float = .01, fail_first: int = 0) -> FastAPI:
    attempts: dict[str, int] = defaultdict(int)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if os.environ.get("BATCHFORGE_TEST_ENGINE") != "1":
            raise RuntimeError("Fake engine is test-only. Explicitly set BATCHFORGE_TEST_ENGINE=1")
        yield

    app = FastAPI(lifespan=lifespan)
    app.state.started = 0
    app.state.finished = 0

    @app.get("/test/state")
    def state():
        return {"test_only": True, "started": app.state.started, "finished": app.state.finished}

    @app.get("/health")
    def health():
        return {"status": "ok", "test_only": True}

    @app.get("/v1/models")
    def models():
        return {"object": "list", "data": [{"id": os.environ.get("MODEL_NAME", "Qwen/Qwen2.5-0.5B-Instruct"), "object": "model", "owned_by": "TEST_FIXTURE"}]}

    @app.post("/v1/chat/completions")
    async def completion(request: Request):
        body = await request.json()
        app.state.started += 1
        await asyncio.sleep(delay)
        content = " ".join(str(message.get("content", "")) for message in body.get("messages", []))
        attempts[content] += 1
        if attempts[content] <= fail_first:
            raise HTTPException(status_code=503, detail="Deliberate test-only transient failure")
        app.state.finished += 1
        return {
            "id": "test-" + uuid.uuid4().hex,
            "object": "chat.completion",
            "created": int(time.time()),
            "model": body["model"],
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "TEST FIXTURE: " + content[:120]}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": max(1, len(content.split())), "completion_tokens": 8, "total_tokens": max(1, len(content.split())) + 8},
            "batchforge_test_only": True,
        }

    return app


app = create_app(delay=float(os.environ.get("TEST_ENGINE_DELAY_SECONDS", ".01")),
                 fail_first=int(os.environ.get("TEST_ENGINE_FAIL_FIRST", "0")))
