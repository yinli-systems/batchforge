import asyncio
import json

import httpx
import pytest

from worker.config import Settings
from worker.runtime import StaleAttempt, Worker


def configuration(origins=("http://api-a", "http://api-b")):
    return Settings(control_url=origins[0], control_urls=origins, worker_token="x" * 32,
        inference_urls=("http://engine",), model="test-model", worker_id="failover-worker", name="test",
        report_retry_seconds=1)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["transport", "server", "invalid_json"])
async def test_failed_claim_is_never_automatically_replayed_but_next_request_uses_other_origin(failure):
    calls = []

    async def control(request):
        calls.append(request)
        if request.url.host == "api-a":
            if failure == "transport":
                raise httpx.ReadError("Acknowledgement lost after possible claim commit")
            if failure == "server":
                return httpx.Response(503)
            return httpx.Response(200, text="invalid-json")
        return httpx.Response(200, json={"tasks": [], "lease_seconds": 90})

    worker = Worker(configuration(), control_transport=httpx.MockTransport(control))
    body = {"worker_id": "failover-worker", "model": "test-model", "limit": 8}
    try:
        with pytest.raises((httpx.HTTPError, ValueError)):
            await worker._post("/internal/tasks/claim", body)
        assert len(calls) == 1  # The ambiguous POST has not been replayed.
        assert worker._control_origin_index == 1
        await worker._post("/internal/tasks/claim", body)
        assert [request.url.host for request in calls] == ["api-a", "api-b"]
        assert all(request.headers["authorization"] == "Bearer " + "x" * 32 for request in calls)
    finally:
        await worker.close()


@pytest.mark.asyncio
async def test_completion_replay_across_origins_preserves_fence_and_does_not_rerun_model():
    completions, inferences = [], []

    async def control(request):
        if request.url.path.endswith("complete"):
            completions.append(request)
            if request.url.host == "api-a":
                raise httpx.ReadError("Commit succeeded but acknowledgement was lost")
            return httpx.Response(200, json={"ok": True, "duplicate": True})
        return httpx.Response(200, json={"ok": True})

    async def engine(request):
        inferences.append(request)
        return httpx.Response(200, json={"choices": [{"message": {"role": "assistant", "content": "test"}}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1}})

    worker = Worker(configuration(), control_transport=httpx.MockTransport(control),
                    inference_transport=httpx.MockTransport(engine))
    worker.engines[0].ready = True
    task = {"id": "task-one", "attempt_token": "fence-one", "body": {"model": "test-model",
        "messages": [{"role": "user", "content": "test"}], "max_tokens": 8}}
    try:
        await worker.execute(task)
        assert len(inferences) == 1
        assert [request.url.host for request in completions] == ["api-a", "api-b"]
        assert completions[0].content == completions[1].content
        assert json.loads(completions[1].content)["attempt_token"] == "fence-one"
        assert worker.metrics.results.labels("completed")._value.get() == 1
    finally:
        await worker.close()


@pytest.mark.asyncio
async def test_delayed_failure_from_an_old_origin_cannot_override_newer_failover_decisions():
    first_release, old_release, both_started = asyncio.Event(), asyncio.Event(), asyncio.Event()
    first_calls = 0

    async def control(request):
        nonlocal first_calls
        if request.url.host == "api-a":
            first_calls += 1
            if first_calls == 2:
                both_started.set()
            await (first_release if first_calls == 1 else old_release).wait()
            raise httpx.ConnectError("Old origin failed")
        if request.url.host == "api-b":
            raise httpx.ConnectError("Second origin failed")
        return httpx.Response(200, json={"ok": True})

    worker = Worker(configuration(("http://api-a", "http://api-b", "http://api-c")),
                    control_transport=httpx.MockTransport(control))
    one = asyncio.create_task(worker._post("/internal/test", {}))
    two = asyncio.create_task(worker._post("/internal/test", {}))
    try:
        await asyncio.wait_for(both_started.wait(), 1)
        first_release.set()
        with pytest.raises(httpx.ConnectError):
            await one
        with pytest.raises(httpx.ConnectError):
            await worker._post("/internal/test", {})
        assert worker._control_origin_index == 2
        old_release.set()
        with pytest.raises(httpx.ConnectError):
            await two
        assert worker._control_origin_index == 2
        assert worker.metrics.control_failovers._value.get() == 2
        assert await worker._post("/internal/test", {}) == {"ok": True}
    finally:
        first_release.set()
        old_release.set()
        await asyncio.gather(one, two, return_exceptions=True)
        await worker.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [401, 409, 422, 429])
async def test_semantic_auth_or_admission_errors_do_not_rotate_control_origin(status):
    requests = []

    def control(request):
        requests.append(request)
        return httpx.Response(status)

    worker = Worker(configuration(), control_transport=httpx.MockTransport(control))
    try:
        with pytest.raises(StaleAttempt if status == 409 else httpx.HTTPStatusError):
            await worker._post("/internal/tasks/task-one/complete", {})
        assert len(requests) == 1
        assert worker._control_origin_index == 0
        assert worker.metrics.control_failovers._value.get() == 0
    finally:
        await worker.close()


def test_control_urls_configuration_is_explicit_deduplicated_and_backwards_compatible(monkeypatch):
    monkeypatch.setenv("BATCHFORGE_WORKER_TOKEN", "x" * 32)
    monkeypatch.setenv("CONTROL_PLANE_URL", "http://legacy")
    monkeypatch.setenv("CONTROL_PLANE_URLS", "http://api-a/,https://api-b,https://api-b/")
    settings = Settings.from_env()
    assert settings.control_origins == ("http://api-a", "https://api-b")
    assert settings.control_url == "http://api-a"
    monkeypatch.delenv("CONTROL_PLANE_URLS")
    assert Settings.from_env().control_origins == ("http://legacy",)
