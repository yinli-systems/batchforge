"""Scheduler regressions: refill promptly without spinning against an empty DB."""
import asyncio
import json
import time

import httpx
import pytest

from worker.config import Settings
from worker.runtime import Worker


class LoopHarness:
    def __init__(self, *, tasks: int, capacity: int, poll_seconds: float,
                 claim_batch_size: int | None = None, block_inference: bool = False):
        self.remaining = tasks
        self.expected = tasks
        self.claim_batch_size = claim_batch_size
        self.block_inference = block_inference
        self.claims = []
        self.registrations = []
        self.completed = 0
        self.started = 0
        self.first_claim = asyncio.Event()
        self.all_started = asyncio.Event()
        self.release = asyncio.Event()
        config = Settings(control_url="http://control", worker_token="x" * 32,
            inference_urls=("http://engine",), model="test-model", worker_id="loop-test", name="loop-test",
            max_inflight=capacity, token_budget=capacity * 2048, max_model_len=2048,
            poll_seconds=poll_seconds)
        self.worker = Worker(config, control_transport=httpx.MockTransport(self.control),
                             inference_transport=httpx.MockTransport(self.engine))

    async def control(self, request):
        body = json.loads(request.content)
        if request.url.path.endswith("register"):
            self.registrations.append(body)
            return httpx.Response(200, json={"worker_id": "loop-test", "lease_seconds": 90, "heartbeat_interval_seconds": 10})
        if request.url.path.endswith("claim"):
            self.claims.append((time.monotonic(), body["limit"]))
            self.first_claim.set()
            count = min(body["limit"], self.remaining)
            if self.claim_batch_size is not None:
                count = min(count, self.claim_batch_size)
            tasks = []
            for _ in range(count):
                self.remaining -= 1
                tasks.append({"id": f"task-{self.remaining}", "attempt_token": f"token-{self.remaining}",
                    "body": {"model": "test-model", "messages": [{"role": "user", "content": "test"}], "max_tokens": 8}})
            return httpx.Response(200, json={"tasks": tasks, "lease_seconds": 90})
        if request.url.path.endswith("complete"):
            self.completed += 1
            if self.completed == self.expected:
                self.worker.stop.set()
        return httpx.Response(200, json={"ok": True})

    async def engine(self, request):
        if request.method == "GET":
            return httpx.Response(200, json={"data": [{"id": "test-model"}]})
        self.started += 1
        if self.started == self.expected:
            self.all_started.set()
        if self.block_inference:
            await self.release.wait()
        else:
            # Let the scheduler genuinely wait at full capacity before the
            # completion arrives; this must wake it ahead of the idle timer.
            await asyncio.sleep(.005)
        return httpx.Response(200, json={"choices": [{"message": {"role": "assistant", "content": "test"}}],
                                       "usage": {"prompt_tokens": 1, "completion_tokens": 1}})


@pytest.mark.asyncio
async def test_fast_completion_refills_capacity_before_long_idle_poll_timeout():
    harness = LoopHarness(tasks=4, capacity=1, poll_seconds=10)
    running = asyncio.create_task(harness.worker.run())
    try:
        await asyncio.wait_for(asyncio.shield(running), 1)
        assert harness.completed == 4
        assert len(harness.claims) == 4
    finally:
        harness.worker.stop.set()
        if not running.done():
            await running


@pytest.mark.asyncio
async def test_partial_nonempty_claim_immediately_fills_remaining_capacity():
    harness = LoopHarness(tasks=2, capacity=2, poll_seconds=10,
                          claim_batch_size=1, block_inference=True)
    running = asyncio.create_task(harness.worker.run())
    try:
        await asyncio.wait_for(harness.all_started.wait(), 1)
        assert [limit for _, limit in harness.claims] == [2, 1]
        assert harness.worker.metrics.active._value.get() == 2
        harness.worker.stop.set()
        harness.release.set()
        await asyncio.wait_for(asyncio.shield(running), 1)
        assert harness.completed == 2
    finally:
        harness.worker.stop.set()
        harness.release.set()
        if not running.done():
            await running


@pytest.mark.asyncio
async def test_empty_queue_backs_off_and_shutdown_interrupts_idle_wait(monkeypatch):
    monkeypatch.setattr("worker.runtime.random.uniform", lambda lower, upper: 1.0)
    harness = LoopHarness(tasks=0, capacity=1, poll_seconds=.02)
    running = asyncio.create_task(harness.worker.run())
    try:
        await asyncio.wait_for(harness.first_claim.wait(), 1)
        await asyncio.sleep(.13)
        # Adaptive waits are .02,.04,.08,... rather than a fixed .02 poll.
        assert 1 <= len(harness.claims) <= 4
        assert harness.registrations[-1]["metadata"]["ready_engine_count"] == 1
        assert harness.registrations[-1]["metadata"]["engine_count"] == 1
        harness.worker.stop.set()
        await asyncio.wait_for(asyncio.shield(running), .3)
        assert harness.worker.metrics.stopping is True
    finally:
        harness.worker.stop.set()
        if not running.done():
            await running
