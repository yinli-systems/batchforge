"""Lease-fenced async inference worker; all durable scheduling lives in Postgres."""
from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import signal
import time
from dataclasses import dataclass
from typing import Any

import httpx
import uvicorn

from worker.config import Settings
from worker.metrics import Metrics

log = logging.getLogger("batchforge.worker")


class StaleAttempt(Exception):
    pass


class InferenceError(Exception):
    def __init__(self, code: str, message: str, retryable: bool = True) -> None:
        super().__init__(message)
        self.code, self.retryable = code, retryable


@dataclass
class Engine:
    url: str
    inflight: int = 0
    ready: bool = False
    failures: int = 0
    available_after: float = 0.0


class Worker:
    def __init__(self, settings: Settings, *, control_transport=None, inference_transport=None) -> None:
        self.config = settings
        self.metrics = Metrics()
        self.metrics.slots.set(settings.slots)
        self.stop = asyncio.Event()
        self.active: set[asyncio.Task] = set()
        self.engines = [Engine(url) for url in settings.inference_urls]
        self.lease_seconds = 90.0
        self.heartbeat_seconds = 10.0
        self.control = httpx.AsyncClient(
            base_url=settings.control_url,
            headers={"Authorization": f"Bearer {settings.worker_token}"},
            timeout=httpx.Timeout(10, connect=5),
            limits=httpx.Limits(max_connections=settings.slots * 2 + 4),
            transport=control_transport,
        )
        headers = {"Authorization": f"Bearer {settings.inference_api_key}"} if settings.inference_api_key else {}
        self.inference = httpx.AsyncClient(
            timeout=httpx.Timeout(settings.request_timeout, connect=10),
            headers=headers,
            limits=httpx.Limits(max_connections=settings.slots + len(self.engines)),
            transport=inference_transport,
        )

    async def close(self) -> None:
        await self.control.aclose()
        await self.inference.aclose()

    async def _post(self, path: str, body: dict) -> dict:
        operation = path.rsplit("/", 1)[-1]
        try:
            response = await self.control.post(path, json=body)
            if response.status_code == 409:
                raise StaleAttempt("Control plane fenced the attempt")
            response.raise_for_status()
            value = response.json()
        except Exception:
            self.metrics.requests.labels(operation, "error").inc()
            raise
        self.metrics.requests.labels(operation, "ok").inc()
        self.metrics.last_control_success = time.monotonic()
        return value

    async def register(self) -> None:
        result = await self._post("/internal/workers/register", {
            "worker_id": self.config.worker_id,
            "name": self.config.name,
            "model": self.config.model,
            "concurrency": self.config.slots,
            "metadata": {"engine_count": len(self.engines), "max_model_len": self.config.max_model_len,
                         "token_budget": self.config.token_budget, "version": "0.1.0"},
        })
        if result["worker_id"] != self.config.worker_id:
            raise ValueError("Control plane changed the worker identity")
        self.lease_seconds = float(result["lease_seconds"])
        self.heartbeat_seconds = min(float(result["heartbeat_interval_seconds"]), self.lease_seconds / 4)
        if self.lease_seconds < 5 or self.heartbeat_seconds <= 0:
            raise ValueError("Unsafe lease configuration from control plane")
        self.metrics.registered = True

    async def probe_engines(self) -> None:
        async def probe(engine: Engine) -> None:
            if time.monotonic() < engine.available_after:
                return
            try:
                result = await asyncio.wait_for(self.inference.get(engine.url + "/v1/models", timeout=5), timeout=5)
                result.raise_for_status()
                engine.ready = any(item.get("id") == self.config.model for item in result.json()["data"])
                if not engine.ready:
                    self._engine_unavailable(engine)
            except (httpx.HTTPError, ValueError, KeyError, TypeError, asyncio.TimeoutError):
                self._engine_unavailable(engine)
            self._engine_metrics()
        await asyncio.gather(*(probe(engine) for engine in self.engines))
        self._engine_metrics()

    async def _probe_loop(self) -> None:
        while True:
            await self.probe_engines()
            await asyncio.sleep(10)

    def _engine_metrics(self) -> None:
        count = sum(engine.ready for engine in self.engines)
        self.metrics.engine_ready = count > 0
        self.metrics.engines.set(count)

    def _engine_unavailable(self, engine: Engine) -> None:
        engine.ready = False
        engine.failures += 1
        engine.available_after = time.monotonic() + min(60, 2 ** min(engine.failures, 6))
        self._engine_metrics()

    async def _heartbeat(self, task: dict, lost: asyncio.Event) -> None:
        # Stop computing well before the last confirmed lease can expire. A
        # successful heartbeat renews the lease; failed renewals never extend it.
        last_confirmed = time.monotonic()
        identity = {"worker_id": self.config.worker_id, "attempt_token": task["attempt_token"]}
        while True:
            await asyncio.sleep(self.heartbeat_seconds)
            try:
                await asyncio.wait_for(self._post(f"/internal/tasks/{task['id']}/heartbeat", identity),
                                       timeout=min(10, self.lease_seconds * .2))
                last_confirmed = time.monotonic()
            except StaleAttempt:
                lost.set()
                return
            except (httpx.HTTPError, ValueError, asyncio.TimeoutError):
                log.warning("Lease heartbeat unavailable", extra={"task_id": task["id"]})
                if time.monotonic() - last_confirmed >= self.lease_seconds * .65:
                    lost.set()
                    return

    async def _report(self, task: dict, operation: str, payload: dict, lost: asyncio.Event) -> None:
        # Keep the same token on retries. An ambiguous completion response must
        # never cause another model invocation; accepted completion is replayable.
        deadline = time.monotonic() + self.config.report_retry_seconds
        delay = .25
        body = {"worker_id": self.config.worker_id, "attempt_token": task["attempt_token"], **payload}
        while not lost.is_set():
            try:
                await self._post(f"/internal/tasks/{task['id']}/{operation}", body)
                return
            except StaleAttempt:
                raise
            except (httpx.HTTPError, ValueError):
                if time.monotonic() >= deadline:
                    raise
                await asyncio.sleep(delay + random.random() * delay * .2)
                delay = min(delay * 2, 5)
        raise StaleAttempt("Lease renewal could not be confirmed")

    async def _invoke(self, task: dict, engine: Engine) -> dict:
        body = dict(task["body"])
        # A task can only select the registered model, never an HTTP destination,
        # SDK tool, or executable code. Streaming is disabled for durable results.
        if body.get("model") != self.config.model:
            raise InferenceError("unsupported_model", "Task model does not match worker", False)
        allowed = {"model", "messages", "max_tokens", "max_completion_tokens", "temperature", "top_p", "seed", "stop", "response_format", "stream", "n"}
        if set(body) - allowed:
            raise InferenceError("unsupported_parameter", "Task contains unsupported inference parameters", False)
        if body.get("stream", False) is not False or body.get("n", 1) != 1:
            raise InferenceError("unsupported_parameter", "Batch requires stream=false and n=1", False)
        body["stream"] = False
        body["max_tokens"] = body.pop("max_completion_tokens", body.get("max_tokens", 256))
        if type(body["max_tokens"]) is not int or not 0 < body["max_tokens"] <= self.config.max_model_len:
            raise InferenceError("context_limit", "max_tokens exceeds the configured context bound", False)
        started = time.monotonic()
        try:
            response = await self.inference.post(engine.url + "/v1/chat/completions", json=body)
        except httpx.TimeoutException as exc:
            self._engine_unavailable(engine)
            raise InferenceError("inference_timeout", "Model request exceeded timeout") from exc
        except httpx.HTTPError as exc:
            self._engine_unavailable(engine)
            raise InferenceError("engine_unavailable", "Model engine connection failed") from exc
        finally:
            self.metrics.latency.observe(time.monotonic() - started)
        if response.status_code != 200:
            retryable = response.status_code in {408, 425, 429} or response.status_code >= 500
            if retryable:
                self._engine_unavailable(engine)
            # Never persist arbitrary engine error bodies: they may contain
            # prompts, headers, or internals. Status is sufficient for routing.
            raise InferenceError(f"engine_http_{response.status_code}", f"Model engine returned HTTP {response.status_code}", retryable)
        try:
            value = response.json()
            usage = value["usage"]
            if not isinstance(value.get("choices"), list) or not value["choices"]:
                raise ValueError("missing choices")
            for key in ("prompt_tokens", "completion_tokens"):
                if type(usage[key]) is not int or usage[key] < 0:
                    raise ValueError("invalid usage")
            if usage["prompt_tokens"] + usage["completion_tokens"] > self.config.max_model_len:
                raise ValueError("engine context cap does not match worker")
        except (KeyError, TypeError, ValueError) as exc:
            raise InferenceError("invalid_engine_response", "Engine did not return valid choices and token usage", False) from exc
        engine.failures = 0
        return value

    async def _invoke_available(self, task: dict) -> dict:
        deadline = time.monotonic() + self.config.request_timeout
        while True:
            ready = [engine for engine in self.engines if engine.ready]
            if ready:
                engine = min(ready, key=lambda item: item.inflight)
                engine.inflight += 1
                try:
                    return await self._invoke(task, engine)
                finally:
                    engine.inflight -= 1
            if time.monotonic() >= deadline:
                raise InferenceError("engine_unavailable", "No healthy inference engine became available")
            # Engine probes in the claim loop restore eligibility. Heartbeats
            # keep an already received claim valid while endpoints recover.
            await asyncio.sleep(.25)

    async def execute(self, task: dict) -> None:
        self.metrics.active.inc()
        self.metrics.tokens.inc(self.config.max_model_len)
        lost = asyncio.Event()
        heartbeat = asyncio.create_task(self._heartbeat(task, lost))
        invocation = asyncio.create_task(self._invoke_available(task))
        lease_lost = asyncio.create_task(lost.wait())
        try:
            done, _ = await asyncio.wait({invocation, lease_lost}, return_when=asyncio.FIRST_COMPLETED)
            if lease_lost in done:
                raise StaleAttempt("Lease lost during inference")
            try:
                result = await invocation
            except InferenceError as exc:
                await self._report(task, "fail", {"error": {"code": exc.code, "message": str(exc)}, "retryable": exc.retryable}, lost)
                self.metrics.results.labels("retry" if exc.retryable else "failed").inc()
            else:
                await self._report(task, "complete", {"response": result}, lost)
                self.metrics.results.labels("completed").inc()
                self.metrics.prompt.inc(result["usage"]["prompt_tokens"])
                self.metrics.completion.inc(result["usage"]["completion_tokens"])
        except StaleAttempt:
            self.metrics.results.labels("fenced").inc()
            log.info("Discarding fenced attempt", extra={"task_id": task["id"]})
        except asyncio.CancelledError:
            self.metrics.results.labels("interrupted").inc()
            raise
        except Exception:
            # Let lease recovery requeue unresolved work. Do not turn an
            # ambiguous durable completion into a different failure report.
            self.metrics.results.labels("report_unconfirmed").inc()
            log.exception("Task report could not be confirmed", extra={"task_id": task["id"]})
        finally:
            for child in (heartbeat, invocation, lease_lost):
                child.cancel()
            await asyncio.gather(heartbeat, invocation, lease_lost, return_exceptions=True)
            self.metrics.active.dec()
            self.metrics.tokens.dec(self.config.max_model_len)

    async def _pause(self, seconds: float) -> None:
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self.stop.wait(), timeout=seconds)

    async def run(self) -> None:
        backoff = .5
        next_registration = 0.0
        probes = asyncio.create_task(self._probe_loop())
        try:
            while not self.stop.is_set():
                try:
                    if time.monotonic() >= next_registration:
                        await self.register()
                        next_registration = time.monotonic() + min(self.heartbeat_seconds, 20)
                    available = self.config.slots - len(self.active)
                    if available and self.metrics.engine_ready:
                        result = await self._post("/internal/tasks/claim", {
                            "worker_id": self.config.worker_id, "model": self.config.model, "limit": min(available, 256),
                        })
                        tasks = result["tasks"]
                        if len(tasks) > min(available, 256):
                            raise ValueError("Control plane exceeded requested claim capacity")
                        for task in tasks:
                            future = asyncio.create_task(self.execute(task))
                            self.active.add(future)
                            future.add_done_callback(self.active.discard)
                    backoff = .5
                    await self._pause(self.config.poll_seconds)
                except (httpx.HTTPError, ValueError, KeyError, StaleAttempt):
                    log.warning("Control plane unavailable; claims paused", exc_info=True)
                    await self._pause(backoff + random.random() * backoff * .2)
                    backoff = min(backoff * 2, 20)
        finally:
            self.metrics.stopping = True
            if self.active:
                log.info("Draining %s in-flight tasks", len(self.active))
                _, pending = await asyncio.wait(self.active, timeout=self.config.shutdown_seconds)
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
            probes.cancel()
            await asyncio.gather(probes, return_exceptions=True)
            await self.close()


async def serve(settings: Settings) -> None:
    worker = Worker(settings)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, worker.stop.set)
    server = uvicorn.Server(uvicorn.Config(worker.metrics.app(), host=settings.metrics_host,
                                           port=settings.metrics_port, log_level="warning"))
    # Own signals so SIGTERM first drains leased work, then stops health server.
    server.install_signal_handlers = lambda: None
    server.capture_signals = contextlib.nullcontext
    health_task = asyncio.create_task(server.serve())
    try:
        await worker.run()
    finally:
        server.should_exit = True
        await health_task


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    asyncio.run(serve(Settings.from_env()))
