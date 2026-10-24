"""Lease-fenced async inference worker; all durable scheduling lives in Postgres."""
from __future__ import annotations

import asyncio
import contextlib
import json
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
from worker.kv_admission import (AdmissionPaused, PROTOCOL as KV_PROTOCOL,
    TokenizerProtocolError, WeightedLimiter, engine_quiescent, reservation,
    token_count, tokenizer_body)

log = logging.getLogger("batchforge.worker")


class StaleAttempt(Exception):
    pass


class InferenceError(Exception):
    def __init__(self, code: str, message: str, retryable: bool = True) -> None:
        super().__init__(message)
        self.code, self.retryable = code, retryable


def is_strict_json_object(content: Any) -> bool:
    """Validate raw output without repairing fences, control bytes or constants."""
    if not isinstance(content, str):
        return False

    def reject_constant(_value):
        raise ValueError("Non-finite JSON constant")

    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("Duplicate JSON object field")
            result[key] = value
        return result

    try:
        value = json.loads(content, parse_constant=reject_constant, object_pairs_hook=unique_object)
    except (ValueError, RecursionError):
        return False
    return isinstance(value, dict)


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
        self._capacity_changed = asyncio.Event()
        self._ready_engine_count = 0
        self._registered_ready_engine_count: int | None = None
        self._control_origins = settings.control_origins
        self._control_origin_index = 0
        self._control_origin_epoch = 0
        self.active: set[asyncio.Task] = set()
        self.engines = [Engine(url) for url in settings.inference_urls]
        self._adaptive_validated = False
        self.kv_admission = (WeightedLimiter(settings.token_budget, self._kv_metrics)
                             if settings.adaptive_kv_enabled else None)
        if self.kv_admission:
            self.metrics.kv_budget.set(settings.token_budget)
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

    def _kv_metrics(self, used, waiting, peak, paused):
        self.metrics.tokens.set(used)
        self.metrics.kv_waiting.set(waiting)
        self.metrics.kv_peak.set(peak)
        self.metrics.kv_paused.set(int(paused))

    def _pause_kv_admissions(self):
        if self.kv_admission:
            self.kv_admission.pause()
            for engine in self.engines:
                engine.ready = False
            self._engine_metrics()

    async def _post(self, path: str, body: dict) -> dict:
        operation = path.rsplit("/", 1)[-1]
        origin_epoch = self._control_origin_epoch
        origin = self._control_origins[self._control_origin_index]
        try:
            try:
                # Exactly one HTTP attempt here: claims and other POSTs may
                # have committed before an acknowledgement was lost. Changing
                # origin affects the NEXT caller request, never replays this one.
                response = await self.control.post(origin + path, json=body)
            except httpx.TransportError:
                self._advance_control_origin(origin_epoch)
                raise
            if response.status_code >= 500:
                self._advance_control_origin(origin_epoch)
            if response.status_code == 409:
                raise StaleAttempt("Control plane fenced the attempt")
            response.raise_for_status()
            try:
                value = response.json()
                if not isinstance(value, dict):
                    raise ValueError("Control plane response must be an object")
            except ValueError:
                self._advance_control_origin(origin_epoch)
                raise
        except Exception:
            self.metrics.requests.labels(operation, "error").inc()
            raise
        self.metrics.requests.labels(operation, "ok").inc()
        self.metrics.last_control_success = time.monotonic()
        return value

    def _advance_control_origin(self, observed_epoch: int) -> None:
        if len(self._control_origins) < 2 or observed_epoch != self._control_origin_epoch:
            return
        # Concurrent requests to an old origin cannot rotate a newer decision,
        # even after the configured origin index has cycled back to that host.
        self._control_origin_index = (self._control_origin_index + 1) % len(self._control_origins)
        self._control_origin_epoch += 1
        self.metrics.control_origin.set(self._control_origin_index)
        self.metrics.control_failovers.inc()
        log.warning("Control origin failed; subsequent requests will use configured origin index %s",
                    self._control_origin_index)

    async def register(self) -> None:
        if self.kv_admission and not self._adaptive_validated:
            raise ValueError('Adaptive tokenizer protocol has not passed startup validation')
        ready_engine_count = self._ready_engine_count
        scheduling_metadata = ({"scheduling_profile": self.config.scheduling_profile}
                               if self.config.scheduling_profile is not None else {})
        if self.kv_admission:
            scheduling_metadata['kv_admission'] = {'protocol': KV_PROTOCOL, 'budget': self.config.token_budget,
                'block_size': 16, 'single_worker_single_engine': True, 'paused': self.kv_admission.paused}
        result = await self._post("/internal/workers/register", {
            "worker_id": self.config.worker_id,
            "name": self.config.name,
            "model": self.config.model,
            "concurrency": self.config.slots,
            "metadata": {"engine_count": len(self.engines), "ready_engine_count": ready_engine_count,
                         "max_model_len": self.config.max_model_len,
                         "token_budget": self.config.token_budget, "version": "0.1.0",
                         "control_origin_count": len(self._control_origins), **scheduling_metadata},
        })
        if result["worker_id"] != self.config.worker_id:
            raise ValueError("Control plane changed the worker identity")
        self.lease_seconds = float(result["lease_seconds"])
        self.heartbeat_seconds = min(float(result["heartbeat_interval_seconds"]), self.lease_seconds / 4)
        if self.lease_seconds < 5 or self.heartbeat_seconds <= 0:
            raise ValueError("Unsafe lease configuration from control plane")
        self.metrics.registered = True
        self._registered_ready_engine_count = ready_engine_count

    async def probe_engines(self) -> None:
        async def probe(engine: Engine) -> None:
            if self.kv_admission and self.kv_admission.paused:
                # An uncertain remote invocation is not proof of freed GPU KV.
                # No automatic reset on a merely successful health probe.
                engine.ready = False
                self._engine_metrics()
                return
            if time.monotonic() < engine.available_after:
                return
            try:
                result = await asyncio.wait_for(self.inference.get(engine.url + "/v1/models", timeout=5), timeout=5)
                result.raise_for_status()
                engine.ready = any(item.get("id") == self.config.model for item in result.json()["data"])
                if engine.ready and self.kv_admission and not self._adaptive_validated:
                    if {item.get('id') for item in result.json()['data']} != {self.config.model}:
                        raise ValueError('Adaptive engine must advertise only its exact Qwen model')
                    await self._validate_adaptive_engine(engine)
                if not engine.ready:
                    self._engine_unavailable(engine)
            except (httpx.HTTPError, ValueError, KeyError, TypeError, asyncio.TimeoutError, InferenceError):
                self._engine_unavailable(engine)
            self._engine_metrics()
        await asyncio.gather(*(probe(engine) for engine in self.engines))
        self._engine_metrics()

    async def _validate_adaptive_engine(self, engine):
        version = await self.inference.get(engine.url + '/version', timeout=5)
        version.raise_for_status()
        version_data = version.json()
        if not isinstance(version_data, dict) or version_data.get('version') != '0.10.2':
            raise ValueError('Adaptive tokenizer protocol requires vLLM0.10.2')
        await self._exact_prompt_count({'model': self.config.model,
            'messages': [{'role':'user','content':'BatchForge tokenizer protocol check.'}]}, engine)
        metrics = await self.inference.get(engine.url + '/metrics', timeout=5)
        metrics.raise_for_status()
        if len(metrics.content) > 2*1024*1024 or not engine_quiescent(metrics.text):
            raise ValueError('Adaptive startup requires quiescent private engine')
        self._adaptive_validated = True

    async def _exact_prompt_count(self, body, engine):
        try:
            payload = tokenizer_body(body, self.config.model)
        except TokenizerProtocolError as exc:
            raise InferenceError('unsupported_tokenizer_request', 'Adaptive tokenizer supports only the validated text-chat shape', False) from exc
        try:
            response = await self.inference.post(engine.url + '/tokenize', json=payload, timeout=10)
            response.raise_for_status()
            if len(response.content) > 2*1024*1024:
                raise TokenizerProtocolError('Tokenizer response exceeds bound')
            return token_count(response.json(), self.config.max_model_len)
        except httpx.HTTPStatusError as exc:
            retryable = exc.response.status_code >= 500 or exc.response.status_code in {408,425,429}
            if retryable:self._pause_kv_admissions()
            raise InferenceError('tokenizer_http_error', 'Exact tokenizer request was rejected', retryable) from exc
        except (httpx.HTTPError, ValueError, TypeError) as exc:
            self._pause_kv_admissions()
            raise InferenceError('tokenizer_unavailable', 'Exact tokenizer protocol could not be verified') from exc

    async def _probe_loop(self) -> None:
        while True:
            await self.probe_engines()
            await asyncio.sleep(10)

    def _engine_metrics(self) -> None:
        count = sum(engine.ready for engine in self.engines)
        self.metrics.engine_ready = count > 0
        self.metrics.engines.set(count)
        if count != self._ready_engine_count:
            self._ready_engine_count = count
            self._capacity_changed.set()

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
        with self.metrics.time_stage('report_ack'):
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

    def _inference_body(self, task):
        body = dict(task["body"])
        # A task can only select the registered model, never an HTTP destination,
        # SDK tool, or executable code. Streaming is disabled for durable results.
        if body.get("model") != self.config.model:
            raise InferenceError("unsupported_model", "Task model does not match worker", False)
        allowed = {"model", "messages", "max_tokens", "max_completion_tokens", "min_tokens", "temperature", "top_p", "seed", "stop", "response_format", "stream", "n", "reasoning_effort"}
        if set(body) - allowed:
            raise InferenceError("unsupported_parameter", "Task contains unsupported inference parameters", False)
        if "reasoning_effort" in body and (not isinstance(body["reasoning_effort"], str) or body["reasoning_effort"] not in {"low", "medium", "high"}):
            raise InferenceError("unsupported_parameter", "reasoning_effort must be low, medium, or high", False)
        if body.get("stream", False) is not False or body.get("n", 1) != 1:
            raise InferenceError("unsupported_parameter", "Batch requires stream=false and n=1", False)
        body["stream"] = False
        body["max_tokens"] = body.pop("max_completion_tokens", body.get("max_tokens", 256))
        if type(body["max_tokens"]) is not int or not 0 < body["max_tokens"] <= self.config.max_model_len:
            raise InferenceError("context_limit", "max_tokens exceeds the configured context bound", False)
        if 'min_tokens' in body and (type(body['min_tokens']) is not int or not 0 <= body['min_tokens'] <= body['max_tokens']):
            raise InferenceError('unsupported_parameter', 'min_tokens must fit the output limit', False)
        return body

    async def _invoke(self, task: dict, engine: Engine, *, expected_prompt_tokens=None) -> dict:
        body = self._inference_body(task)
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
            if not body.get('min_tokens',0) <= usage['completion_tokens'] <= body['max_tokens']:
                raise ValueError('engine did not respect requested output token bounds')
        except (KeyError, TypeError, ValueError) as exc:
            raise InferenceError("invalid_engine_response", "Engine did not return valid choices and token usage", False) from exc
        if expected_prompt_tokens is not None:
            if value.get('model') != self.config.model or usage['prompt_tokens'] != expected_prompt_tokens:
                raise InferenceError('tokenizer_mismatch', 'Inference model or prompt usage differs from exact tokenizer preflight', False)
        engine.failures = 0
        if task["body"].get("response_format") == {"type": "json_object"}:
            try:
                content = value["choices"][0]["message"]["content"]
            except (KeyError, TypeError):
                content = None
            if not is_strict_json_object(content):
                # These counters observe actual engine work, independently of
                # whether a durable fail acknowledgement can be confirmed.
                # Rejected output is never sent as accepted completion usage.
                self.metrics.invalid_outputs.inc()
                self.metrics.rejected_prompt.inc(usage["prompt_tokens"])
                self.metrics.rejected_generated.inc(usage["completion_tokens"])
                raise InferenceError("invalid_json_output", "Model output did not satisfy the requested JSON object format") from None
        return value

    async def _invoke_adaptive(self, task, engine):
        if not self._adaptive_validated or self.kv_admission.paused:
            raise InferenceError('kv_admission_paused', 'Exact-token admission needs operator validation')
        remote_started = False
        try:
            async with asyncio.timeout(self.config.request_timeout):
                body = self._inference_body(task)
                with self.metrics.time_stage('tokenize'):
                    prompt = await self._exact_prompt_count(body, engine)
                try:weight = reservation(prompt, body['max_tokens'], self.config.max_model_len)
                except ValueError as exc:
                    raise InferenceError('context_limit', 'Exact prompt plus requested output exceeds configured context', False) from exc
                async with contextlib.AsyncExitStack() as stack:
                    with self.metrics.time_stage('kv_admission_wait'):
                        await stack.enter_async_context(self.kv_admission.reserve(weight))
                    remote_started = True
                    return await self._invoke(task, engine, expected_prompt_tokens=prompt)
        except AdmissionPaused as exc:
            raise InferenceError('kv_admission_paused', 'Exact-token admission needs operator intervention') from exc
        except asyncio.CancelledError:
            if remote_started:self._pause_kv_admissions()
            raise
        except TimeoutError as exc:
            if remote_started:self._pause_kv_admissions()
            raise InferenceError('kv_admission_timeout', 'Exact-token admission or invocation exceeded its bounded wait') from exc
        except InferenceError as exc:
            if remote_started and (exc.code in {'inference_timeout','engine_unavailable','invalid_engine_response','tokenizer_mismatch'}
                    or exc.code.startswith('engine_http_') and exc.retryable):
                self._pause_kv_admissions()
            raise

    async def _select_engine(self) -> Engine:
        deadline = time.monotonic() + self.config.request_timeout
        with self.metrics.time_stage('engine_ready_wait'):
            while True:
                if self.kv_admission and self.kv_admission.paused:
                    raise InferenceError('kv_admission_paused', 'Exact-token admission needs operator intervention')
                ready = [engine for engine in self.engines if engine.ready]
                if ready:
                    engine = min(ready, key=lambda item: item.inflight)
                    engine.inflight += 1
                    return engine
                if time.monotonic() >= deadline:
                    raise InferenceError("engine_unavailable", "No healthy inference engine became available")
                # Engine probes restore eligibility. Heartbeats preserve claims.
                await asyncio.sleep(.25)

    async def _invoke_available(self, task: dict) -> dict:
        engine = await self._select_engine()
        try:
            if self.kv_admission:
                return await self._invoke_adaptive(task, engine)
            return await self._invoke(task, engine)
        finally:
            engine.inflight -= 1

    async def execute(self, task: dict) -> None:
        self.metrics.active.inc()
        if not self.kv_admission:self.metrics.tokens.inc(self.config.max_model_len)
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
            if not self.kv_admission:self.metrics.tokens.dec(self.config.max_model_len)

    async def _pause(self, seconds: float) -> None:
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self.stop.wait(), timeout=seconds)

    def _task_finished(self, task: asyncio.Task) -> None:
        self.active.discard(task)
        self._capacity_changed.set()

    async def _wait_for_capacity(self, seconds: float) -> None:
        # Both events retain notifications sent before this wait is installed.
        # The loop clears capacity notifications before reading current state,
        # so a completion between state inspection and this wait cannot be lost.
        stopping = asyncio.create_task(self.stop.wait())
        changed = asyncio.create_task(self._capacity_changed.wait())
        try:
            await asyncio.wait({stopping, changed}, timeout=max(0, seconds),
                               return_when=asyncio.FIRST_COMPLETED)
        finally:
            stopping.cancel()
            changed.cancel()
            await asyncio.gather(stopping, changed, return_exceptions=True)

    async def run(self) -> None:
        backoff = .5
        idle_delay = self.config.poll_seconds
        idle_cap = max(self.config.poll_seconds, 2.0)
        next_registration = 0.0
        probes = asyncio.create_task(self._probe_loop())
        try:
            while not self.stop.is_set():
                self._capacity_changed.clear()
                try:
                    if (time.monotonic() >= next_registration or
                            self._registered_ready_engine_count != self._ready_engine_count):
                        await self.register()
                        next_registration = time.monotonic() + min(self.heartbeat_seconds, 20)
                    available = self.config.slots - len(self.active)
                    wait_seconds = max(0, next_registration - time.monotonic())
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
                            future.add_done_callback(self._task_finished)
                        if tasks:
                            # A claim may stop at one batch while more ready
                            # batches remain. Fill the remaining capacity now.
                            # Full workers then wait for a completion event.
                            idle_delay = self.config.poll_seconds
                            backoff = .5
                            await asyncio.sleep(0)
                            continue
                        wait_seconds = min(wait_seconds, idle_delay * random.uniform(.8, 1.0))
                        idle_delay = min(idle_delay * 2, idle_cap)
                    backoff = .5
                    await self._wait_for_capacity(wait_seconds)
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
