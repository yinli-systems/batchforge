import asyncio
from dataclasses import replace
import json

import httpx
import pytest

from worker.config import Settings
from worker.runtime import Worker


def settings(**overrides):
    return replace(Settings(control_url="http://control", worker_token="a" * 32,
                            inference_urls=("http://engine",), model="test-model",
                            worker_id="test-worker", name="test", report_retry_seconds=1), **overrides)


def task():
    return {"id": "task1", "attempt_token": "attempt1", "body": {"model": "test-model", "messages": [{"role": "user", "content": "hello"}], "max_tokens": 16}}


def response():
    return {"choices": [{"message": {"role": "assistant", "content": "hello"}}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1}}


def json_object_task():
    value = task()
    value["body"]["response_format"] = {"type": "json_object"}
    return value


def generated_response(content):
    value = response()
    value["choices"][0]["message"]["content"] = content
    value["usage"] = {"prompt_tokens": 5, "completion_tokens": 7}
    return value


def make_worker(*args, **kwargs):
    worker = Worker(*args, **kwargs)
    # Unit execution tests start after the independent readiness probes would
    # have admitted these fixtures. Full run tests exercise the actual probes.
    for engine in worker.engines:
        engine.ready = True
    worker._engine_metrics()
    return worker


@pytest.mark.asyncio
async def test_ambiguous_completion_is_retried_with_same_token_without_reinference():
    completions, inferences = [], []

    async def control(request):
        if request.url.path.endswith("complete"):
            completions.append(request.content)
            if len(completions) == 1:
                # Simulate a lost response after the DB could have committed.
                raise httpx.ReadError("lost acknowledgement")
        return httpx.Response(200, json={"ok": True})

    async def engine(request):
        inferences.append(request)
        return httpx.Response(200, json=response())

    worker = make_worker(settings(), control_transport=httpx.MockTransport(control), inference_transport=httpx.MockTransport(engine))
    try:
        await worker.execute(task())
        assert len(inferences) == 1
        assert len(completions) == 2 and completions[0] == completions[1]
    finally:
        await worker.close()


@pytest.mark.asyncio
async def test_stale_heartbeat_cancels_inference_and_discards_result():
    requests, cancelled = [], asyncio.Event()

    async def control(request):
        requests.append(request.url.path)
        return httpx.Response(409, json={"error": {"code": "stale_attempt"}})

    async def engine(request):
        try:
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            cancelled.set()
            raise
        return httpx.Response(200, json=response())

    worker = make_worker(settings(), control_transport=httpx.MockTransport(control), inference_transport=httpx.MockTransport(engine))
    worker.heartbeat_seconds = .01
    try:
        await worker.execute(task())
        assert cancelled.is_set()
        assert requests == ["/internal/tasks/task1/heartbeat"]
        assert worker.metrics.active._value.get() == 0
    finally:
        await worker.close()


@pytest.mark.asyncio
async def test_transient_engine_failure_reports_retryable_without_exposing_body():
    reports = []

    async def control(request):
        import json
        reports.append(json.loads(request.content))
        return httpx.Response(200, json={"ok": True, "status": "pending"})

    worker = make_worker(settings(), control_transport=httpx.MockTransport(control),
                    inference_transport=httpx.MockTransport(lambda _: httpx.Response(503, text="SECRET PROMPT")))
    try:
        await worker.execute(task())
        assert reports[0]["retryable"] is True
        assert reports[0]["error"]["code"] == "engine_http_503"
        assert "SECRET" not in str(reports)
    finally:
        await worker.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("content", ['{}', ' {"translation_zh":"你好","nested":{"ok":true}} \n'])
async def test_requested_json_object_is_completed_without_claiming_field_fidelity(content):
    reports = []

    async def control(request):
        reports.append((request.url.path, json.loads(request.content)))
        return httpx.Response(200, json={"ok": True})

    worker = make_worker(settings(), control_transport=httpx.MockTransport(control),
                         inference_transport=httpx.MockTransport(lambda _: httpx.Response(200, json=generated_response(content))))
    try:
        await worker.execute(json_object_task())
        assert len(reports) == 1 and reports[0][0].endswith("/complete")
        assert reports[0][1]["response"]["choices"][0]["message"]["content"] == content
        assert worker.metrics.prompt._value.get() == 5
        assert worker.metrics.completion._value.get() == 7
        assert worker.metrics.invalid_outputs._value.get() == 0
        assert worker.metrics.rejected_generated._value.get() == 0
    finally:
        await worker.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("content", [
    '```json\n{"value":"PRIVATE-GENERATED-TEXT"}\n```',
    '{"value":"PRIVATE-GENERATED-TEXT\nraw newline"}',
    '{"value":"PRIVATE-GENERATED-TEXT”}<tool_call>',
    '{"value":NaN}', '{"value":Infinity}',
    '[]', '"PRIVATE-GENERATED-TEXT"', 'null',
    '{"value":1,"value":2}', None,
])
async def test_invalid_json_output_uses_durable_retry_without_accepted_usage_or_reinference(content):
    reports, inferences = [], []

    async def control(request):
        reports.append((request.url.path, json.loads(request.content)))
        return httpx.Response(200, json={"ok": True, "status": "pending"})

    async def engine(request):
        inferences.append(request)
        return httpx.Response(200, json=generated_response(content))

    worker = make_worker(settings(), control_transport=httpx.MockTransport(control),
                         inference_transport=httpx.MockTransport(engine))
    try:
        await worker.execute(json_object_task())
        assert len(inferences) == 1
        assert len(reports) == 1 and reports[0][0].endswith("/fail")
        assert reports[0][1] == {
            "worker_id": "test-worker", "attempt_token": "attempt1",
            "error": {"code": "invalid_json_output", "message": "Model output did not satisfy the requested JSON object format"},
            "retryable": True,
        }
        assert "PRIVATE-GENERATED-TEXT" not in json.dumps(reports)
        assert worker.metrics.prompt._value.get() == worker.metrics.completion._value.get() == 0
        assert worker.metrics.invalid_outputs._value.get() == 1
        assert worker.metrics.rejected_prompt._value.get() == 5
        assert worker.metrics.rejected_generated._value.get() == 7
        assert worker.engines[0].ready is True
        samples = [sample for metric in worker.metrics.registry.collect()
                   for sample in metric.samples if sample.name.startswith("batchforge_worker_rejected_")]
        assert samples and all(sample.labels == {} for sample in samples)
    finally:
        await worker.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("response_format", [None, {"type": "text"}])
async def test_output_without_explicit_json_object_contract_is_unchanged(response_format):
    value = task()
    if response_format is not None:
        value["body"]["response_format"] = response_format
    reports = []

    async def control(request):
        reports.append((request.url.path, json.loads(request.content)))
        return httpx.Response(200, json={"ok": True})

    worker = make_worker(settings(), control_transport=httpx.MockTransport(control),
                         inference_transport=httpx.MockTransport(lambda _: httpx.Response(200, json=generated_response("ordinary non-JSON output"))))
    try:
        await worker.execute(value)
        assert len(reports) == 1 and reports[0][0].endswith("/complete")
        assert reports[0][1]["response"]["choices"][0]["message"]["content"] == "ordinary non-JSON output"
        assert worker.metrics.completion._value.get() == 7
        assert worker.metrics.invalid_outputs._value.get() == worker.metrics.rejected_generated._value.get() == 0
    finally:
        await worker.close()


@pytest.mark.asyncio
async def test_invalid_output_report_ack_loss_keeps_same_fence_and_never_generates_again():
    reports, inferences = [], []

    async def control(request):
        assert request.url.path.endswith("/fail")
        reports.append(request.content)
        if len(reports) == 1:
            raise httpx.ReadError("lost failure acknowledgement")
        return httpx.Response(409, json={"error": {"code": "stale_attempt"}})

    async def engine(request):
        inferences.append(request)
        return httpx.Response(200, json=generated_response('{"value":"unterminated'))

    worker = make_worker(settings(), control_transport=httpx.MockTransport(control),
                         inference_transport=httpx.MockTransport(engine))
    try:
        await worker.execute(json_object_task())
        assert len(inferences) == 1
        assert len(reports) == 2 and reports[0] == reports[1]
        assert worker.metrics.results.labels("fenced")._value.get() == 1
        assert worker.metrics.prompt._value.get() == worker.metrics.completion._value.get() == 0
        assert worker.metrics.invalid_outputs._value.get() == 1
        assert worker.metrics.rejected_generated._value.get() == 7
    finally:
        await worker.close()


@pytest.mark.asyncio
async def test_external_destination_is_not_accepted_as_task_parameter():
    reports = []

    async def control(request):
        import json
        reports.append(json.loads(request.content))
        return httpx.Response(200, json={"ok": True})

    def engine(request):
        pytest.fail("Engine must not be called for an invalid task")

    worker = make_worker(settings(), control_transport=httpx.MockTransport(control), inference_transport=httpx.MockTransport(engine))
    invalid = task()
    invalid["body"]["base_url"] = "http://attacker"
    try:
        await worker.execute(invalid)
        assert reports[0]["retryable"] is False
        assert reports[0]["error"]["code"] == "unsupported_parameter"
    finally:
        await worker.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("effort", [None, "low", "medium", "high"])
async def test_reasoning_effort_forwarded_unchanged_and_omitted_by_default(effort):
    invoked, reports = [], []
    def engine(request):
        invoked.append(json.loads(request.content))
        return httpx.Response(200, json=response())
    def control(request):
        reports.append(request.url.path)
        return httpx.Response(200, json={"ok": True})
    worker = make_worker(settings(), control_transport=httpx.MockTransport(control), inference_transport=httpx.MockTransport(engine))
    item = task()
    if effort is not None:
        item["body"]["reasoning_effort"] = effort
    original = dict(item["body"])
    try:
        await worker.execute(item)
        assert len(invoked) == 1 and reports == ["/internal/tasks/task1/complete"]
        assert ("reasoning_effort" in invoked[0]) == (effort is not None)
        assert invoked[0].get("reasoning_effort") == effort
        assert item["body"] == original
    finally:
        await worker.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", [None, True, 1, [], {}, "", "LOW", "none", "minimal"])
async def test_invalid_reasoning_effort_fails_without_engine_call(invalid):
    reports = []
    def control(request):
        reports.append(json.loads(request.content))
        return httpx.Response(200, json={"ok": True})
    def engine(request):
        pytest.fail("An invalid effort must not reach the engine")
    worker = make_worker(settings(), control_transport=httpx.MockTransport(control), inference_transport=httpx.MockTransport(engine))
    item = task()
    item["body"]["reasoning_effort"] = invalid
    try:
        await worker.execute(item)
        assert len(reports) == 1 and reports[0]["error"]["code"] == "unsupported_parameter"
        assert reports[0]["retryable"] is False
    finally:
        await worker.close()


def test_context_budget_is_a_hard_capacity_bound():
    assert settings(max_inflight=100, token_budget=8192, max_model_len=4096).slots == 2


@pytest.mark.asyncio
async def test_failed_generation_endpoint_is_cooled_down_and_healthy_engine_takes_next_task():
    destinations = []

    async def engine(request):
        destinations.append(request.url.host)
        if request.url.host == "broken":
            return httpx.Response(503)
        return httpx.Response(200, json=response())

    worker = make_worker(settings(inference_urls=("http://broken", "http://healthy")),
        control_transport=httpx.MockTransport(lambda _: httpx.Response(200, json={"ok": True})),
        inference_transport=httpx.MockTransport(engine))
    try:
        await worker.execute(task())
        await worker.execute(task())
        assert destinations == ["broken", "healthy"]
        assert worker.engines[0].ready is False
        assert worker.engines[0].available_after > 0
    finally:
        await worker.close()


@pytest.mark.asyncio
async def test_claim_capacity_and_graceful_stop_preserve_outstanding_work():
    import json
    started, release = asyncio.Event(), asyncio.Event()
    invocations, reports, claim_limits = [], [], []

    async def control(request):
        body = json.loads(request.content)
        if request.url.path.endswith("register"):
            return httpx.Response(200, json={"worker_id": "test-worker", "lease_seconds": 90, "heartbeat_interval_seconds": 10})
        if request.url.path.endswith("claim"):
            claim_limits.append(body["limit"])
            items = []
            for i in range(body["limit"]):
                value = task()
                value["id"] += str(i)
                items.append(value)
            return httpx.Response(200, json={"tasks": items, "lease_seconds": 90})
        reports.append(request.url.path)
        return httpx.Response(200, json={"ok": True})

    async def engine(request):
        if request.method == "GET":
            return httpx.Response(200, json={"data": [{"id": "test-model"}]})
        invocations.append(request)
        if len(invocations) == 2:
            started.set()
        await release.wait()
        return httpx.Response(200, json=response())

    config = settings(max_inflight=100, token_budget=4096, max_model_len=2048, poll_seconds=.01)
    worker = Worker(config, control_transport=httpx.MockTransport(control), inference_transport=httpx.MockTransport(engine))
    running = asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(started.wait(), 1)
        assert worker.metrics.tokens._value.get() == 4096
        worker.stop.set()
        await asyncio.sleep(.02)
        assert not running.done()
        release.set()
        await asyncio.wait_for(running, 1)
        assert claim_limits == [2]
        assert len(reports) == 2 and all(path.endswith("complete") for path in reports)
        assert worker.metrics.tokens._value.get() == 0
    finally:
        release.set()
        worker.stop.set()
        if not running.done():
            await running


@pytest.mark.asyncio
async def test_stalled_engine_probe_does_not_block_healthy_replica_or_claim_loop():
    import json
    completed, stalled = asyncio.Event(), asyncio.Event()
    claimed = False

    async def control(request):
        nonlocal claimed
        if request.url.path.endswith("register"):
            return httpx.Response(200, json={"worker_id": "test-worker", "lease_seconds": 90, "heartbeat_interval_seconds": 10})
        if request.url.path.endswith("claim"):
            tasks = [] if claimed else [task()]
            claimed = True
            return httpx.Response(200, json={"tasks": tasks, "lease_seconds": 90})
        if request.url.path.endswith("complete"):
            completed.set()
            worker.stop.set()
        return httpx.Response(200, json={"ok": True})

    async def engine(request):
        if request.url.host == "stalled":
            stalled.set()
            await asyncio.Event().wait()
        if request.method == "GET":
            return httpx.Response(200, json={"data": [{"id": "test-model"}]})
        return httpx.Response(200, json=response())

    worker = Worker(settings(inference_urls=("http://stalled", "http://healthy"), poll_seconds=.01),
        control_transport=httpx.MockTransport(control), inference_transport=httpx.MockTransport(engine))
    running = asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(completed.wait(), .5)
        assert stalled.is_set()
        assert worker.engines[0].ready is False
        await asyncio.wait_for(running, .5)
    finally:
        worker.stop.set()
        if not running.done():
            await running
