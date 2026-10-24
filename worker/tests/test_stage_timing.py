"""Stage timing must preserve cancellation, KV accounting and report fencing."""
import asyncio
from types import SimpleNamespace

import httpx
import pytest
from prometheus_client import generate_latest

from worker.metrics import Metrics
from worker.runtime import InferenceError, StaleAttempt
from worker.tests.test_kv_admission import generated, prepared, task


def sample(metrics, stage, outcome, suffix):
    return metrics.registry.get_sample_value(
        'batchforge_worker_stage_seconds_' + suffix,
        {'stage': stage, 'outcome': outcome},
    )


@pytest.mark.parametrize('outcome', ['ok', 'error', 'cancelled'])
def test_monotonic_observation_preserves_outcome_and_exception(monkeypatch, outcome):
    clock = [10.0]
    monkeypatch.setattr('worker.metrics.time', SimpleNamespace(monotonic=lambda: clock[0]))
    metrics = Metrics()
    error = None if outcome == 'ok' else (asyncio.CancelledError() if outcome == 'cancelled' else ValueError('secret'))
    try:
        with metrics.time_stage('report_ack'):
            clock[0] = 12.5
            if error is not None:
                raise error
    except BaseException as caught:
        assert caught is error
    assert sample(metrics, 'report_ack', outcome, 'count') == 1
    assert sample(metrics, 'report_ack', outcome, 'sum') == 2.5
    assert b'secret' not in generate_latest(metrics.registry)


def test_dynamic_stage_labels_are_rejected_without_allocating_series():
    metrics = Metrics()
    with pytest.raises(ValueError, match='Unknown worker timing stage'):
        with metrics.time_stage('task-secret-tenant'):
            pytest.fail('unreachable')
    assert b'task-secret-tenant' not in generate_latest(metrics.registry)


@pytest.mark.asyncio
async def test_tokenization_kv_wait_and_report_wait_exclude_remote_execution(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr('worker.metrics.time', SimpleNamespace(monotonic=lambda: clock[0]))
    calls = []

    async def engine(request):
        calls.append(request.url.path)
        if request.url.path == '/tokenize':
            clock[0] = 2.0
            return httpx.Response(200, json={'count':16, 'tokens':list(range(16)), 'max_model_len':128})
        clock[0] = 100.0
        return httpx.Response(200, json=generated())

    async def control(request):
        clock[0] = 104.0
        return httpx.Response(200, json={'ok':True})

    worker = prepared(engine, control=control)
    future = None
    try:
        async with worker.kv_admission.reserve(256):
            future = asyncio.create_task(worker.execute(task('stage-private-task')))
            async with asyncio.timeout(2):
                while worker.kv_admission.waiting != 1:
                    await asyncio.sleep(0)
            assert calls == ['/tokenize']
            clock[0] = 5.0
        await asyncio.wait_for(future, 2)
        for stage, duration in [('engine_ready_wait',0.0), ('tokenize',2.0), ('kv_admission_wait',3.0), ('report_ack',4.0)]:
            assert sample(worker.metrics, stage, 'ok', 'count') == 1
            assert sample(worker.metrics, stage, 'ok', 'sum') == duration
        assert worker.kv_admission.used == worker.kv_admission.waiting == 0
        assert worker.engines[0].inflight == 0
        assert worker.metrics.results.labels('completed')._value.get() == 1
        exposition = generate_latest(worker.metrics.registry)
        assert b'stage-private-task' not in exposition
        assert b'hello' not in exposition
    finally:
        if future:
            future.cancel()
            await asyncio.gather(future, return_exceptions=True)
        await worker.close()


@pytest.mark.asyncio
async def test_cancelled_kv_wait_is_observed_without_remote_invocation_or_pause():
    calls = []

    async def engine(request):
        calls.append(request.url.path)
        return httpx.Response(200, json={'count':16, 'tokens':list(range(16)), 'max_model_len':128})

    worker = prepared(engine)
    future = None
    try:
        async with worker.kv_admission.reserve(256):
            future = asyncio.create_task(worker._invoke_available(task()))
            async with asyncio.timeout(2):
                while worker.kv_admission.waiting != 1:
                    await asyncio.sleep(0)
            future.cancel()
            with pytest.raises(asyncio.CancelledError):
                await future
            assert calls == ['/tokenize']
            assert worker.kv_admission.used == 256
            assert worker.kv_admission.waiting == 0
            assert not worker.kv_admission.paused
            assert worker.engines[0].inflight == 0
            assert sample(worker.metrics, 'kv_admission_wait', 'cancelled', 'count') == 1
        assert worker.kv_admission.used == 0
    finally:
        if future:
            future.cancel()
            await asyncio.gather(future, return_exceptions=True)
        await worker.close()


@pytest.mark.asyncio
async def test_tokenizer_error_remains_fail_closed_and_does_not_count_admission():
    worker = prepared(lambda request: httpx.Response(503, text='private engine details'))
    try:
        with pytest.raises(InferenceError) as error:
            await worker._invoke_available(task())
        assert error.value.code == 'tokenizer_http_error'
        assert worker.kv_admission.paused and worker.kv_admission.used == 0
        assert worker.engines[0].inflight == 0
        assert sample(worker.metrics, 'tokenize', 'error', 'count') == 1
        assert sample(worker.metrics, 'kv_admission_wait', 'ok', 'count') is None
        assert b'private engine details' not in generate_latest(worker.metrics.registry)
    finally:
        await worker.close()


@pytest.mark.asyncio
async def test_ambiguous_completion_times_one_ack_interval_without_reinvocation():
    invocations, reports = [], []

    async def engine(request):
        if request.url.path == '/tokenize':
            return httpx.Response(200, json={'count':16, 'tokens':list(range(16)), 'max_model_len':128})
        invocations.append(request.url.path)
        return httpx.Response(200, json=generated())

    async def control(request):
        reports.append(request.content)
        if len(reports) == 1:
            raise httpx.ReadError('lost ack')
        return httpx.Response(200, json={'ok':True})

    worker = prepared(engine, control=control)
    try:
        await worker.execute(task())
        assert len(invocations) == 1 and len(reports) == 2 and reports[0] == reports[1]
        assert sample(worker.metrics, 'report_ack', 'ok', 'count') == 1
        assert sample(worker.metrics, 'report_ack', 'ok', 'sum') >= .25
        assert worker.metrics.results.labels('completed')._value.get() == 1
    finally:
        await worker.close()


@pytest.mark.asyncio
async def test_fenced_report_remains_error_without_changing_attempt_outcome():
    worker = prepared(lambda request: httpx.Response(200, json=generated()),
                      control=lambda request: httpx.Response(409, json={'error':{'code':'stale_attempt'}}))
    try:
        with pytest.raises(StaleAttempt):
            await worker._report(task(), 'complete', {'response':generated()}, asyncio.Event())
        assert sample(worker.metrics, 'report_ack', 'error', 'count') == 1
        assert worker.metrics.prompt._value.get() == 0
    finally:
        await worker.close()


@pytest.mark.asyncio
async def test_engine_readiness_wait_cancellation_does_not_allocate_engine_slot():
    worker = prepared(lambda request: pytest.fail('no inference while unready'))
    worker.engines[0].ready = False
    future = asyncio.create_task(worker._invoke_available(task()))
    try:
        await asyncio.sleep(0)
        future.cancel()
        with pytest.raises(asyncio.CancelledError):
            await future
        assert worker.engines[0].inflight == 0
        assert sample(worker.metrics, 'engine_ready_wait', 'cancelled', 'count') == 1
        assert sample(worker.metrics, 'tokenize', 'ok', 'count') is None
    finally:
        future.cancel()
        await asyncio.gather(future, return_exceptions=True)
        await worker.close()


@pytest.mark.asyncio
async def test_async_exit_stack_grant_then_cancel_releases_exactly_once():
    calls=[]
    async def engine(request):
        calls.append(request.url.path)
        return httpx.Response(200,json={'count':16,'tokens':list(range(16)),'max_model_len':128})
    worker=prepared(engine)
    holder=worker.kv_admission.reserve(256)
    await holder.__aenter__()
    invocation=asyncio.create_task(worker._invoke_available(task()))
    try:
        async with asyncio.timeout(2):
            while worker.kv_admission.waiting != 1: await asyncio.sleep(0)
        await holder.__aexit__(None,None,None)
        # _drain already granted synchronously; cancellation wins before waiter resumes.
        assert worker.kv_admission.used == 32 and worker.kv_admission.waiting == 0
        invocation.cancel()
        with pytest.raises(asyncio.CancelledError): await invocation
        assert calls == ['/tokenize']
        assert worker.kv_admission.used == worker.kv_admission.waiting == 0
        assert not worker.kv_admission.paused and worker.engines[0].inflight == 0
        assert sample(worker.metrics,'kv_admission_wait','cancelled','count') == 1
    finally:
        invocation.cancel(); await asyncio.gather(invocation,return_exceptions=True)
        await worker.close()

@pytest.mark.asyncio
async def test_remote_cancel_after_instrumented_reservation_latches_pause():
    entered=asyncio.Event()
    async def engine(request):
        if request.url.path == '/tokenize':
            return httpx.Response(200,json={'count':16,'tokens':list(range(16)),'max_model_len':128})
        entered.set(); await asyncio.sleep(60)
    worker=prepared(engine)
    invocation=asyncio.create_task(worker._invoke_available(task()))
    try:
        await asyncio.wait_for(entered.wait(),2)
        assert worker.kv_admission.used == 32
        invocation.cancel()
        with pytest.raises(asyncio.CancelledError): await invocation
        assert worker.kv_admission.used == worker.kv_admission.waiting == 0
        assert worker.kv_admission.paused and not worker.metrics.engine_ready
        assert worker.engines[0].inflight == 0
        assert sample(worker.metrics,'kv_admission_wait','ok','count') == 1
    finally:
        invocation.cancel(); await asyncio.gather(invocation,return_exceptions=True)
        await worker.close()

@pytest.mark.asyncio
async def test_cancel_during_ambiguous_report_preserves_no_reinvocation_or_usage():
    reports=[];invocations=[];sent=asyncio.Event()
    async def engine(request):
        if request.url.path == '/tokenize':
            return httpx.Response(200,json={'count':16,'tokens':list(range(16)),'max_model_len':128})
        invocations.append(request.url.path)
        return httpx.Response(200,json=generated())
    async def control(request):
        reports.append(request.content);sent.set();raise httpx.ReadError('uncertain acknowledgement')
    worker=prepared(engine,control=control)
    execution=asyncio.create_task(worker.execute(task()))
    try:
        await asyncio.wait_for(sent.wait(),2)
        execution.cancel()
        with pytest.raises(asyncio.CancelledError): await execution
        assert len(invocations) == len(reports) == 1
        assert worker.kv_admission.used == 0 and not worker.kv_admission.paused
        assert worker.engines[0].inflight == 0
        assert sample(worker.metrics,'report_ack','cancelled','count') == 1
        assert worker.metrics.prompt._value.get() == worker.metrics.completion._value.get() == 0
        assert worker.metrics.results.labels('interrupted')._value.get() == 1
        assert worker.metrics.results.labels('completed')._value.get() == 0
    finally:
        execution.cancel(); await asyncio.gather(execution,return_exceptions=True)
        await worker.close()

@pytest.mark.asyncio
async def test_already_lost_lease_cannot_post_even_with_report_timing():
    worker=prepared(lambda req: pytest.fail('no engine invocation'),
                    control=lambda req:pytest.fail('lost lease must not POST'))
    lost=asyncio.Event();lost.set()
    try:
        with pytest.raises(StaleAttempt): await worker._report(task(),'complete',{'response':generated()},lost)
        assert sample(worker.metrics,'report_ack','error','count') == 1
        assert worker.metrics.prompt._value.get() == 0
    finally: await worker.close()
