import asyncio
from dataclasses import replace
import json

import httpx
import pytest

from worker.config import Settings
from worker.kv_admission import (AdmissionPaused, MODEL, TokenizerProtocolError,
    WeightedLimiter, engine_quiescent, reservation, token_count)
from worker.runtime import InferenceError, Worker


def configuration(**changes):
    return replace(Settings(control_url='http://control', worker_token='x'*32,
        inference_urls=('http://engine',), model=MODEL, worker_id='worker', name='test',
        max_model_len=128, token_budget=256, max_inflight=8, adaptive_kv_enabled=True), **changes)


def task(identifier='task', prompt='hello', cap=16):
    return {'id':identifier,'attempt_token':identifier+'-token','body':{'model':MODEL,
        'messages':[{'role':'user','content':prompt}],'max_tokens':cap}}


def generated(count=16, completion=16, model=MODEL):
    return {'model':model,'choices':[{'message':{'role':'assistant','content':'result'},'finish_reason':'length'}],
        'usage':{'prompt_tokens':count,'completion_tokens':completion}}


def prepared(engine, *, control=None, **changes):
    worker=Worker(configuration(**changes), inference_transport=httpx.MockTransport(engine),
        control_transport=httpx.MockTransport(control or (lambda request:httpx.Response(200,json={'ok':True}))))
    worker._adaptive_validated=True
    worker.engines[0].ready=True
    worker._engine_metrics()
    return worker


def test_off_by_default_preserves_legacy_slot_bound_and_qwen_only_configuration():
    value=configuration(adaptive_kv_enabled=False,model='openai/gpt-oss-120b')
    assert value.slots==2 and configuration().slots==8
    for change in [{'model':'Qwen/another-model'}, {'inference_urls':('http://a','http://b')},
                   {'token_budget':127}, {'adaptive_kv_enabled':'true'},
                   {'request_timeout':float('inf')}, {'request_timeout':float('nan')}]:
        with pytest.raises(ValueError):configuration(**change)


def test_operator_environment_flag_is_strict_and_not_a_tenant_field(monkeypatch):
    for name,value in {'BATCHFORGE_WORKER_TOKEN':'x'*32,'INFERENCE_BASE_URLS':'http://engine',
        'MODEL_NAME':MODEL,'ENGINE_MAX_MODEL_LEN':'128','WORKER_MAX_INFLIGHT':'8',
        'WORKER_TOKEN_BUDGET':'256','WORKER_ADAPTIVE_KV_ENABLED':'true'}.items():monkeypatch.setenv(name,value)
    assert Settings.from_env().slots==8
    monkeypatch.setenv('WORKER_ADAPTIVE_KV_ENABLED','false')
    assert Settings.from_env().slots==2
    monkeypatch.setenv('WORKER_ADAPTIVE_KV_ENABLED','guess')
    with pytest.raises(ValueError):Settings.from_env()


@pytest.mark.parametrize('value',[{'count':True,'tokens':[1],'max_model_len':128},
    {'count':2,'tokens':[1],'max_model_len':128}, {'count':1,'tokens':[False],'max_model_len':128},
    {'count':1,'tokens':[1],'max_model_len':256}, {'count':1,'tokens':['1'],'max_model_len':128}])
def test_exact_tokenizer_shape_is_not_a_heuristic(value):
    with pytest.raises(TokenizerProtocolError):token_count(value,128)


def test_context_and_block_rounding_and_quiescence_fail_closed():
    assert reservation(16,17,128)==48
    assert reservation(127,1,128)==128
    with pytest.raises(ValueError):reservation(128,1,128)
    assert engine_quiescent('vllm:num_requests_running{model_name="x"} 0\nvllm:num_requests_waiting 0\n')
    for body in ['', 'vllm:num_requests_running 0', 'vllm:num_requests_running NaN\nvllm:num_requests_waiting 0',
                 'vllm:num_requests_running 0\nvllm:num_requests_waiting 1']:
        assert not engine_quiescent(body)


@pytest.mark.asyncio
async def test_weighted_concurrency_never_exceeds_budget_and_has_no_leak():
    observations=[]
    limiter=WeightedLimiter(64,lambda used,waiting,peak,paused:observations.append((used,waiting)))
    async def invoke(amount):
        async with limiter.reserve(amount):
            await asyncio.sleep(.001)
    await asyncio.gather(*(invoke([16,32,48][n%3]) for n in range(30)))
    assert limiter.used==limiter.waiting==0 and limiter.peak<=64
    assert all(0<=used<=64 for used,_ in observations) and any(waiting>0 for _,waiting in observations)


@pytest.mark.asyncio
async def test_fifo_prevents_small_requests_bypassing_an_older_large_waiter():
    limiter=WeightedLimiter(64);order=[]
    async def enter(amount,label):
        async with limiter.reserve(amount):order.append(label)
    async with limiter.reserve(48):
        large=asyncio.create_task(enter(32,'large'));await asyncio.sleep(0)
        small=asyncio.create_task(enter(16,'small'));await asyncio.sleep(0)
        assert not order and limiter.waiting==2
    await asyncio.gather(large,small)
    assert order==['large','small'] and limiter.used==0


@pytest.mark.asyncio
async def test_cancelled_waiter_and_grant_cancel_race_release_exactly_once():
    limiter=WeightedLimiter(32)
    release=asyncio.Event()
    async def holder():
        async with limiter.reserve(32):await release.wait()
    async def waiter():
        async with limiter.reserve(32):await asyncio.sleep(60)
    first=asyncio.create_task(holder());await asyncio.sleep(0)
    queued=asyncio.create_task(waiter());await asyncio.sleep(0)
    assert limiter.used==32 and limiter.waiting==1
    queued.cancel();await asyncio.gather(queued,return_exceptions=True)
    assert limiter.used==32 and limiter.waiting==0
    granted=asyncio.create_task(waiter());await asyncio.sleep(0)
    release.set();await asyncio.sleep(0)
    granted.cancel();await asyncio.gather(first,granted,return_exceptions=True)
    assert limiter.used==limiter.waiting==0


@pytest.mark.asyncio
async def test_pause_wakes_waiters_and_does_not_reset_on_release():
    limiter=WeightedLimiter(32)
    async def wait():
        async with limiter.reserve(32):pass
    async with limiter.reserve(32):
        pending=asyncio.create_task(wait());await asyncio.sleep(0)
        limiter.pause()
        with pytest.raises(AdmissionPaused):await pending
        assert limiter.used==32
    assert limiter.used==0 and limiter.paused
    with pytest.raises(AdmissionPaused):
        async with limiter.reserve(1):pass


@pytest.mark.asyncio
async def test_startup_requires_exact_version_tokenizer_and_idle_engine_before_registration():
    calls=[]
    async def engine(request):
        calls.append(request.url.path)
        value={'/v1/models':{'data':[{'id':MODEL}]},'/version':{'version':'0.10.2'},
            '/tokenize':{'count':8,'tokens':list(range(8)),'max_model_len':128}}
        if request.url.path=='/metrics':
            return httpx.Response(200,text='vllm:num_requests_running 0\nvllm:num_requests_waiting 0')
        return httpx.Response(200,json=value[request.url.path])
    registrations=[]
    async def control(request):
        registrations.append(json.loads(request.content))
        return httpx.Response(200,json={'worker_id':'worker','lease_seconds':90,'heartbeat_interval_seconds':10})
    worker=Worker(configuration(),control_transport=httpx.MockTransport(control),inference_transport=httpx.MockTransport(engine))
    try:
        with pytest.raises(ValueError):await worker.register()
        assert not registrations
        await worker.probe_engines();await worker.register()
        assert worker.metrics.engine_ready and worker._adaptive_validated
        assert registrations[0]['concurrency']==8
        assert registrations[0]['metadata']['kv_admission']['single_worker_single_engine']
        assert '/v1/chat/completions' not in calls
    finally:await worker.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('problem',['version','version_shape','busy','model'])
async def test_invalid_startup_cannot_register_optimistic_capacity(problem):
    async def engine(request):
        if request.url.path=='/v1/models':return httpx.Response(200,json={'data':[{'id':'wrong' if problem=='model' else MODEL}]})
        if request.url.path=='/version':return httpx.Response(200,json=[] if problem=='version_shape' else {'version':'0.11' if problem=='version' else '0.10.2'})
        if request.url.path=='/tokenize':return httpx.Response(200,json={'count':8,'tokens':list(range(8)),'max_model_len':128})
        return httpx.Response(200,text='vllm:num_requests_running 1\nvllm:num_requests_waiting 0')
    worker=Worker(configuration(),inference_transport=httpx.MockTransport(engine))
    try:
        await worker.probe_engines()
        assert not worker._adaptive_validated and not worker.metrics.engine_ready
        with pytest.raises(ValueError):await worker.register()
    finally:await worker.close()


@pytest.mark.asyncio
async def test_tokenizer_and_inference_share_messages_defaults_count_and_reservation():
    calls=[]
    async def engine(request):
        body=json.loads(request.content);calls.append((request.url.path,body))
        if request.url.path=='/tokenize':
            return httpx.Response(200,json={'count':17,'tokens':list(range(17)),'max_model_len':128})
        assert worker.kv_admission.used==48
        return httpx.Response(200,json=generated(17))
    worker=prepared(engine)
    try:
        value=await worker._invoke_available(task())
        assert value['usage']['prompt_tokens']==17 and worker.kv_admission.used==0
        assert calls[0][1]['messages']==calls[1][1]['messages']
        assert calls[0][1]['add_generation_prompt'] and not calls[0][1]['add_special_tokens']
        assert calls[1][1]['max_tokens']==16 and worker.kv_admission.peak==48
    finally:await worker.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('problem',['bad_count','http','usage_mismatch','model_mismatch','transport'])
async def test_protocol_and_uncertain_engine_failures_cannot_accept_or_resume(problem):
    calls=[]
    async def engine(request):
        calls.append(request.url.path)
        if request.url.path=='/tokenize':
            if problem=='http':return httpx.Response(503)
            return httpx.Response(200,json={'count':16,'tokens':[1] if problem=='bad_count' else list(range(16)), 'max_model_len':128})
        if problem=='transport':raise httpx.ReadError('ambiguous disconnect')
        return httpx.Response(200,json=generated(17 if problem=='usage_mismatch' else 16,
            model='another-model' if problem=='model_mismatch' else MODEL))
    worker=prepared(engine)
    try:
        with pytest.raises(InferenceError):await worker._invoke_available(task())
        assert worker.kv_admission.paused and worker.kv_admission.used==0
        assert not worker.metrics.engine_ready
        previous=len(calls);await worker.probe_engines()
        assert len(calls)==previous and not worker.metrics.engine_ready
        with pytest.raises(InferenceError,match='operator intervention'):await worker._invoke_available(task())
        assert len(calls)==previous
        if problem in {'bad_count','http'}:assert '/v1/chat/completions' not in calls
    finally:await worker.close()


@pytest.mark.asyncio
async def test_over_context_rejects_before_generation_without_budget_guess():
    async def engine(request):
        assert request.url.path=='/tokenize'
        return httpx.Response(200,json={'count':120,'tokens':list(range(120)),'max_model_len':128})
    worker=prepared(engine)
    try:
        with pytest.raises(InferenceError) as error:await worker._invoke_available(task(cap=16))
        assert error.value.code=='context_limit' and not error.value.retryable
        assert worker.kv_admission.used==0 and not worker.kv_admission.paused
    finally:await worker.close()


@pytest.mark.asyncio
async def test_task_model_mismatch_never_calls_tokenizer_or_engine():
    calls=[]
    worker=prepared(lambda request:calls.append(request))
    value=task();value['body']['model']='openai/gpt-oss-120b'
    try:
        with pytest.raises(InferenceError) as error:await worker._invoke_available(value)
        assert error.value.code=='unsupported_model' and not calls and worker.kv_admission.used==0
    finally:await worker.close()


@pytest.mark.asyncio
async def test_tenant_cannot_choose_operator_admission_fields():
    calls=[];worker=prepared(lambda request:calls.append(request))
    value=task();value['body']['adaptive_kv_enabled']=True
    try:
        with pytest.raises(InferenceError) as error:await worker._invoke_available(value)
        assert error.value.code=='unsupported_parameter' and not calls
    finally:await worker.close()


@pytest.mark.asyncio
async def test_claim_waiting_for_budget_keeps_heartbeats_cancel_does_not_poison_engine():
    entered,finish=asyncio.Event(),asyncio.Event();heartbeats=[]
    async def engine(request):
        if request.url.path=='/tokenize':return httpx.Response(200,json={'count':48,'tokens':list(range(48)),'max_model_len':64})
        entered.set();await finish.wait();return httpx.Response(200,json=generated(48))
    async def control(request):
        if request.url.path.endswith('/heartbeat'):heartbeats.append(request.url.path)
        return httpx.Response(200,json={'ok':True})
    worker=prepared(engine,control=control,max_model_len=64,token_budget=64,max_inflight=2)
    worker.heartbeat_seconds=.005
    first=asyncio.create_task(worker.execute(task('first')));await entered.wait()
    waiting=asyncio.create_task(worker.execute(task('waiting')))
    try:
        async with asyncio.timeout(2):
            while not worker.kv_admission.waiting or not any('/waiting/' in p for p in heartbeats):await asyncio.sleep(.005)
        waiting.cancel();await asyncio.gather(waiting,return_exceptions=True)
        assert not worker.kv_admission.paused and worker.kv_admission.used==64
        finish.set();await first
        assert worker.kv_admission.used==0 and worker.metrics.results.labels('completed')._value.get()==1
    finally:
        first.cancel();waiting.cancel();await asyncio.gather(first,waiting,return_exceptions=True);await worker.close()


@pytest.mark.asyncio
async def test_stale_waiting_lease_cancels_queued_reservation_without_stopping_active_work():
    entered,finish=asyncio.Event(),asyncio.Event()
    async def engine(request):
        if request.url.path=='/tokenize':return httpx.Response(200,json={'count':48,'tokens':list(range(48)),'max_model_len':64})
        entered.set();await finish.wait();return httpx.Response(200,json=generated(48))
    reports=[]
    async def control(request):
        if '/waiting/heartbeat' in request.url.path:return httpx.Response(409,json={'error':{'code':'stale_attempt'}})
        reports.append(request.url.path);return httpx.Response(200,json={'ok':True})
    worker=prepared(engine,control=control,max_model_len=64,token_budget=64,max_inflight=2)
    worker.heartbeat_seconds=.005
    first=asyncio.create_task(worker.execute(task('first')));await entered.wait()
    waiting=asyncio.create_task(worker.execute(task('waiting')))
    try:
        await asyncio.wait_for(waiting,2)
        assert worker.metrics.results.labels('fenced')._value.get()==1
        assert worker.kv_admission.used==64 and worker.kv_admission.waiting==0 and not worker.kv_admission.paused
        assert not any('/waiting/complete' in route or '/waiting/fail' in route for route in reports)
        finish.set();await first
        assert worker.kv_admission.used==0
    finally:
        first.cancel();waiting.cancel();await asyncio.gather(first,waiting,return_exceptions=True);await worker.close()


@pytest.mark.asyncio
async def test_cancelled_remote_inference_latches_pause_and_releases_only_local_accounting():
    entered=asyncio.Event()
    async def engine(request):
        if request.url.path=='/tokenize':return httpx.Response(200,json={'count':16,'tokens':list(range(16)),'max_model_len':128})
        entered.set();await asyncio.sleep(60)
    worker=prepared(engine)
    invocation=asyncio.create_task(worker._invoke_available(task()))
    try:
        await entered.wait();invocation.cancel();await asyncio.gather(invocation,return_exceptions=True)
        assert worker.kv_admission.paused and worker.kv_admission.used==0 and not worker.metrics.engine_ready
    finally:await worker.close()


@pytest.mark.asyncio
async def test_budget_wait_has_bounded_failure_without_uncertain_remote_invocation():
    async def engine(request):
        assert request.url.path=='/tokenize'
        return httpx.Response(200,json={'count':48,'tokens':list(range(48)),'max_model_len':64})
    worker=prepared(engine,max_model_len=64,token_budget=64,request_timeout=.02)
    try:
        async with worker.kv_admission.reserve(64):
            with pytest.raises(InferenceError) as error:await worker._invoke_available(task())
            assert error.value.code=='kv_admission_timeout' and not worker.kv_admission.paused
        assert worker.kv_admission.used==worker.kv_admission.waiting==0
    finally:await worker.close()
