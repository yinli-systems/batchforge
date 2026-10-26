"""Synthetic HTTP evaluation tests; no GPU, full-dev or platform quality claim."""
import asyncio,copy,json
import httpx,pytest
from scripts.engine_quality import Case,Cohort,Evaluator,MODEL,CONTEXT,plan,body,response_record,prepare


def cases(n=2):return Cohort(tuple(Case(str(i),'Mira left the notebook at north gate.','Where is the notebook?',('north gate',)) for i in range(n)),'synthetic-cpu-fixture')


def reply(**kw):
    v={'model':MODEL,'choices':[{'index':0,'message':{'role':'assistant','content':'{"answer":"north gate"}'},'finish_reason':'stop'}],
       'usage':{'prompt_tokens':100,'completion_tokens':8}}
    v.update(kw);return v


class Engine:
    def __init__(self,*,result=None,transport_error=False,token_count=100):
        self.result=result;self.transport_error=transport_error;self.count=token_count;self.posts=[];self.inflight=0;self.peak=0
    async def __call__(self,request):
        if request.url.path=='/version':return httpx.Response(200,json={'version':'0.10.2'})
        if request.url.path=='/v1/models':return httpx.Response(200,json={'data':[{'id':MODEL,'max_model_len':CONTEXT}]})
        if request.url.path=='/metrics':return httpx.Response(200,text='vllm:num_requests_running 0\nvllm:num_requests_waiting 0\n')
        if request.url.path=='/tokenize':
            return httpx.Response(200,json={'count':self.count,'tokens':[1]*self.count,'max_model_len':CONTEXT})
        assert request.url.path=='/v1/chat/completions'
        self.posts.append(json.loads(request.content));self.inflight+=1;self.peak=max(self.peak,self.inflight)
        try:
            await asyncio.sleep(.002)
            if self.transport_error:raise httpx.ReadError('Synthetic unknown outcome')
            return httpx.Response(200,json=self.result if self.result is not None else reply(usage={'prompt_tokens':self.count,'completion_tokens':8}))
        finally:self.inflight-=1


async def evaluate(engine,cohort=None,authority=lambda:True):
    journal=[];private=[]
    async with httpx.AsyncClient(base_url='http://127.0.0.1:8348',transport=httpx.MockTransport(engine),trust_env=False) as client:
        e=Evaluator(cohort or cases(),client,authority=authority,emit=journal.append,private_response=lambda i,b:private.append((i,b)),runtime_fingerprint='synthetic-not-gpu')
        r=await e.run()
    return r,e,journal,private


async def test_natural_output_quality_uses_frozen_parser_and_exact_usage_but_cannot_publish_fixture_pass():
    engine=Engine();r,e,journal,private=await evaluate(engine)
    assert r['delivery_passed'] and r['exact_match_percent']==r['f1_percent']==100
    assert r['test_fixture_detected'] and not r['passed'] and r['known_completion_tokens']==16
    assert len(private)==2 and all('ignore_eos' not in p and 'min_tokens' not in p and p['max_tokens']==128 for p in engine.posts)
    assert sum(j['event']=='inference.intent' for j in journal)==2
    assert e.limiter.used==0 and not e.limiter.paused


@pytest.mark.parametrize('text',['```json\n{"answer":"north gate"}\n```','{"answer":"north gate","answer":"gate"}',
    '{"answer":"south gate"}','{"answer":"north gate","extra":true}','The answer is north gate'])
async def test_invalid_json_or_invented_span_scores_zero_without_repair(text):
    result=reply();result['choices'][0]['message']['content']=text
    r,e,_,_=await evaluate(Engine(result=result))
    assert r['delivery_passed'] and r['invalid_answer_contracts']==2 and r['exact_match_percent']==0
    assert all(x['exact_match']==0 for x in e.records)


@pytest.mark.parametrize('text',['not JSON','{"answer":"invented"}','{"answer":"a","extra":true}'])
def test_invalid_contract_never_gets_empty_normalized_reference_credit(text):
    case=Case('fixture','The answer is a letter.','What is it?',('a',))
    value=reply();value['choices'][0]['message']['content']=text
    r=response_record(case,value,prompt_count=100)
    assert r['answer_contract_valid'] is False and r['exact_match']==r['f1']==0


def test_valid_article_reference_retains_original_squad_normalization_semantics():
    case=Case('fixture','It is a letter.','What is it?',('a',))
    value=reply();value['choices'][0]['message']['content']='{"answer":"a"}'
    r=response_record(case,value,prompt_count=100)
    assert r['answer_contract_valid'] is True and r['exact_match']==1 and r['f1']==0


@pytest.mark.parametrize('value',[reply(model='wrong'),reply(choices=[]),reply(choices=[None]),
    reply(usage={'prompt_tokens':99,'completion_tokens':8}),reply(usage={'prompt_tokens':100,'completion_tokens':129})])
async def test_response_identity_usage_or_envelope_drift_halts_admission_and_hides_partial_scores(value):
    engine=Engine(result=value);r,e,_,_=await evaluate(engine,cases(64))
    assert not r['delivery_passed'] and r['exact_match_percent'] is None and r['unknown_inference_outcomes']>0
    assert r['inference_intents']<=32 and len(engine.posts)<=32 and e.limiter.paused and e.limiter.used==0


async def test_transport_failure_is_not_retried_and_does_not_report_unknown_usage_as_zero():
    engine=Engine(transport_error=True);r,e,_,private=await evaluate(engine,cases(64))
    assert r['unknown_inference_outcomes']>0 and not r['all_inference_usage_known'] and not private
    assert len(engine.posts)<=32 and len({j['id'] for j in e.records})==len(e.records)


async def test_exact_token_reservation_reduces_actual_inflight_and_never_exceeds_physical_budget():
    engine=Engine(token_count=16000);r,e,_,_=await evaluate(engine,cases(40))
    assert r['delivery_passed'] and r['peak_inflight_requests']<=10 and r['reservation_peak_tokens']<=164256
    assert e.limiter.used==0


async def test_missing_authority_refuses_before_model_or_tokenizer_io():
    engine=Engine()
    with pytest.raises(RuntimeError):await evaluate(engine,authority=lambda:None)
    assert not engine.posts


async def test_revoked_authority_stops_waiters_before_further_inference():
    engine=Engine();calls=0
    def authority():
        nonlocal calls;calls+=1;return calls<6
    r,e,_,_=await evaluate(engine,cases(64),authority)
    assert not r['delivery_passed'] and r['inference_intents']<32 and e.limiter.paused


def test_plan_binds_reference_and_workload_separately_without_sending_gold():
    c=cases();p=plan(c,runtime_fingerprint='fixture')
    changed=Cohort(tuple(Case(x.id,x.context,x.question,('different gold',)) for x in c.cases),c.dataset_sha256)
    other=plan(changed,runtime_fingerprint='fixture')
    assert p['workload_sha256']==other['workload_sha256'] and p['references_sha256']!=other['references_sha256']
    assert 'answers' not in body(c.cases[0]) and p['inference_retries']==0 and not p['forced_output']
    with pytest.raises(ValueError):prepare(b'{"version":"1.1","data":[]}')


def test_dataset_hash_label_cannot_relabel_synthetic_cases_as_full_public_evaluation():
    from scripts.quality_eval import DATASET_SHA256
    fake=Cohort(cases(10570).cases,DATASET_SHA256)
    assert not fake.complete_public_dev and not fake.verified_public_dev
    assert plan(fake,runtime_fingerprint='fake')['test_fixture_detected']
    with pytest.raises(ValueError):Case('id','passage','question',['mutable gold'])


@pytest.mark.parametrize('kwargs',[
    {'base_url':'https://paid-provider.invalid'}, {'base_url':'http://127.0.0.1:8349'},
    {'base_url':'http://127.0.0.1:8348/proxy'}, {'trust_env':True}, {'follow_redirects':True}])
async def test_evaluator_refuses_other_endpoint_proxies_or_redirects_before_any_request(kwargs):
    async with httpx.AsyncClient(**{'base_url':'http://127.0.0.1:8348','trust_env':False,**kwargs}) as c:
        with pytest.raises(ValueError):Evaluator(cases(),c,authority=lambda:True,emit=lambda _:None,
            private_response=lambda *_:None,runtime_fingerprint='synthetic')


def test_standalone_module_cannot_silently_start_inference_or_report_success():
    import subprocess,sys
    from pathlib import Path
    p=subprocess.run([sys.executable,'-m','scripts.engine_quality'],cwd=Path(__file__).resolve().parents[1],
        stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=10)
    assert p.returncode!=0 and b'qualified exclusive maintenance controller' in p.stderr


async def test_absolute_http_bound_covers_a_response_that_keeps_streaming_chunks():
    class Drip(httpx.AsyncByteStream):
        async def __aiter__(self):
            for _ in range(100):await asyncio.sleep(.005);yield b' '
    async def handle(_):return httpx.Response(200,stream=Drip())
    async with httpx.AsyncClient(base_url='http://127.0.0.1:8348',trust_env=False,transport=httpx.MockTransport(handle)) as c:
        e=Evaluator(cases(),c,authority=lambda:True,emit=lambda _:None,private_response=lambda *_:None,runtime_fingerprint='fixture')
        with pytest.raises(TimeoutError):await e.response_bytes('GET','/metrics',timeout=.02)


async def test_absolute_inference_timeout_latches_unknown_outcome_without_retry(monkeypatch):
    engine=Engine()
    async with httpx.AsyncClient(base_url='http://127.0.0.1:8348',trust_env=False,transport=httpx.MockTransport(engine)) as c:
        e=Evaluator(cases(64),c,authority=lambda:True,emit=lambda _:None,private_response=lambda *_:None,runtime_fingerprint='fixture')
        original=e.json
        async def timed(method,path,**kw):
            if path=='/v1/chat/completions':raise TimeoutError('Synthetic post-dispatch timeout')
            return await original(method,path,**kw)
        monkeypatch.setattr(e,'json',timed);r=await e.run()
    assert r['unknown_inference_outcomes']>0 and not r['delivery_passed'] and e.limiter.paused
    assert not r['all_inference_usage_known'] and r['exact_match_percent'] is None
