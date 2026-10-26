"""Exact-token, engine-only SQuAD evaluation inside a qualified exclusive window.

No Batch admission/ledger claim, inference retry, engine restart or cloud action.
The maintenance controller must supply a live authority checkpoint. This module
does not itself grant an exclusive window or change serving configuration.
"""
from __future__ import annotations
import asyncio,hashlib,json,time
from collections import deque,Counter
from dataclasses import dataclass
from datetime import datetime,timezone
from pathlib import Path
import httpx
from agents.contracts import canonical,protocol_json
from scripts import answer_contract,quality_eval,contract_scoring
from scripts.benchmark import distribution
from worker.kv_admission import WeightedLimiter,AdmissionPaused,tokenizer_body,token_count,reservation,engine_quiescent

MODEL=quality_eval.MODEL
ENDPOINT='http://127.0.0.1:8348'
CONTEXT=18440
BUDGET=164256
CASESET_SHA256='3df3ccf00f3d7c9610cacdf4d53980320777b9516a748c60e2cacbd82bb8ca66'


class AuthorityLost(RuntimeError):pass


@dataclass(frozen=True)
class Case:
    id:str
    context:str
    question:str
    answers:tuple[str,...]

    def __post_init__(self):
        if (any(type(v) is not str or '\x00' in v or not 1<=len(v.encode())<=maximum
                for v,maximum in ((self.id,128),(self.context,131072),(self.question,32768)))
            or type(self.answers) is not tuple or not 1<=len(self.answers)<=64
            or any(type(v) is not str or '\x00' in v or not 1<=len(v.encode())<=32768 for v in self.answers)):
            raise ValueError('Immutable bounded public case/reference strings required')


@dataclass(frozen=True)
class Cohort:
    cases:tuple[Case,...]
    dataset_sha256:str
    source_cases:tuple[Case,...]=()

    def __post_init__(self):
        if (type(self.cases) is not tuple or not 1<=len(self.cases)<=10570
            or type(self.source_cases) is not tuple or len(self.source_cases)>10570
            or any(not isinstance(c,Case) for c in (*self.cases,*self.source_cases))
            or type(self.dataset_sha256) is not str):raise ValueError('Immutable bounded cohort required')

    @property
    def cases_sha256(self):
        return hashlib.sha256(canonical([{'id':c.id,'context':c.context,'question':c.question,'answers':list(c.answers)} for c in self.cases])).hexdigest()

    @property
    def verified_public_dev(self):
        if self.dataset_sha256!=quality_eval.DATASET_SHA256:return False
        if len(self.cases)==10570 and self.cases_sha256==CASESET_SHA256:return True
        if len(self.source_cases)!=10570:return False
        parent=Cohort(self.source_cases,self.dataset_sha256)
        if not parent.complete_public_dev:return False
        source={c.id:c for c in self.source_cases}
        return all(source.get(c.id)==c for c in self.cases)

    @property
    def complete_public_dev(self):return len(self.cases)==10570 and self.dataset_sha256==quality_eval.DATASET_SHA256 and self.cases_sha256==CASESET_SHA256


def prepare(raw,*,count=10570,seed=20261026):
    sha=hashlib.sha256(raw).hexdigest()
    if sha!=quality_eval.DATASET_SHA256:raise ValueError('Pinned public SQuAD bytes required')
    data=protocol_json(raw)
    if data.get('version')!='1.1':raise ValueError('SQuAD1.1 required')
    rows=quality_eval.examples(data,10570,20261026)
    if len(rows)!=10570:raise ValueError('Complete10570 cohort required')
    complete=tuple(Case(r['id'],r['context'],r['question'],tuple(r['answers'])) for r in rows)
    result=Cohort(complete,sha)
    if not result.complete_public_dev:raise ValueError('Pinned full case/reference manifest differs')
    if count==10570:return result
    if count!=100:raise ValueError('Only full evaluation or bounded100-case restoration canary supported')
    selected=quality_eval.examples(data,count,seed)
    return Cohort(tuple(Case(r['id'],r['context'],r['question'],tuple(r['answers'])) for r in selected),sha,complete)


def body(case):
    # Exactly the frozen natural-EOS answer contract, no references, forced
    # length, schema repair, post-hoc prompt changes or model-written judge.
    return {'model':MODEL,'temperature':0,'max_tokens':128,
            'messages':answer_contract.messages(case.context,case.question,'json-span-v1')}


def plan(cohort,*,runtime_fingerprint,deadline_seconds=3600):
    if type(deadline_seconds) is not int or not 60<=deadline_seconds<=3600:raise ValueError('Finite evaluation bound required')
    cases=cohort.cases
    if not cases or len(cases)>10570 or len({c.id for c in cases})!=len(cases) or any(not c.answers for c in cases):
        raise ValueError('Unique bounded cases and references required')
    workload=hashlib.sha256()
    for c in cases:workload.update(canonical({'id':c.id,'body':body(c)})+b'\n')
    return {'protocol':'batchforge.engine-squad.v1','scope':'Engine-only HTTP inference; no Batch tasks, acceptance ledger or platform recovery',
        'tasks':len(cases),'dataset_sha256':cohort.dataset_sha256,'complete_public_dev':cohort.complete_public_dev,
        'test_fixture_detected':not cohort.verified_public_dev,'model':MODEL,'runtime_fingerprint':runtime_fingerprint,
        'case_manifest_sha256':cohort.cases_sha256,
        'workload_sha256':workload.hexdigest(),'selected_ids_sha256':hashlib.sha256(canonical(sorted(c.id for c in cases))).hexdigest(),
        'references_sha256':hashlib.sha256(canonical([[c.id,list(c.answers)] for c in cases])).hexdigest(),
        'answer_contract':'json-span-v1','answer_contract_sha256':hashlib.sha256(Path(answer_contract.__file__).read_bytes()).hexdigest(),
        'score_policy':contract_scoring.POLICY,'score_policy_source_sha256':hashlib.sha256(Path(contract_scoring.__file__).read_bytes()).hexdigest(),
        'scorer_sha256':hashlib.sha256(Path(quality_eval.__file__).read_bytes()).hexdigest(),
        'client_source_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        'max_output_tokens':128,'temperature':0,'forced_output':False,'concurrency':32,
        'context_limit':CONTEXT,'token_budget':BUDGET,'request_timeout_seconds':120,'deadline_seconds':deadline_seconds,
        'quality_gate':{'minimum_exact_match_percent':70,'minimum_f1_percent':80},
        'inference_retries':0,'limitations':['Public dev set including prior calibration IDs; not hidden evaluation or a contamination audit',
            'Engine-only throughput and latency are not Batch API goodput/P99',
            'Container journal fsync does not survive Pod loss; export evidence',
            'Uncertain requests pause admission and are never replayed']}


def response_record(case,value,*,prompt_count):
    if not isinstance(value,dict) or value.get('model')!=MODEL:raise ValueError('Engine model identity differs')
    choices=value.get('choices');usage=value.get('usage')
    if not isinstance(choices,list) or len(choices)!=1 or not isinstance(choices[0],dict) or choices[0].get('index')!=0:raise ValueError('Exactly one choice required')
    if (not isinstance(usage,dict) or type(usage.get('prompt_tokens')) is not int or usage['prompt_tokens']!=prompt_count
        or type(usage.get('completion_tokens')) is not int or not 0<=usage['completion_tokens']<=128):raise ValueError('Exact tokenizer and response usage differ')
    choice=choices[0];message=choice.get('message')
    if not isinstance(message,dict):raise ValueError('Assistant message required')
    text=message.get('content')
    if text is None:text=''  # same empty-answer treatment as frozen evaluator
    if not isinstance(text,str) or len(text.encode())>32768 or choice.get('finish_reason') not in ('stop','length'):
        raise ValueError('Bounded terminal text required')
    valid=True
    try:prediction=answer_contract.parse_answer(text,case.context,'json-span-v1')
    except (ValueError,TypeError):valid=False;prediction=''
    return {'id':case.id,'status':'completed','answer_contract_valid':valid,
        **contract_scoring.enforce(valid,quality_eval.score(prediction,case.answers)),'prediction_sha256':hashlib.sha256(text.encode()).hexdigest(),
        'scored_answer_sha256':hashlib.sha256(prediction.encode()).hexdigest(),'finish_reason':choice['finish_reason'],
        'prompt_tokens':prompt_count,'completion_tokens':usage['completion_tokens']}


class Evaluator:
    request_timeout_seconds=120
    def __init__(self,cohort,client,*,authority,emit,private_response,runtime_fingerprint,deadline_seconds=3600):
        if not all(callable(x) for x in (authority,emit,private_response)):raise ValueError('Live authority, durable journal and private raw-response sink required')
        if (not isinstance(client,httpx.AsyncClient) or str(client.base_url).rstrip('/')!=ENDPOINT
            or client.trust_env is not False or client.follow_redirects is not False):
            raise ValueError('Exact private8348 endpoint without ambient proxies or redirects required')
        self.cohort=cohort;self.client=client;self.authority=authority;self.emit=emit;self.private_response=private_response
        self.plan=self.make_plan(cohort,runtime_fingerprint=runtime_fingerprint,deadline_seconds=deadline_seconds)
        self.limiter=WeightedLimiter(BUDGET);self.halted=False;self.records=[];self.inference_intents=0
        self.active=0;self.peak_active=0

    def make_plan(self,cohort,**kwargs):return plan(cohort,**kwargs)

    def checkpoint(self):
        if self.authority() is not True:raise AuthorityLost('Exclusive authority not confirmed')

    async def response_bytes(self,method,path,**kwargs):
        self.checkpoint()
        seconds=kwargs.get('timeout',120)
        if type(seconds) not in (int,float) or not 0<seconds<=self.request_timeout_seconds:raise ValueError('Bounded absolute HTTP lifetime required')
        # HTTPX's per-operation inactivity timer is not a total request bound:
        # a peer can keep sending small chunks. The action has its own deadline.
        async with asyncio.timeout(seconds),self.client.stream(method,path,follow_redirects=False,**kwargs) as response:
            response.raise_for_status();raw=bytearray()
            async for part in response.aiter_bytes():
                raw.extend(part)
                if len(raw)>2*1024**2:raise ValueError('Engine response exceeds bound')
        return bytes(raw)

    async def json(self,method,path,**kwargs):
        raw=await self.response_bytes(method,path,**kwargs)
        return protocol_json(raw),raw

    async def preflight(self):
        version,_=await self.json('GET','/version',timeout=5)
        models,_=await self.json('GET','/v1/models',timeout=5)
        if not isinstance(version,dict) or version.get('version')!='0.10.2' or not isinstance(models,dict) or not isinstance(models.get('data'),list):raise ValueError('Qualified vLLM protocol required')
        if any(not isinstance(m,dict) for m in models['data']):raise ValueError('Model catalogue differs')
        ids=[m.get('id') for m in models['data']]
        if ids!=[MODEL] or models['data'][0].get('max_model_len')!=CONTEXT:raise ValueError('Qualified480B context/model required')
        raw=await self.response_bytes('GET','/metrics',timeout=5)
        if not engine_quiescent(raw.decode()):raise ValueError('Exclusive engine must start quiescent')

    def request(self,case):return body(case)

    def score_response(self,case,value,*,prompt_count):return response_record(case,value,prompt_count=prompt_count)

    async def invoke(self,case,offered):
        request=self.request(case);began=time.monotonic();dispatched=False;record={'id':case.id}
        try:
            self.checkpoint()
            tokenized,_=await self.json('POST','/tokenize',json=tokenizer_body(request,MODEL),timeout=10)
            count=token_count(tokenized,CONTEXT);amount=reservation(count,request['max_tokens'],CONTEXT)
            record.update(tokenize_seconds=time.monotonic()-began,prompt_count=count,reservation_tokens=amount)
            async with self.limiter.reserve(amount):
                if self.halted:raise AdmissionPaused()
                self.checkpoint();start=time.monotonic()
                # emit is synchronous, durable and precedes the only inference.
                self.emit({'event':'inference.intent','id':case.id,'request_sha256':hashlib.sha256(canonical(request)).hexdigest(),
                    'prompt_tokens':count,'reservation_tokens':amount,'at':datetime.now(timezone.utc).isoformat()})
                self.inference_intents+=1;dispatched=True
                self.active+=1;self.peak_active=max(self.peak_active,self.active)
                try:value,raw=await self.json('POST','/v1/chat/completions',json=request,timeout=self.request_timeout_seconds)
                finally:self.active-=1
                elapsed=time.monotonic()-start
                self.private_response(case.id,raw)
                record.update(self.score_response(case,value,prompt_count=count),response_bytes_sha256=hashlib.sha256(raw).hexdigest(),
                    admission_wait_seconds=start-began-record['tokenize_seconds'],http_service_seconds=elapsed)
        except asyncio.CancelledError:
            self.halted=True;self.limiter.pause()
            record.update(status='outcome_unknown' if dispatched else 'not_dispatched',error_type='CancelledError')
            raise
        except (httpx.HTTPError,ValueError,TypeError,KeyError,AdmissionPaused,AuthorityLost,OSError,TimeoutError) as exc:
            self.halted=True;self.limiter.pause()
            record.update(status='outcome_unknown' if dispatched else 'not_dispatched',error_type=type(exc).__name__)
        finally:
            record['completion_seconds']=time.monotonic()-offered
            self.records.append(record)
            self.emit({'event':'inference.terminal','record':record,'at':datetime.now(timezone.utc).isoformat()})

    async def run(self):
        await self.preflight();began=time.monotonic();started=datetime.now(timezone.utc).isoformat()
        pending=deque(self.cohort.cases)
        async def consumer():
            while pending and not self.halted:
                await self.invoke(pending.popleft(),began)
        tasks=[asyncio.create_task(consumer()) for _ in range(min(32,len(pending)))]
        deadline_met=True
        try:
            done,left=await asyncio.wait(tasks,timeout=self.plan['deadline_seconds'])
            if left:deadline_met=False
            for t in done:t.result()
        finally:
            self.halted=True
            for t in tasks:
                if not t.done():t.cancel()
            await asyncio.gather(*tasks,return_exceptions=True)
        elapsed=time.monotonic()-began
        complete=[r for r in self.records if r.get('status')=='completed']
        ids=[r['id'] for r in self.records]
        if len(set(ids))!=len(ids):raise RuntimeError('Duplicate terminal identities')
        delivered=len(complete)==len(self.cohort.cases)
        em=100*sum(r['exact_match'] for r in complete)/len(self.cohort.cases)
        f1=100*sum(r['f1'] for r in complete)/len(self.cohort.cases)
        quality=delivered and deadline_met and em>=70 and f1>=80
        return {**self.plan,'passed':quality and self.plan['complete_public_dev'] and not self.plan['test_fixture_detected'],
            'delivery_passed':delivered,'deadline_met':deadline_met,'evaluation_valid':delivered and deadline_met,
            'measurement_started_at':started,'measurement_ended_at':datetime.now(timezone.utc).isoformat(),
            'elapsed_seconds':elapsed,'inference_intents':self.inference_intents,'predictions':len(complete),
            'unknown_inference_outcomes':sum(r.get('status')=='outcome_unknown' for r in self.records),
            'missing_predictions':len(self.cohort.cases)-len(complete),'not_dispatched':len(pending)+sum(r.get('status')=='not_dispatched' for r in self.records),
            'exact_match_percent':em if delivered else None,'f1_percent':f1 if delivered else None,
            'invalid_answer_contracts':sum(not r['answer_contract_valid'] for r in complete),
            'finish_reasons':dict(Counter(r['finish_reason'] for r in complete)),
            'known_prompt_tokens':sum(r['prompt_tokens'] for r in complete),'known_completion_tokens':sum(r['completion_tokens'] for r in complete),
            'all_inference_usage_known':len(complete)==self.inference_intents,
            'reservation_peak_tokens':self.limiter.peak,'admission_paused':self.limiter.paused,
            'peak_inflight_requests':self.peak_active,
            'http_service_seconds':distribution([r['http_service_seconds'] for r in complete]),
            'offered_cohort_completion_seconds':distribution([r['completion_seconds'] for r in complete]),
            'exact_results_per_second':sum(r['exact_match'] for r in complete)/elapsed if delivered else None,
            'cost':{'operator_whole_pod_usd_per_hour':16.10,'window_allocation_usd':16.10*elapsed/3600,
                'scope':'Engine measurement wall allocation; excludes maintenance/restoration; not invoice or marginal cost'}}


if __name__=='__main__':
    raise SystemExit('Use the qualified exclusive maintenance controller; this module grants no execution window.')
