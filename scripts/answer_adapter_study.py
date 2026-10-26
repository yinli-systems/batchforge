"""Frozen post-retrieval engine experiment, never Batch acceptance evidence.

Two protocols see the same top5 BM25 chunks/original question. ABBA repeats are
correlated; pooled descriptions do not imply independent statistical samples.
References are used only by the external grader, never request construction.
"""
from __future__ import annotations
import hashlib,time
from dataclasses import dataclass
from pathlib import Path
from agents.contracts import (Policy,canonical,digest,parse_decision,planner_messages,FinalDecision,AbstainDecision)
from agents.retrieval import Snapshot
from agents.structured import response_format
from agents.tools import SPECS
from agents import answer_adapter
from ops.retrieval_squad import official,selected,support
from scripts.engine_quality import Case,Cohort,Evaluator as BaseEvaluator,MODEL
from scripts.quality_eval import score
from scripts.benchmark import distribution

SEED='2026102606'
PROTOCOL='batchforge.post-retrieval-adapter-study.v1'
ARMS=('full-citation','source-ordinal','source-ordinal','full-citation')


@dataclass(frozen=True)
class Prepared:
    cohort:Cohort
    cases:tuple
    snapshot:Snapshot
    observations:dict
    excluded:tuple
    public_data_verified:bool=False


def prepare(raw,*,count=500):
    if count not in (100,500):raise ValueError('Frozen100 canary or500-case matched evaluation required')
    corpus,rows=official(raw);snapshot=Snapshot(corpus)
    excluded={c['id'] for name in ('retrieval-squad-v1','retrieval-span-v1') for c in selected(raw,name)[1]}
    # The two source-lineage development cases used the seed05 on the remaining rows.
    source_cases=sorted((c for c in rows if c['id'] not in excluded),key=lambda c:hashlib.sha256(('2026102605:'+c['id']).encode()).hexdigest())[:2]
    excluded.update(c['id'] for c in source_cases)
    # Canary IDs also become development observations; full study excludes them.
    ranked=sorted((c for c in rows if c['id'] not in excluded),key=lambda c:hashlib.sha256((SEED+':'+c['id']).encode()).hexdigest())
    chosen=ranked[:100] if count==100 else ranked[100:600]
    if count==500:excluded.update(c['id'] for c in ranked[:100])
    observations={}
    for c in chosen:
        value=snapshot.search(c['question'],5)
        observations[c['id']]={'tool':'retrieval.search','arguments_sha256':digest({'query':c['question'],'limit':5}),
            'result':value,'result_sha256':digest(value)}
    cases=tuple(Case(c['id'],'post-retrieval context selected independently below',c['question'],tuple(a['text'] for a in c['references'])) for c in chosen)
    cohort=Cohort(cases,hashlib.sha256(raw).hexdigest())
    return Prepared(cohort,tuple(chosen),snapshot,observations,tuple(sorted(excluded)),True)


def context(prepared,case):
    result=prepared.observations[case.id]['result'];prepared.snapshot.verify(result)
    return {'protocol':answer_adapter.PROTOCOL,'snapshot_id':prepared.snapshot.id,
        'sources':[{k:v for k,v in m.items() if k!='score'} for m in result['matches']]}


def policy(prepared):
    return Policy(allowed_tools=('retrieval.search',),memory_namespace='study',retrieval_snapshot=prepared.snapshot.id,
        require_citations=True,answer_format='retrieved_span',planner_protocol='schema-v1',tool_sequence=('retrieval.search',))


def body(prepared,case,arm):
    p=policy(prepared);c=context(prepared,case)
    if arm=='source-ordinal':messages=answer_adapter.messages(case.question,c);grammar=answer_adapter.response_format(c)
    elif arm=='full-citation':
        messages=planner_messages(case.question,p,SPECS,[prepared.observations[case.id]])
        grammar=response_format(p,observed_citations=[m['citation'] for m in c['sources']],remaining_tools=0,executed_tools=['retrieval.search'],retrieval_observed=True)
    else:raise ValueError('Frozen arm required')
    return {'model':MODEL,'messages':messages,'response_format':grammar,'temperature':0,'max_tokens':512}


def plan(prepared,*,runtime_fingerprint,deadline_seconds=1800):
    if type(deadline_seconds) is not int or not 60<=deadline_seconds<=3600:raise ValueError('Bounded study window required')
    h=hashlib.sha256()
    for phase,arm in enumerate(ARMS):
        for c in prepared.cohort.cases:h.update(canonical({'phase':phase,'id':c.id,'body':body(prepared,c,arm)})+b'\n')
    return {'protocol':PROTOCOL,'tasks':4*len(prepared.cases),'case_count':len(prepared.cases),'selection_seed':SEED,
        'dataset_sha256':prepared.cohort.dataset_sha256,'selected_ids_sha256':digest([c.id for c in prepared.cohort.cases]),
        'references_sha256':digest([[c.id,list(c.answers)] for c in prepared.cohort.cases]),
        'excluded_ids':list(prepared.excluded),'snapshot_id':prepared.snapshot.id,'snapshot':prepared.snapshot.manifest(),
        'workload_sha256':h.hexdigest(),'client_source_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        'adapter_source_sha256':hashlib.sha256(Path(answer_adapter.__file__).read_bytes()).hexdigest(),
        'model':MODEL,'runtime_fingerprint':runtime_fingerprint,'deadline_seconds':deadline_seconds,
        'arms':list(ARMS),'concurrency':32,'max_output_tokens':512,'inference_retries':0,
        'context_limit':18440,'token_budget':164256,'retrieval':'Original question, same frozen BM25 top5 in each arm, no hit filtering',
        'quality_gate':{'minimum_exact_match_percent':70,'minimum_f1_percent':80,'minimum_joint_percent':70},
        'scope':'Post-retrieval engine subsystem; no Batch/Agent delivery, acceptance, metering or SLO claim',
        'limitations':['Public dev, not hidden; previous full engine work saw this dataset',
            'ABBA repetitions are correlated, not independent samples or significance evidence',
            'Prompt packing and ordinal encoding change together; their separate causal effects are not identified',
            'Exact byte/source consistency does not prove semantic correctness; references grade externally']}


def response_record(prepared,case,arm,value,*,prompt_count):
    # Validate transport identity/usage separately from answer validity. A malformed
    # answer still consumes accepted engine tokens and scores zero.
    if not isinstance(value,dict) or value.get('model')!=MODEL:raise ValueError('Engine model differs')
    choices=value.get('choices');usage=value.get('usage')
    if (not isinstance(choices,list) or len(choices)!=1 or not isinstance(choices[0],dict) or choices[0].get('index')!=0
        or not isinstance(usage,dict) or type(usage.get('prompt_tokens')) is not int or usage['prompt_tokens']!=prompt_count
        or type(usage.get('completion_tokens')) is not int or not 0<=usage['completion_tokens']<=512):raise ValueError('Exact engine delivery/usage required')
    choice=choices[0];message=choice.get('message')
    if not isinstance(message,dict):raise ValueError('Assistant message required')
    text=message.get('content')
    if not isinstance(text,str) or len(text.encode())>32768 or choice.get('finish_reason') not in ('stop','length'):raise ValueError('Terminal engine output required')
    c=context(prepared,case);valid=False;answered=False;answer='';cited=[];abstained=False
    try:
        decision=answer_adapter.parse(text,c) if arm=='source-ordinal' else parse_decision(text)
        if choice['finish_reason']!='stop':raise ValueError('Truncated output is not valid')
        if isinstance(decision,AbstainDecision):valid=True;abstained=True
        elif isinstance(decision,FinalDecision):
            policy(prepared).validate_final(decision)
            matches={m['citation']:m for m in c['sources']}
            if not decision.citations or not set(decision.citations)<=matches.keys():raise ValueError('Unobserved source')
            if not any(decision.answer in matches[ci]['text'] for ci in decision.citations):raise ValueError('No verbatim answer span')
            answer=decision.answer;cited=decision.citations;valid=True;answered=True
        else:raise ValueError('Final extraction decision required')
    except (ValueError,TypeError,RecursionError):pass
    ref=next(v for v in prepared.cases if v['id']==case.id)
    supported={m['citation'] for m in c['sources'] if support(ref,m)}
    citation_valid=bool(cited) and set(cited)<=supported
    metrics=score(answer,case.answers) if answered else {'exact_match':0,'f1':0}
    return {'id':case.id,'status':'completed','answer_contract_valid':valid,'abstained':abstained,**metrics,
        'reference_supported_citation':bool(citation_valid),'joint_exact_and_supported':bool(metrics['exact_match']==1 and citation_valid),
        'prediction_sha256':hashlib.sha256(text.encode()).hexdigest(),'scored_answer_sha256':hashlib.sha256(answer.encode()).hexdigest(),
        'finish_reason':choice['finish_reason'],'prompt_tokens':prompt_count,'completion_tokens':usage['completion_tokens']}


class ArmEvaluator(BaseEvaluator):
    def __init__(self,prepared,arm,*args,**kw):
        self.prepared=prepared;self.arm=arm
        super().__init__(prepared.cohort,*args,**kw)
    def make_plan(self,cohort,*,runtime_fingerprint,deadline_seconds):
        workload=hashlib.sha256()
        for c in cohort.cases:workload.update(canonical({'id':c.id,'body':self.request(c)})+b'\n')
        return {'protocol':PROTOCOL+'.arm','scope':'One post-retrieval engine arm, not Batch or full public-dev qualification',
            'arm':self.arm,'tasks':len(cohort.cases),'workload_sha256':workload.hexdigest(),
            'dataset_sha256':cohort.dataset_sha256,'complete_public_dev':False,'test_fixture_detected':not self.prepared.public_data_verified,
            'runtime_fingerprint':runtime_fingerprint,'deadline_seconds':deadline_seconds,'answer_contract':self.arm,
            'concurrency':32,'max_output_tokens':512,'context_limit':18440,'token_budget':164256}
    def request(self,case):return body(self.prepared,case,self.arm)
    def score_response(self,case,value,*,prompt_count):return response_record(self.prepared,case,self.arm,value,prompt_count=prompt_count)


class Evaluator:
    def __init__(self,prepared,client,*,authority,emit,private_response,runtime_fingerprint,deadline_seconds=1800):
        self.prepared=prepared;self.client=client;self.authority=authority;self.emit=emit;self.private=private_response
        self.fingerprint=runtime_fingerprint;self.seconds=deadline_seconds
    async def run(self):
        start=time.monotonic();phase_reports=[];records=[]
        for phase,arm in enumerate(ARMS):
            remaining=int(self.seconds-(time.monotonic()-start))
            if remaining<60:break
            self.emit({'event':'phase.started','phase':phase,'arm':arm})
            def emit(v):self.emit({**v,'phase':phase,'arm':arm})
            def private(case,raw):self.private(f'{phase}:{case}',raw)
            evaluator=ArmEvaluator(self.prepared,arm,self.client,authority=self.authority,emit=emit,private_response=private,
                runtime_fingerprint=self.fingerprint,deadline_seconds=remaining)
            report=await evaluator.run();phase_reports.append({'phase':phase,'arm':arm,**report})
            records.extend({**r,'phase':phase,'arm':arm} for r in evaluator.records)
            if not report['evaluation_valid']:break
        elapsed=time.monotonic()-start
        valid=len(phase_reports)==4 and all(r['evaluation_valid'] for r in phase_reports)
        arms={}
        for arm in dict.fromkeys(ARMS):
            rows=[r for r in records if r['arm']==arm and r['status']=='completed'];wall=sum(r['elapsed_seconds'] for r in phase_reports if r['arm']==arm)
            n=2*len(self.prepared.cases);em=100*sum(r['exact_match'] for r in rows)/n;f1=100*sum(r['f1'] for r in rows)/n
            joint=100*sum(r['joint_exact_and_supported'] for r in rows)/n;cost=wall*16.10/3600;success=sum(r['joint_exact_and_supported'] for r in rows)
            arms[arm]={'predictions':len(rows),'expected':n,'wall_seconds':wall,'exact_match_percent':em if valid else None,
                'f1_percent':f1 if valid else None,'joint_percent':joint if valid else None,
                'quality_passed':valid and em>=70 and f1>=80 and joint>=70,
                'invalid_outputs':sum(not r['answer_contract_valid'] for r in rows),'abstentions':sum(r['abstained'] for r in rows),
                'prompt_tokens':sum(r['prompt_tokens'] for r in rows),'output_tokens':sum(r['completion_tokens'] for r in rows),
                'http_service_seconds':distribution([r['http_service_seconds'] for r in rows]),
                'offered_cohort_completion_seconds':distribution([r['completion_seconds'] for r in rows]),
                'joint_results_per_second':success/wall if valid else None,'whole_pod_wall_allocation_usd':cost,
                'allocation_per_1000_joint_results_usd':cost*1000/success if valid and success else None}
        return {**plan(self.prepared,runtime_fingerprint=self.fingerprint,deadline_seconds=self.seconds),
            'evaluation_valid':valid,'test_fixture_detected':not self.prepared.public_data_verified,
            'passed':valid and self.prepared.public_data_verified and arms['source-ordinal']['quality_passed'],'elapsed_seconds':elapsed,
            'arms_results':arms,'phases':phase_reports,
            'known_prompt_tokens':sum(r.get('prompt_tokens',0) for r in records),
            'known_completion_tokens':sum(r.get('completion_tokens',0) for r in records),
            'unknown_inference_outcomes':sum(r.get('status')=='outcome_unknown' for r in records),
            'inference_intents':sum(r['inference_intents'] for r in phase_reports),'cost_scope':'Whole-Pod measured phase wall allocations, not invoices/marginal cost; do not add nested controller window'}
