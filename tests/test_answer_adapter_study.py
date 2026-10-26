"""Synthetic CPU transport checks, not public/GPU experiment results."""
import copy,json
import httpx,pytest
from scripts import answer_adapter_study as s
from scripts.engine_quality import Case,Cohort
from agents.contracts import digest
from test_agent_retrieval import snapshot
from test_engine_quality import Engine,reply


def prepared():
    snap=snapshot();case=Case('synthetic','not passed to model','When did Mars launch?',('2030',))
    value=snap.search(case.question,5)
    original={'id':case.id,'question':case.question,'document_id':'mars','references':[{'text':'2030','start_byte':33,'end_byte':37}]}
    # Derive the fixture byte range independently from the published text.
    text=next(m for m in value['matches'] if m['document_id']=='mars')['text'];start=text.encode().index(b'2030')
    original['references'][0].update(start_byte=start,end_byte=start+4)
    observation={'tool':'retrieval.search','arguments_sha256':digest({'query':case.question,'limit':5}),'result':value,'result_sha256':digest(value)}
    return s.Prepared(Cohort((case,),'synthetic-cpu-only'),(original,),snap,{case.id:observation},())


def test_matched_bodies_share_actual_ranked_evidence_without_reference_injection():
    p=prepared();c=p.cohort.cases[0]
    old=s.body(p,c,'full-citation');new=s.body(p,c,'source-ordinal')
    assert old['max_tokens']==new['max_tokens']==512
    assert 'references' not in str(old) and 'references' not in str(new)
    assert old['messages'][1]['content']==c.question and c.question in new['messages'][1]['content']
    assert 'retrieval:' in str(old) and 'retrieval:' not in str(new)
    assert len(s.plan(p,runtime_fingerprint='fixture')['arms'])==4


@pytest.mark.parametrize('arm',['full-citation','source-ordinal'])
def test_invalid_format_empty_fallback_and_truncation_cannot_get_article_reference_credit(arm):
    p=prepared();c=p.cohort.cases[0]
    for text in ('not JSON','{"answer":"a","source":true}'):
        v=reply();v['choices'][0]['message']['content']=text
        r=s.response_record(p,c,arm,v,prompt_count=100)
        assert not r['answer_contract_valid'] and r['exact_match']==r['f1']==0
    v=reply();v['choices'][0]['finish_reason']='length'
    r=s.response_record(p,c,arm,v,prompt_count=100);assert not r['answer_contract_valid']


async def test_balanced_four_phase_protocol_preserves_actual_usage_and_source_quality():
    p=prepared();cite=p.observations[p.cohort.cases[0].id]['result']['matches'][0]['citation']
    class AdapterEngine(Engine):
        async def __call__(self,request):
            if request.url.path=='/v1/chat/completions':
                body=json.loads(request.content);ordinal=body['response_format']['json_schema']['name']=='batchforge_source_span_v1'
                value=reply();value['choices'][0]['message']['content']=json.dumps({'answer':'2030','source':0} if ordinal else {'type':'final','answer':'2030','citations':[cite]})
                self.result=value
            return await super().__call__(request)
    engine=AdapterEngine();journal=[];private=[]
    async with httpx.AsyncClient(base_url='http://127.0.0.1:8348',transport=httpx.MockTransport(engine),trust_env=False) as client:
        e=s.Evaluator(p,client,authority=lambda:True,emit=journal.append,private_response=lambda i,b:private.append(i),runtime_fingerprint='synthetic-cpu-only')
        r=await e.run()
    assert r['evaluation_valid'] and len(private)==4 and len(set(private))==4
    assert all(v['joint_percent']==100 and v['output_tokens']==16 for v in r['arms_results'].values())
    assert [x['arm'] for x in r['phases']]==list(s.ARMS)
    assert all(x['answer_contract'] in s.ARMS for x in r['phases'])


@pytest.mark.parametrize('arm',['full-citation','source-ordinal'])
def test_typed_abstention_is_valid_format_but_zero_answer_score(arm):
    p=prepared();case=p.cohort.cases[0];v=reply()
    v['choices'][0]['message']['content']=json.dumps({'answer':None,'source':None} if arm=='source-ordinal' else {'type':'abstain','reason':'insufficient_evidence'})
    r=s.response_record(p,case,arm,v,prompt_count=100)
    assert r['answer_contract_valid'] and r['abstained'] and r['exact_match']==r['f1']==0
    assert not r['joint_exact_and_supported']


@pytest.mark.parametrize('message',[None,[],1,'not-an-envelope'])
def test_malformed_transport_envelope_is_not_mislabelled_a_zero_cost_model_answer(message):
    p=prepared();v=reply();v['choices'][0]['message']=message
    with pytest.raises(ValueError):s.response_record(p,p.cohort.cases[0],'source-ordinal',v,prompt_count=100)
