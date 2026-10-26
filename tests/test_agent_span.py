import copy,hashlib
import pytest
from agents.contracts import Policy,FinalDecision,AbstainDecision,OutputContractError,parse_decision,planner_messages,digest
from agents.retrieval import Registry,evidence,quote_evidence
from agents.structured import response_format
from agents.tools import SPECS
from test_agent_retrieval import registry


def setup(tmp):
    provider=Registry(registry(tmp));sid=provider.visible('tenant-a')[0]['id']
    policy=Policy(allowed_tools=('retrieval.search',),retrieval_snapshot=sid,memory_namespace='facts',
        answer_format='retrieved_span',require_citations=True,planner_protocol='schema-v1')
    result=provider.require('tenant-a','facts',sid).search('Mars mission')
    observation={'tool':'retrieval.search','arguments_sha256':digest({'query':'Mars mission'}),'result':result,'result_sha256':digest(result)}
    return provider,policy,observation


def test_abstention_is_typed_but_permission_remains_policy_authority():
    decision=parse_decision('{"type":"abstain","reason":"insufficient_evidence"}')
    assert isinstance(decision,AbstainDecision)
    with pytest.raises(PermissionError):Policy().authorize(decision)
    for raw in ('{"type":"abstain","reason":"anything"}',
        '{"type":"abstain","reason":"insufficient_evidence","citations":[]}',
        '{"type":"abstain","reason":"insufficient_evidence","reason":"insufficient_evidence"}'):
        with pytest.raises(ValueError):parse_decision(raw)


@pytest.mark.parametrize('kw',[{}, {'allowed_tools':('retrieval.search',),'retrieval_snapshot':'a'*64},
    {'allowed_tools':('retrieval.search',),'retrieval_snapshot':'a'*64,'require_citations':True,'max_tool_calls':0}])
def test_extractive_contract_requires_actual_retrieval_capability_and_budget(kw):
    with pytest.raises(ValueError):Policy(answer_format='retrieved_span',**kw)
    assert 'answer_format' not in Policy().document()


def test_quote_verification_returns_original_byte_location_without_gold_or_answer_repair(tmp_path):
    provider,policy,o=setup(tmp_path);hit=o['result']['matches'][0]
    final=FinalDecision(type='final',answer='2030',citations=[hit['citation']])
    proof=quote_evidence(final,[o],provider,'tenant-a',policy)
    quote=proof['quotes'][0];assert quote['answer_sha256']==hashlib.sha256(b'2030').hexdigest()
    assert quote['end_byte']-quote['start_byte']==4 and quote['document_sha256']==hit['document_sha256']
    assert proof['snapshot_id']==policy.retrieval_snapshot and 'semantic correctness is not established' in proof['scope']
    assert final.answer=='2030'


@pytest.mark.parametrize('answer',['The year was 2030.','2030 ',' 2030','X'*129,'x '*16+'x','Mars launch year 2020'])
def test_narration_whitespace_overflow_and_uncited_text_are_rejected_not_extracted(tmp_path,answer):
    provider,policy,o=setup(tmp_path);hit=o['result']['matches'][0]
    with pytest.raises(OutputContractError):quote_evidence(FinalDecision(type='final',answer=answer,citations=[hit['citation']]),[o],provider,'tenant-a',policy)


def test_source_and_argument_commitments_cannot_be_rebound_to_a_fake_quote(tmp_path):
    provider,policy,o=setup(tmp_path);hit=o['result']['matches'][0]
    bad=copy.deepcopy(o);bad['result']['matches'][0]['text']+=' Fabricated 2099.';bad['result_sha256']=digest(bad['result'])
    with pytest.raises(ValueError):evidence([bad],provider,'tenant-a',policy)
    bad=copy.deepcopy(o);bad['arguments_sha256']='f'*64
    with pytest.raises(ValueError):evidence([bad],provider,'tenant-a',policy)
    with pytest.raises(PermissionError):quote_evidence(FinalDecision(type='final',answer='2030',citations=[hit['citation']]),[o],provider,'tenant-b',policy)
    with pytest.raises(OutputContractError):quote_evidence(FinalDecision(type='final',answer='2030',citations=['not-a-source']),[o],provider,'tenant-a',policy)


def test_repeated_source_from_another_query_is_not_ambiguous_ranking_evidence(tmp_path):
    provider,policy,o=setup(tmp_path);value=provider.require('tenant-a','facts',policy.retrieval_snapshot).search('Mars')
    second={'tool':'retrieval.search','arguments_sha256':digest({'query':'Mars'}),'result':value,'result_sha256':digest(value)}
    matches,queries=evidence([o,second],provider,'tenant-a',policy)
    assert len(queries)==2 and o['result']['matches'][0]['citation'] in matches


def test_engine_grammar_never_allows_answer_or_abstain_before_actual_retrieval(tmp_path):
    _,policy,o=setup(tmp_path)
    def variants(observed,cites=(),tools=1):return response_format(policy,observed_citations=cites,remaining_tools=tools,
        retrieval_observed=observed)['json_schema']['schema']['anyOf']
    assert all(v['properties']['type']['const']=='tool' for v in variants(False,['forged']))
    empty=variants(True,[],tools=0);assert len(empty)==1 and empty[0]['properties']['type']['const']=='abstain'
    after=variants(True,[o['result']['matches'][0]['citation']],tools=0)
    assert {v['properties']['type']['const'] for v in after}=={'final','abstain'}
    final=next(v for v in after if v['properties']['type']['const']=='final')
    assert final['properties']['answer']['maxLength']==128 and final['properties']['citations']['minItems']==1


def test_prompt_contract_synthetic_example_has_no_gold_or_execution_permission(tmp_path):
    _,policy,_=setup(tmp_path)
    message=planner_messages('Find a date',policy,SPECS,output_feedback='retrieved_span')[0]['content']
    assert 'Synthetic style example only' in message and 'insufficient_evidence' in message and '128 UTF8 bytes' in message
    assert 'gold' not in message.lower() and 'shell' not in message


def test_repeated_citation_is_runtime_refused_even_without_a_decoder_uniqueness_hint(tmp_path):
    provider,policy,o=setup(tmp_path);cite=o['result']['matches'][0]['citation']
    with pytest.raises(OutputContractError):quote_evidence(FinalDecision(type='final',answer='2030',citations=[cite,cite]),[o],provider,'tenant-a',policy)
