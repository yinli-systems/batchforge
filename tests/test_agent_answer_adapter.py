import copy,json
import pytest
from dataclasses import replace
from agents import answer_adapter as a
from agents.app import RunRequest
from agents.contracts import Policy,FinalDecision,AbstainDecision,canonical,digest
from agents.workflows import Workflow
from test_agent_span import setup
from test_agent_workflows import graph


def prepared(tmp):
    registry,p,o=setup(tmp);p=replace(p,answer_adapter=a.NAME)
    return registry,p,o,a.context(p,[o],registry,'tenant-a')


def test_model_ordinal_maps_only_to_pinned_source_without_rewriting_answer(tmp_path):
    reg,p,o,c=prepared(tmp_path)
    decision=a.parse('{"answer":"2030","source":0}',c)
    assert decision==FinalDecision(type='final',answer='2030',citations=[o['result']['matches'][0]['citation']])
    assert isinstance(a.parse('{"answer":null,"source":null}',c),AbstainDecision)
    prompt=canonical(a.messages('When did Mars launch?',c)).decode()
    assert '2030' in prompt and 'Mars' in prompt and 'Tool catalogue' not in prompt
    assert 'retrieval:' not in prompt and p.retrieval_snapshot not in prompt
    assert a.response_format(c)['json_schema']['schema']['anyOf'][0]['properties']['source']['enum']==list(range(len(c['sources'])))


@pytest.mark.parametrize('raw',[
 '{"answer":"2030","source":true}', '{"answer":"2030","source":-1}',
 '{"answer":"2030","source":999}', '{"answer":"2030","source":0.0}',
 '{"answer":"2030","source":"0"}', '{"answer":"2030","source":null}',
 '{"answer":null,"source":0}', '{"answer":" 2030","source":0}',
 '{"answer":"Fabricated","source":0}', '{"answer":"2030","source":0,"type":"final"}',
 '{"answer":"2030","source":0,"source":1}', '[]'])
def test_untrusted_invalid_or_fabricated_selections_are_refused(tmp_path,raw):
    _,_,_,c=prepared(tmp_path)
    with pytest.raises(ValueError):a.parse(raw,c)


def test_actual_query_and_typed_sequence_are_prerequisites_not_model_declarations(tmp_path):
    reg,p,o,c=prepared(tmp_path)
    assert a.context(p,[],reg,'tenant-a') is None
    ordered=replace(p,tool_sequence=('retrieval.search','retrieval.search'))
    assert a.context(ordered,[o],reg,'tenant-a') is None
    empty=copy.deepcopy(o);value=reg.require('tenant-a','facts',p.retrieval_snapshot).search('zzzzunknown')
    empty.update(result=value,result_sha256=digest(value),arguments_sha256=digest({'query':'zzzzunknown'}))
    empty=a.context(p,[empty],reg,'tenant-a');assert empty['sources']==[]
    assert len(a.response_format(empty)['json_schema']['schema']['anyOf'])==1


def test_mapping_requires_replayed_source_authority_not_cached_text(tmp_path):
    reg,p,o,c=prepared(tmp_path)
    bad=copy.deepcopy(o);bad['result']['matches'][0]['text']='fabricated';bad['result_sha256']=digest(bad['result'])
    with pytest.raises(ValueError):a.context(p,[bad],reg,'tenant-a')
    with pytest.raises(PermissionError):a.context(p,[o],reg,'tenant-b')


def test_new_policy_is_explicit_and_old_canonical_request_documents_unchanged(tmp_path):
    assert 'answer_adapter' not in Policy().document()
    for kw in ({},{'answer_format':'retrieved_span','allowed_tools':('retrieval.search',),'require_citations':True,'retrieval_snapshot':'a'*64}):
        with pytest.raises(ValueError):Policy(answer_adapter=a.NAME,**kw)
    reg,p,_,_=prepared(tmp_path);assert p.document()['answer_adapter']==a.NAME
    request=RunRequest.model_validate_json(canonical({'goal':'When?','model':'test',**p.document()}))
    assert request.policy()==p
    old=Workflow.model_validate(graph());assert all('answer_adapter' not in n for n in old.document()['nodes'])
