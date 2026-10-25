import re
import pytest
from agents.contracts import Policy
from agents.structured import response_format


def variants(policy,*,cites=(),tools=3):
    return response_format(policy,observed_citations=cites,remaining_tools=tools)['json_schema']['schema']['anyOf']


def test_old_protocol_stays_byte_compatible_and_does_not_silently_enable_grammar():
    assert response_format(Policy(),observed_citations=[],remaining_tools=0)=={'type':'json_object'}
    assert 'planner_protocol' not in Policy().document()
    assert Policy(planner_protocol='schema-v1').document()['planner_protocol']=='schema-v1'


def test_engine_tool_language_is_a_subset_of_server_grants_without_tenant_authority():
    p=Policy(planner_protocol='schema-v1',allowed_tools=('memory.get',))
    tools=[v for v in variants(p) if 'name' in v['properties']]
    assert len(tools)==1 and tools[0]['properties']['name']['const']=='memory.get'
    assert tools[0]['properties']['type']['const']=='tool' and tools[0]['additionalProperties'] is False
    args=tools[0]['properties']['arguments']
    assert args['additionalProperties'] is False and set(args['properties'])=={'key'}
    assert tools[0]['required']==['type','name','arguments']


def test_required_evidence_cannot_be_fabricated_by_a_final_without_retrieval():
    p=Policy(planner_protocol='schema-v1',require_citations=True)
    assert all(v['properties']['type']['const']=='tool' for v in variants(p))
    with pytest.raises(ValueError):variants(p,tools=0)
    final=next(v for v in variants(p,cites=['memory:ns:key:v1:'+'a'*64]) if v['properties']['type']['const']=='final')
    assert final['properties']['citations']['items']['enum']==['memory:ns:key:v1:'+'a'*64]
    assert final['properties']['citations']['minItems']==1


def test_pure_math_cannot_cite_a_tool_name_and_exhausted_tools_disappear():
    p=Policy(planner_protocol='schema-v1',answer_format='number')
    schema=variants(p,tools=0)
    assert len(schema)==1 and schema[0]['properties']['type']['const']=='final'
    assert schema[0]['properties']['answer']['type']=='string'
    assert schema[0]['properties']['citations']['maxItems']==0
    assert 'citations' in schema[0]['required']
    pattern=schema[0]['properties']['answer']['pattern']
    assert re.fullmatch(pattern,'-3.25') and not re.fullmatch(pattern,'The answer is 42')


def test_identifier_grammar_and_runtime_contract_share_the_same_shape():
    p=Policy(planner_protocol='schema-v1',answer_format='identifier')
    final=variants(p,tools=0)[0]
    assert re.fullmatch(final['properties']['answer']['pattern'],'NOVA-3091')
    assert not re.fullmatch(final['properties']['answer']['pattern'],'Project code: NOVA-3091')


def test_ordered_tool_contract_excludes_premature_final_and_out_of_order_tools():
    p=Policy(planner_protocol='schema-v1',tool_sequence=('memory.get','math.calculate'))
    def schema(executed):
        return response_format(p,observed_citations=['memory:ns:key:v1:'+'a'*64] if executed else [],
            remaining_tools=3-len(executed),executed_tools=executed)['json_schema']['schema']['anyOf']
    assert [v['properties']['name']['const'] for v in schema([])]==['memory.get']
    assert [v['properties']['name']['const'] for v in schema(['memory.get'])]==['math.calculate']
    assert any(v['properties']['type']['const']=='final' for v in schema(['memory.get','math.calculate']))
    with pytest.raises(ValueError):schema(['math.calculate'])
    with pytest.raises(ValueError):Policy(allowed_tools=('math.calculate',),tool_sequence=('memory.get',))
    with pytest.raises(ValueError):Policy(max_tool_calls=1,tool_sequence=('memory.get','math.calculate'))
