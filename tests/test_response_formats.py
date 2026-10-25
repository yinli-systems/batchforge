import json
import pytest
from agents.contracts import Policy
from agents.structured import response_format
from backend.config import Settings
from backend.response_formats import validate_response_format
from backend.validation import validate_rows,InvalidInput
from ops.qualify_agent_grammar import fixture


def envelope(schema):return {'type':'json_schema','json_schema':{'name':'test','schema':schema}}


def test_grammar_fixture_is_accepted_only_when_operator_enables_bounded_schemas():
    cases,_=fixture();raw=b'\n'.join(json.dumps(c).encode() for c in cases)
    with pytest.raises(InvalidInput):validate_rows(raw,Settings(model_name=cases[0]['body']['model']))
    assert len(validate_rows(raw,Settings(model_name=cases[0]['body']['model'],json_schema_enabled=True)))==4


def test_actual_actor_schema_remains_admissible_after_retrieval_citation_projection():
    p=Policy(planner_protocol='schema-v1',answer_format='identifier',require_citations=True)
    fmt=response_format(p,observed_citations=['memory:workspace:fact:v1:'+'a'*64],remaining_tools=2)
    validate_response_format(fmt,allow_json_schema=True)


@pytest.mark.parametrize('schema',[
    {'$ref':'https://example.invalid/private'}, {'$ref':'file:///root/private'},
    {'$defs':{'recursive':{'$ref':'#/$defs/recursive'}}},
    {'type':'object','additionalProperties':True},
    {'type':'string','pattern':'(a+)+$'},
    {'type':'string','pattern':{}},
    {'type':'integer','maximum':10**1000},
    {'type':'string','enum':[10**1000]},
    {'type':None,'const':'x'},
    {'type':'string','maxLength':1000000000},
    {'type':'string','minimum':0},
    {'type':'number','minimum':10,'maximum':1},
    {'type':'number','maxLength':1},
    {'type':'array','items':{'type':'string'},'maxItems':1000000000},
    {'type':'string','enum':['x']*65},
    {'anyOf':[{'type':'string'}]*9},
    {'type':'object','properties':{'x':{'type':'string'}},'required':['other'],'additionalProperties':False},
])
def test_unqualified_schema_cannot_trigger_reference_resolution_or_unbounded_grammar(schema):
    with pytest.raises(ValueError):validate_response_format(envelope(schema),allow_json_schema=True)


def test_depth_and_total_bytes_are_bounded_before_engine_dispatch():
    schema={'type':'string'}
    for _ in range(14):schema={'anyOf':[schema]}
    with pytest.raises(ValueError):validate_response_format(envelope(schema),allow_json_schema=True)
    with pytest.raises(ValueError):validate_response_format(envelope({'type':'string','enum':['a'*512]*64}),allow_json_schema=True)
