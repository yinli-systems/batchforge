import pytest
from ops.qualify_agent_grammar import fixture,validate
from ops.agent_endpoint import qualification_endpoint


def test_four_decoder_fixtures_use_real_schema_constraints_and_explicit_synthetic_data():
    cases,citation=fixture()
    assert len(cases)==4 and len({c['custom_id'] for c in cases})==4
    assert all(c['body']['response_format']['type']=='json_schema' for c in cases)
    assert citation.startswith('memory:decoder:fixture:v1:')
    assert all(c['body']['max_tokens']==256 for c in cases)


def test_calibration_does_not_grade_an_invented_tool_or_wrapped_answer_as_valid():
    _,cite=fixture()
    assert validate('math-tool','{"type":"tool","name":"math.calculate","arguments":{"expression":"20+22"}}',cite)
    assert not validate('math-tool','{"type":"tool","name":"memory.get","arguments":{"key":"decoder-fixture"}}',cite)
    assert validate('identifier-final','{"type":"final","answer":"CALIB-701","citations":["'+cite+'"]}',cite)
    with pytest.raises(ValueError):validate('identifier-final','{"type":"final","answer":"The code is CALIB-701","citations":["'+cite+'"]}',cite)


def test_finite_qualification_cannot_forward_executor_keys_to_another_endpoint():
    assert qualification_endpoint('http://127.0.0.1:18172').endswith(':18172')
    for value in ('https://example.com','http://127.0.0.1:8348','http://127.0.0.1:18172/private','http://localhost:18172','http://127.0.0.1:18172?token=x'):
        with pytest.raises(ValueError):qualification_endpoint(value)
