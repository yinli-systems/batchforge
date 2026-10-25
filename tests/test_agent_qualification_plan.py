from datetime import datetime,timezone
import pytest
from ops.qualify_agent_runtime import fixtures,heldout_fixtures,schema_fixtures,plan,grade


def test_native_control_state_and_azure_model_state_are_not_conflated():
    result=plan(datetime(2026,10,25,22,tzinfo=timezone.utc),80)
    assert result['cases']==20 and result['max_new_tasks']==80
    assert 'not Azure Agent' in result['control_state']
    assert result['service_changes'] is False and result['quota_changes'] is False


@pytest.mark.parametrize('now,headroom',[(datetime(2026,10,26,8,40,tzinfo=timezone.utc),1000),
                                      (datetime(2026,10,25,22,tzinfo=timezone.utc),79)])
def test_agent_qualification_cannot_escape_task_or_absolute_time_budget(now,headroom):
    with pytest.raises(ValueError):plan(now,headroom)


def test_task_success_requires_actual_required_tool_and_not_just_final_answer():
    case=fixtures()[0]
    run={'status':'completed','result':{'answer':case['expected'],'citations':[]},'state':{'observations':[]}}
    assert grade(case,run)['passed'] is False
    run['state']['observations']=[{'tool':'math.calculate','result':{'value':int(case['expected'])}}]
    assert grade(case,run)['passed'] is True
    run['state']['observations'][0]['result']['value']=0
    assert grade(case,run)['passed'] is False


def test_no_grade_for_an_unfinished_or_failed_agent_even_if_model_produced_a_string():
    assert not grade(fixtures()[0],{'status':'failed'})['passed']


def test_value_contract_holdout_has_new_ids_and_answers_and_explicit_generic_policy():
    baseline=fixtures();heldout=heldout_fixtures()
    assert not ({c['id'] for c in baseline}&{c['id'] for c in heldout})
    assert not ({c['expected'] for c in baseline}&{c['expected'] for c in heldout})
    assert len(heldout)==20 and all(c['answer_format'] in ('identifier','number') for c in heldout)
    assert sum(c['require_citations'] for c in heldout)==12
    result=plan(datetime(2026,10,25,22,tzinfo=timezone.utc),80,'value-contract-heldout-v1')
    assert result['max_new_tasks']==80 and result['fixture_sha256']!=plan(datetime(2026,10,25,22,tzinfo=timezone.utc),80)['fixture_sha256']


def test_schema_holdout_is_disjoint_and_does_not_rewrite_the_value_holdout():
    from agents.contracts import digest
    before=digest(heldout_fixtures());fresh=schema_fixtures()
    assert digest(heldout_fixtures())==before=='19382f4e66b1a2c959c4e2532bc590907688da06ab72fadd8dc10d11e85894ec'
    prior=fixtures()+heldout_fixtures()
    assert len(fresh)==20 and all(c['planner_protocol']=='schema-v1' for c in fresh)
    assert not ({c['id'] for c in fresh}&{c['id'] for c in prior})
    assert not ({c['expected'] for c in fresh}&{c['expected'] for c in prior})
