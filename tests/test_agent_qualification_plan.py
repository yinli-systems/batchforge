from datetime import datetime,timezone
import pytest
from ops.qualify_agent_runtime import fixtures,heldout_fixtures,schema_fixtures,workflow_fixtures,plan,grade


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


def test_workflow_contract_fixtures_declare_actual_tools_without_reusing_prior_answers():
    from agents.contracts import Policy,digest
    previous=fixtures()+heldout_fixtures()+schema_fixtures();before=digest(schema_fixtures());values=workflow_fixtures()
    assert len(values)==20 and digest(schema_fixtures())==before
    assert not ({c['id'] for c in previous}&{c['id'] for c in values})
    assert not ({c['expected'] for c in previous}&{c['expected'] for c in values})
    for case in values:
        assert case['tool_sequence']==case['tools']
        policy=Policy(planner_protocol='schema-v1',tool_sequence=tuple(case['tool_sequence']))
        assert case['expected'] not in str(policy.document())


def test_short_lease_qualification_has_disjoint_finite_cases_and_explicit_recovery_gate():
    from ops.qualify_agent_runtime import lease_fixtures
    old=fixtures()+heldout_fixtures()+schema_fixtures()+workflow_fixtures();fresh=lease_fixtures()
    assert len(fresh)==4 and not {c['id'] for c in old}&{c['id'] for c in fresh}
    assert not {c['expected'] for c in old}&{c['expected'] for c in fresh}
    p=plan(datetime(2026,10,26,1,tzinfo=timezone.utc),16,'lease-contract-heldout-v1')
    assert p['max_new_tasks']==16 and p['fault_reclaim_gate_seconds']==30
    assert p['action_lease']=={'seconds':15,'renew_interval_seconds':5,'action_deadline_seconds':45}
