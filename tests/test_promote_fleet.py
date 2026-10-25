import pytest
from ops.promote_fleet import qualify_recovered_run


def evidence():
    original={name:{'pid':index+1,'start_ticks':'1','cwd':'test-fixture'} for index,name in enumerate(
        ['api-a','api-b','gateway-edge','engine-qwen480bench','worker-qwen480'])}
    recovered={'pid':99,'start_ticks':'2','cwd':'test-fixture'}
    return [dict(phase='failed',error_type='RuntimeError',exit_code=0,original_identities=original),
        dict(passed=True,accepted_tasks=3600,completed_tasks=3600,unfinished_tasks=0,failed_tasks=0,
             accepted_exactly_once=True,accepted_usage_matches_task_usage=True,fixed_output_lengths_exact=True),
        dict(passed=True,phase='complete',quiescence_observed_twice=True,accepted_pending_identities_retained=True,
             engine_actions=False,api_actions=False,source_or_environment_changed=False,
             original_identities=original,recovered_worker_identity=recovered),
        dict(phase='complete',passed=False,managed_identities_unchanged=True,
             original_identities={**original,'worker-qwen480':recovered},
             calibration=dict(delivery_passed=True,evaluation_valid=True),calibration_paired=dict(tasks=100),
             service_actions=False,cloud_actions=False)]


def test_separate_qualification_does_not_upgrade_failed_endurance_or_quality():
    result=qualify_recovered_run(*evidence())
    assert result['separate_post_incident_delivery_qualified'] is True
    assert result['uninterrupted_endurance_passed'] is False
    assert result['squad_subset_gate_passed'] is False


@pytest.mark.parametrize('index,key,value',[(0,'phase','complete'),(0,'exit_code',1),
    (1,'accepted_tasks',3599),(1,'accepted_exactly_once',False),(1,'unfinished_tasks',1),
    (2,'source_or_environment_changed',True),(2,'engine_actions',True),(2,'quiescence_observed_twice',False),
    (3,'managed_identities_unchanged',False),(3,'service_actions',True),(3,'phase','holdout_running')])
def test_missing_or_ambiguous_reconciliation_blocks_release(index,key,value):
    inputs=evidence();inputs[index][key]=value
    with pytest.raises(ValueError):qualify_recovered_run(*inputs)


def test_partial_new_qualification_does_not_authorize_rollout():
    inputs=evidence();inputs[3]['calibration_paired']['tasks']=99
    with pytest.raises(ValueError):qualify_recovered_run(*inputs)


def test_identity_reconciliation_is_required_after_recovery():
    inputs=evidence();inputs[3]['original_identities']['engine-qwen480bench']={'pid':100}
    with pytest.raises(ValueError):qualify_recovered_run(*inputs)
