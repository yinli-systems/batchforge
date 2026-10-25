from decimal import Decimal
import pytest
from scripts.cost_allocation import allocate


def report():
    return {'protocol':'squad-1.1-em-f1-v1','elapsed_seconds':3600,'tasks':4,'passed':False,
        'evaluation_valid':True,'delivery_passed':True,'deadline_met':True,'exact_match_percent':50}


def scores():return [{'id':str(i),'exact_match':int(i<2)} for i in range(4)]


def test_exact_results_have_a_defined_cost_even_when_the_original_cohort_gate_failed():
    cost=allocate(report(),'16.10',scores=scores())
    assert Decimal(cost['pod_window_allocation_usd'])==Decimal('16.10')
    assert Decimal(cost['allocation_usd_per_on_time_reference_exact_result'])==Decimal('8.05')
    assert cost['gate_passing_cohorts']==0 and cost['allocation_usd_per_gate_passing_cohort'] is None
    assert cost['measured_invoice_usd'] is None


def test_late_or_unverified_results_cannot_be_billed_as_on_time_exact_results():
    r=report();r['deadline_met']=False
    cost=allocate(r,'16.10',scores=scores())
    assert cost['on_time_reference_exact_results']==0 and cost['allocation_usd_per_on_time_reference_exact_result'] is None
    r['evaluation_valid']=False
    with pytest.raises(ValueError):allocate(r,'16.10',scores=scores())


def test_fixed_output_compute_does_not_invent_quality_verified_costs():
    r={'elapsed_seconds_including_disconnect_and_validation':3600,'completed_tasks':100,'passed':True}
    cost=allocate(r,'16.10')
    assert Decimal(cost['allocation_usd_per_delivered_result'])==Decimal('0.161')
    assert not cost['quality_scored'] and cost['allocation_usd_per_on_time_reference_exact_result'] is None


@pytest.mark.parametrize('rate',['0','-1','NaN','Infinity'])
def test_invalid_rate_cannot_create_a_plausible_cost(rate):
    with pytest.raises(ValueError):allocate(report(),rate,scores=scores())


def test_duplicate_or_inconsistent_score_material_is_rejected():
    s=scores();s[0]['id']=s[1]['id']
    with pytest.raises(ValueError):allocate(report(),'16.10',scores=s)
    r=report();r['exact_match_percent']=75
    with pytest.raises(ValueError):allocate(r,'16.10',scores=scores())
