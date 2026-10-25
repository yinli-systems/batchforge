import hashlib,json
import pytest
from backend.cost_evidence import allocation_view
from backend.evaluations import EvidenceCatalogue
from scripts.cost_allocation import allocate
from test_evaluations import report,write_catalogue,ledger


def overlay():
    value=allocate(report(),'16.10',scores=[{'id':'a','exact_match':1},{'id':'b','exact_match':0}])
    value.update(source_report_sha256='a'*64,source_scores_sha256='b'*64,allocator_sha256='c'*64,original_report_modified=False)
    return value


def test_exact_answer_cost_is_not_a_passing_cohort_or_invoice():
    value=allocation_view(overlay(),report(),'a'*64)
    assert value['measured_invoice_usd'] is None
    assert value['on_time_reference_exact_results']==1
    assert value['kind']=='operator_rate_time_allocation'


@pytest.mark.parametrize('key,value',[
    ('measured_invoice_usd',1),('hourly_rate','NaN'),('measured_wall_seconds','9'),
    ('pod_window_allocation_usd','0'),('source_report_sha256','d'*64),
    ('on_time_reference_exact_results',2),('allocation_usd_per_on_time_reference_exact_result','999'),
    ('quality_cohort_gate_passed',True),('gate_passing_cohorts',1),
    ('allocation_usd_per_gate_passing_cohort','1'),('delivered_results',True)])
def test_mislinked_or_inconsistent_allocation_cannot_be_published(key,value):
    raw=overlay();raw[key]=value
    with pytest.raises(ValueError):allocation_view(raw,report(),'a'*64)


def test_catalogue_checks_cost_bytes_and_hides_allocation_on_ledger_drift(tmp_path):
    path=write_catalogue(tmp_path);data=json.loads(path.read_text());entry=data['evaluations'][0]
    cost=overlay();cost['source_report_sha256']=entry['sha256'];raw=json.dumps(cost).encode()
    (tmp_path/'allocation.json').write_bytes(raw);entry.update(cost_report='allocation.json',cost_sha256=hashlib.sha256(raw).hexdigest())
    path.write_text(json.dumps(data));catalogue=EvidenceCatalogue(path)
    assert catalogue.visible(ledger())[0]['cost']['on_time_reference_exact_results']==1
    drift=ledger();drift[0]['completion_tokens']=2
    assert catalogue.visible(drift)[0]['cost'] is None
    (tmp_path/'allocation.json').write_text('{}')
    with pytest.raises(ValueError,match='hash differs'):EvidenceCatalogue(path)
