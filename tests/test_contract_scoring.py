import pytest
from scripts.contract_scoring import enforce
from scripts.quality_eval import score


@pytest.mark.parametrize('reference',['a','an','the','The!'])
def test_invalid_protocol_is_zero_even_when_reference_normalizes_to_empty(reference):
    legacy=score('',[reference]);assert legacy=={'exact_match':1,'f1':0}
    assert enforce(False,legacy)=={'exact_match':0,'f1':0.0}
    assert legacy=={'exact_match':1,'f1':0}  # no mutation of raw legacy record


def test_valid_prediction_retains_official_v11_empty_token_edge_case():
    assert enforce(True,score('a',['the']))=={'exact_match':1,'f1':0}


@pytest.mark.parametrize('valid,grade',[(1,{'exact_match':1,'f1':1}),(False,{'exact_match':True,'f1':1}),
    (True,{'exact_match':1,'f1':float('nan')}),(True,{'exact_match':2,'f1':1})])
def test_broken_grade_is_refused_not_silently_repaired(valid,grade):
    with pytest.raises(ValueError):enforce(valid,grade)
