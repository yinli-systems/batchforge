from scripts.quality_diagnostics import classify


def test_gold_containment_is_diagnostic_and_never_changes_em_or_f1():
    row=classify('The answer is Paris.',['Paris'],'Paris is the capital of France.')
    assert row['exact_match']==0 and row['f1']<1
    assert row['category']=='miss_with_complete_reference_and_extra_text'
    assert row['answer_prefix_heuristic']
    assert not row['prediction_is_context_span']


def test_context_span_is_not_correctness_and_token_boundaries_matter():
    wrong=classify('France',['Paris'],'Paris is the capital of France.')
    assert wrong['prediction_is_context_span'] and not wrong['exact_match']
    assert wrong['category']=='miss_without_token_overlap'
    partial=classify('cart',['art'],'The word cart appears here.')
    assert partial['category']=='miss_without_token_overlap'
    assert classify('The Paris!',['Paris'],'Paris is here.')['category']=='exact'
