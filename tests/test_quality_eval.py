import pytest
from scripts.quality_eval import normalize, score, examples


def test_squad_v1_normalization_and_multiset_f1_are_not_a_model_judge():
    assert normalize('  The, CAT!  ')== 'cat'
    assert score('cat',['The cat','a feline'])=={'exact_match':1,'f1':1}
    assert score('cat cat',['cat'])['f1']==pytest.approx(2/3)
    assert score('a b c',['b c d'])['f1']==pytest.approx(.8)  # article "a" is removed
    assert score('a',['the'])=={'exact_match':1,'f1':0}  # v1.1 empty-token convention
    with pytest.raises(ValueError):score('answer',[])


def test_subset_is_reproducible_and_full_set_does_not_drop_questions():
    dataset={'data':[{'paragraphs':[{'context':'passage','qas':[
        {'id':str(i),'question':'q','answers':[{'text':'answer'}]} for i in range(10)]}]}]}
    assert examples(dataset,3,7)==examples(dataset,3,7)
    assert len(examples(dataset,10,7))==10
    with pytest.raises(ValueError):examples(dataset,11,7)
