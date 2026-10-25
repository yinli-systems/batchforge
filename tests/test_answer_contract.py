import json
import pytest
from scripts.answer_contract import messages, parse_answer
from scripts.quality_eval import examples


def test_default_prompt_preserves_baseline():
    m=messages('A passage','A question')
    assert len(m)==2 and m[1]['content']=='Passage:\nA passage\n\nQuestion: A question'
    assert parse_answer('extra text','passage')=='extra text'


def test_contract_uses_no_gold_and_extracts_only_declared_field():
    m=messages('A clock opened in 1987.','When?', 'json-span-v1')
    assert len(m)==6 and m[-1]['role']=='user'
    assert parse_answer('{"answer":"1987"}', 'A clock opened in 1987.', 'json-span-v1')=='1987'


@pytest.mark.parametrize('raw', ['{"answer":"invented"}', '{"answer":""}', '{"answer":1987}',
    '{"answer":"1987","explanation":"why"}', '{"answer":"1987","answer":"1987"}',
    '```json\n{"answer":"1987"}\n```','["1987"]','{"answer":"19"} extra'])
def test_malformed_or_nonspan_prediction_is_not_repaired(raw):
    with pytest.raises(ValueError):parse_answer(raw,'A clock opened in 1987.','json-span-v1')


def test_calibration_ids_cannot_leak_into_holdout():
    dataset={'data':[{'paragraphs':[{'context':'text','qas':[{'id':str(i),'question':'q','answers':[{'text':'text'}]} for i in range(12)]}]}]}
    calibration=examples(dataset,3,42)
    holdout=examples(dataset,8,43,[r['id'] for r in calibration])
    assert not {r['id'] for r in calibration}&{r['id'] for r in holdout}
    assert holdout==examples(dataset,8,43,[r['id'] for r in calibration])
    with pytest.raises(ValueError):examples(dataset,12,43,[r['id'] for r in calibration])
