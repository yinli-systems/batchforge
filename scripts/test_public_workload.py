import json
from types import SimpleNamespace

import pytest

from scripts.public_workload import evaluate, score
from scripts.benchmark import generate_workload


def test_quality_denominator_includes_missing_invalid_and_truncated_outputs(tmp_path):
    manifest = {"profile": "analysis", "dataset_sha256": "fixture", "examples": {
        f"squad-{i}": {"gold_answers": ["the red fox"]} for i in range(4)}}
    def row(identifier, text, reason="stop"):
        return {"custom_id": "bench-abc-squad-"+identifier, "response": {"body": {"choices": [{"message": {"content": text}, "finish_reason": reason}]}}}
    rows = [row("0", '{"answer":"red fox","evidence":"fox","analysis":"text"}'),
            row("1", '```json\n{"answer":"red fox"}\n```'), row("2", '{"answer":', "length")]
    (tmp_path/"test-output.jsonl").write_text("\n".join(json.dumps(r) for r in rows))
    result = evaluate(manifest, tmp_path)
    assert result["missing"] == 1 and result["invalid_json_contract"] == 2
    assert result["length_truncated"] == 1
    assert result["answer_exact_match_over_expected"] == .25
    assert result["json_contract_success_rate_over_expected"] == .25
    assert result["analysis_word_count_for_valid_json"]["mean"] == 1
    assert result["requested_180_to_220_analysis_words_success_rate_over_expected"] == 0


def test_public_workload_partition_does_not_duplicate_or_drop_examples(tmp_path):
    path=tmp_path/"workload.jsonl"
    path.write_text("\n".join(json.dumps({"custom_id": f"squad-{i}", "body": {"model":"test-model", "messages":[{"role":"user","content":str(i)}]}}) for i in range(7)))
    args=SimpleNamespace(input_jsonl=path,tasks=7,batches=3,model="test-model")
    parts=generate_workload(args,"bench-unique")
    rows=[json.loads(line) for part in parts for line in part.data.splitlines()]
    assert [p.count for p in parts]==[3,2,2]
    assert [r["body"]["messages"][0]["content"] for r in rows]==list(map(str,range(7)))
    assert len({r["custom_id"] for r in rows})==7
    args.model="wrong-model"
    with pytest.raises(ValueError):generate_workload(args,"another")


def test_answer_scoring_uses_best_reference_and_handles_empty_answers():
    assert score("A red, fox!",["blue fox","the red fox"])==(1,1)
    assert score("fox",["the red fox"])==(0,2/3)
    assert score("",["nothing"])==(0,0)


def test_translation_contract_does_not_claim_answer_or_fidelity_scores(tmp_path):
    manifest = {"profile": "translate", "dataset_sha256": "fixture", "examples": {"squad-0": {"gold_answers": ["unused"]}}}
    row = {"custom_id": "squad-0", "response": {"body": {"choices": [{"message": {"content": '{"translation_zh":"示例译文"}'}, "finish_reason": "stop"}]}}}
    (tmp_path / "test-output.jsonl").write_text(json.dumps(row))
    result = evaluate(manifest, tmp_path)
    assert result["json_contract_success_rate_over_expected"] == 1
    assert result["answer_exact_match_over_expected"] is None
    assert result["translation_character_count_for_valid_json"]["mean"] == 4


def test_reasoning_only_and_nonstandard_json_are_failed_answers_not_evaluator_crashes(tmp_path):
    manifest = {"profile": "answer", "dataset_sha256": "fixture", "examples": {
        f"squad-{i}": {"gold_answers": ["fox"]} for i in range(4)}}
    contents = [None, '{"answer":"wrong","answer":"fox"}',
                '{"answer":"fox","extra":NaN}', '{"answer":"fox"}']
    rows = [{"custom_id": f"squad-{i}", "response": {"body": {"choices": [{
        "message": {"content": content, "reasoning_content": "working"},
        "finish_reason": "length" if i == 0 else "stop"}]}}}
        for i, content in enumerate(contents)]
    (tmp_path / 'test-output.jsonl').write_text('\n'.join(json.dumps(row) for row in rows))
    result = evaluate(manifest, tmp_path)
    assert result['observed_unique'] == 4 and result['missing'] == 0
    assert result['invalid_json_contract'] == 3 and result['length_truncated'] == 1
    assert result['json_contract_success_rate_over_expected'] == .25
    assert result['answer_exact_match_over_expected'] == .25
