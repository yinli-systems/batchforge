import hashlib,json
from pathlib import Path
import pytest
from scripts.engine_quality_correction import correction

BASE=Path(__file__).resolve().parents[1]/'docs/benchmarks/platform480-2026-10-25/soak480-engine-quality-full-20261026-v1-complete/run'


def test_real_legacy_journal_has_one_contract_correction_without_replaying_inference():
    report=(BASE/'report.json').read_bytes();scores=(BASE/'scores.jsonl').read_bytes();before=hashlib.sha256(scores).hexdigest()
    r=correction(report,scores)
    assert r['original_exact_results']==8668 and r['corrected_exact_results']==8667
    assert r['corrected_exact_match_percent']==pytest.approx(81.99621570482498)
    assert r['corrected_f1_percent']==pytest.approx(r['original_f1_percent']) and r['corrected_quality_passed']
    assert r['changed_cases']==[{'id':'5725bad5271a42140099d0c1','original':{'exact_match':1,'f1':0.0},
        'corrected':{'exact_match':0,'f1':0.0},'reason':'invalid_answer_contract'}]
    assert r['new_inference_requests']==0 and r['original_artifacts_modified'] is False
    assert (BASE/'scores.jsonl').read_bytes()==scores and hashlib.sha256(scores).hexdigest()==before


@pytest.mark.parametrize('mutation',['partial','duplicate','unknown_ids','changed_metric','wrong_dataset'])
def test_correction_refuses_incomplete_or_relabelled_original_evidence(mutation):
    report=json.loads((BASE/'report.json').read_bytes());scores=(BASE/'scores.jsonl').read_bytes()
    if mutation=='partial':scores=scores[:-1]
    if mutation=='duplicate':
        lines=scores.splitlines();lines[-1]=lines[0];scores=b'\n'.join(lines)+b'\n'
    if mutation=='unknown_ids':scores=scores.replace(b'56be4db0acb8001400a502ec',b'000000000000000000000000')
    if mutation=='changed_metric':report['exact_match_percent']+=1
    if mutation=='wrong_dataset':report['dataset_sha256']='a'*64
    with pytest.raises(ValueError):correction(json.dumps(report).encode(),scores)
