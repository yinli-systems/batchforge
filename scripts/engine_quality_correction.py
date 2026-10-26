"""Offline contract correction bound to unchanged original engine artifacts.

No inference, parser repair, reference selection, task deletion or rewrite of
the raw journal. The caller separately qualifies replay against private bytes.
"""
import hashlib,json
from pathlib import Path
from agents.contracts import protocol_json,canonical
from scripts import contract_scoring


def correction(report_raw,scores_raw):
    report=protocol_json(report_raw)
    if (report.get('protocol')!='batchforge.engine-squad.v1' or report.get('tasks')!=10570
        or report.get('evaluation_valid') is not True or report.get('delivery_passed') is not True
        or report.get('complete_public_dev') is not True or report.get('test_fixture_detected') is not False
        or report.get('dataset_sha256')!='95aa6a52d5d6a735563366753ca50492a658031da74f301ac5238b03966972c9'
        or report.get('case_manifest_sha256')!='3df3ccf00f3d7c9610cacdf4d53980320777b9516a748c60e2cacbd82bb8ca66'
        or not scores_raw.endswith(b'\n')):raise ValueError('Closed full public engine journal required')
    if len(scores_raw)>16*1024**2 or len(scores_raw.splitlines())!=21140:raise ValueError('Bounded full score journal required')
    records={};intents=set()
    for line in scores_raw.splitlines():
        row=protocol_json(line)
        if row['event']=='inference.intent':
            if row['id'] in intents:raise ValueError('Duplicate original intent')
            intents.add(row['id'])
        elif row['event']=='inference.terminal':
            r=row['record']
            if r['id'] in records or r.get('status')!='completed':raise ValueError('Unique completed original response required')
            records[r['id']]=r
        else:raise ValueError('Unknown original event')
    if len(records)!=10570 or set(records)!=intents:raise ValueError('Complete original case set required')
    if hashlib.sha256(canonical(sorted(records))).hexdigest()!='cf69e8ff6cba219a05acae3d23b3c2af4b2096c9421a3ac8a945767ad17b1492':
        raise ValueError('Official complete public case IDs required')
    raw_exact=sum(r['exact_match'] for r in records.values());raw_f1=sum(r['f1'] for r in records.values())
    if abs(report['exact_match_percent']-100*raw_exact/10570)>1e-8 or abs(report['f1_percent']-100*raw_f1/10570)>1e-8:
        raise ValueError('Original score arithmetic differs')
    grades=[];changes=[]
    for key,r in records.items():
        old={k:r[k] for k in ('exact_match','f1')};new=contract_scoring.enforce(r['answer_contract_valid'],old);grades.append(new)
        if new!=old:changes.append({'id':key,'original':old,'corrected':new,'reason':'invalid_answer_contract'})
    exact=sum(r['exact_match'] for r in grades);f1=100*sum(r['f1'] for r in grades)/10570;em=100*exact/10570
    return {'protocol':'batchforge.engine-score-contract-correction.v1','original_report_sha256':hashlib.sha256(report_raw).hexdigest(),
        'original_scores_sha256':hashlib.sha256(scores_raw).hexdigest(),'dataset_sha256':report['dataset_sha256'],
        'policy':contract_scoring.POLICY,'policy_source_sha256':hashlib.sha256(Path(contract_scoring.__file__).read_bytes()).hexdigest(),
        'tasks':10570,'invalid_answer_contracts':sum(not r['answer_contract_valid'] for r in records.values()),
        'original_exact_results':raw_exact,'original_exact_match_percent':report['exact_match_percent'],'original_f1_percent':report['f1_percent'],
        'corrected_exact_results':exact,'corrected_exact_match_percent':em,'corrected_f1_percent':f1,
        'quality_gate':report['quality_gate'],'corrected_quality_passed':em>=70 and f1>=80 and report['deadline_met'],
        'changed_cases':changes,'new_inference_requests':0,'original_artifacts_modified':False,
        'scope':'Format-invalid output is zero before reference normalization. Original report, raw responses, inputs, usage, times and cost remain unchanged.'}
