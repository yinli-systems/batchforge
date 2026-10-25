"""CPU-only public SQuAD error analysis; no inference, model judge or score repair.

Reference containment is a gold-dependent diagnostic, never a valid prediction
selector or an improved quality score. No answer/context text is exported.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
import re

from scripts.quality_eval import DATASET_SHA256,examples,normalize,score
from scripts.benchmark import distribution


def contains_tokens(haystack,needle):
    return bool(needle) and (' '+needle+' ') in (' '+haystack+' ')


def classify(answer,references,context):
    grade=score(answer,references);predicted=normalize(answer)
    gold_in_answer=any(contains_tokens(predicted,normalize(a)) for a in references)
    category=('exact' if grade['exact_match'] else 'miss_with_complete_reference_and_extra_text' if gold_in_answer
              else 'miss_with_token_overlap' if grade['f1']>0 else 'miss_without_token_overlap')
    return {**grade,'category':category,'normalized_words':len(predicted.split()),
            'prediction_is_context_span':contains_tokens(normalize(context),predicted),
            'answer_prefix_heuristic':bool(re.match(r'^\s*(?:answer\s*:|the answer is\b|according to the passage\b)',answer,re.I)),
            'empty_final_text':not answer.strip()}


def analyze(dataset,run):
    raw=dataset.read_bytes()
    if hashlib.sha256(raw).hexdigest()!=DATASET_SHA256:raise ValueError('Pinned dataset differs')
    report_path=run/'report.json';report=json.loads(report_path.read_bytes())
    measured=json.loads((run/'benchmark.json').read_bytes())
    if not report.get('evaluation_valid') or report['tasks']!=10570 or measured['test_fixture_detected']:
        raise ValueError('Require a complete valid real 10570-question evaluation')
    references={r['id']:r for r in examples(json.loads(raw),10570,report['seed'])}
    prefix=measured['run_id']+'-squad-';seen=set();counts=Counter();finishes=Counter()
    words=[];em=[];f1=[];length_capped=[];digests={};caps=Counter()
    for path in sorted((run/'outputs').glob('*-output.jsonl')):
        digests[path.name]=hashlib.sha256(path.read_bytes()).hexdigest()
        for line in path.read_text().splitlines():
            row=json.loads(line);cid=row['custom_id']
            if not cid.startswith(prefix):raise ValueError('Unexpected prediction identity')
            qid=cid[len(prefix):]
            if qid not in references or qid in seen:raise ValueError('Duplicate or unknown prediction')
            seen.add(qid);body=row['response']['body']
            if body.get('batchforge_test_only') or len(body['choices'])!=1:raise ValueError('Unsupported prediction population')
            choice=body['choices'][0];answer=choice['message'].get('content')
            if not isinstance(answer,str):raise ValueError('Final prediction must be text')
            result=classify(answer,references[qid]['answers'],references[qid]['context'])
            counts[result['category']]+=1
            for key in ['prediction_is_context_span','answer_prefix_heuristic','empty_final_text']:counts[key]+=int(result[key])
            words.append(result['normalized_words']);em.append(result['exact_match']);f1.append(result['f1'])
            finishes[str(choice.get('finish_reason'))]+=1
            if choice.get('finish_reason')=='length':
                length_capped.append(qid);caps[result['category']]+=1
    if set(references)!=seen:raise ValueError('Prediction population is incomplete')
    em_percent=100*sum(em)/len(em);f1_percent=100*sum(f1)/len(f1)
    if abs(em_percent-report['exact_match_percent'])>1e-9 or abs(f1_percent-report['f1_percent'])>1e-9:
        raise ValueError('Independent recomputation differs from original report')
    return {'protocol':'squad-diagnostics.v1','captured_at':datetime.now(timezone.utc).isoformat(),
        'cpu_only':True,'inference_requests':0,'tasks':len(seen),'original_report_sha256':hashlib.sha256(report_path.read_bytes()).hexdigest(),
        'source_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'dataset_sha256':DATASET_SHA256,
        'original_quality_gate_passed':report['passed'],'recomputed_exact_match_percent':em_percent,
        'recomputed_f1_percent':f1_percent,'category_counts':dict(counts),'finish_reasons':dict(finishes),
        'length_capped_ids':length_capped,'length_capped_categories':dict(caps),
        'normalized_answer_words':distribution(words),'prediction_files_sha256':digests,
        'limitations':['Gold-dependent diagnostic categories are not a corrected score or deployable answer selection',
            'Context-span membership and prefix heuristics do not establish correctness',
            'Public dev set, not a hidden benchmark or contamination audit','Original predictions and thresholds remain unchanged']}


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--dataset',type=Path,required=True)
    p.add_argument('--run',type=Path,required=True);p.add_argument('--out',type=Path,required=True)
    args=p.parse_args();result=analyze(args.dataset,args.run)
    with args.out.open('x') as f:json.dump(result,f,indent=2);f.write('\n')
    print(json.dumps({k:result[k] for k in ['tasks','category_counts','finish_reasons','recomputed_exact_match_percent','recomputed_f1_percent']}))


if __name__=='__main__':main()
