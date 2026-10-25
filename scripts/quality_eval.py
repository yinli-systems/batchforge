"""Ground-truth SQuAD 1.1 evaluation through an existing real Batch API.

Reference normalization/metric: the official v1.1 evaluator, maintained at
https://github.com/allenai/bi-att-flow/blob/master/squad/evaluate-v1.1.py
Dataset: Stanford SQuAD, CC BY-SA 4.0. No model-written judge is involved.
"""
from __future__ import annotations

import argparse
import asyncio
from collections import Counter
import hashlib
import json
import math
import os
from pathlib import Path
import random
import re
import string
from types import SimpleNamespace

from ops.gpu_campaign import write_json
from scripts import benchmark

DATASET_SHA256='95aa6a52d5d6a735563366753ca50492a658031da74f301ac5238b03966972c9'
MODEL='QuantTrio/Qwen3-Coder-480B-A35B-Instruct-AWQ'


def normalize(text):
    text=''.join(c for c in text.lower() if c not in string.punctuation)
    return ' '.join(re.sub(r'\b(a|an|the)\b',' ',text).split())


def score(prediction, answers):
    if not answers:raise ValueError('At least one ground-truth answer is required')
    predicted=normalize(prediction).split();matches=[]
    for answer in answers:
        expected=normalize(answer).split()
        overlap=sum((Counter(predicted)&Counter(expected)).values())
        f1=2*overlap/(len(predicted)+len(expected)) if overlap else 0.0
        matches.append((int(normalize(prediction)==normalize(answer)),f1))
    return {'exact_match':max(x[0] for x in matches),'f1':max(x[1] for x in matches)}


def examples(dataset, count, seed):
    rows=[{'id':qa['id'],'context':p['context'],'question':qa['question'],
           'answers':[a['text'] for a in qa['answers']]}
          for article in dataset['data'] for p in article['paragraphs'] for qa in p['qas']]
    if len({r['id'] for r in rows})!=len(rows) or any(not r['answers'] for r in rows):
        raise ValueError('Dataset has duplicate IDs or missing answers')
    if not 1<=count<=len(rows):raise ValueError('Requested count exceeds dataset')
    rows.sort(key=lambda row:row['id'])
    if count<len(rows):rows=random.Random(seed).sample(rows,count)
    return rows


def evaluate(args):
    content=args.dataset.read_bytes()
    if hashlib.sha256(content).hexdigest()!=DATASET_SHA256:raise ValueError('Pinned public dev set differs')
    dataset=json.loads(content)
    if dataset.get('version')!='1.1':raise ValueError('SQuAD 1.1 required')
    rows=examples(dataset,args.tasks,args.seed)
    args.out.mkdir(mode=0o700,parents=True,exist_ok=False);os.umask(0o077)
    workload=args.out/'workload.jsonl'
    requests=[{'custom_id':'squad-'+r['id'],'method':'POST','url':'/v1/chat/completions','body':{
        'model':args.model,'temperature':0,'max_tokens':128,'messages':[
            {'role':'system','content':'Answer the question using the passage. Return only the shortest answer span from the passage. Do not explain or add a prefix.'},
            {'role':'user','content':'Passage:\n'+r['context']+'\n\nQuestion: '+r['question']}]}}
        for r in rows]
    workload.write_text(''.join(json.dumps(r,ensure_ascii=False)+'\n' for r in requests))
    plan={'protocol':'squad-1.1-em-f1-v1','dataset_sha256':DATASET_SHA256,'tasks':len(rows),
        'scope':'complete dev set' if len(rows)==10570 else 'seeded dev subset','seed':args.seed,
        'model':args.model,'runtime_fingerprint':args.runtime_fingerprint,
        'max_tokens':128,'min_tokens':None,'temperature':0,'deadline_seconds':args.deadline,
        'batch_size':64,'max_inflight_batches':2,'submission_concurrency':1,'create_retries':5,
        'quality_gate':{'minimum_exact_match':args.min_em,'minimum_f1':args.min_f1},
        'workload_sha256':hashlib.sha256(workload.read_bytes()).hexdigest(),
        'source_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        'benchmark_sha256':hashlib.sha256(Path(benchmark.__file__).read_bytes()).hexdigest(),
        'limitations':['Public dev set, not a hidden test or contamination audit',
                      'Deadline applies to the complete client-observed cohort; no per-task on-time claim if exceeded',
                      'Real inference quality run; not comparable to forced-length capacity experiments']}
    run_args=SimpleNamespace(url=args.url,model=args.model,tasks=len(rows),batches=math.ceil(len(rows)/64),
        max_tokens=128,temperature=0,prompt='',engine_label='squad-quality-'+args.out.name,
        metadata={'name':'SQuAD 1.1 quality evaluation','evaluation_protocol':plan['protocol'],
                  'dataset_sha256':DATASET_SHA256},
        deadline=args.deadline,run_deadline=args.deadline,max_inflight_batches=2,request_timeout=90,poll=1,workload_dir=args.out/'inputs',
        input_jsonl=workload,output_dir=args.out/'outputs',submission_concurrency=1,create_retries=5)
    payloads=benchmark.generate_workload(run_args,'bench-000000000000')
    plan['input_bytes']=sum(len(p.data) for p in payloads)
    plan['max_batch_input_bytes']=max(len(p.data) for p in payloads)
    if plan['input_bytes']>64*1024*1024 or plan['max_batch_input_bytes']>10*1024*1024:
        raise ValueError('Public evaluation exceeds its existing-resource input budget')
    write_json(args.out/'plan.json',plan)
    measured=asyncio.run(benchmark.run(run_args));write_json(args.out/'benchmark.json',measured)
    references={r['id']:r for r in rows};grades={};prefix=measured['run_id']+'-squad-'
    for path in sorted((args.out/'outputs').glob('*-output.jsonl')):
        for line in path.read_text().splitlines():
            result=json.loads(line);cid=result['custom_id']
            if not cid.startswith(prefix):raise ValueError('Unknown prediction ID')
            qid=cid[len(prefix):]
            if qid not in references or qid in grades:raise ValueError('Duplicate or unknown prediction')
            body=result['response']['body'];answer=body['choices'][0]['message'].get('content') or ''
            if not isinstance(answer,str):raise ValueError('Prediction is not final text')
            grades[qid]={'id':qid,**score(answer,references[qid]['answers']),
                'prediction_sha256':hashlib.sha256(answer.encode()).hexdigest(),
                'finish_reason':body['choices'][0].get('finish_reason'),
                'prompt_tokens':body['usage']['prompt_tokens'],'completion_tokens':body['usage']['completion_tokens']}
    em=sum(g['exact_match'] for g in grades.values())/len(rows)
    f1=sum(g['f1'] for g in grades.values())/len(rows)
    delivered=measured['passed'] and not measured['test_fixture_detected'] and len(grades)==len(rows)
    on_time=delivered and measured['elapsed_seconds']<=args.deadline
    with (args.out/'scores.jsonl').open('x') as out:
        for qid in sorted(references):out.write(json.dumps(grades.get(qid,{'id':qid,'exact_match':0,'f1':0,'missing':True}))+'\n')
    report={**plan,'passed':delivered and on_time and em>=args.min_em and f1>=args.min_f1,
        'delivery_passed':delivered,'deadline_met':on_time,'evaluation_valid':delivered,
        'predictions':len(grades),'missing':len(rows)-len(grades),
        'exact_match_percent':100*em if delivered else None,'f1_percent':100*f1 if delivered else None,
        'elapsed_seconds':measured['elapsed_seconds'],
        'exact_results_per_second':sum(g['exact_match'] for g in grades.values())/measured['elapsed_seconds'] if delivered else None,
        'on_time_exact_results_per_second':sum(g['exact_match'] for g in grades.values())/measured['elapsed_seconds'] if on_time else None,
        'batch_ids':measured['batch_ids'],'prompt_tokens':measured['observed_prompt_tokens'],
        'completion_tokens':measured['observed_completion_tokens'],'cost':None}
    write_json(args.out/'report.json',report)
    print(json.dumps({k:report[k] for k in ['passed','delivery_passed','tasks','exact_match_percent','f1_percent','elapsed_seconds']}),flush=True)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--dataset',type=Path,required=True);p.add_argument('--out',type=Path,required=True)
    p.add_argument('--url',default='http://127.0.0.1:8080');p.add_argument('--model',default=MODEL)
    p.add_argument('--tasks',type=int,choices=[100,1000,10570],default=10570);p.add_argument('--seed',type=int,default=20261025)
    p.add_argument('--deadline',type=int,default=1200);p.add_argument('--runtime-fingerprint',required=True)
    p.add_argument('--min-em',type=float,default=.70);p.add_argument('--min-f1',type=float,default=.80)
    args=p.parse_args()
    if not 0<=args.min_em<=1 or not 0<=args.min_f1<=1 or not 60<=args.deadline<=3600:p.error('Invalid finite quality gate')
    evaluate(args)


if __name__=='__main__':main()
