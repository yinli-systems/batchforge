"""Operator-published quality evidence, linked to tenant-owned durable batches.

Files are immutable release inputs with pinned hashes. This catalogue does not
execute code, grade model answers, infer cost, or attest independent hardware.
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat

HEX=re.compile('[a-f0-9]{64}\\Z')
BID=re.compile('batch_[a-f0-9]{32}\\Z')
EID=re.compile('[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}\\Z')
SQUAD_DEV_SHA256='95aa6a52d5d6a735563366753ca50492a658031da74f301ac5238b03966972c9'
LEDGER_SQL="""SELECT id,model,status,total_count,completed_count,failed_count,cancelled_count,
                    prompt_tokens,completion_tokens,runtime_fingerprint
              FROM batches WHERE tenant_id=%s AND id=ANY(%s)"""


def private_release_bytes(path,limit):
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    with os.fdopen(fd,'rb') as f:
        info=os.fstat(f.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid!=os.geteuid() or info.st_mode&0o022:
            raise ValueError('Quality evidence must be an owned non-writable release file')
        raw=f.read(limit+1)
    if len(raw)>limit:raise ValueError('Quality evidence file exceeds bound')
    return raw


def finite(value,low,high):
    if type(value) not in (int,float) or not math.isfinite(value) or not low<=value<=high:
        raise ValueError('Invalid bounded quality metric')
    return value


def integer(value,low,high):
    if type(value) is not int or not low<=value<=high:raise ValueError('Invalid quality counter')
    return value


@dataclass(frozen=True)
class Evaluation:
    id: str
    batch_ids: tuple[str,...]
    view: dict


def evaluation(entry,report):
    if not isinstance(entry,dict) or not isinstance(report,dict):raise ValueError('Evidence objects required')
    eid,name=entry.get('id'),entry.get('name')
    if not isinstance(eid,str) or not EID.fullmatch(eid) or not isinstance(name,str) or not 1<=len(name)<=128:
        raise ValueError('Invalid catalogue identity')
    if (report.get('protocol')!='squad-1.1-em-f1-v1' or report.get('evaluation_valid') is not True
        or report.get('delivery_passed') is not True):
        raise ValueError('Only complete valid reference-based evaluations can be published')
    ids=report.get('batch_ids')
    if (not isinstance(ids,list) or not 1<=len(ids)<=256
        or any(not isinstance(x,str) or not BID.fullmatch(x) for x in ids) or len(set(ids))!=len(ids)):
        raise ValueError('Invalid bounded batch cohort')
    tasks=integer(report.get('tasks'),1,10570)
    scope=report.get('scope')
    if (report.get('dataset_sha256')!=SQUAD_DEV_SHA256 or scope not in ('complete dev set','seeded dev subset')
        or (scope=='complete dev set')!=(tasks==10570)):
        raise ValueError('Pinned public dataset and explicit cohort scope required')
    if integer(report.get('predictions'),0,1000000)!=tasks or integer(report.get('missing'),0,1000000)!=0:
        raise ValueError('Incomplete evaluation cohort')
    model=report.get('model');fingerprint=report.get('runtime_fingerprint')
    if not isinstance(model,str) or not 1<=len(model)<=256:raise ValueError('Invalid evidence model')
    if not isinstance(fingerprint,str) or not re.fullmatch('sha256:[a-f0-9]{64}',fingerprint):
        raise ValueError('Pinned runtime fingerprint required')
    for key in ['dataset_sha256','source_sha256','benchmark_sha256']:
        if not isinstance(report.get(key),str) or not HEX.fullmatch(report[key]):raise ValueError('Pinned source hashes required')
    em=finite(report.get('exact_match_percent'),0,100);f1=finite(report.get('f1_percent'),0,100)
    if abs(tasks*em/100-round(tasks*em/100))>1e-6:raise ValueError('Exact-match count must be integral')
    gate=report.get('quality_gate',{})
    if not isinstance(gate,dict):raise ValueError('Explicit quality gate required')
    minimum_em=finite(gate.get('minimum_exact_match'),0,1)*100
    minimum_f1=finite(gate.get('minimum_f1'),0,1)*100
    if type(report.get('deadline_met')) is not bool:raise ValueError('Explicit deadline state required')
    passed=report['deadline_met'] and em>=minimum_em and f1>=minimum_f1
    if report.get('passed') is not passed:raise ValueError('Quality gate contradicts recorded metrics')
    seconds=finite(report.get('elapsed_seconds'),.000001,86400*365)
    input_tokens=integer(report.get('prompt_tokens'),0,2**53-1)
    output_tokens=integer(report.get('completion_tokens'),0,2**53-1)
    goodput=report.get('on_time_exact_results_per_second')
    if goodput is not None:
        finite(goodput,0,1000000)
        if not report['deadline_met'] or abs(goodput-(em/100*tasks/seconds))>1e-6:
            raise ValueError('On-time exact goodput contradicts recorded metrics')
    contract=report.get('answer_contract','span-v1')
    if contract not in ('span-v1','json-span-v1'):raise ValueError('Unknown published answer contract')
    contract_sha=report.get('answer_contract_sha256')
    selected_sha=report.get('selected_ids_sha256');excluded_sha=report.get('excluded_ids_sha256')
    for value in (contract_sha,selected_sha,excluded_sha):
        if value is not None and (not isinstance(value,str) or not HEX.fullmatch(value)):raise ValueError('Invalid answer/cohort hash')
    invalid=report.get('invalid_answer_contracts')
    if invalid is not None:integer(invalid,0,tasks)
    if contract=='json-span-v1' and (None in (contract_sha,selected_sha,excluded_sha,invalid) or tasks*em/100>tasks-invalid+1e-6):
        raise ValueError('Complete answer contract evidence required')
    view={'id':eid,'object':'quality_evaluation','name':name,'model':model,
        'runtime_fingerprint':fingerprint,'protocol':report['protocol'],
        'dataset':{'name':'SQuAD 1.1','scope':scope+' (public)','sha256':report['dataset_sha256']},
        'tasks':tasks,'batch_count':len(ids),'delivery_passed':True,'deadline_met':report['deadline_met'],
        'quality_state':'passed' if passed else 'failed',
        'metrics':{'exact_match_percent':em,'f1_percent':f1,'on_time_exact_results_per_second':goodput,
                   'cohort_seconds':seconds,'prompt_tokens':input_tokens,'completion_tokens':output_tokens},
        'gate':{'minimum_exact_match_percent':minimum_em,'minimum_f1_percent':minimum_f1},
        'report_sha256':entry['sha256'],'scorer_sha256':report['source_sha256'],
        'benchmark_sha256':report['benchmark_sha256'],'cost':None,
        'answer_contract':{'id':contract,'source_sha256':contract_sha,'invalid_results':invalid,
            'selected_ids_sha256':selected_sha,'excluded_ids_sha256':excluded_sha},
        'verification_scope':['Operator-published hash-bound report, not an issuer signature',
            'Batch ownership, model, runtime, counters and usage reconciled on each read',
            'Public dev score is not a hidden test, contamination audit or production accuracy guarantee',
            'Runtime identity is worker-reported; this endpoint does not independently attest hardware or re-audit attempts']}
    return Evaluation(eid,tuple(ids),view)


class EvidenceCatalogue:
    def __init__(self,path):
        path=Path(path).absolute();raw=private_release_bytes(path,65536)
        data=json.loads(raw)
        if not isinstance(data,dict) or data.get('protocol')!='batchforge.quality-catalogue.v1' or not isinstance(data.get('evaluations'),list) or len(data['evaluations'])>16:
            raise ValueError('Invalid bounded quality catalogue')
        records=[]
        for entry in data['evaluations']:
            if not isinstance(entry,dict):raise ValueError('Evidence entry required')
            filename=entry.get('report');expected=entry.get('sha256')
            if not isinstance(filename,str) or not re.fullmatch('[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}\\.json',filename):
                raise ValueError('Evidence report must be a release basename')
            if not isinstance(expected,str) or not HEX.fullmatch(expected):raise ValueError('Report hash required')
            content=private_release_bytes(path.parent/filename,2*1024**2)
            if hashlib.sha256(content).hexdigest()!=expected:raise ValueError('Evidence report hash differs')
            report=json.loads(content);record=evaluation(entry,report)
            if 'cost_report' in entry or 'cost_sha256' in entry:
                filename=entry.get('cost_report');expected_cost=entry.get('cost_sha256')
                if (not isinstance(filename,str) or not re.fullmatch('[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}\\.json',filename)
                    or not isinstance(expected_cost,str) or not HEX.fullmatch(expected_cost)):raise ValueError('Invalid cost report pin')
                cost_raw=private_release_bytes(path.parent/filename,65536)
                if hashlib.sha256(cost_raw).hexdigest()!=expected_cost:raise ValueError('Cost report hash differs')
                from backend.cost_evidence import allocation_view
                record.view['cost']=allocation_view(json.loads(cost_raw),report,entry['sha256'])
                record.view['cost']['report_sha256']=expected_cost
            records.append(record)
        if len({r.id for r in records})!=len(records):raise ValueError('Duplicate evaluation identity')
        self.records=tuple(records)

    def visible(self,rows,*,details=False):
        owned={r['id']:r for r in rows};result=[]
        for record in self.records:
            if not set(record.batch_ids)<=owned.keys():continue
            batch=[owned[bid] for bid in record.batch_ids];view=deepcopy(record.view);metrics=view['metrics']
            linked=(all(b['status']=='completed' and b['model']==view['model'] and
                        b['runtime_fingerprint']==view['runtime_fingerprint'] for b in batch)
                    and all(sum(b[k] for b in batch)==target for k,target in [
                        ('total_count',view['tasks']),('completed_count',view['tasks']),
                        ('failed_count',0),('cancelled_count',0),
                        ('prompt_tokens',metrics['prompt_tokens']),('completion_tokens',metrics['completion_tokens'])]))
            view['verification_state']='batch_ledger_reconciled' if linked else 'batch_ledger_mismatch'
            if not linked:
                view['quality_state']='unverified';view['metrics']=None;view['cost']=None
            if details:view['batch_ids']=list(record.batch_ids)
            result.append(view)
        return result
