"""Curated engine-only evidence; never fabricate or imply a Batch ledger link.

Owned immutable release files are checked at startup. A private service profile
or model response is never returned. Tenant authentication is handled by the
existing API; the catalogue additionally declares each record's audience.
"""
from __future__ import annotations
from copy import deepcopy
from datetime import datetime
import hashlib,json,math,re,stat
from pathlib import Path
from agents.contracts import canonical,protocol_json
from backend.evaluations import private_release_bytes,finite,integer,HEX,EID,SQUAD_DEV_SHA256

MODEL='QuantTrio/Qwen3-Coder-480B-A35B-Instruct-AWQ'
CASESET='3df3ccf00f3d7c9610cacdf4d53980320777b9516a748c60e2cacbd82bb8ca66'
IDS='cf69e8ff6cba219a05acae3d23b3c2af4b2096c9421a3ac8a945767ad17b1492'
CONTRACT='a7359175cc5bb7d8e1a462ae1121b01a8d00cdacd8871fa89b3957f441e96e09'
SCORER='ba6ce8d53a3cab9e856660e8f31483a7e13457b6ac3a93aa450943240aa087c8'
ARTIFACTS=('report','plan','scores','audit','campaign','restore','correction','correction_audit')
TENANT=re.compile(r'[A-Za-z0-9_-]{1,128}\Z')
CASE=re.compile(r'[a-f0-9]{24}\Z')


def digest(raw):return hashlib.sha256(raw).hexdigest()


def same_number(actual,expected):
    finite(actual,0,2**53-1)
    if abs(actual-expected)>1e-8:raise ValueError('Engine evidence arithmetic differs')


def timestamp(value):
    if not isinstance(value,str):raise ValueError('Timezone-aware evidence timestamp required')
    result=datetime.fromisoformat(value)
    if result.utcoffset() is None:raise ValueError('Timezone-aware evidence timestamp required')
    return result


def distribution(values):
    # Same declared linear interpolation; recomputed from bounded records,
    # independently of the report's precomputed quantiles.
    ordered=sorted(values)
    def percentile(p):
        position=(len(ordered)-1)*p;lower,upper=math.floor(position),math.ceil(position)
        return round(ordered[lower]+(ordered[upper]-ordered[lower])*(position-lower),6)
    return {'samples':len(values),'p50':percentile(.5),'p95':percentile(.95),'p99':percentile(.99),
        'max':round(max(ordered),6),'mean':round(sum(values)/len(values),6)}


def score_records(raw,report):
    if not raw.endswith(b'\n'):raise ValueError('Closed complete engine score journal required')
    lines=raw.splitlines()
    if len(lines)!=21140 or any(len(line)>32768 for line in lines):raise ValueError('Full bounded unique intent/terminal journal required')
    intents={};complete={}
    for line in lines:
        row=protocol_json(line)
        if not isinstance(row,dict):raise ValueError('Engine journal object required')
        event=row.get('event');record=row if event=='inference.intent' else row.get('record')
        if not isinstance(record,dict) or not isinstance(record.get('id'),str) or not CASE.fullmatch(record['id']):
            raise ValueError('Official public case ID required')
        key=record['id'];at=timestamp(row.get('at'))
        if not timestamp(report.get('measurement_started_at'))<=at<=timestamp(report.get('measurement_ended_at')):
            raise ValueError('Journal timestamp lies outside measured window')
        if event=='inference.intent':
            if key in intents:raise ValueError('Duplicate inference intent')
            if not isinstance(record.get('request_sha256'),str) or not HEX.fullmatch(record['request_sha256']):raise ValueError('Request commitment required')
            count=integer(record.get('prompt_tokens'),1,18312);amount=((count+128+15)//16)*16
            if record.get('reservation_tokens')!=amount:raise ValueError('Exact-token reservation differs')
            intents[key]=record
        elif event=='inference.terminal':
            if key not in intents or key in complete or record.get('status')!='completed':raise ValueError('Unique completed engine response required')
            prompt=integer(record.get('prompt_tokens'),1,18312);integer(record.get('completion_tokens'),0,128)
            if (integer(record.get('prompt_count'),1,18312)!=prompt or prompt!=intents[key]['prompt_tokens']
                or record.get('reservation_tokens')!=intents[key]['reservation_tokens']):raise ValueError('Returned tokenizer/usage differs')
            exact=integer(record.get('exact_match'),0,1);f1=finite(record.get('f1'),0,1)
            valid=record.get('answer_contract_valid')
            # Validate the ORIGINAL grade without silently changing history.
            # Official empty-token normalization can yield EM1/F10; the
            # mandatory correction below enforces format validity first.
            if type(valid) is not bool:raise ValueError('Answer contract validity required')
            if record.get('finish_reason') not in ('stop','length'):raise ValueError('Terminal finish reason required')
            for name in ('prediction_sha256','scored_answer_sha256','response_bytes_sha256'):
                if not isinstance(record.get(name),str) or not HEX.fullmatch(record[name]):raise ValueError('Response commitment required')
            for name in ('completion_seconds','http_service_seconds','tokenize_seconds','admission_wait_seconds'):
                finite(record.get(name),0,3601)
            if record['completion_seconds']+1e-6<sum(record[k] for k in ('http_service_seconds','tokenize_seconds','admission_wait_seconds')):
                raise ValueError('Engine latency intervals contradict')
            complete[key]=record
        else:raise ValueError('Unknown engine journal event')
    if len(complete)!=10570 or set(intents)!=set(complete) or digest(canonical(sorted(complete)))!=IDS:
        raise ValueError('Complete official case-ID commitment differs')
    records=list(complete.values());exact=sum(x['exact_match'] for x in records)
    em=100*exact/10570;f1=100*sum(x['f1'] for x in records)/10570
    same_number(report.get('exact_match_percent'),em);same_number(report.get('f1_percent'),f1)
    for name,expected in [('known_prompt_tokens',sum(x['prompt_tokens'] for x in records)),
        ('known_completion_tokens',sum(x['completion_tokens'] for x in records)),
        ('invalid_answer_contracts',sum(not x['answer_contract_valid'] for x in records))]:
        if type(report.get(name)) is not int or report[name]!=expected:raise ValueError('Engine usage/format count differs')
    from collections import Counter
    if report.get('finish_reasons')!=dict(Counter(x['finish_reason'] for x in records)):raise ValueError('Finish counts differ')
    for name,field in [('http_service_seconds','http_service_seconds'),('offered_cohort_completion_seconds','completion_seconds')]:
        if report.get(name)!=distribution([x[field] for x in records]):raise ValueError('Pooled engine latency differs')
    return exact


def evaluation(entry,blobs):
    data={name:protocol_json(raw) for name,raw in blobs.items() if name!='scores'}
    r,plan,audit,parent,restore=(data[name] for name in ('report','plan','audit','campaign','restore'))
    if any(not isinstance(x,dict) for x in data.values()):raise ValueError('Engine release objects required')
    if (r.get('protocol')!='batchforge.engine-squad.v1' or r.get('dataset_sha256')!=SQUAD_DEV_SHA256
        or r.get('case_manifest_sha256')!=CASESET or r.get('selected_ids_sha256')!=IDS
        or r.get('answer_contract')!='json-span-v1' or r.get('answer_contract_sha256')!=CONTRACT
        or r.get('scorer_sha256')!=SCORER or r.get('model')!=MODEL):raise ValueError('Qualified full public engine protocol required')
    for key,expected in [('complete_public_dev',True),('test_fixture_detected',False),('delivery_passed',True),
        ('evaluation_valid',True),('deadline_met',True),('all_inference_usage_known',True),('forced_output',False),('admission_paused',False)]:
        if r.get(key) is not expected:raise ValueError('Complete valid natural-EOS engine delivery required')
    for key,expected in [('tasks',10570),('predictions',10570),('inference_intents',10570),('unknown_inference_outcomes',0),
        ('missing_predictions',0),('not_dispatched',0),('inference_retries',0),('max_output_tokens',128),('temperature',0),
        ('concurrency',32),('context_limit',18440),('token_budget',164256),('deadline_seconds',3600)]:
        if type(r.get(key)) is not int or r[key]!=expected:raise ValueError('Pinned engine delivery/profile counters required')
    if r.get('quality_gate')!={'minimum_exact_match_percent':70,'minimum_f1_percent':80}:raise ValueError('Original quality thresholds required')
    if any(r.get(k)!=v for k,v in plan.items()) or set(plan)-r.keys():raise ValueError('Engine plan differs from measured report')
    for key in ('runtime_fingerprint','client_source_sha256','workload_sha256','references_sha256'):
        value=r.get(key)
        if not isinstance(value,str) or not (re.fullmatch('sha256:[a-f0-9]{64}',value) if key=='runtime_fingerprint' else HEX.fullmatch(value)):
            raise ValueError('Source/runtime commitments required')
    seconds=finite(r.get('elapsed_seconds'),.000001,3601)
    if abs((timestamp(r.get('measurement_ended_at'))-timestamp(r.get('measurement_started_at'))).total_seconds()-seconds)>1:
        raise ValueError('Engine measurement timestamps differ')
    integer(r.get('peak_inflight_requests'),1,32);integer(r.get('reservation_peak_tokens'),1,164256)
    exact=score_records(blobs['scores'],r);passed=r['exact_match_percent']>=70 and r['f1_percent']>=80
    if r.get('passed') is not passed or parent.get('quality_passed') is not passed:raise ValueError('Engine gate contradicts scores')
    same_number(r.get('exact_results_per_second'),exact/seconds)
    if (parent.get('phase')!='complete' or parent.get('passed') is not True or parent.get('restoration_passed') is not True
        or parent.get('evaluation_valid') is not True or parent.get('engine_actions') is not False or parent.get('new_cloud_resources') is not False
        or restore.get('phase')!='complete' or restore.get('passed') is not True or restore.get('engine_actions') is not False
        or restore.get('cloud_actions') is not False or restore.get('direct_runner_stopped') is not True or restore.get('errors')!=[]):
        raise ValueError('Closed independent service restoration required')
    if (parent.get('evaluation_report_sha256')!=digest(blobs['report']) or parent.get('restoration_report_sha256')!=digest(blobs['restore'])
        or audit.get('report_sha256')!=digest(blobs['report']) or audit.get('plan_sha256')!=digest(blobs['plan'])
        or audit.get('scores_sha256')!=digest(blobs['scores']) or audit.get('restore_report_sha256')!=digest(blobs['restore'])):
        raise ValueError('Engine audit/campaign commitments differ')
    for key in ('passed','private_raw_bytes_replayed','request_hashes_rebuilt_without_gold',
        'all_returned_usage_and_distribution_arithmetic_matches','actual_restored_service_profiles_verified',
        'original_configuration_bytes_match','protected_engine_gateway_unchanged'):
        if audit.get(key) is not True:raise ValueError('Complete raw replay and restored-profile audit required')
    for key in ('tasks','unique_intents','unique_completed','private_response_count'):
        if type(audit.get(key)) is not int or audit[key]!=10570:raise ValueError('Audit cohort differs')
    if audit.get('quality_passed') is not passed or audit.get('exact_results')!=exact:raise ValueError('Audit quality differs')
    same_number(audit.get('exact_match_percent'),r['exact_match_percent']);same_number(audit.get('f1_percent'),r['f1_percent'])
    if audit.get('operator_commit')!=parent.get('operator_commit') or not re.fullmatch('[a-f0-9]{40}',str(parent.get('operator_commit'))):
        raise ValueError('Operator source commit required')
    outer=finite(parent.get('elapsed_seconds'),seconds,7200)
    if abs((timestamp(parent.get('finished_at'))-timestamp(parent.get('started_at'))).total_seconds()-outer)>1e-6:
        raise ValueError('Controller wall arithmetic differs')
    if not timestamp(parent['started_at'])<=timestamp(r['measurement_started_at'])<=timestamp(r['measurement_ended_at'])<=timestamp(restore['finished_at'])<=timestamp(parent['finished_at']):
        raise ValueError('Nested engine/restoration window differs')
    if not isinstance(r.get('cost'),dict) or not isinstance(parent.get('cost'),dict):raise ValueError('Explicit rate allocations required')
    rate=finite(r['cost'].get('operator_whole_pod_usd_per_hour'),.000001,100000)
    if parent.get('cost',{}).get('operator_whole_pod_usd_per_hour')!=rate:raise ValueError('Allocation rates differ')
    engine_usd=rate*seconds/3600;outer_usd=rate*outer/3600
    same_number(r['cost'].get('window_allocation_usd'),engine_usd);same_number(parent['cost'].get('window_allocation_usd'),outer_usd)
    from scripts.engine_quality_correction import correction
    corrected=correction(blobs['report'],blobs['scores'])
    if data['correction']!=corrected:raise ValueError('Contract correction differs from original journal')
    replay=data['correction_audit']
    if (replay.get('protocol')!='batchforge.engine-contract-correction-audit.v1' or replay.get('passed') is not True
        or replay.get('original_report_sha256')!=digest(blobs['report']) or replay.get('original_scores_sha256')!=digest(blobs['scores'])
        or replay.get('correction_sha256')!=digest(blobs['correction']) or replay.get('policy_source_sha256')!=corrected['policy_source_sha256']
        or replay.get('private_response_count')!=10570 or replay.get('all_raw_responses_replayed') is not True
        or replay.get('original_reference_metric_functions_unchanged') is not True
        or replay.get('original_artifacts_modified') is not False or replay.get('new_inference_requests')!=0):
        raise ValueError('Raw-response replay of corrected policy required')
    exact=corrected['corrected_exact_results'];passed=corrected['corrected_quality_passed']
    return {'id':entry['id'],'object':'engine_quality_evaluation','name':entry['name'],
        'verification_state':'operator_report_replayed','quality_state':'passed' if passed else 'failed',
        'scope':{'kind':'engine_only','batch_ledger_verified':False,'dataset':'SQuAD1.1 complete public dev including prior calibration IDs',
            'dataset_sha256':SQUAD_DEV_SHA256,'case_manifest_sha256':CASESET,'hidden_test':False},
        'model':MODEL,'runtime_fingerprint':r['runtime_fingerprint'],'tasks':10570,'exact_results':exact,
        'metrics':{'exact_match_percent':corrected['corrected_exact_match_percent'],'f1_percent':corrected['corrected_f1_percent'],
            'invalid_answer_contracts':r['invalid_answer_contracts'],'prompt_tokens':r['known_prompt_tokens'],
            'completion_tokens':r['known_completion_tokens'],'exact_results_per_second':exact/seconds},
        'gate':{'minimum_exact_match_percent':70,'minimum_f1_percent':80},
        'latency':{'http_service_seconds':r['http_service_seconds'],'offered_cohort_completion_seconds':r['offered_cohort_completion_seconds']},
        'cost':{'kind':'operator_rate_time_allocation','hourly_rate_usd':rate,'controller_window_seconds':outer,
            'engine_window_seconds':seconds,'controller_window_usd':outer_usd,'nested_engine_window_usd':engine_usd,
            'controller_usd_per_1000_reference_exact_answers':outer_usd/exact*1000 if exact else None,'invoice_verified':False},
        'answer_contract':{'id':'json-span-v1','source_sha256':CONTRACT,'scorer_sha256':SCORER,'client_source_sha256':r['client_source_sha256']},
        'scoring_correction':{'policy':corrected['policy'],'policy_source_sha256':corrected['policy_source_sha256'],
            'changed_cases':len(corrected['changed_cases']),'original_exact_match_percent':r['exact_match_percent'],
            'original_f1_percent':r['f1_percent'],'original_report_preserved':True},
        'post_run_restore':{'verified':True,'finished_at':restore['finished_at'],'scope':'Recorded post-run audit; not current availability or whole-Pod failover'},
        'evidence':{**{name+'_sha256':digest(blobs[name]) for name in ARTIFACTS},'operator_commit':parent['operator_commit']},
        'limitations':['Engine-only results have no Batch acceptance/usage ledger link and do not replace earlier Batch cohorts',
            'Operator-published hash-bound evidence, not an issuer signature or independent hardware attestation',
            'Raw-response replay uses the frozen grader, not a second independent scoring implementation',
            'Public dev includes prior calibration IDs; not hidden testing or a contamination audit',
            'HTTP service and offered-cohort completion latency are separate; neither is Batch durable-acceptance latency',
            'Engine cost is nested inside controller cost; never add the windows. Operator allocation excludes unverified extra charges and is not invoice or marginal cost',
            'Reference-exact answers are not business-quality certification; one Pod remains one compute failure domain']}


class EngineEvidenceCatalogue:
    def __init__(self,path):
        path=Path(path).absolute();parent=path.parent.stat()
        import os
        if path.parent.is_symlink() or parent.st_uid!=os.geteuid() or parent.st_mode&0o022:raise ValueError('Owned non-writable release directory required')
        data=protocol_json(private_release_bytes(path,65536))
        if (not isinstance(data,dict) or set(data)!={'protocol','evaluations'} or data['protocol']!='batchforge.engine-quality-catalogue.v1'
            or not isinstance(data['evaluations'],list) or len(data['evaluations'])>8):raise ValueError('Bounded engine catalogue required')
        self.records=[];identities=set();total=0
        for entry in data['evaluations']:
            if (not isinstance(entry,dict) or set(entry)!={'id','name','tenant_id',*ARTIFACTS}
                or not isinstance(entry['id'],str) or not EID.fullmatch(entry['id']) or entry['id'] in identities
                or not isinstance(entry['name'],str) or not 1<=len(entry['name'])<=128
                or not isinstance(entry['tenant_id'],str) or not TENANT.fullmatch(entry['tenant_id'])):
                raise ValueError('Unique named engine evaluation and explicit tenant audience required')
            identities.add(entry['id']);blobs={}
            for name in ARTIFACTS:
                pin=entry[name]
                if (not isinstance(pin,dict) or set(pin)!={'file','sha256'} or not isinstance(pin['file'],str)
                    or not re.fullmatch('[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}\\.(json|jsonl)',pin['file'])
                    or not isinstance(pin['sha256'],str) or not HEX.fullmatch(pin['sha256'])):raise ValueError('Pinned release basename required')
                raw=private_release_bytes(path.parent/pin['file'],16*1024**2 if name=='scores' else 1024**2)
                total+=len(raw)
                if total>64*1024**2 or digest(raw)!=pin['sha256']:raise ValueError('Engine release hash or aggregate bound differs')
                blobs[name]=raw
            self.records.append((entry['tenant_id'],evaluation(entry,blobs)))

    def visible(self,tenant):return [deepcopy(view) for audience,view in self.records if audience==tenant]
