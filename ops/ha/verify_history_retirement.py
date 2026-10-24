"""Prepare/execute one isolated production verification cohort; never default.

No POST or owner mutation without --execute and an immutable prepared report.
Uncertain actions are not retried. Raw fixture, output and generated tenant key
remain private on the same host. This controller never fabricates completions.
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import signal
import time

import httpx
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from backend.blob_storage import S3BlobStore
from backend.migrate import MIGRATIONS
from ops.ha.deploy_storage_admission import bound_owner_dsn
from ops.ha.finalize_api_policy import values
from ops.ha.history_maintenance import (Journal, Maintainer, atomic_report,
    canonical_archive_identity, identity_hash, operation_lock, private_file)
from ops.ha.local_pg17_restore import admission_invariants
from ops.ha.set_scheduler_policy import owned_identity
from ops.supervise import ROOT, load_manifest

MODEL='Qwen/Qwen3-235B-A22B-Instruct-2507-FP8'
SOURCES=('ops/ha/verify_history_retirement.py','ops/ha/history_maintenance.py',
 'backend/history_retirement.py','backend/result_archival.py','backend/blob_storage.py',
 'backend/store.py','backend/migrate.py','backend/migrations/0011_history_retirement.sql',
 'ops/ha/deploy_storage_admission.py','ops/ha/local_pg17_restore.py',
 'ops/ha/finalize_api_policy.py','ops/ha/set_scheduler_policy.py','ops/supervise.py')
OUTPUT_LIMIT=8*1024*1024


class VerificationFailure(RuntimeError):pass


def require(condition,reason):
    if not condition:raise VerificationFailure(reason)


def now():return datetime.now(timezone.utc).isoformat()
def sha(raw):return hashlib.sha256(raw).hexdigest()
def source_hashes():return {p:sha((ROOT/p).read_bytes()) for p in SOURCES}


def json_metadata(value):
    if isinstance(value,Decimal):
        require(value.is_finite() and value==value.to_integral_value(),'non_integral_metadata_number')
        return int(value)
    if isinstance(value,datetime):return value.isoformat()
    if isinstance(value,dict):return {k:json_metadata(v) for k,v in value.items()}
    if isinstance(value,(list,tuple)):return [json_metadata(v) for v in value]
    return value


def global_invariants(conn):
    with conn.transaction():
        conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        return admission_invariants(conn,11)


def validate_fixture(raw,expected):
    require(re.fullmatch('[0-9a-f]{64}',expected) is not None and sha(raw)==expected,'fixture_checksum_differs')
    require(0<len(raw)<=4*1024*1024,'fixture_size_invalid')
    rows=[json.loads(line) for line in raw.splitlines() if line.strip()]
    require(len(rows)==20 and len({r.get('custom_id') for r in rows})==20,'exact_twenty_distinct_requests_required')
    for row in rows:
        body=row.get('body',{})
        require(row.get('method')=='POST' and row.get('url')=='/v1/chat/completions'
            and body.get('model')==MODEL and isinstance(body.get('messages'),list)
            and all(isinstance(m.get('content'),str) for m in body['messages'])
            and type(body.get('max_tokens')) is int and 1<=body['max_tokens']<=256
            and not body.get('stream') and not body.get('ignore_eos') and 'min_tokens' not in body,
            'bounded_natural_qwen_qa_fixture_required')
    return {'sha256':expected,'bytes':len(raw),'requests':20,'model':MODEL}


def one(conn,query,args=()):
    with conn.cursor(row_factory=dict_row) as cursor:
        cursor.execute(query,args);return cursor.fetchone()


def all_rows(conn,query,args=()):
    with conn.cursor(row_factory=dict_row) as cursor:
        cursor.execute(query,args);return cursor.fetchall()


def assert_new_tenant(tenant):
    require(re.fullmatch('bfverify_[0-9a-f]{32}',tenant or '') is not None,'isolated_generated_tenant_required')


def create_tenant(conn,tenant,key,cutoff):
    assert_new_tenant(tenant)
    require(len(key)>=32 and cutoff.tzinfo is not None,'private_key_and_db_cutoff_required')
    with conn.transaction():
        conn.execute('SET TRANSACTION ISOLATION LEVEL READ COMMITTED')
        require(one(conn,'SELECT id FROM tenants WHERE id=%s',(tenant,)) is None,'tenant_already_exists_never_reuse')
        conn.execute("INSERT INTO tenants(id,name) VALUES(%s,'Isolated history verification')",(tenant,))
        conn.execute('INSERT INTO api_keys(key_hash,tenant_id) VALUES(%s,%s)',(sha(key.encode()),tenant))
        conn.execute('''UPDATE tenant_retention_limits SET max_retained_tasks=20,max_payload_bytes=8388608,
          max_response_bytes=65536,max_error_bytes=32768 WHERE tenant_id=%s''',(tenant,))
        conn.execute('''INSERT INTO tenant_history_retirement_policy
          (tenant_id,enabled,eligible_created_after,min_age_seconds,replay_ttl_seconds)
          VALUES(%s,true,%s,60,60)''',(tenant,cutoff))


def validate_schema(conn):
    actual=conn.execute('SELECT version,checksum FROM batchforge_schema_migrations ORDER BY version').fetchall()
    require(actual==[(v,digest) for v,_,digest in MIGRATIONS] and actual[-1][0]==11,'exact_schema11_required')


def default_protection(conn):
    with conn.transaction():
        conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        tasks=one(conn,"""SELECT count(*) AS tasks,md5(string_agg(id,',' ORDER BY id)) AS identity_md5,
          coalesce(sum(retention_body_bytes+retention_response_bytes+retention_error_bytes),0) AS charged
          FROM tasks WHERE tenant_id='default'""")
        batch=one(conn,"""SELECT count(*) AS batches,coalesce(sum(total_count),0) AS total,
          coalesce(sum(completed_count),0) AS completed,coalesce(sum(prompt_tokens),0) AS prompt_tokens,
          coalesce(sum(completion_tokens),0) AS completion_tokens FROM batches WHERE tenant_id='default'""")
        usage=one(conn,"SELECT retained_tasks,charged_payload_bytes FROM tenant_retention_usage WHERE tenant_id='default'")
    return {'tasks':tasks,'batches':batch,'ledger':usage}


def counts(conn,tenant):
    assert_new_tenant(tenant)
    return one(conn,'''SELECT (SELECT count(*) FROM batches WHERE tenant_id=%s) AS batches,
      (SELECT count(*) FROM tasks WHERE tenant_id=%s) AS tasks,
      (SELECT count(*) FROM attempts WHERE tenant_id=%s) AS attempts,
      (SELECT count(*) FROM idempotency WHERE tenant_id=%s) AS keys''',(tenant,)*4)


def batch_state(conn,tenant,bid):
    assert_new_tenant(tenant)
    return one(conn,'''SELECT b.id,b.tenant_id,b.status,b.total_count,b.pending_count,b.running_count,
      b.completed_count,b.failed_count,b.cancelled_count,b.prompt_tokens,b.completion_tokens,b.output_file_id,
      b.completed_at,b.created_at,b.history_retired_tasks,b.history_retired_completed,
      b.history_retired_prompt_tokens,b.history_retired_completion_tokens,
      (SELECT count(*) FROM tasks t WHERE t.tenant_id=b.tenant_id AND t.batch_id=b.id) AS tasks,
      (SELECT count(*) FROM tasks t WHERE t.tenant_id=b.tenant_id AND t.batch_id=b.id AND t.retention_payload_cleared) AS cleared,
      (SELECT count(*) FROM attempts a WHERE a.tenant_id=b.tenant_id AND a.batch_id=b.id AND a.status='completed') AS accepted,
      (SELECT count(*) FROM (SELECT task_id FROM attempts a WHERE a.tenant_id=b.tenant_id AND a.batch_id=b.id
        AND a.status='completed' GROUP BY task_id HAVING count(*)<>1) d) AS duplicate_accepted,
      (SELECT count(*) FROM attempts a WHERE a.tenant_id=b.tenant_id AND a.batch_id=b.id AND a.number<>1) AS repeated_attempts,
      (SELECT count(*) FROM tasks t WHERE t.tenant_id=b.tenant_id AND t.batch_id=b.id
        AND t.response ? 'batchforge_test_only') AS test_only_responses,
      (SELECT count(*) FROM result_archive_runs r WHERE r.tenant_id=b.tenant_id AND r.batch_id=b.id
        AND r.state='ready' AND r.purged_at IS NOT NULL) AS ready_purged_archives
      FROM batches b WHERE b.tenant_id=%s AND b.id=%s''',(tenant,bid))


def accepted_once(state):
    return bool(state and state['status']=='completed' and state['total_count']==state['completed_count']==20
      and state['pending_count']==state['running_count']==state['failed_count']==state['cancelled_count']==0
      and state['tasks']==state['accepted']==20 and state['duplicate_accepted']==state['repeated_attempts']==0
      and state.get('test_only_responses',0)==0)


def live_output_expectations(conn,tenant,bid,fixture):
    """Read only identity/usage, never response text; capture before publisher purge."""
    assert_new_tenant(tenant)
    requested={row['custom_id']:row['body']['max_tokens'] for row in
        (json.loads(line) for line in fixture.splitlines() if line.strip())}
    rows=all_rows(conn,'''SELECT id,custom_id,prompt_tokens,completion_tokens,model,
      response IS NOT NULL AS has_response,response->>'model' AS response_model,
      response ? 'batchforge_test_only' AS test_only FROM tasks
      WHERE tenant_id=%s AND batch_id=%s AND status='completed' ''',(tenant,bid))
    require(len(rows)==20 and {row['custom_id'] for row in rows}==set(requested),'live_output_identity_differs')
    expected={}
    for row in rows:
        require(row['has_response'] and not row['test_only'] and row['model']==row['response_model']==MODEL
            and type(row['prompt_tokens']) is int and row['prompt_tokens']>=0
            and type(row['completion_tokens']) is int and 0<=row['completion_tokens']<=requested[row['custom_id']],
            'live_real_model_response_required_before_purge')
        expected[row['custom_id']]={k:row[k] for k in ('id','prompt_tokens','completion_tokens')}
    return expected


def validate_output(raw,expected):
    """Validate bounded private output against immutable live-SQL identity/usage."""
    require(0<len(raw)<=OUTPUT_LIMIT and len(expected)==20,'bounded_real_output_required')
    try:rows=[json.loads(line) for line in raw.splitlines()]
    except (ValueError,UnicodeError):raise VerificationFailure('output_invalid_jsonl') from None
    require(len(rows)==20 and all(isinstance(row,dict) for row in rows),'exact_twenty_real_output_rows_required')
    seen=set()
    for row in rows:
        custom=row.get('custom_id')
        require(isinstance(custom,str) and custom in expected and custom not in seen,'output_custom_identity_differs')
        seen.add(custom);wanted=expected[custom];response=row.get('response')
        require(row.get('id')==wanted['id'] and row.get('error') is None and isinstance(response,dict)
            and type(response.get('status_code')) is int and response['status_code']==200
            and response.get('request_id')==wanted['id'],'successful_real_output_identity_required')
        body=response.get('body')
        require(isinstance(body,dict) and body.get('model')==MODEL and 'batchforge_test_only' not in body,
            'real_model_output_required')
        usage=body.get('usage');choices=body.get('choices')
        require(isinstance(usage,dict) and all(type(usage.get(k)) is int and usage[k]>=0
            for k in ('prompt_tokens','completion_tokens','total_tokens'))
            and usage['prompt_tokens']==wanted['prompt_tokens']
            and usage['completion_tokens']==wanted['completion_tokens']
            and usage['total_tokens']==usage['prompt_tokens']+usage['completion_tokens'],
            'output_usage_differs_from_accepted_task')
        require(isinstance(choices,list) and len(choices)==1 and isinstance(choices[0],dict)
            and isinstance(choices[0].get('message'),dict)
            and isinstance(choices[0]['message'].get('content'),str)
            and bool(choices[0]['message']['content'].strip())
            and choices[0].get('finish_reason') in ('stop','length'),'real_qa_completion_required')
    require(seen==set(expected),'output_custom_identity_differs')


class Verification:
    def __init__(self,*,journal,prepared,runtime_dsn,owner_dsn,fixture,tenant_key,services,identity,
                 s3_values,bucket,http=None,clock=time.monotonic,sleep=time.sleep,stop=None):
        self.journal=journal;self.report=prepared;self.runtime_dsn=runtime_dsn;self.owner_dsn=owner_dsn
        self.fixture=fixture;self.key=tenant_key;self.services=services;self.identity=identity
        self.s3_values=s3_values;self.bucket=bucket;self.clock=clock;self.sleep=sleep;self.stop=stop or (lambda:False)
        self.tenant=prepared['tenant_id'];assert_new_tenant(self.tenant)
        self.http=http or httpx.Client(base_url='http://127.0.0.1:8080',trust_env=False,follow_redirects=False,
            timeout=httpx.Timeout(30),headers={'Authorization':'Bearer '+tenant_key})
        self.private={'tenant_key':tenant_key}
        self.runtime=None;self.owner=None

    def save(self):
        self.report['updated_at']=now();self.journal.save(json_metadata(self.report))

    def intent(self,stage,**metadata):
        self.guard()
        require(not self.stop(),'interrupted_before_action')
        self.report['phase']=stage+'_intent';self.report['pending_action']=stage
        self.journal.append(stage+'_intent',**metadata);self.save()

    def confirmed(self,stage,**metadata):
        self.report['pending_action']=None;self.report['phase']=stage+'_confirmed'
        self.journal.append(stage+'_confirmed',**metadata);self.save()

    def guard(self):
        require(source_hashes()==self.report['binding']['source_sha256'],'verification_source_changed')
        require(sha(private_file(Path(self.report['binding']['manifest_path'])))==self.report['binding']['manifest_sha256'],'manifest_changed')
        for name,digest in self.report['binding']['environment_sha256'].items():
            require(sha(private_file(Path(name)))==digest,'service_environment_changed')
        current={s['role']+'-'+s['name']:self.identity(s) for s in self.services}
        require(current==self.report['binding']['service_identities'],'service_identity_changed')
        assert_new_tenant(self.tenant)

    def post(self,stage,path,**kwargs):
        self.intent(stage)
        began=self.clock();response=self.http.post(path,**kwargs)
        self.report.setdefault('http',{})[stage]={'status':response.status_code,'elapsed_seconds':self.clock()-began}
        return response

    def download(self,stage,fid,expected):
        self.intent(stage)
        target=self.journal.directory/(stage+'.jsonl')
        fd=os.open(target,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
        digest=hashlib.sha256();size=lines=0
        with os.fdopen(fd,'wb') as output,self.http.stream('GET','/v1/files/'+fid+'/content') as response:
            require(response.status_code==200,'result_download_failed')
            for block in response.iter_bytes():
                size+=len(block);require(size<=OUTPUT_LIMIT,'result_download_exceeds_bound')
                digest.update(block);lines+=block.count(b'\n');output.write(block)
            output.flush();os.fsync(output.fileno())
        require(lines==20,'exact_twenty_output_lines_required')
        validate_output(private_file(target,OUTPUT_LIMIT),expected)
        result={'bytes':size,'lines':lines,'sha256':digest.hexdigest()}
        self.report.setdefault('downloads',{})[stage]=result
        self.confirmed(stage,**result);return result

    def wait(self,predicate,timeout,reason):
        began=self.clock()
        while True:
            self.guard();require(not self.stop(),'interrupted_during_observation')
            value=predicate()
            if value:return value
            require(self.clock()-began<timeout,reason)
            self.sleep(2)

    def completed(self,bid,reason):
        def check():
            state=batch_state(self.runtime,self.tenant,bid)
            require(state is not None,'submitted_batch_missing')
            require(state['failed_count']==state['cancelled_count']==0,'real_workload_failed_no_synthetic_repair')
            return state if accepted_once(state) else None
        return self.wait(check,180,reason)

    def overview(self):
        response=self.http.get('/v1/overview');require(response.status_code==200,'tenant_overview_failed')
        data=response.json();return {'stats':data['stats'],'throughput':data['throughput']}

    def submit(self,stage,fid):
        epoch=one(self.runtime,'SELECT floor(extract(epoch FROM clock_timestamp()))::bigint AS n')['n']
        key='bf1.'+str(epoch)+'.'+secrets.token_hex(16)
        self.private[stage+'_key']=key;atomic_report(self.journal.directory/'private.json',self.private)
        response=self.post(stage,'/v1/batches',json={'input_file_id':fid,
            'metadata':{'history_verification':self.report['operation'],'verification_stage':stage}},headers={'Idempotency-Key':key})
        if stage=='negative':
            require(response.status_code==429 and response.json().get('error',{}).get('code')=='retained_task_limit','exact_atomic_task_limit429_required')
            residue=one(self.runtime,'''SELECT
             (SELECT count(*) FROM idempotency WHERE tenant_id=%s AND scope='/v1/batches' AND key=%s) AS keys,
             (SELECT count(*) FROM batches WHERE tenant_id=%s AND metadata->>'verification_stage'='negative') AS batches''',(self.tenant,key,self.tenant))
            require(not any(residue.values()),'negative_admission_left_residue')
            self.confirmed(stage,http_status=429);return key,None
        require(response.status_code==200,'real_batch_submission_failed')
        body=response.json();bid=body.get('id')
        require(isinstance(bid,str) and body.get('input_file_id')==fid,'batch_identity_invalid')
        self.report.setdefault('batches',{})[stage]=bid;self.confirmed(stage,batch_id=bid)
        return key,bid

    async def retire_pages(self,cutoff):
        options={'autocommit':True,'row_factory':dict_row,
            'options':'-c statement_timeout=10000 -c lock_timeout=1000'}
        async with AsyncConnectionPool(self.owner_dsn,min_size=1,max_size=3,kwargs=options,open=False) as pool:
            adapter=S3BlobStore.from_credentials(endpoint_url=self.s3_values['AWS_ENDPOINT_URL_S3'],
                region=self.s3_values['AWS_REGION'],bucket=self.bucket,
                access_key_id=self.s3_values['AWS_ACCESS_KEY_ID'],secret_access_key=self.s3_values['AWS_SECRET_ACCESS_KEY'])
            args=argparse.Namespace(tenant=self.tenant,created_after=cutoff,min_age=60,max_cycles=3,
                execute=True,retire_keys=False,batch_limit=1,task_page_limit=7,key_page_limit=7,
                interval=10,archive_byte_limit=OUTPUT_LIMIT)
            observed=[]
            parent=self
            class ObservedMaintainer(Maintainer):
                async def cycle(inner):
                    parent.guard();require(not parent.stop(),'interrupted_before_retirement_page')
                    result=await super().cycle()
                    observed.append({'before_retained_tasks':result['before']['retained_tasks'],
                        'after_retained_tasks':result['after']['retained_tasks'],
                        'retired_tasks':result['retired_tasks'],
                        'elapsed_seconds':result['elapsed_seconds']})
                    return result
            worker=ObservedMaintainer(pool,args,Journal(self.journal.directory/'maintenance'),adapter)
            result=await worker.run()
        require(result['phase']=='complete' and result['totals']['retired_tasks']==20,'bounded_three_cycle_retirement_incomplete')
        report=worker.journal.path.read_text().splitlines()
        pages=[json.loads(line)['result']['tasks'] for line in report if json.loads(line).get('event')=='page_completed' and json.loads(line).get('result',{}).get('apply')]
        require(pages==[7,7,6] and [(c['before_retained_tasks'],c['after_retained_tasks']) for c in observed]==[(20,13),(13,6),(6,0)],'exact_seven_seven_six_retirement_required')
        return {'pages':pages,'cycles':observed,'retained_task_quota_path':[20,13,6,0],'retired_tasks':20,'retired_attempts':result['totals']['retired_attempts'],
            'archive_verified_bytes':result['totals']['archive_verified_bytes']}

    def run(self):
        require(self.report['phase']=='prepared' and not self.report.get('pending_action'),'prepared_once_no_uncertain_resume')
        try:
            self.runtime=psycopg.connect(self.runtime_dsn,autocommit=True,connect_timeout=10,
                options='-c default_transaction_read_only=on -c statement_timeout=15000')
            self.owner=psycopg.connect(self.owner_dsn,autocommit=True,connect_timeout=10,
                options='-c statement_timeout=15000 -c lock_timeout=1000')
            validate_schema(self.runtime);self.guard()
            require(one(self.owner,"SELECT current_user=pg_get_userbyid(relowner) AS owner FROM pg_class WHERE oid='public.tenant_history_retirement_policy'::regclass")['owner'],'exact_schema_owner_required')
            require(one(self.runtime,"SELECT count(*) AS n FROM tasks WHERE status IN ('pending','running')")['n']==0,'exclusive_drained_operator_window_required')
            require(default_protection(self.runtime)==self.report['protected_default'],'default_changed_since_preparation')
            self.report['global_before']=global_invariants(self.runtime)
            cutoff=one(self.runtime,'SELECT clock_timestamp() AS now')['now']
            self.intent('tenant_create')
            create_tenant(self.owner,self.tenant,self.key,cutoff)
            self.report['cohort_created_after']=cutoff.isoformat();self.confirmed('tenant_create')
            response=self.post('upload','/v1/files',files={'file':('verification20.jsonl',self.fixture)})
            require(response.status_code==200,'fixture_upload_failed')
            fid=response.json().get('id');require(isinstance(fid,str),'uploaded_file_identity_invalid')
            self.report['input_file_id']=fid;self.confirmed('upload',input_file_id=fid)
            first_key,bid=self.submit('first',fid)
            first=self.completed(bid,'real_first_twenty_not_accepted_once')
            first_expected=live_output_expectations(self.runtime,self.tenant,bid,self.fixture)
            self.report['first_usage']={k:first[k] for k in ('prompt_tokens','completion_tokens','accepted','repeated_attempts')}
            self.report['first_retention_ledger']=one(self.runtime,'SELECT retained_tasks,charged_payload_bytes FROM tenant_retention_usage WHERE tenant_id=%s',(self.tenant,))
            require(self.report['first_retention_ledger']['retained_tasks']==20,'first_task_cap_not_full')
            before=counts(self.runtime,self.tenant)
            self.submit('negative',fid)
            require(counts(self.runtime,self.tenant)==before,'negative_admission_changed_tenant_counts')
            original=self.download('original',first['output_file_id'],first_expected)
            self.report['first_overview']=self.overview()
            archived=self.wait(lambda: (s if (s:=batch_state(self.runtime,self.tenant,bid)) and s['ready_purged_archives']==1 and s['cleared']==20 else None),480,'automatic_publisher_did_not_purge_twenty')
            require(accepted_once(archived),'archive_changed_accepted_identity')
            self.report['archived_retention_ledger']=one(self.runtime,'SELECT retained_tasks,charged_payload_bytes FROM tenant_retention_usage WHERE tenant_id=%s',(self.tenant,))
            require(self.report['archived_retention_ledger']=={'retained_tasks':20,'charged_payload_bytes':0},'automatic_purge_did_not_release_payload_only')
            require(self.download('autoarchive',first['output_file_id'],first_expected)==original,'automatic_archive_output_differs')
            self.intent('retirement')
            self.report['retirement']=asyncio.run(self.retire_pages(cutoff));self.confirmed('retirement')
            retired=batch_state(self.runtime,self.tenant,bid)
            self.report['retired_retention_ledger']=one(self.runtime,'SELECT retained_tasks,charged_payload_bytes FROM tenant_retention_usage WHERE tenant_id=%s',(self.tenant,))
            require(self.report['retired_retention_ledger']=={'retained_tasks':0,'charged_payload_bytes':0},'retirement_did_not_release_exact_task_cap')
            require(retired['tasks']==retired['accepted']==0 and retired['history_retired_tasks']==retired['history_retired_completed']==20,'retired_history_counts_incorrect')
            require((retired['prompt_tokens'],retired['completion_tokens'])==(first['prompt_tokens'],first['completion_tokens']),'retirement_changed_usage')
            require(self.overview()==self.report['first_overview'],'retirement_changed_overview_or_utc_metering')
            require(self.download('retired',first['output_file_id'],first_expected)==original,'retired_output_differs')
            self.intent('key_compaction')
            with self.owner.transaction():
                self.owner.execute('SET TRANSACTION ISOLATION LEVEL READ COMMITTED')
                floor=one(self.owner,'SELECT floor(extract(epoch FROM clock_timestamp()))::bigint-60 AS value')['value']
                result=one(self.owner,'SELECT batchforge_retire_idempotency_page(%s,%s,20,true) AS result',(self.tenant,floor))['result']
            require(result.get('apply') and result.get('keys')==1,'resolved_original_key_not_compacted')
            require(one(self.runtime,'SELECT count(*) AS n FROM idempotency WHERE tenant_id=%s AND key=%s',(self.tenant,first_key))['n']==0,'expired_key_still_present')
            self.confirmed('key_compaction',removed_keys=1)
            stable=counts(self.runtime,self.tenant)
            response=self.post('expired_replay','/v1/batches',json={'input_file_id':fid,
                'metadata':{'history_verification':self.report['operation'],'verification_stage':'first'}},headers={'Idempotency-Key':first_key})
            require(response.status_code==410 and response.json().get('error',{}).get('code')=='idempotency_expired','old_key_not_fenced410')
            self.confirmed('expired_replay',http_status=410)
            response=self.post('retired_retry','/v1/batches/'+bid+'/retry')
            require(response.status_code==410 and response.json().get('error',{}).get('code')=='history_retired','retired_retry_not_fenced410')
            self.confirmed('retired_retry',http_status=410)
            require(counts(self.runtime,self.tenant)==stable,'410_fences_created_work')
            _,second_bid=self.submit('second',fid)
            second=self.completed(second_bid,'readmitted_real_twenty_not_accepted_once')
            second_expected=live_output_expectations(self.runtime,self.tenant,second_bid,self.fixture)
            self.download('readmitted',second['output_file_id'],second_expected)
            self.report['second_usage']={k:second[k] for k in ('prompt_tokens','completion_tokens','accepted','repeated_attempts')}
            require(counts(self.runtime,self.tenant)['batches']==2,'unexpected_extra_batches')
            require(default_protection(self.runtime)==self.report['protected_default'],'protected_default_changed')
            self.report['global_after']=global_invariants(self.runtime);self.guard()
            self.report.update(phase='passed',finished_at=now(),pending_action=None,
                checks={'new_tenant_only':True,'real_twenty_accepted_once_twice':True,'atomic_task_cap429':True,
                    'automatic_publisher_archive_purge':True,'exact_7_7_6_retirement':True,'preserved_output_and_metering':True,
                    'physical_key_compaction_and_410':True,'retired_retry410':True,'capacity_re_admission':True,
                    'protected_default_unchanged':True,'global_ledger_invariants_zero':True,'service_identities_unchanged':True})
            self.save();return self.report
        except BaseException as error:
            self.report.update(phase='failed',failed_at=now(),error_type=type(error).__name__,
                reason=str(error) if isinstance(error,VerificationFailure) else None,sqlstate=getattr(error,'sqlstate',None))
            self.save();raise
        finally:
            if self.runtime:self.runtime.close()
            if self.owner:self.owner.close()
            self.http.close()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest',type=Path,required=True);parser.add_argument('--owner-env',type=Path,required=True)
    parser.add_argument('--access-env',type=Path,required=True);parser.add_argument('--runtime-env',type=Path)
    parser.add_argument('--fixture',type=Path,required=True);parser.add_argument('--fixture-sha256',required=True)
    parser.add_argument('--operation',required=True);parser.add_argument('--execute',action='store_true')
    args=parser.parse_args();os.umask(0o077)
    require(re.fullmatch('[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}',args.operation) is not None,'invalid_unique_operation')
    manifest,services,_=load_manifest(args.manifest)
    apis=sorted((s for s in services if s['role']=='api'),key=lambda s:s['name'])
    require([(s['name'],s['port']) for s in apis]==[('a',8082),('b',8083)],'exact_current_api_pair_required')
    if args.runtime_env:require(args.runtime_env==Path(apis[0]['env_file']),'runtime_env_must_be_manifest_api_a')
    envs=[values(private_file(Path(s['env_file']))) for s in apis]
    require(all(e.get('RESULT_ARCHIVE_ENABLED','').lower() in ('true','1') and e.get('RESULT_READERS_ENABLED','').lower() in ('true','1')
       and 120<=int(e.get('RESULT_ARCHIVE_MIN_AGE_SECONDS','0'))<=240 and e.get('BLOB_STORAGE_BACKEND')=='s3' for e in envs),'existing_automatic_s3_publisher_required')
    require(envs[0].get('BLOB_STORAGE_BUCKET')==envs[1].get('BLOB_STORAGE_BUCKET') and envs[0].get('BLOB_STORAGE_BUCKET'),'same_existing_archive_bucket_required')
    owner=bound_owner_dsn(private_file(args.owner_env),[e['DATABASE_URL'] for e in envs])
    raw=private_file(args.fixture,4*1024*1024);fixture=validate_fixture(raw,args.fixture_sha256)
    access=values(private_file(args.access_env))
    storage={k:access.get(k) or envs[0].get(k) for k in ('AWS_ENDPOINT_URL_S3','AWS_REGION','AWS_ACCESS_KEY_ID','AWS_SECRET_ACCESS_KEY')}
    require(all(storage.values()),'existing_private_s3_credentials_required')
    archive=canonical_archive_identity({'backend':'s3','endpoint':storage['AWS_ENDPOINT_URL_S3'],
        'region':storage['AWS_REGION'],'bucket':envs[0]['BLOB_STORAGE_BUCKET'],'prefix':'batchforge'})
    require(all(e.get('AWS_ENDPOINT_URL_S3')==storage['AWS_ENDPOINT_URL_S3'] and e.get('AWS_REGION')==storage['AWS_REGION'] for e in envs),'archive_endpoint_or_region_differs')
    watchdog={'role':'supervisor','name':'primary','env_file':str(ROOT.parent/'supervisor.env')}
    selected=[*services,watchdog]
    identities={s['role']+'-'+s['name']:owned_identity(ROOT,s) for s in selected}
    require(all(identities.values()),'owned_services_required')
    binding={'source_sha256':source_hashes(),'fixture':fixture,'manifest_sha256':sha(private_file(args.manifest)),'manifest_path':str(args.manifest),
        'environment_sha256':{str(Path(s['env_file'])):sha(private_file(Path(s['env_file']))) for s in selected},
        'service_identities':identities,'archive_identity_sha256':identity_hash(archive)}
    journal=Journal(ROOT/'.runtime'/('history-verification-'+args.operation))
    with operation_lock(journal.directory):
        previous=journal.load()
        with psycopg.connect(envs[0]['DATABASE_URL'],autocommit=True,connect_timeout=10,
            options='-c default_transaction_read_only=on -c statement_timeout=15000') as conn:
            validate_schema(conn)
            protected=default_protection(conn)
            database_now=one(conn,'SELECT clock_timestamp() AS now')['now']
            for e in envs:
                require(e.get('MODEL_NAME')==MODEL,'current_qwen235b_api_model_required')
                archive_after=datetime.fromisoformat(e['RESULT_ARCHIVE_AFTER'].replace('Z','+00:00')) if e.get('RESULT_ARCHIVE_AFTER') else None
                require(archive_after is None or archive_after<=database_now,'publisher_cutoff_is_in_future')
                pending=one(conn,'''SELECT count(*) AS n FROM batches b WHERE b.tenant_id='default'
                  AND (%s::timestamptz IS NULL OR b.created_at>=%s::timestamptz)
                  AND NOT EXISTS(SELECT 1 FROM result_archive_runs r WHERE r.tenant_id=b.tenant_id AND r.batch_id=b.id
                    AND r.state='ready' AND r.purged_at IS NOT NULL)''',(archive_after,archive_after))['n']
                require(pending==0,'default_publisher_history_must_finish_before_verification')
            require(one(conn,"SELECT count(*) AS n FROM tasks WHERE status IN ('pending','running')")['n']==0,'drained_operator_window_required')
        if previous:
            require(previous.get('phase')=='prepared' and previous.get('binding')==binding,'operation_not_prepared_or_binding_drift_no_replay')
            prepared=previous;private=json.loads(private_file(journal.directory/'private.json'))
            require(sha(private['tenant_key'].encode())==prepared['tenant_key_sha256'],'private_tenant_key_changed')
        else:
            require(not args.execute,'prepare_before_execute')
            tenant='bfverify_'+secrets.token_hex(16);key=secrets.token_urlsafe(40)
            private={'tenant_key':key};atomic_report(journal.directory/'private.json',private)
            prepared={'version':1,'operation':args.operation,'phase':'prepared','prepared_at':now(),'tenant_id':tenant,
                'tenant_key_sha256':sha(key.encode()),'binding':binding,'protected_default':protected,
                'scope':'Only one freshly generated isolated tenant; no existing tenant/history mutation',
                'limitations':['Exclusive drained private-operator window required','Raw output/key remain private same-Pod',
                    'No synthetic completion or timestamp manipulation','No backup export or new infrastructure']}
            journal.save(json_metadata(prepared))
        if args.execute:
            stop={'value':False}
            for signum in (signal.SIGINT,signal.SIGTERM):signal.signal(signum,lambda *_:stop.__setitem__('value',True))
            Verification(journal=journal,prepared=prepared,runtime_dsn=envs[0]['DATABASE_URL'],owner_dsn=owner,
                fixture=raw,tenant_key=private['tenant_key'],services=selected,identity=lambda s:owned_identity(ROOT,s),
                s3_values=storage,bucket=envs[0]['BLOB_STORAGE_BUCKET'],stop=lambda:stop['value']).run()
    print(json.dumps({'phase':prepared['phase'],'report':str(journal.report_path)}))


if __name__=='__main__':
    try:main()
    except Exception as error:
        print(json.dumps({'ok':False,'error_type':type(error).__name__}),flush=True)
        raise SystemExit(1) from None
