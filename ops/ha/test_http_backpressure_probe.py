from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import secrets

import httpx
import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.types.json import Jsonb
import pytest

from ops.ha.http_backpressure_probe import refusal_reason, run_once, snapshot


def eligible():
    now=datetime.now(timezone.utc)
    return {'at':now,'tenant_id':'test','input_file_id':'file_existing','cohort_age_seconds':5,
        'all_cohort_after_cutoff':True,'cohort':{'n':100,'identity_hash':'fixed','completed':0,'failed_cancelled':0},
        'selected':{'n':20,'intact':True},'required_payload_bytes':200,'remaining_payload_bytes':100,
        'remaining_task_rows':1000,'ledger':{'retained_tasks':100,'charged_payload_bytes':1000},
        'derived':{'n':100,'bytes':1000},'residue':{'keys':0,'batches':0,'tasks':0,'attempts':0},
        'tenant_totals':{'batches':5,'tasks':100},'duplicate_accepted_tasks':0,
        'archive_horizon_seconds':120,'active_unpurged_archives':0,
        'file':{'kind':'input','admission_tasks':0,'storage_deleted_at':None,
            'storage_expires_at':now+timedelta(hours=1),'has_blob':True},
        'auxiliary':[{'resource':'idempotency','rows':5,'max_rows':10,'bytes':50,'max_bytes':100,'max_row_bytes':10},
                     {'resource':'input_read_leases','rows':0,'max_rows':10,'bytes':0,'max_bytes':0,'max_row_bytes':4096}]}


@pytest.mark.parametrize('change,expected',[
    ({'remaining_payload_bytes':200},'headroom_adequate_no_post'),
    ({'remaining_payload_bytes':201},'headroom_adequate_no_post'),
    ({'archive_horizon_seconds':25},'purge_could_release_budget'),
    ({'archive_horizon_seconds':90},'purge_could_release_budget'),
    ({'active_unpurged_archives':1},'purge_could_release_budget'),
    ({'cohort_age_seconds':61},'outside_fresh_cohort_window'),
    ({'all_cohort_after_cutoff':False},'outside_fresh_cohort_window'),
    ({'remaining_task_rows':19},'task_limit_would_confound_payload_limit'),
    ({'residue':{'keys':1}},'existing_probe_no_replay'),
])
def test_unsafe_snapshot_never_posts(tmp_path,change,expected):
    before=eligible();before.update(change);calls=[]
    doc=run_once('private-dsn','private-key','bench-0123456789ab',datetime.now(timezone.utc)-timedelta(minutes=1),120,3,
        tmp_path/'report.json',snapshot_fn=lambda *args:before,post=lambda *args:calls.append(args))
    assert doc['phase']=='skipped_no_post' and doc['reason']==expected and not calls
    assert 'private-key' not in (tmp_path/'report.json').read_text()


def test_one_actual_http429_shape_no_residue_and_normal_completion_progress(tmp_path):
    before=eligible();after=deepcopy(before);after['cohort']['completed']=3
    snapshots=iter([before,after]);requests=[]
    def post(body,key):
        requests.append((body,key))
        return httpx.Response(429,json={'error':{'code':'retained_payload_limit'}})
    report=tmp_path/'proof.json'
    doc=run_once('private-dsn','private-key','bench-0123456789ab',datetime.now(timezone.utc)-timedelta(minutes=1),120,3,
        report,snapshot_fn=lambda *args:next(snapshots),post=post)
    assert doc['phase']=='passed' and all(doc['checks'].values()) and len(requests)==1
    assert requests[0][0]['input_file_id']=='file_existing'
    assert (tmp_path/'proof.started').exists()
    with pytest.raises(FileExistsError):
        run_once('private-dsn','private-key','bench-0123456789ab',datetime.now(timezone.utc),120,3,
                 report,snapshot_fn=lambda *args:pytest.fail('no replay snapshot'),post=post)
    assert len(requests)==1


def test_transport_uncertainty_is_never_retried_or_claimed429(tmp_path):
    before=eligible();calls=[]
    def post(*args):calls.append(1);raise httpx.ReadTimeout('body could have committed')
    doc=run_once('secret-dsn','secret-key','bench-0123456789ab',datetime.now(timezone.utc),120,3,
        tmp_path/'proof.json',snapshot_fn=lambda *args:before,post=post)
    assert calls==[1] and doc['phase']=='uncertain_no_retry'
    assert not doc['checks']['http429_payload_limit']
    assert 'secret' not in (tmp_path/'proof.json').read_text()


def test_unexpected_success_or_probe_residue_fails_without_retry(tmp_path):
    before=eligible();after=deepcopy(before);after['residue']['keys']=1
    snapshots=iter([before,after]);calls=[]
    def post(*args):calls.append(1);return httpx.Response(200,json={'id':'unexpected'})
    doc=run_once('dsn','key','bench-0123456789ab',datetime.now(timezone.utc),120,3,
        tmp_path/'proof.json',snapshot_fn=lambda *args:next(snapshots),post=post)
    assert doc['phase']=='failed_no_retry' and calls==[1] and not doc['checks']['no_probe_residue']


@pytest.mark.parametrize('status,code',[(200,None),(429,'retained_payload_limit')])
def test_known_http_outcome_persists_if_after_snapshot_fails(tmp_path,status,code):
    snapshots=0;posts=[];report=tmp_path/'proof.json'
    def read(*args):
        nonlocal snapshots
        snapshots+=1
        if snapshots==2:
            on_disk=json.loads(report.read_text())
            assert on_disk['http_status']==status and on_disk['error_code']==code
            assert on_disk['phase'] in {'unexpected_http_no_retry','http429_verification_pending'}
            raise psycopg.OperationalError('verification unavailable')
        return eligible()
    def post(*args):posts.append(1);return httpx.Response(status,json={'error':{'code':code}})
    result=run_once('dsn','secret','bench-0123456789ab',datetime.now(timezone.utc),120,3,
                    report,snapshot_fn=read,post=post)
    assert result['phase']=='verification_failed_no_retry' and posts==[1]
    assert result['http_status']==status and result['error_code']==code
    assert result['http_outcome_phase'] in {'unexpected_http_no_retry','http429_verification_pending'}
    assert 'not cancel server-side' in ' '.join(result['limitations'])


def test_ledger_and_auxiliary_limits_are_required():
    before=eligible();before['derived']['bytes']-=1
    assert refusal_reason(before)=='ledger_not_derived'
    before=eligible();before['auxiliary'][0]['bytes']=100
    assert refusal_reason(before)=='auxiliary_limit_would_confound'
    before=eligible();before['selected']['intact']=False
    assert refusal_reason(before)=='selected_input_not_intact'


def test_real_pg_snapshot_exact_reservation_and_atomic429_rollback(tmp_path):
    source=os.getenv('TEST_DATABASE_URL')
    if not source:pytest.skip('Requires isolated PostgreSQL')
    name='bf_probe_'+secrets.token_hex(8)
    with psycopg.connect(source,autocommit=True) as conn:
        conn.execute(sql.SQL('CREATE DATABASE {} TEMPLATE template0').format(sql.Identifier(name)))
    dsn=make_conninfo(source,dbname=name)
    run='bench-0123456789ab';tenant='probe';body={'model':'test','messages':[{'role':'user','content':'public test'}]}
    try:
        with psycopg.connect(dsn) as conn:
            conn.execute(Path('backend/schema.sql').read_text())
            for n in range(2,11):conn.execute(next(Path('backend/migrations').glob(f'{n:04d}_*.sql')).read_text())
            conn.execute("INSERT INTO tenants(id,name) VALUES(%s,'test')",(tenant,))
            conn.execute("INSERT INTO blob_objects(id,tenant_id,sha256,backend,object_key,bytes,state) VALUES('object',%s,%s,'local','test/key',1,'ready')",(tenant,'a'*64))
            for i in range(5):
                conn.execute("INSERT INTO files(id,tenant_id,filename,kind,blob_id,storage_expires_at) VALUES(%s,%s,'test','input','object',now()+interval '1 hour')",(f'file{i}',tenant))
                conn.execute("INSERT INTO batches(id,tenant_id,input_file_id,model,status,total_count,pending_count,metadata) VALUES(%s,%s,%s,'test','in_progress',20,20,%s)",
                    (f'batch{i}',tenant,f'file{i}',Jsonb({'benchmark_run':run})))
                for j in range(20):
                    conn.execute("INSERT INTO tasks(id,tenant_id,batch_id,custom_id,model,body,status,max_attempts) VALUES(%s,%s,%s,%s,'test',%s,'pending',3)",
                        (f'task{i}_{j}',tenant,f'batch{i}',str(j),Jsonb(body)))
            conn.execute('UPDATE tenant_retention_limits SET max_payload_bytes=(SELECT charged_payload_bytes+10 FROM tenant_retention_usage WHERE tenant_id=%s) WHERE tenant_id=%s',(tenant,tenant))
        cutoff=datetime.now(timezone.utc)-timedelta(minutes=1)
        before=snapshot(dsn,run,'retention-proof-'+run,cutoff,120,3)
        assert before['selected']['n']==20 and before['remaining_payload_bytes']==10
        assert before['required_payload_bytes']==20*(1048576+4*32768+len(json.dumps(body,separators=(', ',': ')).encode()))
        assert refusal_reason(before) is None
        posted=[]
        def post(payload,key):
            posted.append(1)
            with pytest.raises(psycopg.Error) as raised:
                with psycopg.connect(dsn) as conn:
                    conn.execute("INSERT INTO idempotency(tenant_id,scope,key,body_hash) VALUES(%s,'/v1/batches',%s,'probe')",(tenant,key))
                    conn.execute("INSERT INTO batches(id,tenant_id,input_file_id,model,status,total_count,pending_count,metadata) VALUES('negative',%s,%s,'test','in_progress',20,20,%s)",
                        (tenant,payload['input_file_id'],Jsonb(payload['metadata'])))
                    conn.execute("INSERT INTO tasks(id,tenant_id,batch_id,custom_id,model,body,status,max_attempts) VALUES('negative-task',%s,'negative','negative','test',%s,'pending',3)",(tenant,Jsonb(body)))
            assert raised.value.sqlstate=='BQ429'
            return httpx.Response(429,json={'error':{'code':'retained_payload_limit'}})
        result=run_once(dsn,'not-logged',run,cutoff,120,3,tmp_path/'proof.json',post=post)
        assert result['phase']=='passed' and posted==[1] and all(result['checks'].values())
    finally:
        with psycopg.connect(source,autocommit=True) as conn:
            conn.execute(sql.SQL('DROP DATABASE {} WITH(FORCE)').format(sql.Identifier(name)))
