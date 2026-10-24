"""New-tenant scope, intent durability and isolated real-PG verification guards."""
import json
import os
from datetime import datetime,timezone
from decimal import Decimal
from pathlib import Path
import secrets

import httpx
import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
import pytest

from backend.migrate import migrate_database
from ops.ha.history_maintenance import Journal,atomic_report
from ops.ha import verify_history_retirement as verify


def fixture_bytes(**overrides):
    rows=[]
    for n in range(20):
        body={'model':verify.MODEL,'messages':[{'role':'user','content':'public test QA'}],'max_tokens':96}
        body.update(overrides)
        rows.append({'custom_id':'qa-'+str(n),'method':'POST','url':'/v1/chat/completions','body':body})
    return b''.join((json.dumps(r)+'\n').encode() for r in rows)


def test_exact_natural_fixture_validates_without_reporting_prompt_text():
    raw=fixture_bytes()
    result=verify.validate_fixture(raw,verify.sha(raw))
    assert result=={'sha256':verify.sha(raw),'bytes':len(raw),'requests':20,'model':verify.MODEL}
    assert 'public test QA' not in json.dumps(result)


@pytest.mark.parametrize('change',[{'model':'wrong'},{'ignore_eos':True},{'min_tokens':1},{'max_tokens':257},{'stream':True}])
def test_unsupported_fixture_refused_before_mutations(change):
    raw=fixture_bytes(**change)
    with pytest.raises(verify.VerificationFailure):verify.validate_fixture(raw,verify.sha(raw))


def test_wrong_fixture_checksum_or_count_is_refused():
    raw=fixture_bytes()
    with pytest.raises(verify.VerificationFailure):verify.validate_fixture(raw,'0'*64)
    raw=b'\n'.join(raw.splitlines()[:19])+b'\n'
    with pytest.raises(verify.VerificationFailure):verify.validate_fixture(raw,verify.sha(raw))


@pytest.mark.parametrize('tenant',['default','existing-tenant','bfverify_wrong','bfverify_'+'a'*31])
def test_no_existing_tenant_name_can_reach_owner_connection(tenant):
    class ForbiddenConnection:
        def transaction(self):raise AssertionError('No SQL permitted')
    with pytest.raises(verify.VerificationFailure,match='isolated_generated_tenant'):
        verify.create_tenant(ForbiddenConnection(),tenant,'private-new-key-'*3,datetime.now(timezone.utc))


def test_decimal_metadata_is_exact_and_nonfinite_values_fail_closed():
    assert verify.json_metadata({'charged':Decimal('12345678901234567890'),'when':datetime(2026,1,1,tzinfo=timezone.utc)})=={
        'charged':12345678901234567890,'when':'2026-01-01T00:00:00+00:00'}
    for value in ('NaN','Infinity','1.5'):
        with pytest.raises(verify.VerificationFailure):verify.json_metadata(Decimal(value))


@pytest.fixture
def prepared(tmp_path):
    manifest=tmp_path/'services.json';manifest.write_text('{}');manifest.chmod(0o600)
    env=tmp_path/'runtime.env';env.write_text('PLACEHOLDER=private-value\n');env.chmod(0o600)
    service={'role':'api','name':'a','env_file':str(env)}
    identity={'pid':1,'start_ticks':'2','cwd':'owned'}
    key='private-generated-tenant-key-never-report'
    plan={'phase':'prepared','tenant_id':'bfverify_'+secrets.token_hex(16),'operation':'unit-operation',
        'tenant_key_sha256':verify.sha(key.encode()),'binding':{'source_sha256':verify.source_hashes(),
            'manifest_path':str(manifest),'manifest_sha256':verify.sha(manifest.read_bytes()),
            'environment_sha256':{str(env):verify.sha(env.read_bytes())},
            'service_identities':{'api-a':identity}}}
    journal=Journal(tmp_path/'operation');journal.save(plan)
    return plan,journal,key,service,identity


def actor(prepared,http):
    plan,journal,key,service,identity=prepared
    return verify.Verification(journal=journal,prepared=plan,runtime_dsn='never-connect-runtime',
        owner_dsn='never-connect-owner',fixture=fixture_bytes(),tenant_key=key,
        services=[service],identity=lambda s:identity,s3_values={},bucket='existing',http=http)


def test_http_intent_is_fsynced_before_post_and_credentials_never_enter_metadata(prepared):
    plan,journal,key,_,_=prepared
    class Http:
        def post(self,path,**kwargs):
            report=journal.load()
            assert report['phase']=='upload_intent' and report['pending_action']=='upload'
            assert key not in journal.path.read_text()+json.dumps(report)
            return httpx.Response(200,json={'id':'file_test'})
    v=actor(prepared,Http())
    assert v.post('upload','/v1/files').status_code==200
    v.confirmed('upload',input_file_id='file_test')
    assert journal.load()['pending_action'] is None


def test_unknown_post_outcome_is_not_retried_or_auto_resumed(prepared,monkeypatch):
    calls=[]
    class Http:
        def post(self,path,**kwargs):calls.append(path);raise httpx.ReadTimeout('private transport detail')
    v=actor(prepared,Http())
    with pytest.raises(httpx.ReadTimeout):v.post('first','/v1/batches')
    assert calls==['/v1/batches'] and v.journal.load()['phase']=='first_intent'
    monkeypatch.setattr(verify.psycopg,'connect',lambda *a,**k:(_ for _ in ()).throw(AssertionError('No reconnect')))
    with pytest.raises(verify.VerificationFailure,match='prepared_once_no_uncertain_resume'):v.run()
    assert calls==['/v1/batches']


@pytest.mark.parametrize('drift',['source','environment','manifest','service'])
def test_guard_drift_refuses_before_any_http(prepared,monkeypatch,drift):
    class Http:
        def post(self,*args,**kwargs):raise AssertionError('No HTTP after drift')
    v=actor(prepared,Http())
    if drift=='source':monkeypatch.setattr(verify,'source_hashes',lambda:{'changed':'0'*64})
    elif drift=='environment':Path(v.services[0]['env_file']).write_text('CHANGED=1\n')
    elif drift=='manifest':Path(v.report['binding']['manifest_path']).write_text('changed')
    else:v.identity=lambda s:{'pid':999}
    with pytest.raises(verify.VerificationFailure):v.post('first','/v1/batches')


@pytest.fixture
def database():
    source=os.getenv('TEST_DATABASE_URL')
    if not source:pytest.skip('Requires isolated real PostgreSQL')
    name='bf_verify_history_'+secrets.token_hex(6)
    with psycopg.connect(source,autocommit=True) as conn:
        conn.execute(sql.SQL('CREATE DATABASE {} TEMPLATE template0').format(sql.Identifier(name)))
    dsn=make_conninfo(source,dbname=name)
    try:
        migrate_database(dsn,'only-disposable-bootstrap-key',target_version=11)
        yield dsn
    finally:
        with psycopg.connect(source,autocommit=True) as conn:
            conn.execute(sql.SQL('DROP DATABASE {} WITH(FORCE)').format(sql.Identifier(name)))


def test_real_new_tenant_policy_is_exact_and_default_rows_are_untouched(database):
    with psycopg.connect(database,autocommit=True) as owner:
        verify.validate_schema(owner)
        before=verify.default_protection(owner)
        tenant='bfverify_'+secrets.token_hex(16);key='private-test-tenant-'+secrets.token_hex(20)
        cutoff=verify.one(owner,'SELECT clock_timestamp() AS now')['now']
        verify.create_tenant(owner,tenant,key,cutoff)
        assert verify.default_protection(owner)==before
        limits=verify.one(owner,'SELECT max_retained_tasks,max_payload_bytes,max_response_bytes,max_error_bytes FROM tenant_retention_limits WHERE tenant_id=%s',(tenant,))
        assert limits=={'max_retained_tasks':20,'max_payload_bytes':8*1024*1024,'max_response_bytes':65536,'max_error_bytes':32768}
        policy=verify.one(owner,'SELECT enabled,eligible_created_after,min_age_seconds,replay_ttl_seconds FROM tenant_history_retirement_policy WHERE tenant_id=%s',(tenant,))
        assert policy=={'enabled':True,'eligible_created_after':cutoff,'min_age_seconds':60,'replay_ttl_seconds':60}
        with pytest.raises(verify.VerificationFailure,match='never_reuse'):
            verify.create_tenant(owner,tenant,key,cutoff)
        assert verify.default_protection(owner)==before
        assert all(v==0 for v in verify.global_invariants(owner).values())


def test_accepted_once_requires_all_twenty_real_first_attempts():
    state=dict(status='completed',total_count=20,completed_count=20,pending_count=0,running_count=0,
        failed_count=0,cancelled_count=0,tasks=20,accepted=20,duplicate_accepted=0,repeated_attempts=0)
    assert verify.accepted_once(state)
    for field,value in [('accepted',19),('duplicate_accepted',1),('repeated_attempts',1),('failed_count',1),('test_only_responses',1)]:
        assert not verify.accepted_once({**state,field:value})


def real_output_rows():
    expected={};rows=[]
    for n in range(20):
        custom='qa-'+str(n);tid='task-'+str(n)
        expected[custom]={'id':tid,'prompt_tokens':12+n,'completion_tokens':6}
        rows.append({'id':tid,'custom_id':custom,'error':None,'response':{'status_code':200,'request_id':tid,
            'body':{'model':verify.MODEL,'usage':{'prompt_tokens':12+n,'completion_tokens':6,'total_tokens':18+n},
                'choices':[{'message':{'role':'assistant','content':'Private real answer'},'finish_reason':'stop'}]}}})
    return rows,expected


def test_real_output_validates_ids_model_and_exact_live_usage_without_reporting_text():
    rows,expected=real_output_rows()
    assert verify.validate_output(b''.join((json.dumps(row)+'\n').encode() for row in rows),expected) is None


@pytest.mark.parametrize('simulated',[False,True])
def test_download_wiring_requires_real_output_before_confirming_baseline(prepared,simulated):
    rows,expected=real_output_rows()
    if simulated:
        for row in rows:row['response']['body']['batchforge_test_only']=True
    raw=b''.join((json.dumps(row)+'\n').encode() for row in rows)
    client=httpx.Client(base_url='http://private.test',transport=httpx.MockTransport(
        lambda request:httpx.Response(200,content=raw)))
    v=actor(prepared,client)
    try:
        if simulated:
            with pytest.raises(verify.VerificationFailure,match='real_model_output'):
                v.download('original','file-private',expected)
            assert v.journal.load()['pending_action']=='original'
            assert 'downloads' not in v.journal.load()
        else:
            result=v.download('original','file-private',expected)
            assert result=={'bytes':len(raw),'lines':20,'sha256':verify.sha(raw)}
            assert v.journal.load()['pending_action'] is None
            assert 'Private real answer' not in v.journal.path.read_text()+json.dumps(v.journal.load())
        assert (v.journal.directory/'original.jsonl').stat().st_mode&0o777==0o600
    finally:client.close()


@pytest.mark.parametrize('mutation',['fake_engine','duplicate','wrong_id','wrong_usage','bool_usage','wrong_model','error','malformed'])
def test_twenty_lines_cannot_substitute_for_twenty_real_outputs(mutation):
    rows,expected=real_output_rows()
    if mutation=='fake_engine':
        for row in rows:row['response']['body']['batchforge_test_only']=True
    elif mutation=='duplicate':rows[19]=rows[0]
    elif mutation=='wrong_id':rows[0]['id']='foreign-task'
    elif mutation=='wrong_usage':rows[0]['response']['body']['usage']['completion_tokens']=7
    elif mutation=='bool_usage':rows[0]['response']['body']['usage']['completion_tokens']=True
    elif mutation=='wrong_model':rows[0]['response']['body']['model']='synthetic-model'
    elif mutation=='error':rows[0]['error']={'message':'Private error'}
    else:rows[0]={'no':'response'}
    with pytest.raises(verify.VerificationFailure):
        verify.validate_output(b''.join((json.dumps(row)+'\n').encode() for row in rows),expected)


def test_live_output_expectations_reject_purged_or_simulated_responses(monkeypatch):
    rows,expected=real_output_rows()
    live=[dict(custom_id=cid,model=verify.MODEL,response_model=verify.MODEL,has_response=True,test_only=False,**value)
        for cid,value in expected.items()]
    monkeypatch.setattr(verify,'all_rows',lambda *a:live)
    tenant='bfverify_'+'a'*32
    assert verify.live_output_expectations(None,tenant,'batch-new',fixture_bytes())==expected
    live[0]['test_only']=True
    with pytest.raises(verify.VerificationFailure,match='real_model_response'):
        verify.live_output_expectations(None,tenant,'batch-new',fixture_bytes())
    live[0]['test_only']=False;live[0]['has_response']=False
    with pytest.raises(verify.VerificationFailure,match='real_model_response'):
        verify.live_output_expectations(None,tenant,'batch-new',fixture_bytes())


def test_real_pg_live_output_query_rejects_missing_batch_without_payload_reads(database):
    with psycopg.connect(database,autocommit=True) as conn:
        with pytest.raises(verify.VerificationFailure,match='live_output_identity_differs'):
            verify.live_output_expectations(conn,'bfverify_'+'b'*32,'absent',fixture_bytes())
