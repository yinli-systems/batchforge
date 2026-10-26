from functools import lru_cache
from pathlib import Path
import copy,hashlib,json,os
from fastapi.testclient import TestClient
import pytest
from backend.app import create_app
from backend.config import Settings
from backend.store import Store,APIError
from backend.engine_evaluations import EngineEvidenceCatalogue,ARTIFACTS
from scripts.engine_quality_correction import correction

ROOT=Path(__file__).resolve().parents[1]/'docs/benchmarks/platform480-2026-10-25'
BASE=ROOT/'soak480-engine-quality-full-20261026-v1-complete'
CORRECTION=ROOT/'evidence-api-engine-contract-correction-20261026-v1'


@lru_cache
def original():
    paths={'report':BASE/'run/report.json','plan':BASE/'run/plan.json','scores':BASE/'run/scores.jsonl',
        'audit':ROOT/'evidence-api-engine-quality-full-audit-20261026-v1/validation.json',
        'campaign':BASE/'campaign.json','restore':BASE/'recovery-validation.json',
        'correction':CORRECTION/'run/report.json','correction_audit':CORRECTION/'validation.json'}
    return {key:path.read_bytes() for key,path in paths.items()}


def write(tmp,blobs=None,**changes):
    blobs=blobs or original();entry={'id':'engine-squad-full','name':'Full public engine SQuAD','tenant_id':'owner-a'}
    for key,raw in blobs.items():
        name=key+('.jsonl' if key=='scores' else '.json');(tmp/name).write_bytes(raw)
        entry[key]={'file':name,'sha256':hashlib.sha256(raw).hexdigest()}
    entry.update(changes);p=tmp/'catalogue.json'
    p.write_text(json.dumps({'protocol':'batchforge.engine-quality-catalogue.v1','evaluations':[entry]}));return p


def mutate(name,key,value):
    blobs=dict(original());record=json.loads(blobs[name]);record[key]=value;blobs[name]=json.dumps(record).encode();return blobs


def test_actual_full_release_publishes_corrected_quality_for_explicit_owner_only(tmp_path):
    c=EngineEvidenceCatalogue(write(tmp_path));v=c.visible('owner-a')[0]
    assert v['object']=='engine_quality_evaluation' and v['scope']['batch_ledger_verified'] is False
    assert v['metrics']['exact_match_percent']==pytest.approx(81.99621570482498) and v['exact_results']==8667
    assert v['metrics']['f1_percent']==pytest.approx(90.94000661220909) and v['quality_state']=='passed'
    assert v['cost']['controller_window_usd']==pytest.approx(2.2318954736944443)
    assert v['cost']['nested_engine_window_usd']==pytest.approx(2.1355551338282885)
    assert v['cost']['controller_usd_per_1000_reference_exact_answers']==pytest.approx(.2575164963302693)
    assert not v['cost']['invoice_verified'] and v['scoring_correction']['changed_cases']==1
    assert c.visible('owner-b')==[] and 'tenant_id' not in v and 'batch_ids' not in v
    v['metrics']['exact_match_percent']=100
    assert c.visible('owner-a')[0]['metrics']['exact_match_percent']<100


def test_unknown_private_report_fields_are_dropped_even_with_consistent_operator_pins(tmp_path):
    blobs=mutate('report','operator_private_note','never-expose-this');report_sha=hashlib.sha256(blobs['report']).hexdigest()
    for name,key in [('audit','report_sha256'),('campaign','evaluation_report_sha256')]:
        r=json.loads(blobs[name]);r[key]=report_sha;blobs[name]=json.dumps(r).encode()
    c=correction(blobs['report'],blobs['scores']);blobs['correction']=json.dumps(c).encode()
    a=json.loads(blobs['correction_audit']);a.update(original_report_sha256=report_sha,correction_sha256=hashlib.sha256(blobs['correction']).hexdigest());blobs['correction_audit']=json.dumps(a).encode()
    v=EngineEvidenceCatalogue(write(tmp_path,blobs)).visible('owner-a')[0]
    assert 'never-expose' not in json.dumps(v) and '/root/' not in json.dumps(v)


@pytest.mark.parametrize('name,key,value',[
    ('report','tasks',100),('report','test_fixture_detected',True),('report','unknown_inference_outcomes',1),
    ('report','model','fake-model'),('report','dataset_sha256','0'*64),('report','case_manifest_sha256','0'*64),
    ('report','forced_output',True),('report','exact_match_percent',float('nan')),
    ('report','quality_gate',{'minimum_exact_match_percent':50,'minimum_f1_percent':80}),
    ('report','known_completion_tokens',True),('audit','private_raw_bytes_replayed',False),
    ('restore','passed',False),('campaign','quality_passed',False),
    ('correction','policy_source_sha256','0'*64),('correction','corrected_exact_results',8668),
    ('correction_audit','all_raw_responses_replayed',False),
])
def test_partial_forged_scope_wrong_arithmetic_and_unreplayed_correction_refuse_publication(tmp_path,name,key,value):
    with pytest.raises(ValueError):EngineEvidenceCatalogue(write(tmp_path,mutate(name,key,value)))


@pytest.mark.parametrize('variant',['duplicate','partial','unknown_ids','nonzero_unknown','changed_usage'])
def test_closed_journal_cannot_be_relabelled_or_counted_twice(tmp_path,variant):
    blobs=dict(original());raw=blobs['scores']
    if variant=='duplicate':
        lines=raw.splitlines();lines[-1]=lines[0];raw=b'\n'.join(lines)+b'\n'
    elif variant=='partial':raw=raw[:-1]
    elif variant=='unknown_ids':raw=raw.replace(b'56be4db0acb8001400a502ec',b'000000000000000000000000')
    elif variant=='nonzero_unknown':raw=raw.replace(b'"status": "completed"',b'"status": "outcome_unknown"',1)
    elif variant=='changed_usage':raw=raw.replace(b'"prompt_count": 333',b'"prompt_count": true',1)
    blobs['scores']=raw
    with pytest.raises(ValueError):EngineEvidenceCatalogue(write(tmp_path,blobs))


@pytest.mark.parametrize('variant',['without_correction','path','symlink','fifo','writable','duplicate_json','tenant'])
def test_unpinned_or_unsafe_release_files_and_audience_fail_closed(tmp_path,variant):
    p=write(tmp_path)
    if variant=='without_correction':
        x=json.loads(p.read_text());del x['evaluations'][0]['correction'];p.write_text(json.dumps(x))
    elif variant=='path':
        x=json.loads(p.read_text());x['evaluations'][0]['report']['file']='../restore-private.json';p.write_text(json.dumps(x))
    elif variant in ('symlink','fifo'):
        (tmp_path/'report.json').unlink()
        if variant=='symlink':(tmp_path/'report.json').symlink_to(tmp_path/'plan.json')
        else:os.mkfifo(tmp_path/'report.json',0o600)
    elif variant=='writable':(tmp_path/'report.json').chmod(0o666)
    elif variant=='duplicate_json':p.write_text('{"protocol":"bad",'+p.read_text()[1:])
    elif variant=='tenant':
        x=json.loads(p.read_text());x['evaluations'][0]['tenant_id']='';p.write_text(json.dumps(x))
    with pytest.raises((ValueError,OSError)):EngineEvidenceCatalogue(p)


def test_disabled_store_and_foreign_tenant_never_invent_batch_ledger_queries(tmp_path):
    class NoLedger:
        def connection(self):pytest.fail('Engine publication must not fabricate a Batch ledger lookup')
    import asyncio
    async def check():
        off=Store(NoLedger(),Settings());assert (await off.list_engine_evaluations('owner-a'))['data']==[]
        on=Store(NoLedger(),Settings(engine_evidence_catalogue=str(write(tmp_path))),schema_version=11)
        assert len((await on.list_engine_evaluations('owner-a'))['data'])==1
        assert (await on.list_engine_evaluations('owner-b'))['data']==[]
        for store in (off,on):
            with pytest.raises(APIError) as e:await store.get_engine_evaluation('owner-b','engine-squad-full')
            assert e.value.status==404
    asyncio.run(check())


def test_engine_routes_require_auth_keep_tenant_scope_and_hash_exact_response(tmp_path):
    catalogue=EngineEvidenceCatalogue(write(tmp_path))
    class Fake:
        async def tenant(self,key):return key
        async def list_engine_evaluations(self,tenant):return {'object':'list','data':catalogue.visible(tenant)}
        async def get_engine_evaluation(self,tenant,eid):
            for v in catalogue.visible(tenant):
                if v['id']==eid:return v
            raise APIError(404,'Engine evaluation not found','not_found')
    app=create_app(Settings());app.state.store=Fake();client=TestClient(app)
    assert client.get('/v1/engine-evaluations').status_code==401
    assert client.get('/v1/engine-evaluations/engine-squad-full').status_code==401
    owner={'Authorization':'Bearer owner-a'};other={'Authorization':'Bearer owner-b'}
    assert client.get('/v1/engine-evaluations',headers=other).json()['data']==[]
    assert client.get('/v1/engine-evaluations/engine-squad-full',headers=other).status_code==404
    r=client.get('/v1/engine-evaluations/engine-squad-full',headers=owner)
    assert r.status_code==200 and r.json()['scope']['batch_ledger_verified'] is False
    assert r.headers['Cache-Control']=='no-store' and r.headers['X-BatchForge-Evidence-SHA256']==hashlib.sha256(r.content).hexdigest()


def test_release_cannot_be_accidentally_served_by_static_frontend(tmp_path):
    with pytest.raises(ValueError,match='outside the static'):
        Settings(frontend_dir=str(tmp_path),engine_evidence_catalogue=str(tmp_path/'nested/catalogue.json'))


def test_operator_package_validates_before_writing_and_never_overwrites_an_existing_release(tmp_path):
    from ops.package_engine_evidence import package
    source=tmp_path/'source';source.mkdir(mode=0o700);write(source)
    paths={k:source/(k+('.jsonl' if k=='scores' else '.json')) for k in ARTIFACTS}
    dest=tmp_path/'release';r=package(paths,dest,tenant='owner-a',eid='full-engine',name='Full engine')
    assert r['passed'] and r['new_inference_requests']==0 and r['database_changes'] is False
    before=(dest/'catalogue.json').read_bytes()
    with pytest.raises(FileExistsError):package(paths,dest,tenant='owner-a',eid='full-engine',name='Replacement')
    assert (dest/'catalogue.json').read_bytes()==before
    (source/'report.json').write_text('{}');bad=tmp_path/'invalid'
    with pytest.raises(ValueError):package(paths,bad,tenant='owner-a',eid='full-engine',name='Bad')
    assert not bad.exists()
