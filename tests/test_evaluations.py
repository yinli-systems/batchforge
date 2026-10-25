import copy
from contextlib import asynccontextmanager
import hashlib
import json
from pathlib import Path

from fastapi.testclient import TestClient
import pytest

from backend.app import create_app
from backend.config import Settings
from backend.evaluations import EvidenceCatalogue,SQUAD_DEV_SHA256
from backend.store import Store,APIError

BIDS=['batch_'+'a'*32,'batch_'+'b'*32]
FP='sha256:'+'f'*64


def report():
    return {'protocol':'squad-1.1-em-f1-v1','evaluation_valid':True,'delivery_passed':True,
        'batch_ids':BIDS,'tasks':2,'predictions':2,'missing':0,'model':'test-model','runtime_fingerprint':FP,
        'dataset_sha256':SQUAD_DEV_SHA256,'scope':'seeded dev subset','source_sha256':'b'*64,'benchmark_sha256':'c'*64,
        'exact_match_percent':50.,'f1_percent':75.,'quality_gate':{'minimum_exact_match':.7,'minimum_f1':.8},
        'deadline_met':True,'passed':False,'elapsed_seconds':10.,'prompt_tokens':200,'completion_tokens':100,
        'on_time_exact_results_per_second':.1,'extra_private_field':'never-export-this'}


def write_catalogue(tmp_path,data=None,**entry_changes):
    raw=json.dumps(data or report()).encode();(tmp_path/'report.json').write_bytes(raw)
    entry={'id':'qa-test','name':'QA test','report':'report.json','sha256':hashlib.sha256(raw).hexdigest(),**entry_changes}
    path=tmp_path/'catalogue.json';path.write_text(json.dumps({'protocol':'batchforge.quality-catalogue.v1','evaluations':[entry]}))
    return path


def ledger():
    return [{'id':bid,'model':'test-model','status':'completed','runtime_fingerprint':FP,
             'total_count':1,'completed_count':1,'failed_count':0,'cancelled_count':0,
             'prompt_tokens':100,'completion_tokens':50} for bid in BIDS]


def test_valid_delivery_can_fail_quality_and_unknown_fields_never_become_public(tmp_path):
    catalogue=EvidenceCatalogue(write_catalogue(tmp_path))
    row=catalogue.visible(ledger())[0]
    assert row['delivery_passed'] and row['quality_state']=='failed'
    assert row['verification_state']=='batch_ledger_reconciled'
    assert row['metrics']['exact_match_percent']==50
    assert 'never-export-this' not in str(row) and 'batch_ids' not in row
    assert catalogue.visible(ledger(),details=True)[0]['batch_ids']==BIDS
    assert catalogue.visible(ledger()[:1])==[]


@pytest.mark.parametrize('change',[
    {'model':'another-model'},{'runtime_fingerprint':'sha256:'+'e'*64},
    {'completion_tokens':49},{'total_count':2},{'status':'in_progress'},
])
def test_runtime_counter_and_usage_drift_suppress_quality_certification(tmp_path,change):
    catalogue=EvidenceCatalogue(write_catalogue(tmp_path));rows=ledger();rows[0].update(change)
    view=catalogue.visible(rows)[0]
    assert view['quality_state']=='unverified' and view['metrics'] is None
    assert view['verification_state']=='batch_ledger_mismatch'


def test_tampering_path_traversal_and_symlink_are_rejected(tmp_path):
    path=write_catalogue(tmp_path);(tmp_path/'report.json').write_text('{}')
    with pytest.raises(ValueError,match='hash differs'):EvidenceCatalogue(path)
    path=write_catalogue(tmp_path,report='../report.json')
    with pytest.raises(ValueError,match='basename'):EvidenceCatalogue(path)
    path=write_catalogue(tmp_path);(tmp_path/'report.json').rename(tmp_path/'moved.json')
    (tmp_path/'report.json').symlink_to(tmp_path/'moved.json')
    with pytest.raises((OSError,ValueError)):EvidenceCatalogue(path)


@pytest.mark.parametrize('change',[
    {'passed':True},{'exact_match_percent':float('nan')},{'predictions':True},
    {'on_time_exact_results_per_second':2},{'batch_ids':[BIDS[0],BIDS[0]]},
    {'evaluation_valid':False},
    {'exact_match_percent':33.3},
    {'dataset_sha256':'d'*64},
])
def test_inconsistent_or_incomplete_published_report_fails_closed(tmp_path,change):
    data=report();data.update(change)
    with pytest.raises(ValueError):EvidenceCatalogue(write_catalogue(tmp_path,data))


class Connection:
    def __init__(self):self.calls=[];self.rows=[]
    async def execute(self,query,params):
        self.calls.append((query,params));self.rows=ledger() if params[0]=='owner-a' else ledger()[:1];return self
    async def fetchall(self):return copy.deepcopy(self.rows)


class Pool:
    def __init__(self):self.conn=Connection()
    @asynccontextmanager
    async def connection(self):yield self.conn


@pytest.mark.asyncio
async def test_store_binds_owner_and_requires_every_batch_in_the_cohort(tmp_path):
    pool=Pool();store=Store(pool,Settings(quality_evidence_catalogue=str(write_catalogue(tmp_path))),schema_version=11)
    assert len((await store.list_evaluations('owner-a'))['data'])==1
    assert (await store.list_evaluations('owner-b'))['data']==[]
    with pytest.raises(APIError) as hidden:await store.get_evaluation('owner-b','qa-test')
    assert hidden.value.status==404
    assert pool.conn.calls[-1][1][0]=='owner-b'
    assert 'tenant_id=%s' in pool.conn.calls[-1][0] and 'SELECT *' not in pool.conn.calls[-1][0]
    assert (await store.get_evaluation('owner-a','qa-test'))['batch_ids']==BIDS


def test_quality_routes_require_auth_and_forward_the_same_tenant():
    class Fake:
        async def tenant(self,key):return key
        async def list_evaluations(self,tenant):return {'owner':tenant}
        async def get_evaluation(self,tenant,eid):return {'owner':tenant,'id':eid}
    app=create_app(Settings());app.state.store=Fake();c=TestClient(app)
    assert c.get('/v1/evaluations').status_code==401
    assert c.get('/v1/evaluations/private').status_code==401
    assert c.get('/v1/evaluations',headers={'Authorization':'Bearer owner-a'}).json()['owner']=='owner-a'
    response=c.get('/v1/evaluations/qa',headers={'Authorization':'Bearer owner-b'})
    assert response.json()=={'owner':'owner-b','id':'qa'}
    assert response.headers['X-BatchForge-Evidence-SHA256']==hashlib.sha256(response.content).hexdigest()
    assert response.headers['Cache-Control']=='no-store'


def test_fifo_cannot_block_release_file_validation(tmp_path):
    import os
    path=write_catalogue(tmp_path)
    (tmp_path/'report.json').unlink()
    os.mkfifo(tmp_path/'report.json',0o600)
    with pytest.raises(ValueError,match='release file'):EvidenceCatalogue(path)
