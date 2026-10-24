"""Real HTTP state machine with scheduling, SQL recovery and external artifacts."""
import json
import os
from pathlib import Path
import uuid

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from fastapi.testclient import TestClient
import pytest

from backend.app import create_app
from backend.config import Settings
from backend.migrate import migrate_database

DSN = os.getenv('TEST_DATABASE_URL')
pytestmark = pytest.mark.skipif(not DSN, reason='Requires disposable real PostgreSQL')
KEY = 'integration-v6-public-test-key'
TOKEN = 'integration-v6-worker-test-token'
PUBLIC = {'Authorization': f'Bearer {KEY}'}
WORKER = {'Authorization': f'Bearer {TOKEN}'}
PROFILE = dict(profile_id='test-hardware', runtime_fingerprint='test-runtime-v1',
    prefill_tokens_per_second=1000, decode_tokens_per_second=100, hourly_cost_usd=1,
    calibration_label='test-only-profile', cost_source='test-only-cost')


@pytest.fixture
def client(tmp_path):
    name = 'bf_v6_' + uuid.uuid4().hex
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute(sql.SQL('CREATE DATABASE {} TEMPLATE template0').format(sql.Identifier(name)))
    values = conninfo_to_dict(DSN)
    values['dbname'] = name
    database = make_conninfo(**values)
    try:
        migrate_database(database, KEY)
        app = create_app(Settings(database_url=database, batchforge_worker_token=TOKEN,
            model_name='test-model', scheduler_policy='cost_aware', sql_completion_enabled=True,
            sql_recovery_enabled=True, blob_storage_backend='local', blob_storage_root=str(tmp_path/'blobs'),
            max_queued_tasks_per_tenant=3, max_stored_input_bytes_per_tenant=4096))
        with TestClient(app) as http:
            http.database = database
            yield http
    finally:
        with psycopg.connect(DSN, autocommit=True) as conn:
            conn.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(name)))


def upload(client, count=1):
    content = ''.join(json.dumps(dict(custom_id=str(i), method='POST', url='/v1/chat/completions',
        body=dict(model='test-model',messages=[dict(role='user',content='test')],max_completion_tokens=32)))+'\n' for i in range(count))
    return client.post('/v1/files', headers=PUBLIC, files={'file': ('input.jsonl',content)})


def register(client):
    response = client.post('/internal/workers/register', headers=WORKER, json=dict(worker_id='worker',
        name='worker',model='test-model',concurrency=3,metadata={'scheduling_profile':PROFILE,'ready_engine_count':1}))
    assert response.status_code == 200, response.text


def claim(client):
    response = client.post('/internal/tasks/claim', headers=WORKER, json=dict(worker_id='worker',model='test-model',limit=3))
    assert response.status_code == 200, response.text
    return response.json()['tasks']


def test_upload_admission_claim_completion_and_idempotent_download(client):
    result = upload(client, 3)
    assert result.status_code == 200, result.text
    fid = result.json()['id']
    assert upload(client).status_code == 429
    assert client.get(f'/v1/files/{fid}/content',headers=PUBLIC).status_code == 200
    with psycopg.connect(client.database) as conn:
        assert conn.execute('SELECT content IS NULL,blob_id IS NOT NULL FROM files WHERE id=%s',(fid,)).fetchone() == (True,True)
    body = {'input_file_id':fid}
    headers = {**PUBLIC,'Idempotency-Key':'once'}
    response = client.post('/v1/batches',headers=headers,json=body)
    assert response.status_code == 200, response.text
    bid = response.json()['id']
    assert client.post('/v1/batches',headers=headers,json=body).json()['id'] == bid
    register(client)
    tasks = claim(client)
    assert len(tasks) == 3
    for task in tasks:
        response = client.post(f"/internal/tasks/{task['id']}/complete",headers=WORKER,json=dict(
            worker_id='worker',attempt_token=task['attempt_token'],response={'choices':[{'message':{'content':'ok'}}],
            'usage':{'prompt_tokens':8,'completion_tokens':2}}))
        assert response.status_code == 200,response.text
    batch = client.get(f'/v1/batches/{bid}',headers=PUBLIC).json()
    assert batch['status']=='completed' and batch['request_counts']['completed']==3
    output = client.get(f"/v1/files/{batch['output_file_id']}/content",headers=PUBLIC)
    assert len(output.text.splitlines())==3
    with psycopg.connect(client.database) as conn:
        assert conn.execute("SELECT count(*) FROM attempts WHERE scheduling_decision->>'policy'='cost_aware'").fetchone()[0]==3
        assert conn.execute('SELECT min(estimated_output_tokens) FROM tasks').fetchone()[0]==32
        assert conn.execute('SELECT held_tasks FROM tenant_blob_usage').fetchone()[0]==0
    assert upload(client).status_code==200


def test_failed_batch_retry_uses_artifacts_and_sql_failure(client):
    fid=upload(client).json()['id']
    response=client.post('/v1/batches',headers=PUBLIC,json={'input_file_id':fid})
    assert response.status_code==200,response.text
    bid=response.json()['id'];register(client);task=claim(client)[0]
    response=client.post(f"/internal/tasks/{task['id']}/fail",headers=WORKER,json=dict(worker_id='worker',
        attempt_token=task['attempt_token'],error={'code':'test_failure','message':'test'},retryable=False))
    assert response.status_code==200,response.text
    assert client.get(f'/v1/batches/{bid}',headers=PUBLIC).json()['status']=='failed'
    headers={**PUBLIC,'Idempotency-Key':'retry-once'}
    response=client.post(f'/v1/batches/{bid}/retry',headers=headers)
    assert response.status_code==200,response.text
    retried=response.json()
    assert client.post(f'/v1/batches/{bid}/retry',headers=headers).json()['id']==retried['id']
    with psycopg.connect(client.database) as conn:
        assert conn.execute('SELECT content IS NULL,blob_id IS NOT NULL FROM files WHERE id=%s',(retried['input_file_id'],)).fetchone()==(True,True)
        assert conn.execute('SELECT held_tasks FROM tenant_blob_usage').fetchone()[0]==0


def test_missing_profile_is_rejected_before_registration(client):
    response=client.post('/internal/workers/register',headers=WORKER,json=dict(worker_id='worker',name='worker',model='test-model',concurrency=1))
    assert response.status_code==400
    with psycopg.connect(client.database) as conn:
        assert conn.execute('SELECT count(*) FROM workers').fetchone()[0]==0


def test_corrupt_external_input_fails_before_download_headers(client):
    fid=upload(client).json()['id']
    with psycopg.connect(client.database) as conn:
        key=conn.execute('SELECT object_key FROM blob_objects JOIN files ON files.blob_id=blob_objects.id WHERE files.id=%s',(fid,)).fetchone()[0]
    adapter=client.app.state.store.blobs.adapter
    (Path(adapter.root)/key).write_bytes(b'corruption')
    response=client.get(f'/v1/files/{fid}/content',headers=PUBLIC)
    assert response.status_code==503,response.text
    assert response.json()['error']['code']=='storage_unavailable'
    with psycopg.connect(client.database) as conn:
        assert conn.execute('SELECT count(*) FROM blob_read_leases').fetchone()[0]==0
