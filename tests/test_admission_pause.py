"""The operator's drain gate must close every path that can enqueue new work."""
from fastapi.testclient import TestClient

from backend.app import create_app
from backend.config import Settings


class Store:
    def __init__(self):self.enqueued=[]
    async def tenant(self,key):return 'tenant'
    async def create(self,*args):self.enqueued.append(args);return {'id':'created'}
    async def retry(self,*args):self.enqueued.append(args);return {'id':'retried'}
    async def get_batch(self,tenant,bid):return {'id':bid,'status':'in_progress'}
    async def cancel(self,tenant,bid):return {'id':bid,'status':'cancelled'}
    async def heartbeat(self,*args):return {'ok':True}


def client(enabled):
    app=create_app(Settings(batch_submission_enabled=enabled,batchforge_worker_token='w'*32))
    app.state.store=Store()
    app.state.worker_tokens=('w'*32,)
    # No lifespan: this unit test supplies a fake store and never opens a DB.
    return TestClient(app),app.state.store


def test_paused_admission_rejects_create_and_retry_before_any_enqueue():
    c,store=client(False)
    headers={'Authorization':'Bearer tenant-key','Idempotency-Key':'stable'}
    for path,body in [('/v1/batches',{'input_file_id':'file_existing'}),('/v1/batches/existing/retry',None)]:
        response=c.post(path,headers=headers,json=body)
        assert response.status_code==503 and response.headers['Retry-After']=='5'
        assert response.json()['error']['code']=='admission_paused'
    assert not store.enqueued
    assert c.post('/v1/batches',json={'input_file_id':'file_existing'}).status_code==401
    assert c.get('/v1/batches/existing',headers=headers).status_code==200
    assert c.post('/v1/batches/existing/cancel',headers=headers).status_code==200
    assert c.get('/v1/models',headers=headers).status_code==200
    assert c.post('/internal/tasks/task/heartbeat',headers={'Authorization':'Bearer '+'w'*32},
                  json={'worker_id':'worker','attempt_token':'attempt-token'}).status_code==200


def test_normal_admission_stays_enabled_by_default():
    assert Settings().batch_submission_enabled is True
    c,store=client(True)
    headers={'Authorization':'Bearer tenant-key'}
    assert c.post('/v1/batches',headers=headers,json={'input_file_id':'file_existing'}).status_code==200
    assert c.post('/v1/batches/existing/retry',headers=headers).status_code==200
    assert len(store.enqueued)==2
