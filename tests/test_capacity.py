"""Admission headroom is a logical, owner-scoped advisory snapshot."""
from fastapi.testclient import TestClient
from backend.app import create_app
from backend.capacity import capacity_view
from backend.config import Settings


def test_reservations_are_included_and_over_limit_history_never_becomes_negative_headroom():
    row=dict(max_retained_tasks=50000,retained_tasks=50001,max_payload_bytes=320*1024**2,
        charged_payload_bytes=300*1024**2,max_response_bytes=65536,max_error_bytes=32768,
        input_stored_bytes=250*1024**2,input_reserved_bytes=7*1024**2,held_tasks=900,queued_tasks=100)
    result=capacity_view(row,Settings(max_attempts=3))
    assert result['retained_tasks']['available']==0 and result['retained_tasks']['over_limit']
    assert result['input_bytes']['available']==0 and result['input_bytes']['over_limit']
    assert result['task_payload_bytes']['available']==20*1024**2
    assert result['new_task_reservation']['fixed_bytes_per_task']==196608
    assert result['queue_tasks']['used']==100 and result['queue_tasks']['reserved']==900
    assert 'after completion' in result['reservation_release']
    assert result['advisory'] is True and 'not disk' in result['snapshot_scope']


class ScopedStore:
    async def tenant(self,key):return 'owner-'+key
    async def capacity(self,tenant):return {'owner':tenant}
    async def batch_receipt(self,tenant,bid):return {'owner':tenant,'batch':bid}
    async def task_page(self,tenant,bid,limit,after):return {'owner':tenant,'batch':bid,'limit':limit,'after':after}


def test_read_routes_require_tenant_auth_and_forward_only_valid_bounded_pages():
    app=create_app(Settings());app.state.store=ScopedStore();client=TestClient(app)
    paths=['/v1/capacity','/v1/batches/batch/receipt','/v1/batches/batch/tasks']
    for path in paths:
        assert client.get(path).status_code==401
        assert client.get(path,headers={'Authorization':'Bearer one'}).json()['owner']=='owner-one'
        assert client.get(path,headers={'Authorization':'Bearer two'}).json()['owner']=='owner-two'
    h={'Authorization':'Bearer one'}
    for query in ['limit=0','limit=201','after='+'x'*81]:
        assert client.get('/v1/batches/batch/tasks?'+query,headers=h).status_code==422
    assert client.get('/v1/batches/batch/tasks?limit=2&after=cursor',headers=h).json()=={
        'owner':'owner-one','batch':'batch','limit':2,'after':'cursor'}
