import copy
import asyncio
from datetime import datetime,timedelta,timezone
import hashlib
import json
from types import SimpleNamespace
import pytest
from backend.fleet import PROTOCOL,fleet_view,inventory_view
from worker.inventory import collect,selection
from worker.config import Settings
from worker.runtime import Worker
import httpx
from fastapi.testclient import TestClient
from backend.app import create_app
from backend.config import Settings as ApiSettings

NOW=datetime(2026,10,25,15,tzinfo=timezone.utc)


def report():
    return {'protocol':PROTOCOL,'node_id':'a'*64,'observed_at':NOW.isoformat(),
        'devices':[{'id':str(i)*64,'index':i,'name':'NVIDIA H100 80GB HBM3','memory_total_mib':81920,
            'memory_used_mib':74000,'utilization_percent':99,'power_watts':690,'temperature_celsius':60} for i in (0,1)],
        'links':[{'a':0,'b':1,'relation':'NV18'}],'private_secret':'must-not-project'}


def row(inventory=None,**changes):
    return {'id':'worker-one','model':'test-model','status':'online','inventory':inventory or report(),**changes}


def test_multiple_workers_do_not_multiply_physical_gpu_counts_or_infer_free_capacity():
    out=fleet_view([row(),row(id='worker-two')],NOW)
    node=out['data'][0]
    assert node['observed_device_count']==node['fresh_device_count']==2
    assert len(node['reports'])==2 and node['placement_capacity'] is None
    assert 'must-not-project' not in json.dumps(out)


@pytest.mark.parametrize('offset,status',[(61,'online'),(-6,'online'),(0,'offline')])
def test_stale_clock_skew_and_missing_heartbeats_cannot_advertise_fresh_hardware(offset,status):
    out=fleet_view([row(status=status)],NOW+timedelta(seconds=offset))
    assert out['data'][0]['fresh_device_count']==0
    assert out['data'][0]['reports'][0]['inventory_state']=='stale'


def test_conflicting_host_membership_marks_both_nodes_and_never_counts_capacity():
    other=report();other['node_id']='b'*64
    out=fleet_view([row(),row(other,id='worker-two')],NOW)
    assert len(out['data'])==2
    assert all(n['conflicts']==['device_identity_conflict'] and n['placement_capacity'] is None for n in out['data'])


@pytest.mark.parametrize('mutate',[
    lambda r:r['devices'].append(copy.deepcopy(r['devices'][0])),
    lambda r:r['devices'][0].update(name='<script>'),
    lambda r:r.update(observed_at='2026-10-25T15:00:00'),
    lambda r:r['links'].append({'a':0,'b':1,'relation':'NV18'}),
    lambda r:r.update(devices=[None]),
])
def test_malformed_reports_fail_closed_without_raw_metadata_projection(mutate):
    value=report();mutate(value)
    out=fleet_view([row(value)],NOW)
    assert out['data']==[] and out['rejected_reports']==1


def test_unknown_measurement_remains_unknown():
    value=report();value['devices'][0].update(power_watts=float('nan'),memory_used_mib=-1,utilization_percent=True)
    safe=inventory_view(value)['devices'][0]
    assert safe['power_watts'] is safe['memory_used_mib'] is safe['utilization_percent'] is None


def test_fleet_requires_tenant_authentication_and_disables_caching():
    class Fake:
        async def tenant(self,key):return key
        async def fleet(self,tenant):return {'object':'list','data':[]}
    app=create_app(ApiSettings());app.state.store=Fake();client=TestClient(app)
    assert client.get('/v1/fleet').status_code==401
    response=client.get('/v1/fleet',headers={'Authorization':'Bearer owner'})
    assert response.status_code==200 and response.json()['data']==[]
    assert response.headers['Cache-Control']=='no-store'


def test_collector_only_queries_explicit_devices_and_does_not_export_host_identifiers(monkeypatch):
    calls=[]
    def run(args,**kwargs):
        calls.append(args)
        if 'topo' in args:return SimpleNamespace(stdout=b'GPU0 GPU1 CPU Affinity\nGPU0 X NV18 0-3\nGPU1 NV18 X 0-3\n')
        return SimpleNamespace(stdout=b'0, GPU-private0, NVIDIA H100, 81920, 74000, 100, 691, 59\n1, GPU-private1, NVIDIA H100, 81920, 74000, 99, N/A, 58\n')
    monkeypatch.setattr('worker.inventory.subprocess.run',run)
    value=collect('private-host',(0,1))
    assert calls[0][1:3]==['-i','0,1']
    assert value['links']==[{'a':0,'b':1,'relation':'NV18'}]
    assert value['devices'][1]['power_watts'] is None
    assert 'private-host' not in json.dumps(value) and 'GPU-private' not in json.dumps(value)
    assert value['node_id']==hashlib.sha256(b'private-host').hexdigest()
    for invalid in ['0,0','0;shutdown','128','-1','']:
        with pytest.raises(ValueError):selection(invalid)


@pytest.mark.asyncio
async def test_background_inventory_uses_existing_registration_and_shuts_down_without_claims(monkeypatch):
    registrations=[]
    config=Settings(control_url='http://control',worker_token='a'*32,inference_urls=('http://engine',),model='test',
        worker_id='test-worker',name='test',inventory_node_id='node',inventory_gpu_indices=(0,1))
    async def control(request):
        assert request.url.path=='/internal/workers/register'
        registrations.append(json.loads(request.content))
        return httpx.Response(200,json={'worker_id':'test-worker','lease_seconds':90,'heartbeat_interval_seconds':10})
    worker=Worker(config,control_transport=httpx.MockTransport(control))
    monkeypatch.setattr('worker.inventory.collect',lambda node,indices:report())
    loop=asyncio.create_task(worker._inventory_loop())
    try:
        for _ in range(100):
            if worker._gpu_inventory is not None:break
            await asyncio.sleep(.001)
        await worker.register()
        assert registrations[0]['metadata']['gpu_inventory']==report()
        worker.stop.set();await asyncio.wait_for(loop,.1)
    finally:
        loop.cancel();await asyncio.gather(loop,return_exceptions=True);await worker.close()
