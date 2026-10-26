"""Console and child-receipt security; no GPU or live production credentials."""
from pathlib import Path
import hashlib,json
import httpx,pytest
from agents.app import create_app
from agents.receipts import reconcile,endpoint
from agents.store import AgentError
from backend.config import Settings
from backend.receipts import canonical,DOMAIN


def proof(**changes):
    body={'batch_id':'batch_one','model':'test-model','ledger_state':'consistent_ledger',
          'counts':{'total':1,'completed':1},'checks':{'test_only_consistency':True},
          'accepted_usage':{'prompt_tokens':20,'completion_tokens':10},**changes}
    return {'protocol':'batchforge.receipt.v1','receipt':body,'receipt_sha256':hashlib.sha256(DOMAIN+canonical(body)).hexdigest()}


def test_child_archive_metadata_may_change_checksum_but_not_committed_identity_or_usage():
    run={'id':'run','model':'test-model'};committed={'batch_id':'batch_one','usage':{'prompt_tokens':20,'completion_tokens':10},'receipt_sha256':'a'*64}
    result=reconcile(run,committed,proof(artifacts=[{'id':'file','sha256':'b'*64}]))
    assert result['accepted_usage_reconciled'] and result['committed_receipt_sha256']=='a'*64
    for value in (None,[],{'receipt':[]},proof(model='other'),proof(accepted_usage={'prompt_tokens':21,'completion_tokens':10}),proof(checks={'test_only_consistency':False}),proof(checks=[True])):
        with pytest.raises(AgentError) as exc:reconcile(run,committed,value)
        assert exc.value.code=='agent_child_ledger_drift'


@pytest.mark.parametrize('url',['https://user:secret@example.com','http://outside.example','http://127.0.0.1/path','https://example.com?token=x','file:///root/private'])
def test_child_receipt_proxy_cannot_be_redirected_by_request_or_model(url):
    with pytest.raises(ValueError):endpoint(url)


async def test_console_assets_are_fixed_same_origin_and_readonly_snapshot_refuses_mutations():
    class Store:pass
    async def auth(key):return 'tenant'
    app=create_app(Settings(model_name='test-model'),store=Store(),auth=auth,executor_tenants=['tenant'],read_only=True)
    async with app.router.lifespan_context(app),httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as c:
        r=await c.get('/console/');assert r.status_code==200 and 'Agent workflows' in r.text
        assert "connect-src 'self'" in r.headers['Content-Security-Policy'] and 'no-store' in r.headers['Cache-Control']
        assert (await c.get('/console/core.js')).status_code==200
        assert (await c.get('/console/not-an-asset')).status_code==404
        assert (await c.get('/v1/agent-capabilities')).status_code==401
        caps=(await c.get('/v1/agent-capabilities',headers={'Authorization':'Bearer test'})).json()
        assert not caps['admission_enabled'] and not caps['memory_writes_enabled'] and caps['read_only_snapshot']
        for path,method in [('/v1/agent-runs','POST'),('/v1/agent-runs/any/cancel','POST'),('/v1/agent-memory/default/key','PUT')]:
            assert (await c.request(method,path,headers={'Authorization':'Bearer test'},json={})).status_code==403


async def test_unqualified_schema_request_is_refused_before_store_admission():
    class Store:
        async def create(self,*args,**kwargs):raise AssertionError('Must refuse before creating Agent state')
    async def auth(key):return 'tenant'
    app=create_app(Settings(model_name='test-model'),store=Store(),auth=auth,executor_tenants=['tenant'])
    async with app.router.lifespan_context(app),httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as c:
        r=await c.post('/v1/agent-runs',headers={'Authorization':'Bearer test','Idempotency-Key':'key'},json={'goal':'Compute','model':'test-model','planner_protocol':'schema-v1'})
        assert r.status_code==400 and r.json()['error']['code']=='agent_schema_ingress_not_enabled'


def test_read_only_cli_never_silently_starts_a_mutating_coordinator(monkeypatch):
    from agents import __main__ as cli
    import sys
    monkeypatch.setattr(sys,'argv',['agents','coordinator','--executor-keys','/must-not-read','--read-only'])
    monkeypatch.setattr(cli,'credentials',lambda _:pytest.fail('No credential or database access before refusal'))
    with pytest.raises(SystemExit) as exc:cli.main()
    assert exc.value.code==2
