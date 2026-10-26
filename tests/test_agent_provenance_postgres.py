"""Actual native PostgreSQL plus deterministic model adapter; no GPU evidence."""
import json
import pytest
from psycopg.types.json import Jsonb
from test_agent_postgres import store,A,B,DSN,TestBatch,expire_test_lease
from test_agent_workflows_postgres import claim_parent
from test_agent_provenance import graph
from test_agent_retrieval import registry
from agents.contracts import canonical,digest
from agents.retrieval import Registry
from agents.runtime import Coordinator
from agents.workflows import Workflow,WorkflowStore
from agents.provenance import resolve

pytestmark=pytest.mark.skipif(not DSN,reason='Actual isolated PostgreSQL required')


class NativeModel(TestBatch):
    """CPU scripted adapter, not model quality or real Batch acceptance."""
    def __init__(self,citation,*,retrieval=False,crash_relay=False):
        super().__init__([]);self.citation=citation;self.retrieval=retrieval;self.crash_relay=crash_relay;self.posts=0
    async def find_batch(self,run,state):return self.batches.get(state['child_key'])
    async def create(self,run,state):
        self.posts+=1;bid=await super().create(run,state)
        if self.crash_relay and run['policy']['parent_node']=='relay':self.crash_relay=False;raise RuntimeError('Lost Batch ACK')
        return bid
    async def poll(self,run,state):
        if run['policy']['parent_node']=='source' and state['step']==1:
            decision={'type':'tool','name':'retrieval.search' if self.retrieval else 'memory.get',
                'arguments':{'query':'Mars mission'} if self.retrieval else {'key':'project'}}
        else:decision={'type':'final','answer':'2030' if self.retrieval else 'NOVA-9071','citations':[self.citation]}
        return {'content':canonical(decision).decode(),'usage':{'prompt_tokens':100,'completion_tokens':20},'receipt_sha256':'a'*64}


async def prepare(store,tmp_path,retrieval=False,**kw):
    provider=None;sid=''
    if retrieval:
        path=registry(tmp_path);v=json.loads(path.read_bytes());v['snapshots'][0]['tenant_id']=A;path.write_bytes(canonical(v));provider=Registry(path)
        sid=provider.visible(A)[0]['id'];cite=provider.require(A,'facts',sid).search('Mars mission')['matches'][0]['citation']
    else:
        m=await store.put_memory(A,'facts','project','Project code: NOVA-9071.',expected_version=0)
        cite=f"memory:facts:project:v1:{m['sha256']}"
    wf=WorkflowStore(store);parent=await wf.create(A,'lineage-native',Workflow.model_validate(graph(sid)))
    return parent,NativeModel(cite,retrieval=retrieval,**kw),provider


async def pump(store,parent,batch,provider,*,until=None,turns=160):
    wf=WorkflowStore(store)
    for _ in range(turns):
        value=await store.get(A,parent['id'])
        if value['status'] in ('completed','failed','cancelled'):return value
        parent_claim=await claim_parent(store)
        if parent_claim:await wf.reconcile(parent_claim)
        if until:
            value=await store.get(A,parent['id'])
            if await until(value):return value
        await Coordinator(store,batch,owner='lineage-restarted',tenants=[A],retrieval=provider).tick()
    raise AssertionError('Bounded workflow did not finish')


@pytest.mark.parametrize('retrieval',[False,True])
async def test_three_real_durable_roles_forward_only_verified_sources_across_restarts(store,tmp_path,retrieval):
    parent,batch,provider=await prepare(store,tmp_path,retrieval)
    done=await pump(store,parent,batch,provider);assert done['status']=='completed'
    reader=await store.get(A,done['state']['nodes']['reader']['run_id'])
    proof=await resolve(store,reader,provider,tenant=A)
    assert proof==reader['result']['dependency_evidence'] and len(proof['sources'])==1
    assert {n['node'] for n in proof['sources'][0]['nodes']}=={'source','relay'}
    assert reader['model_calls']==1 and reader['tool_calls']==0 and reader['policy']['allowed_tools']==[]
    assert len(batch.batches)==batch.posts==4 and done['result']['usage']['generated_tokens']==80
    assert proof['sources'][0]['source']['kind']==('retrieval' if retrieval else 'memory')
    assert 'VERIFIED WORKFLOW SOURCE COMMITS' in reader['state']['request']['messages'][-1]['content']
    if not retrieval:
        await store.put_memory(A,'facts','project','Current code: OTHER.',expected_version=1)
        again=await resolve(store,reader,None,tenant=A)
        assert again==proof and proof['sources'][0]['source']['version']==1 # committed history, not latest mutable text
    for node in done['state']['nodes'].values():await store.events(A,node['run_id'])


@pytest.mark.parametrize('mutation',['run_id','result_sha256','event_head_sha256'])
async def test_changed_activation_projection_fails_before_new_model_io(store,tmp_path,mutation):
    parent,batch,provider=await prepare(store,tmp_path)
    async def source_done(v):return v['state']['nodes']['relay']['status']=='queued'
    value=await pump(store,parent,batch,provider,until=source_done)
    rid=value['state']['nodes']['relay']['run_id'];relay=await store.get(A,rid)
    state=relay['state'];state['workflow_inputs'][0][mutation]='f'*64
    async with store.pool.connection() as db:await db.execute('UPDATE bf_agents.runs SET state=%s WHERE id=%s',(Jsonb(state),rid))
    done=await pump(store,parent,batch,provider)
    assert done['status']=='failed' and len(batch.batches)==2
    assert (await store.get(A,rid))['result']['code']=='workflow_source_provenance_invalid'
    assert (await store.get(A,value['state']['nodes']['source']['run_id']))['generated_tokens']==40


async def test_registry_revocation_before_child_planning_preserves_upstream_meter_and_blocks_dispatch(store,tmp_path):
    parent,batch,provider=await prepare(store,tmp_path,True)
    async def ready(v):return v['state']['nodes']['relay']['status']=='queued'
    value=await pump(store,parent,batch,provider,until=ready)
    done=await pump(store,parent,batch,Registry())
    assert done['status']=='failed' and len(batch.batches)==2
    assert (await store.get(A,value['state']['nodes']['relay']['run_id']))['model_calls']==0


async def test_revoked_source_after_lost_batch_ack_is_reconciled_readonly_and_usage_stays_charged(store,tmp_path):
    parent,batch,provider=await prepare(store,tmp_path,True,crash_relay=True)
    with pytest.raises(RuntimeError,match='Lost Batch ACK'):await pump(store,parent,batch,provider)
    p=await store.get(A,parent['id']);rid=p['state']['nodes']['relay']['run_id'];await expire_test_lease(store,rid)
    assert (await store.get(A,rid))['state']['batch_attempted']
    posts=batch.posts;done=await pump(store,parent,batch,Registry())
    relay=await store.get(A,rid)
    assert done['status']=='failed' and batch.posts==posts==3 and len(batch.batches)==3
    assert relay['generated_tokens']==20 and relay['model_calls']==1
    assert relay['result']['code']=='workflow_source_authorization_lost'
    assert any(e['kind']=='model.reconciled' for e in (await store.events(A,rid))['events'])


async def test_unresolved_revoked_post_never_creates_another_batch_or_claims_zero_cost(store,tmp_path):
    parent,batch,provider=await prepare(store,tmp_path,True,crash_relay=True)
    with pytest.raises(RuntimeError):await pump(store,parent,batch,provider)
    p=await store.get(A,parent['id']);rid=p['state']['nodes']['relay']['run_id'];await expire_test_lease(store,rid)
    async def unknown(run,state):return None
    batch.find_batch=unknown;posts=batch.posts
    await Coordinator(store,batch,owner='revoked-unknown',tenants=[A],retrieval=Registry()).tick()
    relay=await store.get(A,rid)
    assert relay['status']=='waiting_approval' and relay['model_calls']==1 and batch.posts==posts==3
    assert relay['result']['code']=='batch_acceptance_unresolved_after_revocation'


@pytest.mark.parametrize('retrieval',[False,True])
async def test_actual_api_and_node_client_independently_verify_native_source_graph(store,tmp_path,retrieval):
    import httpx,subprocess,shutil,os
    from pathlib import Path
    from agents.app import create_app
    from backend.config import Settings
    parent,batch,provider=await prepare(store,tmp_path,retrieval)
    done=await pump(store,parent,batch,provider);assert done['status']=='completed'
    rid=done['state']['nodes']['reader']['run_id'];snapshots={};pages={}
    async def auth(k):return A if k=='a' else B
    app=create_app(Settings(model_name='test'),store=store,auth=auth,executor_tenants=[A],retrieval=provider)
    async with app.router.lifespan_context(app),httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as c:
        headers={'Authorization':'Bearer a'}
        response=await c.get('/v1/agent-runs/'+rid+'/sources',headers=headers);assert response.status_code==200,response.text
        for identity in [done['id'],*[x['run_id'] for x in done['state']['nodes'].values()]]:
            x=await c.get('/v1/agent-runs/'+identity+'/snapshot',headers=headers);assert x.status_code==200;snapshots[identity]=x.json();pages[identity]={};after=-1
            while True:
                r=await c.get(f'/v1/agent-runs/{identity}/events?after={after}&limit=100',headers=headers);assert r.status_code==200;p=r.json();pages[identity][str(after)]=p
                if not p['has_more']:break
                after=p['events'][-1]['revision']
        assert (await c.get('/v1/agent-runs/'+rid+'/sources',headers={'Authorization':'Bearer b'})).status_code==404
    fixture=tmp_path/'client-native-private.json';fixture.write_text(json.dumps({'tenant':A,'current':snapshots[rid],'response':response.json(),'snapshots':snapshots,'pages':pages}));fixture.chmod(0o600)
    node=shutil.which('node');assert node,'Node is part of required native CI toolchain'
    root=Path(__file__).resolve().parents[1]
    tested=subprocess.run([node,str(root/'ops/verify_workflow_sources.mjs'),str(root),str(fixture)],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,timeout=30)
    assert tested.returncode==0,tested.stderr
    result=json.loads(tested.stdout);assert result['passed'] and result['adversarial_refusals']==5


async def test_batch_lookup_reconciles_real_api_commit_without_creating_a_second_task(store):
    import hashlib,httpx,psycopg
    from agents.batch import BatchAdapter
    from backend.app import create_app
    from backend.config import Settings
    from agents.runtime import DeliveryError
    key='test-only-source-reconciliation-key'
    with psycopg.connect(DSN,autocommit=True) as db:db.execute('INSERT INTO api_keys(key_hash,tenant_id) VALUES(%s,%s) ON CONFLICT DO NOTHING',(hashlib.sha256(key.encode()).hexdigest(),A))
    app=create_app(Settings(database_url=DSN,model_name='test-model'))
    request={'model':'test-model','messages':[{'role':'user','content':'test-only synthetic data'}],'max_tokens':32}
    run={'id':'agrun_'+'f'*32,'tenant_id':A,'model':'test-model'}
    state={'request':request,'request_sha256':digest(request),'step':1,'child_key':'source-reconcile-native'}
    async with app.router.lifespan_context(app),httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://127.0.0.1:8080') as c:
        adapter=BatchAdapter(store.pool,base_url='http://127.0.0.1:8080',tenant_keys={A:key},client=c)
        state['file_id']=await adapter.upload(run,state)
        assert await adapter.find_batch(run,state) is None
        bid=await adapter.create(run,state)
        assert await adapter.find_batch(run,state)==bid
        assert await adapter.find_batch({**run,'tenant_id':B},state) is None
        with pytest.raises(DeliveryError):await adapter.find_batch(run,{**state,'file_id':'file_different'})
        async with store.pool.connection() as db:
            row=await (await db.execute('SELECT count(*) n FROM public.tasks WHERE tenant_id=%s AND batch_id=%s',(A,bid))).fetchone()
        assert row['n']==1


async def test_fanin_combines_memory_and_retrieval_without_merging_their_authorities(store,tmp_path):
    path=registry(tmp_path);v=json.loads(path.read_bytes());v['snapshots'][0]['tenant_id']=A;path.write_bytes(canonical(v));provider=Registry(path);sid=provider.visible(A)[0]['id']
    retrieval_cite=provider.require(A,'facts',sid).search('Mars mission')['matches'][0]['citation']
    m=await store.put_memory(A,'facts','project','Project code: NOVA-9071.',expected_version=0);memory_cite=f"memory:facts:project:v1:{m['sha256']}"
    g=graph(sid);g['allowed_tools']=['memory.get','retrieval.search'];g['nodes'][1]['depends_on'].append('memory')
    g['nodes'].insert(1,{'id':'memory','goal':'Read the project code','model':'test','allowed_tools':['memory.get'],'require_citations':True,'planner_protocol':'schema-v1'})
    parent=await WorkflowStore(store).create(A,'mixed-native',Workflow.model_validate(g))
    class Mixed(NativeModel):
        async def poll(self,run,state):
            role=run['policy']['parent_node']
            if role=='memory' and state['step']==1:decision={'type':'tool','name':'memory.get','arguments':{'key':'project'}}
            elif role=='memory':decision={'type':'final','answer':'NOVA-9071','citations':[memory_cite]}
            elif role=='source':return await super().poll(run,state)
            else:decision={'type':'final','answer':'NOVA-9071 | 2030','citations':[memory_cite,retrieval_cite]}
            return {'content':canonical(decision).decode(),'usage':{'prompt_tokens':100,'completion_tokens':20},'receipt_sha256':'a'*64}
    batch=Mixed(retrieval_cite,retrieval=True);done=await pump(store,parent,batch,provider)
    assert done['status']=='completed'
    reader=await store.get(A,done['state']['nodes']['reader']['run_id']);proof=await resolve(store,reader,provider,tenant=A)
    assert len(proof['sources'])==2 and {s['source']['kind'] for s in proof['sources']}=={'memory','retrieval'}
    assert reader['tool_calls']==0 and batch.posts==len(batch.batches)==6
