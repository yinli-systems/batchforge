"""DAG races and failure boundaries on real isolated PostgreSQL, not production."""
import asyncio,copy,os
import psycopg,pytest
from psycopg.types.json import Jsonb
from test_agent_postgres import store,A,B,DSN,TestBatch
from test_agent_workflows import graph
from agents.store import AgentError,one
from agents.contracts import digest,canonical
from agents.workflows import Workflow,WorkflowStore,WorkflowCoordinator
from agents.runtime import Coordinator

pytestmark=pytest.mark.skipif(not DSN,reason='Actual isolated PostgreSQL required')


async def make(store,**kw):return await WorkflowStore(store).create(A,'graph',Workflow.model_validate(graph(**kw)))


async def claim_parent(store):
    async with store.pool.connection() as db:
        await db.execute("UPDATE bf_agents.runs SET ready_at=clock_timestamp() WHERE policy->>'execution_kind'='workflow' AND status NOT IN ('completed','failed','cancelled')")
    return await store.claim('workflow-test',[A],kind='workflow')


async def complete(store,rid,answer='42'):
    async with store.pool.connection() as db:
        row=await one(db,"SELECT *,clock_timestamp() AS db_now FROM bf_agents.runs WHERE id=%s",(rid,))
    assert not row['state'].get('workflow_blocked')
    # Claim the exact ready child through ordinary eligibility, with other ready
    # children held by a test lease. No fixture bypasses dependency activation.
    held=[]
    while True:
        c=await store.claim('child-test',[A])
        assert c is not None
        if c['id']==rid:break
        held.append(c)
    result=await store.transition(A,rid,c['lease_token'],c['revision'],'test.final',kind='run.completed',state=c['state'],status='completed',
        result={'type':'final','answer':answer,'citations':[],'batch_receipts':[]},model_calls=1,generated_tokens=10)
    for c in held:await store.release_idle(A,c['id'],c['lease_token'],seconds=1)
    return result


async def test_atomic_graph_admission_replay_and_claim_partition(store):
    wf=WorkflowStore(store);spec=Workflow.model_validate(graph())
    runs=await asyncio.gather(*[wf.create(A,'graph',spec) for _ in range(8)])
    assert len({r['id'] for r in runs})==1
    assert await store.claim('agent',[A]) is None
    assert await store.claim('outsider',[B],kind='workflow') is None
    async with store.pool.connection() as db:
        usage=await one(db,'SELECT active_runs,retained_runs FROM bf_agents.usage WHERE tenant_id=%s',(A,))
        children=await (await db.execute("SELECT * FROM bf_agents.runs WHERE policy->>'parent_workflow'=%s",(runs[0]['id'],))).fetchall()
    assert usage=={'active_runs':4,'retained_runs':4} and len(children)==3
    assert all(c['deadline_at'].isoformat()==runs[0]['deadline_at'] and c['state']['workflow_blocked'] for c in children)
    with pytest.raises(AgentError):await wf.create(A,'graph',Workflow.model_validate(graph(max_parallel=1)))
    with pytest.raises(AgentError):await wf.get(B,runs[0]['id'])


@pytest.mark.parametrize('column,limit',[('max_active_runs',3),('max_retained_runs',3)])
async def test_quota_failure_rolls_back_entire_graph_and_events(store,column,limit):
    async with store.pool.connection() as db:await db.execute(f'UPDATE bf_agents.limits SET {column}=%s WHERE tenant_id=%s',(limit,A))
    with pytest.raises(psycopg.Error) as exc:await make(store)
    assert exc.value.sqlstate=='BA429'
    async with store.pool.connection() as db:
        assert (await one(db,'SELECT count(*) n FROM bf_agents.runs'))['n']==0
        assert (await one(db,'SELECT count(*) n FROM bf_agents.events'))['n']==0
        assert (await one(db,'SELECT retained_runs FROM bf_agents.usage WHERE tenant_id=%s',(A,)))['retained_runs']==0


async def test_fanout_join_preserves_dependency_hashes_and_global_usage(store):
    run=await make(store);wf=WorkflowStore(store)
    first=await wf.reconcile(await claim_parent(store))
    assert [first['state']['nodes'][n]['status'] for n in ('left','right','join')]==['queued','queued','blocked']
    ids={n:v['run_id'] for n,v in first['state']['nodes'].items()}
    left=await complete(store,ids['left'],'20')
    middle=await wf.reconcile(await claim_parent(store));assert middle['state']['nodes']['join']['status']=='blocked'
    right=await complete(store,ids['right'],'22')
    ready=await wf.reconcile(await claim_parent(store));assert ready['state']['nodes']['join']['status']=='queued'
    child=await store.get(A,ids['join']);inputs=child['state']['workflow_inputs']
    assert [i['node'] for i in inputs]==['left','right']
    assert inputs[0]['result_sha256']==digest(left['result']) and inputs[1]['event_head_sha256']==right['event_hash']
    await complete(store,ids['join'])
    done=await wf.reconcile(await claim_parent(store))
    assert done['status']=='completed' and done['result']['outputs']['join']['result']['answer']=='42'
    assert done['result']['usage']=={'model_calls':3,'tool_calls':0,'generated_tokens':30}
    assert done['model_calls']==0  # control program does not pretend to infer
    for rid in [run['id'],*ids.values()]:await store.events(A,rid)


async def test_max_parallel_one_and_lost_activation_ack_do_not_create_children_twice(store):
    run=await make(store,max_parallel=1);wf=WorkflowStore(store);old=await claim_parent(store)
    # Commit activation, then discard its response. A new coordinator repeats a
    # reconciliation, preserving all IDs and the single activation operation.
    await wf.reconcile(old)
    again=await wf.reconcile(await claim_parent(store))
    assert sum(v['status']=='queued' for v in again['state']['nodes'].values())==1
    assert again['revision']==old['revision']+2  # one activation + later lease
    with pytest.raises(AgentError):await wf.reconcile(old)
    await complete(store,again['state']['nodes']['left']['run_id'])
    next_run=await wf.reconcile(await claim_parent(store))
    assert next_run['state']['nodes']['right']['status']=='queued'
    async with store.pool.connection() as db:
        assert (await one(db,"SELECT count(*) n FROM bf_agents.runs WHERE policy->>'parent_workflow'=%s",(run['id'],)))['n']==3
        assert (await one(db,"SELECT count(*) n FROM bf_agents.events WHERE kind='workflow.inputs_ready'"))['n']==2


async def test_cancel_race_cannot_activate_after_parent_terminal(store):
    run=await make(store);claimed=await claim_parent(store);wf=WorkflowStore(store)
    results=await asyncio.gather(store.cancel(A,run['id']),wf.reconcile(claimed),return_exceptions=True)
    assert (await wf.get(A,run['id']))['status']=='cancelled'
    async with store.pool.connection() as db:
        rows=await (await db.execute('SELECT * FROM bf_agents.runs WHERE tenant_id=%s',(A,))).fetchall()
        usage=await one(db,'SELECT active_runs,retained_runs FROM bf_agents.usage WHERE tenant_id=%s',(A,))
    assert all(r['status']=='cancelled' and r['lease_token'] is None for r in rows)
    assert usage=={'active_runs':0,'retained_runs':4}
    assert await store.claim('late-agent',[A]) is None
    assert await store.claim('late-workflow',[A],kind='workflow') is None
    for r in rows:await store.events(A,r['id'])


async def test_child_failure_cascades_without_cancelling_completed_sibling(store):
    run=await make(store);wf=WorkflowStore(store);active=await wf.reconcile(await claim_parent(store))
    await complete(store,active['state']['nodes']['left']['run_id'])
    await store.cancel(A,active['state']['nodes']['right']['run_id'])
    done=await wf.reconcile(await claim_parent(store))
    assert done['status']=='failed' and done['result']['code']=='workflow_child_failed'
    assert (await store.get(A,active['state']['nodes']['left']['run_id']))['status']=='completed'
    assert (await store.get(A,active['state']['nodes']['join']['run_id']))['status']=='cancelled'


async def test_expired_parent_lease_rolls_back_activation_and_late_coordinator_is_fenced(store):
    run=await make(store);wf=WorkflowStore(store);old=await claim_parent(store)
    async with store.pool.connection() as db:
        await db.execute("UPDATE bf_agents.runs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=%s",(run['id'],))
    with pytest.raises(AgentError):await wf.reconcile(old)
    assert await store.claim('agent',[A]) is None
    next_run=await claim_parent(store);assert next_run['lease_epoch']==2
    await wf.reconcile(next_run)


async def test_lease_expiring_during_activation_rolls_back_child_events(store,monkeypatch):
    run=await make(store);wf=WorkflowStore(store)
    old=await store.claim('short',[A],kind='workflow',seconds=5)
    event=store.event;delayed=False
    async def delayed_event(*args,**kw):
        nonlocal delayed
        result=await event(*args,**kw)
        if kw['kind']=='workflow.inputs_ready' and not delayed:
            delayed=True;await asyncio.sleep(5.1)
        return result
    monkeypatch.setattr(store,'event',delayed_event)
    with pytest.raises(AgentError) as exc:await wf.reconcile(old)
    assert exc.value.code=='stale_agent_lease'
    assert await store.claim('child',[A]) is None
    async with store.pool.connection() as db:
        assert (await one(db,"SELECT count(*) n FROM bf_agents.events WHERE kind='workflow.inputs_ready'"))['n']==0
    assert all(v['status']=='blocked' for v in (await wf.get(A,run['id']))['state']['nodes'].values())


async def test_deadline_cascades_to_reserved_blocked_children(store):
    # Use a genuinely expired DB-clock deadline in an isolated fixture, without
    # weakening the immutable-identity trigger or touching production policy.
    spec=Workflow.model_validate(graph());wf=WorkflowStore(store)
    async with store.pool.connection() as db,db.transaction():
        await store.tenant_lock(db,A)
        deadline=(await one(db,"SELECT clock_timestamp()-interval '1 second' AS deadline"))['deadline']
        ids={n.id:'agrun_'+os.urandom(16).hex() for n in spec.nodes};rid='agrun_'+os.urandom(16).hex()
        await store.create_locked(db,A,'expired',goal=canonical(spec.model_dump()).decode(),
            model='batchforge/workflow-v1',policy=spec.policy(),rid=rid,deadline=deadline,
            state={'phase':'workflow','nodes':{n:{'run_id':i,'status':'blocked'} for n,i in ids.items()}})
        for n in spec.nodes:
            await store.create_locked(db,A,'expired.'+n.id,goal=n.goal,model=n.model,policy=n.policy(spec,parent=rid,node=n.id),
                rid=ids[n.id],deadline=deadline,state={'phase':'plan','workflow_blocked':True})
    assert await store.claim('deadline',[A],kind='workflow') is None
    assert (await wf.get(A,rid))['status']=='failed'
    for i in ids.values():assert (await store.get(A,i))['status']=='cancelled'
    async with store.pool.connection() as db:
        assert (await one(db,'SELECT active_runs FROM bf_agents.usage WHERE tenant_id=%s',(A,)))['active_runs']==0


async def test_dependency_overflow_fails_without_partial_activation_or_broken_event_chain(store):
    g=graph(max_dependency_bytes=256)
    # First ready sibling can activate before a later join discovers overflow;
    # failure must still cancel that sibling with a continuous event chain.
    g['nodes'].insert(2,{'id':'extra','goal':'Additional branch','model':'test-model','depends_on':['left']})
    g['nodes'][-1]['depends_on'].append('extra')
    spec=Workflow.model_validate(g);run=await WorkflowStore(store).create(A,'graph',spec);wf=WorkflowStore(store)
    active=await wf.reconcile(await claim_parent(store))
    await complete(store,active['state']['nodes']['left']['run_id'],'x'*300)
    failed=await wf.reconcile(await claim_parent(store))
    assert failed['status']=='failed' and failed['result']['code']=='workflow_dependency_context_exceeded'
    for v in failed['state']['nodes'].values():await store.events(A,v['run_id'])


async def test_real_agent_state_machine_consumes_only_activated_nodes(store):
    run=await make(store);wf=WorkflowStore(store)
    batch=TestBatch(['{"type":"final","answer":"42","citations":[]}'])
    for _ in range(90):
        async with store.pool.connection() as db:
            await db.execute("UPDATE bf_agents.runs SET ready_at=clock_timestamp() WHERE status NOT IN ('completed','failed','cancelled')")
        await WorkflowCoordinator(store,owner='wf',tenants=[A]).tick()
        await Coordinator(store,batch,owner='agent',tenants=[A]).tick()
        done=await wf.get(A,run['id'])
        if done['status'] in ('completed','failed'):break
    assert done['status']=='completed' and len(batch.batches)==3 and batch.uploads==3
    join=await store.get(A,done['state']['nodes']['join']['run_id'])
    assert any('UNTRUSTED WORKFLOW RESULTS' in m['content'] for m in join['state']['request']['messages'])
    assert done['result']['usage']['model_calls']==3
