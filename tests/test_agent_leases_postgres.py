"""Real native PostgreSQL lease renewal/fencing across slow and uncertain I/O."""
import asyncio
import pytest
from test_agent_postgres import store,A,DSN,make,TestBatch,advance_to_terminal,expire_test_lease
from agents.runtime import Coordinator
from agents.store import AgentError,one
from agents.contracts import Policy

pytestmark=pytest.mark.skipif(not DSN,reason='Actual isolated PostgreSQL required')


async def at_upload(store,batch):
    r=await make(store);c=Coordinator(store,batch,owner='prepare',tenants=[A])
    for _ in range(4):
        r=await store.get(A,r['id'])
        if r['state'].get('phase')=='input_uncertain':return r
        await c.tick()
    raise AssertionError('Upload preparation did not reach durable intent')


async def test_slow_upload_renews_lease_so_other_coordinator_cannot_reclaim(store):
    entered=asyncio.Event()
    class Slow(TestBatch):
        async def upload(self,run,state):
            entered.set();await asyncio.sleep(5.2);return await super().upload(run,state)
    batch=Slow(['{"type":"final","answer":"42","citations":[]}']);r=await at_upload(store,batch)
    c=Coordinator(store,batch,owner='slow',tenants=[A],lease_seconds=5,renew_interval=1,action_seconds=12)
    task=asyncio.create_task(c.tick());await entered.wait();await asyncio.sleep(5.05)
    assert await store.claim('contender',[A]) is None
    await task;v=await store.get(A,r['id'])
    assert v['state']['phase']=='batch_intent' and batch.uploads==1 and v['lease_epoch']==r['lease_epoch']+1
    done=await advance_to_terminal(store,r,batch);assert done['status']=='completed' and batch.uploads==1


async def test_cancellation_during_slow_upload_loses_renewal_and_preserves_terminal_state(store):
    entered=asyncio.Event();cancelled=asyncio.Event()
    class Slow(TestBatch):
        async def upload(self,run,state):
            entered.set()
            try:await asyncio.sleep(10)
            finally:cancelled.set()
    batch=Slow([]);r=await at_upload(store,batch)
    c=Coordinator(store,batch,owner='slow',tenants=[A],lease_seconds=5,renew_interval=.1)
    task=asyncio.create_task(c.tick());await entered.wait()
    before=await store.cancel(A,r['id']);await asyncio.wait_for(task,2)
    after=await store.get(A,r['id'])
    assert cancelled.is_set() and after==before and not batch.batches


async def test_action_deadline_after_upload_acceptance_reconciles_original_input(store):
    class UnknownAck(TestBatch):
        async def upload(self,run,state):
            fid=await super().upload(run,state);await asyncio.sleep(10);return fid
    batch=UnknownAck(['{"type":"final","answer":"42","citations":[]}']);r=await at_upload(store,batch)
    c=Coordinator(store,batch,owner='deadline',tenants=[A],lease_seconds=5,renew_interval=.02,action_seconds=.1)
    await c.tick();waiting=await store.get(A,r['id'])
    assert waiting['state']['phase']=='input_uncertain' and waiting['state']['upload_attempted'] and batch.uploads==1
    assert await store.claim('too-early',[A]) is None
    await expire_test_lease(store,r['id'])  # fixture owner only, accelerates natural reclaim
    done=await advance_to_terminal(store,r,batch)
    assert done['status']=='completed' and len(batch.batches)==1 and batch.uploads==1 and done['model_calls']==1


async def test_ordinary_transition_expiring_in_open_transaction_rolls_back_event_and_usage(store,monkeypatch):
    r=await make(store);c=await store.claim('short',[A],seconds=5);event=store.event
    async def delayed(*a,**kw):
        result=await event(*a,**kw)
        if kw['kind']=='model.reserved':await asyncio.sleep(5.1)
        return result
    monkeypatch.setattr(store,'event',delayed)
    with pytest.raises(AgentError) as exc:
        await store.transition(A,r['id'],c['lease_token'],c['revision'],'reserve',kind='model.reserved',state={'intent':'preserved'},model_calls=1)
    assert exc.value.code=='stale_agent_lease'
    after=await store.get(A,r['id']);assert after['model_calls']==0 and after['revision']==c['revision'] and after['state']=={}
    assert len((await store.events(A,r['id']))['events'])==2


async def test_heartbeat_cannot_revive_lease_that_expires_while_lock_is_held(store,monkeypatch):
    r=await make(store);c=await store.claim('short',[A],seconds=5);locked=store.locked_run
    async def delayed(*a,**kw):
        row=await locked(*a,**kw);await asyncio.sleep(5.1);return row
    monkeypatch.setattr(store,'locked_run',delayed)
    with pytest.raises(AgentError):await store.heartbeat(A,r['id'],c['lease_token'],seconds=120)
    async with store.pool.connection() as db:
        row=await one(db,'SELECT lease_expires_at<=clock_timestamp() expired FROM bf_agents.runs WHERE id=%s',(r['id'],))
    assert row['expired']


async def test_claim_and_renewal_never_extend_absolute_run_deadline(store):
    async with store.pool.connection() as db,db.transaction():
        await store.tenant_lock(db,A)
        deadline=(await one(db,"SELECT clock_timestamp()+interval '6 seconds' deadline"))['deadline']
        r=await store.create_locked(db,A,'short-deadline',goal='Bounded test',model='test-model',policy=Policy(),deadline=deadline)
    c=await store.claim('holder',[A],seconds=15)
    assert c['lease_expires_at']==deadline
    renewed=await store.heartbeat(A,r['id'],c['lease_token'],seconds=120)
    assert renewed['lease_expires_at']==deadline
