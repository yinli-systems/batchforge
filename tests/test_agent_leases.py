"""Action lifetime/renewal semantics; deterministic adapters, not GPU evidence."""
import asyncio
import pytest
from agents.leases import ActionLease,LeaseLost,ActionDeadline,UnstoppableAction,checkpoint


class Renewal:
    def __init__(self,*,reject=False):self.calls=0;self.reject=reject
    async def heartbeat(self,*a,**kw):
        self.calls+=1
        if self.reject:raise RuntimeError('Test-only renewal refusal')


def lease(store,**kw):
    return ActionLease(store,{'tenant_id':'test','id':'run','lease_token':'test-token'},
        claimed_at=asyncio.get_running_loop().time(),seconds=5,renew_interval=.01,action_seconds=.2,**kw)


async def test_renewal_is_independent_and_stops_after_action_completion():
    store=Renewal();g=lease(store)
    async def action():await asyncio.sleep(.045);checkpoint();return 'done'
    assert await g.run(action)=='done' and store.calls>=2
    n=store.calls;await asyncio.sleep(.025);assert store.calls==n


async def test_renewal_failure_cancels_action_before_later_side_effect():
    store=Renewal(reject=True);g=lease(store);cancelled=asyncio.Event();effects=[]
    async def action():
        try:await asyncio.sleep(1);checkpoint();effects.append('forbidden')
        finally:cancelled.set()
    with pytest.raises(LeaseLost):await g.run(action)
    assert cancelled.is_set() and not effects and g.revoked


async def test_total_action_deadline_is_not_extended_by_successful_renewal():
    store=Renewal();g=ActionLease(store,{'tenant_id':'t','id':'r','lease_token':'k'},
        claimed_at=asyncio.get_running_loop().time(),seconds=5,renew_interval=.01,action_seconds=.045)
    with pytest.raises(ActionDeadline):await g.run(lambda:asyncio.sleep(1))
    assert store.calls>=2 and g.revoked


async def test_delayed_claim_ack_cannot_start_an_already_stale_action():
    g=lease(Renewal());g.valid_until=asyncio.get_running_loop().time()-1;called=False
    async def action():
        nonlocal called;called=True
    with pytest.raises(LeaseLost):await g.run(action)
    assert not called


async def test_noncooperative_action_is_revoked_and_rejected_as_fatal():
    g=lease(Renewal(reject=True));release=asyncio.Event();stopped=asyncio.Event()
    async def action():
        try:
            try:await asyncio.sleep(10)
            except asyncio.CancelledError:await release.wait()
            checkpoint()
        finally:stopped.set()
    try:
        with pytest.raises(UnstoppableAction):await g.run(action)
    finally:release.set();await asyncio.wait_for(stopped.wait(),1)


@pytest.mark.parametrize('kw',[{'seconds':True},{'seconds':4},{'renew_interval':6},{'renew_interval':0},
    {'action_seconds':float('inf')},{'action_seconds':61}])
def test_action_lease_configuration_is_bounded_operator_input(kw):
    with pytest.raises(ValueError):ActionLease(None,{},claimed_at=0,**kw)
