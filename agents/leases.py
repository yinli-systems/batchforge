"""Short renewable action leases, with a finite total action lifetime.

Monotonic checks are conservative local stops, not the ownership authority.
PostgreSQL still fences every mutation against its own clock and lease token.
Accepted external requests can survive cancellation; their outbox keys persist.
"""
from __future__ import annotations
import asyncio,contextvars,math


class LeaseLost(Exception):pass
class ActionDeadline(Exception):pass
class UnstoppableAction(RuntimeError):pass

CURRENT=contextvars.ContextVar('batchforge_agent_action_lease',default=None)
LEASE_SECONDS=15
RENEW_INTERVAL=5
ACTION_SECONDS=45


def checkpoint():
    guard=CURRENT.get()
    if guard is not None:guard.checkpoint()


class ActionLease:
    def __init__(self,store,run,*,claimed_at,seconds=LEASE_SECONDS,renew_interval=RENEW_INTERVAL,action_seconds=ACTION_SECONDS):
        if (type(seconds) is not int or not 5<=seconds<=120
            or any(type(x) not in (int,float) or not math.isfinite(x) for x in (claimed_at,renew_interval,action_seconds))
            or not 0<renew_interval<=seconds/3 or not 0<action_seconds<=60):
            raise ValueError('Short lease, independent renewal and bounded total action time required')
        self.store=store;self.run_row=run;self.seconds=seconds;self.interval=renew_interval
        self.valid_until=claimed_at+seconds-1;self.deadline=claimed_at+action_seconds;self.revoked=False

    def checkpoint(self):
        now=asyncio.get_running_loop().time()
        if self.revoked or now>=self.valid_until:raise LeaseLost()
        if now>=self.deadline:raise ActionDeadline()

    async def renew(self):
        while True:
            await asyncio.sleep(self.interval);self.checkpoint()
            started=asyncio.get_running_loop().time()
            try:
                await asyncio.wait_for(self.store.heartbeat(self.run_row['tenant_id'],self.run_row['id'],
                    self.run_row['lease_token'],seconds=self.seconds),timeout=min(3,self.interval))
            except Exception:raise LeaseLost() from None
            # Start before the DB call, not after a delayed ACK. This never
            # interprets network delay as extra granted ownership time.
            self.valid_until=started+self.seconds-1

    async def run(self,operation):
        self.checkpoint()
        async def invoke():
            token=CURRENT.set(self)
            try:
                self.checkpoint();return await operation()
            finally:CURRENT.reset(token)
        action=asyncio.create_task(invoke());renewer=asyncio.create_task(self.renew())
        try:
            done,_=await asyncio.wait((action,renewer),timeout=max(0,self.deadline-asyncio.get_running_loop().time()),
                return_when=asyncio.FIRST_COMPLETED)
            # A release/terminal commit can race a renewal refusal. Its ACK is
            # authoritative if the action itself has already completed.
            if action in done:return action.result()
            if renewer in done:
                renewer.result();raise LeaseLost()
            raise ActionDeadline()
        finally:
            self.revoked=True
            for task in (action,renewer):
                if not task.done():task.cancel()
            done,pending=await asyncio.wait((action,renewer),timeout=2)
            for task in done:
                if not task.cancelled():task.exception()  # consume abandoned errors
            if pending:
                for task in pending:task.add_done_callback(lambda t:None if t.cancelled() else t.exception())
                raise UnstoppableAction('Action ignored cancellation; coordinator must stop')
