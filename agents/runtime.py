"""Crash-resumable, capability-checked Agent coordinator.

Model work goes through BatchForge, never a second GPU engine. Persist the input
intent before I/O and reconcile uncertain uploads instead of blindly replaying.
Only trusted read-only/data tools are currently executable. The database lease
fences Agent commits; an already accepted child Batch can still consume GPU after
Agent cancellation. Its accepted usage stays in the original Batch ledger.
"""
from __future__ import annotations
import copy
from .contracts import Policy,ToolDecision,FinalDecision,OutputContractError,canonical,digest,parse_decision,planner_messages
from .tools import Context,SPECS,execute
from .memory import StoreMemory
from .store import AgentError


class RetryLater(Exception):pass
class DeliveryError(Exception):pass


def policy_from(value):return Policy(**{**value,'allowed_tools':tuple(value['allowed_tools'])})


def citations(observations):
    found=set()
    def visit(value):
        if isinstance(value,dict):
            if isinstance(value.get('citation'),str):found.add(value['citation'])
            for child in value.values():visit(child)
        elif isinstance(value,list):
            for child in value:visit(child)
    for item in observations:visit(item.get('result'))
    return found


class Coordinator:
    def __init__(self,store,batch,*,owner,tenants,memory=None):
        self.store=store;self.batch=batch;self.owner=owner;self.tenants=tuple(tenants)
        self.memory=memory or StoreMemory(store)

    async def tick(self):
        run=await self.store.claim(self.owner,self.tenants,seconds=120)
        if run is None:return False
        try:await self.advance(run)
        except RetryLater:
            await self.change(run,'retry',kind='dependency.retry',state=run['state'],delay_seconds=5)
        except AgentError as exc:
            if exc.code in ('stale_agent_lease','agent_revision_conflict','agent_deadline_exceeded'):return True
            await self.fail(run,exc.code)
        except (ValueError,PermissionError,DeliveryError) as exc:
            # Do not journal arbitrary DB/HTTP diagnostics or tool content.
            await self.fail(run,{'PermissionError':'tool_not_granted','DeliveryError':'model_delivery_invalid'}.get(type(exc).__name__,'agent_protocol_invalid'))
        return True

    async def change(self,run,label,*,kind,state,status='running',**kwargs):
        value=await self.store.transition(run['tenant_id'],run['id'],run['lease_token'],run['revision'],
            f"epoch.{run['lease_epoch']}.rev.{run['revision']}.{label}",kind=kind,state=state,status=status,release=True,**kwargs)
        run.update(value)
        return value

    async def fail(self,run,code):
        return await self.change(run,'failed',kind='run.failed',state=run['state'],status='failed',result={'code':code})

    async def reject_output(self,run,state,code):
        state.setdefault('output_rejections',[]).append({'step':state['step'],'code':code,
            'content_sha256':digest(state['model_content']),'receipt_sha256':state['receipts'][-1]['receipt_sha256']})
        state['output_feedback']=code;state['phase']='plan'
        for key in ('request','batch_id','file_id','upload_attempted','model_content','delivery_invalid'):state.pop(key,None)
        return await self.change(run,'output_rejected',kind='output.rejected',state=state)

    async def advance(self,run):
        state=copy.deepcopy(run['state']);phase=state.get('phase','plan');policy=policy_from(run['policy'])
        step=state.get('step',0)
        if phase=='plan':
            if run['model_calls']>=run['max_model_calls']:return await self.fail(run,'model_call_budget_exceeded')
            remaining=run['max_generated_tokens']-run['generated_tokens']
            if remaining<64:return await self.fail(run,'generated_token_budget_exceeded')
            observations=[{k:v for k,v in item.items() if k!='elapsed_seconds'} for item in state.get('observations',[])]
            from .structured import response_format
            body={'model':run['model'],'messages':planner_messages(run['goal'],policy,SPECS,observations,output_feedback=state.get('output_feedback')),
                  'max_tokens':min(512,remaining),'temperature':0,
                  'response_format':response_format(policy,observed_citations=citations(observations),remaining_tools=run['max_tool_calls']-run['tool_calls'])}
            state.update(phase='input_intent',step=step+1,request=body,
                         request_sha256=digest(body),child_key=f"agent.{run['id']}.{step+1}")
            return await self.change(run,'intent',kind='model.intent',state=state)
        if phase=='input_intent':
            # Commit the uncertain state before the only upload attempt. Crash
            # before or after this call is reconciled by exact tenant/filename/
            # content SHA. No automatic duplicate upload if no object appears.
            state['phase']='input_uncertain'
            await self.change(run,'upload_intent',kind='artifact.upload_intent',state=state)
            return  # Separate durable claim performs reconciliation first.
        if phase=='input_uncertain':
            fid=await self.batch.find_input(run,state)
            if fid:
                state.update(phase='batch_intent',file_id=fid)
                return await self.change(run,'input_found',kind='artifact.reconciled',state=state)
            if state.get('upload_attempted'):
                # Human inspection/cancellation is required; neither model text
                # nor a coordinator restart grants permission to replay I/O.
                return await self.change(run,'uncertain',kind='artifact.unresolved',state=state,status='waiting_approval',
                    result={'code':'input_upload_unresolved','action':'Inspect the named artifact or cancel; upload was not replayed'})
            state['upload_attempted']=True
            # Keep the lease for the upload; the durable bit must precede it.
            updated=await self.store.transition(run['tenant_id'],run['id'],run['lease_token'],run['revision'],
                f"upload.{step}",kind='artifact.upload_started',state=state)
            run.update(updated)
            fid=await self.batch.upload(run,state)
            state.update(phase='batch_intent',file_id=fid)
            return await self.change(run,'input_ready',kind='artifact.ready',state=state)
        if phase=='batch_intent':
            # Charge a reserved model call before the side effect. BatchForge's
            # stable key owns accepted-task uniqueness, including lost ACKs.
            state['phase']='batch_uncertain'
            return await self.change(run,'model_reserve',kind='model.reserved',state=state,model_calls=1)
        if phase=='batch_uncertain':
            bid=await self.batch.create(run,state)
            state.update(phase='waiting_model',batch_id=bid)
            return await self.change(run,'batch_ready',kind='model.enqueued',state=state,status='waiting_model')
        if phase=='waiting_model':
            delivery=await self.batch.poll(run,state)
            if delivery is None:
                return await self.store.release_idle(run['tenant_id'],run['id'],run['lease_token'])
            usage=delivery['usage']
            if type(usage.get('completion_tokens')) is not int or not 0<=usage['completion_tokens']<=state['request']['max_tokens']:
                raise DeliveryError('Output usage exceeds reserved request')
            state.setdefault('receipts',[]).append({'batch_id':state['batch_id'],'request_sha256':state['request_sha256'],
                'receipt_sha256':delivery['receipt_sha256'],'usage':usage})
            if not isinstance(delivery['content'],str) or len(delivery['content'].encode())>32768:
                raise DeliveryError('Planner response exceeds bound')
            # Charge accepted usage before parsing. Invalid planner JSON still
            # consumed real model tokens and must not disappear from accounting.
            state['model_content']=delivery['content'];state['phase']='decision'
            state['delivery_invalid']=delivery.get('invalid_code')
            return await self.change(run,'decision',kind='model.delivered',state=state,generated_tokens=usage['completion_tokens'])
        if phase=='decision':
            if state.get('delivery_invalid'):
                return await self.fail(run,'model_output_truncated' if state['delivery_invalid']=='model_output_truncated' else 'model_delivery_invalid')
            try:decision=parse_decision(state['model_content'])
            except ValueError:
                if policy.planner_protocol!='schema-v1':raise
                return await self.reject_output(run,state,'planner_json_invalid')
            policy.authorize(decision)
            if isinstance(decision,FinalDecision):
                if not set(decision.citations)<=citations(state.get('observations',[])):
                    if policy.planner_protocol=='schema-v1':return await self.reject_output(run,state,'citation_unobserved')
                    raise ValueError('Unretrieved citation')
                try:policy.validate_final(decision)
                except OutputContractError as exc:
                    return await self.reject_output(run,state,exc.code)
                return await self.change(run,'final',kind='run.completed',state=state,status='completed',
                    result={**decision.model_dump(),'batch_receipts':state.get('receipts',[])})
            if run['tool_calls']>=run['max_tool_calls']:return await self.fail(run,'tool_call_budget_exceeded')
            observation=await execute(decision,Context(run['tenant_id'],policy,self.memory))
            state.setdefault('observations',[]).append(observation)
            state.pop('model_content');state['phase']='plan'
            for key in ('request','batch_id','file_id','upload_attempted','delivery_invalid'):state.pop(key,None)
            return await self.change(run,'tool',kind='tool.completed',state=state,tool_calls=1)
        raise ValueError('Unknown durable phase')
