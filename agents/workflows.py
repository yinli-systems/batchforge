"""Bounded, static multi-Agent DAGs over the existing fenced execution store.

The graph is admitted atomically with all child quotas reserved. A control row
never calls inference; only dependency-ready child Agents do. There is no model
authority to add nodes, change grants, move tenants or expand a total budget.
"""
from __future__ import annotations
import copy,secrets
from pydantic import Field,model_validator
from psycopg.types.json import Jsonb
from .contracts import Strict,Policy,canonical,digest,protocol_json
from .store import AgentError,TERMINAL,one,view

PROGRAM='batchforge/workflow-v1'


class Node(Strict):
    id:str=Field(pattern=r'^[A-Za-z][A-Za-z0-9_-]{0,31}$')
    goal:str=Field(min_length=1,max_length=16384)
    model:str=Field(min_length=1,max_length=256)
    depends_on:list[str]=Field(default_factory=list,max_length=16)
    allowed_tools:list[str]=Field(default_factory=lambda:['math.calculate'],max_length=4)
    max_model_calls:int=Field(default=4,ge=1,le=32)
    max_tool_calls:int=Field(default=2,ge=0,le=64)
    max_generated_tokens:int=Field(default=2048,ge=64,le=32768)
    answer_format:str=Field(default='text',pattern=r'^(text|identifier|number|retrieved_span)$')
    require_citations:bool=False
    planner_protocol:str=Field(default='json-v1',pattern=r'^(json-v1|schema-v1)$')
    tool_sequence:list[str]=Field(default_factory=list,max_length=8)

    def policy(self,spec,*,parent='',node=''):
        return Policy(allowed_tools=tuple(self.allowed_tools),memory_namespace=spec.memory_namespace,
            max_model_calls=self.max_model_calls,max_tool_calls=self.max_tool_calls,
            max_generated_tokens=self.max_generated_tokens,max_wall_seconds=spec.max_wall_seconds,
            answer_format=self.answer_format,require_citations=self.require_citations,
            planner_protocol=self.planner_protocol,tool_sequence=tuple(self.tool_sequence),
            retrieval_snapshot=spec.retrieval_snapshot if 'retrieval.search' in self.allowed_tools else '',
            parent_workflow=parent,parent_node=node)


class Workflow(Strict):
    nodes:list[Node]=Field(min_length=1,max_length=16)
    outputs:list[str]=Field(min_length=1,max_length=16)
    max_parallel:int=Field(default=2,ge=1,le=8)
    allowed_tools:list[str]=Field(default_factory=lambda:list(Policy().allowed_tools),max_length=4)
    memory_namespace:str=Field(default='default',pattern=r'^[a-zA-Z0-9_-]{1,64}$')
    retrieval_snapshot:str=Field(default='',pattern=r'^(|[0-9a-f]{64})$')
    max_model_calls:int=Field(default=16,ge=1,le=32)
    max_tool_calls:int=Field(default=16,ge=0,le=64)
    max_generated_tokens:int=Field(default=16384,ge=64,le=32768)
    max_wall_seconds:int=Field(default=900,ge=30,le=3600)
    max_dependency_bytes:int=Field(default=8192,ge=256,le=16384)

    def policy(self):
        return Policy(allowed_tools=tuple(self.allowed_tools),memory_namespace=self.memory_namespace,
            max_model_calls=self.max_model_calls,max_tool_calls=self.max_tool_calls,
            max_generated_tokens=self.max_generated_tokens,max_wall_seconds=self.max_wall_seconds,
            execution_kind='workflow',retrieval_snapshot=self.retrieval_snapshot)

    def document(self):
        # Adding an absent optional snapshot must not alter old request keys.
        value=self.model_dump()
        if not self.retrieval_snapshot:value.pop('retrieval_snapshot')
        return value

    @model_validator(mode='after')
    def valid_graph(self):
        self.policy()
        nodes={n.id:n for n in self.nodes}
        if len(nodes)!=len(self.nodes) or len(set(self.outputs))!=len(self.outputs) or not set(self.outputs)<=nodes.keys():
            raise ValueError('Unique node IDs and declared output nodes required')
        visited=set();visiting=set()
        def visit(nid):
            if nid in visiting:raise ValueError('Workflow must be acyclic')
            if nid in visited:return
            visiting.add(nid)
            n=nodes[nid]
            if len(set(n.depends_on))!=len(n.depends_on) or not set(n.depends_on)<=nodes.keys():
                raise ValueError('Unique existing dependencies required')
            for dependency in n.depends_on:visit(dependency)
            visiting.remove(nid);visited.add(nid)
        for output in self.outputs:visit(output)
        if visited!=nodes.keys():raise ValueError('Every node must contribute to an output')
        for n in self.nodes:
            n.policy(self)
            if not set(n.allowed_tools)<=set(self.allowed_tools):raise ValueError('Child grants exceed workflow grants')
            if '\x00' in n.goal:raise ValueError('JSONB-safe goal required')
        for budget in ('max_model_calls','max_tool_calls','max_generated_tokens'):
            if sum(getattr(n,budget) for n in self.nodes)>getattr(self,budget):raise ValueError('Allocated child budgets exceed global budget')
        if len(canonical(self.model_dump()))>32768:raise ValueError('Graph exceeds immutable execution document bound')
        return self


def spec_from(row):
    if row['policy'].get('execution_kind')!='workflow' or row['model']!=PROGRAM:
        raise AgentError(404,'workflow_not_found')
    spec=Workflow.model_validate(protocol_json(row['goal']))
    if digest(spec.policy().document())!=digest(row['policy']):raise AgentError(409,'workflow_policy_drift')
    return spec


def projection(child):
    result=child['result']
    if child['status']!='completed' or not isinstance(result,dict) or result.get('type')!='final':
        raise AgentError(409,'workflow_dependency_invalid')
    # Pass answer data, not another Agent's outbox or raw receipts. Full result
    # and projected-data commitments remain independently distinguishable.
    data={'answer':result['answer'],'citations':result.get('citations',[])}
    return {'node':child['policy']['parent_node'],'run_id':child['id'],
        'result':data,'result_sha256':digest(result),'projection_sha256':digest(data),
        'event_revision':child['revision'],'event_head_sha256':child['event_hash']}


class WorkflowStore:
    def __init__(self,store):self.store=store

    async def create(self,tenant,key,spec):
        if not isinstance(spec,Workflow):raise AgentError(422,'invalid_workflow')
        goal=canonical(spec.document()).decode();policy=spec.policy()
        async with self.store.pool.connection() as conn,conn.transaction():
            await self.store.tenant_lock(conn,tenant)
            old=await one(conn,'SELECT * FROM bf_agents.runs WHERE tenant_id=%s AND request_key=%s',(tenant,key))
            if old:
                if old['request_hash']!=digest({'goal':goal,'model':PROGRAM,'policy':policy.document()}):
                    raise AgentError(409,'agent_idempotency_conflict')
                return view(old)
            rid='agrun_'+secrets.token_hex(16)
            state={'phase':'workflow','nodes':{n.id:{'run_id':'agrun_'+secrets.token_hex(16),'status':'blocked'} for n in spec.nodes}}
            parent=await self.store.create_locked(conn,tenant,key,goal=goal,model=PROGRAM,policy=policy,rid=rid,state=state)
            for n in spec.nodes:
                child=await self.store.create_locked(conn,tenant,f'workflow.{rid}.{n.id}',goal=n.goal,model=n.model,
                    policy=n.policy(spec,parent=rid,node=n.id),rid=state['nodes'][n.id]['run_id'],
                    state={'phase':'plan','workflow_blocked':True},deadline=parent['deadline_at'])
                if child['id']!=state['nodes'][n.id]['run_id']:raise AgentError(409,'workflow_child_conflict')
            return view(parent)

    async def get(self,tenant,rid):
        row=await self.store.get(tenant,rid);spec_from(row);return row

    async def children_locked(self,conn,parent,spec):
        rows=await (await conn.execute("""SELECT * FROM bf_agents.runs WHERE tenant_id=%s
            AND policy->>'parent_workflow'=%s ORDER BY id FOR UPDATE""",(parent['tenant_id'],parent['id']))).fetchall()
        children={r['policy'].get('parent_node'):r for r in rows}
        if len(rows)!=len(spec.nodes) or set(children)!=set(n.id for n in spec.nodes):raise AgentError(409,'workflow_child_set_drift')
        for n in spec.nodes:
            row=children[n.id]
            if (row['id']!=parent['state']['nodes'][n.id]['run_id'] or row['goal']!=n.goal or row['model']!=n.model
                or row['deadline_at']!=parent['deadline_at'] or digest(row['policy'])!=digest(n.policy(spec,parent=parent['id'],node=n.id).document())):
                raise AgentError(409,'workflow_child_policy_drift')
        return children

    async def reconcile(self,run):
        """One short, fenced transaction. No network/model I/O while locked.

        Activations, parent checkpoint and child inputs commit together. A lost
        ACK leaves exactly the same child IDs and dependency snapshots in PG.
        """
        s=self.store;tenant=run['tenant_id'];rid=run['id']
        async with s.pool.connection() as conn,conn.transaction():
            parent=await s.locked_run(conn,tenant,rid);s.require_lease(parent,run['lease_token'])
            if parent['revision']!=run['revision']:raise AgentError(409,'agent_revision_conflict')
            spec=spec_from(parent);children=await self.children_locked(conn,parent,spec)
            state=copy.deepcopy(parent['state']);failure=None;activated=[]
            for nid,child in children.items():
                state['nodes'][nid]['status']='blocked' if child['state'].get('workflow_blocked') and child['status'] not in TERMINAL else child['status']
            if parent['deadline_at']<=parent['db_now']:failure='workflow_deadline_exceeded'
            elif any(c['status'] in ('failed','cancelled') for c in children.values()):failure='workflow_child_failed'
            elif any(c['status']=='completed' and isinstance(c['result'],dict) and c['result'].get('type')=='abstain' for c in children.values()):
                failure='workflow_child_abstained'
            active=sum(c['status'] not in TERMINAL and not c['state'].get('workflow_blocked') for c in children.values())
            if not failure:
                for n in spec.nodes:
                    child=children[n.id]
                    if active>=spec.max_parallel:break
                    if child['status'] in TERMINAL or not child['state'].get('workflow_blocked'):continue
                    if not all(children[d]['status']=='completed' for d in n.depends_on):continue
                    inputs=[projection(children[d]) for d in n.depends_on]
                    if len(canonical(inputs))>spec.max_dependency_bytes:
                        failure='workflow_dependency_context_exceeded';break
                    if child['lease_token'] or child['revision']!=0 or child['model_calls'] or child['tool_calls']:
                        raise AgentError(409,'workflow_blocked_child_drift')
                    child_state={**child['state'],'workflow_blocked':False,'workflow_inputs':inputs}
                    child['revision']+=1
                    tail=await s.event(conn,child,kind='workflow.inputs_ready',payload={'state_sha256':digest(child_state),'parent_id':rid},
                        operation_key='workflow.activate',request_hash=digest(child_state))
                    await conn.execute('''UPDATE bf_agents.runs SET state=%s,revision=%s,event_hash=%s,updated_at=clock_timestamp()
                        WHERE tenant_id=%s AND id=%s''',(Jsonb(child_state),child['revision'],tail,tenant,child['id']))
                    child.update(state=child_state,event_hash=tail)
                    state['nodes'][n.id]['status']='queued';activated.append(n.id);active+=1
            result=None;status='running'
            if failure:
                status='failed';state['phase']='failed';result={'code':failure}
                # Same locks as cancellation; never revive or erase a child.
                for child in children.values():
                    if child['status'] not in TERMINAL:
                        await s.finish_locked(conn,child,'cancelled','workflow.cancelled',{'parent_id':rid,'code':failure})
                        state['nodes'][child['policy']['parent_node']]['status']='cancelled'
            elif all(c['status']=='completed' for c in children.values()):
                status='completed';state['phase']='completed'
                result={'protocol':'batchforge.workflow-result.v1','outputs':{n:projection(children[n]) for n in spec.outputs},
                    'usage':{k:sum(c[k] for c in children.values()) for k in ('model_calls','tool_calls','generated_tokens')},
                    'children':{nid:{'run_id':c['id'],'revision':c['revision'],'event_hash':c['event_hash']} for nid,c in children.items()},
                    'scope':'Aggregated child execution counters; accepted GPU usage remains in the child Batch ledger'}
                if len(canonical(result))>60000:
                    status='failed';state['phase']='failed';result={'code':'workflow_output_context_exceeded'}
            changed=state!=parent['state'] or status in TERMINAL
            revision=parent['revision'];tail=parent['event_hash']
            if changed:
                parent['revision']+=1;revision=parent['revision']
                payload={'state_sha256':digest(state),'result_sha256':digest(result),'status':status,'activated_nodes':activated}
                tail=await s.event(conn,parent,kind='workflow.'+('completed' if status=='completed' else 'failed' if failure or status=='failed' else 'checkpoint'),
                    payload=payload,operation_key=f'workflow.{revision}',request_hash=digest(payload))
            # Check the actual DB clock after work, not just the admission-time
            # snapshot. If the lease expired, all child activation/events roll back.
            row=await one(conn,"""UPDATE bf_agents.runs SET state=%s,status=%s,result=%s,revision=%s,event_hash=%s,
                lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL,ready_at=clock_timestamp()+interval '1 second',updated_at=clock_timestamp()
                WHERE tenant_id=%s AND id=%s AND lease_token=%s AND lease_expires_at>clock_timestamp()
                AND (deadline_at>clock_timestamp() OR %s='failed') RETURNING *""",
                (Jsonb(state),status,Jsonb(result) if result is not None else None,revision,tail,tenant,rid,run['lease_token'],status))
            if row is None:raise AgentError(409,'stale_agent_lease')
            return view(row)


class WorkflowCoordinator:
    def __init__(self,store,*,owner,tenants):
        self.store=store;self.workflows=WorkflowStore(store);self.owner=owner;self.tenants=tuple(tenants)

    async def tick(self):
        run=await self.store.claim(self.owner,self.tenants,kind='workflow',seconds=30)
        if run is None:return False
        try:await self.workflows.reconcile(run)
        except AgentError as exc:
            if exc.code not in ('stale_agent_lease','agent_revision_conflict'):raise
        return True
