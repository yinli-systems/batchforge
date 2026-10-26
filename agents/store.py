"""Tenant-isolated Agent runs, append-only events and fenced coordinator leases.

The Agent schema is independent from the serving Batch API ledger. Model steps
will reference real Batch IDs; these counters are not a second billing ledger.
"""
from __future__ import annotations
import hashlib,re,secrets
from psycopg.types.json import Jsonb
from .contracts import Policy,canonical,digest
from .leases import checkpoint

ACTIVE=('queued','running','waiting_model','waiting_approval')
TERMINAL=('completed','failed','cancelled')
ZERO='0'*64


class AgentError(Exception):
    def __init__(self,status,code):self.status=status;self.code=code;super().__init__(code)


def view(row):
    keys=('id','goal','model','policy','status','state','result','model_calls','tool_calls',
          'generated_tokens','max_model_calls','max_tool_calls','max_generated_tokens',
          'revision','event_hash','lease_epoch','deadline_at','created_at','updated_at')
    return {k:row[k].isoformat() if hasattr(row[k],'isoformat') else row[k] for k in keys}


def event_document(tenant,run,revision,kind,payload,previous):
    return {'protocol':'batchforge.agent-event.v1','tenant_id':tenant,'run_id':run,'revision':revision,
            'kind':kind,'payload':payload,'previous_hash':previous}


async def one(conn,sql,params=()):return await (await conn.execute(sql,params)).fetchone()


class AgentStore:
    def __init__(self,pool):self.pool=pool

    async def tenant_lock(self,conn,tenant):
        if not await one(conn,'SELECT id FROM public.tenants WHERE id=%s FOR NO KEY UPDATE',(tenant,)):
            raise AgentError(404,'tenant_not_found')

    async def locked_run(self,conn,tenant,rid):
        await self.tenant_lock(conn,tenant)
        row=await one(conn,'SELECT *,clock_timestamp() AS db_now FROM bf_agents.runs WHERE tenant_id=%s AND id=%s FOR UPDATE',(tenant,rid))
        if not row:raise AgentError(404,'agent_run_not_found')
        return row

    async def event(self,conn,row,*,kind,payload,operation_key,request_hash):
        if len(canonical(payload))>16384:raise AgentError(413,'agent_event_too_large')
        document=event_document(row['tenant_id'],row['id'],row['revision'],kind,payload,row['event_hash'])
        tail=digest(document)
        await conn.execute('INSERT INTO bf_agents.events(tenant_id,run_id,revision,operation_key,request_hash,kind,payload,previous_hash,event_hash) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)',
            (row['tenant_id'],row['id'],row['revision'],operation_key,request_hash,kind,Jsonb(payload),row['event_hash'],tail))
        return tail

    async def create(self,tenant,key,*,goal,model,policy=Policy()):
        async with self.pool.connection() as conn,conn.transaction():
            await self.tenant_lock(conn,tenant)
            return view(await self.create_locked(conn,tenant,key,goal=goal,model=model,policy=policy))

    async def create_locked(self,conn,tenant,key,*,goal,model,policy=Policy(),rid=None,state=None,deadline=None):
        """Internal admission primitive; caller already holds the tenant lock.

        Workflow admission reserves the parent and every blocked child in this
        one transaction. No partially admitted graph survives a quota failure.
        Initial state/lineage/deadline are server assignments, not HTTP fields.
        """
        if (not isinstance(key,str) or not 1<=len(key)<=128 or not isinstance(goal,str)
            or not 1<=len(goal.encode())<=32768 or '\x00' in goal or not isinstance(model,str) or not 1<=len(model)<=256
            or not isinstance(policy,Policy)):raise AgentError(400,'invalid_agent_run')
        body={'goal':goal,'model':model,'policy':policy.document()};request_hash=digest(body)
        old=await one(conn,'SELECT * FROM bf_agents.runs WHERE tenant_id=%s AND request_key=%s',(tenant,key))
        if old:
            if old['request_hash']!=request_hash:raise AgentError(409,'agent_idempotency_conflict')
            return old
        rid=rid or 'agrun_'+secrets.token_hex(16)
        row=await one(conn,"""INSERT INTO bf_agents.runs(id,tenant_id,request_key,request_hash,goal,model,policy,
                max_model_calls,max_tool_calls,max_generated_tokens,event_hash,state,deadline_at)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,COALESCE(%s::timestamptz,clock_timestamp()+(%s*interval '1 second'))) RETURNING *""",
                (rid,tenant,key,request_hash,goal,model,Jsonb(policy.document()),policy.max_model_calls,policy.max_tool_calls,
                 policy.max_generated_tokens,ZERO,Jsonb(state or {}),deadline,policy.max_wall_seconds))
        payload={'request_sha256':request_hash}
        if state:payload['state_sha256']=digest(state)
        tail=await self.event(conn,row,kind='run.created',payload=payload,operation_key='create',request_hash=request_hash)
        return await one(conn,'UPDATE bf_agents.runs SET event_hash=%s WHERE tenant_id=%s AND id=%s RETURNING *',(tail,tenant,rid))

    async def get(self,tenant,rid):
        async with self.pool.connection() as conn:
            row=await one(conn,'SELECT * FROM bf_agents.runs WHERE tenant_id=%s AND id=%s',(tenant,rid))
        if not row:raise AgentError(404,'agent_run_not_found')
        return view(row)

    async def list(self,tenant,*,limit=20,after=None):
        if type(limit) is not int or not 1<=limit<=100:raise AgentError(400,'invalid_agent_page')
        async with self.pool.connection() as conn:
            anchor=None
            if after:
                anchor=await one(conn,'SELECT created_at,id FROM bf_agents.runs WHERE tenant_id=%s AND id=%s',(tenant,after))
                if not anchor:raise AgentError(404,'agent_cursor_not_found')
            rows=await (await conn.execute('''SELECT id,left(goal,160) AS goal_preview,length(goal)>160 AS goal_truncated,
                model,status,model_calls,tool_calls,generated_tokens,revision,event_hash,deadline_at,created_at,updated_at
                FROM bf_agents.runs WHERE tenant_id=%s
                AND (%s::timestamptz IS NULL OR (created_at,id)<(%s::timestamptz,%s::text))
                ORDER BY created_at DESC,id DESC LIMIT %s''',
                (tenant,anchor['created_at'] if anchor else None,anchor['created_at'] if anchor else None,after,limit+1))).fetchall()
        return {'data':[{k:v.isoformat() if hasattr(v,'isoformat') else v for k,v in r.items()} for r in rows[:limit]],'has_more':len(rows)>limit,
                'next_cursor':rows[limit-1]['id'] if len(rows)>limit else None}

    async def claim(self,owner,tenants,*,seconds=30,kind='agent'):
        if (not isinstance(owner,str) or not 1<=len(owner)<=128 or type(seconds) is not int
            or not 5<=seconds<=120 or not tenants or len(tenants)>64 or kind not in ('agent','workflow')):raise AgentError(400,'invalid_agent_claim')
        async with self.pool.connection() as conn,conn.transaction():
            # Tenant lock before run, including coordinator contention with
            # cancellation/admission. SKIP LOCKED never waits behind another tenant.
            tenant=await one(conn,"""SELECT t.id FROM public.tenants t WHERE t.id=ANY(%s) AND EXISTS(
                SELECT 1 FROM bf_agents.runs r WHERE r.tenant_id=t.id
                AND COALESCE(r.policy->>'execution_kind','agent')=%s AND r.state->>'workflow_blocked' IS DISTINCT FROM 'true'
                AND (r.status IN ('queued','running','waiting_model') OR (r.status='waiting_approval' AND r.deadline_at<=clock_timestamp()))
                AND (r.lease_expires_at IS NULL OR r.lease_expires_at<=clock_timestamp()) AND r.ready_at<=clock_timestamp())
                ORDER BY (SELECT min(r.ready_at) FROM bf_agents.runs r WHERE r.tenant_id=t.id
                    AND COALESCE(r.policy->>'execution_kind','agent')=%s AND r.state->>'workflow_blocked' IS DISTINCT FROM 'true'
                    AND r.status IN ('queued','running','waiting_model','waiting_approval')),t.id
                LIMIT 1 FOR NO KEY UPDATE SKIP LOCKED""",(list(tenants),kind,kind))
            if not tenant:return None
            row=await one(conn,"""SELECT *,clock_timestamp() AS db_now FROM bf_agents.runs WHERE tenant_id=%s
                AND COALESCE(policy->>'execution_kind','agent')=%s AND state->>'workflow_blocked' IS DISTINCT FROM 'true'
                AND (status IN ('queued','running','waiting_model') OR (status='waiting_approval' AND deadline_at<=clock_timestamp()))
                AND ready_at<=clock_timestamp()
                AND (lease_expires_at IS NULL OR lease_expires_at<=clock_timestamp())
                ORDER BY created_at,id LIMIT 1 FOR UPDATE SKIP LOCKED""",(tenant['id'],kind))
            if not row:return None
            if row['deadline_at']<=row['db_now']:
                await self.finish_locked(conn,row,'failed','run.deadline',{'code':'agent_deadline_exceeded'});return None
            row['revision']+=1;epoch=row['lease_epoch']+1;token=secrets.token_hex(24)
            tail=await self.event(conn,row,kind='lease.acquired',payload={'owner':owner,'epoch':epoch},
                operation_key=f'lease.{epoch}',request_hash=digest({'owner':owner,'epoch':epoch}))
            claimed=await one(conn,"""UPDATE bf_agents.runs SET status=CASE WHEN status='queued' THEN 'running' ELSE status END,
                lease_owner=%s,lease_token=%s,lease_epoch=%s,lease_expires_at=LEAST(deadline_at,clock_timestamp()+(%s*interval '1 second')),
                revision=%s,event_hash=%s,updated_at=clock_timestamp() WHERE tenant_id=%s AND id=%s
                AND deadline_at>clock_timestamp() RETURNING *""",
                (owner,token,epoch,seconds,row['revision'],tail,row['tenant_id'],row['id']))
            if claimed is None:raise AgentError(409,'agent_deadline_exceeded')
            return claimed

    async def heartbeat(self,tenant,rid,token,*,seconds=30):
        if type(seconds) is not int or not 5<=seconds<=120:raise AgentError(400,'invalid_agent_lease')
        async with self.pool.connection() as conn,conn.transaction():
            row=await self.locked_run(conn,tenant,rid);self.require_lease(row,token)
            renewed=await one(conn,"""UPDATE bf_agents.runs SET lease_expires_at=LEAST(deadline_at,clock_timestamp()+(%s*interval '1 second')),
                updated_at=clock_timestamp() WHERE tenant_id=%s AND id=%s AND lease_token=%s
                AND lease_expires_at>clock_timestamp() AND deadline_at>clock_timestamp() RETURNING lease_expires_at""",(seconds,tenant,rid,token))
            if renewed is None:raise AgentError(409,'stale_agent_lease')
            return renewed

    async def release_idle(self,tenant,rid,token,*,seconds=2):
        """Do not rewrite planner context or append identical waiting payloads."""
        if type(seconds) is not int or not 1<=seconds<=60:raise AgentError(400,'invalid_agent_wait')
        checkpoint()
        async with self.pool.connection() as conn,conn.transaction():
            row=await self.locked_run(conn,tenant,rid);self.require_lease(row,token)
            checkpoint()
            released=await one(conn,'''UPDATE bf_agents.runs SET lease_owner=NULL,lease_token=NULL,lease_expires_at=NULL,
                ready_at=clock_timestamp()+(%s*interval '1 second'),updated_at=clock_timestamp()
                WHERE tenant_id=%s AND id=%s AND lease_token=%s AND lease_expires_at>clock_timestamp()
                AND deadline_at>clock_timestamp() RETURNING id''',(seconds,tenant,rid,token))
            if released is None:raise AgentError(409,'stale_agent_lease')

    @staticmethod
    def require_lease(row,token):
        if row['status'] not in ACTIVE or row['lease_token']!=token or row['lease_expires_at'] is None or row['lease_expires_at']<=row['db_now']:
            raise AgentError(409,'stale_agent_lease')

    async def transition(self,tenant,rid,token,revision,operation,*,kind,state,status='running',
                         model_calls=0,tool_calls=0,generated_tokens=0,result=None,delay_seconds=0,release=False):
        checkpoint()
        if (status not in (*ACTIVE,*TERMINAL) or not isinstance(operation,str) or not 1<=len(operation)<=128
            or not isinstance(kind,str) or not re.fullmatch('[a-z][a-z0-9_.]{0,63}',kind)
            or type(revision) is not int or any(type(v) is not int or v<0 for v in (model_calls,tool_calls,generated_tokens,delay_seconds))
            or delay_seconds>60 or len(canonical(state))>131072 or len(canonical(result))>65536):raise AgentError(400,'invalid_agent_transition')
        request={'kind':kind,'state':state,'status':status,'model_calls':model_calls,'tool_calls':tool_calls,
            'generated_tokens':generated_tokens,'result':result,'delay_seconds':delay_seconds,'release':release}
        request_hash=digest(request)
        async with self.pool.connection() as conn,conn.transaction():
            row=await self.locked_run(conn,tenant,rid)
            previous=await one(conn,'SELECT request_hash FROM bf_agents.events WHERE tenant_id=%s AND run_id=%s AND operation_key=%s',(tenant,rid,operation))
            if previous:
                if previous['request_hash']!=request_hash:raise AgentError(409,'agent_operation_conflict')
                return view(row)  # Lost ACK replay, never another state mutation.
            self.require_lease(row,token)
            if row['revision']!=revision:raise AgentError(409,'agent_revision_conflict')
            if row['deadline_at']<=row['db_now']:raise AgentError(409,'agent_deadline_exceeded')
            if (row['model_calls']+model_calls>row['max_model_calls'] or row['tool_calls']+tool_calls>row['max_tool_calls']
                or row['generated_tokens']+generated_tokens>row['max_generated_tokens']):raise AgentError(429,'agent_budget_exceeded')
            row['revision']+=1
            payload={'state_sha256':digest(state),'result_sha256':digest(result),'status':status,
                     'model_calls':model_calls,'tool_calls':tool_calls,'generated_tokens':generated_tokens}
            tail=await self.event(conn,row,kind=kind,payload=payload,operation_key=operation,request_hash=request_hash)
            drop=release or status in TERMINAL
            checkpoint()
            row=await one(conn,"""UPDATE bf_agents.runs SET status=%s,state=%s,result=%s,model_calls=model_calls+%s,
                tool_calls=tool_calls+%s,generated_tokens=generated_tokens+%s,revision=%s,event_hash=%s,
                lease_owner=CASE WHEN %s THEN NULL ELSE lease_owner END,lease_token=CASE WHEN %s THEN NULL ELSE lease_token END,
                lease_expires_at=CASE WHEN %s THEN NULL ELSE lease_expires_at END,
                ready_at=clock_timestamp()+(%s*interval '1 second'),updated_at=clock_timestamp()
                WHERE tenant_id=%s AND id=%s AND lease_token=%s AND lease_expires_at>clock_timestamp()
                AND deadline_at>clock_timestamp() RETURNING *""",
                (status,Jsonb(state),Jsonb(result) if result is not None else None,model_calls,tool_calls,generated_tokens,
                 row['revision'],tail,drop,drop,drop,delay_seconds,tenant,rid,token))
            if row is None:raise AgentError(409,'stale_agent_lease')
            return view(row)

    async def finish_locked(self,conn,row,status,kind,payload):
        # Parent cancellation/deadline and child cancellation share one tenant
        # transaction with activation. Already accepted Batch work is not erased.
        state=row['state']
        if row['policy'].get('execution_kind')=='workflow' and status in ('failed','cancelled'):
            children=await (await conn.execute("""SELECT * FROM bf_agents.runs WHERE tenant_id=%s
                AND policy->>'parent_workflow'=%s
                ORDER BY id FOR UPDATE""",(row['tenant_id'],row['id']))).fetchall()
            state={**state,'phase':status,'nodes':{k:dict(v) for k,v in state.get('nodes',{}).items()}}
            for child in children:
                if child['status'] not in TERMINAL:
                    child=await self.finish_locked(conn,child,'cancelled','workflow.cancelled',{'parent_id':row['id'],'code':'parent_'+status})
                node=child['policy'].get('parent_node')
                if node in state['nodes']:state['nodes'][node]['status']=child['status']
            payload={**payload,'state_sha256':digest(state)}
        row['revision']+=1
        tail=await self.event(conn,row,kind=kind,payload=payload,operation_key=kind,request_hash=digest(payload))
        return await one(conn,"""UPDATE bf_agents.runs SET status=%s,result=%s,state=%s,lease_owner=NULL,lease_token=NULL,
            lease_expires_at=NULL,revision=%s,event_hash=%s,updated_at=clock_timestamp() WHERE tenant_id=%s AND id=%s RETURNING *""",
            (status,Jsonb(payload),Jsonb(state),row['revision'],tail,row['tenant_id'],row['id']))

    async def cancel(self,tenant,rid):
        async with self.pool.connection() as conn,conn.transaction():
            row=await self.locked_run(conn,tenant,rid)
            if row['status'] in TERMINAL:return view(row)
            return view(await self.finish_locked(conn,row,'cancelled','run.cancelled',{'source':'tenant_request'}))

    async def events(self,tenant,rid):
        async with self.pool.connection() as conn,conn.transaction():
            # One read transaction snapshot for event stream and run head.
            await conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
            row=await one(conn,'SELECT revision,event_hash FROM bf_agents.runs WHERE tenant_id=%s AND id=%s',(tenant,rid))
            if not row:raise AgentError(404,'agent_run_not_found')
            events=await (await conn.execute('SELECT revision,kind,payload,previous_hash,event_hash,created_at FROM bf_agents.events WHERE tenant_id=%s AND run_id=%s ORDER BY revision',(tenant,rid))).fetchall()
        previous=ZERO
        for seq,e in enumerate(events):
            if e['revision']!=seq or e['previous_hash']!=previous or digest(event_document(tenant,rid,seq,e['kind'],e['payload'],previous))!=e['event_hash']:
                raise AgentError(409,'agent_event_chain_inconsistent')
            previous=e['event_hash']
        if len(events)!=row['revision']+1 or previous!=row['event_hash']:raise AgentError(409,'agent_event_head_inconsistent')
        return {'protocol':'batchforge.agent-events.v1','run_id':rid,'revision':row['revision'],
                'head_sha256':previous,'events':[{**e,'created_at':e['created_at'].isoformat()} for e in events],
                'scope':'Consistent PostgreSQL event hash chain; not an issuer signature or GPU attestation'}

    async def events_page(self,tenant,rid,*,after=-1,limit=50):
        if type(after) is not int or after< -1 or type(limit) is not int or not 1<=limit<=100:
            raise AgentError(400,'invalid_agent_event_page')
        async with self.pool.connection() as conn,conn.transaction():
            await conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
            head=await one(conn,'SELECT revision,event_hash FROM bf_agents.runs WHERE tenant_id=%s AND id=%s',(tenant,rid))
            if not head:raise AgentError(404,'agent_run_not_found')
            if after>head['revision']:raise AgentError(400,'agent_event_cursor_ahead')
            anchor=await one(conn,'SELECT event_hash FROM bf_agents.events WHERE tenant_id=%s AND run_id=%s AND revision=%s',(tenant,rid,after)) if after>=0 else None
            if after>=0 and not anchor:raise AgentError(409,'agent_event_anchor_missing')
            rows=await (await conn.execute('''SELECT revision,kind,payload,previous_hash,event_hash,created_at
                FROM bf_agents.events WHERE tenant_id=%s AND run_id=%s AND revision>%s ORDER BY revision LIMIT %s''',
                (tenant,rid,after,limit))).fetchall()
        previous=anchor['event_hash'] if anchor else ZERO;start=previous
        for seq,event in enumerate(rows,after+1):
            if (event['revision']!=seq or event['previous_hash']!=previous
                or digest(event_document(tenant,rid,seq,event['kind'],event['payload'],previous))!=event['event_hash']):
                raise AgentError(409,'agent_event_chain_inconsistent')
            previous=event['event_hash']
        last=rows[-1]['revision'] if rows else after
        if last==head['revision'] and previous!=head['event_hash']:raise AgentError(409,'agent_event_head_inconsistent')
        return {'protocol':'batchforge.agent-events-page.v1','run_id':rid,'after':after,'previous_hash':start,
                'revision':head['revision'],'head_sha256':head['event_hash'],'has_more':last<head['revision'],
                'events':[{**e,'created_at':e['created_at'].isoformat()} for e in rows],
                'scope':'Page continuity from stored anchor; fetch from revision 0 to independently verify complete chain; not a signature'}

    async def put_memory(self,tenant,namespace,key,text,*,expected_version):
        if (not isinstance(namespace,str) or not isinstance(key,str)
            or not re.fullmatch('[a-zA-Z0-9_-]{1,64}',namespace) or not re.fullmatch('[a-zA-Z0-9_.:-]{1,128}',key)
            or not isinstance(text,str) or '\x00' in text or len(text.encode())>32768 or type(expected_version) is not int or expected_version<0):raise AgentError(400,'invalid_agent_memory')
        sha=hashlib.sha256(text.encode()).hexdigest()
        async with self.pool.connection() as conn,conn.transaction():
            await self.tenant_lock(conn,tenant)
            old=await one(conn,'SELECT version FROM bf_agents.memories WHERE tenant_id=%s AND namespace=%s AND key=%s FOR UPDATE',(tenant,namespace,key))
            if (old['version'] if old else 0)!=expected_version:raise AgentError(409,'agent_memory_version_conflict')
            # Do not use INSERT ON CONFLICT here: PostgreSQL fires BEFORE INSERT
            # as well as BEFORE UPDATE, which would charge the quota twice.
            if old:
                row=await one(conn,'UPDATE bf_agents.memories SET text=%s,sha256=%s,version=%s,updated_at=clock_timestamp() WHERE tenant_id=%s AND namespace=%s AND key=%s RETURNING namespace,key,sha256,version',
                    (text,sha,expected_version+1,tenant,namespace,key))
            else:
                row=await one(conn,'INSERT INTO bf_agents.memories(tenant_id,namespace,key,text,sha256,version) VALUES(%s,%s,%s,%s,%s,%s) RETURNING namespace,key,sha256,version',
                    (tenant,namespace,key,text,sha,1))
        return row

    async def memory_get(self,tenant,namespace,key):
        async with self.pool.connection() as conn:
            row=await one(conn,'SELECT namespace,key,text,sha256,version FROM bf_agents.memories WHERE tenant_id=%s AND namespace=%s AND key=%s',(tenant,namespace,key))
        if not row:raise AgentError(404,'agent_memory_not_found')
        return row

    async def memory_search(self,tenant,namespace,query,limit):
        if type(limit) is not int or not 1<=limit<=20 or not isinstance(query,str) or not 1<=len(query)<=1024:raise AgentError(400,'invalid_memory_search')
        async with self.pool.connection() as conn:
            rows=await (await conn.execute("""SELECT namespace,key,text,sha256,version FROM bf_agents.memories
                WHERE tenant_id=%s AND namespace=%s AND search@@plainto_tsquery('simple',%s)
                ORDER BY ts_rank_cd(search,plainto_tsquery('simple',%s)) DESC,key LIMIT %s""",(tenant,namespace,query,query,limit))).fetchall()
        selected=[];truncated=False
        for row in rows:
            raw=row['text'].encode();row['text']=raw[:2048].decode('utf-8',errors='ignore');row['text_truncated']=len(raw)>2048
            if len(canonical({'matches':[*selected,row]}))>15000:truncated=True;break
            selected.append(row)
        return {'matches':selected,'truncated':truncated,'ranking':'postgresql_full_text','namespace':namespace}
