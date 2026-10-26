"""Real PG outbox/reclaim/meter tests; scripted output is not GPU evidence."""
from dataclasses import replace
import pytest
from agents.runtime import Coordinator
from agents.retrieval import Registry
from agents.contracts import canonical
from test_agent_span_postgres import setup,finish,SEARCH
from test_agent_postgres import store,A,DSN,make,TestBatch,expire_test_lease
pytestmark=pytest.mark.skipif(not DSN,reason='Actual isolated PostgreSQL required')


class Model(TestBatch):
    async def find_batch(self,run,state):return self.batches.get(state['child_key'])


def policy(tmp):
    path,p=setup(tmp);return path,replace(p,answer_adapter='source-span-v1')


async def test_restarted_steps_keep_exact_source_mapping_and_charge_invalid_outputs(store,tmp_path):
    path,p=policy(tmp_path);batch=Model([SEARCH,'{"answer":"2030","source":true}','{"answer":"2030","source":0}'])
    done=await finish(store,await make(store,policy=p),batch,path)
    assert done['status']=='completed' and done['result']['answer']=='2030'
    assert done['generated_tokens']==60 and done['model_calls']==3 and done['tool_calls']==1
    assert len(done['state']['output_rejections'])==1 and len(batch.batches)==3
    assert done['state']['answer_adapter_context']['protocol']=='batchforge.source-ordinals.v1'
    assert done['result']['source_evidence']['quotes'][0]['answer_sha256']
    await store.events(A,done['id'])


async def test_empty_sources_abstain_without_fabricated_grants_and_keep_usage(store,tmp_path):
    path,p=policy(tmp_path);batch=Model(['{"type":"tool","name":"retrieval.search","arguments":{"query":"zzzzunknown"}}','{"answer":null,"source":null}'])
    done=await finish(store,await make(store,policy=p),batch,path)
    assert done['result']['type']=='abstain' and done['generated_tokens']==40
    assert not done['state']['answer_adapter_context']['sources']


async def test_revoked_source_after_lost_batch_ack_reconciles_read_only_then_charges(store,tmp_path):
    path,p=policy(tmp_path)
    class Lost(Model):
        def __init__(self):super().__init__([SEARCH,'{"answer":"2030","source":0}']);self.posts=0
        async def create(self,run,state):
            self.posts+=1;bid=await super().create(run,state)
            if state['step']==2:raise RuntimeError('Lost accepted ACK')
            return bid
    batch=Lost();run=await make(store,policy=p)
    for _ in range(60):
        try:await Coordinator(store,batch,owner='before-crash',tenants=[A],retrieval=Registry(path)).tick()
        except RuntimeError:break
    else:raise AssertionError('Expected scripted accepted ACK loss')
    await expire_test_lease(store,run['id'])
    for _ in range(20):
        await Coordinator(store,batch,owner='after-revocation',tenants=[A],retrieval=Registry()).tick()
        done=await store.get(A,run['id'])
        if done['status']=='failed':break
    assert done['status']=='failed' and done['result']['code']=='workflow_source_authorization_lost'
    assert done['generated_tokens']==40 and done['model_calls']==2 and batch.posts==2
    assert len(done['state']['receipts'])==2


async def test_durable_ordinal_drift_is_refused_before_a_new_upload(store,tmp_path):
    from psycopg.types.json import Jsonb
    path,p=policy(tmp_path);batch=Model([SEARCH]);run=await make(store,policy=p)
    for _ in range(35):
        await Coordinator(store,batch,owner='planner',tenants=[A],retrieval=Registry(path)).tick()
        current=await store.get(A,run['id'])
        if current['state'].get('answer_adapter_context') is not None:break
    else:raise AssertionError('Expected durable extraction intent')
    state=current['state'];state['answer_adapter_context']['sources'][0]['text']='Fabricated source'
    async with store.pool.connection() as db:await db.execute('UPDATE bf_agents.runs SET state=%s WHERE id=%s',(Jsonb(state),run['id']))
    done=await finish(store,run,batch,path)
    assert done['status']=='failed' and done['result']['code']=='workflow_source_provenance_invalid'
    assert batch.uploads==1 and len(batch.batches)==1 and done['generated_tokens']==20


async def test_inherited_extraction_uses_original_source_without_new_query_permission(store,tmp_path):
    from test_agent_provenance_postgres import NativeModel,pump
    from test_agent_provenance import graph
    from agents.workflows import Workflow,WorkflowStore
    path,p=policy(tmp_path);provider=Registry(path)
    spec=graph(p.retrieval_snapshot)
    node=next(n for n in spec['nodes'] if n['id']=='reader')
    node.update(answer_format='retrieved_span',answer_adapter='source-span-v1')
    parent=await WorkflowStore(store).create(A,'adapter-reader',Workflow.model_validate(spec))
    citation=provider.require(A,'facts',p.retrieval_snapshot).search('Mars mission')['matches'][0]['citation']
    class Inherited(NativeModel):
        async def poll(self,run,state):
            if run['policy'].get('answer_adapter'):
                return {'content':'{"answer":"2030","source":0}','usage':{'prompt_tokens':100,'completion_tokens':20},'receipt_sha256':'a'*64}
            return await super().poll(run,state)
    batch=Inherited(citation,retrieval=True);done=await pump(store,parent,batch,provider)
    assert done['status']=='completed'
    reader=await store.get(A,done['state']['nodes']['reader']['run_id'])
    assert reader['result']['answer']=='2030' and reader['result']['source_evidence']['quotes'][0]['citation']==citation
    assert reader['policy']['allowed_tools']==[] and reader['tool_calls']==0
    assert reader['result']['dependency_evidence']['sources'][0]['citation']==citation
