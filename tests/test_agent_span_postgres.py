"""Native PostgreSQL contract tests. Deterministic adapter, no GPU evidence."""
import json
import pytest
from test_agent_postgres import store,A,DSN,TestBatch,make
from test_agent_retrieval import registry
from test_agent_workflows_postgres import claim_parent,complete
from test_agent_workflows import graph
from agents.contracts import Policy,canonical
from agents.retrieval import Registry
from agents.runtime import Coordinator
from agents.workflows import Workflow,WorkflowStore

pytestmark=pytest.mark.skipif(not DSN,reason='Actual isolated PostgreSQL required')
ABSTAIN='{"type":"abstain","reason":"insufficient_evidence"}'
SEARCH='{"type":"tool","name":"retrieval.search","arguments":{"query":"Mars mission"}}'


def setup(tmp):
    path=registry(tmp)
    data=json.loads(path.read_bytes());data['snapshots'][0]['tenant_id']=A
    path.write_bytes(canonical(data));r=Registry(path);sid=r.visible(A)[0]['id']
    return path,Policy(allowed_tools=('retrieval.search',),memory_namespace='facts',retrieval_snapshot=sid,
        require_citations=True,answer_format='retrieved_span',planner_protocol='schema-v1')


async def finish(store,run,batch,path):
    for _ in range(100):
        await Coordinator(store,batch,owner='span-restart',tenants=[A],retrieval=Registry(path)).tick()
        value=await store.get(A,run['id'])
        if value['status'] in ('completed','failed','cancelled'):return value
    raise AssertionError('Finite contract did not finish')


async def test_rejected_answer_and_premature_abstention_are_retried_with_meter_preserved(store,tmp_path):
    path,policy=setup(tmp_path);hit=Registry(path).require(A,'facts',policy.retrieval_snapshot).search('Mars mission')['matches'][0]
    final=lambda text:canonical({'type':'final','answer':text,'citations':[hit['citation']]}).decode()
    batch=TestBatch([ABSTAIN,SEARCH,final('The year was 2030.'),final('2030')])
    done=await finish(store,await make(store,policy=policy),batch,path)
    assert done['status']=='completed' and done['result']['answer']=='2030'
    assert done['model_calls']==4 and done['generated_tokens']==80 and len(batch.batches)==4
    assert len(done['state']['output_rejections'])==2 and done['tool_calls']==1
    assert done['result']['source_evidence']['quotes'][0]['end_byte']>0
    listed=await store.list(A);assert listed['data'][0]['answer_disposition']=='answered'
    await store.events(A,done['id'])


async def test_empty_retrieval_allows_typed_abstention_but_never_marks_answer_correct(store,tmp_path):
    path,policy=setup(tmp_path)
    batch=TestBatch(['{"type":"tool","name":"retrieval.search","arguments":{"query":"zzzzunknown"}}',ABSTAIN])
    done=await finish(store,await make(store,policy=policy),batch,path)
    assert done['status']=='completed' and done['result']['type']=='abstain' and 'answer' not in done['result']
    assert done['model_calls']==2 and done['generated_tokens']==40
    assert len(done['result']['abstention_evidence']['queries_sha256'])==1
    assert (await store.list(A))['data'][0]['answer_disposition']=='abstained'
    assert any(e['kind']=='run.abstained' for e in (await store.events(A,done['id']))['events'])


async def test_old_contract_cannot_use_abstention_to_skip_its_authorized_work(store,tmp_path):
    path,_=setup(tmp_path);batch=TestBatch([ABSTAIN])
    done=await finish(store,await make(store),batch,path)
    assert done['status']=='failed' and done['generated_tokens']==20 and done['model_calls']==1


async def test_workflow_abstention_cascades_and_retains_completed_child_meter(store):
    wf=WorkflowStore(store);parent=await wf.create(A,'abstain-graph',Workflow.model_validate(graph()))
    active=await wf.reconcile(await claim_parent(store));ids={k:v['run_id'] for k,v in active['state']['nodes'].items()}
    await complete(store,ids['left'])
    child=await store.claim('abstain-test',[A]);assert child['id']==ids['right']
    await store.transition(A,child['id'],child['lease_token'],child['revision'],'test.abstain',kind='run.abstained',state=child['state'],status='completed',
        result={'type':'abstain','reason':'insufficient_evidence'},model_calls=1,generated_tokens=20)
    done=await wf.reconcile(await claim_parent(store))
    assert done['status']=='failed' and done['result']['code']=='workflow_child_abstained'
    assert (await store.get(A,ids['join']))['status']=='cancelled'
    assert (await store.get(A,ids['right']))['generated_tokens']==20
    await store.events(A,parent['id'])


async def test_revoked_grant_fails_before_opening_new_model_child(store,tmp_path):
    path,policy=setup(tmp_path);run=await make(store,policy=policy)
    path.write_bytes(canonical({'protocol':'batchforge.retrieval-catalogue.v1','snapshots':[]}))
    batch=TestBatch([]);done=await finish(store,run,batch,path)
    assert done['status']=='failed' and done['model_calls']==0 and batch.uploads==0 and not batch.batches
