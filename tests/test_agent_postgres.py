"""Agent persistence qualification uses actual PostgreSQL, never SQLite/fake leases."""
import asyncio,os
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
import pytest
from backend.migrate import migrate_database,verify_schema
from agents.migrate import migrate,verify
from agents.contracts import Policy
from agents.store import AgentStore,AgentError

DSN=os.getenv('TEST_DATABASE_URL')
pytestmark=pytest.mark.skipif(not DSN,reason='Actual isolated PostgreSQL required')
A='agent-native-test-a';B='agent-native-test-b'


@pytest.fixture
async def store():
    migrate_database(DSN)
    with psycopg.connect(DSN,autocommit=True) as db:
        for tenant in [A,B]:db.execute('INSERT INTO public.tenants(id,name) VALUES(%s,%s) ON CONFLICT DO NOTHING',(tenant,tenant))
    migrate(DSN)
    with psycopg.connect(DSN,autocommit=True) as db:
        db.execute('TRUNCATE bf_agents.events,bf_agents.runs,bf_agents.memories')
        # Isolated cluster owner resets test-only policy/usage; no production path.
        for tenant in [A,B]:
            db.execute('INSERT INTO bf_agents.limits(tenant_id) VALUES(%s) ON CONFLICT(tenant_id) DO UPDATE SET max_active_runs=32,max_retained_runs=1000,max_memory_bytes=16777216',(tenant,))
            db.execute('INSERT INTO bf_agents.usage(tenant_id) VALUES(%s) ON CONFLICT(tenant_id) DO UPDATE SET active_runs=0,retained_runs=0,memory_bytes=0',(tenant,))
    pool=AsyncConnectionPool(DSN,min_size=1,max_size=8,open=False,kwargs={'autocommit':True,'row_factory':dict_row})
    await pool.open()
    try:yield AgentStore(pool)
    finally:await pool.close()


async def make(store,key='run',tenant=A,**kwargs):
    return await store.create(tenant,key,goal='Compute invoice total using allowed tools',model='test-model',**kwargs)


async def test_separate_schema_does_not_advance_serving_batch_migrations(store):
    async with store.pool.connection() as db:
        assert await verify_schema(db)==11
        await verify(db)
    assert migrate(DSN) is False


async def test_idempotent_create_concurrency_and_request_conflict(store):
    runs=await asyncio.gather(*[make(store) for _ in range(12)])
    assert len({r['id'] for r in runs})==1
    with pytest.raises(AgentError) as exc:await store.create(A,'run',goal='Different body',model='test-model')
    assert exc.value.code=='agent_idempotency_conflict'
    async with store.pool.connection() as db:
        usage=await (await db.execute('SELECT active_runs,retained_runs FROM bf_agents.usage WHERE tenant_id=%s',(A,))).fetchone()
    assert usage=={'active_runs':1,'retained_runs':1}


async def test_tenant_isolation_and_lease_tokens_are_not_in_public_projection(store):
    r=await make(store)
    with pytest.raises(AgentError) as exc:await store.get(B,r['id'])
    assert exc.value.status==404
    with pytest.raises(AgentError):await store.events(B,r['id'])
    claimed=await store.claim('coordinator-a',[A])
    assert claimed['lease_token'] and 'lease_token' not in await store.get(A,r['id'])
    assert await store.claim('coordinator-b',[B]) is None


async def test_expired_coordinator_cannot_commit_after_reclaim(store):
    r=await make(store);old=await store.claim('old',[A])
    async with store.pool.connection() as db:
        await db.execute("UPDATE bf_agents.runs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=%s",(r['id'],))
    new=await store.claim('new',[A]);assert new['lease_epoch']==2 and new['lease_token']!=old['lease_token']
    with pytest.raises(AgentError) as exc:
        await store.transition(A,r['id'],old['lease_token'],old['revision'],'tool.1',kind='tool.completed',state={},tool_calls=1)
    assert exc.value.code=='stale_agent_lease'
    done=await store.transition(A,r['id'],new['lease_token'],new['revision'],'tool.1',kind='tool.completed',state={'value':42},tool_calls=1)
    replay=await store.transition(A,r['id'],new['lease_token'],new['revision'],'tool.1',kind='tool.completed',state={'value':42},tool_calls=1)
    assert done['revision']==replay['revision'] and replay['tool_calls']==1
    with pytest.raises(AgentError) as exc:
        await store.transition(A,r['id'],new['lease_token'],done['revision'],'tool.1',kind='tool.completed',state={'value':99},tool_calls=1)
    assert exc.value.code=='agent_operation_conflict'
    chain=await store.events(A,r['id']);assert chain['revision']==done['revision'] and len(chain['head_sha256'])==64


async def test_cancel_fences_late_work_and_frees_only_active_quota(store):
    r=await make(store);claimed=await store.claim('old',[A]);done=await store.cancel(A,r['id'])
    assert done['status']=='cancelled'
    assert (await store.cancel(A,r['id']))['revision']==done['revision']
    with pytest.raises(AgentError):await store.transition(A,r['id'],claimed['lease_token'],claimed['revision'],'final',kind='run.completed',state={},status='completed')
    async with store.pool.connection() as db:
        usage=await (await db.execute('SELECT active_runs,retained_runs FROM bf_agents.usage WHERE tenant_id=%s',(A,))).fetchone()
    assert usage=={'active_runs':0,'retained_runs':1}


async def test_model_tool_and_generated_token_budgets_never_reset_on_restart(store):
    r=await make(store,policy=Policy(max_model_calls=1,max_tool_calls=1,max_generated_tokens=64))
    c=await store.claim('first',[A])
    v=await store.transition(A,r['id'],c['lease_token'],c['revision'],'model.1',kind='model.accepted',state={'value':42},model_calls=1,generated_tokens=64,release=True)
    c=await store.claim('restart',[A]);assert c['model_calls']==1 and c['generated_tokens']==64
    with pytest.raises(AgentError) as exc:await store.transition(A,r['id'],c['lease_token'],c['revision'],'model.2',kind='model.requested',state={},model_calls=1)
    assert exc.value.code=='agent_budget_exceeded'
    assert (await store.get(A,r['id']))['model_calls']==1


async def test_append_only_events_and_run_head_reconcile(store):
    r=await make(store);c=await store.claim('worker',[A])
    await store.transition(A,r['id'],c['lease_token'],c['revision'],'final',kind='run.completed',state={},status='completed',result={'answer':'42'})
    chain=await store.events(A,r['id']);assert len(chain['events'])==3
    async with store.pool.connection() as db:
        with pytest.raises(psycopg.Error) as exc:await db.execute("UPDATE bf_agents.events SET kind='tampered' WHERE run_id=%s",(r['id'],))
    assert exc.value.sqlstate=='BA409'


async def test_database_owns_agent_run_quota_even_if_client_admission_races(store):
    async with store.pool.connection() as db:await db.execute('UPDATE bf_agents.limits SET max_active_runs=1 WHERE tenant_id=%s',(A,))
    results=await asyncio.gather(make(store,'first'),make(store,'second'),return_exceptions=True)
    assert sum(isinstance(r,dict) for r in results)==1
    assert [r.sqlstate for r in results if isinstance(r,psycopg.Error)]==['BA429']


async def test_memory_namespace_tenant_versions_search_and_byte_accounting(store):
    a=await store.put_memory(A,'workspace','invoice','invoice total 42',expected_version=0)
    await store.put_memory(B,'workspace','invoice','invoice total 99',expected_version=0)
    assert (await store.memory_get(A,'workspace','invoice'))['text']=='invoice total 42'
    a=await store.put_memory(A,'workspace','invoice','invoice subtotal 21 and tax 21',expected_version=1)
    assert a['version']==2
    with pytest.raises(AgentError):await store.put_memory(A,'workspace','invoice','wrong old version',expected_version=1)
    search=await store.memory_search(A,'workspace','invoice',5)
    assert len(search['matches'])==1 and search['matches'][0]['sha256']==a['sha256']
    assert (await store.memory_search(A,'other','invoice',5))['matches']==[]
    async with store.pool.connection() as db:
        used=await (await db.execute('SELECT memory_bytes FROM bf_agents.usage WHERE tenant_id=%s',(A,))).fetchone()
    assert used['memory_bytes']==len('invoice subtotal 21 and tax 21'.encode())


async def test_memory_over_quota_fails_atomically(store):
    async with store.pool.connection() as db:await db.execute('UPDATE bf_agents.limits SET max_memory_bytes=1024 WHERE tenant_id=%s',(A,))
    with pytest.raises(psycopg.Error) as exc:await store.put_memory(A,'default','too-big','x'*1025,expected_version=0)
    assert exc.value.sqlstate=='BA429'
    with pytest.raises(AgentError):await store.memory_get(A,'default','too-big')


class TestBatch:
    """Deterministic test adapter, not GPU/inference evidence."""
    __test__=False
    def __init__(self,outputs,*,crash_upload=False,crash_create=False):
        self.outputs=outputs;self.files={};self.batches={};self.uploads=0
        self.crash_upload=crash_upload;self.crash_create=crash_create
    async def find_input(self,run,state):return self.files.get(state['child_key'])
    async def upload(self,run,state):
        self.uploads+=1;fid='file_'+str(self.uploads);self.files[state['child_key']]=fid
        if self.crash_upload:self.crash_upload=False;raise RuntimeError('Simulated process death after upload ACK lost')
        return fid
    async def create(self,run,state):
        bid=self.batches.setdefault(state['child_key'],'batch_'+str(len(self.batches)+1))
        if self.crash_create:self.crash_create=False;raise RuntimeError('Simulated process death after Batch commit')
        return bid
    async def poll(self,run,state):
        return {'content':self.outputs[state['step']-1],'usage':{'prompt_tokens':100,'completion_tokens':20},'receipt_sha256':'a'*64}


async def advance_to_terminal(store,run,batch,*,turns=80):
    from agents.runtime import Coordinator
    for _ in range(turns):
        # New coordinator object on every turn exercises DB-resident state.
        await Coordinator(store,batch,owner='test-restarted',tenants=[A]).tick()
        value=await store.get(A,run['id'])
        if value['status'] in ('completed','failed','cancelled','waiting_approval'):return value
    raise AssertionError('Finite Agent did not finish')


async def expire_test_lease(store,rid):
    async with store.pool.connection() as db:
        await db.execute("UPDATE bf_agents.runs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=%s",(rid,))


async def test_durable_tool_loop_restarts_coordinator_each_step(store):
    from agents.contracts import canonical
    run=await make(store)
    batch=TestBatch([canonical({'type':'tool','name':'math.calculate','arguments':{'expression':'21*2'}}).decode(),
                     canonical({'type':'final','answer':'42','citations':[]}).decode()])
    done=await advance_to_terminal(store,run,batch)
    assert done['status']=='completed' and done['result']['answer']=='42'
    assert done['model_calls']==2 and done['tool_calls']==1 and done['generated_tokens']==40
    assert len(batch.batches)==2 and batch.uploads==2
    assert done['state']['observations'][0]['result']=={'value':42}
    assert (await store.events(A,run['id']))['revision']==done['revision']


@pytest.mark.parametrize('boundary',['upload','create'])
async def test_ambiguous_io_recovers_same_file_and_child_without_duplicate_acceptance(store,boundary):
    from agents.runtime import Coordinator
    run=await make(store);batch=TestBatch(['{"type":"final","answer":"42","citations":[]}'],
        crash_upload=boundary=='upload',crash_create=boundary=='create')
    coordinator=Coordinator(store,batch,owner='crashed',tenants=[A])
    crashed=False
    for _ in range(20):
        try:await coordinator.tick()
        except RuntimeError:crashed=True;break
    assert crashed
    await expire_test_lease(store,run['id'])
    done=await advance_to_terminal(store,run,batch)
    assert done['status']=='completed' and batch.uploads==1 and len(batch.batches)==1
    assert done['model_calls']==1 and done['generated_tokens']==20


async def test_unresolved_upload_never_replays_or_enqueues_after_restart(store):
    from agents.runtime import Coordinator
    class UnknownUpload(TestBatch):
        async def upload(self,run,state):self.uploads+=1;raise RuntimeError('Unknown external outcome')
    run=await make(store);batch=UnknownUpload([]);coordinator=Coordinator(store,batch,owner='crashed',tenants=[A])
    for _ in range(12):
        try:await coordinator.tick()
        except RuntimeError:break
    await expire_test_lease(store,run['id'])
    done=await advance_to_terminal(store,run,batch)
    assert done['status']=='waiting_approval' and batch.uploads==1 and not batch.batches


async def test_invalid_model_json_still_charges_accepted_usage(store):
    run=await make(store);batch=TestBatch(['not json'])
    done=await advance_to_terminal(store,run,batch)
    assert done['status']=='failed' and done['generated_tokens']==20 and done['model_calls']==1


async def test_truncated_planner_does_not_get_a_free_retry_or_discard_usage(store):
    class Truncated(TestBatch):
        async def poll(self,run,state):return {**await super().poll(run,state),'invalid_code':'model_output_truncated'}
    run=await make(store);batch=Truncated(['{"type":"final","answer":"partial"}'])
    done=await advance_to_terminal(store,run,batch)
    assert done['status']=='failed' and done['generated_tokens']==20 and len(batch.batches)==1
    assert done['result']['code']=='model_output_truncated'


async def test_ungranted_tool_and_fabricated_citation_fail_closed(store):
    for n,content in enumerate(['{"type":"tool","name":"shell.exec","arguments":{"command":"id"}}',
                                '{"type":"final","answer":"42","citations":["fabricated"]}']):
        run=await make(store,key='denied-'+str(n));batch=TestBatch([content])
        done=await advance_to_terminal(store,run,batch)
        assert done['status']=='failed' and done['tool_calls']==0


async def test_agent_api_auth_model_capability_admission_and_cross_tenant_events(store):
    from agents.app import create_app
    from backend.config import Settings
    import httpx
    async def auth(key):
        if key not in ('A','B'):raise AgentError(401,'invalid_api_key')
        return A if key=='A' else B
    app=create_app(Settings(model_name='test-model'),store=store,auth=auth,executor_tenants=[A])
    async with app.router.lifespan_context(app),httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as client:
        headers={'Authorization':'Bearer A','Idempotency-Key':'same'}
        body={'goal':'Compute 21*2','model':'test-model'}
        first=await client.post('/v1/agent-runs',headers=headers,json=body)
        assert first.status_code==200,first.text
        rid=first.json()['id']
        assert (await client.post('/v1/agent-runs',headers=headers,json=body)).json()['id']==rid
        assert (await client.post('/v1/agent-runs',headers=headers,json={**body,'allowed_tools':['shell.exec']})).status_code==422
        assert (await client.post('/v1/agent-runs',headers=headers,json={**body,'model':'unserved'})).status_code==400
        assert (await client.get('/v1/agent-runs/'+rid+'/events',headers={'Authorization':'Bearer B'})).status_code==404
        assert (await client.post('/v1/agent-runs',headers={'Authorization':'Bearer B','Idempotency-Key':'key'},json=body)).status_code==503
        assert (await client.get('/v1/agent-runs')).status_code==401
        assert (await client.get('/v1/agent-runs',headers=headers)).json()['data'][0]['id']==rid
        assert (await client.get('/v1/agent-runs/'+rid+'/events?limit=1',headers=headers)).json()['events'][0]['revision']==0
        assert (await client.post('/v1/agent-runs/'+rid+'/cancel',headers=headers)).json()['status']=='cancelled'
        duplicate=b'{"goal":"one","goal":"two","model":"test-model"}'
        assert (await client.post('/v1/agent-runs',headers=headers,content=duplicate)).status_code==422
        assert (await client.post('/v1/agent-runs',headers=headers,content=b' '*196609)).status_code==413


async def test_event_paging_verifies_anchors_without_rereading_the_complete_stream(store):
    run=await make(store);c=await store.claim('worker',[A])
    await store.transition(A,run['id'],c['lease_token'],c['revision'],'final',kind='run.completed',state={},status='completed')
    first=await store.events_page(A,run['id'],limit=1)
    assert first['has_more'] and first['events'][0]['revision']==0 and first['previous_hash']=='0'*64
    second=await store.events_page(A,run['id'],after=0,limit=2)
    assert second['previous_hash']==first['events'][0]['event_hash'] and not second['has_more']
    assert second['events'][-1]['event_hash']==second['head_sha256']
    assert (await store.events_page(A,run['id'],after=2))['events']==[]
    with pytest.raises(AgentError):await store.events_page(B,run['id'])
    with pytest.raises(AgentError):await store.events_page(A,run['id'],after=3)


async def test_runtime_role_cannot_widen_database_owned_agent_quotas(store):
    role='bf_agent_native_restricted'
    with psycopg.connect(DSN,autocommit=True) as db:
        if not db.execute('SELECT 1 FROM pg_roles WHERE rolname=%s',(role,)).fetchone():db.execute('CREATE ROLE bf_agent_native_restricted')
    migrate(DSN,runtime_role=role)
    with psycopg.connect(DSN,autocommit=True) as db:
        db.execute('SET ROLE bf_agent_native_restricted')
        assert db.execute('SELECT retained_runs FROM bf_agents.usage WHERE tenant_id=%s',(A,)).fetchone()[0]==0
        with pytest.raises(psycopg.errors.InsufficientPrivilege):db.execute('UPDATE bf_agents.limits SET max_retained_runs=1000000 WHERE tenant_id=%s',(A,))
        with pytest.raises(psycopg.errors.InsufficientPrivilege):db.execute('UPDATE bf_agents.usage SET retained_runs=0 WHERE tenant_id=%s',(A,))
        with pytest.raises(psycopg.errors.InsufficientPrivilege):db.execute('DELETE FROM bf_agents.events')
        db.execute('INSERT INTO bf_agents.memories(tenant_id,namespace,key,text,sha256,version) VALUES(%s,%s,%s,%s,%s,1)',(A,'default','restricted','data','a'*64))
        assert db.execute('SELECT memory_bytes FROM bf_agents.usage WHERE tenant_id=%s',(A,)).fetchone()[0]==4


async def test_coordinator_uses_actual_batch_http_pg_ledger_and_receipt(store):
    """Actual HTTP+PG; synthetic worker output, deliberately not GPU evidence."""
    import hashlib,httpx,json
    from backend.app import create_app as batch_app
    from backend.config import Settings
    from agents.batch import BatchAdapter
    from agents.runtime import Coordinator
    key='test-only-agent-http-key';token='test-only-agent-worker'
    with psycopg.connect(DSN,autocommit=True) as db:
        db.execute('INSERT INTO api_keys(key_hash,tenant_id) VALUES(%s,%s) ON CONFLICT DO NOTHING',(hashlib.sha256(key.encode()).hexdigest(),A))
    app=batch_app(Settings(database_url=DSN,model_name='test-model',batchforge_worker_token=token))
    async with app.router.lifespan_context(app),httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://127.0.0.1:8080') as client:
        internal={'Authorization':'Bearer '+token}
        response=await client.post('/internal/workers/register',headers=internal,json={
            'worker_id':'agent-http-test','name':'test-only synthetic model output','model':'test-model','concurrency':1})
        assert response.status_code==200,response.text
        adapter=BatchAdapter(store.pool,base_url='http://127.0.0.1:8080',tenant_keys={A:key},client=client)
        run=await make(store);coordinator=Coordinator(store,adapter,owner='test-http',tenants=[A])
        for _ in range(12):
            await coordinator.tick()
            value=await store.get(A,run['id'])
            if value['state'].get('phase')=='waiting_model':break
        assert value['state']['phase']=='waiting_model'
        claim=await client.post('/internal/tasks/claim',headers=internal,json={'worker_id':'agent-http-test','model':'test-model','limit':1})
        task=claim.json()['tasks'][0]
        model={'id':'synthetic-agent-test','object':'chat.completion','model':'test-model',
            'choices':[{'index':0,'message':{'role':'assistant','content':'{"type":"final","answer":"42","citations":[]}'},'finish_reason':'stop'}],
            'usage':{'prompt_tokens':80,'completion_tokens':20,'total_tokens':100}}
        complete=await client.post('/internal/tasks/'+task['id']+'/complete',headers=internal,json={
            'worker_id':'agent-http-test','attempt_token':task['attempt_token'],'response':model})
        assert complete.status_code==200,complete.text
        done=await advance_to_terminal(store,run,adapter)
        assert done['status']=='completed' and done['generated_tokens']==20
        assert len(done['result']['batch_receipts'])==1
        receipt=await client.get('/v1/batches/'+value['state']['batch_id']+'/receipt',headers={'Authorization':'Bearer '+key})
        assert receipt.json()['receipt']['history']['observed_accepted_attempts']==1


async def test_legacy_default_policy_hash_and_key_replay_survive_output_contract_upgrade(store):
    from agents.contracts import digest
    run=await make(store)
    old_policy={'allowed_tools':['math.calculate','memory.get','memory.search'],'memory_namespace':'default',
        'max_model_calls':8,'max_tool_calls':8,'max_generated_tokens':4096,'max_wall_seconds':900}
    old_hash=digest({'goal':'Compute invoice total using allowed tools','model':'test-model','policy':old_policy})
    async with store.pool.connection() as db:
        row=await (await db.execute('SELECT request_hash,policy FROM bf_agents.runs WHERE id=%s',(run['id'],))).fetchone()
    assert row=={'request_hash':old_hash,'policy':old_policy}
    assert (await make(store))['id']==run['id']
    with pytest.raises(AgentError) as exc:await make(store,policy=Policy(answer_format='identifier'))
    assert exc.value.code=='agent_idempotency_conflict'


async def test_output_correction_is_a_new_metered_model_step_not_gold_extraction(store):
    run=await make(store,policy=Policy(allowed_tools=(),answer_format='identifier',max_tool_calls=0,max_model_calls=2,max_generated_tokens=128))
    batch=TestBatch(['{"type":"final","answer":"The project code is ALPHA-42","citations":[]}',
                     '{"type":"final","answer":"ALPHA-42","citations":[]}'])
    done=await advance_to_terminal(store,run,batch)
    assert done['status']=='completed' and done['result']['answer']=='ALPHA-42'
    assert done['model_calls']==2 and done['generated_tokens']==40 and len(batch.batches)==2
    assert len(done['state']['output_rejections'])==1
    assert done['state']['output_rejections'][0]['receipt_sha256']=='a'*64
    # No reference answer is passed to the coordinator/validator.
    assert 'ALPHA-42' not in str(done['state']['request']['messages'])


async def test_output_correction_cannot_reset_or_overrun_model_budget(store):
    run=await make(store,policy=Policy(answer_format='number',max_model_calls=2,max_generated_tokens=128))
    batch=TestBatch(['{"type":"final","answer":"The result is 42"}']*2)
    done=await advance_to_terminal(store,run,batch)
    assert done['status']=='failed' and done['model_calls']==2 and done['generated_tokens']==40
    assert done['result']['code']=='model_call_budget_exceeded' and len(batch.batches)==2


async def test_schema_provider_violation_is_rejected_and_correction_is_metered(store):
    run=await make(store,policy=Policy(planner_protocol='schema-v1',answer_format='identifier',max_model_calls=2,max_generated_tokens=128))
    batch=TestBatch(['{"type":"memory.get","arguments":{"key":"untrusted"}}',
                     '{"type":"final","answer":"VALID-CODE","citations":[]}'])
    done=await advance_to_terminal(store,run,batch)
    assert done['status']=='completed' and done['model_calls']==2 and done['generated_tokens']==40
    assert done['tool_calls']==0 and done['state']['output_rejections'][0]['code']=='planner_json_invalid'
    assert done['state']['request']['response_format']['type']=='json_schema'


async def test_schema_does_not_turn_unobserved_citation_into_evidence(store):
    run=await make(store,policy=Policy(planner_protocol='schema-v1',answer_format='number',max_model_calls=2,max_generated_tokens=128))
    batch=TestBatch(['{"type":"final","answer":"42","citations":["math.calculate"]}',
                     '{"type":"final","answer":"42","citations":[]}'])
    done=await advance_to_terminal(store,run,batch)
    assert done['status']=='completed' and done['model_calls']==2 and done['generated_tokens']==40
    assert done['state']['output_rejections'][0]['code']=='citation_unobserved'
    assert done['result']['citations']==[]


async def test_provider_cannot_skip_an_immutable_tool_prerequisite_even_with_correct_answer(store):
    run=await make(store,policy=Policy(planner_protocol='schema-v1',answer_format='number',
        tool_sequence=('math.calculate',),max_model_calls=3,max_generated_tokens=128))
    batch=TestBatch(['{"type":"final","answer":"42","citations":[]}',
                     '{"type":"tool","name":"math.calculate","arguments":{"expression":"20+22"}}',
                     '{"type":"final","answer":"42","citations":[]}'])
    done=await advance_to_terminal(store,run,batch)
    assert done['status']=='completed' and done['model_calls']==3 and done['generated_tokens']==60
    assert done['tool_calls']==1 and done['state']['observations'][0]['result']=={'value':42}
    assert done['state']['output_rejections'][0]['code']=='tool_sequence_required'
    assert len(batch.batches)==3 and len(done['state']['receipts'])==3
    with pytest.raises(AgentError) as exc:
        await make(store,policy=Policy(tool_sequence=('memory.get',)))
    assert exc.value.code=='agent_idempotency_conflict'


async def test_out_of_order_granted_tool_is_refused_before_execution_without_budget_reset(store):
    run=await make(store,policy=Policy(planner_protocol='schema-v1',tool_sequence=('memory.get','math.calculate'),
        max_model_calls=1,max_generated_tokens=64))
    batch=TestBatch(['{"type":"tool","name":"math.calculate","arguments":{"expression":"20+22"}}'])
    done=await advance_to_terminal(store,run,batch)
    assert done['status']=='failed' and done['result']['code']=='model_call_budget_exceeded'
    assert done['tool_calls']==0 and done['model_calls']==1 and done['generated_tokens']==20
    assert done['state'].get('observations',[])==[] and len(batch.batches)==1


async def test_bounded_schema_canary_preserves_response_format_through_http_db_claim(store):
    import hashlib,httpx,json
    from backend.app import create_app as batch_app
    from backend.config import Settings
    from ops.qualify_agent_grammar import fixture
    from ops.product_campaign import MODEL
    key='test-only-schema-ingress-key';token='test-only-schema-worker'
    cases,_=fixture()
    with psycopg.connect(DSN,autocommit=True) as db:
        db.execute('INSERT INTO api_keys(key_hash,tenant_id) VALUES(%s,%s) ON CONFLICT DO NOTHING',(hashlib.sha256(key.encode()).hexdigest(),A))
    app=batch_app(Settings(database_url=DSN,model_name=MODEL,json_schema_enabled=True,batchforge_worker_token=token))
    async with app.router.lifespan_context(app),httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as client:
        public={'Authorization':'Bearer '+key};internal={'Authorization':'Bearer '+token}
        response=await client.post('/v1/files',headers=public,data={'purpose':'batch'},files={'file':('schema.jsonl',b'\n'.join(json.dumps(c).encode() for c in cases),'application/jsonl')})
        assert response.status_code==200,response.text
        created=await client.post('/v1/batches',headers={**public,'Idempotency-Key':'schema-test'},json={'input_file_id':response.json()['id']})
        assert created.status_code==200,created.text
        reg=await client.post('/internal/workers/register',headers=internal,json={'worker_id':'schema-http-test','name':'synthetic schema transport only','model':MODEL,'concurrency':4})
        assert reg.status_code==200,reg.text
        claim=await client.post('/internal/tasks/claim',headers=internal,json={'worker_id':'schema-http-test','model':MODEL,'limit':4})
        assert claim.status_code==200,claim.text
        tasks=claim.json()['tasks'];assert len(tasks)==4
        expected={c['custom_id']:c['body']['response_format'] for c in cases}
        assert {t['custom_id']:t['body']['response_format'] for t in tasks}==expected
        # Cancel only this isolated test cohort; no GPU or production process.
        assert (await client.post('/v1/batches/'+created.json()['id']+'/cancel',headers=public)).status_code==200


async def test_agent_list_avoids_fetching_model_inputs_results_or_memory_documents(store):
    run=await store.create(A,'long-goal',goal='G'*2000,model='test-model')
    page=await store.list(A,limit=1);row=page['data'][0]
    assert row['id']==run['id'] and row['goal_preview']=='G'*160 and row['goal_truncated']
    assert not {'state','result','policy','goal'}&set(row)
    assert (await store.get(A,run['id']))['goal']=='G'*2000


async def test_agent_mount_reuses_actual_parent_pool_auth_and_stays_off_by_default(store,tmp_path):
    import hashlib,json,httpx
    from backend.app import create_app as batch_app
    from backend.config import Settings
    key='test-only-shared-agent-console-key';token='test-only-shared-worker-token'
    with psycopg.connect(DSN,autocommit=True) as db:
        db.execute('INSERT INTO api_keys(key_hash,tenant_id) VALUES(%s,%s) ON CONFLICT DO NOTHING',(hashlib.sha256(key.encode()).hexdigest(),A))
    path=tmp_path/'executors.json';path.write_text(json.dumps({A:key}));path.chmod(0o600)
    app=batch_app(Settings(database_url=DSN,model_name='test-model',batchforge_worker_token=token,
        agent_api_enabled=True,agent_executor_keys_file=str(path),db_pool_min=1,db_pool_max=2))
    agent=next(r.app for r in app.routes if getattr(r,'path',None)=='/agents')
    async with app.router.lifespan_context(app),httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as c:
        assert agent.state.store.pool is app.state.pool
        headers={'Authorization':'Bearer '+key}
        assert (await c.get('/agents/console/')).status_code==200
        caps=(await c.get('/agents/v1/agent-capabilities',headers=headers)).json()
        assert caps['admission_enabled'] and caps['batch_console_path']=='/' and caps['planner_protocols']==['json-v1']
        result=await c.post('/agents/v1/agent-runs',headers={**headers,'Idempotency-Key':'shared-agent'},json={'goal':'test only','model':'test-model'})
        assert result.status_code==200,result.text
        assert (await c.get('/v1/overview',headers=headers)).json()['features']['agent_console']=='/agents/console/'
    assert app.state.pool.closed
    disabled=batch_app(Settings(database_url=DSN,model_name='test-model',batchforge_worker_token=token,db_pool_min=1,db_pool_max=2))
    async with disabled.router.lifespan_context(disabled),httpx.AsyncClient(transport=httpx.ASGITransport(app=disabled),base_url='http://test') as c:
        assert (await c.get('/agents/console/')).status_code==404
        assert (await c.get('/v1/overview',headers=headers)).json()['features']['agent_console'] is None


async def test_console_snapshot_preserves_float_commitments_and_scopes_receipt_before_lookup(store):
    import httpx
    from backend.config import Settings
    from agents.app import create_app
    from backend.receipts import DOMAIN,canonical
    from agents.contracts import digest
    run=await make(store);lease=await store.claim('synthetic-console-test',[A])
    state={'observations':[{'tool':'math.calculate','result':{'value':42.0},'elapsed_seconds':.125}],
           'receipts':[{'batch_id':'batch_one','receipt_sha256':'a'*64,'usage':{'prompt_tokens':20,'completion_tokens':10}}]}
    await store.transition(A,run['id'],lease['lease_token'],lease['revision'],'final',kind='run.completed',state=state,status='completed',result={'answer':'42'},model_calls=1,tool_calls=1,generated_tokens=10)
    calls=[]
    class Parent:
        pool=store.pool
        async def tenant(self,key):return A if key=='A' else B
        async def batch_receipt(self,tenant,bid):
            calls.append((tenant,bid));value={'batch_id':bid,'model':'test-model','ledger_state':'consistent_ledger',
                'counts':{'total':1,'completed':1},'checks':{'synthetic_test_consistency':True},'accepted_usage':{'prompt_tokens':20,'completion_tokens':10}}
            return {'protocol':'batchforge.receipt.v1','receipt':value,'receipt_sha256':__import__('hashlib').sha256(DOMAIN+canonical(value)).hexdigest()}
    app=create_app(Settings(model_name='test-model'),shared_backend=lambda:Parent(),executor_tenants=[A])
    async with app.router.lifespan_context(app),httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as c:
        a={'Authorization':'Bearer A'};b={'Authorization':'Bearer B'}
        snapshot=await c.get('/v1/agent-runs/'+run['id']+'/snapshot',headers=a);body=snapshot.json()
        assert snapshot.status_code==200 and '42.0' in body['state_json']
        assert __import__('hashlib').sha256(snapshot.content).hexdigest()==snapshot.headers['X-BatchForge-Agent-Snapshot-SHA256']
        assert __import__('hashlib').sha256(body['state_json'].encode()).hexdigest()==digest(state)
        path='/v1/agent-runs/'+run['id']+'/children/batch_one/receipt'
        assert (await c.get(path,headers=b)).status_code==404 and calls==[]
        assert (await c.get(path.replace('batch_one','batch_other'),headers=a)).status_code==404 and calls==[]
        result=await c.get(path,headers=a)
        assert result.status_code==200,result.text
        assert result.json()['accepted_usage_reconciled'] and calls==[(A,'batch_one')]
