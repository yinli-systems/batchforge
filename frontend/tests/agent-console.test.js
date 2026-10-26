// CPU network/evidence regressions, not browser visual QA or GPU evidence.
const assert=require('node:assert/strict');
const {test}=require('node:test');
const {readFileSync}=require('node:fs');
const {webcrypto}=require('node:crypto');
const path=require('node:path');
const vm=require('node:vm');
const source=readFileSync(path.join(__dirname,'../../agents/console/core.js'),'utf8');
const modulePromise=import('data:text/javascript;base64,'+Buffer.from(source).toString('base64'));
const rid='agrun_'+'a'.repeat(32),other='agrun_'+'b'.repeat(32);
const response=(value,status=200)=>({ok:status<300,status,text:async()=>JSON.stringify(value),json:async()=>value});
function storage(){const map=new Map();return {getItem:k=>map.get(k)??null,setItem:(k,v)=>map.set(k,String(v)),removeItem:k=>map.delete(k)};}
const caps=(tenant='tenant-A')=>({protocol:'batchforge.agent-capabilities.v1',tenant_id:tenant,models:['test-model'],planner_protocols:['json-v1','schema-v1'],admission_enabled:true});
const body={goal:'Compute20+22',model:'test-model',planner_protocol:'schema-v1'};
async function client(fetch,s=storage()) {const {AgentClient}=await modulePromise;const c=new AgentClient({fetch,storage:s,crypto:webcrypto});c.setKey('tenant-key');await c.connect();return c;}

test('lost create acknowledgement survives reload with the original body and key',async()=>{
 const calls=[],s=storage();let creates=0;
 const fetch=async(url,r)=>{if(url.endsWith('agent-capabilities'))return response(caps());calls.push(r);if(++creates===1)throw new TypeError('ack lost');return response({id:rid});};
 const first=await client(fetch,s);await assert.rejects(first.submit(body));
 const reloaded=await client(fetch,s);assert.equal(reloaded.pending().body,JSON.stringify(body));
 assert.equal((await reloaded.submit(body)).id,rid);
 assert.equal(calls[0].headers['Idempotency-Key'],calls[1].headers['Idempotency-Key']);assert.equal(calls[0].body,calls[1].body);
 assert.equal(reloaded.pending(),null);assert.equal(calls[0].redirect,'error');assert.equal(calls[0].credentials,'omit');
});

test('changing an unresolved goal cannot silently allocate another task identity',async()=>{
 let posts=0;const c=await client(async(url)=>{if(url.endsWith('agent-capabilities'))return response(caps());posts++;throw new Error('uncertain');});
 await assert.rejects(c.submit(body));await assert.rejects(c.submit({...body,goal:'Other goal'}),/pending submission/);assert.equal(posts,1);
});

test('a definite first validation refusal can be corrected using the same key',async()=>{
 let posts=[];const c=await client(async(url,r)=>{if(url.endsWith('agent-capabilities'))return response(caps());posts.push(r);return posts.length===1?response({error:{code:'invalid_agent_policy'}},422):response({id:rid});});
 await assert.rejects(c.submit(body),/invalid_agent_policy/);assert.ok(c.pending().rejected);
 await c.submit({...body,max_tool_calls:2});assert.equal(posts[0].headers['Idempotency-Key'],posts[1].headers['Idempotency-Key']);assert.equal(c.pending(),null);
});

test('a lost ACK followed by a validation refusal stays unresolved and cannot change body',async()=>{
 let posts=0;const c=await client(async url=>{if(url.endsWith('agent-capabilities'))return response(caps());if(++posts===1)throw new Error('lost ACK');return response({error:{code:'agent_model_not_served'}},400);});
 await assert.rejects(c.submit(body));await assert.rejects(c.submit(body),/unresolved/);assert.ok(!c.pending().rejected);
 await assert.rejects(c.submit({...body,goal:'different'}),/pending submission/);assert.equal(posts,2);
});

test('an old workspace create acknowledgement cannot expose or clear its pending request in the next workspace',async()=>{
 let c;const fetch=async(url)=>{if(url.endsWith('agent-capabilities'))return response(caps());c.setKey('another-key');return response({id:rid});};
 c=await client(fetch);const {StaleWorkspace}=await modulePromise;await assert.rejects(c.submit(body),StaleWorkspace);
 assert.equal(c.pending(),null);assert.equal(c.detail,null);assert.equal(c.runs.length,0);
 c.setKey('tenant-key');await c.connect();assert.ok(c.pending());
});

test('a stale list response cannot populate a different workspace',async()=>{
 let c;const fetch=async(url)=>{if(url.endsWith('agent-capabilities'))return response(caps());c.setKey('next-key');return response({data:[{id:rid}],has_more:false});};
 c=await client(fetch);await assert.rejects(c.list());assert.equal(c.runs.length,0);
});

test('selection races retain only the most recently selected run',async()=>{
 const pending=[];const c=await client(async(url)=>url.endsWith('agent-capabilities')?response(caps()):new Promise(resolve=>pending.push(resolve)));
 const first=c.select(rid);const second=c.select(other);
 pending[1](response({protocol:'batchforge.agent-snapshot.v1',run:{id:other}}));await second;
 pending[0](response({protocol:'batchforge.agent-snapshot.v1',run:{id:rid}}));await assert.rejects(first);assert.equal(c.detail.run.id,other);
});

test('unsupported planning and read-only admission are refused before a POST',async()=>{
 let calls=0;const c=await client(async()=>{calls++;return response({...caps(),planner_protocols:['json-v1']});});
 await assert.rejects(c.submit(body),/unavailable/);assert.equal(calls,1);assert.equal(c.pending(),null);
 c.caps.admission_enabled=false;await assert.rejects(c.submit({...body,planner_protocol:'json-v1'}),/admission/);assert.equal(calls,1);
});

test('storage failure does not prevent reading but forbids an unpreserved create',async()=>{
 let calls=0;const s={getItem(){throw new Error('disabled');},setItem(){throw new Error('disabled');}};
 const c=await client(async()=>{calls++;return response(caps());},s);
 assert.equal(c.caps.tenant_id,'tenant-A');await assert.rejects(c.submit(body),/storage/);assert.equal(calls,1);
});

test('tenant identifiers cannot inherit a phantom pending submission from Object.prototype',async()=>{
 const c=await client(async()=>response(caps('constructor')));assert.equal(c.pending(),null);
 c.caps.tenant_id='__proto__';assert.equal(c.pending(),null);
});

async function fixture(cancel=false) {
 const {canonical,sha256}=await modulePromise;
 const run={id:rid,goal:'Compute20+22',model:'test-model',policy:{allowed_tools:['math.calculate']},
  state:{observations:[{result:{value:42}}]},result:cancel?{source:'tenant_request'}:{answer:'42'},
  revision:1,model_calls:cancel?0:1,tool_calls:cancel?0:1,generated_tokens:cancel?0:10};
 const state_json='{"observations":[{"result":{"value":42.0}}]}',result_json=JSON.stringify(run.result);
 const created={revision:0,kind:'run.created',payload:{request_sha256:await sha256(canonical({goal:run.goal,model:run.model,policy:run.policy}),webcrypto)},previous_hash:'0'.repeat(64)};
 const eventHash=async e=>sha256(canonical({protocol:'batchforge.agent-event.v1',tenant_id:'tenant-A',run_id:rid,revision:e.revision,kind:e.kind,payload:e.payload,previous_hash:e.previous_hash}),webcrypto);
 created.event_hash=await eventHash(created);
 const final={revision:1,kind:cancel?'run.cancelled':'run.completed',previous_hash:created.event_hash,
  payload:cancel?{source:'tenant_request'}:{state_sha256:await sha256(state_json,webcrypto),result_sha256:await sha256(result_json,webcrypto),model_calls:1,tool_calls:1,generated_tokens:10,status:'completed'}};
 final.event_hash=await eventHash(final);run.event_hash=final.event_hash;
 const page=async after=>({protocol:'batchforge.agent-events-page.v1',run_id:rid,after,previous_hash:after<0?'0'.repeat(64):created.event_hash,
  revision:1,head_sha256:run.event_hash,events:after<0?[created]:[final],has_more:after<0});
 return {snapshot:{protocol:'batchforge.agent-snapshot.v1',run,state_json,result_json},page,created,final};
}

test('event verification preserves Python float bytes instead of JS reserialization',async()=>{
 const {verifyHistory}=await modulePromise,{snapshot,page}=await fixture();
 const result=await verifyHistory(snapshot,'tenant-A',page,webcrypto);assert.ok(result.stateVerified&&result.resultVerified);assert.equal(result.events.length,2);
 snapshot.state_json=JSON.stringify(snapshot.run.state);await assert.rejects(verifyHistory(snapshot,'tenant-A',page,webcrypto),/state differs/);
});

test('changed goal, wrong tenant and forged event payload never acquire a verified label',async()=>{
 const {verifyHistory}=await modulePromise;let f=await fixture();f.snapshot.run.goal='different';await assert.rejects(verifyHistory(f.snapshot,'tenant-A',f.page,webcrypto),/policy differs/);
 f=await fixture();await assert.rejects(verifyHistory(f.snapshot,'tenant-B',f.page,webcrypto),/checksum differs/);
 f=await fixture();f.final.payload.generated_tokens=11;await assert.rejects(verifyHistory(f.snapshot,'tenant-A',f.page,webcrypto),/checksum differs/);
});

test('cancellation result is bound to its own terminal event rather than a prior transition',async()=>{
 const {verifyHistory}=await modulePromise,{snapshot,page}=await fixture(true);const result=await verifyHistory(snapshot,'tenant-A',page,webcrypto);
 assert.ok(result.resultVerified);assert.equal(result.stateVerified,false);
});

test('same-origin API paths reject an external or traversal target',async()=>{
 const {AgentClient}=await modulePromise;
 for(const base of ['https://other/v1/','//other/v1/','/../v1/'])assert.throws(()=>new AgentClient({base,storage:storage()}));
 const c=await client(async()=>response(caps()));await assert.rejects(c.request('../admin'),/Invalid API path/);
});

test('the actual console startup and connection remain usable when the browser storage getter throws',async()=>{
 const core=await modulePromise,nodes=new Map(),requests=[];
 class Element {
   constructor(){this.handlers={};this.children=[];this.options=[];this.value='';}
   addEventListener(name,fn){this.handlers[name]=fn;}
   replaceChildren(...values){this.children=values;this.options=values;}
   append(...values){this.children.push(...values);this.options=this.children;}
   reset(){}
   set innerHTML(_){throw new Error('Untrusted renderer must use text nodes');}
 }
 const document={getElementById(id){if(!nodes.has(id))nodes.set(id,new Element());return nodes.get(id);},createElement(){return new Element();},hidden:false};
 const fetch=async(url,options)=>{requests.push(options.method);return url.endsWith('agent-capabilities')?response({...caps(),tools:[{name:'math.calculate'}],memory_writes_enabled:true}):response({data:[],has_more:false});};
 const context=vm.createContext({document,location:{href:'http://127.0.0.1/console/'},URL,TextEncoder,setInterval(){},console,
   injectedCore:{...core,AgentClient:class extends core.AgentClient{constructor(options){super({...options,fetch,crypto:webcrypto});}}}});
 Object.defineProperty(context,'sessionStorage',{get(){throw new Error('Browser storage denied');}});
 const app=readFileSync(path.join(__dirname,'../../agents/console/app.js'),'utf8').replace(/^import[^\n]+/,'const {AgentClient,StaleWorkspace,terminal,retrievalSelection,selectedTools,displayStatus}=injectedCore;');
 vm.runInContext(app,context);document.getElementById('key').value='test-key';
 document.getElementById('connect-form').handlers.submit({preventDefault(){}});
 for(let i=0;i<20&&document.getElementById('workspace').hidden!==false;i++)await new Promise(resolve=>setImmediate(resolve));
 assert.equal(document.getElementById('workspace').hidden,false);assert.equal(document.getElementById('submit').disabled,true);
 assert.match(document.getElementById('notice').textContent,/Submission-key storage is unavailable/);assert.ok(requests.every(method=>method==='GET'));
});

test('retrieval requires an explicit currently granted snapshot and never expands default tools',async()=>{
 const {selectedTools,retrievalSelection}=await modulePromise,id='c'.repeat(64);
 const capabilities={tools:[{name:'math.calculate'},{name:'retrieval.search'}],retrieval_snapshots:[{id,namespace:'facts',source:{name:'A'}}]};
 assert.deepEqual(selectedTools(capabilities,''),['math.calculate']);
 assert.deepEqual(selectedTools(capabilities,id),['math.calculate','retrieval.search']);
 assert.equal(retrievalSelection(capabilities,id).namespace,'facts');
 assert.throws(()=>retrievalSelection({...capabilities,retrieval_snapshots:[]},id),/unavailable/);
 assert.throws(()=>retrievalSelection({...capabilities,tools:[]},id),/unavailable/);
 assert.throws(()=>retrievalSelection(capabilities,'../private'),/unavailable/);
});

test('a definite first retrieval-grant refusal is corrected under the same submission key',async()=>{
 const posts=[];const c=await client(async(url,r)=>{
  if(url.endsWith('agent-capabilities'))return response(caps());posts.push(r);
  return posts.length===1?response({error:{code:'retrieval_snapshot_not_granted'}},403):response({id:rid});
 });
 await assert.rejects(c.submit({...body,retrieval_snapshot:'a'.repeat(64)}),/retrieval_snapshot_not_granted/);
 assert.ok(c.pending().rejected);await c.submit(body);
 assert.equal(posts[0].headers['Idempotency-Key'],posts[1].headers['Idempotency-Key']);
});

test('child checks also verify the checksum and committed model usage in the client',async()=>{
 const {canonical,sha256}=await modulePromise;
 const committed={batch_id:'batch_one',receipt_sha256:'a'.repeat(64),usage:{prompt_tokens:20,completion_tokens:10}};
 const receipt={batch_id:'batch_one',model:'test-model',ledger_state:'consistent_ledger',counts:{total:1,completed:1},checks:{test_only:true},accepted_usage:{...committed.usage}};
 const envelope={protocol:'batchforge.receipt.v1',receipt,receipt_sha256:await sha256('batchforge.receipt.v1\0'+canonical(receipt),webcrypto)};
 const c=await client(async url=>url.endsWith('agent-capabilities')?response(caps()):response({protocol:'batchforge.agent-child-receipt.v1',run_id:rid,batch_id:'batch_one',committed_receipt_sha256:committed.receipt_sha256,accepted_usage_reconciled:true,current:envelope}));
 c.detail={run:{id:rid,model:'test-model',state:{receipts:[committed]}}};assert.equal((await c.receipt('batch_one')).accepted_usage_reconciled,true);
 receipt.accepted_usage.completion_tokens=11;await assert.rejects(c.receipt('batch_one'),/differs/);
});


test('typed abstention stays distinct from a completed answer in list and detail projections',async()=>{
 const {displayStatus}=await modulePromise;
 assert.equal(displayStatus({status:'completed',answer_disposition:'abstained'}),'abstained');
 assert.equal(displayStatus({status:'completed',result:{type:'abstain'}}),'abstained');
 assert.equal(displayStatus({status:'completed',result:{type:'final'}}),'completed');
 assert.equal(displayStatus({status:'running',result:{type:'abstain'}}),'running');
});

test('source verification stops before follow-up requests when workspace or selection changes',async()=>{
 for(const changed of ['workspace','selection']){
  let c,calls=0;
  c=await client(async url=>{calls++;if(url.endsWith('agent-capabilities'))return response({...caps(),source_lineage_enabled:true});
   if(changed==='workspace')c.setKey('new-workspace');else c.selection++;
   return response({});});
  c.detail={run:{id:rid,policy:{inherit_sources:true}}};await assert.rejects(c.sources());
  assert.equal(calls,2);assert.equal(c.sourceVerified,null);
 }
});

test('workflow-child cancellation keeps its own terminal result commitment',async()=>{
 const {verifyHistory,canonical,sha256}=await modulePromise,f=await fixture(true);
 f.final.kind='workflow.cancelled';f.final.event_hash=await sha256(canonical({protocol:'batchforge.agent-event.v1',tenant_id:'tenant-A',run_id:rid,revision:1,
  kind:f.final.kind,payload:f.final.payload,previous_hash:f.created.event_hash}),webcrypto);
 f.snapshot.run.event_hash=f.final.event_hash;
 const result=await verifyHistory(f.snapshot,'tenant-A',f.page,webcrypto);assert.ok(result.resultVerified);
});
