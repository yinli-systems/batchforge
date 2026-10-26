// Network/state logic is separate from the DOM so uncertainty and workspace
// races are testable without making model calls or simulating GPU measurements.
export const terminal = status => ['completed','failed','cancelled'].includes(status);
export const displayStatus = run => run.status==='completed'&&(run.answer_disposition==='abstained'||run.result?.type==='abstain')?'abstained':run.status;
const zero='0'.repeat(64), attemptKey='batchforge-agent-pending-v1';
export class StaleWorkspace extends Error {}

export function retrievalSelection(caps,id) {
  if(!id)return null;
  const selected=(caps.retrieval_snapshots||[]).find(v=>v.id===id);
  if(!/^[0-9a-f]{64}$/.test(id)||!selected||! /^[A-Za-z0-9_-]{1,64}$/.test(selected.namespace)
     ||!caps.tools.some(t=>t.name==='retrieval.search'))throw new Error('The selected knowledge snapshot is unavailable.');
  return selected;
}

export function selectedTools(caps,snapshot) {
  retrievalSelection(caps,snapshot);
  return caps.tools.map(t=>t.name).filter(name=>name!=='retrieval.search'||!!snapshot);
}

export function canonical(value) {
  if(value===null || typeof value==='boolean')return JSON.stringify(value);
  if(typeof value==='string') {
    if([...value].some(c=>c.codePointAt(0)>=0xd800&&c.codePointAt(0)<=0xdfff))throw new Error('Invalid Unicode');
    return JSON.stringify(value);
  }
  if(typeof value==='number') {
    // Event payload counters are integers. State/result floats are verified
    // from their server-produced canonical JSON bytes instead of reserialized.
    if(!Number.isSafeInteger(value))throw new Error('Unqualified event number');
    return JSON.stringify(value);
  }
  if(Array.isArray(value))return '['+value.map(canonical).join(',')+']';
  if(value && Object.getPrototypeOf(value)===Object.prototype) {
    const compare=(a,b)=>{const x=[...a],y=[...b];for(let i=0;i<Math.min(x.length,y.length);i++){const d=x[i].codePointAt(0)-y[i].codePointAt(0);if(d)return d;}return x.length-y.length;};
    return '{'+Object.keys(value).sort(compare).map(k=>canonical(k)+':'+canonical(value[k])).join(',')+'}';
  }
  throw new Error('Unsupported evidence value');
}

export async function sha256(text,crypto=globalThis.crypto) {
  if(!crypto?.subtle)throw new Error('Evidence verification requires HTTPS or a localhost secure context.');
  const bytes=await crypto.subtle.digest('SHA-256',new TextEncoder().encode(text));
  return [...new Uint8Array(bytes)].map(v=>v.toString(16).padStart(2,'0')).join('');
}

export async function verifyHistory(snapshot,tenant,page,crypto=globalThis.crypto) {
  const run=snapshot.run,target=run.revision;
  if(!Number.isSafeInteger(target)||target<0||target>6400)throw new Error('History exceeds verification bound');
  let cursor=-1,previous=zero,events=[];
  for(let pages=0;pages<65 && cursor<target;pages++) {
    const value=await page(cursor);
    if(value.protocol!=='batchforge.agent-events-page.v1'||value.run_id!==run.id||value.after!==cursor
       ||value.previous_hash!==previous||value.revision<target||!Array.isArray(value.events)||value.events.length>100)
      throw new Error('History page identity or anchor differs');
    let moved=false;
    for(const e of value.events) {
      if(e.revision>target)break;
      if(e.revision!==cursor+1||e.previous_hash!==previous)throw new Error('History has a gap');
      const document={protocol:'batchforge.agent-event.v1',tenant_id:tenant,run_id:run.id,
        revision:e.revision,kind:e.kind,payload:e.payload,previous_hash:previous};
      if(await sha256(canonical(document),crypto)!==e.event_hash)throw new Error('History checksum differs');
      cursor=e.revision;previous=e.event_hash;events.push(e);moved=true;
    }
    if(!moved)throw new Error('History stopped before the selected snapshot');
  }
  if(cursor!==target||previous!==run.event_hash)throw new Error('History head differs from selected run');
  if(events[0]?.kind!=='run.created'||await sha256(canonical({goal:run.goal,model:run.model,policy:run.policy}),crypto)!==events[0].payload.request_sha256)
    throw new Error('Goal, model or immutable policy differs from creation');
  for(const counter of ['model_calls','tool_calls','generated_tokens']) {
    const sum=events.reduce((n,e)=>n+(e.payload[counter]||0),0);
    if(!Number.isSafeInteger(sum)||sum!==run[counter])throw new Error('Run counters differ from recorded transitions');
  }
  const stateAnchor=[...events].reverse().find(e=>typeof e.payload?.state_sha256==='string');
  let stateVerified=false,resultVerified=false;
  if(stateAnchor) {
    if(typeof snapshot.state_json!=='string'||typeof snapshot.result_json!=='string')throw new Error('Missing snapshot byte commitment');
    // Also bind the displayed parsed objects to those bytes. JSON equality is
    // semantic here; float spelling is preserved for the actual hash input.
    if(JSON.stringify(JSON.parse(snapshot.state_json))!==JSON.stringify(run.state)
       ||JSON.stringify(JSON.parse(snapshot.result_json))!==JSON.stringify(run.result)) {
      // Object key order may differ after JSONB serialization.
      const stable=v=>v&&typeof v==='object'?Array.isArray(v)?v.map(stable):Object.fromEntries(Object.keys(v).sort().map(k=>[k,stable(v[k])])):v;
      if(JSON.stringify(stable(JSON.parse(snapshot.state_json)))!==JSON.stringify(stable(run.state))
         ||JSON.stringify(stable(JSON.parse(snapshot.result_json)))!==JSON.stringify(stable(run.result)))throw new Error('Displayed snapshot differs from committed bytes');
    }
    stateVerified=await sha256(snapshot.state_json,crypto)===stateAnchor.payload.state_sha256;
    resultVerified=await sha256(snapshot.result_json,crypto)===stateAnchor.payload.result_sha256;
    if(!stateVerified)throw new Error('Snapshot state differs from event commitment');
  }
  const last=events.at(-1);
  if(['run.cancelled','run.deadline','workflow.cancelled'].includes(last?.kind))resultVerified=canonical(run.result)===canonical(last.payload);
  if(stateAnchor&&!resultVerified)throw new Error('Snapshot result differs from event commitment');
  return {revision:target,head:previous,events,stateVerified,resultVerified,
    scope:'Hash-chain consistency and available byte commitments; unsigned evidence.'};
}

function readAttempts(storage) {
  try {const raw=storage.getItem(attemptKey);if(!raw)return {};const v=JSON.parse(raw);
    if(!v||Array.isArray(v)||typeof v!=='object'||Object.keys(v).length>16)throw new Error();
    for(const [tenant,a] of Object.entries(v))if(!tenant||!a||typeof a.key!=='string'||a.key.length>128||typeof a.body!=='string'||a.body.length>65536)throw new Error();
    return v;
  }catch{throw new Error('Pending submission storage is unavailable or invalid. Preserve this tab before submitting.');}
}

// Default documents are deliberately identical to server Policy.document().
export function workflowChildPolicy(spec,node,parent) {
 const value={allowed_tools:node.allowed_tools??['math.calculate'],memory_namespace:spec.memory_namespace??'default',
  max_model_calls:node.max_model_calls??4,max_tool_calls:node.max_tool_calls??2,max_generated_tokens:node.max_generated_tokens??2048,
  max_wall_seconds:spec.max_wall_seconds??900,parent_workflow:parent,parent_node:node.id};
 for(const [key,defaultValue] of [['answer_format','text'],['require_citations',false],['planner_protocol','json-v1']])
  if((node[key]??defaultValue)!==defaultValue)value[key]=node[key];
 if(node.tool_sequence?.length)value.tool_sequence=node.tool_sequence;
 if(node.inherit_sources)value.inherit_sources=true;
 if(spec.retrieval_snapshot&&(value.allowed_tools.includes('retrieval.search')||node.inherit_sources))value.retrieval_snapshot=spec.retrieval_snapshot;
 return value;
}

export async function verifySources(response,current,tenant,fetchSnapshot,page,crypto=globalThis.crypto) {
 const run=current.run,doc=response.document,id=/^agrun_[a-f0-9]{32}$/;
 if(response.protocol!=='batchforge.workflow-sources-response.v1'||response.run_id!==run.id||response.revision!==run.revision
    ||!run.policy.inherit_sources||!id.test(doc?.workflow_id)||doc.workflow_id!==run.policy.parent_workflow
    ||doc.consumer_node!==run.policy.parent_node||doc.protocol!=='batchforge.workflow-sources.v1'
    ||!Array.isArray(doc.sources)||doc.sources.length>64||new TextEncoder().encode(canonical(doc)).length>16384
    ||await sha256(canonical(doc),crypto)!==response.document_sha256)throw Error('Source envelope identity or checksum differs');
 const verified=new Map(),snapshots=new Map([[run.id,current]]);
 async function load(rid) {
  if(!id.test(rid))throw Error('Invalid source run identity');
  if(!snapshots.has(rid)){if(snapshots.size>=17)throw Error('Source graph exceeds bound');snapshots.set(rid,await fetchSnapshot(rid));}
  const snapshot=snapshots.get(rid);
  if(snapshot.protocol!=='batchforge.agent-snapshot.v1'||snapshot.run?.id!==rid)throw Error('Source snapshot identity differs');
  if(!verified.has(rid)){
   const v=await verifyHistory(snapshot,tenant,after=>page(rid,after),crypto);
   if(!v.stateVerified||!v.resultVerified)throw Error('Source state and result commitments required');verified.set(rid,v);
  }
  return snapshot;
 }
 await load(run.id);const parent=await load(doc.workflow_id);
 if(parent.run.model!=='batchforge/workflow-v1')throw Error('Workflow program differs');
 const spec=JSON.parse(parent.run.goal),nodes=new Map((spec.nodes??[]).map(n=>[n.id,n]));
 if(nodes.size<1||nodes.size>16||!nodes.has(doc.consumer_node)||nodes.size!==spec.nodes.length)throw Error('Frozen source graph differs');
 const seen=new Set(),visiting=new Set();
 async function ancestor(nid){
  if(seen.has(nid))return;if(visiting.has(nid)||!nodes.has(nid))throw Error('Invalid source DAG');visiting.add(nid);
  const definition=nodes.get(nid),rid=parent.run.state.nodes?.[nid]?.run_id,snapshot=await load(rid),r=snapshot.run;
  if(r.goal!==definition.goal||r.model!==definition.model||canonical(r.policy)!==canonical(workflowChildPolicy(spec,definition,parent.run.id)))throw Error('Source policy differs from workflow declaration');
  for(const d of definition.depends_on??[])await ancestor(d);
  const expected=[];
  for(const d of definition.depends_on??[]){
   const child=snapshots.get(parent.run.state.nodes[d].run_id),c=child.run,result={answer:c.result?.answer,citations:c.result?.citations??[]};
   if(c.status!=='completed'||c.result?.type!=='final')throw Error('Dependency is not a committed answer');
   expected.push({node:d,run_id:c.id,result,result_sha256:await sha256(child.result_json,crypto),projection_sha256:await sha256(canonical(result),crypto),event_revision:c.revision,event_head_sha256:c.event_hash});
  }
  if(canonical(r.state.workflow_inputs??[])!==canonical(expected))throw Error('Declared dependency projection differs');
  visiting.delete(nid);seen.add(nid);
 }
 await ancestor(doc.consumer_node);
 const citations=new Set();
 for(const value of doc.sources){
  const c=value.citation,s=value.source;
  if(typeof c!=='string'||citations.has(c)||!Array.isArray(value.nodes)||value.nodes.length>16
    ||!Array.isArray(value.edges)||value.edges.length>256||!Array.isArray(value.origins)||!value.origins.length||value.origins.length>64)throw Error('Bounded unique source lineage required');
  citations.add(c);const declared=new Set();
  for(const anchor of value.nodes){
   if(!seen.has(anchor.node)||anchor.node===doc.consumer_node||declared.has(anchor.node)||parent.run.state.nodes[anchor.node].run_id!==anchor.run_id)throw Error('Undeclared source node');
   declared.add(anchor.node);const snapshot=await load(anchor.run_id),r=snapshot.run;
   if(r.status!=='completed'||r.result?.type!=='final'||!r.result.citations.includes(c)||r.revision!==anchor.revision||r.event_hash!==anchor.event_head_sha256
      ||await sha256(snapshot.state_json,crypto)!==anchor.state_sha256||await sha256(snapshot.result_json,crypto)!==anchor.result_sha256)throw Error('Upstream source commit differs');
  }
  for(const edge of value.edges){
   if(!declared.has(edge.from)||!(declared.has(edge.to)||edge.to===doc.consumer_node)||!nodes.get(edge.to)?.depends_on?.includes(edge.from))throw Error('Source edge not declared by workflow');
  }
  function reaches(n,visited=new Set()){
   if(n===doc.consumer_node)return true;if(visited.has(n))return false;visited.add(n);
   return value.edges.filter(e=>e.from===n).some(e=>reaches(e.to,new Set(visited)));
  }
  if([...declared].some(n=>!reaches(n)))throw Error('Disconnected source lineage');
  const text=s.kind==='retrieval'?s.match?.text:s.text;
  if(typeof text!=='string'||new TextEncoder().encode(text).length>4096)throw Error('Bounded source text required');
  if(s.kind==='memory'){
   if(s.namespace!==run.policy.memory_namespace||c!==`memory:${s.namespace}:${s.key}:v${s.version}:${s.document_sha256}`
      ||await sha256(text,crypto)!==s.text_sha256||!s.text_truncated&&s.document_sha256!==s.text_sha256)throw Error('Memory source bytes differ');
  }else if(s.kind==='retrieval'){
   const m=s.match;
   if(s.snapshot_id!==run.policy.retrieval_snapshot||c!==m.citation||await sha256(text,crypto)!==m.text_sha256
      ||m.end_byte-m.start_byte!==new TextEncoder().encode(text).length
      ||c!==`retrieval:${s.snapshot_id}:${m.document_id}:v${m.document_version}:${m.start_byte}:${m.end_byte}:${m.text_sha256}`)throw Error('Retrieval source bytes differ');
  }else throw Error('Unsupported source kind');
  for(const origin of value.origins){
   if(!declared.has(origin.node))throw Error('Source origin has no committed node');
   const r=snapshots.get(parent.run.state.nodes[origin.node].run_id).run;
   const matched=(r.state.observations??[]).some(o=>{
    if(o.result_sha256!==origin.observation_sha256||!r.policy.allowed_tools.includes(o.tool))return false;
    if(s.kind==='retrieval'){
     if(o.tool!=='retrieval.search'||o.result.snapshot_id!==s.snapshot_id)return false;
     return o.result.matches.some(m=>canonical(Object.fromEntries(Object.entries(m).filter(([k])=>k!=='score')))===canonical(s.match));
    }
    if(!['memory.get','memory.search'].includes(o.tool))return false;
    return (o.tool==='memory.get'?[o.result]:o.result.matches).some(m=>m.citation===c&&m.namespace===s.namespace&&m.key===s.key&&m.version===s.version&&m.sha256===s.document_sha256&&m.text.startsWith(s.text));
   });
   if(!matched)throw Error('Source does not occur in committed upstream tool result');
  }
 }
 if(run.status==='completed'&&run.result?.type==='final'){
  const selected={...doc,sources:doc.sources.filter(s=>run.result.citations.includes(s.citation))};
  if(canonical(selected)!==canonical(run.result.dependency_evidence))throw Error('Delivered source proof differs from committed result');
 }
 return {verified:true,document:doc,document_sha256:response.document_sha256,workflow_id:doc.workflow_id,
  sources:doc.sources.length,verified_runs:verified.size,scope:'Independent event/snapshot, declaration and source-membership checks; not semantic correctness or proof of model attention.'};
}


export class AgentClient {
  constructor({base='/v1/',fetch=globalThis.fetch,crypto=globalThis.crypto,storage=globalThis.sessionStorage}={}) {
    if(!/^\/(?:[A-Za-z0-9_-]+\/)*v1\/$/.test(base))throw new Error('Same-origin Agent API path required');
    Object.assign(this,{base,fetch,crypto,storage,epoch:0,selection:0,key:'',caps:null,runs:[],detail:null,verified:null,sourceVerified:null});
    try{this.attempts=readAttempts(storage);}catch(error){this.attempts={};this.storageError=error;}
  }
  setKey(value) {
    this.epoch++;this.selection++;this.key=value;this.caps=null;this.runs=[];this.detail=null;this.verified=null;this.sourceVerified=null;this.nextCursor=null;
  }
  async request(path,{method='GET',body,key=this.key}={}) {
    if(!key)throw new Error('Connect a workspace first.');
    if(!/^[a-z0-9][A-Za-z0-9_?=&%/.-]*$/.test(path)||path.includes('..'))throw new Error('Invalid API path');
    const headers={'Authorization':'Bearer '+key};if(body!==undefined)headers['Content-Type']='application/json';
    const r=await this.fetch(this.base+path,{method,headers,body:body===undefined?undefined:typeof body==='string'?body:JSON.stringify(body),
      cache:'no-store',credentials:'omit',redirect:'error',referrerPolicy:'no-referrer'});
    const raw=await r.text();if(raw.length>4*1024*1024)throw new Error('API response exceeds bound');
    let value;try{value=JSON.parse(raw);}catch{throw new Error('The Agent service returned an invalid response.');}
    if(!r.ok) {const code=value?.error?.code;const error=new Error(typeof code==='string'&&/^[a-z0-9_]{1,80}$/.test(code)?code:'Agent request failed');error.status=r.status;throw error;}
    return value;
  }
  async connect() {
    const epoch=this.epoch,value=await this.request('agent-capabilities');
    if(epoch!==this.epoch)throw new StaleWorkspace();
    if(value.protocol!=='batchforge.agent-capabilities.v1'||typeof value.tenant_id!=='string'||!Array.isArray(value.models))throw new Error('Agent capability response is invalid.');
    this.caps=value;return value;
  }
  pending() {return this.caps&&Object.hasOwn(this.attempts,this.caps.tenant_id)?this.attempts[this.caps.tenant_id]:null;}
  saveAttempts(value) {
    const raw=JSON.stringify(value);this.storage.setItem(attemptKey,raw);
    if(this.storage.getItem(attemptKey)!==raw)throw new Error('Cannot preserve the submission key in this tab.');
    this.attempts=value;
  }
  async submit(body) {
    if(this.storageError)throw this.storageError;
    if(!this.caps?.admission_enabled)throw new Error('Agent admission is unavailable.');
    const epoch=this.epoch,tenant=this.caps.tenant_id,raw=JSON.stringify(body);let a=this.pending();
    if(raw.length>65536)throw new Error('Submission JSON exceeds the64 KiB client limit.');
    if(!a&&(!this.caps.models.includes(body.model)||!this.caps.planner_protocols.includes(body.planner_protocol)))throw new Error('The selected model or planner protocol is unavailable.');
    if(a&&a.body!==raw&&!a.rejected)throw new Error('Resolve the pending submission before changing its goal or policy.');
    if(!a) {
      if(Object.keys(this.attempts).length>=16)throw new Error('Resolve existing pending submissions first.');
      const bytes=this.crypto.getRandomValues(new Uint8Array(16));
      a={key:'console-agent-'+[...bytes].map(b=>b.toString(16).padStart(2,'0')).join(''),body:raw,uncertain:false};
      this.saveAttempts({...this.attempts,[tenant]:a});
    }
    if(a.rejected)a={...a,body:raw,rejected:false};
    const wasUncertain=a.uncertain!==false;
    a={...a,uncertain:true};this.saveAttempts({...this.attempts,[tenant]:a});
    // No automatic creation retry with a new key, including after a reload.
    const r=await this.fetch(this.base+'agent-runs',{method:'POST',headers:{'Authorization':'Bearer '+this.key,'Content-Type':'application/json','Idempotency-Key':a.key},
      body:a.body,cache:'no-store',credentials:'omit',redirect:'error',referrerPolicy:'no-referrer'});
    const value=await r.json();
    if(epoch!==this.epoch||this.caps?.tenant_id!==tenant)throw new StaleWorkspace();
    if(!r.ok){
      const code=value?.error?.code;
      if(!wasUncertain&&[400,403,413,422,429].includes(r.status)&&[
        'invalid_agent_policy','invalid_agent_request','invalid_agent_value','invalid_agent_run',
        'agent_schema_ingress_not_enabled','agent_model_not_served','agent_request_too_large','agent_quota_reached','retrieval_snapshot_not_granted'].includes(code)) {
        // A definite first refusal permits correction under the SAME key. If
        // another request ever accepted it, the server's body hash still fences
        // the corrected request. An earlier lost ACK never takes this branch.
        this.saveAttempts({...this.attempts,[tenant]:{...a,rejected:true,uncertain:false}});
        throw new Error(code);
      }
      throw new Error('Submission is unresolved; retry the same saved request.');
    }
    if(!/^agrun_[a-f0-9]{32}$/.test(value.id))throw new Error('Submission acknowledgement is invalid.');
    const rest={...this.attempts};delete rest[tenant];this.saveAttempts(rest);return value;
  }
  async list(more=false) {
    if(!this.caps)return;
    const epoch=this.epoch,anchor=more?this.nextCursor:null;
    const value=await this.request('agent-runs?limit=10'+(anchor?'&after='+encodeURIComponent(anchor):''));
    if(epoch!==this.epoch)throw new StaleWorkspace();
    if(!Array.isArray(value.data))throw new Error('Run list is invalid.');
    this.runs=more?[...this.runs,...value.data.filter(r=>!this.runs.some(old=>old.id===r.id))]:value.data;
    this.nextCursor=value.has_more?value.next_cursor:null;return this.runs;
  }
  async select(id) {
    if(!/^agrun_[a-f0-9]{32}$/.test(id))throw new Error('Invalid run identity.');
    const epoch=this.epoch,selection=++this.selection;this.detail=null;this.verified=null;this.sourceVerified=null;
    const snapshot=await this.request('agent-runs/'+id+'/snapshot');
    if(epoch!==this.epoch||selection!==this.selection)throw new StaleWorkspace();
    if(snapshot.protocol!=='batchforge.agent-snapshot.v1'||snapshot.run?.id!==id)throw new Error('Snapshot identity differs.');
    this.detail=snapshot;return snapshot;
  }
  async verify() {
    const epoch=this.epoch,selection=this.selection,snapshot=this.detail,tenant=this.caps?.tenant_id;
    if(!snapshot||!tenant)throw new Error('Select a run first.');
    const value=await verifyHistory(snapshot,tenant,after=>this.request('agent-runs/'+snapshot.run.id+'/events?after='+after+'&limit=100'),this.crypto);
    if(epoch!==this.epoch||selection!==this.selection)throw new StaleWorkspace();
    this.verified=value;return value;
  }
  async cancel() {
    const snapshot=this.detail,epoch=this.epoch,selection=this.selection;
    if(!snapshot||!this.caps?.cancellation_enabled)throw new Error('Cancellation is unavailable.');
    const result=await this.request('agent-runs/'+snapshot.run.id+'/cancel',{method:'POST'});
    if(epoch!==this.epoch||selection!==this.selection)throw new StaleWorkspace();
    return result;
  }
  async sources() {
    const epoch=this.epoch,selection=this.selection,snapshot=this.detail,tenant=this.caps?.tenant_id;
    if(!snapshot||!tenant||!this.caps.source_lineage_enabled)throw Error('Source verification is unavailable.');
    const guard=()=>{if(epoch!==this.epoch||selection!==this.selection)throw new StaleWorkspace();};
    const response=await this.request('agent-runs/'+snapshot.run.id+'/sources');guard();
    const result=await verifySources(response,snapshot,tenant,
      id=>{guard();return this.request('agent-runs/'+id+'/snapshot');},(id,after)=>{guard();return this.request('agent-runs/'+id+'/events?after='+after+'&limit=100');},this.crypto);
    if(epoch!==this.epoch||selection!==this.selection)throw new StaleWorkspace();
    this.sourceVerified=result;return result;
  }
  async receipt(bid) {
    if(!/^batch_[A-Za-z0-9_-]{1,80}$/.test(bid)||!this.detail)throw new Error('Invalid child identity.');
    const epoch=this.epoch,selection=this.selection,id=this.detail.run.id;
    const result=await this.request('agent-runs/'+id+'/children/'+bid+'/receipt');
    if(epoch!==this.epoch||selection!==this.selection)throw new StaleWorkspace();
    const envelope=result.current,receipt=envelope?.receipt;
    const committed=this.detail.run.state.receipts.find(c=>c.batch_id===bid);
    if(result.protocol!=='batchforge.agent-child-receipt.v1'||result.run_id!==id||result.batch_id!==bid||!committed
       ||!result.accepted_usage_reconciled||envelope?.protocol!=='batchforge.receipt.v1'
       ||await sha256('batchforge.receipt.v1\0'+canonical(receipt),this.crypto)!==envelope.receipt_sha256
       ||receipt.batch_id!==bid||receipt.model!==this.detail.run.model||receipt.ledger_state!=='consistent_ledger'
       ||receipt.counts?.total!==1||receipt.counts?.completed!==1||!receipt.checks||!Object.values(receipt.checks).every(v=>v===true)
       ||canonical(receipt.accepted_usage)!==canonical(committed.usage)||result.committed_receipt_sha256!==committed.receipt_sha256)
      throw new Error('Child receipt checksum, identity or usage differs.');
    return result;
  }
}
