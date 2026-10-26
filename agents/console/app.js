import {AgentClient,StaleWorkspace,terminal} from './core.js';

const $=id=>document.getElementById(id);
const base=new URL('../v1/',location.href).pathname;
// Browser storage access itself can throw. Delay it until the client's guarded
// read, keeping authenticated inspection usable when persistence is disabled.
const tabStorage={getItem:key=>globalThis.sessionStorage.getItem(key),setItem:(key,value)=>globalThis.sessionStorage.setItem(key,value),removeItem:key=>globalThis.sessionStorage.removeItem(key)};
const client=new AgentClient({base,storage:tabStorage});
let busy=false,selectedId=null,refreshing=false,verifying=false;
const keyStorage='batchforge-api-key';
function rememberKey(key){try{key?tabStorage.setItem(keyStorage,key):tabStorage.removeItem(keyStorage);}catch{}}
const count=n=>Number.isSafeInteger(n)?new Intl.NumberFormat().format(n):'—';
const date=value=>value?new Date(value).toLocaleString():'—';
const element=(tag,text,cls)=>{const e=document.createElement(tag);if(text!==undefined)e.textContent=String(text);if(cls)e.className=cls;return e;};
const button=(text,action,cls='')=>{const e=element('button',text,cls);e.type='button';e.addEventListener('click',()=>perform(action));return e;};
function notice(text,error=false){$('notice').textContent=text;$('notice').className=error?'error':'';$('notice').hidden=!text;}
function human(error) {
  return ({agent_quota_reached:'Workspace capacity has been reached. Existing runs remain available.',
    invalid_api_key:'The workspace key was not accepted.',admission_paused:'New workflows are paused. Existing runs remain available.',
    agent_schema_ingress_not_enabled:'Structured planning is unavailable on this deployment.',
    agent_memory_version_conflict:'The memory version changed. Read its current version before saving.',
    agent_child_ledger_drift:'The current child ledger does not match this run. The receipt is not verified.',
    agent_read_only_snapshot:'This workspace is a read-only snapshot.'})[error.message]||error.message||'Request unavailable.';
}
async function perform(action) {
  const epoch=client.epoch;
  try{await action();}catch(error){if(!(error instanceof StaleWorkspace)&&epoch===client.epoch)notice(human(error),true);}
}
function statusChip(status){return element('span',status,'chip '+(status==='completed'?'good':status==='failed'?'bad':''));}
function renderRuns(){
  const list=$('run-list');list.replaceChildren();$('run-count').textContent=count(client.runs.length)+' shown';
  if(!client.runs.length)list.append(element('div','No workflows submitted yet.','empty'));
  for(const run of client.runs) {
    const row=button('',()=>openRun(run.id),'run-row'+(run.id===selectedId?' selected':''));
    const top=element('div',undefined,'row-top');top.append(element('span',run.id.slice(0,18),'run-meta'),statusChip(run.status));
    row.append(top,element('div',(run.goal_preview||'')+(run.goal_truncated?'…':''),'run-title'),
      element('div',date(run.created_at)+' · '+count(run.model_calls)+' reserved model steps','run-meta'));
    list.append(row);
  }
  $('more').hidden=!client.nextCursor;
}
function renderComposer(){
  const caps=client.caps,pending=client.pending();
  $('pending-note').hidden=!pending;
  $('pending-note').textContent=pending?.rejected?'The request was refused before acceptance. Correct it and submit using the same request key.':'A submission is unresolved. Retry its saved goal and policy with the same request key.';
  $('run-fields').disabled=busy||!!pending&&!pending.rejected||!caps?.admission_enabled;
  $('submit').disabled=busy||!caps?.admission_enabled||!!client.storageError;
  $('submit').textContent=busy?'Submitting…':pending?.rejected?'Submit corrected request':pending?'Retry saved submission':'Start workflow';
}
function loadComposer(caps){
  $('model').replaceChildren(...caps.models.map(v=>{const e=element('option',v);e.value=v;return e;}));
  $('protocol').replaceChildren(...caps.planner_protocols.map(v=>{const e=element('option',v==='schema-v1'?'Structured decisions':'JSON decisions');e.value=v;return e;}));
  if(caps.planner_protocols.includes('schema-v1'))$('protocol').value='schema-v1';
  const pending=client.pending();
  if(pending){
    const body=JSON.parse(pending.body),mapping={'goal':'goal','model':'model','protocol':'planner_protocol','namespace':'memory_namespace',
      'answer-format':'answer_format','model-budget':'max_model_calls','tool-budget':'max_tool_calls','token-budget':'max_generated_tokens','wall-budget':'max_wall_seconds'};
    for(const [id,key] of Object.entries(mapping))if(body[key]!==undefined){
      if(['model','protocol'].includes(id)&&![...$(id).options].some(v=>v.value===body[key])){const o=element('option',body[key]);o.value=body[key];$(id).append(o);}
      $(id).value=body[key];
    }
    $('sequence').value=(body.tool_sequence||[]).join(',');$('require-citations').checked=!!body.require_citations;
  }
  $('mode').textContent=caps.read_only_snapshot?'Read-only snapshot':client.storageError?'Submission storage unavailable':caps.admission_enabled?'Admission open':'Admission paused';
  $('memory-save').disabled=!caps.memory_writes_enabled;
  $('batch-link').hidden=caps.batch_console_path!=='/';renderComposer();
}
function formBody(){
  const body={goal:$('goal').value,model:$('model').value,planner_protocol:$('protocol').value,
    allowed_tools:client.caps.tools.map(t=>t.name),memory_namespace:$('namespace').value,
    answer_format:$('answer-format').value,require_citations:$('require-citations').checked,
    tool_sequence:$('sequence').value?$('sequence').value.split(','):[]};
  for(const [id,key] of [['model-budget','max_model_calls'],['tool-budget','max_tool_calls'],['token-budget','max_generated_tokens'],['wall-budget','max_wall_seconds']]) {
    const value=Number($(id).value);if(!Number.isSafeInteger(value))throw new Error('Execution limits must be whole numbers.');body[key]=value;
  }
  if(new TextEncoder().encode(body.goal).length>32768)throw new Error('The goal exceeds 32 KiB.');
  return body;
}
function renderDetail(){
  const panel=$('detail-panel'),snapshot=client.detail;
  if(!snapshot){panel.replaceChildren(element('div',selectedId?'Loading run…':'Select a run to inspect its execution.','empty'));return;}
  const run=snapshot.run,header=element('div',undefined,'section-title');
  const title=element('div');title.append(element('p',run.id,'eyebrow'),element('h2',run.goal));header.append(title,statusChip(run.status));
  const actions=element('div',undefined,'detail-actions');
  actions.append(button('Refresh run',()=>openRun(run.id)),button('Verify execution history',async()=>{verifying=true;try{await client.verify();renderDetail();}finally{verifying=false;}}));
  if(!terminal(run.status)&&client.caps?.cancellation_enabled)actions.append(button('Cancel workflow',async()=>{
    if(!confirm('Cancel this workflow? Already accepted model work can still finish and remains metered.'))return;
    const result=await client.cancel();notice('Run is '+result.status+'. Accepted model work remains in the Batch ledger.');await openRun(run.id);await refreshList();
  },'danger'));
  const verified=client.verified;
  actions.append(element('span',verified?(verified.stateVerified&&verified.resultVerified?'History and snapshot verified':'History verified; snapshot has no state commitment'):'History not verified','chip'+(verified?' good':'')));
  const metrics=element('div',undefined,'metrics');
  for(const [n,label] of [[run.model_calls,'Reserved model steps'],[run.tool_calls,'Executed tool calls'],[run.generated_tokens,'Delivered completion tokens'],[run.lease_epoch,'Coordinator epochs']]) {
    const e=element('div',undefined,'metric');e.append(element('strong',count(n)),element('span',label));metrics.append(e);
  }
  panel.replaceChildren(header,actions,metrics);
  panel.append(element('p',run.model+' · Deadline '+date(run.deadline_at),'muted'));
  panel.append(element('h3','Required tool order'),element('p',(run.policy.tool_sequence||[]).join(' → ')||'Agent chooses from its granted tools.','copy-note'));
  panel.append(element('h3',run.status==='completed'?'Final answer':'Current result'));
  const result=run.result;panel.append(element('pre',result?.answer??(result?JSON.stringify(result,null,2):'No final result delivered.')));
  if(result?.citations?.length){const list=element('ul');for(const c of result.citations)list.append(element('li',c));panel.append(element('h3','Retrieved citations'),list);}
  const columns=element('div',undefined,'two-columns'),tools=element('section'),children=element('section');tools.append(element('h3','Committed tool observations'));
  const observations=run.state.observations||[],ol=element('ol',undefined,'steps');
  for(const [index,o] of observations.entries()) {
    const li=element('li');li.append(element('strong',(index+1)+'. '+o.tool),element('pre',JSON.stringify(o.result,null,2)),element('p','Result SHA-256: '+o.result_sha256,'copy-note'));ol.append(li);
  }
  if(!observations.length)ol.append(element('li','No committed tool calls.','muted'));tools.append(ol);
  children.append(element('h3','Delivered model steps'));const list=element('ol',undefined,'steps');
  for(const [i,child] of (run.state.receipts||[]).entries()) {
    const li=element('li'),head=element('div',undefined,'step-head');head.append(element('strong','Model step '+(i+1)));
    if(client.caps?.child_receipts_enabled)head.append(button('Check ledger',async()=>{
      const proof=await client.receipt(child.batch_id);
      if(!proof.accepted_usage_reconciled||proof.batch_id!==child.batch_id)throw new Error('The child ledger did not reconcile.');
      const pre=element('pre',JSON.stringify(proof,null,2));li.append(pre);
      notice('This child identity, model and accepted usage match the committed Agent step.');
    }));
    li.append(head,element('p',child.batch_id,'copy-note'),element('p',count(child.usage?.prompt_tokens)+' input · '+count(child.usage?.completion_tokens)+' output tokens','step-meta'),
      element('p','Committed receipt: '+child.receipt_sha256,'copy-note'));list.append(li);
  }
  if(!(run.state.receipts||[]).length)list.append(element('li','No delivered child receipts yet.','muted'));children.append(list);columns.append(tools,children);panel.append(columns);
  if(run.state.output_rejections?.length)panel.append(element('h3','Rejected model decisions'),element('pre',JSON.stringify(run.state.output_rejections,null,2)));
  if(verified){
    panel.append(element('h3','Verified event history'),element('p',verified.scope,'muted'));
    const events=element('div',undefined,'history-list');
    for(const e of verified.events){const row=element('div',undefined,'history-event');row.append(element('code',String(e.revision)),element('span',e.kind),element('span',date(e.created_at),'muted'));events.append(row);}
    panel.append(events,element('p','Event head: '+verified.head,'copy-note'));
  }
}
async function openRun(id){selectedId=id;const task=client.select(id);renderRuns();renderDetail();await task;renderDetail();}
async function refreshList(more=false){await client.list(more);renderRuns();}
async function refresh(){if(refreshing)return;refreshing=true;try{await refreshList();if(selectedId)await openRun(selectedId);}finally{refreshing=false;}}

// DOM event registration stays separate from the testable client/state module.
$('connect-form').addEventListener('submit',event=>{event.preventDefault();
  const key=$('key').value.trim();if(!key)return;
  selectedId=null;client.setKey(key);$('workspace').hidden=true;$('run-form').reset();$('memory-form').reset();rememberKey('');
  perform(async()=>{
  await client.connect();rememberKey(key);$('key').value='';
  $('connect-panel').hidden=true;$('workspace').hidden=false;$('disconnect').hidden=false;$('refresh').disabled=false;
  $('connection').textContent='Workspace connected';$('connection').className='chip good';loadComposer(client.caps);await refreshList();renderDetail();notice(client.storageError?'Connected. Submission-key storage is unavailable; existing runs remain accessible.':'Workspace connected.');
});});
$('disconnect').addEventListener('click',()=>{
  client.setKey('');selectedId=null;rememberKey('');$('workspace').hidden=true;$('connect-panel').hidden=false;
  $('disconnect').hidden=true;$('refresh').disabled=true;$('connection').textContent='Disconnected';$('connection').className='chip';$('run-form').reset();$('memory-form').reset();notice('Workspace key cleared from this tab.');
});
$('run-form').addEventListener('submit',event=>{event.preventDefault();perform(async()=>{
  if(busy)return;busy=true;renderComposer();
  try{const pending=client.pending(),body=pending&&!pending.rejected?JSON.parse(pending.body):formBody(),run=await client.submit(body);notice('Workflow accepted.');await refreshList();await openRun(run.id);}
  finally{busy=false;renderComposer();}
});});
$('refresh').addEventListener('click',()=>perform(refresh));$('more').addEventListener('click',()=>perform(()=>refreshList(true)));
$('memory-load').addEventListener('click',()=>perform(async()=>{
  const epoch=client.epoch,namespace=$('memory-namespace').value,key=$('memory-key').value;
  if(!namespace||!key)throw new Error('Enter a memory namespace and key.');
  const doc=await client.request('agent-memory/'+encodeURIComponent(namespace)+'/'+encodeURIComponent(key));
  if(epoch!==client.epoch||namespace!==$('memory-namespace').value||key!==$('memory-key').value)throw new StaleWorkspace();
  $('memory-text').value=doc.text;$('memory-version').value=doc.version;$('memory-state').textContent='Loaded version '+doc.version+' · SHA-256 '+doc.sha256;
}));
$('memory-form').addEventListener('submit',event=>{event.preventDefault();perform(async()=>{
  if(!client.caps?.memory_writes_enabled)throw new Error('Memory writes are unavailable.');
  const epoch=client.epoch,namespace=$('memory-namespace').value,key=$('memory-key').value,text=$('memory-text').value,version=Number($('memory-version').value);
  if(!Number.isSafeInteger(version)||version<0||new TextEncoder().encode(text).length>32768)throw new Error('A safe version and document of at most32 KiB are required.');
  const doc=await client.request('agent-memory/'+encodeURIComponent(namespace)+'/'+encodeURIComponent(key),{method:'PUT',body:{text,expected_version:version}});
  if(epoch!==client.epoch||namespace!==$('memory-namespace').value||key!==$('memory-key').value)throw new StaleWorkspace();
  $('memory-version').value=doc.version;$('memory-state').textContent='Saved version '+doc.version+' · SHA-256 '+doc.sha256+($('memory-text').value!==text?' · Current edits are not saved yet.':'');
});});

let saved;try{saved=tabStorage.getItem(keyStorage);}catch{}if(saved)$('key').value=saved;
setInterval(()=>{if(!document.hidden&&client.caps&&client.detail&&!terminal(client.detail.run.status)&&!busy&&!verifying&&!refreshing)perform(refresh);},5000);
