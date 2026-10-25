// Native HTTP/client qualification over prior real Agent records. No browser,
// new inference, uploads, state writes or fabricated execution measurements.
import fs from 'node:fs';
import {pathToFileURL} from 'node:url';
import path from 'node:path';
import {createHash} from 'node:crypto';
const [source,cohort,base,output]=process.argv.slice(2);
const result={passed:false,phase:'prepared',browser_visual_qa:false,new_gpu_tasks:0,node_version:process.version};
try {
  if(base!=='http://127.0.0.1:18174')throw new Error('Pinned console endpoint required');
  const report=JSON.parse(fs.readFileSync(path.join(cohort,'report.json'))),manifest=JSON.parse(fs.readFileSync(path.dirname(source)+'/source-receipt.json'));
  const file=path.join(cohort,'.runtime/executors.json'),fd=fs.openSync(file,fs.constants.O_RDONLY|fs.constants.O_NOFOLLOW),stat=fs.fstatSync(fd);
  if(stat.uid!==process.getuid()||(stat.mode&0o077))throw new Error('Registry permission check failed');
  const keys=JSON.parse(fs.readFileSync(fd,'utf8'));fs.closeSync(fd);
  if(Object.keys(keys).length!==1)throw new Error('One existing cohort tenant required');
  const [tenant,key]=Object.entries(keys)[0];
  const {AgentClient}=await import(pathToFileURL(path.join(source,'agents/console/core.js')));
  const memory=new Map(),storage={getItem:k=>memory.get(k)??null,setItem:(k,v)=>memory.set(k,v)};
  const client=new AgentClient({base:'/v1/',storage,fetch:(url,options)=>fetch(base+url,options)});
  client.setKey(key);const caps=await client.connect();
  if(caps.tenant_id!==tenant||!caps.read_only_snapshot||caps.admission_enabled||caps.memory_writes_enabled||caps.cancellation_enabled||!caps.child_receipts_enabled)throw new Error('Capability boundary differs');
  result.phase='asset_checks';result.assets=[];
  for(const [asset,file] of [['','index.html'],['app.js','app.js'],['core.js','core.js'],['styles.css','styles.css']]) {
    const response=await fetch(base+'/console/'+asset),data=new Uint8Array(await response.arrayBuffer()),sha=createHash('sha256').update(data).digest('hex');
    if(!response.ok||sha!==manifest.files['agents/console/'+file]||!response.headers.get('Content-Security-Policy')?.includes("connect-src 'self'"))throw new Error('Asset or CSP differs');
    result.assets.push({file,sha256:sha,bytes:data.length});
  }
  if((await fetch(base+'/v1/agent-capabilities')).status!==401)throw new Error('Unauthenticated metadata exposed');
  for(const [method,url] of [['POST','/v1/agent-runs'],['PUT','/v1/agent-memory/console-read-only/new'],['POST','/v1/agent-runs/'+report.runs[0].run_id+'/cancel']]) {
    if((await fetch(base+url,{method,headers:{Authorization:'Bearer '+key,'Content-Type':'application/json'},body:'{}'})).status!==403)throw new Error('Snapshot mutation was not refused');
  }
  await client.list();while(client.nextCursor){if(client.runs.length>100)throw new Error('Unexpected run count');await client.list(true);}
  if(client.runs.length!==20||client.runs.some(r=>'state'in r||'result'in r))throw new Error('List overfetch or missing cohort');
  result.phase='real_record_verification';result.runs=[];let receipts=0,events=0;
  for(const definition of report.runs) {
    const snapshot=await client.select(definition.run_id),verified=await client.verify();
    if(!verified.stateVerified||!verified.resultVerified||snapshot.run.status!=='completed')throw new Error('Real snapshot verification failed');
    const children=[];
    for(const committed of snapshot.run.state.receipts) {
      const proof=await client.receipt(committed.batch_id);receipts++;
      children.push({batch_id:committed.batch_id,current_receipt_sha256:proof.current.receipt_sha256,committed_receipt_sha256:proof.committed_receipt_sha256});
    }
    events+=verified.events.length;
    result.runs.push({run_id:snapshot.run.id,case_id:definition.case_id,revision:verified.revision,event_head:verified.head,
      state_sha256:createHash('sha256').update(snapshot.state_json).digest('hex'),result_sha256:createHash('sha256').update(snapshot.result_json).digest('hex'),children});
  }
  if(receipts!==44)throw new Error('Child cardinality differs');
  const first=client.runs[0],other=result.runs.find(r=>r.run_id!==first.id)?.children[0]?.batch_id;
  if(other&&(await fetch(base+'/v1/agent-runs/'+first.id+'/children/'+other+'/receipt',{headers:{Authorization:'Bearer '+key}})).status!==404)throw new Error('Unassociated child was exposed');
  result.passed=true;result.phase='complete';result.run_count=20;result.child_receipts=receipts;result.events=events;
  result.ui_source_commit=manifest.git_commit;result.cohort_source_commit=report.source_commit;
  result.source_report_sha256=createHash('sha256').update(fs.readFileSync(path.join(cohort,'report.json'))).digest('hex');
  result.scope='Actual shared client logic over preserved real-Agent native records and current Azure Batch ledger receipts; source/CSP checks only, no browser rendering or new model quality run.';
}catch(error){result.phase_before_failure=result.phase;result.passed=false;result.phase='failed';result.error_type=error.name;}
fs.writeFileSync(output,JSON.stringify(result,null,2)+'\n',{mode:0o600});
console.log(JSON.stringify({passed:result.passed,runs:result.run_count,receipts:result.child_receipts,error_type:result.error_type}));
if(!result.passed)process.exitCode=1;
