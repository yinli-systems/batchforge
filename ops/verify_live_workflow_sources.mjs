// Actual private API client over closed real Agent roles; no new inference.
import fs from 'node:fs';
import path from 'node:path';
import {pathToFileURL} from 'node:url';
const [source,cohort,out]=process.argv.slice(2),base='http://127.0.0.1:18170';
const result={passed:false,new_gpu_tasks:0,browser_visual_qa:false};
try {
 const report=JSON.parse(fs.readFileSync(path.join(cohort,'report.json')));
 if(report.plan.fixture_set!=='source-lineage-v1'||report.runs.length!==3)throw Error('Frozen source study required');
 const fd=fs.openSync(path.join(cohort,'.runtime/executors.json'),fs.constants.O_RDONLY|fs.constants.O_NOFOLLOW),stat=fs.fstatSync(fd);
 if(stat.uid!==process.getuid()||(stat.mode&0o077))throw Error('Protected executor file required');
 const entries=Object.entries(JSON.parse(fs.readFileSync(fd,'utf8')));fs.closeSync(fd);if(entries.length!==1)throw Error('One original tenant required');
 const [tenant,key]=entries[0],{AgentClient}=await import(pathToFileURL(path.join(source,'agents/console/core.js')));
 const memory=new Map(),client=new AgentClient({base:'/v1/',storage:{getItem:k=>memory.get(k)??null,setItem:(k,v)=>memory.set(k,v)},
  fetch:(url,opts)=>fetch(base+url,{...opts,signal:AbortSignal.timeout(8000)})});client.setKey(key);
 const caps=await client.connect();if(caps.tenant_id!==tenant||!caps.source_lineage_enabled)throw Error('Source capability differs');
 result.records=[];let events=0,children=0;
 for(const workflow of report.runs){
  await client.select(workflow.run_id);const parent=await client.verify();events+=parent.events.length;
  for(const [node,definition] of Object.entries(workflow.nodes)){
   const snapshot=await client.select(definition.run_id),verified=await client.verify();events+=verified.events.length;
   let sources=null;
   if(snapshot.run.policy.inherit_sources&&snapshot.run.status==='completed')sources=await client.sources();
   for(const r of snapshot.run.state.receipts??[]){await client.receipt(r.batch_id);children++;}
   result.records.push({workflow_id:workflow.run_id,node,run_id:snapshot.run.id,status:snapshot.run.status,event_head_sha256:verified.head,
    source_verified:sources?.verified??false,source_count:sources?.sources??0,verified_lineage_runs:sources?.verified_runs??0});
  }
 }
 result.passed=result.records.length===10&&result.records.filter(r=>r.source_verified).length===6;
 result.roles=result.records.length;result.events=events;result.child_receipts=children;result.verified_inheriting_roles=6;
 result.scope='Actual same-origin client verifies workflow declarations, complete event/snapshot chains, source membership/lineage and current child receipts; not semantic correctness or browser visual QA';
}catch(e){result.error_type=e.name;}
fs.writeFileSync(out,JSON.stringify(result,null,2)+'\n',{mode:0o600});console.log(JSON.stringify({passed:result.passed,roles:result.roles,events:result.events,error_type:result.error_type}));if(!result.passed)process.exitCode=1;
