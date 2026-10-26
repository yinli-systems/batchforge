// Actual client over a finite real RAG cohort. No DOM/browser or new inference.
import fs from 'node:fs';
import path from 'node:path';
import {pathToFileURL} from 'node:url';
import {createHash} from 'node:crypto';
const [source,cohort,output]=process.argv.slice(2),base='http://127.0.0.1:18170';
const result={passed:false,new_gpu_tasks:0,browser_visual_qa:false,node_version:process.version};
try {
 const report=JSON.parse(fs.readFileSync(path.join(cohort,'report.json')));
 const index=JSON.parse(fs.readFileSync(path.join(cohort,'retrieval-index.json')));
 if(!['retrieval-squad-v1','retrieval-span-v1'].includes(report.plan.fixture_set)||report.runs.length!==8)throw Error('Pinned RAG cohort required');
 const fd=fs.openSync(path.join(cohort,'.runtime/executors.json'),fs.constants.O_RDONLY|fs.constants.O_NOFOLLOW),stat=fs.fstatSync(fd);
 if(stat.uid!==process.getuid()||(stat.mode&0o077))throw Error('Private cohort key file required');
 const entries=Object.entries(JSON.parse(fs.readFileSync(fd,'utf8')));fs.closeSync(fd);
 if(entries.length!==1)throw Error('One existing tenant required');
 const [tenant,key]=entries[0];
 const {AgentClient,retrievalSelection,selectedTools,displayStatus}=await import(pathToFileURL(path.join(source,'agents/console/core.js')));
 const memory=new Map(),storage={getItem:k=>memory.get(k)??null,setItem:(k,v)=>memory.set(k,v)};
 const client=new AgentClient({base:'/v1/',storage,fetch:(url,options)=>fetch(base+url,{...options,signal:AbortSignal.timeout(8000)})});
 client.setKey(key);const caps=await client.connect();
 if(caps.tenant_id!==tenant||caps.retrieval_snapshots.length!==1||caps.retrieval_snapshots[0].id!==index.id)throw Error('Actual retrieval grant differs');
 if(retrievalSelection(caps,index.id).namespace!=='canary'||!selectedTools(caps,index.id).includes('retrieval.search')||selectedTools(caps,'').includes('retrieval.search'))throw Error('Console snapshot selection differs');
 result.records=[];let children=0,events=0;
 for(const definition of report.runs) {
  const snapshot=await client.select(definition.run_id),verified=await client.verify();
  if(!verified.stateVerified||!verified.resultVerified||snapshot.run.policy.retrieval_snapshot!==index.id)throw Error('Durable RAG snapshot failed client verification');
  const disposition=displayStatus(snapshot.run);
  if(report.plan.fixture_set==='retrieval-span-v1'&&snapshot.run.status==='completed') {
   const final=snapshot.run.result;
   if(final.type==='abstain') {if(disposition!=='abstained'||final.answer!==undefined)throw Error('Abstention disposition differs');}
   else {
    const proof=final.source_evidence;
    if(!proof||proof.protocol!=='batchforge.quoted-answer.v1'||proof.snapshot_id!==index.id||!proof.quotes.length)throw Error('Missing exact source proof');
    const matches=new Map(snapshot.run.state.observations.filter(o=>o.tool==='retrieval.search').flatMap(o=>o.result.matches.map(m=>[m.citation,m])));
    for(const quote of proof.quotes){
     const match=matches.get(quote.citation),answer=Buffer.from(final.answer);
     if(!match||!final.citations.includes(quote.citation)||quote.document_sha256!==match.document_sha256||quote.document_id!==match.document_id||quote.document_version!==match.document_version||quote.start_byte<match.start_byte||quote.end_byte>match.end_byte||quote.end_byte-quote.start_byte!==answer.length||quote.answer_sha256!==createHash('sha256').update(answer).digest('hex')||!Buffer.from(match.text).subarray(quote.start_byte-match.start_byte,quote.end_byte-match.start_byte).equals(answer))throw Error('Quoted source bytes differ');
    }
   }
  }
  const receipts=[];
  for(const r of snapshot.run.state.receipts) {
   const proof=await client.receipt(r.batch_id);children++;
   receipts.push({batch_id:r.batch_id,current_receipt_sha256:proof.current.receipt_sha256});
  }
  events+=verified.events.length;
  result.records.push({run_id:snapshot.run.id,status:snapshot.run.status,answer_disposition:disposition,revision:verified.revision,event_head_sha256:verified.head,
   state_sha256:createHash('sha256').update(snapshot.state_json).digest('hex'),result_sha256:createHash('sha256').update(snapshot.result_json).digest('hex'),receipts});
 }
 const sourceFiles=JSON.parse(fs.readFileSync(path.dirname(source)+'/source-receipt.json')).files;
 result.assets=[];
 for(const file of ['index.html','app.js','core.js','styles.css']) {
  const response=await fetch(base+'/console/'+file,{signal:AbortSignal.timeout(8000)}),bytes=new Uint8Array(await response.arrayBuffer());
  const sha=createHash('sha256').update(bytes).digest('hex');
  if(!response.ok||sha!==sourceFiles['agents/console/'+file])throw Error('RAG console asset differs');
  result.assets.push({file,sha256:sha,bytes:bytes.length});
 }
 result.passed=true;result.run_count=result.records.length;result.child_receipts=children;result.events=events;result.snapshot_id=index.id;
 result.scope='Actual Agent client, event/snapshot byte commitments, pinned retrieval selection and current Azure child receipts; no browser visual QA or independent answer grading';
}catch(error){result.error_type=error.name;}
fs.writeFileSync(output,JSON.stringify(result,null,2)+'\n',{mode:0o600});
console.log(JSON.stringify({passed:result.passed,records:result.run_count,child_receipts:result.child_receipts,error_type:result.error_type}));
if(!result.passed)process.exitCode=1;
