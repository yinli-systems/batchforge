// Actual console functions over a bounded loopback candidate, no browser claim.
import {readFileSync,writeFileSync} from 'node:fs';
import {createHash,webcrypto} from 'node:crypto';
import vm from 'node:vm';
const config=JSON.parse(readFileSync(process.argv[2],'utf8'));
if (!/^http:\/\/127\.0\.0\.1:(18160|8080|8082|8083)$/.test(config.base_url)) throw Error('Known private API endpoint required');
const source=readFileSync(config.source+'/frontend/app.js','utf8');
const boundary=source.indexOf("document.addEventListener('click'");
if (boundary<=0) throw Error('Console module boundary changed');
const observed={downloads:[],blobs:[],notices:[]};
const context=vm.createContext({Headers,FormData,TextEncoder,TextDecoder,Uint8Array,Blob,
  crypto:webcrypto,setTimeout:callback=>callback(),location:{origin:config.base_url,hash:''},
  sessionStorage:{getItem:()=>null,setItem(){},removeItem(){}},
  URL:{createObjectURL(blob){observed.blobs.push(blob);return 'blob:verified';},revokeObjectURL(){}},
  document:{querySelector(){throw Error('Unexpected DOM use');},createElement(tag){
    if(tag!=='a')throw Error('Unexpected element');return {href:'',download:'',click(){observed.downloads.push(this.download);}};
  }},observed,
  fetch:(path,request)=>fetch(new URL(path,config.base_url),{...request,signal:AbortSignal.timeout(10000)})});
vm.runInContext(source.slice(0,boundary)+`
 toast=(message,type)=>observed.notices.push({message,type});
 globalThis.actions={state,loadEvaluations,qualityView,downloadEvaluation};
`,context,{filename:'qualified-frontend/app.js'});
const app=context.actions;app.state.key=config.api_key;app.state.connected=true;
await app.loadEvaluations();
if (app.state.qualityError || app.state.engineQualityError || app.state.engineEvaluations.length!==1) throw Error('Quality source loading failed');
const value=app.state.engineEvaluations[0];
if (value.scope.kind!=='engine_only' || value.scope.batch_ledger_verified!==false || value.exact_results!==8667 || value.scoring_correction.changed_cases!==1) throw Error('Corrected engine scope differs');
const html=app.qualityView();
for(const text of ['Batch API evidence','Engine benchmarks','Quality gate failed','Engine quality gate passed','82.00','90.94','1 legacy scores corrected; original report preserved','do not add','not an invoice']){
  if(!html.includes(text))throw Error('Console evidence distinction missing');
}
if(html.includes('82.01'))throw Error('Legacy uncorrected score displayed');
await app.downloadEvaluation({disabled:false,dataset:{evaluationId:value.id,evidenceKind:'engine'}});
if(observed.downloads.length!==1 || !observed.notices.some(n=>n.message.includes('checksum verified')))throw Error('Actual console download did not verify');
const raw=await observed.blobs[0].text();const downloaded=JSON.parse(raw);
if(downloaded.id!==value.id || downloaded.metrics.exact_match_percent!==value.metrics.exact_match_percent)throw Error('Downloaded engine view differs');
const report={passed:true,scope:'Actual console source functions and WebCrypto over real candidate HTTP; no browser visual QA',
  frontend_source_sha256:createHash('sha256').update(source).digest('hex'),engine_evidence_id:value.id,
  corrected_exact_match_percent:value.metrics.exact_match_percent,f1_percent:value.metrics.f1_percent,
  download_bytes:Buffer.byteLength(raw),download_sha256:createHash('sha256').update(raw).digest('hex'),
  batch_quality_states:Array.from(app.state.evaluations,e=>e.quality_state),engine_quality_state:value.quality_state,
  legacy_batch_failure_preserved:true,corrected_score_rendered:true,nested_cost_labelled:true,webcrypto_download_verified:true,
  new_gpu_tasks:0,browser_visual_qa:false};
writeFileSync(config.output,JSON.stringify(report,null,2)+'\n',{mode:0o600});
