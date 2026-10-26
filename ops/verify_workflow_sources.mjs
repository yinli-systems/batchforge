// Native integration fixture generated from actual isolated PostgreSQL. No GPU.
import fs from 'node:fs';
import path from 'node:path';
import {pathToFileURL} from 'node:url';
import {webcrypto} from 'node:crypto';
const [source,file]=process.argv.slice(2),fixture=JSON.parse(fs.readFileSync(file));
const {verifySources,canonical,sha256}=await import(pathToFileURL(path.join(source,'agents/console/core.js')));
const check=async f=>verifySources(f.response,f.current,f.tenant,async id=>f.snapshots[id],async(id,after)=>f.pages[id][String(after)],webcrypto);
const verified=await check(fixture);if(!verified.verified||verified.sources!==1||verified.verified_runs!==4)throw Error('Native lineage not verified');
let rejected=0;
for(const mutation of [
 f=>f.response.document.sources[0].nodes[0].run_id='agrun_'+'f'.repeat(32),
 f=>f.response.document.sources[0].edges.splice(0,1),
 f=>f.response.document.sources[0].origins[0].observation_sha256='f'.repeat(64),
 f=>{const s=f.response.document.sources[0].source;if(s.kind==='memory'){s.text='forged';s.text_truncated=true;}else s.match.document_version++;},
 f=>{f.snapshots[f.response.document.workflow_id].run.goal='{"nodes":[]}';},
]) {
 const f=structuredClone(fixture);mutation(f);f.response.document_sha256=await sha256(canonical(f.response.document),webcrypto);
 try{await check(f);}catch{rejected++;continue;}
 throw Error('Altered provenance passed client verification');
}
console.log(JSON.stringify({passed:true,verified_runs:verified.verified_runs,sources:verified.sources,adversarial_refusals:rejected,browser_visual_qa:false,gpu_inference:false}));
