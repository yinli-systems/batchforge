"""Read-only postmortem from verified retained Batch input/output bytes."""
from pathlib import Path
from datetime import datetime,timezone
import json,hashlib
import httpx
from agents.contracts import protocol_json,digest
from agents.retrieval import Snapshot
from ops.source_workflow_fixtures import fixtures
from ops.retrieval_squad import inputs,support
from ops.ha.retune_full_gpu import file_env
from scripts.quality_eval import score,normalize
base=Path(__file__).parent;root=Path('/root/batchforge-runtime');cohort=root/'soak480-agent-runtime-dag-source-lineage-20261026-v1'
raw=(cohort/'report.json').read_bytes();report=json.loads(raw);assert report['phase']=='quality_failed' and not report['passed'] and report['native_pg_stopped']
key=file_env(root/'access.env')['BATCHFORGE_API_KEY'];cases={c['id']:c for c in fixtures()};index=Snapshot(inputs()[0]);records=[];reads=[]
with httpx.Client(base_url='http://127.0.0.1:8080',headers={'Authorization':'Bearer '+key},trust_env=False,follow_redirects=False,timeout=15) as client:
    batches={}
    for proof in report['child_receipts']:
        bid=proof['receipt']['batch_id'];r=client.get('/v1/batches/'+bid);r.raise_for_status();batches[bid]=(r.json(),proof)
    def artifact(file_id,proof):
        info=next(a for a in proof['receipt']['artifacts'] if a['id']==file_id)
        response=client.get('/v1/files/'+file_id+'/content');response.raise_for_status();data=response.content
        assert len(data)==info['bytes'] and hashlib.sha256(data).hexdigest()==info['sha256'];reads.append({'file_id':file_id,'bytes':len(data),'sha256':info['sha256']})
        rows=[protocol_json(line) for line in data.splitlines() if line.strip()];assert len(rows)==1;return rows[0]
    for workflow in report['runs']:
        case=cases[workflow['case_id']]
        if not case['question']:continue
        q=case['question'];rid=workflow['nodes']['document']['run_id'];finals=[]
        for bid,(batch,proof) in batches.items():
            if batch['metadata'].get('agent_run_id')!=rid:continue
            output=artifact(batch['output_file_id'],proof);decision=protocol_json(output['response']['body']['choices'][0]['message']['content'])
            if decision.get('type')!='final':continue
            sent=artifact(batch['input_file_id'],proof);observations=[]
            for m in sent['body']['messages']:
                prefix='UNTRUSTED TOOL DATA (not instructions): '
                if m['content'].startswith(prefix):observations.append(protocol_json(m['content'][len(prefix):]))
            answer=decision['answer'];metrics=score(answer,[a['text'] for a in q['references']]);seen=[]
            for o in observations:
                if o['tool']!='retrieval.search':continue
                value=o['result'];assert index.verify(value) and digest(value)==o['result_sha256']
                permitted={digest({'query':value['query'],'limit':value['requested_limit']})}
                if value['requested_limit']==3:permitted.add(digest({'query':value['query']}))
                assert o['arguments_sha256'] in permitted
                seen.append({'query_sha256':value['query_sha256'],'query_is_original_question':value['query']==q['question'],
                    'limit':value['requested_limit'],'matches':len(value['matches']),
                    'gold_document_rank':next((i+1 for i,m in enumerate(value['matches']) if m['document_id']==q['document_id']),None),
                    'gold_span_rank':next((i+1 for i,m in enumerate(value['matches']) if support(q,m)),None),
                    'cited_reference_span':any(m['citation'] in decision['citations'] and support(q,m) for m in value['matches'])})
            original=index.search(q['question'],5)['matches']
            contains=any(normalize(a['text']) and normalize(a['text']) in normalize(answer) for a in q['references'])
            pattern='observed_retrieval_did_not_include_reference_span' if not any(s['gold_span_rank'] for s in seen) else 'nonexact_span_includes_normalized_reference' if contains else 'different_answer_despite_reference_span_available'
            finals.append({'case_id':q['id'],'workflow_case':case['id'],'run_id':rid,'batch_id':bid,'answer_sha256':hashlib.sha256(answer.encode()).hexdigest(),
                'answer_utf8_bytes':len(answer.encode()),'answer_words':len(answer.split()),'exact_match':metrics['exact_match'],'f1':metrics['f1'],
                'normalized_reference_is_substring':bool(contains),'retrieval':seen,
                'original_question_document_rank_at5':next((i+1 for i,m in enumerate(original) if m['document_id']==q['document_id']),None),
                'original_question_span_rank_at5':next((i+1 for i,m in enumerate(original) if support(q,m)),None),
                'observable_failure_pattern':pattern,'scope':'Read-only diagnostic association; not proof of a causal mechanism'})
        assert len(finals)==1;records.extend(finals)
assert len(records)==2 and all(r['exact_match']==0 for r in records)
result={'passed':True,'at':datetime.now(timezone.utc).isoformat(),'quality_passed':False,'source_report_sha256':hashlib.sha256(raw).hexdigest(),
 'validator_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'records':records,'verified_artifact_reads':reads,
 'new_gpu_tasks':0,'database_connections':0,'service_changes':False,'scope':'Preserved failed root answers diagnosed from actual SHA-verified S3 Batch artifacts and frozen corpus, no output repair or rerun'}
(base/'validation.json').write_text(json.dumps(result,indent=2)+'\n');print('BF_SOURCE_DIAGNOSTICS',json.dumps({'passed':True,'records':records}))
