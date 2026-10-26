"""Frozen public RAG protocol: index passages only, grade references separately."""
from pathlib import Path
from functools import lru_cache
from datetime import datetime,timezone
import argparse,hashlib,json,os,time
from agents.retrieval import Snapshot,CORPUS
from agents.contracts import canonical,digest,protocol_json
from backend.evaluations import SQUAD_DEV_SHA256
from scripts.quality_eval import score
from ops.ha.set_scheduler_policy import atomic_bytes

ORIGIN='https://rajpurkar.github.io/SQuAD-explorer/dataset/dev-v1.1.json'
DATASET=Path('/root/batchforge-runtime/app/.runtime/public-data/dev-v1.1.json')
SEED='2026102603'
COUNT=8


def write(path,value):atomic_bytes(path,(json.dumps(value,indent=2,allow_nan=False)+'\n').encode())


def extract(raw):
    data=protocol_json(raw);documents=[];cases=[];ids=set()
    if data.get('version')!='1.1':raise ValueError('SQuAD1.1 schema required')
    for ai,article in enumerate(data['data']):
        for pi,paragraph in enumerate(article['paragraphs']):
            did=f'squad-{ai}-{pi}';text=paragraph['context']
            documents.append({'id':did,'version':1,'title':article['title'],'source_ref':f'article/{ai}/paragraph/{pi}','text':text})
            for qa in paragraph['qas']:
                if qa['id'] in ids or not qa.get('answers'):raise ValueError('Unique answerable public cases required')
                ids.add(qa['id']);references=[]
                for a in qa['answers']:
                    start=a['answer_start'];answer=a['text']
                    if text[start:start+len(answer)]!=answer:raise ValueError('Public reference span differs')
                    references.append({'text':answer,'start_byte':len(text[:start].encode()),'end_byte':len(text[:start+len(answer)].encode())})
                cases.append({'id':qa['id'],'question':qa['question'],'document_id':did,'references':references})
    corpus={'protocol':CORPUS,'source':{'name':'SQuAD1.1 public dev passages','origin':ORIGIN,
        'licence':'See original SQuAD release and source Wikipedia terms','source_sha256':hashlib.sha256(raw).hexdigest()},'documents':documents}
    return canonical(corpus),cases


def official(raw):
    if hashlib.sha256(raw).hexdigest()!=SQUAD_DEV_SHA256:raise ValueError('Pinned official public dataset required')
    corpus,cases=extract(raw)
    if len(cases)!=10570 or len(protocol_json(corpus)['documents'])!=2067:raise ValueError('Complete official corpus required')
    return corpus,cases


def selected(raw):
    corpus,cases=official(raw);snapshot=Snapshot(corpus)
    selected=sorted(cases,key=lambda c:hashlib.sha256((SEED+':'+c['id']).encode()).hexdigest())[:COUNT]
    values=[]
    for c in selected:
        values.append({**c,'kind':'retrieval-squad','snapshot_id':snapshot.id,'tools':['retrieval.search'],
            'goal':'Search the granted knowledge corpus using retrieval.search to answer this question. Return the shortest answer span supported by the source, with its exact retrieval citation: '+c['question'],
            'planner_protocol':'schema-v1','require_citations':True,'tool_sequence':['retrieval.search']})
    return corpus,values


@lru_cache(maxsize=1)
def inputs():return selected(DATASET.read_bytes())


def support(case,match):
    return match['document_id']==case['document_id'] and any(match['start_byte']<=a['start_byte'] and match['end_byte']>=a['end_byte'] for a in case['references'])


def grade(case,run):
    """Gold stays in this external grader, never the corpus/query/repair path."""
    result={'passed':False,'completed':run['status']=='completed','answer_correct':False,'required_tools':False,
        'citation_verified':False,'exact_match':0,'f1':0.0,'retrieval_replay_verified':False}
    corpus,_=inputs();snapshot=Snapshot(corpus)
    observations=[o for o in run.get('state',{}).get('observations',[]) if o['tool']=='retrieval.search']
    replay=bool(observations)
    for o in observations:
        try:
            value=o['result'];permitted={digest({'query':value['query'],'limit':value['requested_limit']})}
            if value['requested_limit']==3:permitted.add(digest({'query':value['query']}))
            replay=replay and o['arguments_sha256'] in permitted and o['result_sha256']==digest(value) and snapshot.verify(value)
        except (ValueError,TypeError,KeyError):replay=False
    result['retrieval_replay_verified']=bool(replay);result['required_tools']=bool(observations)
    if not result['completed']:return result
    answer=run['result']['answer'];metrics=score(answer,[a['text'] for a in case['references']])
    citations=set(run['result'].get('citations',[]));supported={m['citation'] for o in observations for m in o['result']['matches'] if support(case,m)}
    cited=bool(citations) and citations<=supported and replay
    result.update(exact_match=metrics['exact_match'],f1=metrics['f1'],answer_correct=metrics['exact_match']==1,
        citation_verified=bool(cited),answer_sha256=hashlib.sha256(answer.encode()).hexdigest(),
        passed=metrics['exact_match']==1 and bool(cited))
    return result


def evaluate(raw,destination):
    destination=Path(destination);destination.mkdir(mode=0o700,exist_ok=False)
    begin=time.monotonic();corpus,cases=official(raw);snapshot=Snapshot(corpus);built=time.monotonic();results=[]
    with (destination/'scores.jsonl').open('x') as stream:
        for case in cases:
            start=time.monotonic();hits=snapshot.search(case['question'],5)['matches'];elapsed=time.monotonic()-start
            rank=next((i+1 for i,h in enumerate(hits) if h['document_id']==case['document_id']),None)
            span=next((i+1 for i,h in enumerate(hits) if support(case,h)),None)
            row={'id':case['id'],'query_sha256':hashlib.sha256(case['question'].encode()).hexdigest(),'document_rank':rank,
                'answer_span_rank':span,'latency_seconds':elapsed,'ranked':[{'document_id':h['document_id'],'chunk_id':h['chunk_id']} for h in hits]}
            stream.write(json.dumps(row,allow_nan=False)+'\n');results.append(row)
        stream.flush();os.fsync(stream.fileno())
    end=time.monotonic();latencies=sorted(r['latency_seconds'] for r in results)
    def quantile(p):
        x=(len(latencies)-1)*p;i=int(x);return latencies[i]+(latencies[min(i+1,len(latencies)-1)]-latencies[i])*(x-i)
    _,chosen=selected(raw)
    report={'protocol':'batchforge.public-retrieval-evaluation.v1','evaluation_valid':len(results)==10570,
        'source_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        'retrieval_source_sha256':hashlib.sha256(Path(__file__).resolve().parents[1].joinpath('agents/retrieval.py').read_bytes()).hexdigest(),
        'scores_sha256':hashlib.sha256((destination/'scores.jsonl').read_bytes()).hexdigest(),
        'dataset_sha256':SQUAD_DEV_SHA256,'snapshot':snapshot.manifest(),'cases':len(results),'fixture_set':'retrieval-squad-v1',
        'selected_cases':[{k:v for k,v in c.items() if k not in ('references',)} for c in chosen],
        'fixture_sha256':digest(chosen),'corpus_contains_questions_or_reference_fields':False,
        'build_seconds':built-begin,'query_wall_seconds':end-built,'elapsed_seconds':end-begin,
        'query_latency_seconds':{'p50':quantile(.5),'p95':quantile(.95),'p99':quantile(.99)},
        'document_recall_at_k':{str(k):sum(r['document_rank'] is not None and r['document_rank']<=k for r in results)/len(results) for k in (1,3,5)},
        'answer_span_coverage_at_k':{str(k):sum(r['answer_span_rank'] is not None and r['answer_span_rank']<=k for r in results)/len(results) for k in (1,3,5)},
        'document_mrr_at_5':sum(1/r['document_rank'] if r['document_rank'] else 0 for r in results)/len(results),
        'new_gpu_tasks':0,'browser_visual_qa':False,'scope':'CPU BM25 retrieval over complete public passages, not vector embedding search or inference/task success',
        'real_agent_plan':{'cases':8,'max_new_tasks':32,'selection_seed':SEED,'selection':'SHA256(seed:public_case_id) ascending, before observing RAG answers',
            'quality_gate':{'em_percent':75,'f1_percent':80,'supported_citation_percent':75,'task_success_percent':75},'public_dev_not_hidden':True}}
    write(destination/'report.json',report);return report


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--dataset',required=True,type=Path);p.add_argument('--out',required=True,type=Path)
    a=p.parse_args();report=evaluate(a.dataset.read_bytes(),a.out)
    print(json.dumps({k:report[k] for k in ('cases','document_recall_at_k','answer_span_coverage_at_k','elapsed_seconds','fixture_sha256')}))


if __name__=='__main__':main()
