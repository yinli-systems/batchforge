"""Four real Batch requests qualify schema decoding, not an Agent cohort.

Synthetic tool observations are explicit test inputs. Memory proposals are not
executed or presented as persistent retrieval. No service/model/quota changes.
"""
from __future__ import annotations
import argparse,fcntl,hashlib,json,os,time
from pathlib import Path
from datetime import datetime,timezone
import httpx,psycopg
from psycopg.rows import dict_row
from agents.contracts import Policy,canonical,digest,parse_decision,ToolDecision,FinalDecision,planner_messages
from agents.structured import response_format
from agents.tools import SPECS,calculate,Calculate,MemoryGet
from backend.receipts import verify as verify_receipt
from ops.worker_latch_guard import Pins
from ops.ha.retune_full_gpu import file_env
from ops.product_campaign import MODEL
from ops.agent_endpoint import qualification_endpoint


def fixture():
    text='Synthetic decoder test: project code is CALIB-701.'
    citation='memory:decoder:fixture:v1:'+hashlib.sha256(text.encode()).hexdigest()
    definitions=[
        ('math-tool',Policy(planner_protocol='schema-v1',allowed_tools=('math.calculate',)),
         'Return the tool decision calling math.calculate for 20+22.',[],1),
        ('number-final',Policy(planner_protocol='schema-v1',allowed_tools=(),answer_format='number'),
         'Return a final answer string with the result of 20+22; no citations.',[],0),
        ('memory-tool',Policy(planner_protocol='schema-v1',allowed_tools=('memory.get',),require_citations=True),
         'Retrieve the memory key decoder-fixture using memory.get.',[],1),
        ('identifier-final',Policy(planner_protocol='schema-v1',allowed_tools=('memory.get',),answer_format='identifier',require_citations=True),
         'Return the project code from this synthetic test observation and its citation.',
         [{'tool':'memory.get','result':{'text':text,'citation':citation}}],0)]
    cases=[]
    for name,policy,goal,observations,tools in definitions:
        fmt=response_format(policy,observed_citations=[citation] if observations else [],remaining_tools=tools)
        if name=='math-tool':
            # Decoder calibration explicitly requests a tool envelope. This
            # narrowed test schema is not a claim about automatic Agent planning.
            fmt['json_schema']['schema']['anyOf']=[v for v in fmt['json_schema']['schema']['anyOf'] if 'name' in v['properties']]
        body={'model':MODEL,'messages':planner_messages(goal,policy,SPECS,observations),
              'max_tokens':256,'temperature':0,'response_format':fmt}
        cases.append({'custom_id':name,'method':'POST','url':'/v1/chat/completions','body':body})
    return cases,citation


def validate(name,content,citation):
    decision=parse_decision(content)
    if name=='math-tool':
        if not isinstance(decision,ToolDecision) or decision.name!='math.calculate':return False
        return calculate(Calculate.model_validate(decision.arguments).expression)==42
    if name=='memory-tool':
        return isinstance(decision,ToolDecision) and decision.name=='memory.get' and MemoryGet.model_validate(decision.arguments).key=='decoder-fixture'
    if not isinstance(decision,FinalDecision):return False
    if name=='number-final':
        Policy(answer_format='number').validate_final(decision)
        return decision.answer=='42' and decision.citations==[]
    Policy(answer_format='identifier',require_citations=True).validate_final(decision)
    return decision.answer=='CALIB-701' and decision.citations==[citation]


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run',type=Path);p.add_argument('--apply',action='store_true')
    p.add_argument('--batch-url',type=qualification_endpoint,default='http://127.0.0.1:8080');args=p.parse_args()
    cases,citation=fixture();raw=b''.join(canonical(c)+b'\n' for c in cases)
    if not args.apply:
        print(json.dumps({'tasks':4,'fixture_sha256':hashlib.sha256(raw).hexdigest(),
            'scope':'Real-GPU decoder calibration with synthetic observations, not durable Agent execution'}));return
    root=Path('/root/batchforge-runtime/app');base=args.run.resolve() if args.run else None
    if (not base or base.parent!=root.parent or not base.name.startswith('soak480-agent-runtime-grammar-')
        or base.exists() or datetime.now(timezone.utc)>=datetime(2026,10,26,8,30,tzinfo=timezone.utc)):
        raise ValueError('New owned run and bounded recovery margin required')
    os.umask(0o077);base.mkdir(mode=0o700);start=time.monotonic()
    report={'phase':'prepared','passed':False,'scope':'Four real Batch schema-decoder requests; synthetic supplied observations; not a durable Agent/RAG quality cohort',
            'planned_tasks':4,'fixture_sha256':hashlib.sha256(raw).hexdigest(),'service_changes':False,'quota_changes':False,
            'batch_ingress':args.batch_url}
    def save():
        temp=base/'report.json.tmp';temp.write_bytes(canonical(report)+b'\n');temp.replace(base/'report.json')
    save()
    try:
        lock=(root/'.runtime/product-campaign.lock').open('a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        source=Path.cwd();receipt=json.loads((source.parent/'source-receipt.json').read_bytes())
        for name,sha in receipt['files'].items():
            if hashlib.sha256((source/name).read_bytes()).hexdigest()!=sha:raise RuntimeError('Immutable source changed')
        report['source_commit']=receipt['git_commit'];pins=Pins(root);report['primary_before']=dict(pins.identities)
        env=file_env(next(s for s in pins.services if s['role']=='api')['env_file']);key=file_env(root.parent/'access.env')['BATCHFORGE_API_KEY']
        with psycopg.connect(env['DATABASE_URL'],row_factory=dict_row,connect_timeout=5,options='-c default_transaction_read_only=on -c statement_timeout=5000') as db:
            tenant=db.execute('SELECT tenant_id FROM api_keys WHERE key_hash=%s AND NOT revoked',(hashlib.sha256(key.encode()).hexdigest(),)).fetchone()['tenant_id']
            quota=db.execute('''SELECT l.max_retained_tasks-u.retained_tasks AS remaining FROM tenant_retention_limits l
                JOIN tenant_retention_usage u USING(tenant_id) WHERE tenant_id=%s''',(tenant,)).fetchone()['remaining']
            if quota<4 or db.execute("SELECT count(*) AS n FROM tasks WHERE status IN ('pending','running')").fetchone()['n']:
                raise RuntimeError('Existing quota or queue preflight refused')
        idem='grammar.'+base.name;report['idempotency_key']=idem
        with httpx.Client(base_url=args.batch_url,headers={'Authorization':'Bearer '+key},trust_env=False,follow_redirects=False,timeout=20) as client:
            # No blind upload retry. Any uncertain outcome remains a preserved
            # failed directory; a later operator must reconcile the original key.
            report['phase']='upload_started';save()
            response=client.post('/v1/files',files={'file':(base.name+'.jsonl',raw,'application/jsonl')},data={'purpose':'batch'});response.raise_for_status()
            report['input_file_id']=response.json()['id'];report['phase']='batch_started';save()
            body={'input_file_id':report['input_file_id'],'endpoint':'/v1/chat/completions','completion_window':'24h','metadata':{'grammar_calibration':base.name}}
            report['batch_body_sha256']=digest(body);save()
            response=client.post('/v1/batches',headers={'Idempotency-Key':idem},json=body);response.raise_for_status();bid=response.json()['id']
            report['batch_id']=bid;report['phase']='running';save();deadline=time.monotonic()+240
            while time.monotonic()<deadline:
                response=client.get('/v1/batches/'+bid);response.raise_for_status();batch=response.json()
                if batch['status']!='in_progress':break
                time.sleep(1)
            else:raise RuntimeError('Decoder calibration deadline')
            proof=client.get('/v1/batches/'+bid+'/receipt');proof.raise_for_status();proof=proof.json();report['receipt']=proof
            if not verify_receipt(proof) or proof['receipt']['ledger_state']!='consistent_ledger' or not all(proof['receipt']['checks'].values()):raise RuntimeError('Calibration receipt mismatch')
            if batch['request_counts']['completed']!=4 or batch['request_counts']['failed']!=0:raise RuntimeError('Engine did not deliver all schema requests')
            response=client.get('/v1/files/'+batch['output_file_id']+'/content');response.raise_for_status();output=response.content
            verdicts=[];usage={'prompt_tokens':0,'completion_tokens':0}
            for line in output.splitlines():
                row=json.loads(line);body=row['response']['body'];choice=body['choices'][0];content=choice['message']['content']
                for k in usage:usage[k]+=body['usage'][k]
                try:passed=choice['finish_reason']=='stop' and body['model']==MODEL and validate(row['custom_id'],content,citation)
                except ValueError:passed=False
                verdicts.append({'custom_id':row['custom_id'],'passed':passed,'content_sha256':hashlib.sha256(content.encode()).hexdigest()})
            report['verdicts']=verdicts;report['output_bytes_sha256']=hashlib.sha256(output).hexdigest()
            report['response_usage']=usage
            report['passed']=len(verdicts)==4 and {v['custom_id'] for v in verdicts}=={c['custom_id'] for c in cases} and all(v['passed'] for v in verdicts) and usage==proof['receipt']['accepted_usage']
            report['phase']='complete' if report['passed'] else 'quality_failed';pins.check(full=True);report['primary_after']=dict(pins.identities)
    except BaseException as exc:
        report.update(phase_before_failure=report['phase'],phase='failed',passed=False,error_type=type(exc).__name__)
    finally:
        report['whole_controller_seconds']=time.monotonic()-start;report['whole_pod_allocation_usd']=report['whole_controller_seconds']*16.10/3600
        report['cost_scope']='Whole-controller Pod-hour allocation, not invoice or marginal cost';save()


if __name__=='__main__':main()
