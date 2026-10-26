from pathlib import Path
from datetime import datetime
import json,traceback,statistics
from ops.engine_quality_campaign import load_config,Platform,read,save,sha,acquire,release
from ops.manage import process_identity
from ops.worker_latch_guard import Pins
from scripts.answer_adapter_study import prepare,body,response_record,plan
from agents.contracts import canonical
root=Path('/root/batchforge-runtime/soak480-engine-quality-adapter-matched-20261026-v1')
base=Path('/root/batchforge-runtime/evidence-api-adapter-failure-audit-20261026-v1')
result={'passed':False,'phase':'auditing','study_passed':False,'new_gpu_inference_calls':0,'scope':'Independent failure/partial raw-response replay and exact profile restoration check; no valid complete matched quality/performance conclusion'}
locks=[];p=None
try:
 locks=acquire();parent=read(root/'campaign.json');r=read(root/'run/report.json')
 assert parent['phase']=='failed' and parent['passed'] is False and r['evaluation_valid'] is False and r['unknown_inference_outcomes']==1
 assert all(v['exact_match_percent'] is None and v['f1_percent'] is None and v['joint_percent'] is None for v in r['arms_results'].values())
 c=load_config(root/'restore-private.json',parent['config_sha256']);restored=read(root/'recovery-validation.json')
 assert restored['passed'] and c['operator_commit']=='c5ccc0d6c29a711f7a6af1b0633ff77feb116326'
 assert process_identity(c['controller']['pid'])!=c['controller'] and process_identity(read(root/'runner.json')['identity']['pid'])!=read(root/'runner.json')['identity']
 prepared=prepare(Path('/root/batchforge-runtime/app/.runtime/public-data/dev-v1.1.json').read_bytes(),count=500)
 expected=plan(prepared,runtime_fingerprint=c['plan']['runtime_fingerprint'],deadline_seconds=1800)
 assert read(root/'run/plan.json')==expected==c['plan']
 cases={v.id:v for v in prepared.cohort.cases};intents={};known={};unknown=[]
 for line in (root/'run/scores.jsonl').read_bytes().splitlines():
  v=json.loads(line);assert v['phase']==0 and v['arm']=='full-citation'
  if v['event']=='phase.started':continue
  if v['event']=='inference.intent':
   assert v['id'] not in intents and v['request_sha256']==sha(canonical(body(prepared,cases[v['id']],'full-citation')))
   intents[v['id']]=v
  else:
   value=v['record']
   if value['status']=='completed':known[value['id']]=value
   elif value['status']=='outcome_unknown':unknown.append(value)
 assert len(unknown)==1 and unknown[0]['error_type']=='TimeoutError'
 assert len(intents)==r['inference_intents']==len(known)+1
 expected_files={sha(f'0:{identity}'.encode())+'.json' for identity in known};private=root/'private-responses'
 assert {p.name for p in private.iterdir()}==expected_files
 for identity,v in known.items():
  path=private/(sha(f'0:{identity}'.encode())+'.json');assert not path.is_symlink()
  raw=path.read_bytes();assert sha(raw)==v['response_bytes_sha256']
  replay=response_record(prepared,cases[identity],'full-citation',json.loads(raw),prompt_count=intents[identity]['prompt_tokens'])
  assert all(v[k]==x for k,x in replay.items())
 assert sum(v['prompt_tokens'] for v in known.values())==r['known_prompt_tokens']
 assert sum(v['completion_tokens'] for v in known.values())==r['known_completion_tokens']
 p=Platform(c);p.check()
 for path,raw in p.original.items():assert path.read_bytes()==raw
 pins=Pins();pins.check(full=True);assert pins.identities==restored['restored_identities']
 for s in pins.services:p.identity(s);assert p.manager.readiness(s)
 quiet=p.quiet(seconds=20)
 guard=read(Path(restored['guard_run'])/'validation.json');assert guard['phase'] in ('armed','waiting_campaign_boundary') and guard['recovery_invocations']==0
 assert process_identity(restored['guard_identity']['pid'])==restored['guard_identity']
 capacity=p.http.get('http://127.0.0.1:8080/v1/capacity',headers={'Authorization':'Bearer '+p.key});capacity.raise_for_status();capacity=capacity.json()
 observations=[json.loads(v) for v in (root/'run/observations.jsonl').read_text().splitlines()];busy={i:[] for i in range(4)}
 for v in observations:
  for line in v.get('gpu_csv',[]):
   fields=[x.strip() for x in line.split(',')];busy[int(fields[0])].append(float(fields[5]))
 gaps=[b['monotonic']-a['monotonic'] for a,b in zip(observations,observations[1:])]
 assert abs(parent['cost']['window_allocation_usd']-parent['elapsed_seconds']*16.10/3600)<1e-12
 result.update(passed=True,phase='complete',source_commit=c['operator_commit'],failed_study_preserved=True,
  completed_known_responses_replayed=len(known),journaled_inference_intents=len(intents),unknown_outcome=unknown[0],unknown_usage_is_not_zero=True,
  later_arms_not_run=True,partial_quality_and_comparison_withheld=True,accepted_known_engine_usage={'prompt_tokens':r['known_prompt_tokens'],'completion_tokens':r['known_completion_tokens']},
  whole_controller_seconds=parent['elapsed_seconds'],whole_pod_wall_allocation_usd=parent['cost']['window_allocation_usd'],
  primary_configuration_bytes_restored=True,identities=pins.identities,protected_engine_gateway_unchanged=all(pins.identities[k]==c['identities'][k] for k in ('engine-qwen480bench','gateway-edge')),
  finite_guard_run=restored['guard_run'],finite_guard_identity=restored['guard_identity'],finite_guard_recovery_invocations=0,quiet_observations=quiet,
  retained_tasks=capacity['retained_tasks'],gpu_sample_count=len(observations),gpu_busy_mean=[statistics.mean(busy[i]) if busy[i] else None for i in range(4)],
  maximum_sample_gap_seconds=max(gaps,default=None),sample_gaps_over_10_seconds=sum(g>10 for g in gaps),
  no_new_batch_acceptance_claim=True,quality_not_qualified=True)
except BaseException as exc:
 result.update(phase='failed',error_type=type(exc).__name__,frames=[{'file':Path(f.filename).name,'line':f.lineno} for f in traceback.extract_tb(exc.__traceback__)])
finally:
 if p:p.close()
 release(locks);result['validator_sha256']=sha((base/'validator.py').read_bytes());save(base/'validation.json',result)
print('BF_ADAPTER_AUDIT',json.dumps({'passed':result['passed'],'phase':result['phase']}))
