from pathlib import Path
from datetime import datetime
import json,hashlib,statistics,traceback
from collections import Counter
from ops.engine_quality_campaign import (load_config,Platform,save,read,sha,process_environment)
from ops.worker_latch_guard import Pins
from ops.manage import process_identity
from scripts.answer_adapter_study import prepare,plan,body,response_record,ARMS
from scripts.benchmark import distribution
from agents.contracts import canonical
root=Path('/root/batchforge-runtime/soak480-engine-quality-adapter-matched-20261026-v2')
base=Path('/root/batchforge-runtime/evidence-api-adapter-study-audit-20261026-v2')
result={'phase':'checking','passed':False,'new_gpu_tasks':0,'production_database_writes':0,
 'scope':'Independent raw-response replay and arithmetic/source/configuration check using the frozen scorer, not a second independent semantic judge or GPU attestation'}
p=None
try:
 parent=read(root/'campaign.json');assert parent['phase']=='complete' and parent['passed'] and parent['restoration_passed']
 c=load_config(root/'restore-private.json',parent['config_sha256']);p=Platform(c)
 report=read(root/'run/report.json');restored=read(root/'recovery-validation.json')
 assert report['evaluation_valid'] and restored['passed'] and report['unknown_inference_outcomes']==0
 prepared=prepare(Path('/root/batchforge-runtime/app/.runtime/public-data/dev-v1.1.json').read_bytes(),count=250)
 expected=plan(prepared,runtime_fingerprint=c['plan']['runtime_fingerprint'],deadline_seconds=1800)
 assert read(root/'run/plan.json')==expected==c['plan']
 assert all(report[k]==v for k,v in expected.items())
 cases={v.id:v for v in prepared.cohort.cases};intents={};terminals={};total_bytes=0
 for line in (root/'run/scores.jsonl').read_bytes().splitlines():
  row=json.loads(line);phase=row['phase'];arm=row['arm'];assert ARMS[phase]==arm
  if row['event']=='phase.started':continue
  key=(phase,row.get('id',row.get('record',{}).get('id')));assert key[1] in cases
  if row['event']=='inference.intent':
   assert key not in intents and row['request_sha256']==sha(canonical(body(prepared,cases[key[1]],arm)))
   intents[key]=row
  else:
   assert row['event']=='inference.terminal' and key not in terminals
   value=row['record'];assert value['status']=='completed';terminals[key]=value
 assert set(intents)==set(terminals)=={(phase,identity) for phase in range(4) for identity in cases}
 private=root/'private-responses';assert not private.is_symlink()
 expected_files={sha(f'{phase}:{identity}'.encode())+'.json' for phase,identity in terminals}
 assert {p.name for p in private.iterdir()}==expected_files
 for (phase,identity),value in terminals.items():
  path=private/(sha(f'{phase}:{identity}'.encode())+'.json');assert not path.is_symlink() and path.stat().st_size<=2*1024**2
  raw=path.read_bytes();total_bytes+=len(raw);assert sha(raw)==value['response_bytes_sha256']
  assert value['prompt_count']==intents[(phase,identity)]['prompt_tokens']
  replay=response_record(prepared,cases[identity],ARMS[phase],json.loads(raw),prompt_count=value['prompt_count'])
  assert all(value[k]==v for k,v in replay.items())
 pooled={};matched=[]
 for arm in dict.fromkeys(ARMS):
  rows=[v for (phase,_),v in terminals.items() if ARMS[phase]==arm];out=report['arms_results'][arm]
  assert len(rows)==out['expected']==out['predictions']==500
  for key in ('prompt_tokens','output_tokens'):
   original='completion_tokens' if key=='output_tokens' else key
   assert out[key]==sum(v[original] for v in rows)
  exact=sum(v['exact_match'] for v in rows);f1=sum(v['f1'] for v in rows);joint=sum(v['joint_exact_and_supported'] for v in rows)
  assert out['exact_match_percent']==exact/5 and abs(out['f1_percent']-f1/5)<1e-10 and out['joint_percent']==joint/5
  assert out['invalid_outputs']==sum(not v['answer_contract_valid'] for v in rows)
  assert out['abstentions']==sum(v['abstained'] for v in rows)
  for key in ('http_service_seconds','offered_cohort_completion_seconds'):
   name='completion_seconds' if key.startswith('offered') else key
   assert out[key]==distribution([v[name] for v in rows])
  wall=sum(v['elapsed_seconds'] for v in report['phases'] if v['arm']==arm)
  assert out['wall_seconds']==wall and abs(out['whole_pod_wall_allocation_usd']-wall*16.10/3600)<1e-12
  assert abs(out['joint_results_per_second']-joint/wall)<1e-12
  assert out['allocation_per_1000_joint_results_usd'] is None if joint==0 else abs(out['allocation_per_1000_joint_results_usd']-wall*16.10/3600*1000/joint)<1e-12
  pooled[arm]=out
 for old_phase,new_phase in ((0,1),(3,2)):
  counts=Counter()
  for identity in cases:
   a=terminals[(old_phase,identity)]['joint_exact_and_supported'];b=terminals[(new_phase,identity)]['joint_exact_and_supported']
   counts['candidate_win' if b and not a else 'candidate_loss' if a and not b else 'tie']+=1
  matched.append({'old_phase':old_phase,'new_phase':new_phase,**dict(counts)})
 assert report['known_prompt_tokens']==sum(v['prompt_tokens'] for v in terminals.values())
 assert report['known_completion_tokens']==sum(v['completion_tokens'] for v in terminals.values())
 assert report['inference_intents']==1000
 observations=[json.loads(v) for v in (root/'run/observations.jsonl').read_text().splitlines()]
 sampling={}
 for phase in report['phases']:
  start=datetime.fromisoformat(phase['measurement_started_at']).timestamp();end=datetime.fromisoformat(phase['measurement_ended_at']).timestamp()
  rows=[v for v in observations if start<=datetime.fromisoformat(v['at']).timestamp()<=end]
  devices={i:[] for i in range(4)};errors=0
  for v in rows:
   if v.get('gpu_error') or v.get('metrics_error'):errors+=1
   if len(v.get('gpu_csv',[]))!=4:continue
   for csv in v['gpu_csv']:
    fields=[f.strip() for f in csv.split(',')];index=int(fields[0]);assert 'H100' in fields[2]
    devices[index].append(float(fields[5]))
  gaps=[b['monotonic']-a['monotonic'] for a,b in zip(rows,rows[1:])]
  sampling[str(phase['phase'])]={'arm':phase['arm'],'samples':len(rows),'errors':errors,'maximum_gap_seconds':max(gaps,default=None),
   'gaps_over_10_seconds':sum(g>10 for g in gaps),'gpu_busy_mean':[statistics.mean(devices[i]) if devices[i] else None for i in range(4)],
   'gpu_busy_max':[max(devices[i],default=None) for i in range(4)],'scope':'Busy gauge sampled during measured arm wall, not FLOP saturation'}
 assert process_identity(c['controller']['pid'])!=c['controller']
 assert process_identity(read(root/'runner.json')['identity']['pid'])!=read(root/'runner.json')['identity']
 for path,raw in p.original.items():assert path.read_bytes()==raw
 pins=Pins();pins.check(full=True)
 for key in ('engine-qwen480bench','gateway-edge'):assert pins.identities[key]==c['identities'][key]
 for service in pins.services:assert p.manager.readiness(service)
 p.quiet(seconds=20)
 guard=read(Path(restored['guard_run'])/'validation.json');assert guard['phase'] in ('armed','waiting_campaign_boundary') and guard['recovery_invocations']==0
 assert process_identity(restored['guard_identity']['pid'])==restored['guard_identity']
 result.update(phase='complete',passed=True,quality_passed=report['passed'],source_commit=c['operator_commit'],
  report_sha256=sha((root/'run/report.json').read_bytes()),raw_responses_replayed=1000,raw_response_bytes=total_bytes,
  all_accepted_engine_usage_known=True,source_bound_records_verified=True,arms=pooled,matched_pairs=matched,gpu_sampling=sampling,
  primary_configuration_restored=True,identities=pins.identities,finite_guard_run=restored['guard_run'],finite_guard_identity=restored['guard_identity'],
  finite_guard_recovery_invocations=0,azure_agent_ddl_deployed=False,model='480B total/~35B active MoE, existing4H100')
except BaseException as exc:
 result.update(phase='failed',error_type=type(exc).__name__,frames=[{'file':Path(v.filename).name,'line':v.lineno} for v in traceback.extract_tb(exc.__traceback__)])
finally:
 if p:p.close()
 result['validator_sha256']=sha((base/'validator.py').read_bytes());save(base/'validation.json',result)
print('BF_ADAPTER_AUDIT',json.dumps({'passed':result['passed'],'quality_passed':result.get('quality_passed'),'phase':result['phase']}))
