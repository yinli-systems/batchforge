
from pathlib import Path
import json,hashlib,math,statistics,collections,shutil,traceback
from datetime import datetime
from ops.engine_quality_campaign import load_config,Platform,save
from ops.worker_latch_guard import Pins
from ops.manage import process_identity
from scripts.engine_quality import prepare,body,response_record,plan
from scripts.benchmark import distribution
from agents.contracts import canonical
root=Path('/root/batchforge-runtime/soak480-engine-quality-full-20261026-v1')
base=Path('/root/batchforge-runtime/evidence-api-engine-quality-full-audit-20261026-v1')
parent=json.loads((root/'campaign.json').read_text())
assert parent['phase']=='complete' and parent['passed'] and parent['restoration_passed']
base.mkdir(mode=0o700,exist_ok=False)
r={'phase':'auditing','passed':False,'quality_passed':None,'new_gpu_tasks_submitted':0,'new_cloud_resources':False,'scope':'Read-only raw-response replay, arithmetic and actual post-restoration profile audit. Not a second independent scoring implementation or hardware attestation.'}
p=None
try:
 c=load_config(root/'restore-private.json',parent['config_sha256'])
 report=json.loads((root/'run/report.json').read_text());restored=json.loads((root/'recovery-validation.json').read_text())
 assert restored['passed'] and report['evaluation_valid'] and report['delivery_passed'] and report['deadline_met']
 assert report['predictions']==10570 and report['missing_predictions']==report['unknown_inference_outcomes']==0 and report['all_inference_usage_known']
 corpus=prepare(Path('/root/batchforge-runtime/app/.runtime/public-data/dev-v1.1.json').read_bytes());cases={x.id:x for x in corpus.cases}
 expected_plan=plan(corpus,runtime_fingerprint=c['plan']['runtime_fingerprint'],deadline_seconds=3600)
 assert json.loads((root/'run/plan.json').read_text())==expected_plan
 assert all(report[k]==v for k,v in expected_plan.items())
 rows=[json.loads(v) for v in (root/'run/scores.jsonl').read_text().splitlines()]
 intents={};terminals={}
 for row in rows:
  if row['event']=='inference.intent':
   assert row['id'] in cases and row['id'] not in intents
   assert row['request_sha256']==hashlib.sha256(canonical(body(cases[row['id']]))).hexdigest()
   intents[row['id']]=row
  else:
   assert row['event']=='inference.terminal';v=row['record'];assert v['id'] in cases and v['id'] not in terminals;terminals[v['id']]=v
 assert set(intents)==set(terminals)==set(cases)
 expected_files={hashlib.sha256(k.encode()).hexdigest()+'.json' for k in cases}
 private=root/'private-responses';assert private.resolve()==private and {p.name for p in private.iterdir()}==expected_files
 total_bytes=0
 for key,record in terminals.items():
  assert record['status']=='completed' and record['prompt_count']==intents[key]['prompt_tokens']
  path=private/(hashlib.sha256(key.encode()).hexdigest()+'.json');assert not path.is_symlink() and path.stat().st_size<=2*1024**2
  raw=path.read_bytes();total_bytes+=len(raw);assert total_bytes<=64*1024**2
  assert hashlib.sha256(raw).hexdigest()==record['response_bytes_sha256']
  value=json.loads(raw);replayed=response_record(cases[key],value,prompt_count=record['prompt_count'])
  assert all(record[k]==v for k,v in replayed.items())
  assert value['usage']['total_tokens']==record['prompt_tokens']+record['completion_tokens']
 complete=list(terminals.values());exact=sum(v['exact_match'] for v in complete);f1=sum(v['f1'] for v in complete)
 em=100*exact/10570;f1_percent=100*f1/10570
 assert abs(report['exact_match_percent']-em)<1e-9 and abs(report['f1_percent']-f1_percent)<1e-9
 assert report['known_prompt_tokens']==sum(v['prompt_tokens'] for v in complete)
 assert report['known_completion_tokens']==sum(v['completion_tokens'] for v in complete)
 assert report['invalid_answer_contracts']==sum(not v['answer_contract_valid'] for v in complete)
 assert report['finish_reasons']==dict(collections.Counter(v['finish_reason'] for v in complete))
 assert report['http_service_seconds']==distribution([v['http_service_seconds'] for v in complete])
 assert report['offered_cohort_completion_seconds']==distribution([v['completion_seconds'] for v in complete])
 assert abs(report['cost']['window_allocation_usd']-16.10*report['elapsed_seconds']/3600)<1e-9
 quality=em>=70 and f1_percent>=80;assert report['passed']==quality and parent['quality_passed']==quality
 p=Platform(c);p.check();fresh=Pins();state=fresh.check(full=True)
 assert all(p.identity(s)==restored['restored_identities'][s['role']+'-'+s['name']] for s in p.services)
 assert all(path.read_bytes()==v for path,v in p.original.items())
 assert all(p.manager.readiness(s) for s in fresh.services)
 observation=p.observe();guard=Path(restored['guard_run']);g=json.loads((guard/'validation.json').read_text());launch=json.loads((guard/'launcher.json').read_text())
 assert g['phase']=='armed' and g['recovery_invocations']==0 and process_identity(launch['identity']['pid'])==launch['identity']
 assert fresh.check(full=True)==state
 r.update(phase='complete',passed=True,quality_passed=quality,operator_commit=c['operator_commit'],tasks=10570,unique_intents=len(intents),unique_completed=len(terminals),exact_results=exact,exact_match_percent=em,f1_percent=f1_percent,private_response_count=len(expected_files),private_response_bytes=total_bytes,private_raw_bytes_replayed=True,request_hashes_rebuilt_without_gold=True,all_returned_usage_and_distribution_arithmetic_matches=True,actual_restored_service_profiles_verified=True,original_configuration_bytes_match=True,guard_run=str(guard),guard_identity=launch['identity'],guard_recovery_invocations=0,post_restore_observation=observation,post_restore_state_sha256=state,protected_engine_gateway_unchanged=True,root_disk_free_bytes=shutil.disk_usage('/root').free,report_sha256=hashlib.sha256((root/'run/report.json').read_bytes()).hexdigest(),plan_sha256=hashlib.sha256((root/'run/plan.json').read_bytes()).hexdigest(),scores_sha256=hashlib.sha256((root/'run/scores.jsonl').read_bytes()).hexdigest(),restore_report_sha256=hashlib.sha256((root/'recovery-validation.json').read_bytes()).hexdigest())
except BaseException as e:
 r.update(phase='failed',error_type=type(e).__name__,frames=[{'file':Path(t.filename).name,'line':t.lineno,'function':t.name} for t in traceback.extract_tb(e.__traceback__)])
finally:
 if p:p.close()
 save(base/'validation.json',r)
print(json.dumps(r))
