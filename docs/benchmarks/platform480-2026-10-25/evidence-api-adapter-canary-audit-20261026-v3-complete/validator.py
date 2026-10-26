from pathlib import Path
from datetime import datetime
import json,traceback
from ops.engine_quality_campaign import load_config,Platform,read,save,sha,acquire,release
from ops.manage import process_identity
from ops.worker_latch_guard import Pins
root=Path('/root/batchforge-runtime/soak480-engine-quality-adapter-controller-loss-20261026-v4')
base=Path('/root/batchforge-runtime/evidence-api-adapter-canary-audit-20261026-v3')
result={'passed':False,'phase':'auditing','new_gpu_inference_calls':0,'scope':'Source-matched controller-loss and exact profile restoration qualification, not complete canary delivery/quality or GPU hardware failure tolerance'}
locks=[];p=None
try:
 locks=acquire();parent=read(root/'campaign.json');c=load_config(root/'restore-private.json',parent['config_sha256'])
 restored=read(root/'recovery-validation.json');proof=read(root/'dispatch-observation.json')
 assert proof['intentional_controller_exit']==42 and proof['engine_not_quiescent'] and proof['four_h100_sample_observed']
 assert restored['passed'] and c['operator_commit']=='9f6c1b075d0e624190828dcd129ae95d0e45a64e'
 assert process_identity(c['controller']['pid'])!=c['controller'] and process_identity(read(root/'runner.json')['identity']['pid'])!=read(root/'runner.json')['identity']
 p=Platform(c);p.check()
 for path,raw in p.original.items():assert path.read_bytes()==raw
 pins=Pins();pins.check(full=True);assert pins.identities==restored['restored_identities']
 for s in pins.services:p.identity(s);assert p.manager.readiness(s)
 quiet=p.quiet(seconds=20)
 guard=read(Path(restored['guard_run'])/'validation.json');assert guard['phase'] in ('armed','waiting_campaign_boundary') and guard['recovery_invocations']==0
 assert process_identity(restored['guard_identity']['pid'])==restored['guard_identity']
 assert guard['deadline_epoch']==1793005200
 h=p.http
 capacity=h.get('http://127.0.0.1:8080/v1/capacity',headers={'Authorization':'Bearer '+p.key});capacity.raise_for_status();capacity=capacity.json()
 rows=[json.loads(v) for v in (root/'run/scores.jsonl').read_bytes().splitlines()]
 intents={(r['phase'],r['id']) for r in rows if r['event']=='inference.intent'}
 completed={(r['phase'],r['record']['id']) for r in rows if r['event']=='inference.terminal' and r['record'].get('status')=='completed'}
 assert completed<=intents and len(intents)>0
 result.update(passed=True,phase='complete',source_commit=c['operator_commit'],
  actual_engine_dispatch_recorded=True,primary_configuration_bytes_restored=True,identities=pins.identities,
  protected_engine_gateway_unchanged=all(pins.identities[k]==c['identities'][k] for k in ('engine-qwen480bench','gateway-edge')),
  observed_controller_loss_to_restoration_seconds=(datetime.fromisoformat(restored['finished_at'])-datetime.fromisoformat(proof['at'])).total_seconds(),
  finite_guard_run=restored['guard_run'],finite_guard_identity=restored['guard_identity'],finite_guard_recovery_invocations=0,
  journaled_engine_inference_intents=len(intents),known_completed_before_loss=len(completed),uncertain_or_unfinished_outcomes=len(intents-completed),
  unknown_outcome_usage_is_not_zero=True,quality_not_measured=True,quiet_observations=quiet,
  retained_tasks=capacity['retained_tasks'],task_payload_bytes=capacity['task_payload_bytes'],
  restoration_report_sha256=sha((root/'recovery-validation.json').read_bytes()))
except BaseException as exc:
 result.update(phase='failed',error_type=type(exc).__name__,frames=[{'file':Path(f.filename).name,'line':f.lineno} for f in traceback.extract_tb(exc.__traceback__)])
finally:
 if p:p.close()
 release(locks);result['validator_sha256']=sha((base/'validator.py').read_bytes());save(base/'validation.json',result)
print('BF_ADAPTER_AUDIT',json.dumps({'passed':result['passed'],'phase':result['phase']}))
