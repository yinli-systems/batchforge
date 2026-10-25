
from pathlib import Path
from datetime import datetime,timezone
import sys,json,hashlib,os
os.umask(0o077);base=Path(__file__).resolve().parent;sys.path.insert(0,str(base/'source'))
from ops import recover_latched_worker as module
from ops.product_campaign import managed_identity
from ops.supervise import load_manifest
from ops.gpu_campaign import write_json
root=Path('/root/batchforge-runtime/app');_,services,_=load_manifest(root/'.runtime/services-primary.json',root=root)
watch={'role':'supervisor','name':'primary','env_file':str(root.parent/'supervisor.env'),'python':str(root.parent/'venv/bin/python')}
key=lambda s:s['role']+'-'+s['name'];before={key(s):managed_identity(root,s) for s in [*services,watch]}
trace=[];observations=[]
for name in ('require_engine_binding','require_effective_configuration','probe_source'):
 original=getattr(module,name)
 def wrapped(*args,_name=name,_original=original,**kw):
  try:result=_original(*args,**kw)
  except BaseException as exc:trace.append({'check':_name,'passed':False,'error_type':type(exc).__name__});raise
  trace.append({'check':_name,'passed':True});return result
 setattr(module,name,wrapped)
original=module.require_quiescence
def capture(observation,**kwargs):
 observations.append(observation);return original(observation,**kwargs)
module.require_quiescence=capture
run=root.parent/'evidence-api-worker-recovery-native-healthy-20261025-v1';assert not run.exists()
sys.argv=['recovery-native-healthy','--run',str(run)]
exit_code=0
try:module.main()
except SystemExit as exc:exit_code=exc.code
raw=(run/'validation.json').read_bytes();report=json.loads(raw)
after={key(s):managed_identity(root,s) for s in [*services,watch]}
passed=(exit_code==1 and len(observations)==1 and observations[0]['worker_ready_status']==200
 and observations[0]['worker_metrics']['batchforge_worker_adaptive_kv_paused']==0
 and report['service_mutation_attempted'] is False and report['worker_restarted'] is False
 and all(x['passed'] for x in trace) and {x['check'] for x in trace}>={'require_engine_binding','require_effective_configuration','probe_source'} and before==after)
write_json(base/'validation.json',{'passed':passed,'captured_at':datetime.now(timezone.utc).isoformat(),'source_commit':json.loads((base/'source-receipt.json').read_text())['git_commit'],
 'driver_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'module_sha256':hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest(),
 'checks':trace,'observations':observations,'healthy_refusal_receipt_sha256':hashlib.sha256(raw).hexdigest(),'expected_refusal':report,'managed_identities_unchanged':before==after,
 'services_changed':False,'inference_submissions':0,'scope':'Actual healthy-worker refusal after immutable source/effective config/engine binding checks; no fault injected, no recovery or daemon enabled'})
print(json.dumps({'native_healthy_refusal_verified':passed,'checks':trace}))
