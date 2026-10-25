
from pathlib import Path
from datetime import datetime,timezone
import sys,json,hashlib,os,subprocess,fcntl,time
os.umask(0o077);base=Path(__file__).resolve().parent;sys.path.insert(0,str(base/'source'))
from ops.product_campaign import managed_identity
from ops.supervise import load_manifest,private_read
from ops.gpu_campaign import write_json
root=Path('/root/batchforge-runtime/app');_,services,_=load_manifest(root/'.runtime/services-primary.json',root=root)
watch={'role':'supervisor','name':'primary','env_file':str(root.parent/'supervisor.env'),'python':str(root.parent/'venv/bin/python')}
key=lambda s:s['role']+'-'+s['name'];result={'passed':False,'source_commit':json.loads((base/'source-receipt.json').read_text())['git_commit'],'driver_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
try:
 lock=(root/'.runtime/product-campaign.lock').open('a');deadline=time.monotonic()+600
 while True:
  try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);fcntl.flock(lock,fcntl.LOCK_UN);break
  except BlockingIOError:
   if time.monotonic()>deadline:raise RuntimeError('CI boundary did not close')
   time.sleep(2)
 before={key(s):managed_identity(root,s) for s in [*services,watch]}
 configs={s['env_file']:hashlib.sha256(private_read(Path(s['env_file']))).hexdigest() for s in [*services,watch]}
 budget=root/'.runtime/worker-kv-recovery-budget.json';budget_before=hashlib.sha256(private_read(budget)).hexdigest()
 run=root.parent/'evidence-api-worker-latch-guard-native-healthy-20261025-v1'
 command=[sys.executable,'-m','ops.worker_latch_guard','--run',str(run),'--qualified-trial',str(root.parent/'soak480-latch-qualification-20261025-v2'),'--qualified-audit',str(root.parent/'evidence-api-latch-recovery-audit-20261025-v1'),'--seconds','900','--max-observations','3','--apply']
 c=subprocess.run(command,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=90)
 gr=json.loads(private_read(run/'validation.json'));result['finite_guard_report']=gr
 state=gr['initial_state_sha256'];refusal=root.parent/'evidence-api-worker-recovery-guard-native-healthy-20261025-v1'
 r=subprocess.run([sys.executable,'-m','ops.recover_latched_worker','--run',str(refusal),'--expected-state',state],stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=60)
 rr=json.loads(private_read(refusal/'validation.json'));result['locked_healthy_refusal']=rr
 after={key(s):managed_identity(root,s) for s in [*services,watch]}
 result['managed_identities_unchanged']=before==after
 result['configuration_bytes_unchanged']=all(hashlib.sha256(private_read(Path(p))).hexdigest()==sha for p,sha in configs.items())
 result['persistent_budget_unchanged']=hashlib.sha256(private_read(budget)).hexdigest()==budget_before
 result['passed']=(c.returncode==0 and gr['passed'] and gr['phase']=='complete' and gr['observations']==3 and gr['recovery_invocations']==0
  and gr['last_observation']['worker_ready_status']==200 and gr['last_observation']['worker_metrics']['batchforge_worker_adaptive_kv_paused']==0
  and r.returncode==1 and rr['locked_state_sha256']==state and rr['service_mutation_attempted'] is False
  and result['managed_identities_unchanged'] and result['configuration_bytes_unchanged'] and result['persistent_budget_unchanged'])
except BaseException as exc:result['error_type']=type(exc).__name__
result['captured_at']=datetime.now(timezone.utc).isoformat();write_json(base/'validation.json',result)
print(json.dumps({'passed':result['passed'],'error_type':result.get('error_type'),'managed_identities_unchanged':result.get('managed_identities_unchanged'),'persistent_budget_unchanged':result.get('persistent_budget_unchanged')}),flush=True)
