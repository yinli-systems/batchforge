
from pathlib import Path
from datetime import datetime,timezone
import sys,json,hashlib,os,subprocess,fcntl,time
os.umask(0o077);base=Path(__file__).resolve().parent;sys.path.insert(0,str(base/'source'))
from ops.product_campaign import managed_identity
from ops.supervise import load_manifest
from ops.gpu_campaign import write_json
from ops.native_pause_probe import run
root=Path('/root/batchforge-runtime/app');_,services,_=load_manifest(root/'.runtime/services-primary.json',root=root)
watch={'role':'supervisor','name':'primary','env_file':str(root.parent/'supervisor.env'),'python':str(root.parent/'venv/bin/python')}
key=lambda s:s['role']+'-'+s['name'];before={key(s):managed_identity(root,s) for s in [*services,watch]}
result={'passed':False,'source_commit':json.loads((base/'source-receipt.json').read_text())['git_commit'],'driver_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
try:
 result['disposable_cpu_probe']=run()
 lock=(root/'.runtime/product-campaign.lock').open('a');deadline=time.monotonic()+600
 while True:
  try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);fcntl.flock(lock,fcntl.LOCK_UN);break
  except BlockingIOError:
   if time.monotonic()>deadline:raise RuntimeError('CI boundary did not close')
   time.sleep(2)
 preflight=root.parent/'soak480-latch-qualification-preflight-20261025-v4'
 child=subprocess.run([sys.executable,'-m','ops.qualify_worker_latch','--run',str(preflight)],stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=120)
 report=json.loads((preflight/'campaign.json').read_bytes());result['prepare_only']=report
 after={key(s):managed_identity(root,s) for s in [*services,watch]};result['managed_identities_unchanged']=before==after
 result['passed']=(child.returncode==0 and report['phase']=='prepared' and result['disposable_cpu_probe']['passed'] and before==after)
except BaseException as exc:result['error_type']=type(exc).__name__
result['captured_at']=datetime.now(timezone.utc).isoformat();write_json(base/'validation.json',result)
print(json.dumps({'passed':result['passed'],'native_cpu_rescue_test':result.get('disposable_cpu_probe',{}).get('passed'),'error_type':result.get('error_type')}),flush=True)
