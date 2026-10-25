
from pathlib import Path
import sys,time,json,subprocess,csv,hashlib
from datetime import datetime,timezone
from collections import defaultdict
sys.path.insert(0,'/root/batchforge-runtime/evidence-api-20261025-v2/source')
from ops.ha.retune_full_gpu import file_env
from ops.supervise import load_manifest
from backend.admission_metrics import STAGES
from prometheus_client.parser import text_string_to_metric_families
import httpx
root=Path('/root/batchforge-runtime/app');base=Path('/root/batchforge-runtime/quality-squad-20261025-v4')
_,ss,_=load_manifest(root/'.runtime/services-primary.json',root=root)
apis=[s for s in ss if s['role']=='api'];token=file_env(apis[0]['env_file'])['BATCHFORGE_WORKER_TOKEN']
def scrape():
 out={}
 with httpx.Client(headers={'Authorization':'Bearer '+token},trust_env=False,timeout=10) as c:
  for s in apis:
   r=c.get('http://127.0.0.1:'+str(s['port'])+'/metrics');r.raise_for_status()
   for family in text_string_to_metric_families(r.text):
    for x in family.samples:
     if x.name in ('batchforge_admission_stage_seconds_sum','batchforge_admission_stage_seconds_count') and x.labels.get('stage') in STAGES:
      out[(s['name'],x.labels['stage'],x.labels.get('outcome'),x.name.rsplit('_',1)[-1])]=x.value
 return out
start=datetime.now(timezone.utc).isoformat();before=scrape();samples=[]
for index in range(30):
 p=subprocess.run(['nvidia-smi','--query-gpu=index,memory.used,utilization.gpu,power.draw','--format=csv,noheader,nounits'],capture_output=True,text=True,timeout=5,check=True)
 samples.append({'at':datetime.now(timezone.utc).isoformat(),'gpu_csv':p.stdout.strip().splitlines()})
 if index<29:time.sleep(2)
after=scrape();stages=[]
for api,stage,outcome,kind in before:
 if kind!='count':continue
 count=after.get((api,stage,outcome,'count'),0)-before[(api,stage,outcome,'count')]
 total=after.get((api,stage,outcome,'sum'),0)-before.get((api,stage,outcome,'sum'),0)
 if count>0 and total>=0:stages.append({'api':api,'stage':stage,'outcome':outcome,'count':count,'seconds_sum':total,'mean_seconds':total/count})
report={'started_at':start,'finished_at':datetime.now(timezone.utc).isoformat(),'source_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'stages':stages,'samples':samples,'scope':'Read-only two-API counter deltas and short GPU sample window; stages overlap and must not be added; not whole-campaign utilization'}
(base/'admission-observation.json').write_text(json.dumps(report,indent=2)+chr(10))
