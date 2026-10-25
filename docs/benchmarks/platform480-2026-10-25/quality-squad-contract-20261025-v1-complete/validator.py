from pathlib import Path
from datetime import datetime,timezone
import os,sys,json,fcntl,time,hashlib,subprocess
BASE=Path(__file__).resolve().parent
ROOT=Path('/root/batchforge-runtime/app')
SOURCE=BASE/'source-v1';sys.path.insert(0,str(SOURCE))
from ops.ha.retune_full_gpu import file_env
from ops.ha.set_scheduler_policy import PolicyManager
from ops.supervise import load_manifest
from ops.product_campaign import managed_identity
from ops.gpu_campaign import write_json
from scripts.quality_eval import DATASET_SHA256,examples,MODEL
from scripts.answer_contract import messages
import psycopg
os.umask(0o077)
report={'phase':'waiting_ci','passed':False,'events':[],'model_switch':False,'service_actions':False,'cloud_actions':False,'quota_changes':False,'retention_changes':False,'hourly_rate_operator_confirmed':16.10,'measured_invoice':None}
def event(phase):
 report['phase']=phase;report['events'].append({'phase':phase,'at':datetime.now(timezone.utc).isoformat()});write_json(BASE/'validation.json',report)
def paired(scores,baseline):
 rows=[json.loads(x) for x in scores.read_text().splitlines()]
 assert rows and len({r['id'] for r in rows})==len(rows)
 assert all(r['id'] in baseline for r in rows)
 return {'tasks':len(rows),'baseline_exact_match_percent':100*sum(baseline[r['id']]['exact_match'] for r in rows)/len(rows),'baseline_f1_percent':100*sum(baseline[r['id']]['f1'] for r in rows)/len(rows),
  'candidate_wins':sum(r['exact_match']>baseline[r['id']]['exact_match'] for r in rows),'candidate_losses':sum(r['exact_match']<baseline[r['id']]['exact_match'] for r in rows),
  'same_exact_status':sum(r['exact_match']==baseline[r['id']]['exact_match'] for r in rows),'baseline_scores_sha256':hashlib.sha256(baseline_raw).hexdigest()}
def run_quality(out,tasks,seed,exclude=None):
 cmd=[str(ROOT.parent/'venv/bin/python'),'-m','scripts.quality_eval','--dataset',str(dataset),'--out',str(out),'--tasks',str(tasks),'--seed',str(seed),'--runtime-fingerprint',fp,'--answer-contract','json-span-v1','--deadline','1200','--min-em','.70','--min-f1','.80']
 if exclude:cmd+=['--exclude-ids',str(exclude)]
 with (out.parent/'quality-private.log').open('xb') as log:
  rc=subprocess.run(cmd,cwd=SOURCE,env={'PATH':'/usr/bin:/bin','HOME':'/root','BATCHFORGE_API_KEY':key},stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,timeout=1350).returncode
 if rc or not (out/'report.json').exists():raise RuntimeError('Quality child did not close')
 return json.loads((out/'report.json').read_text())
event('waiting_ci')
try:
 end=time.monotonic()+1200
 while time.monotonic()<end:
  ci=Path('/root/batchforge-runtime/evidence-api-ci-20261025-v3/ci-state.json')
  if ci.exists():
   state=json.loads(ci.read_text())
   if state['phase']=='failed':raise RuntimeError('Predecessor CPU validation failed')
   if state['phase']=='runner_exited':break
  time.sleep(5)
 else:raise RuntimeError('CPU predecessor did not close')
 lock=(ROOT/'.runtime/product-campaign.lock').open('a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
 event('preflight')
 receipt=json.loads((BASE/'source-receipt.json').read_text())
 for name,sha in receipt['source_sha256'].items():assert hashlib.sha256((SOURCE/name).read_bytes()).hexdigest()==sha
 _,services,_=load_manifest(ROOT/'.runtime/services-primary.json',root=ROOT)
 manager=PolicyManager(root=ROOT,timeout=30)
 identities={s['role']+'-'+s['name']:managed_identity(ROOT,s) for s in services}
 assert all(manager.readiness(s) for s in services)
 worker=file_env(next(s['env_file'] for s in services if s['role']=='worker'));assert worker['MODEL_NAME']==MODEL
 fp=json.loads(worker['SCHEDULER_PROFILE_JSON'])['runtime_fingerprint']
 api=file_env(next(s['env_file'] for s in services if s['role']=='api'));key=file_env(ROOT.parent/'access.env')['BATCHFORGE_API_KEY']
 dataset=ROOT/'.runtime/public-data/dev-v1.1.json';assert hashlib.sha256(dataset.read_bytes()).hexdigest()==DATASET_SHA256
 with psycopg.connect(api['DATABASE_URL'],connect_timeout=5,options='-c default_transaction_read_only=on -c statement_timeout=5000') as db:
  tenant=db.execute('SELECT tenant_id FROM api_keys WHERE key_hash=%s',(hashlib.sha256(key.encode()).hexdigest(),)).fetchone()[0]
  retained,maximum=db.execute('SELECT u.retained_tasks,l.max_retained_tasks FROM tenant_retention_usage u JOIN tenant_retention_limits l USING(tenant_id) WHERE u.tenant_id=%s',(tenant,)).fetchone();assert maximum-retained>=1100
  assert db.execute("SELECT count(*) FROM tasks WHERE status IN ('pending','running')").fetchone()[0]==0
 data=json.loads(dataset.read_text());cal=examples(data,100,202610251);ids=[r['id'] for r in cal]
 hold=examples(data,1000,202610252,ids);assert not set(ids)&{r['id'] for r in hold}
 excluded=BASE/'calibration-ids.json';excluded.write_text(json.dumps(ids)+'\n')
 baseline_raw=Path('/root/batchforge-runtime/quality-squad-20261025-v4/run/scores.jsonl').read_bytes();baseline={r['id']:r for r in map(json.loads,baseline_raw.splitlines())}
 assert len(baseline)==10570
 report.update(source_commit=receipt['git_commit'],runtime_fingerprint=fp,original_identities=identities,
  calibration_ids_sha256=hashlib.sha256(json.dumps(sorted(ids),separators=(',',':')).encode()).hexdigest(),
  holdout_ids_sha256=hashlib.sha256(json.dumps(sorted(r['id'] for r in hold),separators=(',',':')).encode()).hexdigest(),
  baseline_scores_sha256=hashlib.sha256(baseline_raw).hexdigest(),source_contract_sha256=receipt['source_sha256']['scripts/answer_contract.py'],retained_before=retained)
 event('calibration_running');cal_report=run_quality(BASE/'run',100,202610251)
 report['calibration']={k:cal_report.get(k) for k in ['passed','delivery_passed','evaluation_valid','exact_match_percent','f1_percent','invalid_answer_contracts','elapsed_seconds']}
 if cal_report.get('evaluation_valid'):report['calibration_paired']=paired(BASE/'run/scores.jsonl',baseline)
 if cal_report.get('passed') is True:
  holdbase=ROOT.parent/'quality-squad-contract-holdout-20261025-v1';holdbase.mkdir(mode=0o700,exist_ok=False)
  write_json(holdbase/'source-receipt.json',receipt)
  event('holdout_running');h=run_quality(holdbase/'run',1000,202610252,excluded)
  report['holdout_run']=str(holdbase);report['holdout']={k:h.get(k) for k in ['passed','delivery_passed','evaluation_valid','exact_match_percent','f1_percent','invalid_answer_contracts','elapsed_seconds']}
  if h.get('evaluation_valid'):report['holdout_paired']=paired(holdbase/'run/scores.jsonl',baseline)
  report['passed']=h.get('passed') is True
 else:report['holdout_skipped']='Calibration failed unchanged 70/80 quality gate; no frozen holdout consumed'
 assert {s['role']+'-'+s['name']:managed_identity(ROOT,s) for s in services}==identities
 report['managed_identities_unchanged']=True;event('complete')
except BaseException as exc:
 report.update(phase_before_failure=report['phase'],error_type=type(exc).__name__,passed=False);event('failed')
print(json.dumps({'phase':report['phase'],'passed':report['passed']}),flush=True)
