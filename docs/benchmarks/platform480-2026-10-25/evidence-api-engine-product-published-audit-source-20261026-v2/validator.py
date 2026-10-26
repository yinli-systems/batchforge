from pathlib import Path
import json,hashlib,sys,subprocess,os
import httpx,psycopg
from ops.engine_quality_campaign import acquire,release,save,read,sha,process_environment
from ops.ha.retune_full_gpu import file_env
from ops.recover_latched_worker import require_effective_configuration
from ops.worker_latch_guard import Pins,ROOT
from ops.manage import process_identity
os.umask(0o077)
base=ROOT.parent/'evidence-api-engine-product-published-audit-20261026-v2';base.mkdir(mode=0o700,exist_ok=False)
locks=[];result={'phase':'checking','passed':False,'new_gpu_tasks':0,'production_database_mutated':False,'browser_visual_qa':False}
try:
 locks=acquire();source=Path('/root/batchforge-runtime/evidence-api-agent-runtime-20261026-v18/source')
 sr=read(source.parent/'source-receipt.json');assert sr['git_commit']=='5a0a4461d4b469875335549524bed0c52ca35cbe'
 assert all(sha((source/f).read_bytes())==v for f,v in sr['files'].items())
 run=ROOT.parent/'soak480-engine-product-release-primary-20261026-v1'
 parent=read(run/'campaign.json');restored=read(run/'recovery-validation.json');c=read(run/'restore-private.json',1024**2)
 assert parent['passed'] is True and parent['phase']=='complete' and parent['outcome']=='published'
 assert restored['passed'] is True and restored['outcome']=='published' and parent['recovery_report_sha256']==sha((run/'recovery-validation.json').read_bytes())
 assert process_identity(c['controller']['pid'])!=c['controller']
 pins=Pins();initial=pins.check(full=True)
 for k in ('engine-qwen480bench','worker-qwen480','gateway-edge'):assert pins.identities[k]==c['identities'][k]
 for api in (s for s in pins.services if s['role']=='api'):
  env=file_env(api['env_file']);assert all(env[k]==v for k,v in c['target_values'].items()) and env['BATCH_SUBMISSION_ENABLED']=='true'
  require_effective_configuration(env,Path('/proc/'+str(pins.identities['api-'+api['name']]['pid'])+'/environ').read_bytes())
 key=file_env(ROOT.parent/'access.env')['BATCHFORGE_API_KEY']
 result['endpoints']=[]
 with httpx.Client(trust_env=False,timeout=10,headers={'Authorization':'Bearer '+key}) as http:
  for port in (8080,8082,8083):
   url=f'http://127.0.0.1:{port}'
   assert http.get(url+'/readyz').status_code==200
   e=http.get(url+'/v1/engine-evaluations/engine-squad-jsonspan-20261026');e.raise_for_status()
   assert sha(e.content)==c['engine_view_sha256']==e.headers['X-BatchForge-Evidence-SHA256']
   assert http.get(url+'/v1/engine-evaluations',headers={'Authorization':'Bearer invalid_key'}).status_code==401
   assert http.get(url+'/v1/engine-evaluations/missing').status_code==404
   batch=http.get(url+'/v1/evaluations');batch.raise_for_status();states=[v['quality_state'] for v in batch.json()['data']];assert 'failed' in states and 'passed' in states
   fleet=http.get(url+'/v1/fleet');fleet.raise_for_status();fleet=fleet.json();assert fleet['rejected_reports']==0 and len(fleet['data'])==1 and fleet['data'][0]['fresh_device_count']==4
   result['endpoints'].append({'port':port,'ready':True,'engine_evidence_sha256':sha(e.content),'batch_quality_states':states,'fresh_fleet_devices':4,'invalid_key_401':True,'missing_evidence_404':True})
  fe=http.get('http://127.0.0.1:8080/app.js');fe.raise_for_status();assert sha(fe.content)==sr['files']['frontend/app.js']
 save(base/'node-private.json',{'source':str(source),'base_url':'http://127.0.0.1:8080','api_key':key,'output':str(base/'readback.json')})
 with (base/'node-private.log').open('xb') as log:
  done=subprocess.run(['/tmp/bf-ci-20261025-v19/runner/externals/node24/bin/node',str(source/'ops/verify_engine_evidence.mjs'),str(base/'node-private.json')],stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,timeout=60)
 assert done.returncode==0
 result['actual_console_client']=read(base/'readback.json');assert result['actual_console_client']['passed'] is True
 with psycopg.connect(env['DATABASE_URL'],autocommit=True,connect_timeout=5,options='-c default_transaction_read_only=on -c statement_timeout=5000') as db:
  assert db.execute('SHOW transaction_read_only').fetchone()[0]=='on'
  count,active=db.execute("SELECT count(*),count(*) FILTER(WHERE status IN ('pending','running')) FROM tasks").fetchone()
  result.update(retained_tasks=count,remaining_retained_headroom=50000-count,active_batch_tasks=active)
  assert active==0
 g=read(Path(restored['finite_guard']['run'])/'validation.json');assert g['phase'] in ('armed','waiting_campaign_boundary') and g['recovery_invocations']==0
 assert process_identity(restored['finite_guard']['identity']['pid'])==restored['finite_guard']['identity']
 assert pins.check(full=True)==initial
 result.update(phase='complete',passed=True,source_commit=sr['git_commit'],protected_inference_gateway_identities_unchanged=True,
  effective_api_sources_and_admission_verified=True,finite_guard={'run':restored['finite_guard']['run'],'identity':restored['finite_guard']['identity'],'recovery_invocations':0,'deadline_epoch':g['deadline_epoch']},identities=pins.identities,source_files=len(sr['files']))
except BaseException as exc:
 import traceback
 result.update(phase='failed',error_type=type(exc).__name__,frames=[{'file':Path(f.filename).name,'line':f.lineno} for f in traceback.extract_tb(exc.__traceback__)])
finally:
 (base/'node-private.json').unlink(missing_ok=True)
 save(base/'validation.json',result);release(locks)
print('BF_ENGINE_PUBLISHED_AUDIT',json.dumps({k:result.get(k) for k in ('phase','passed','error_type','retained_tasks','remaining_retained_headroom')}))
if not result['passed']:raise SystemExit(1)
