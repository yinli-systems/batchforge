"""Independent post-study read-only closure, preserving task-quality failure."""
from pathlib import Path
from datetime import datetime,timezone
import hashlib,json,socket
import httpx,psycopg
from psycopg.rows import dict_row
from ops.worker_latch_guard import Pins
from ops.manage import process_identity
from ops.ha.retune_full_gpu import file_env
from backend.receipts import verify
base=Path(__file__).parent;root=Path('/root/batchforge-runtime/app');cohort=root.parent/'soak480-agent-runtime-dag-source-lineage-20261026-v1'
raw=(cohort/'report.json').read_bytes();assert hashlib.sha256(raw).hexdigest()=='ac3ba779f4e47ce4459a520eddc092090d5be2e574989557df40da4011e77072'
x=json.loads(raw);assert x['phase']=='quality_failed' and not x['passed'] and len(x['verdicts'])==3
pins=Pins(root);pins.check(full=True)
launch=json.loads((root.parent/'evidence-api-agent-runtime-20261026-v23/validation-launcher.json').read_bytes())['identity']
checks={'quality_failure_preserved':sum(v['passed'] for v in x['verdicts'])==1 and sum(v['checks']['root_reference_correct'] for v in x['verdicts'])==1,
 'all_nonquality_workflow_checks_passed':all(v['checks'][k] for v in x['verdicts'] for k in v['checks'] if k!='root_reference_correct'),
 'primary_identity_source_configuration_unchanged':pins.identities==x['primary_before']==x['primary_after'],
 'controller_stopped':process_identity(launch['pid'])!=launch,'all_trial_processes_stopped':x['all_child_processes_stopped'],
 'native_pg_stopped':x['native_pg_stopped'] and x['native_pg_timeout_exited'] and not Path('/tmp/bf-soak480-agent-runtime-dag-source-lineage-20261026-v1/data/postmaster.pid').exists(),
 'native_and_api_restart_readback':x['native_postgres_and_api_restart_readback'],
 'client_verified_all_six_inheriting_roles':x['actual_client_readback']['passed'] and x['actual_client_readback']['verified_inheriting_roles']==6 and x['actual_client_readback']['roles']==10,
 'committed_activation_fault_recorded':bool(x.get('coordinator_fault'))}
for port in (18170,18172):
 with socket.socket() as s:s.settimeout(1);checks['closed_'+str(port)]=s.connect_ex(('127.0.0.1',port))!=0
for port in (8080,8082,8083,9105):checks['ready_'+str(port)]=httpx.get(f'http://127.0.0.1:{port}/readyz',trust_env=False,timeout=3).status_code==200
gpath=root.parent/'evidence-api-worker-latch-guard-soak480-engine-product-release-primary-20261026-v1';g=json.loads((gpath/'validation.json').read_bytes());gi=json.loads((gpath/'launcher.json').read_bytes())['identity']
checks['guard_live_zero_actions']=g['phase'] in ('armed','waiting_campaign_boundary') and g['recovery_invocations']==0 and process_identity(gi['pid'])==gi
key=file_env(root.parent/'access.env')['BATCHFORGE_API_KEY'];env=file_env(next(s for s in pins.services if s['role']=='api')['env_file'])
proofs=x['child_receipts'];bids=[p['receipt']['batch_id'] for p in proofs];assert len(bids)==len(set(bids));usage=x['accepted_usage']
with psycopg.connect(env['DATABASE_URL'],row_factory=dict_row,connect_timeout=5,options='-c default_transaction_read_only=on -c statement_timeout=5000 -c lock_timeout=2000') as db:
 assert db.info.get_parameters().get('sslmode')=='verify-full' and db.execute('SHOW transaction_read_only').fetchone()['transaction_read_only']=='on'
 tenant=db.execute('SELECT tenant_id FROM api_keys WHERE key_hash=%s AND NOT revoked',(hashlib.sha256(key.encode()).hexdigest(),)).fetchone()['tenant_id']
 row=db.execute('SELECT count(*) n,count(DISTINCT id) unique_tasks,sum(attempts) attempts,sum(prompt_tokens) prompt,sum(completion_tokens) output,count(*) FILTER(WHERE response IS NULL) purged FROM tasks WHERE tenant_id=%s AND batch_id=ANY(%s)',(tenant,bids)).fetchone()
 checks['azure_unique_tasks_and_attempts']=row['n']==row['unique_tasks']==x['unique_accepted_tasks']==len(bids) and int(row['attempts'])==x['attempts']
 checks['azure_usage_matches']=int(row['prompt'])==usage['prompt_tokens'] and int(row['output'])==usage['completion_tokens']
 checks['azure_purged_payloads_retained_identities']=row['purged']==len(bids)
 ready=db.execute("SELECT count(*) n FROM result_archive_runs WHERE tenant_id=%s AND batch_id=ANY(%s) AND state='ready' AND purged_at IS NOT NULL",(tenant,bids)).fetchone()['n']
 checks['all_archives_ready_purged']=ready==len(bids)
 checks['queue_empty']=db.execute("SELECT count(*) n FROM tasks WHERE status IN ('pending','running')").fetchone()['n']==0
reads=[]
with httpx.Client(base_url='http://127.0.0.1:8080',headers={'Authorization':'Bearer '+key},trust_env=False,follow_redirects=False,timeout=15) as c:
 for old in proofs:
  r=c.get('/v1/batches/'+old['receipt']['batch_id']+'/receipt');r.raise_for_status();p=r.json()
  assert verify(p) and all(p['receipt']['checks'].values()) and p['receipt']['accepted_usage']==old['receipt']['accepted_usage']
  for f in p['receipt']['artifacts']:
   r=c.get('/v1/files/'+f['id']+'/content');r.raise_for_status();sha=hashlib.sha256(r.content).hexdigest();assert sha==f['sha256'] and len(r.content)==f['bytes'];reads.append({'file_id':f['id'],'sha256':sha,'bytes':len(r.content)})
 r=c.get('/v1/capacity');r.raise_for_status();cap=r.json()
checks['all_distinct_artifacts_read']=len(reads)==len({r['file_id'] for r in reads})==2*len(bids)
checks['quota_unchanged']=cap['retained_tasks']['limit']==50000 and cap['retained_tasks']['reserved']==0 and cap['retained_tasks']['available']==x['headroom_before']-len(bids)
result={'passed':all(checks.values()),'workflow_quality_passed':False,'workflows_reference_correct':1,'workflows':3,'roles_completed':10,
 'source_report_sha256':hashlib.sha256(raw).hexdigest(),'validator_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
 'at':datetime.now(timezone.utc).isoformat(),'checks':checks,'source_commit':x['source_commit'],'accepted_tasks':len(bids),'attempts':int(row['attempts']),
 'accepted_usage':usage,'artifact_byte_readbacks':reads,'remaining_retained_headroom':cap['retained_tasks']['available'],
 'new_gpu_tasks':0,'database_writes':0,'scope':'Independent closure/ledger/storage/client verification that preserves failed root answer quality, not a passed workflow task qualification or Azure Agent production rollout'}
(base/'validation.json').write_text(json.dumps(result,indent=2)+'\n');print('BF_SOURCE_AUDIT',json.dumps({'passed':result['passed'],'checks':checks,'tasks':len(bids),'usage':usage,'headroom':cap['retained_tasks']['available']}));assert result['passed']
