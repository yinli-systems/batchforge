"""Read-only independent closeout; source-bound reports supplied before launch."""
from pathlib import Path
from datetime import datetime,timezone
import hashlib,json,socket
import httpx,psycopg
from psycopg.rows import dict_row
from ops.worker_latch_guard import Pins
from ops.manage import process_identity
from ops.ha.retune_full_gpu import file_env
from backend.receipts import verify
base=Path(__file__).parent;root=Path('/root/batchforge-runtime/app')
expected=[('evidence-api-agent-schema-retrieval-span-20261026-v1', 'validation.json', 'ec25747264d48bb87d8ed2d06cec3ceb56787bda217f0baf2eaf69f4b78a3b69'), ('soak480-agent-runtime-schema-retrieval-span-20261026-v1', 'report.json', '185f036996dea80fd91864955b841628b758612f1c509aed3046017120344e95'), ('soak480-agent-runtime-grammar-retrieval-span-20261026-v1', 'report.json', 'b818f395ddcfcab978c52b7be2d434380ed0adcc211c2e249edd4ebc1d132eaf')]
records=[]
for name,filename,expected_sha in expected:
 raw=(root.parent/name/filename).read_bytes();assert hashlib.sha256(raw).hexdigest()==expected_sha;records.append(json.loads(raw))
p,a,d=records
assert p['phase'] in ('complete','failed') and a['phase'] in ('complete','quality_failed') and d['passed']
assert p['passed']==a['passed'] and a['plan']['fixture_set']=='retrieval-span-v1'
pins=Pins(root);pins.check(full=True)
launch=json.loads((root.parent/'evidence-api-agent-runtime-20261026-v21/validation-launcher.json').read_bytes())['identity']
checks={'primary_identity_source_configuration_unchanged':pins.identities==p['primary_before']==a['primary_identities_before']==a['primary_identities_after'],
 'temporary_api_stopped':p['private_api_stopped'],
 'native_pg_stopped':not Path('/tmp/bf-soak480-agent-runtime-schema-retrieval-span-20261026-v1/data/postmaster.pid').exists() and a['isolated_pg_stopped'] and a['native_pg_timeout_exited'],
 'controller_stopped':process_identity(launch['pid'])!=launch,
 'fault_reclaim_within_30s':0<=a['fault_ack_to_next_db_lease_seconds']<=30,
 'all8_terminal':len(a['task_results'])==8 and all(r['status'] in ('completed','failed','cancelled') for r in a['task_results']),
 'actual_client_consistency_verified':a['actual_client_readback']['passed'] and a['actual_client_readback']['run_count']==8}
for port in (18170,18172):
 with socket.socket() as s:s.settimeout(1);checks['closed_'+str(port)]=s.connect_ex(('127.0.0.1',port))!=0
for port in (8080,8082,8083,9105):checks['ready_'+str(port)]=httpx.get(f'http://127.0.0.1:{port}/readyz',trust_env=False,timeout=3).status_code==200
gpath=root.parent/'evidence-api-worker-latch-guard-soak480-engine-product-release-primary-20261026-v1'
g=json.loads((gpath/'validation.json').read_bytes());identity=json.loads((gpath/'launcher.json').read_bytes())['identity']
checks['guard_live_zero_actions']=g['phase'] in ('armed','waiting_campaign_boundary') and g['recovery_invocations']==0 and process_identity(identity['pid'])==identity
needle=str(root.parent/'soak480-agent-runtime-schema-retrieval-span-20261026-v1/.runtime/config.json').encode();live=[]
for proc in Path('/proc').iterdir():
 if not proc.name.isdigit():continue
 try:
  parts=(proc/'cmdline').read_bytes().split(b'\0')
  if needle in parts and b'ops.qualify_agent_runtime' in parts:live.append(int(proc.name))
 except (OSError,PermissionError):pass
checks['no_cohort_coordinators_live']=not live
key=file_env(root.parent/'access.env')['BATCHFORGE_API_KEY'];env=file_env(next(s for s in pins.services if s['role']=='api')['env_file'])
proofs=a['child_receipts']+[d['receipt']];bids=[x['receipt']['batch_id'] for x in proofs]
assert len(bids)==len(set(bids))
expected_tasks=sum(x['receipt']['counts']['total'] for x in proofs)
usage={k:sum(x['receipt']['accepted_usage'][k] for x in proofs) for k in ('prompt_tokens','completion_tokens')}
with psycopg.connect(env['DATABASE_URL'],row_factory=dict_row,connect_timeout=5,options='-c default_transaction_read_only=on -c statement_timeout=5000 -c lock_timeout=2000') as db:
 assert db.info.get_parameters().get('sslmode')=='verify-full' and db.execute('SHOW transaction_read_only').fetchone()['transaction_read_only']=='on'
 tenant=db.execute('SELECT tenant_id FROM api_keys WHERE key_hash=%s AND NOT revoked',(hashlib.sha256(key.encode()).hexdigest(),)).fetchone()['tenant_id']
 row=db.execute('SELECT count(*) n,count(DISTINCT id) unique_tasks,sum(attempts) attempts,sum(prompt_tokens) prompt,sum(completion_tokens) output,count(*) FILTER(WHERE response IS NULL) purged FROM tasks WHERE tenant_id=%s AND batch_id=ANY(%s)',(tenant,bids)).fetchone()
 checks['azure_unique_task_count_matches']=row['n']==row['unique_tasks']==expected_tasks
 checks['azure_usage_matches']=int(row['prompt'])==usage['prompt_tokens'] and int(row['output'])==usage['completion_tokens']
 checks['azure_payloads_purged_identities_preserved']=row['purged']==expected_tasks
 archives=db.execute("SELECT count(*) n FROM result_archive_runs WHERE tenant_id=%s AND batch_id=ANY(%s) AND state='ready' AND purged_at IS NOT NULL",(tenant,bids)).fetchone()['n']
 checks['all_archives_ready_purged']=archives==len(bids)
 checks['queue_empty']=db.execute("SELECT count(*) n FROM tasks WHERE status IN ('pending','running')").fetchone()['n']==0
readbacks=[]
with httpx.Client(base_url='http://127.0.0.1:8080',headers={'Authorization':'Bearer '+key},trust_env=False,follow_redirects=False,timeout=15) as c:
 for old in proofs:
  r=c.get('/v1/batches/'+old['receipt']['batch_id']+'/receipt');r.raise_for_status();proof=r.json()
  assert verify(proof) and all(proof['receipt']['checks'].values()) and proof['receipt']['accepted_usage']==old['receipt']['accepted_usage']
  for f in proof['receipt']['artifacts']:
   r=c.get('/v1/files/'+f['id']+'/content');r.raise_for_status();sha=hashlib.sha256(r.content).hexdigest();assert sha==f['sha256'] and len(r.content)==f['bytes']
   readbacks.append({'file_id':f['id'],'sha256':sha,'bytes':len(r.content)})
 cap=c.get('/v1/capacity');cap.raise_for_status();capacity=cap.json()
checks['all_distinct_artifacts_read']=len(readbacks)==len({r['file_id'] for r in readbacks})==2*len(bids)
checks['quota_unchanged']=capacity['retained_tasks']['limit']==50000 and capacity['retained_tasks']['reserved']==0
checks['headroom_matches_unique_acceptance']=capacity['retained_tasks']['available']==a['headroom_before']-expected_tasks+4 # Agent preflight runs after four decoder tasks.
qualified=sum(r['passed'] for r in a['task_results'])
wall=p['controller_seconds'];cost=wall*16.10/3600
receipt={'passed':all(checks.values()),'rag_quality_passed':a['passed'],'checks':checks,'at':datetime.now(timezone.utc).isoformat(),
 'source_commit':a['source_commit'],'source_reports_sha256':[v[2] for v in expected],'validator_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
 'unique_tasks':expected_tasks,'attempts':int(row['attempts']),'archive_batches':len(bids),'accepted_usage_including_decoder':usage,
 'whole_wrapper_seconds':wall,'whole_wrapper_pod_allocation_usd':cost,'quality_qualified_results':qualified,
 'cost_per_quality_qualified_result':cost/qualified if qualified else None,
 'cost_scope':'Whole wrapper wall allocation; nested child windows overlap; reference-exact plus supported-citation task, not invoice, marginal or business result price',
 'artifact_byte_readbacks':readbacks,'new_gpu_tasks':0,'database_writes':0,'primary_identities':pins.identities,'remaining_retained_headroom':capacity['retained_tasks']['available'],
 'scope':'Independent closure, identity, ledger, client and S3 verification. Quality gate remains separately reported; not an Azure Agent rollout or whole-Pod HA'}
(base/'validation.json').write_text(json.dumps(receipt,indent=2)+'\n');print('BF_RAG_SPAN_AUDIT',json.dumps({'passed':receipt['passed'],'quality_passed':a['passed'],'checks':checks,'usage':usage}));assert receipt['passed']
