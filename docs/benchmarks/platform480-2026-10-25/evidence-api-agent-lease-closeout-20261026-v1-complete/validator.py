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
parent=root.parent/'evidence-api-agent-schema-lease-20261026-v1/validation.json'
agent=root.parent/'soak480-agent-runtime-schema-lease-20261026-v1/report.json'
decoder=root.parent/'soak480-agent-runtime-grammar-lease-20261026-v1/report.json'
expected=['c98dab8282b303b2050af43b05b3bf3fb805816fd433a3c8f8fdf7d5ebf0746b','35ee75cd4765b40d125e65b888f0e5df0fa9410f246e927909b1bb0c389dbab8','7de669c68f7eaf77ab559ebe6a1a07d919d02f10852fd88594d914d2261a08f0']
records=[]
for p,sha in zip([parent,agent,decoder],expected):
 raw=p.read_bytes();assert hashlib.sha256(raw).hexdigest()==sha;r=json.loads(raw);assert r['phase']=='complete' and r['passed'];records.append(r)
p,a,d=records;pins=Pins(root);pins.check(full=True)
checks={'primary_identity_source_configuration_unchanged':pins.identities==p['primary_before']==p['primary_after']==a['primary_identities_before']==a['primary_identities_after'],
 'native_pg_stopped':not Path('/tmp/bf-soak480-agent-runtime-schema-lease-20261026-v1/data/postmaster.pid').exists(),
 'controller_stopped':process_identity(708879) is None,'fault_reclaim_gate':0<=a['fault_ack_to_next_db_lease_seconds']<=30,
 'agent_artifacts_20_distinct':len(a['artifact_readbacks'])==len({v['file_id'] for v in a['artifact_readbacks']})==20}
for port in [18170,18172]:
 with socket.socket() as s:s.settimeout(1);checks['closed_'+str(port)]=s.connect_ex(('127.0.0.1',port))!=0
for port in [8080,8082,8083,9105]:checks['ready_'+str(port)]=httpx.get(f'http://127.0.0.1:{port}/readyz',trust_env=False,timeout=3).status_code==200
g=json.loads((root.parent/'evidence-api-worker-latch-guard-primary-20261025-v1/validation.json').read_bytes());identity=process_identity(642163)
checks['guard_armed_same_identity']=g['phase']=='armed' and identity is not None and identity['start_ticks']=='48039170'
key=file_env(root.parent/'access.env')['BATCHFORGE_API_KEY'];env=file_env(next(s for s in pins.services if s['role']=='api')['env_file'])
proofs=a['child_receipts']+[d['receipt']];bids=[x['receipt']['batch_id'] for x in proofs]
usage={k:sum(x['receipt']['accepted_usage'][k] for x in proofs) for k in ['prompt_tokens','completion_tokens']}
with psycopg.connect(env['DATABASE_URL'],row_factory=dict_row,connect_timeout=5,options='-c default_transaction_read_only=on -c statement_timeout=5000 -c lock_timeout=2000') as db:
 assert db.info.get_parameters().get('sslmode')=='verify-full'
 tenant=db.execute('SELECT tenant_id FROM api_keys WHERE key_hash=%s AND NOT revoked',(hashlib.sha256(key.encode()).hexdigest(),)).fetchone()['tenant_id']
 row=db.execute('SELECT count(*) n,count(DISTINCT id) unique_tasks,sum(attempts) attempts,sum(prompt_tokens) prompt,sum(completion_tokens) output,count(*) FILTER(WHERE response IS NULL) purged FROM tasks WHERE tenant_id=%s AND batch_id=ANY(%s)',(tenant,bids)).fetchone()
 checks['azure_14_unique_14_attempts']=row['n']==row['unique_tasks']==int(row['attempts'])==14
 checks['azure_accepted_usage_matches']=int(row['prompt'])==usage['prompt_tokens'] and int(row['output'])==usage['completion_tokens']
 checks['azure_all14_response_payloads_purged_identities_preserved']=row['purged']==14
 archives=db.execute("SELECT count(*) n FROM result_archive_runs WHERE tenant_id=%s AND batch_id=ANY(%s) AND state='ready' AND purged_at IS NOT NULL",(tenant,bids)).fetchone()['n']
 checks['all11_batch_archives_ready_purged']=archives==11
readbacks=[]
with httpx.Client(base_url='http://127.0.0.1:8080',headers={'Authorization':'Bearer '+key},trust_env=False,follow_redirects=False,timeout=15) as c:
 r=c.get('/v1/batches/'+d['batch_id']+'/receipt');r.raise_for_status();proof=r.json();assert verify(proof) and all(proof['receipt']['checks'].values())
 assert proof['receipt']['accepted_usage']==d['response_usage']
 for f in proof['receipt']['artifacts']:
  r=c.get('/v1/files/'+f['id']+'/content');r.raise_for_status();sha=hashlib.sha256(r.content).hexdigest();assert sha==f['sha256'] and len(r.content)==f['bytes']
  readbacks.append({'file_id':f['id'],'sha256':sha,'bytes':len(r.content)})
checks['decoder_two_archived_objects_byte_verified']=len(readbacks)==len({x['file_id'] for x in readbacks})==2
receipt={'passed':all(checks.values()),'checks':checks,'at':datetime.now(timezone.utc).isoformat(),'source_commit':a['source_commit'],'source_reports_sha256':expected,'validator_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'accepted_usage_total_including_decoder':usage,'whole_wrapper_seconds':p['controller_seconds'],'whole_wrapper_pod_allocation_usd':p['controller_seconds']*16.10/3600,'cost_scope':'Whole wrapper wall allocation; nested child windows overlap and must not be summed; not invoice or marginal cost','decoder_artifact_byte_readbacks':readbacks,'new_gpu_tasks':0,'database_writes':0,'fresh_payload_downloads':2,'primary_identities':pins.identities}
(base/'validation.json').write_text(json.dumps(receipt,indent=2)+'\n');print('BF_LEASE_AUDIT',json.dumps({'passed':receipt['passed'],'checks':checks,'usage':usage}));assert receipt['passed']
