from pathlib import Path
import hashlib,json,os,socket
from datetime import datetime,timezone
import httpx
from ops.worker_latch_guard import Pins
from ops.manage import process_identity
base=Path(__file__).parent
run=Path('/root/batchforge-runtime/soak480-agent-runtime-dag-20261026-v1')
report_raw=(run/'report.json').read_bytes();r=json.loads(report_raw)
assert hashlib.sha256(report_raw).hexdigest()=='b13a613d89e180f95a9b64a4a9b1c6d41f3e534e9011f94c9d9bddaa23e23811'
assert r['phase']=='complete' and r['passed'] and r['unique_accepted_tasks']==r['attempts']==14
assert len(r['artifact_readbacks'])==len({a['file_id'] for a in r['artifact_readbacks']})==28
pins=Pins(Path('/root/batchforge-runtime/app'));pins.check(full=True)
checks={'primary_identities_match_original':pins.identities==r['primary_before']==r['primary_after'],
 'native_pg_stopped':not Path('/tmp/bf-soak480-agent-runtime-dag-20261026-v1/data/postmaster.pid').exists(),
 'controller_stopped':process_identity(704001) is None,
 'cost_arithmetic':abs(r['whole_pod_allocation_usd']-r['whole_controller_seconds']*16.10/3600)<1e-10}
for port in (18170,18172):
 with socket.socket() as s:
  s.settimeout(2);checks['private_port_'+str(port)+'_closed']=s.connect_ex(('127.0.0.1',port))!=0
for port in (8080,8082,8083,9105):checks['ready_'+str(port)]=httpx.get(f'http://127.0.0.1:{port}/readyz',timeout=3,trust_env=False).status_code==200
g=json.loads(Path('/root/batchforge-runtime/evidence-api-worker-latch-guard-primary-20261025-v1/validation.json').read_bytes())
guard=process_identity(642163)
checks['guard_armed_same_process']=g['phase']=='armed' and guard is not None and str(guard['start_ticks'])=='48039170'
s={'passed':all(checks.values()),'checks':checks,'created_at':datetime.now(timezone.utc).isoformat(),
 'source_commit':r['source_commit'],'source_report_sha256':hashlib.sha256(report_raw).hexdigest(),
 'validator_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
 'gpu_tasks_submitted':0,'database_writes':0,'fresh_payload_downloads':0,
 'primary_identities':pins.identities,'guard_identity':guard,
 'scope':'Independent current process/source/configuration and cleanup checks; original controller performed PostgreSQL restart, Azure reconciliation and 28 artifact byte reads'}
(base/'validation.json').write_text(json.dumps(s,indent=2)+'\n')
print('BF_DAG_AUDIT',json.dumps({'passed':s['passed'],'checks':checks}))
assert s['passed']
