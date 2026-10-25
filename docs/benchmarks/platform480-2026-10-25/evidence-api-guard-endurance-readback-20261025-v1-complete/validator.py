
from pathlib import Path
from datetime import datetime,timezone
import sys,json,time,os,hashlib
os.umask(0o077)
root=Path('/root/batchforge-runtime');source=root/'evidence-api-fleet-20261025-v2/source';sys.path.insert(0,str(source))
from ops.ha.retune_full_gpu import file_env
from ops.gpu_campaign import write_json
from scripts.verify_receipt import readback
import httpx,psycopg
from psycopg.rows import dict_row
base=root/'soak480-guard-endurance-20261025-v1';audit=Path(__file__).resolve().parent;out=audit/'validation.json';assert not out.exists()
release=json.loads((root/'evidence-api-fleet-release-20261025-v2/release.json').read_bytes());assert release['passed'] and release['phase']=='complete'
deadline=time.monotonic()+6000
while True:
 state=json.loads((base/'campaign.json').read_text())
 if state['phase']=='failed':raise RuntimeError('Fixed800-task predecessor failed; retain audit failure')
 if state['phase']=='complete':
  assert state['passed'];break
 if time.monotonic()>deadline:raise RuntimeError('Fixed800-task report not closed within bound')
 time.sleep(5)

config=json.loads((root/'app/.runtime/services-primary.json').read_bytes())['services']
env=file_env(next(s['env_file'] for s in config if s['role']=='api'));key=file_env(root/'access.env')['BATCHFORGE_API_KEY']
report={'passed':False,'started_at':datetime.now(timezone.utc).isoformat(),'source_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
 'sample_indices':[0,20,39],'services_changed':False,'input_uploads':0,'inference_submissions':0,
 'scope':'Closed800-task cohort; independent authenticated S3-backed byte sample after normal DB payload purge; no raw payload export'}
try:
 with psycopg.connect(env['DATABASE_URL'],autocommit=True,row_factory=dict_row,connect_timeout=5,
  options='-c default_transaction_read_only=on -c statement_timeout=10000') as db:
  tenant=db.execute('SELECT tenant_id FROM api_keys WHERE key_hash=%s AND NOT revoked',(hashlib.sha256(key.encode()).hexdigest(),)).fetchone()['tenant_id']
  rows=db.execute("SELECT id,metadata,total_count,completed_count FROM batches WHERE tenant_id=%s AND metadata->>'sustained_job'=%s",(tenant,base.name)).fetchall()
  assert len(rows)==40 and all(r['total_count']==r['completed_count']==20 for r in rows)
  selected={int(r['metadata']['sustained_index']):r['id'] for r in rows}
  until=time.monotonic()+240
  while True:
   archive=db.execute("SELECT count(*) FILTER(WHERE r.state='ready' AND r.backend='s3') AS ready_s3_archives,coalesce(sum(r.rows_purged),0) AS rows_purged,count(*) FILTER(WHERE r.purged_at IS NOT NULL) AS fully_purged FROM result_archive_runs r JOIN batches b ON b.id=r.batch_id AND b.tenant_id=r.tenant_id WHERE b.tenant_id=%s AND b.metadata->>'sustained_job'=%s",(tenant,base.name)).fetchone()
   payload=db.execute("SELECT count(*) AS tasks,count(*) FILTER(WHERE t.response IS NOT NULL) AS responses_remaining FROM tasks t JOIN batches b ON b.id=t.batch_id WHERE b.tenant_id=%s AND b.metadata->>'sustained_job'=%s",(tenant,base.name)).fetchone()
   if archive['ready_s3_archives']==40 and archive['fully_purged']==40 and archive['rows_purged']==800 and payload['tasks']==800 and payload['responses_remaining']==0:break
   if time.monotonic()>until:raise RuntimeError('Normal full-cohort archive/purge not observed within bound')
   time.sleep(5)
  report['full_cohort_archive']={k:int(v) for k,v in archive.items()};report['full_cohort_database_payload']={k:int(v) for k,v in payload.items()}
  report['database_read_only_verified']=db.execute('SHOW default_transaction_read_only').fetchone()['default_transaction_read_only']=='on'
 with httpx.Client(base_url='http://127.0.0.1:8080',headers={'Authorization':'Bearer '+key},trust_env=False,timeout=30,follow_redirects=False) as client:
  records=[]
  for index in report['sample_indices']:
   deadline=time.monotonic()+240
   while True:
    response=client.get('/v1/batches/'+selected[index]+'/receipt');response.raise_for_status();envelope=response.json()
    assert envelope['receipt']['ledger_state']=='consistent_ledger' and envelope['receipt']['checks']['live_task_attempt_counts_match']
    ready=all(a.get('sha256') for a in envelope['receipt']['artifacts'])
    if ready:break
    if time.monotonic()>deadline:raise RuntimeError('Archive hash not published within bounded wait')
    time.sleep(10)
   proof=readback(client,envelope,max_bytes=2*1024**2)
   records.append({'index':index,'readback':proof,'accepted_usage':envelope['receipt']['accepted_usage']})
  report['batches']=records
  guard=root/'evidence-api-worker-latch-guard-primary-20261025-v1'
  g=json.loads((guard/'validation.json').read_bytes());launch=json.loads((guard/'launcher.json').read_bytes())
  from ops.manage import process_identity
  assert process_identity(launch['identity']['pid'])==launch['identity'] and g['phase']=='armed' and g['recovery_invocations']==0
  report['finite_guard_observation']={k:g[k] for k in ['phase','observations','recovery_invocations','last_observation','controller_sha256','initial_budget_available','deadline_epoch']}
  report['finite_guard_identity']=launch['identity']
  report['passed']=all(r['readback']['passed'] for r in records)
except BaseException as exc:report['error_type']=type(exc).__name__
report['finished_at']=datetime.now(timezone.utc).isoformat();write_json(out,report)
print(json.dumps({'passed':report['passed'],'artifacts':sum(len(r['readback']['artifacts']) for r in report.get('batches',[])),'error_type':report.get('error_type')}))
