from pathlib import Path
from datetime import datetime,timezone
import sys,json,time,os,hashlib
os.umask(0o077)
root=Path('/root/batchforge-runtime');source=root/'evidence-api-latch-qualification-candidate-20261025-v4/source';sys.path.insert(0,str(source))
from ops.ha.retune_full_gpu import file_env
from ops.gpu_campaign import write_json
from ops.product_campaign import managed_identity
from ops.qualify_worker_latch import strict_receipt
from scripts.verify_receipt import readback
from ops.worker_source import probe_source
import httpx,psycopg
from psycopg.rows import dict_row
base=root/'soak480-latch-qualification-20261025-v2';audit=Path(__file__).resolve().parent;out=audit/'validation.json';assert not out.exists()
trial=json.loads((base/'campaign.json').read_text());assert trial['passed'] and trial['original_configuration_restored'] and not trial['restore_errors']
config=json.loads((root/'app/.runtime/services-primary.json').read_text())['services'];before={s['role']+'-'+s['name']:managed_identity(root/'app',s) for s in config}
env=file_env(next(s['env_file'] for s in config if s['role']=='api'));key=file_env(root/'access.env')['BATCHFORGE_API_KEY']
report={'passed':False,'started_at':datetime.now(timezone.utc).isoformat(),'source_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
 'trial_report_sha256':hashlib.sha256((base/'campaign.json').read_bytes()).hexdigest(),'services_changed':False,'input_uploads':0,'inference_submissions':0,
 'scope':'Independent48-task fault-cohort strict ledger, all4 retained input/output objects byte readback, normal S3 archive/purge, restored source and fleet; no raw payload export'}
try:
 with psycopg.connect(env['DATABASE_URL'],autocommit=True,row_factory=dict_row,connect_timeout=5,
  options='-c default_transaction_read_only=on -c statement_timeout=10000') as db:
  tenant=db.execute('SELECT tenant_id FROM api_keys WHERE key_hash=%s AND NOT revoked',(hashlib.sha256(key.encode()).hexdigest(),)).fetchone()['tenant_id']
  rows=db.execute("SELECT id,metadata,total_count,completed_count FROM batches WHERE tenant_id=%s AND metadata->>'job_id'=%s",(tenant,base.name)).fetchall()
  assert len(rows)==2 and sum(r['total_count'] for r in rows)==48 and all(r['total_count']==r['completed_count'] for r in rows)
  until=time.monotonic()+240
  while True:
   archive=db.execute("SELECT count(*) FILTER(WHERE r.state='ready' AND r.backend='s3') AS ready_s3_archives,coalesce(sum(r.rows_purged),0) AS rows_purged,count(*) FILTER(WHERE r.purged_at IS NOT NULL) AS fully_purged FROM result_archive_runs r JOIN batches b ON b.id=r.batch_id AND b.tenant_id=r.tenant_id WHERE b.tenant_id=%s AND b.metadata->>'job_id'=%s",(tenant,base.name)).fetchone()
   payload=db.execute("SELECT count(*) AS tasks,count(*) FILTER(WHERE t.response IS NOT NULL) AS responses_remaining FROM tasks t JOIN batches b ON b.id=t.batch_id WHERE b.tenant_id=%s AND b.metadata->>'job_id'=%s",(tenant,base.name)).fetchone()
   if archive['ready_s3_archives']==2 and archive['fully_purged']==2 and archive['rows_purged']==48 and payload['tasks']==48 and payload['responses_remaining']==0:break
   if time.monotonic()>until:raise RuntimeError('Normal full-cohort archive/purge not observed within bound')
   time.sleep(5)
  report['full_cohort_archive']={k:int(v) for k,v in archive.items()};report['full_cohort_database_payload']={k:int(v) for k,v in payload.items()}
  report['attempt_outcomes']=db.execute("SELECT a.status,coalesce(a.error->>'code','none') AS error_code,count(*) AS records FROM attempts a JOIN batches b ON b.id=a.batch_id WHERE b.tenant_id=%s AND b.metadata->>'job_id'=%s GROUP BY a.status,coalesce(a.error->>'code','none') ORDER BY a.status,error_code",(tenant,base.name)).fetchall()
  report['latencies']=db.execute("SELECT b.metadata->>'cohort' AS cohort,count(*) AS tasks,percentile_cont(.5) WITHIN GROUP(ORDER BY extract(epoch FROM(t.finished_at-t.created_at))) AS p50_seconds,percentile_cont(.95) WITHIN GROUP(ORDER BY extract(epoch FROM(t.finished_at-t.created_at))) AS p95_seconds,percentile_cont(.99) WITHIN GROUP(ORDER BY extract(epoch FROM(t.finished_at-t.created_at))) AS p99_seconds FROM tasks t JOIN batches b ON b.id=t.batch_id WHERE b.tenant_id=%s AND b.metadata->>'job_id'=%s GROUP BY b.metadata->>'cohort' ORDER BY cohort",(tenant,base.name)).fetchall()
  report['retention']=db.execute('SELECT retained_tasks,max_retained_tasks,max_retained_tasks-retained_tasks AS headroom FROM tenant_retention_limits JOIN tenant_retention_usage USING(tenant_id) WHERE tenant_id=%s',(tenant,)).fetchone()
  report['database_read_only_verified']=db.execute('SHOW default_transaction_read_only').fetchone()['default_transaction_read_only']=='on'
 with httpx.Client(base_url='http://127.0.0.1:8080',headers={'Authorization':'Bearer '+key},trust_env=False,timeout=30,follow_redirects=False) as client:
  records=[]
  for row in rows:
   response=client.get('/v1/batches/'+row['id']+'/receipt');response.raise_for_status();envelope=response.json();strict_receipt(envelope,row['total_count'])
   proof=readback(client,envelope,max_bytes=2*1024**2)
   records.append({'cohort':row['metadata']['cohort'],'readback':proof,'accepted_usage':envelope['receipt']['accepted_usage']})
  report['batches']=records
  w=next(s for s in config if s['role']=='worker');we=file_env(w['env_file']);ws=Path(we['PYTHONPATH']);receipt=json.loads((ws.parent/'source-receipt.json').read_text())
  report['worker_source_probe']=probe_source(w['python'],ws,we,root/'app',receipt['files']);assert we['INFERENCE_TIMEOUT_SECONDS']=='1200'
  for port in [8082,8083,9105]:
   response=client.get(f'http://127.0.0.1:{port}/readyz');response.raise_for_status()
  response=client.get('/v1/fleet');response.raise_for_status();report['fleet']=response.json()
  after={s['role']+'-'+s['name']:managed_identity(root/'app',s) for s in config}
  report['managed_identities_unchanged_during_audit']=before==after
  report['passed']=all(r['readback']['passed'] for r in records) and before==after
except BaseException as exc:report['error_type']=type(exc).__name__
report['finished_at']=datetime.now(timezone.utc).isoformat();write_json(out,report)
print(json.dumps({'passed':report['passed'],'artifacts':sum(len(r['readback']['artifacts']) for r in report.get('batches',[])),'error_type':report.get('error_type')}))
