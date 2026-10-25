
from pathlib import Path
from datetime import datetime,timezone
import sys,os,json,time,hashlib,fcntl,urllib.request
os.umask(0o077)
root=Path('/root/batchforge-runtime/app');base=root.parent/'evidence-api-reuse-recovery-20261025-v1'
sys.path.insert(0,str(root))
from ops.ha.retune_full_gpu import file_env
from ops.ha.set_scheduler_policy import PolicyManager
from ops.product_campaign import managed_identity,wait_until
from ops.supervise import load_manifest
from ops.gpu_campaign import write_json
from worker.kv_admission import engine_quiescent
import psycopg
from psycopg.rows import dict_row
MODEL='QuantTrio/Qwen3-Coder-480B-A35B-Instruct-AWQ'
report={'passed':False,'phase':'preflight','started_at':datetime.now(timezone.utc).isoformat(),'events':[],
 'reason':'Unplanned engine transport failure latched exact-token admission; supervisor only restarts dead processes',
 'source_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'source_or_environment_changed':False,
 'engine_actions':False,'api_actions':False,'cloud_actions':False,'cost':None,
 'classification':'Unplanned recovery; the 3600-task campaign is not uninterrupted endurance even if task delivery later succeeds'}
def save(phase):
 report['phase']=phase;report['events'].append({'phase':phase,'at':datetime.now(timezone.utc).isoformat()});write_json(base/'validation.json',report)
def text(port,path):
 with urllib.request.urlopen('http://127.0.0.1:'+str(port)+path,timeout=5) as r:
  data=r.read(2*1024**2+1)
  if len(data)>2*1024**2:raise RuntimeError('Metrics size bound')
  return data.decode()
def gauge(body,name):
 values=[float(x.split()[-1]) for x in body.splitlines() if x.startswith(name+' ')]
 if len(values)!=1:raise RuntimeError('Worker gauge unavailable')
 return values[0]
db=None;watch_attempted=False;manager=PolicyManager(root=root,timeout=30)
try:
 lock=(root/'.runtime/worker-kv-recovery.lock').open('a+');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
 _,services,_=load_manifest(root/'.runtime/services-primary.json',root=root)
 worker=next(s for s in services if s['role']=='worker');engine=next(s for s in services if s['role']=='engine')
 assert worker['name']=='qwen480' and engine['name']=='qwen480bench'
 watchdog={'role':'supervisor','name':'primary','env_file':str(root.parent/'supervisor.env'),'python':str(root.parent/'venv/bin/python')}
 before={s['role']+'-'+s['name']:managed_identity(root,s) for s in [*services,watchdog]}
 env=file_env(worker['env_file']);assert env['MODEL_NAME']==MODEL
 env_sha=hashlib.sha256(Path(worker['env_file']).read_bytes()).hexdigest()
 worker_files={n:hashlib.sha256((root/'worker'/n).read_bytes()).hexdigest() for n in ['__init__.py','__main__.py','config.py','runtime.py','metrics.py','kv_admission.py']}
 api=file_env(next(s['env_file'] for s in services if s['role']=='api'))
 db=psycopg.connect(api['DATABASE_URL'],autocommit=True,row_factory=dict_row,connect_timeout=5,options='-c default_transaction_read_only=on -c statement_timeout=10000')
 queue=db.execute("SELECT count(*) FILTER(WHERE status='pending') AS pending,count(*) FILTER(WHERE status='running') AS running FROM tasks WHERE status IN ('pending','running')").fetchone()
 assert queue['running']==0 and queue['pending']>0
 pending=[r['id'] for r in db.execute("SELECT t.id FROM tasks t JOIN batches b ON b.id=t.batch_id WHERE b.metadata->>'sustained_job'='soak480-reuse-20261025-v1' AND t.status='pending'").fetchall()]
 completed_before=db.execute("SELECT coalesce(sum(completed_count),0) AS n FROM batches WHERE metadata->>'sustained_job'='soak480-reuse-20261025-v1'").fetchone()['n']
 metrics=text(9105,'/metrics')
 assert gauge(metrics,'batchforge_worker_adaptive_kv_paused')==1 and gauge(metrics,'batchforge_worker_inflight')==0
 assert gauge(metrics,'batchforge_worker_reserved_context_tokens')==0 and gauge(metrics,'batchforge_worker_adaptive_kv_waiting')==0
 assert engine_quiescent(text(8348,'/metrics'))
 time.sleep(1);assert engine_quiescent(text(8348,'/metrics'))
 assert {x['id'] for x in json.loads(text(8348,'/v1/models'))['data']}=={MODEL}
 report.update(original_identities=before,worker_environment_sha256=env_sha,worker_source_sha256=worker_files,
  pending_tasks_preserved=len(pending),pending_identity_sha256=hashlib.sha256('\n'.join(sorted(pending)).encode()).hexdigest(),
  completed_before=completed_before,quiescence_observed_twice=True,db_running_before=0,worker_active_before=0,
  recovery_scope='Restart only the same paused worker/configuration with zero active invocations; retain the durable pending queue and do not restart the engine')
 save('quiescence_verified')
 incident=root.parent/'soak480-reuse-20261025-v1/admission-observation.json';assert not incident.exists();write_json(incident,report.copy())
 for s in [*services,watchdog]:assert managed_identity(root,s)==before[s['role']+'-'+s['name']]
 watch_attempted=True;_,error=manager.invoke(watchdog,'stop');assert not error
 save('watchdog_paused')
 assert managed_identity(root,worker)==before['worker-qwen480']
 assert engine_quiescent(text(8348,'/metrics')) and gauge(text(9105,'/metrics'),'batchforge_worker_inflight')==0
 assert db.execute("SELECT count(*) AS n FROM tasks WHERE status='running'").fetchone()['n']==0
 _,error=manager.invoke(worker,'stop');assert not error
 save('old_worker_stopped')
 assert hashlib.sha256(Path(worker['env_file']).read_bytes()).hexdigest()==env_sha
 assert all(hashlib.sha256((root/'worker'/n).read_bytes()).hexdigest()==h for n,h in worker_files.items())
 _,error=manager.invoke(worker,'start');assert not error
 wait_until(lambda:manager.readiness(worker),90,'Same-source worker not ready')
 after=managed_identity(root,worker);assert after!=before['worker-qwen480'];report['recovered_worker_identity']=after
 save('worker_ready')
 def progressed():
  n=db.execute("SELECT coalesce(sum(completed_count),0) AS n FROM batches WHERE metadata->>'sustained_job'='soak480-reuse-20261025-v1'").fetchone()['n']
  report['completed_after']=n
  return n>=completed_before+10
 wait_until(progressed,180,'Accepted work did not resume')
 assert db.execute('SELECT count(*) AS n FROM tasks WHERE id=ANY(%s)',(pending,)).fetchone()['n']==len(pending)
 assert gauge(text(9105,'/metrics'),'batchforge_worker_adaptive_kv_paused')==0
 for s in services:
  if s['role']!='worker':assert managed_identity(root,s)==before[s['role']+'-'+s['name']]
 report['accepted_pending_identities_retained']=True;report['passed']=True;save('progress_verified')
except BaseException as exc:
 report['phase_before_failure']=report['phase'];report['error_type']=type(exc).__name__;report['passed']=False
 try:save('failed')
 except BaseException:pass
finally:
 if watch_attempted:
  _,error=manager.invoke(watchdog,'start');report['watchdog_restart_error']=error
  if error:report['passed']=False
 if db is not None:db.close()
 report['finished_at']=datetime.now(timezone.utc).isoformat();save('complete' if report['passed'] else 'failed')
print(json.dumps({'passed':report['passed'],'phase':report['phase'],'error_type':report.get('error_type')}))
