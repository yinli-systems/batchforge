
from pathlib import Path
import sys,json,time,os,hashlib,subprocess
base=Path(__file__).resolve().parent
root=Path('/root/batchforge-runtime/app')
sys.path.insert(0,str(root))
from ops.gpu_campaign import write_json
from ops.ha.retune_full_gpu import file_env
from ops.supervise import load_manifest,Manager
import psycopg
os.umask(0o077)
report={'phase':'waiting_for_prior_qualification','source_commit':'f884d7f','paid_resources_created':False}
def save():write_json(base/'queue-report.json',report)
def require(v,why):
 if not v:raise RuntimeError(why)
save()
try:
 deadline=time.monotonic()+3600
 prior=Path('/root/batchforge-runtime/platform480-20261025-v1/trial-v1/trial.json')
 while True:
  p=json.loads(prior.read_text())
  if p.get('finished_at'):break
  require(time.monotonic()<deadline,'prior_trial_wait_deadline')
  time.sleep(5)
 require(p['phase']=='platform_canary_passed' and p.get('restore_failures')==[]
         and not p.get('candidate_stop_failed'),'prior_qualification_or_restoration_failed')
 platform=json.loads((prior.parent/'platform/report.json').read_text())
 require(platform['passed'] and not platform.get('cleanup_error_type'),'prior_canary_cleanup_failed')
 _,services,_=load_manifest(root/'.runtime/services-primary.json',root=root)
 manager=Manager(root=root,timeout=90)
 for service in services:
  state,error=manager.invoke(service,'status')
  require(error is None and state['running'],'serving_identity_not_restored')
 e=file_env(next(s for s in services if s['role']=='api')['env_file'])
 with psycopg.connect(e['DATABASE_URL'],connect_timeout=10,
     options='-c default_transaction_read_only=on -c statement_timeout=10000') as conn:
  require(conn.execute("select count(*) from tasks where status in ('pending','running')").fetchone()[0]==0,
          'global_queue_not_drained_for_source_update')
 expected={'worker/config.py':'e6f70e8ab7ef47059a74a09a4f105e6de8c57bcca2b990d7fd7e6a53bc9d24e4',
           'worker/kv_admission.py':'e26dff615204edde0ffdeba44271dc84ced4b6d26477f90b6df874fd6a270d6e'}
 for name,digest in expected.items():
  require(hashlib.sha256((root/name).read_bytes()).hexdigest()==digest,'serving_source_changed_inspect_before_deploy')
 backup=base/'original-worker-source';backup.mkdir(mode=0o700)
 updated={}
 for name,digest in expected.items():
  target=root/name;new=(base/'source-v1'/name).read_bytes()
  (backup/target.name).write_bytes(target.read_bytes())
  temporary=target.with_suffix('.py.platform480.tmp')
  with temporary.open('xb') as out:out.write(new);out.flush();os.fsync(out.fileno())
  os.replace(temporary,target)
  updated[name]={'before_sha256':digest,'after_sha256':hashlib.sha256(new).hexdigest()}
 report.update(phase='qualified_source_deployed_starting_kv_trial',source_updates=updated,
               prior_report_sha256=hashlib.sha256((prior.parent/'platform/report.json').read_bytes()).hexdigest())
 save()
 command=[str(root.parent/'venv/bin/python'),'-m','ops.gpu_model_trial','--run',str(base/'trial-v1'),
          '--model','/root/batchforge-runtime/gpu-takeover-20261025/model-spool',
          '--benchmark-source',str(base/'source-v1'),'--platform-canary','--abba-repeats','3',
          '--workload-repeats','3','--studies','kv']
 child=subprocess.Popen(command,cwd=base/'source-v1',stdin=subprocess.DEVNULL)
 report.update(phase='kv_trial_running',child_pid=child.pid);save()
 code=child.wait()
 report.update(phase='kv_trial_finished' if code==0 else 'kv_trial_failed',returncode=code);save()
except Exception as error:
 report.update(phase='queue_failed',error_type=type(error).__name__)
 if type(error) is RuntimeError:report['error_code']=str(error)
 save()
 raise SystemExit(1)
