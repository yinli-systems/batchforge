"""One job, existing Pod CPU, non-root; never copy production credentials."""
from pathlib import Path
import os,json,subprocess,hashlib,urllib.request,tarfile,shutil,pwd,fcntl,time,signal
BASE=Path('/root/batchforge-runtime/evidence-api-ci-20261025-v7')
STAGE=Path('/tmp/bf-ci-20261025-v7')
EXPECTED='f1caf9ae966d969f059719f52608b8982ad63836' # checked through exact workflow job SHA by the operator
SHA='08e9ccb3604d2462c24b5e33604dc30810296ca15cc65f23cffd4317eb144c37'
URL='https://github.com/actions/runner/releases/download/v2.338.1/actions-runner-linux-x64-2.338.1.tar.gz'
state={'phase':'prepared','passed':False,'gpu_inference':False,'production_credentials_copied':False,'new_cloud_resources':False}
def event(phase,**kw):
 state.update(phase=phase,updated_epoch=time.time(),**kw)
 tmp=BASE/'ci-state.json.tmp';tmp.write_text(json.dumps(state,indent=2)+'\n');tmp.replace(BASE/'ci-state.json')
def drop():
 os.setgroups([]);os.setgid(account.pw_gid);os.setuid(account.pw_uid);os.nice(19)
try:
 lock=(Path('/root/batchforge-runtime/app/.runtime/product-campaign.lock')).open('a')
 deadline=time.monotonic()+600
 while True:
  try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);break
  except BlockingIOError:
   if time.monotonic()>deadline:raise RuntimeError('Current fixed GPU study did not close')
   time.sleep(5)
 prior=json.loads(Path('/root/batchforge-runtime/soak480-reuse-20261025-v1/.runtime/run/report.json').read_text())
 assert prior['completed_tasks']==3600 and prior['unfinished_tasks']==0
 assert not Path('/proc/587745').exists()
 assert not STAGE.exists();STAGE.mkdir(mode=0o755);STAGE.chmod(0o755)
 account=pwd.getpwnam('nobody')
 state['uid']=account.pw_uid
 token=(BASE/'registration-token').read_text().strip()
 assert len(token)>10
 event('downloading_verified_public_runner')
 archive=BASE/'runner.tar.gz'
 previous=Path('/root/batchforge-runtime/evidence-api-ci-20261025-v1/runner.tar.gz')
 if previous.is_file() and hashlib.sha256(previous.read_bytes()).hexdigest()==SHA:
  shutil.copyfile(previous,archive)
 else:
  with urllib.request.urlopen(URL,timeout=90) as r,archive.open('xb') as w:shutil.copyfileobj(r,w)
 assert hashlib.sha256(archive.read_bytes()).hexdigest()==SHA
 runner=STAGE/'runner';runner.mkdir(mode=0o700)
 with tarfile.open(archive,'r:gz') as t:t.extractall(runner,filter='data')
 toolchain=STAGE/'toolchain'
 shutil.copytree('/root/batchforge-runtime/postgres-server-17/rootfs',toolchain,symlinks=True)
 shutil.copytree('/root/batchforge-runtime/postgres-client-17/rootfs',toolchain,dirs_exist_ok=True,symlinks=True)
 for p in [toolchain,*toolchain.rglob('*')]:
  if not p.is_symlink():p.chmod(0o755 if p.is_dir() or p.stat().st_mode&0o111 else 0o644)
 home=STAGE/'home';home.mkdir(mode=0o700)
 venv=STAGE/'venv'
 event('preparing_cpu_environment')
 subprocess.run(['/usr/bin/python3','-m','venv',str(venv)],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.STDOUT,timeout=90)
 for directory in [runner,home,venv]:
  for p in [directory,*directory.rglob('*')]:
   if not p.is_symlink():os.chown(p,account.pw_uid,account.pw_gid)
 env={'PATH':str(venv/'bin')+':'+str(runner/'externals/node24/bin')+':/usr/bin:/bin',
      'HOME':str(home),'LANG':'C.UTF-8','LC_ALL':'C.UTF-8','CUDA_VISIBLE_DEVICES':'',
      'AWS_EC2_METADATA_DISABLED':'true','BF_CI_PG_ROOT':str(toolchain),
      'ACTIONS_RUNNER_INPUT_TOKEN':token}
 import shlex
 hook=STAGE/'job-guard.sh'
 check="import os; expected="+repr(EXPECTED)+"; assert os.geteuid()==65534; assert os.environ.get('GITHUB_REPOSITORY')=='yinli-systems/batchforge'; assert os.environ.get('GITHUB_REF')=='refs/heads/codex/gpu-campaign-300b-evidence'; assert os.environ.get('GITHUB_EVENT_NAME')=='push'; assert os.environ.get('GITHUB_SHA')==expected; print('Trusted exact head guard passed')"
 hook.write_text('#!/bin/sh\nexec /usr/bin/python3 -c '+shlex.quote(check)+'\n');hook.chmod(0o555)
 env['ACTIONS_RUNNER_HOOK_JOB_STARTED']=str(hook)
 configlog=(BASE/'config-private.log').open('ab')
 event('configuring_ephemeral_runner')
 subprocess.run([str(runner/'config.sh'),'--unattended','--url','https://github.com/yinli-systems/batchforge',
   '--name','batchforge-cpu-20261025-v1','--labels','batchforge-cpu-ephemeral','--work','work','--ephemeral','--disableupdate'],
   cwd=runner,env=env,preexec_fn=drop,stdout=configlog,stderr=subprocess.STDOUT,check=True,timeout=90)
 env.pop('ACTIONS_RUNNER_INPUT_TOKEN');del token;(BASE/'registration-token').unlink()
 event('waiting_for_one_job',runner_archive_sha256=SHA)
 with (BASE/'runner-private.log').open('ab') as output:
  child=subprocess.Popen([str(runner/'run.sh')],cwd=runner,env=env,preexec_fn=drop,start_new_session=True,
     stdin=subprocess.DEVNULL,stdout=output,stderr=subprocess.STDOUT)
  state['runner_pid']=child.pid
  try:rc=child.wait(timeout=900)
  except subprocess.TimeoutExpired:
   os.killpg(child.pid,signal.SIGTERM)
   try:child.wait(timeout=30)
   except subprocess.TimeoutExpired:os.killpg(child.pid,signal.SIGKILL);child.wait(timeout=10)
   raise RuntimeError('One-job runner exceeded bounded lifetime')
  event('runner_exited',exit_code=rc,runner_exited_cleanly=rc==0,passed=False,job_conclusion='Verify independently through GitHub; runner exit alone is not a passed job')
except BaseException as exc:
 event('failed',phase_before_failure=state['phase'],error_type=type(exc).__name__,errno=getattr(exc,'errno',None),public_file=getattr(exc,'filename',None))
 raise SystemExit(1)
finally:
 # A leftover registration token is secret, even after an unsuccessful setup.
 (BASE/'registration-token').unlink(missing_ok=True)
