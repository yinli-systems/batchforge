from pathlib import Path
import subprocess,json,time,os,sys
base=Path(sys.argv[1]);command=json.loads(sys.argv[2])
start=time.time()
with (base/'operator-private.log').open('xb') as log:
 child=subprocess.Popen(command,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
 (base/'launcher.json').write_text(json.dumps({'pid':child.pid,'command':command,'scope':'Bounded existing-resource API-only publication; no GPU inference'})+'\n')
 rc=child.wait()
 (base/'validation.json').write_text(json.dumps({'phase':'controller_exited','observed_exit_code':rc,'elapsed_seconds':time.time()-start,'scope':'OS wait status of timeout-wrapped controller; restoration has separate receipt'})+'\n')
