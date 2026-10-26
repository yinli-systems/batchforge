"""Real isolated Linux controller/guardian processes; no production services."""
import json,os,subprocess,sys,time
from pathlib import Path
import pytest
from ops.maintenance_guard import validate


@pytest.mark.parametrize('identity,seconds,command',[
    ({'pid':1,'start_ticks':'1','cwd':'/'},1,['/bin/true']),
    ({'pid':10,'start_ticks':'bad','cwd':'/'},1,['/bin/true']),
    ({'pid':10,'start_ticks':'1','cwd':'relative'},1,['/bin/true']),
    ({'pid':10,'start_ticks':'1','cwd':'/'},float('nan'),['/bin/true']),
    ({'pid':10,'start_ticks':'1','cwd':'/'},7201,['/bin/true']),
    ({'pid':10,'start_ticks':'1','cwd':'/'},1,['relative']),
    ({'pid':10,'start_ticks':'1','cwd':'/'},1,['/bin/true','bad\x00arg']),
])
def test_guard_refuses_unbound_identity_unbounded_lifetime_or_invalid_argv(identity,seconds,command):
    with pytest.raises(ValueError):validate(identity,seconds,command,1)


PARENT=r'''
import json,os,signal,sys,time
from pathlib import Path
from ops.maintenance_guard import RestorationGuard
from ops.manage import process_identity
base=Path(sys.argv[1]);mode=sys.argv[2];marker=base/'restored'
handler="from pathlib import Path;import sys,time;p=Path(sys.argv[1]);p.write_text('handler_called');time.sleep(float(sys.argv[2]));sys.exit(int(sys.argv[3]))"
delay='5' if mode=='handler_timeout' else '0'
status='7' if mode=='handler_failure' else '0'
command=[sys.executable,'-c',handler,str(marker),delay,status]
log=base/'restore-private.log'
if mode=='log_failure':log.write_text('Existing log must be preserved')
g=RestorationGuard(process_identity(os.getpid()),.15 if mode=='deadline' else 5,
    command,restore_timeout=.15 if mode=='handler_timeout' else 3,log_path=log)
(base/'guardian.json').write_text(json.dumps(process_identity(g.child.pid)))
if mode=='unused':
 g.close_pipes();g.child.wait(timeout=3);sys.exit(0)
if mode=='deadline':signal.signal(signal.SIGTERM,signal.SIG_IGN)
g.begin()
if mode=='controller_exit':os._exit(42)
if mode=='deadline':
 time.sleep(20);sys.exit(99)
if mode=='guardian_term':
 os.kill(g.child.pid,signal.SIGTERM);time.sleep(20);sys.exit(99)
try:
 result=g.restore();(base/'result.json').write_text(json.dumps(result))
except RuntimeError:
 (base/'failure.json').write_text(json.dumps(g.events))
finally:g.close_pipes()
'''


@pytest.mark.skipif(sys.platform!='linux',reason='Actual Linux pidfd guardian qualification')
@pytest.mark.parametrize('mode',['restore','controller_exit','deadline','guardian_term','unused','log_failure','handler_failure','handler_timeout'])
def test_independent_guard_survives_controller_exit_deadline_and_log_failure(tmp_path,mode):
    assert hasattr(os,'pidfd_open') and hasattr(signal_module(),'pidfd_send_signal')
    child=subprocess.Popen([sys.executable,'-c',PARENT,str(tmp_path),mode],cwd=Path(__file__).resolve().parents[1],
        stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True)
    try:rc=child.wait(timeout=12)
    except subprocess.TimeoutExpired:
        child.kill();child.wait(timeout=3);raise
    deadline=time.monotonic()+5
    while time.monotonic()<deadline:
        if mode=='unused' or (tmp_path/'restored').exists():break
        time.sleep(.02)
    if mode=='unused':assert rc==0 and not (tmp_path/'restored').exists()
    elif mode=='controller_exit':assert rc==42 and (tmp_path/'restored').exists()
    elif mode in ('deadline','guardian_term'):
        assert rc<0 and (tmp_path/'restored').exists()
        if mode=='deadline':assert rc==-signal_module().SIGKILL
    elif mode in ('handler_failure','handler_timeout'):
        assert rc==0 and (tmp_path/'failure.json').exists()
        assert any(e['phase']=='restore_failed' for e in json.loads((tmp_path/'failure.json').read_bytes()))
    else:
        r=json.loads((tmp_path/'result.json').read_bytes());assert rc==0 and r['phase']=='restored'
        if mode=='log_failure':assert r['log_error_type']=='FileExistsError' and (tmp_path/'restore-private.log').read_text()=='Existing log must be preserved'
    # Verify the same guardian exits; no retry or second handler is launched.
    from ops.manage import process_identity
    g=json.loads((tmp_path/'guardian.json').read_bytes())
    until=time.monotonic()+3
    while time.monotonic()<until and process_identity(g['pid'])==g:time.sleep(.02)
    assert process_identity(g['pid']) is None


def signal_module():
    import signal
    return signal
