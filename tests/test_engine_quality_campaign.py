import base64,json,os,signal,subprocess,sys,time
from pathlib import Path
import pytest
from ops import engine_quality_campaign as q


def test_gate_preserves_every_unrelated_byte_and_rejects_ambiguous_assignments():
    raw=b'# private\r\nMODEL_NAME="unchanged"\nexport BATCH_SUBMISSION_ENABLED = true\nSECRET=opaque\n'
    assert q.gated(raw)==b'# private\r\nMODEL_NAME="unchanged"\nBATCH_SUBMISSION_ENABLED=false\nSECRET=opaque\n'
    for value in (b'OTHER=true\n',b'BATCH_SUBMISSION_ENABLED=true\nBATCH_SUBMISSION_ENABLED=false\n'):
        with pytest.raises(ValueError):q.gated(value)


@pytest.mark.parametrize('epoch,seconds',[(q.STOP-3600,3600),(q.STOP,60),(0,59),(0,3601),(0,True)])
def test_no_evaluation_can_consume_its_recovery_margin(epoch,seconds):
    with pytest.raises(ValueError):q.window_end(epoch,seconds)


def test_cache_capacity_must_belong_to_latest_current_api_parent_segment():
    version=' vLLM API server version 0.10.2\n'
    with pytest.raises(ValueError):q.physical_kv('APIServer pid=5'+version+'GPU KV cache size: 172,912 tokens',{'pid':9})
    with pytest.raises(ValueError):q.physical_kv('APIServer pid=9'+version+'GPU KV cache size: 172,912 tokens\nAPIServer pid=9'+version+'GPU KV cache size: 1 tokens',{'pid':9})
    log='APIServer pid=5'+version+'GPU KV cache size: 1 tokens\nAPIServer pid=9'+version+'GPU KV cache size: 172,912 tokens\nAPIServer pid=9 request complete'
    assert q.physical_kv(log,{'pid':9})==172912


class Fixture:
    def __init__(self,tmp,*,worker_live=False,foreign=False,quiet=True):
        self.events=[];self.worker={'role':'worker'};self.watch={'role':'supervisor'};self.apis=[];self.original={}
        self.live=worker_live;self.foreign=foreign;self.quiet_ok=quiet
        for name in ('a','b'):
            p=tmp/(name+'.env');p.write_bytes(b'BATCH_SUBMISSION_ENABLED=false\n');p.chmod(0o600)
            self.apis.append({'role':'api','name':name,'env_file':str(p)});self.original[p]=b'BATCH_SUBMISSION_ENABLED=true\n'
    def check(self):
        self.events.append('check')
        if self.foreign:raise ValueError('Foreign source/config')
    def quiet(self,**kw):
        assert kw=={'worker':False,'seconds':240};self.events.append('quiet')
        if not self.quiet_ok:raise RuntimeError('Engine not quiescent')
        return [{'fresh':1},{'fresh':2}]
    def identity(self,s,optional=False):return {'owned':True} if s['role']=='worker' and self.live else None
    def invoke(self,s,action):
        assert action=='start';self.events.append('start '+s['role'])
    def configure(self,s,closed):
        assert closed is False;self.events.append('reopen '+s['name']);p=Path(s['env_file']);p.write_bytes(self.original[p])


def test_direct_runner_must_be_stopped_and_engine_proven_idle_before_any_reopen(tmp_path):
    f=Fixture(tmp_path)
    result=q.restore_services(f,direct_runner=True,stop_runner=lambda:f.events.append('runner stopped'))
    assert result==[{'fresh':1},{'fresh':2}]
    assert f.events==['check','runner stopped','quiet','start worker','reopen a','reopen b','start supervisor','check']


def test_unknown_engine_activity_does_not_reopen_api_or_start_worker(tmp_path):
    f=Fixture(tmp_path,quiet=False)
    with pytest.raises(RuntimeError):q.restore_services(f,direct_runner=True,stop_runner=lambda:f.events.append('runner stopped'))
    assert f.events==['check','runner stopped','quiet']
    assert all(Path(s['env_file']).read_bytes()==b'BATCH_SUBMISSION_ENABLED=false\n' for s in f.apis)


def test_before_runner_start_restore_preserves_already_accepted_batch_work(tmp_path):
    f=Fixture(tmp_path,worker_live=True,quiet=False)
    assert q.restore_services(f,direct_runner=False,stop_runner=lambda:pytest.fail('No direct runner')) is None
    assert 'quiet' not in f.events and 'start worker' not in f.events
    assert f.events==['check','reopen a','reopen b','start supervisor','check']


def test_foreign_configuration_refuses_all_mutations_including_runner_signals(tmp_path):
    f=Fixture(tmp_path,foreign=True)
    with pytest.raises(ValueError):q.restore_services(f,direct_runner=True,stop_runner=lambda:pytest.fail('No signal before validation'))
    assert f.events==['check']


def test_full_inference_requires_actual_closed_canary():
    with pytest.raises(ValueError,match='Actual controller-loss'):q.qualified_canary({},None)


def test_replaced_pid_is_not_signalled(monkeypatch):
    monkeypatch.setattr(q,'process_identity',lambda _:None)
    monkeypatch.setattr(os,'pidfd_open',lambda *_:pytest.fail('Reused/missing PID cannot bind'),raising=False)
    q.signal_exact({'pid':22,'start_ticks':'123','cwd':'/'})


def test_missing_managed_record_does_not_hide_a_live_original_process(tmp_path,monkeypatch):
    monkeypatch.setattr(q,'ROOT',tmp_path);(tmp_path/'.runtime').mkdir()
    identity={'pid':22,'start_ticks':'123','cwd':str(tmp_path)}
    p=object.__new__(q.Platform);p.c={'identities':{'worker-test':identity}}
    monkeypatch.setattr(q,'process_identity',lambda _:identity)
    with pytest.raises(ValueError,match='lost its managed record'):p.identity({'role':'worker','name':'test'},optional=True)
    monkeypatch.setattr(q,'process_identity',lambda _:None)
    assert p.identity({'role':'worker','name':'test'},optional=True) is None


@pytest.mark.skipif(sys.platform!='linux',reason='Native isolated process restoration qualification')
@pytest.mark.parametrize('mode',['controller_exit','normal','nonquiescent'])
def test_native_restoration_owner_after_controller_loss_keeps_unsafe_window_closed(tmp_path,mode):
    # Real controller/guardian/handler and an independently running toy client.
    # Production Platform remains unqualified by this CPU fixture.
    handler=r'''
import json,os,signal,sys,time
from pathlib import Path
from ops.engine_quality_campaign import restore_services,signal_exact
base=Path(sys.argv[1]);mode=sys.argv[2]
class P:
 worker={'role':'worker'};watch={'role':'supervisor'}
 apis=[{'role':'api','env_file':str(base/'a.env')}]
 original={base/'a.env':b'BATCH_SUBMISSION_ENABLED=true\n'}
 def check(self):pass
 def quiet(self,**kw):
  if mode=='nonquiescent':raise RuntimeError('Unknown engine activity')
  assert not Path('/proc/'+str(json.loads((base/'client.json').read_text())['pid'])+'/cwd').exists()
  return [{},{}]
 def identity(self,s,optional=False):return None
 def invoke(self,s,action):(base/(s['role']+'-started')).write_text('yes')
 def configure(self,s,closed):(base/'a.env').write_bytes(self.original[base/'a.env'])
p=P();identity=json.loads((base/'client.json').read_text())
try:
 restore_services(p,direct_runner=True,stop_runner=lambda:signal_exact(identity,grace=1,kill=True))
 (base/'restored').write_text('yes')
except BaseException:(base/'refused').write_text('yes');sys.exit(1)
'''
    (tmp_path/'handler.py').write_text(handler)
    parent=r'''
import os,sys,subprocess,json,time
from pathlib import Path
from ops.maintenance_guard import RestorationGuard
from ops.manage import process_identity
b=Path(sys.argv[1]);mode=sys.argv[2];(b/'a.env').write_bytes(b'BATCH_SUBMISSION_ENABLED=false\n');(b/'a.env').chmod(0o600)
client=subprocess.Popen([sys.executable,'-c','import time;time.sleep(30)'],start_new_session=True)
(b/'client.json').write_text(json.dumps(process_identity(client.pid)))
g=RestorationGuard(process_identity(os.getpid()),5,[sys.executable,str(b/'handler.py'),str(b),mode],restore_timeout=3)
g.begin()
if mode=='controller_exit':os._exit(42)
try:g.restore()
except RuntimeError:pass
'''
    env=os.environ.copy();env['PYTHONPATH']=str(Path(__file__).resolve().parents[1])
    child=subprocess.Popen([sys.executable,'-c',parent,str(tmp_path),mode],env=env,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    try:
        rc=child.wait(timeout=10);end=time.monotonic()+5
        while time.monotonic()<end and not any((tmp_path/n).exists() for n in ('restored','refused')):time.sleep(.02)
        if mode=='nonquiescent':
            assert (tmp_path/'refused').exists() and not (tmp_path/'worker-started').exists()
            assert (tmp_path/'a.env').read_bytes()==b'BATCH_SUBMISSION_ENABLED=false\n'
        else:
            assert (tmp_path/'restored').exists() and (tmp_path/'worker-started').exists()
            assert (tmp_path/'a.env').read_bytes()==b'BATCH_SUBMISSION_ENABLED=true\n'
        assert rc==(42 if mode=='controller_exit' else 0)
    finally:
        if child.poll() is None:child.kill();child.wait()
        if (tmp_path/'client.json').exists():q.signal_exact(json.loads((tmp_path/'client.json').read_text()),grace=1,kill=True)
