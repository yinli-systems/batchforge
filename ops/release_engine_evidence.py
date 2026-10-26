"""API-only engine evidence release with a separately qualified restore owner.

No worker/engine/gateway action or new Batch inference. Preparation is the
default; --apply requires a closed source-matched read-only qualification.
The sole handler restores old APIs on controller loss before commit, and only
opens the new APIs after revalidating the committed evidence. Accepted work
continues on the unchanged worker and must drain before either outcome opens.
"""
from __future__ import annotations
import argparse,base64,fcntl,json,math,os,re,signal,subprocess,sys,time
from pathlib import Path
from backend.engine_evaluations import EngineEvidenceCatalogue
from ops.engine_quality_campaign import (Platform,capture,acquire,release,sha,save,read,now,gated,
    signal_exact,process_environment)
from ops.ha.set_scheduler_policy import atomic_bytes
from ops.maintenance_guard import RestorationGuard
from ops.manage import process_identity
from ops.supervise import private_read
from ops.worker_latch_guard import Pins,ROOT,STOP
from ops.worker_source import PROBED_SOURCES

PROTOCOL='batchforge.api-engine-evidence-release.v1'
KEYS=('UVICORN_APP_DIR','FRONTEND_DIR','ENGINE_EVIDENCE_CATALOGUE')


def patch(raw,values):
    """Keep unrelated bytes, including credentials and retention policy."""
    if set(values)!=set(KEYS) or any(not v or '\n' in v or '\r' in v or '=' in v or ' ' in v for v in values.values()):
        raise ValueError('Three explicit immutable release paths required')
    rows=[];seen=set()
    for row in raw.decode().splitlines(keepends=True):
        key,sep,_=row.strip().removeprefix('export ').strip().partition('=');key=key.strip()
        if sep and key in values:
            if key in seen:raise ValueError('Duplicate release assignment')
            seen.add(key);rows.append(key+'='+values[key]+'\n')
        else:rows.append(row)
    if rows and not rows[-1].endswith(('\n','\r')):rows[-1]+='\n'
    rows.extend(k+'='+values[k]+'\n' for k in KEYS if k not in seen)
    return ''.join(rows).encode()


def owned(path):
    if (path.parent!=ROOT.parent or path.resolve()!=path or not re.fullmatch(r'soak480-engine-product-release-[a-zA-Z0-9-]+',path.name)):
        raise ValueError('New owned API release run required')
    return path


def prepare(base,guard_run,qualification):
    if (qualification.parent!=ROOT.parent or qualification.resolve()!=qualification
        or not qualification.name.startswith('evidence-api-engine-product-qualification-')):raise ValueError('Owned read-only qualification required')
    q=read(qualification/'validation.json');source=Path(__file__).resolve().parents[1]
    receipt=read(source.parent/'source-receipt.json')
    if (q.get('phase')!='complete' or q.get('passed') is not True or q.get('source_commit')!=receipt['git_commit']
        or q.get('owned_candidate_processes_stopped') is not True or q.get('cleanup_errors')!=[]
        or q.get('primary_pins_unchanged') is not True or q.get('database_read_only_verified') is not True
        or q.get('client_readback',{}).get('passed') is not True):raise ValueError('Closed source-matched real read-only qualification required')
    catalogue=Path(q['package']['catalogue'])
    if catalogue!=qualification/'engine-evidence/catalogue.json':raise ValueError('Fixed qualified catalogue required')
    if sha(private_read(catalogue))!=q['package']['catalogue_sha256']:raise ValueError('Qualified catalogue changed')
    EngineEvidenceCatalogue(catalogue)
    c=capture(base,guard_run,600,100)  # Only captures existing state; no inference is dispatched.
    c.update(protocol=PROTOCOL,qualification=str(qualification),qualification_sha256=sha(private_read(qualification/'validation.json')),
        engine_view_sha256=q['engine_view_sha256'],catalogue=str(catalogue),
        catalogue_files={str(p):sha(private_read(p,16*1024**2)) for p in catalogue.parent.iterdir()},
        target_values={'UVICORN_APP_DIR':str(source),'FRONTEND_DIR':str(source/'frontend'),'ENGINE_EVIDENCE_CATALOGUE':str(catalogue)})
    for s in c['services']:
        if s['role']=='api':
            raw=base64.b64decode(c['originals'][s['env_file']])
            if gated(raw)==raw:raise ValueError('Original primary admission must be explicitly open')
            patch(raw,c['target_values'])
    return c


def load(path,digest):
    owned(path.parent)
    if path!=path.parent/'restore-private.json' or sha(private_read(path,1024**2))!=digest:raise ValueError('Frozen private restore commitment required')
    c=read(path,1024**2);source=Path(__file__).resolve().parents[1]
    if c['protocol']!=PROTOCOL or c['base']!=str(path.parent) or c['operator_source']!=str(source):raise ValueError('Restore role source differs')
    if any(sha((source/n).read_bytes())!=v for n,v in c['operator_files'].items()):raise ValueError('Operator snapshot changed')
    return c


class ReleasePlatform(Platform):
    def __init__(self,c):
        super().__init__(c)
        self.target={Path(s['env_file']):patch(self.original[Path(s['env_file'])],c['target_values']) for s in self.apis}

    def check(self,exclusive=False):
        c=self.c
        if private_read(Path(c['manifest_path']))!=base64.b64decode(c['manifest_b64']):raise ValueError('Manifest changed')
        if any(sha(Path(p).read_bytes())!=v for p,v in c['managers'].items()):raise ValueError('Manager changed')
        if sha(private_read(Path(c['worker_receipt_path'])))!=c['worker_receipt_sha256']:raise ValueError('Worker receipt changed')
        if any(sha((Path(c['worker_source'])/n).read_bytes())!=c['worker_files'][n] for n in PROBED_SOURCES):raise ValueError('Worker module changed')
        for s in self.services:
            p=Path(s['env_file']);raw=private_read(p);original=self.original[p]
            allowed=(original,gated(original),self.target[p],gated(self.target[p])) if s['role']=='api' else (original,)
            if raw not in allowed:raise ValueError('Foreign service configuration')
            if s['role'] in ('worker','engine','gateway') and self.identity(s)!=c['identities'][s['role']+'-'+s['name']]:
                raise ValueError('Protected inference or gateway identity changed')
        if any(sha(private_read(Path(p),16*1024**2))!=v for p,v in c['catalogue_files'].items()):raise ValueError('Published evidence bytes changed')
        return True

    def configure_bytes(self,api,raw):
        self.check();p=Path(api['env_file'])
        if raw not in (self.original[p],gated(self.original[p]),self.target[p],gated(self.target[p])):raise ValueError('Unknown API configuration')
        current=self.identity(api,optional=True)
        if private_read(p)==raw and current is not None:
            if self.manager.readiness(api):return
        if current is not None:self.invoke(api,'stop')
        atomic_bytes(p,raw);self.invoke(api,'start')

    def close_gates(self,rollback=False):
        self.check()
        for api in self.apis:
            # A dead/not-ready candidate cannot acknowledge its own gate. Only
            # in rollback may that replica start the known original API with
            # admission closed. The unchanged worker still owns accepted work.
            raw=private_read(Path(api['env_file']))
            if rollback and (self.identity(api,optional=True) is None or not self.manager.readiness(api)):
                raw=self.original[Path(api['env_file'])]
            self.configure_bytes(api,gated(raw))
        self.gates_closed()

    def install(self,target,closed=True):
        for api in self.apis:
            raw=(self.target if target else self.original)[Path(api['env_file'])]
            self.configure_bytes(api,gated(raw) if closed else raw)

    def verify(self,target):
        self.check();out=[]
        for port in (8082,8083,8080):
            root=f'http://127.0.0.1:{port}'
            r=self.http.get(root+'/readyz');r.raise_for_status()
            r=self.http.get(root+'/v1/evaluations',headers={'Authorization':'Bearer '+self.key});r.raise_for_status()
            states=[x['quality_state'] for x in r.json()['data']]
            if 'failed' not in states or 'passed' not in states:raise ValueError('Existing Batch evidence changed')
            r=self.http.get(root+'/v1/fleet',headers={'Authorization':'Bearer '+self.key});r.raise_for_status()
            row={'port':port,'ready':True,'batch_states':states,'fleet_readable':True}
            if target:
                r=self.http.get(root+'/v1/engine-evaluations/engine-squad-jsonspan-20261026',headers={'Authorization':'Bearer '+self.key});r.raise_for_status()
                if sha(r.content)!=self.c['engine_view_sha256'] or r.headers.get('X-BatchForge-Evidence-SHA256')!=sha(r.content):
                    raise ValueError('Primary corrected engine evidence differs from qualified bytes')
                r=self.http.get(root+'/app.js');r.raise_for_status()
                if sha(r.content)!=self.c['operator_files']['frontend/app.js']:raise ValueError('Primary frontend source differs')
                row['engine_evidence_sha256']=self.c['engine_view_sha256']
            out.append(row)
        return out


def restore_outcome(platform,*,commit):
    """Worker stays alive to deliver any accepted tasks; unsafe drain stays shut."""
    platform.close_gates(rollback=not commit);observations=platform.quiet(seconds=180)
    platform.install(commit,closed=True);platform.gates_closed()
    readback=platform.verify(commit)
    platform.install(commit,closed=False)
    readback=platform.verify(commit)
    return {'outcome':'published' if commit else 'rolled_back','drain_observations':observations,'readback':readback}


def rearm(c,base):
    old=read(Path(c['guard_run'])/'validation.json')
    if old.get('recovery_invocations')!=0:raise ValueError('Guard lifetime budget changed; cannot reset allowance')
    command=c['guard_command'];trial=command[command.index('--qualified-trial')+1];audit=command[command.index('--qualified-audit')+1]
    source=Path(c['guard_identity']['cwd'])
    if any(sha((source/n).read_bytes())!=v for n,v in c['guard_source_files'].items()):raise ValueError('Qualified finite guard source changed')
    boundary=base.with_name(base.name+'-restore-boundary');boundary.mkdir(mode=0o700,exist_ok=False)
    save(boundary/'launcher.json',{'identity':process_identity(os.getpid())})
    save(boundary/'campaign.json',{'phase':'sustained_running','scope':'Sole API restoration owner holds both locks'})
    run=ROOT.parent/('evidence-api-worker-latch-guard-'+base.name)
    remaining=math.ceil(c['guard_deadline']-time.time())
    if remaining<600:raise ValueError('Insufficient finite guard margin')
    args=[sys.executable,'-m','ops.worker_latch_guard','--run',str(run),'--qualified-trial',trial,'--qualified-audit',audit,
        '--active-campaign',str(boundary),'--seconds',str(remaining),'--apply']
    with (base/'guard-private.log').open('xb') as log:
        child=subprocess.Popen(args,cwd=source,env=c['guard_environment'],stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
    identity=process_identity(child.pid)
    save(run.with_name(run.name+'-launcher.json'),{'identity':identity,'command':args})
    until=time.monotonic()+35
    while True:
        if child.poll() is not None:raise RuntimeError('Rearmed finite guard exited')
        if (run/'validation.json').exists():
            r=read(run/'validation.json')
            if r['phase']=='armed' and r['recovery_invocations']==0:break
            if r['phase']=='failed':raise RuntimeError('Rearmed guard refused qualification')
        if time.monotonic()>until:raise RuntimeError('Finite guard startup not verified')
        time.sleep(.5)
    save(run/'launcher.json',{'identity':identity,'command':args,'source_commit':c['guard_source_commit']})
    save(boundary/'campaign.json',{'phase':'complete','passed':True})
    return {'run':str(run),'identity':identity,'recovery_invocations':0,'deadline_epoch':r['deadline_epoch']}


def restore(path,digest):
    c=load(path,digest);base=path.parent;locks=acquire(15);p=None
    lock=(base/'restore.lock').open('a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    if (base/'recovery-validation.json').exists():
        release(locks);lock.close();raise ValueError('Restoration is not replayed')
    result={'phase':'restoring','passed':False,'config_sha256':digest,'started_at':now(),'engine_actions':False,'worker_actions':False,'new_gpu_tasks':0}
    try:
        p=ReleasePlatform(c);p.check()
        signal_exact(c['guard_identity'])
        if p.identity(p.watch,optional=True) is not None:p.invoke(p.watch,'stop')
        marker=base/'commit.json';commit=marker.exists()
        if commit and read(marker)!={'config_sha256':digest,'publish_requested':True}:raise ValueError('Invalid publication marker')
        try:result.update(restore_outcome(p,commit=commit))
        except BaseException as exc:
            if not commit:raise
            result['publication_error_type']=type(exc).__name__
            result.update(restore_outcome(p,commit=False))
        if p.identity(p.watch,optional=True) is None:p.invoke(p.watch,'start')
        fresh=Pins();result['identities']=fresh.identities;result['state_sha256']=fresh.check(full=True)
        result['finite_guard']=rearm(c,base)
        result.update(passed=True,phase='complete')
    except BaseException as exc:result.update(phase='manual_recovery_required',error_type=type(exc).__name__)
    finally:
        result['finished_at']=now();save(base/'recovery-validation.json',result)
        if p:p.close()
        release(locks);lock.close()
    if not result['passed']:raise SystemExit(1)


def require_loss_qualification(path,c):
    owned(path);old=load(path/'restore-private.json',sha(private_read(path/'restore-private.json',1024**2)))
    r=read(path/'recovery-validation.json');proof=read(path/'dispatch-observation.json')
    if (old['operator_commit']!=c['operator_commit'] or old['operator_files']!=c['operator_files']
        or r.get('passed') is not True or r.get('outcome')!='rolled_back' or proof.get('intentional_controller_exit')!=42
        or proof.get('first_api_gate_verified') is not True or process_identity(old['controller']['pid'])==old['controller']):
        raise ValueError('Closed source-matched real API controller-loss qualification required')
    return sha(private_read(path/'recovery-validation.json'))


def execute(base,guard,qualification,apply,qualify_loss,loss_qualification):
    owned(base);base.mkdir(mode=0o700,exist_ok=False);locks=acquire();guardian=None;p=None
    report={'phase':'preflight','passed':False,'started_at':now(),'new_gpu_tasks':0,'worker_actions':False,'engine_actions':False,'new_cloud_resources':False}
    save(base/'launcher.json',{'identity':process_identity(os.getpid())})
    def stage(name):report['phase']=name;save(base/'campaign.json',report)
    try:
        c=prepare(base,guard,qualification);p=ReleasePlatform(c);p.check();p.quiet(seconds=20)
        save(base/'restore-private.json',c);digest=sha(private_read(base/'restore-private.json',1024**2));report['operator_commit']=c['operator_commit'];report['config_sha256']=digest
        stage('prepared')
        if not apply:report['passed']=True;return
        if not qualify_loss:report['loss_qualification_sha256']=require_loss_qualification(loss_qualification,c)
        guardian=RestorationGuard(c['controller'],600,[sys.executable,'-m','ops.release_engine_evidence',
            '--restore-config',str(base/'restore-private.json'),'--config-sha',digest],restore_timeout=600,log_path=base/'restore-private.log')
        save(base/'guard-launcher.json',{'identity':process_identity(guardian.child.pid),'config_sha256':digest,'kind':'Independent sole API restoration trigger'})
        guardian.begin();stage('independent_restoration_armed')
        signal_exact(c['guard_identity']);p.invoke(p.watch,'stop')
        first=p.apis[0];p.configure_bytes(first,gated(p.original[Path(first['env_file'])]))
        for path in ('/v1/batches','/v1/batches/missing/retry'):
            r=p.http.post(f'http://127.0.0.1:{first["port"]}'+path,json={},headers={'Authorization':'Bearer '+p.key})
            if r.status_code!=503 or r.json().get('error',{}).get('code')!='admission_paused':raise ValueError('First API gate not closed')
        stage('first_api_gate_verified')
        if qualify_loss:
            save(base/'dispatch-observation.json',{'intentional_controller_exit':42,'first_api_gate_verified':True,'at':now(),
                'protected_identities':{k:v for k,v in c['identities'].items() if k.startswith(('worker-','engine-','gateway-'))}})
            os._exit(42)
        p.close_gates();stage('both_gates_closed');report['drain_observations']=p.quiet(seconds=180)
        p.install(True,closed=True);p.gates_closed();report['precommit_readback']=p.verify(True);stage('target_verified_gated')
        save(base/'commit.json',{'config_sha256':digest,'publish_requested':True})
    except BaseException as exc:report.update(error_type=type(exc).__name__,failed_stage=report['phase'])
    finally:
        if guardian and guardian.active:
            release(locks)
            try:
                guardian.restore();r=read(base/'recovery-validation.json');report['restoration_passed']=r.get('passed')
                report['outcome']=r.get('outcome');report['passed']=r.get('passed') is True and r.get('outcome')=='published' and 'error_type' not in report
                report['recovery_report_sha256']=sha(private_read(base/'recovery-validation.json'))
            except BaseException as exc:report.update(passed=False,restore_error_type=type(exc).__name__)
            guardian.close_pipes()
        if p:p.close()
        release(locks);report['finished_at']=now();stage('complete' if report['passed'] else 'failed')
    if not report['passed']:raise SystemExit(1)


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run',type=Path);p.add_argument('--guard-run',type=Path)
    p.add_argument('--qualification',type=Path);p.add_argument('--apply',action='store_true');p.add_argument('--qualify-controller-loss',action='store_true')
    p.add_argument('--qualified-loss',type=Path);p.add_argument('--restore-config',type=Path);p.add_argument('--config-sha')
    a=p.parse_args();os.umask(0o077);signal.signal(signal.SIGHUP,signal.SIG_IGN)
    if a.restore_config:
        if not a.config_sha or not re.fullmatch('[0-9a-f]{64}',a.config_sha):p.error('Frozen private restore SHA required')
        return restore(a.restore_config,a.config_sha)
    if not a.run or not a.guard_run or not a.qualification:p.error('New run, current finite guard and closed read-only qualification required')
    if a.apply and not a.qualify_controller_loss and not a.qualified_loss:p.error('Applied release requires real controller-loss qualification')
    if a.qualify_controller_loss and not a.apply:p.error('Loss injection requires explicit --apply')
    execute(a.run,a.guard_run,a.qualification,a.apply,a.qualify_controller_loss,a.qualified_loss)


if __name__=='__main__':main()
