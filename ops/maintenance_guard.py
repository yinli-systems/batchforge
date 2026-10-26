"""Independent pidfd-bound restoration trigger for a trusted maintenance caller.

This primitive does not know how to restore a platform. The operator constructs
the fixed restoration command and must qualify that handler separately. It is
never a model capability or a user-facing arbitrary-command endpoint.
"""
from __future__ import annotations
import argparse,json,math,os,select,signal,subprocess,sys,time
from pathlib import Path
from ops.manage import process_identity


def validate(identity,seconds,command,restore_timeout):
    if (not isinstance(identity,dict) or set(identity)!={'pid','start_ticks','cwd'} or type(identity['pid']) is not int
        or identity['pid']<=1 or not isinstance(identity['start_ticks'],str) or not identity['start_ticks'].isdigit()
        or not isinstance(identity['cwd'],str) or not identity['cwd'].startswith('/')
        or type(seconds) not in (int,float) or not math.isfinite(seconds) or not .1<=seconds<=7200
        or type(restore_timeout) not in (int,float) or not math.isfinite(restore_timeout) or not .1<=restore_timeout<=600
        or not isinstance(command,list) or not 1<=len(command)<=16
        or any(not isinstance(s,str) or not 1<=len(s.encode())<=1024 or '\x00' in s for s in command)
        or not command[0].startswith('/')):
        raise ValueError('Exact controller identity, bounded window and trusted executable required')


def guardian(identity,seconds,command,*,restore_timeout=600,log_path=None,input_fd=0,emit=None):
    validate(identity,seconds,command,restore_timeout)
    if not hasattr(os,'pidfd_open') or not hasattr(signal,'pidfd_send_signal'):raise RuntimeError('Linux pidfd required')
    emit=emit or (lambda v:print(json.dumps(v),flush=True))
    def output(v):
        try:emit(v)
        except (OSError,ValueError):pass  # broken reporting must not skip restoration
    if process_identity(identity['pid'])!=identity:raise ValueError('Controller identity changed before binding')
    handle=os.pidfd_open(identity['pid']);active=False;reason='unused';kill_parent=False;log=None
    try:
        if process_identity(identity['pid'])!=identity:raise ValueError('Controller identity changed after binding')
        output({'phase':'armed','controller':identity,'maximum_window_seconds':seconds})
        # A lost setup handshake must not kill an unchanged controller. Only
        # BEGIN authorizes the caller to mutate its already-backed-up services.
        if not select.select([input_fd,handle],[],[],30)[0]:return
        if select.select([handle],[],[],0)[0]:return
        if os.read(input_fd,32)!=b'BEGIN\n':return
        active=True;end=time.monotonic()+seconds
        output({'phase':'active','deadline_monotonic':end})
        ready=select.select([input_fd,handle],[],[],max(0,end-time.monotonic()))[0]
        if handle in ready:reason='controller_exit'
        elif input_fd in ready:
            raw=os.read(input_fd,32)
            reason='restore_command' if raw==b'RESTORE\n' else 'controller_pipe_eof_or_invalid_command'
            kill_parent=raw!=b'RESTORE\n'
        else:reason='window_deadline';kill_parent=True
    except BaseException:
        reason='guardian_interrupted';kill_parent=active
    finally:
        try:
            if active:
                # A deadline/EOF caller could still mutate configuration. Stop
                # that exact process before the sole restore handler starts.
                if kill_parent and not select.select([handle],[],[],0)[0]:
                    try:signal.pidfd_send_signal(handle,signal.SIGTERM)
                    except ProcessLookupError:pass
                    if not select.select([handle],[],[],5)[0]:
                        try:signal.pidfd_send_signal(handle,signal.SIGKILL)
                        except ProcessLookupError:pass
                        if not select.select([handle],[],[],3)[0]:raise RuntimeError('Controller could not be stopped')
                output({'phase':'restoring','reason':reason})
                log_error=None
                if log_path is not None:
                    try:
                        fd=os.open(log_path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
                        log=os.fdopen(fd,'wb')
                    except OSError as exc:log_error=type(exc).__name__
                try:
                    result=subprocess.run(command,stdin=subprocess.DEVNULL,stdout=log or subprocess.DEVNULL,
                        stderr=subprocess.STDOUT,check=False,timeout=restore_timeout,close_fds=True)
                    output({'phase':'restored' if result.returncode==0 else 'restore_failed','reason':reason,
                        'exit_code':result.returncode,'log_error_type':log_error})
                    if result.returncode:raise RuntimeError('Restoration handler failed')
                except subprocess.TimeoutExpired:
                    output({'phase':'restore_failed','reason':reason,'error_type':'RestorationTimeout','log_error_type':log_error})
                    raise RuntimeError('Restoration handler exceeded its bound') from None
            else:output({'phase':'unused','reason':reason})
        finally:
            if log is not None:log.close()
            os.close(handle)


class RestorationGuard:
    """Two-phase handshake; after begin, the handler is the sole restore owner."""
    def __init__(self,identity,seconds,command,*,restore_timeout=600,log_path=None):
        validate(identity,seconds,command,restore_timeout);self.events=[];self.restore_timeout=restore_timeout
        args=[sys.executable,'-m','ops.maintenance_guard','--identity',json.dumps(identity),'--seconds',str(seconds),
            '--restore-timeout',str(restore_timeout),'--command',json.dumps(command)]
        if log_path is not None:args+=['--log',str(log_path)]
        self.child=subprocess.Popen(args,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,
            start_new_session=True,close_fds=True)
        self.active=False
        try:
            if self.read(5).get('phase')!='armed':raise RuntimeError('Restoration guardian did not arm')
        except BaseException:
            self.child.stdin.close();self.child.wait(timeout=35);self.child.stdout.close();raise

    def read(self,seconds):
        if not select.select([self.child.stdout],[],[],seconds)[0]:raise RuntimeError('Restoration guardian observation timed out')
        raw=self.child.stdout.readline(4097)
        if not raw or len(raw)>4096:raise RuntimeError('Restoration guardian receipt unavailable')
        value=json.loads(raw);self.events.append(value);return value

    def begin(self):
        if self.child.poll() is not None:raise RuntimeError('Restoration guardian exited before BEGIN')
        self.child.stdin.write(b'BEGIN\n');self.child.stdin.flush();self.active=True
        if self.read(5).get('phase')!='active':raise RuntimeError('Restoration guardian did not enter window')

    def restore(self):
        if self.child.poll() is None and not self.child.stdin.closed:
            try:self.child.stdin.write(b'RESTORE\n');self.child.stdin.flush()
            except BrokenPipeError:pass
        end=time.monotonic()+self.restore_timeout+15
        while time.monotonic()<end:
            value=self.read(max(.01,end-time.monotonic()))
            if value.get('phase') in ('restored','restore_failed','unused'):
                self.child.wait(timeout=5)
                if value['phase']!='restored' or self.child.returncode:raise RuntimeError('Restoration did not verify')
                self.active=False;return value
        raise RuntimeError('Inspect the same restoration process; no duplicate handler started')

    def close_pipes(self):
        if not self.child.stdin.closed:self.child.stdin.close()
        if self.child.poll() is not None:self.child.stdout.close()


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--identity',required=True);p.add_argument('--seconds',type=float,required=True)
    p.add_argument('--restore-timeout',type=float,default=600);p.add_argument('--command',required=True);p.add_argument('--log',type=Path)
    args=p.parse_args();os.umask(0o077)
    signal.signal(signal.SIGHUP,signal.SIG_IGN)
    signal.signal(signal.SIGTERM,lambda *_:(_ for _ in ()).throw(SystemExit(1)))
    try:guardian(json.loads(args.identity),args.seconds,json.loads(args.command),restore_timeout=args.restore_timeout,log_path=args.log)
    except BaseException as exc:
        # Never print the command, environment, backup content or handler log.
        print(json.dumps({'phase':'guardian_failed','error_type':type(exc).__name__}),flush=True);raise SystemExit(1) from None


if __name__=='__main__':main()
