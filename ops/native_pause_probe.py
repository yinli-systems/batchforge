"""Native pidfd guard qualification against disposable CPU children only."""
from __future__ import annotations
import json
from pathlib import Path
import subprocess
import sys
import time
from ops.bounded_pause import BoundedPause
from ops.manage import process_identity


def stopped(pid):
    raw=Path(f'/proc/{pid}/stat').read_text()
    return raw[raw.rfind(')')+2:].split()[0] in ('T','t')


def wait(check,seconds):
    end=time.monotonic()+seconds
    while time.monotonic()<end:
        if check():return
        time.sleep(.01)
    raise RuntimeError('Disposable native pause probe timed out')


def run():
    target=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)'],
        stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True)
    results=[]
    try:
        wait(lambda:process_identity(target.pid) is not None,3)
        identity=process_identity(target.pid)
        guard=BoundedPause(identity,.5)
        try:
            guard.stop();wait(lambda:stopped(target.pid),1)
            wait(lambda:not stopped(target.pid),2)
            guard.resume()
            resume=next(x for x in guard.events if x['phase']=='resumed')
            if resume['reason']!='deadline' or not .45<=resume['elapsed_seconds']<2:
                raise RuntimeError('Native absolute resume bound failed')
            results.append({'case':'deadline','passed':True,'resume':resume})
        finally:guard.close()
        # This caller actually exits without resume/close/finally; the guard is
        # independent and observes kernel pipe EOF, including on interpreter death.
        script=('from ops.bounded_pause import BoundedPause;import json,os,time;'
                f'g=BoundedPause(json.loads({json.dumps(identity)!r}),5);'
                'g.stop();time.sleep(.1);os._exit(0)')
        caller=subprocess.Popen([sys.executable,'-c',script],stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True)
        wait(lambda:stopped(target.pid),3);began=time.monotonic()
        if caller.wait(timeout=3)!=0:raise RuntimeError('Disposable caller failed before exit')
        wait(lambda:not stopped(target.pid),2)
        elapsed=time.monotonic()-began
        if elapsed>=2:raise RuntimeError('Caller death did not trigger early resume')
        results.append({'case':'caller_os_exit_without_cleanup','passed':True,'resume_wait_seconds':elapsed})
        return {'passed':True,'scope':'Only disposable CPU children; no service or GPU actions',
                'cases':results,'target_identity_unchanged':process_identity(target.pid)==identity}
    finally:
        if target.poll() is None:target.terminate()
        target.wait(timeout=3)


if __name__=='__main__':print(json.dumps(run()),flush=True)
