"""One Linux app-process pause with an independent pidfd-bound resume guard.

The guard is armed before STOP and survives the caller's session. EOF, a resume
command, TERM, an IPC error or the absolute pause deadline resumes the same
pidfd. It never signals a process group, a driver or a reused numeric PID.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import select
import signal
import subprocess
import sys
import time

from ops.manage import process_identity


def validate(identity, seconds):
    if (not isinstance(identity, dict) or set(identity) != {'pid', 'start_ticks', 'cwd'}
            or type(identity['pid']) is not int or identity['pid'] <= 1
            or not isinstance(identity['start_ticks'], str) or not identity['start_ticks'].isdigit()
            or not isinstance(identity['cwd'], str) or not identity['cwd'].startswith('/')
            or type(seconds) not in (int, float) or not math.isfinite(seconds) or not .1 <= seconds <= 60):
        raise ValueError('An exact owned identity and a pause of at most60 seconds are required')


def guardian(identity, seconds, *, input_fd=0, emit=None):
    validate(identity, seconds)
    if not hasattr(os, 'pidfd_open') or not hasattr(signal, 'pidfd_send_signal'):
        raise RuntimeError('Linux pidfd support is required')
    emit = emit or (lambda value: print(json.dumps(value), flush=True))
    if process_identity(identity['pid']) != identity:
        raise ValueError('Pause target identity changed before binding')
    handle = os.pidfd_open(identity['pid'])
    stopped = False
    try:
        if process_identity(identity['pid']) != identity:
            raise ValueError('Pause target identity changed after binding')
        emit({'phase': 'armed', 'identity': identity, 'maximum_pause_seconds': seconds})
        if not select.select([input_fd], [], [], 30)[0] or os.read(input_fd, 32) != b'STOP\n':
            return
        if process_identity(identity['pid']) != identity:
            raise ValueError('Pause target identity changed before STOP')
        stopped = True  # A failed acknowledgement must still execute CONT.
        began = time.monotonic()
        signal.pidfd_send_signal(handle, signal.SIGSTOP)
        emit({'phase': 'stopped', 'at_monotonic': began})
        reason = 'deadline'
        if select.select([input_fd], [], [], max(0, began + seconds - time.monotonic()))[0]:
            reason = 'command' if os.read(input_fd, 32) == b'CONT\n' else 'caller_eof_or_invalid_command'
    finally:
        if stopped:
            # Resume precedes logging: a closed caller pipe cannot strand STOP.
            try:
                signal.pidfd_send_signal(handle, signal.SIGCONT)
            except ProcessLookupError:
                pass
        os.close(handle)
    if stopped:
        emit({'phase': 'resumed', 'reason': reason, 'elapsed_seconds': time.monotonic() - began})


class BoundedPause:
    def __init__(self, identity, seconds):
        validate(identity, seconds)
        self.events = []
        self.child = subprocess.Popen([sys.executable, '-m', 'ops.bounded_pause', '--guardian',
            '--identity', json.dumps(identity), '--seconds', str(seconds)], stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, start_new_session=True, close_fds=True)
        try:
            if self._read(5).get('phase') != 'armed':
                raise RuntimeError('Resume guard did not arm')
        except BaseException:
            self.close()
            raise

    def _read(self, timeout):
        if not select.select([self.child.stdout], [], [], timeout)[0]:
            raise RuntimeError('Resume guard handshake timed out')
        raw = self.child.stdout.readline(4097)
        if not raw or len(raw) > 4096:
            raise RuntimeError('Resume guard handshake failed')
        result = json.loads(raw)
        self.events.append(result)
        return result

    def stop(self):
        if self.child.poll() is not None:
            raise RuntimeError('Resume guard exited before STOP')
        self.child.stdin.write(b'STOP\n');self.child.stdin.flush()
        if self._read(5).get('phase') != 'stopped':
            raise RuntimeError('STOP was not acknowledged')

    def resume(self):
        if self.child.poll() is None and not self.child.stdin.closed:
            try:self.child.stdin.write(b'CONT\n');self.child.stdin.flush()
            except BrokenPipeError:pass
        self.child.wait(timeout=65)
        # Capture a deadline resume even if it happened before this command.
        for raw in self.child.stdout:
            if len(raw) <= 4096:self.events.append(json.loads(raw))
        if self.child.returncode or not any(e.get('phase') == 'resumed' for e in self.events):
            raise RuntimeError('Resume guard has no successful resume receipt')

    def close(self):
        if not self.child.stdin.closed:self.child.stdin.close()  # EOF is an early resume.
        try:self.child.wait(timeout=65)
        finally:self.child.stdout.close()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--guardian', action='store_true', required=True)
    p.add_argument('--identity', required=True);p.add_argument('--seconds', type=float, required=True)
    args = p.parse_args()
    # TERM is catchable; KILL of the guard/kernel failure is outside this guarantee.
    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(SystemExit(1)))
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    guardian(json.loads(args.identity), args.seconds)


if __name__ == '__main__':main()
