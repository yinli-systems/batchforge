import math
import pytest
from ops import bounded_pause as module

IDENTITY = {'pid': 1234, 'start_ticks': '4567', 'cwd': '/owned/app'}


@pytest.mark.parametrize('identity,seconds', [
    ({**IDENTITY, 'pid': 1}, 1), ({**IDENTITY, 'pid': True}, 1),
    ({**IDENTITY, 'start_ticks': 5}, 1), ({**IDENTITY, 'cwd': 'relative'}, 1),
    ({**IDENTITY, 'extra': 1}, 1), (IDENTITY, True), (IDENTITY, math.nan),
    (IDENTITY, 0), (IDENTITY, 61),
])
def test_unknown_identity_or_unbounded_pause_refuses(identity, seconds):
    with pytest.raises(ValueError):module.validate(identity, seconds)


def setup(monkeypatch, *, replies, commands, emit_error=None, identities=None):
    signals=[];events=[];closed=[]
    states=iter(identities or [IDENTITY]*3)
    monkeypatch.setattr(module, 'process_identity', lambda pid: next(states))
    monkeypatch.setattr(module.os, 'pidfd_open', lambda pid: 90, raising=False)
    monkeypatch.setattr(module.os, 'close', closed.append)
    monkeypatch.setattr(module.signal, 'pidfd_send_signal', lambda fd, sig: signals.append((fd, sig)), raising=False)
    ready=iter(replies);data=iter(commands)
    monkeypatch.setattr(module.select, 'select', lambda *args: (next(ready), [], []))
    monkeypatch.setattr(module.os, 'read', lambda *args: next(data))
    def emit(event):
        events.append(event)
        if event['phase']==emit_error:raise BrokenPipeError('caller disappeared')
    return signals, events, closed, emit


@pytest.mark.parametrize('reply,command,reason', [
    ([], [], 'deadline'), ([0], [b''], 'caller_eof_or_invalid_command'),
    ([0], [b'CONT\n'], 'command'), ([0], [b'bad'], 'caller_eof_or_invalid_command'),
])
def test_deadline_eof_and_explicit_resume_signal_the_pinned_pidfd(monkeypatch, reply, command, reason):
    signals, events, closed, emit=setup(monkeypatch, replies=[[0],reply], commands=[b'STOP\n',*command])
    module.guardian(IDENTITY, 1, emit=emit)
    assert signals==[(90,module.signal.SIGSTOP),(90,module.signal.SIGCONT)]
    assert events[-1]['reason']==reason and closed==[90]


def test_lost_stop_acknowledgement_still_resumes_before_error(monkeypatch):
    signals, _, closed, emit=setup(monkeypatch, replies=[[0]], commands=[b'STOP\n'], emit_error='stopped')
    with pytest.raises(BrokenPipeError):module.guardian(IDENTITY, 1, emit=emit)
    assert signals==[(90,module.signal.SIGSTOP),(90,module.signal.SIGCONT)] and closed==[90]


def test_guard_failure_before_stop_never_signals(monkeypatch):
    signals, _, closed, emit=setup(monkeypatch, replies=[[0]], commands=[b'STOP\n'], identities=[IDENTITY,IDENTITY,None])
    with pytest.raises(ValueError):module.guardian(IDENTITY, 1, emit=emit)
    assert not signals and closed==[90]


@pytest.mark.parametrize('reply,command', [([],[]),([0],[b'']),([0],[b'CONT\n'])])
def test_unarmed_or_disconnected_caller_never_stops_target(monkeypatch, reply, command):
    signals, _, closed, emit=setup(monkeypatch, replies=[reply], commands=command)
    module.guardian(IDENTITY, 1, emit=emit)
    assert not signals and closed==[90]
