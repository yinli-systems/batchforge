from datetime import datetime,timezone,timedelta
import copy,hashlib,json
from pathlib import Path
import pytest
from ops import worker_latch_guard as guard
from ops.recovery_state import state_document,fingerprint,require_expected

NOW=datetime(2026,10,25,19,tzinfo=timezone.utc)


def observation(at=NOW):
    return {'captured_at':at.isoformat(),'worker_ready_status':503,'model_ids':[guard.MODEL],
        'engine_version':'0.10.2','engine_quiescent':True,'db_running':0,'db_pending':40,
        'worker_metrics':dict(zip(guard.METRICS,[1,0,0,0]))}


def test_two_independent_fresh_observations_are_required():
    w=guard.EvidenceWindow();a=observation();b=observation(NOW+timedelta(seconds=5))
    assert not w.observe(a,NOW)
    assert not w.observe(a,NOW)  # Same snapshot is not an independent second observation.
    assert w.observe(b,NOW+timedelta(seconds=5)) and w.evidence==[a,b]


@pytest.mark.parametrize('change',[
    {'worker_ready_status':200},{'engine_quiescent':False},{'db_running':1},{'db_pending':0},
    {'engine_version':'unknown'},{'model_ids':['different']},
    {'captured_at':(NOW-timedelta(seconds=20)).isoformat()},
])
def test_healthy_busy_stale_unknown_samples_break_the_candidate_window(change):
    w=guard.EvidenceWindow();assert not w.observe(observation(),NOW)
    assert not w.observe({**observation(),**change},NOW)
    assert not w.observe(observation(NOW+timedelta(seconds=5)),NOW+timedelta(seconds=5))


@pytest.mark.parametrize('value',[1,None,float('nan')])
def test_unknown_or_active_reservations_never_authorize_a_recovery(value):
    o=observation();o['worker_metrics'][guard.METRICS[2]]=value
    assert not guard.candidate(o,NOW)


@pytest.mark.parametrize('kwargs',[
    {'apply':False},{'apply':1},{'attempts':2},{'attempts':True},{'now':1401},
    {'now':float('nan')},{'end':float('inf')},{'end':True},
])
def test_no_mutation_without_apply_lifetime_budget_and_ten_minute_margin(kwargs):
    assert not guard.action_allowed(**{'now':1000,'end':2000,'attempts':0,'apply':True,**kwargs})


def test_whole_guard_and_persistent_helper_budgets_are_both_needed():
    assert guard.action_allowed(now=1000,end=2000,attempts=1,apply=True)
    assert not guard.action_allowed(now=1000,end=2000,attempts=2,apply=True)


def state():
    return state_document(b'manifest',{Path('/owned/worker.env'):b'TOKEN=secret-not-exported'},b'receipt',
        {Path('/owned/manage.py'):'abc'},{'worker':{'pid':1,'start_ticks':'2','cwd':'/owned'}})


def test_observer_handoff_hashes_values_and_is_canonical_without_credentials():
    s=state();assert 'secret-not-exported' not in json.dumps(s)
    assert require_expected(s,None)==fingerprint(s)
    assert require_expected(s,fingerprint(s))==fingerprint(s)
    assert fingerprint(dict(reversed(list(s.items()))))==fingerprint(s)


@pytest.mark.parametrize('key',list(state()))
def test_locked_helper_rejects_every_observer_snapshot_drift(key):
    old=state();changed=copy.deepcopy(old);changed[key]='different'
    with pytest.raises(ValueError):require_expected(changed,fingerprint(old))


def pins(tmp_path,monkeypatch):
    p=guard.Pins.__new__(guard.Pins);p.root=tmp_path/'app';p.root.mkdir()
    p.manifest=p.root/'manifest';p.original_manifest=b'manifest'
    def put(path,raw):path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(raw);path.chmod(0o600)
    put(p.manifest,p.original_manifest)
    p.source=tmp_path/'evidence-api-test/source';p.receipt_path=p.source.parent/'source-receipt.json'
    p.receipt_raw=b'receipt';put(p.receipt_path,p.receipt_raw)
    p.receipt={'files':{n:hashlib.sha256(b'fixed').hexdigest() for n in guard.PROBED_SOURCES}}
    for n in guard.PROBED_SOURCES:put(p.source/n,b'fixed')
    config=p.root/'worker.env';put(config,b'key=value');p.configs={config:b'key=value'}
    manager=p.root/'manage.py';put(manager,b'manager');p.managers={manager:hashlib.sha256(b'manager').hexdigest()}
    p.worker={'role':'worker','name':'qwen480'};p.watch={'role':'supervisor','name':'primary'}
    p.all=[p.worker,p.watch,{'role':'engine','name':'qwen480bench'},{'role':'api','name':'a'}]
    p.identities={p.key(s):{'pid':100+i,'start_ticks':'1','cwd':str(p.root)} for i,s in enumerate(p.all)}
    current=copy.deepcopy(p.identities);p.environment={'key':'value'}
    monkeypatch.setattr(guard,'managed_identity',lambda root,s:copy.deepcopy(current[p.key(s)]))
    original=Path.read_bytes
    monkeypatch.setattr(Path,'read_bytes',lambda path:b'key=value\0' if str(path).startswith('/proc/') else original(path))
    return p,current


def test_guard_pins_bind_actual_environment_and_every_service_identity(tmp_path,monkeypatch):
    p,current=pins(tmp_path,monkeypatch);before=p.check(full=True)
    current[p.key(p.worker)]['pid']+=1
    with pytest.raises(ValueError):p.check()
    p.check(changed_worker=current[p.key(p.worker)])
    assert p.check()!=before


def test_only_success_reported_worker_and_watchdog_may_change_epoch(tmp_path,monkeypatch):
    p,current=pins(tmp_path,monkeypatch);current[p.key(p.worker)]['pid']+=1;current[p.key(p.watch)]['pid']+=1
    current['engine-qwen480bench']['pid']+=1
    with pytest.raises(ValueError):p.check(changed_worker=current[p.key(p.worker)])


@pytest.mark.parametrize('kind',['manifest','config','receipt','source','manager'])
def test_live_guard_rejects_source_and_file_configuration_drift(tmp_path,monkeypatch,kind):
    p,_=pins(tmp_path,monkeypatch)
    path={'manifest':p.manifest,'config':next(iter(p.configs)),'receipt':p.receipt_path,
          'source':p.source/guard.PROBED_SOURCES[0],'manager':next(iter(p.managers))}[kind]
    path.write_bytes(b'changed')
    with pytest.raises(ValueError):p.check(full=True)
