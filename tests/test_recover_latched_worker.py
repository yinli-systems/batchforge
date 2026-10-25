from datetime import datetime,timezone,timedelta
import copy
import pytest
from ops.recover_latched_worker import MODEL,RecoveryFailed,charge_attempt,require_quiescence,recovery_sequence

NOW=datetime(2026,10,25,18,tzinfo=timezone.utc)


def observation():
    return {'captured_at':NOW.isoformat(),'worker_ready_status':503,'model_ids':[MODEL],'engine_version':'0.10.2',
        'engine_quiescent':True,'db_running':0,'db_pending':127,
        'worker_metrics':{'batchforge_worker_adaptive_kv_paused':1,'batchforge_worker_inflight':0,
            'batchforge_worker_reserved_context_tokens':0,'batchforge_worker_adaptive_kv_waiting':0}}


def test_fresh_quiescent_engine_does_not_require_discarding_pending_work():
    require_quiescence(observation(),now=NOW)


@pytest.mark.parametrize('key,value',[
    ('worker_ready_status',200),('engine_quiescent',False),('engine_quiescent',1),
    ('db_running',1),('db_running',False),('db_pending',0),('db_pending',50001),
    ('model_ids',[]),('model_ids',[MODEL,MODEL]),('model_ids',['another-model']),
    ('engine_version','new-unqualified-version'),
    ('captured_at',(NOW-timedelta(seconds=16)).isoformat()),
    ('captured_at',(NOW+timedelta(seconds=3)).isoformat()),('captured_at','broken')])
def test_healthy_busy_unknown_stale_or_different_engine_cannot_authorize_recovery(key,value):
    value_before=observation();value_before[key]=value
    with pytest.raises(ValueError):require_quiescence(value_before,now=NOW)


@pytest.mark.parametrize('key,value',[
    ('batchforge_worker_adaptive_kv_paused',0),('batchforge_worker_adaptive_kv_paused',True),
    ('batchforge_worker_inflight',1),('batchforge_worker_reserved_context_tokens',16),
    ('batchforge_worker_adaptive_kv_waiting',1),('batchforge_worker_inflight',None),
    ('batchforge_worker_inflight',float('nan'))])
def test_unknown_or_active_worker_reservations_fail_closed(key,value):
    value_before=observation();value_before['worker_metrics'][key]=value
    with pytest.raises(ValueError):require_quiescence(value_before,now=NOW)


def test_persistent_attempt_budget_counts_failures_and_only_expires_old_intents():
    value=charge_attempt([],now=10000)
    value=charge_attempt(value,now=10001)
    with pytest.raises(ValueError,match='exhausted'):charge_attempt(value,now=10002)
    assert charge_attempt(value,now=13600)==[10001,13600]


@pytest.mark.parametrize('previous',[[-1],[False],[float('nan')],[10001],['10000'],{}])
def test_corrupt_or_future_budget_cannot_reset_the_retry_limit(previous):
    with pytest.raises(ValueError):charge_attempt(previous,now=10000)


class Recovery:
    def __init__(self,fail=None):self.calls=[];self.fail=fail;self.validations=0;self.pending=set(range(127));self.finished=set()
    def action(self,name):
        self.calls.append(name)
        if name==self.fail:raise RuntimeError('private credential diagnostic must not escape')
    def validate(self):
        self.validations+=1;self.action('validate'+str(self.validations))
    def watchdog(self,on):self.action('watchdog_on' if on else 'watchdog_off')
    def charge(self):self.action('charge')
    def stop_worker(self):self.action('stop')
    def guard_stopped(self):self.action('source_guard')
    def start_worker(self):self.action('start')
    def verify(self):self.action('verify');self.finished.add(next(iter(self.pending)))
    def run(self,**overrides):
        functions={n:getattr(self,n) for n in ['validate','charge','watchdog','stop_worker','guard_stopped','start_worker','verify']}
        return recovery_sequence(**functions,event=overrides.get('event',lambda stage:None))


def test_recovery_has_a_fresh_post_shutdown_check_and_preserves_accepted_identities():
    r=Recovery();result=r.run()
    assert result['passed'] and not result['engine_actions'] and not result['api_actions']
    assert r.calls==['validate1','charge','watchdog_off','validate2','stop','source_guard','start','verify','watchdog_on']
    assert len(r.pending)==127 and len(r.finished)==1


@pytest.mark.parametrize('stage',['validate1','charge'])
def test_preflight_or_budget_failure_never_signals_a_service(stage):
    r=Recovery(stage)
    with pytest.raises(RecoveryFailed) as error:r.run()
    assert 'watchdog_off' not in r.calls and 'stop' not in r.calls
    assert not error.value.report['watchdog_recovery_attempted']


@pytest.mark.parametrize('stage',['watchdog_off','validate2','stop','source_guard','start','verify'])
def test_each_failure_attempts_safe_watchdog_restoration_without_blind_retry(stage):
    r=Recovery(stage)
    with pytest.raises(RecoveryFailed) as error:r.run()
    assert not error.value.report['passed'] and r.calls[-1]=='watchdog_on'
    assert r.calls.count('start')<=1 and 'private credential' not in str(error.value.report)
    if stage in ['watchdog_off','validate2','stop','source_guard']:assert 'start' not in r.calls
    assert len(r.pending)==127


def test_post_stop_source_change_does_not_restart_worker():
    r=Recovery('source_guard')
    with pytest.raises(RecoveryFailed):r.run()
    assert 'start' not in r.calls


def test_journal_failure_cannot_disable_watchdog_restoration_or_claim_success():
    r=Recovery()
    def journal(stage):
        if stage in ['stopping_paused_worker','watchdog_recovery_finished']:raise OSError('private disk detail')
    with pytest.raises(RecoveryFailed) as error:r.run(event=journal)
    assert not error.value.report['passed'] and r.calls[-1]=='watchdog_on' and 'stop' not in r.calls
    assert error.value.report['journal_error_type']=='OSError'


def test_final_watchdog_failure_keeps_recovery_report_failed():
    r=Recovery('watchdog_on')
    with pytest.raises(RecoveryFailed) as error:r.run()
    assert error.value.report['watchdog_error_type']=='RuntimeError' and not error.value.report['passed']
