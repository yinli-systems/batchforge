import pytest
from ops.fleet_release import ReleaseFailed,release_sequence,require_closed_reuse_campaign


def complete():
    return {'configuration':{'tasks':3600},'passed':True,'accepted_tasks':3600,'completed_tasks':3600,
        'unfinished_tasks':0,'failed_tasks':0,'accepted_exactly_once':True,'accepted_usage_matches_task_usage':True,
        'fixed_output_lengths_exact':True,'input_reuse':{'enabled':True,'new_upload_requests':0}}


@pytest.mark.parametrize('change',[{'passed':False},{'completed_tasks':3599},{'failed_tasks':1},
    {'accepted_exactly_once':False},{'fixed_output_lengths_exact':False},
    {'input_reuse':{'enabled':True,'new_upload_requests':1}}])
def test_partial_or_unverified_gpu_results_cannot_open_a_release(change):
    report=complete();report.update(change)
    with pytest.raises(RuntimeError):require_closed_reuse_campaign({'phase':'complete'},report)


def test_successful_report_still_waits_for_the_controller_lock_boundary():
    with pytest.raises(RuntimeError):require_closed_reuse_campaign({'phase':'sustained_running'},complete())
    require_closed_reuse_campaign({'phase':'complete'},complete())


class Deployment:
    """Accepted work can arrive while the first API is reopened."""
    def __init__(self,fail=None,rollback_fail=None):
        self.gates=True;self.accepted=0;self.worker='original';self.calls=[]
        self.fail=fail;self.rollback_fail=rollback_fail;self.validations=0
    def record(self,name):
        self.calls.append(name)
        if self.fail==name or self.rollback_fail==name:raise RuntimeError('private diagnostic must not escape')
    def preflight(self):self.record('preflight')
    def watchdog(self,on):self.record('watchdog_on' if on else 'watchdog_off')
    def pause(self):self.gates=False;self.record('pause')
    def drain(self):
        assert not self.gates
        self.record('drain');self.accepted=0
    def install_worker(self):
        assert self.accepted==0 and not self.gates
        self.record('install_worker');self.worker='candidate'
    def validate_worker(self):self.record('validate_worker')
    def install_apis(self):self.record('install_apis')
    def validate_apis(self):
        self.validations+=1
        self.record('validate_open' if self.gates else 'validate_closed')
    def reopen(self):
        self.gates=True;self.accepted=7
        self.record('reopen')
    def pause_compatible_apis(self):self.gates=False;self.record('pause_compatible')
    def ensure_draining_worker(self):self.record('ensure_worker')
    def restore_worker(self):
        assert self.accepted==0 and not self.gates,'Never stop a live worker before re-draining new accepts'
        self.record('restore_worker');self.worker='original'
    def validate_original(self):self.record('validate_original_open' if self.gates else 'validate_original_closed')
    def reopen_original(self):self.record('reopen_original');self.gates=True
    def run(self,**overrides):
        record_event=overrides.pop('event',lambda stage:None)
        return release_sequence(**{name:getattr(self,name) for name in ('preflight','watchdog','pause','drain',
            'install_worker','validate_worker','install_apis','validate_apis','reopen','pause_compatible_apis',
            'ensure_draining_worker','restore_worker','validate_original','reopen_original')},event=record_event,**overrides)


def test_success_preserves_new_accepted_work_and_never_touches_the_engine():
    deployment=Deployment();report=deployment.run()
    assert report['passed'] and not report['engine_actions'] and not report['rollback_attempted']
    assert deployment.accepted==7 and deployment.worker=='candidate' and deployment.gates
    assert deployment.calls[-1]=='watchdog_on'


@pytest.mark.parametrize('stage',['install_worker','validate_worker','install_apis','validate_closed','reopen','validate_open'])
def test_each_mutating_failure_closes_compatible_apis_and_redrains_before_worker_restore(stage):
    deployment=Deployment(fail=stage)
    with pytest.raises(ReleaseFailed) as error:deployment.run()
    report=error.value.report
    assert report['rollback_passed'] and not report['passed'] and not report['engine_actions']
    assert deployment.worker=='original' and deployment.gates and deployment.accepted==0
    recover=deployment.calls.index('pause_compatible')
    assert deployment.calls[recover:recover+4]==['pause_compatible','ensure_worker','drain','restore_worker']
    assert 'private diagnostic' not in str(report) and 'private diagnostic' not in str(error.value)


def test_preflight_failure_cannot_signal_or_reconfigure_any_service():
    deployment=Deployment(fail='preflight')
    with pytest.raises(ReleaseFailed) as error:deployment.run()
    assert deployment.calls==['preflight']
    assert not error.value.report['rollback_attempted'] and not error.value.report['watchdog_recovery_attempted']


def test_watchdog_shutdown_failure_only_attempts_watchdog_recovery():
    deployment=Deployment(fail='watchdog_off')
    with pytest.raises(ReleaseFailed):deployment.run()
    assert deployment.calls==['preflight','watchdog_off','watchdog_on']


def test_failed_rollback_drain_keeps_gates_closed_and_never_stops_the_worker():
    deployment=Deployment(fail='validate_open')
    original_drain=deployment.drain
    def drain():
        if 'pause_compatible' in deployment.calls:raise RuntimeError('accepted tasks cannot finish yet')
        original_drain()
    deployment.drain=drain
    with pytest.raises(ReleaseFailed) as error:deployment.run()
    assert error.value.report['admission_left_closed'] and not error.value.report['rollback_passed']
    assert 'restore_worker' not in deployment.calls and deployment.accepted==7 and not deployment.gates


def test_failure_after_rollback_reopens_still_attempts_to_close_again():
    deployment=Deployment(fail='validate_open',rollback_fail='validate_original_open')
    with pytest.raises(ReleaseFailed) as error:deployment.run()
    assert error.value.report['admission_left_closed'] and deployment.calls.count('pause_compatible')==2


def test_failing_journal_cannot_prevent_redrain_restore_or_watchdog_recovery():
    deployment=Deployment()
    def broken_log(stage):
        if stage=='validating_open_primary' or stage.startswith('rollback') or stage=='watchdog_on':
            raise OSError('private disk diagnostic')
    with pytest.raises(ReleaseFailed) as error:deployment.run(event=broken_log)
    report=error.value.report
    assert report['rollback_passed'] and report['journal_errors']
    assert deployment.worker=='original' and deployment.gates and deployment.accepted==0
    assert deployment.calls[-1]=='watchdog_on' and 'private disk' not in str(report)


def test_terminal_journal_failure_keeps_watchdog_and_live_work_but_cannot_claim_success():
    deployment=Deployment()
    def broken_log(stage):
        if stage=='watchdog_on':raise OSError('private disk diagnostic')
    with pytest.raises(ReleaseFailed) as error:deployment.run(event=broken_log)
    assert not error.value.report['passed'] and error.value.report['failed_stage']=='watchdog_on'
    assert deployment.calls[-1]=='watchdog_on' and deployment.worker=='candidate'
    assert deployment.gates and deployment.accepted==7
