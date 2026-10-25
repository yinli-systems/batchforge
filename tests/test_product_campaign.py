import pytest
from ops.product_campaign import rollout_sequence


def test_admission_closes_and_drains_before_install_and_reopens_only_after_validation():
    events=[]
    def action(name):return lambda:events.append(name)
    rollout_sequence(**{n:action(n) for n in ['pause','drain','install','validate','reopen','restore']},
                     watchdog=lambda enabled:events.append(('watchdog',enabled)))
    assert events==[('watchdog',False),'pause','drain','install','validate','reopen','validate',('watchdog',True)]


@pytest.mark.parametrize('failed_phase',['pause','drain','install','validate','reopen'])
def test_rollout_failure_restores_api_configuration_and_watchdog_without_stopping_gpus(failed_phase):
    events=[]
    def action(name):
        def call():
            events.append(name)
            if name==failed_phase:raise RuntimeError('injected_failure')
        return call
    with pytest.raises(RuntimeError,match='injected_failure'):
        rollout_sequence(**{n:action(n) for n in ['pause','drain','install','validate','reopen','restore']},
                         watchdog=lambda enabled:events.append(('watchdog',enabled)))
    assert events[-2:]==['restore',('watchdog',True)]
    if failed_phase in ['pause','drain']:assert 'install' not in events


def test_uncertain_watchdog_stop_still_attempts_idempotent_restart():
    events=[]
    def watchdog(enabled):
        events.append(enabled)
        if not enabled:raise RuntimeError('uncertain_stop')
    with pytest.raises(RuntimeError):
        rollout_sequence(**{n:lambda:None for n in ['pause','drain','install','validate','reopen','restore']},watchdog=watchdog)
    assert events==[False,True]
