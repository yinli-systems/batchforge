from datetime import datetime,timedelta,timezone
import pytest
from ops.stall_watch import assess

NOW=datetime(2026,10,25,16,30,tzinfo=timezone.utc)


def sample():
    return {'at':NOW.isoformat(),'quota':{'queue':{'pending':127}},
        'engine':{'vllm:num_requests_running':0,'vllm:num_requests_waiting':0},
        'gpu_average_over_prior_window':[95,96,94,85]}


def worker():
    return {'ready':False,'batchforge_worker_adaptive_kv_paused':1,'batchforge_worker_inflight':0,
        'batchforge_worker_reserved_context_tokens':0,'batchforge_worker_adaptive_kv_waiting':0}


def test_live_pause_is_actionable_even_while_old_gpu_moving_averages_look_busy():
    result=assess(sample(),worker(),now=NOW)
    assert result['state']=='admission_latched' and result['action_required'] and result['quiescence_observed']


@pytest.mark.parametrize('field',['batchforge_worker_inflight','batchforge_worker_reserved_context_tokens','batchforge_worker_adaptive_kv_waiting'])
def test_live_invocations_or_reservations_prevent_a_quiescence_claim(field):
    w=worker();w[field]=1
    result=assess(sample(),w,now=NOW)
    assert result['action_required'] and not result['quiescence_observed']


@pytest.mark.parametrize('value',[1,None,float('nan')])
def test_nonzero_or_unknown_engine_work_is_not_quiescent(value):
    s=sample();s['engine']['vllm:num_requests_running']=value
    assert not assess(s,worker(),now=NOW)['quiescence_observed']


def test_stale_samples_cannot_authorize_any_recovery_observation():
    result=assess(sample(),worker(),now=NOW+timedelta(seconds=31))
    assert result['state']=='telemetry_stale' and not result['quiescence_observed']


def test_live_process_or_api_health_does_not_substitute_for_worker_readiness():
    w=worker();w['batchforge_worker_adaptive_kv_paused']=0
    assert assess(sample(),w,now=NOW)['state']=='worker_unready'
    del w['ready']
    assert assess(sample(),w,now=NOW)['state']=='insufficient_telemetry'


def test_current_healthy_worker_is_not_turned_into_an_endurance_success_claim():
    w=worker();w.update(ready=True,batchforge_worker_adaptive_kv_paused=0)
    result=assess(sample(),w,now=NOW)
    assert result['state']=='ready' and not result['action_required']
    assert 'throughput' in result['scope']
