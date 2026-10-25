import json
import pytest
from ops.qualify_worker_latch import MODEL,RUNTIME,task_rows,inference_confirmed,strict_receipt,started
from pathlib import Path


def test_only_two_fixed_cohorts_reuse_public_fixture_without_mutating_it():
    template={'custom_id':'original','body':{'model':MODEL,'messages':[{'role':'user','content':'public passage'}],'max_tokens':32}}
    raw=task_rows(template,40,'fault');rows=[json.loads(x) for x in raw.splitlines()]
    assert len(rows)==40 and len({r['custom_id'] for r in rows})==40
    assert all(r['body']['min_tokens']==r['body']['max_tokens']==64 for r in rows)
    assert template['body']['max_tokens']==32 and template['custom_id']=='original'
    with pytest.raises(ValueError):task_rows(template,41,'fault')
    with pytest.raises(ValueError):task_rows({'body':{'model':'wrong'}},40,'fault')


def snapshot():
    return {'batchforge_worker_inflight':32,'batchforge_worker_reserved_context_tokens':8192,
            'batchforge_worker_adaptive_kv_paused':0}


def test_actual_dispatched_inference_needs_fresh_worker_engine_and_db_evidence():
    assert inference_confirmed(snapshot(),'vllm:num_requests_running{model_name="480"} 32\n',32)


@pytest.mark.parametrize('metrics,running,change',[
    ('vllm:num_requests_running 0',32,{}),('vllm:num_requests_running NaN',32,{}),
    ('vllm:num_requests_running -1',32,{}),('vllm:num_requests_running broken',32,{}),
    ('other_metric 32',32,{}),('vllm:num_requests_running 32',0,{}),
    ('vllm:num_requests_running 32',True,{}),
    ('vllm:num_requests_running 32',32,{'batchforge_worker_inflight':0}),
    ('vllm:num_requests_running 32',32,{'batchforge_worker_reserved_context_tokens':0}),
    ('vllm:num_requests_running 32',32,{'batchforge_worker_adaptive_kv_paused':1}),
])
def test_readiness_or_one_busy_component_alone_cannot_authorize_stop(metrics,running,change):
    assert not inference_confirmed({**snapshot(),**change},metrics,running)


def envelope():
    import hashlib
    from backend.receipts import DOMAIN,canonical,PROTOCOL
    r={'model':MODEL,'runtime_fingerprint':RUNTIME,'ledger_state':'consistent_ledger',
       'counts':{'total':40,'pending':0,'running':0,'completed':40,'failed':0,'cancelled':0},
       'checks':{'no_duplicate_accepted_attempts_observed':True},
       'accepted_usage':{'completion_tokens':2560},'history':{'observed_accepted_attempts':40}}
    return {'protocol':PROTOCOL,'receipt':r,'receipt_sha256':hashlib.sha256(DOMAIN+canonical(r)).hexdigest()}


def test_ledger_checksum_alone_is_insufficient_for_qualification():
    import hashlib
    from backend.receipts import DOMAIN,canonical
    strict_receipt(envelope(),40)
    for field,value in [('ledger_state','incomplete_history'),('checks',{'no_duplicate_accepted_attempts_observed':False}),
                         ('history',{'observed_accepted_attempts':41}),('runtime_fingerprint','different-runtime'),
                         ('accepted_usage',{'completion_tokens':2559})]:
        e=envelope();e['receipt'][field]=value
        e['receipt_sha256']=hashlib.sha256(DOMAIN+canonical(e['receipt'])).hexdigest()
        with pytest.raises(RuntimeError):strict_receipt(e,40)


def test_portless_watchdog_needs_exact_managed_identity_not_a_nonexistent_http_probe():
    class Manager:
        def readiness(self,s):raise AssertionError('Portless daemon has no HTTP readiness')
    root=Path('/owned/app');s={'role':'supervisor','name':'primary'}
    assert started(Manager(),root,s,identity=lambda r,s:{'pid':123,'start_ticks':'4','cwd':str(root)})
    assert not started(Manager(),root,s,identity=lambda r,s:{'pid':123,'start_ticks':'4','cwd':'/foreign'})
    def absent(r,s):raise RuntimeError('Managed daemon absent')
    with pytest.raises(RuntimeError):started(Manager(),root,s,identity=absent)


@pytest.mark.parametrize('ready',[None,False,True])
def test_http_service_still_requires_positive_readiness(ready):
    class Manager:
        def readiness(self,s):return ready
    assert started(Manager(),Path('/owned/app'),{'role':'api','port':8082}) is (ready is True)
