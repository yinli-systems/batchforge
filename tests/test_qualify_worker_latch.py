import json
import pytest
from ops.qualify_worker_latch import MODEL,task_rows,inference_confirmed


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
