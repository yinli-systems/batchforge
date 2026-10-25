"""Classify live execution health; process liveness and moving averages cannot.

This module never changes services. Quiescence here is an observation, not
permission to reset a limiter: controlled recovery additionally needs owned PID,
source/environment, database and exclusive-engine checks.
"""
from datetime import datetime
import math


def assess(sample,worker,*,now):
    try:age=(now-datetime.fromisoformat(sample['at'])).total_seconds()
    except (ValueError,TypeError,KeyError):age=None
    def scalar(value):return type(value) in (int,float) and math.isfinite(value)
    def mapping(value):return value if isinstance(value,dict) else {}
    if age is None or not -5<=age<=30:
        return {'state':'telemetry_stale','action_required':True,'quiescence_observed':False}
    pending=mapping(mapping(sample.get('quota')).get('queue')).get('pending')
    worker=mapping(worker)
    paused=worker.get('batchforge_worker_adaptive_kv_paused')
    engine=mapping(sample.get('engine'))
    quiet=all(scalar(engine.get(k)) and engine[k]==0 for k in ('vllm:num_requests_running','vllm:num_requests_waiting'))
    empty=all(scalar(worker.get(k)) and worker[k]==0 for k in
        ('batchforge_worker_inflight','batchforge_worker_reserved_context_tokens','batchforge_worker_adaptive_kv_waiting'))
    if scalar(pending) and pending>0 and scalar(paused) and paused==1:
        return {'state':'admission_latched','action_required':True,'quiescence_observed':quiet and empty,
            'action':'Retain accepted work; inspect current worker readiness and perform bounded owned recovery only after fresh independent quiescence checks'}
    if worker.get('ready') is False:
        return {'state':'worker_unready','action_required':True,'quiescence_observed':False}
    if not scalar(pending) or worker.get('ready') is not True or not scalar(paused):
        return {'state':'insufficient_telemetry','action_required':True,'quiescence_observed':False}
    return {'state':'ready','action_required':False,'quiescence_observed':quiet and empty,
        'scope':'Current readiness, not proof of throughput, long-term stability or queue progress'}
