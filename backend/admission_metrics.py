"""Bounded admission timings, including awaits/DB RTT; not isolated PG lock wait.

Fixed stage/outcome labels only. Stages overlap: do not sum them as wall time.
No caller IDs, model names, exception text, keys, SQL or payload labels.
"""
from contextlib import asynccontextmanager,contextmanager
import asyncio
import sys
import time
from prometheus_client import Histogram

STAGES=frozenset((
    'total','idempotency_precheck','input_read_validate','inline_validation',
    'pool_acquire','pool_release','transaction','transaction_begin',
    'transaction_commit','transaction_rollback','idempotency',
    'idempotency_tenant_lock','input_lookup','input_admission','tenant_lock',
    'retention_lock','queue_precheck','queue_insert','queue_lock','queue_update',
    'batch_insert','task_insert','idempotency_remember',
))
OUTCOMES=('ok','error','cancelled')
BUCKETS=(.001,.005,.01,.025,.05,.1,.25,.5,1,2,5,10,20,40,80,160)
ADMISSION_STAGE_SECONDS=Histogram(
    'batchforge_admission_stage_seconds',
    'Admission stage duration including awaits and DB RTT; overlapping stages, not isolated lock wait',
    ('stage','outcome'),buckets=BUCKETS,
)

@contextmanager
def admission_stage(stage):
    if stage not in STAGES:raise ValueError('Unknown admission metric stage')
    started=time.monotonic();outcome='ok'
    try:yield
    except asyncio.CancelledError:
        outcome='cancelled';raise
    except BaseException:
        outcome='error';raise
    finally:
        ADMISSION_STAGE_SECONDS.labels(stage,outcome).observe(max(0,time.monotonic()-started))

@asynccontextmanager
async def _observed_context(context,enter_stage,exit_stage,error_exit_stage=None):
    # Equivalent to async-with: preserve enter errors, body cancellation,
    # exception suppression and cleanup exceptions. Never retry/shield/commit.
    with admission_stage(enter_stage):value=await context.__aenter__()
    try:yield value
    except BaseException:
        with admission_stage(error_exit_stage or exit_stage):
            suppressed=await context.__aexit__(*sys.exc_info())
        if not suppressed:raise
    else:
        with admission_stage(exit_stage):await context.__aexit__(None,None,None)

@asynccontextmanager
async def observed_connection(pool):
    async with _observed_context(pool.connection(),'pool_acquire','pool_release') as conn:
        yield conn

@asynccontextmanager
async def observed_transaction(conn):
    with admission_stage('transaction'):
        async with _observed_context(conn.transaction(),'transaction_begin',
                                     'transaction_commit','transaction_rollback'):
            yield
