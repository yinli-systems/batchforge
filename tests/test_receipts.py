import copy
from contextlib import asynccontextmanager
import pytest

from backend.receipts import format_receipt,verify
from backend.store import Store,APIError
from backend.config import Settings


def snapshot():
    return {'batch':{'id':'batch-one','model':'model','status':'completed','created_at':'2026-10-25T00:00:00+00:00',
        'total_count':2,'pending_count':0,'running_count':0,'completed_count':2,'failed_count':0,'cancelled_count':0,
        'prompt_tokens':128,'completion_tokens':32,'metadata':{'secret':'must-never-export'},
        'runtime_fingerprint':'sha256:model','runtime_compatibility_scope':'first_dispatch'},
        'tasks':{'total':2,'pending':0,'running':0,'completed':2,'failed':0,'cancelled':0,'prompt_tokens':128,'completion_tokens':32,'reported_attempts':3},
        'attempt_record_checks':{'tasks_missing_records':0,'tasks_with_excess_records':0},
        'attempts':{'total':3,'accepted':2,'accepted_tasks':2,'row_usage_mismatches':0,'prompt_tokens':128,'completion_tokens':32},
        'artifacts':[],'runtimes':[{'runtime_fingerprint':'sha256:model','accepted_attempts':2}],'runtime_groups':1}


def test_receipt_is_stable_across_capture_times_and_tampering_fails():
    row=snapshot();a=format_receipt(row);b=format_receipt(row)
    assert a['receipt_sha256']==b['receipt_sha256'] and verify(a)
    assert a['receipt']['ledger_state']=='consistent_ledger'
    assert 'must-never-export' not in str(a)
    a['receipt']['accepted_usage']['completion_tokens']+=1
    assert not verify(a) and not verify({})


@pytest.mark.parametrize('change',[
    lambda r:r['attempts'].update(accepted=3),
    lambda r:r['attempts'].update(row_usage_mismatches=2),
    lambda r:r['tasks'].update(prompt_tokens=129),
    lambda r:r['tasks'].update(total=1),
])
def test_duplicate_acceptance_swapped_usage_or_counter_drift_never_certifies(change):
    row=snapshot();change(row)
    assert format_receipt(row)['receipt']['ledger_state']=='inconsistent'


def test_legitimate_history_retirement_keeps_usage_but_cannot_certify_missing_attempts():
    row=snapshot()
    row['batch'].update(history_retired_tasks=1,history_retired_completed=1,history_retired_prompt_tokens=64,
                        history_retired_completion_tokens=16)
    row['tasks'].update(total=1,completed=1,prompt_tokens=64,completion_tokens=16)
    row['attempts'].update(total=1,accepted=1,accepted_tasks=1,prompt_tokens=64,completion_tokens=16)
    receipt=format_receipt(row)['receipt']
    assert receipt['ledger_state']=='incomplete_history'
    assert receipt['checks']['batch_and_task_usage_match']
    assert not receipt['history']['full_attempt_history_available']
    row=snapshot();row['attempts'].update(total=0,accepted=0,accepted_tasks=0,prompt_tokens=0,completion_tokens=0)
    assert format_receipt(row)['receipt']['ledger_state']=='incomplete_history'


class Connection:
    def __init__(self,rows):self.rows=rows;self.calls=[]
    async def execute(self,query,params):
        self.calls.append((query,params));self.result=self.rows.get(params[:2]);return self
    async def fetchone(self):return copy.deepcopy(self.result)


class Pool:
    def __init__(self,connection):self.conn=connection
    @asynccontextmanager
    async def connection(self):yield self.conn


@pytest.mark.asyncio
async def test_owner_context_is_bound_on_receipt_and_task_pages_and_lease_tokens_are_not_selected():
    connection=Connection({('batch-one','owner'):snapshot()})
    store=Store(Pool(connection),Settings(),schema_version=11)
    assert verify(await store.batch_receipt('owner','batch-one'))
    with pytest.raises(APIError) as denied:await store.batch_receipt('other','batch-one')
    assert denied.value.status==404
    assert connection.calls[-1][1]==('batch-one','other')
    assert 'attempt_token' not in connection.calls[0][0] and 'SELECT * FROM attempts' not in connection.calls[0][0]
    connection.rows={('batch-one','owner'):{'data':[{'id':'task-one'},{'id':'task-two'}],
        'retired_tasks':0,'cursor_found':False}}
    page=await store.task_page('owner','batch-one',1)
    assert page['has_more'] and page['last_id']=='task-one' and len(page['data'])==1
    with pytest.raises(APIError) as cursor:await store.task_page('owner','batch-one',1,'other-batch-task')
    assert cursor.value.status==404
    with pytest.raises(APIError):await store.task_page('owner','batch-one',201)


def test_missing_failed_retry_records_cannot_certify_full_history_despite_conserved_acceptance():
    row=snapshot()
    row['tasks']['reported_attempts']=4
    row['attempt_record_checks']['tasks_missing_records']=1
    receipt=format_receipt(row)['receipt']
    assert receipt['checks']['live_completed_tasks_have_accepted_attempts']
    assert receipt['checks']['live_task_and_accepted_attempt_usage_match']
    assert not receipt['checks']['live_task_attempt_counts_match']
    assert receipt['ledger_state']=='incomplete_history'
    assert not receipt['history']['full_attempt_history_available']


def test_per_task_attempt_mismatch_cannot_hide_behind_equal_aggregate_totals():
    row=snapshot()
    # Two records moved between tasks keep total attempts and accepted usage
    # unchanged, but are not a consistent execution ledger.
    row['attempt_record_checks'].update(tasks_missing_records=1,tasks_with_excess_records=1)
    assert format_receipt(row)['receipt']['ledger_state']=='inconsistent'
    del row['attempt_record_checks']
    assert not format_receipt(row)['receipt']['history']['full_attempt_history_available']
