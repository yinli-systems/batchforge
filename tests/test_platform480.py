import pytest

from ops.platform480 import experiment_plan, check_capacity, audit_metering


def test_abba_capacity_and_policy_studies_change_only_the_intended_factor():
    plan=experiment_plan(3)
    assert len(plan)==24
    for repeat in range(3):
        capacity=[x for x in plan if x['study']=='capacity' and x['repeat']==repeat]
        policy=[x for x in plan if x['study']=='policy' and x['repeat']==repeat]
        assert [x['treatment'] for x in capacity]==list('ABBA')
        assert [x['slots'] for x in capacity]==[4,8,8,4]
        assert {x['policy'] for x in capacity}=={'fair'}
        assert {x['slots'] for x in policy}=={8}
        assert [x['policy'] for x in policy]==['fair','cost_aware','cost_aware','fair']


def test_static_slots_reserve_block_rounded_full_context_against_actual_cache():
    assert check_capacity(172912,8)==147584
    with pytest.raises(RuntimeError,match='fresh_physical_cache'):
        check_capacity(172912,16)


def test_duplicate_accepted_result_or_drifted_metering_cannot_pass():
    tasks=[{'id':'t','status':'completed','prompt_tokens':128,'completion_tokens':32}]
    attempts=[{'task_id':'t','status':'completed','prompt_tokens':128,'completion_tokens':32},
              {'task_id':'t','status':'expired','prompt_tokens':0,'completion_tokens':0}]
    batches=[{'prompt_tokens':128,'completion_tokens':32}]
    assert audit_metering(tasks,attempts,batches)['accepted_attempts']==1
    with pytest.raises(RuntimeError,match='exactly_one'):
        audit_metering(tasks,attempts+[attempts[0]],batches)
    with pytest.raises(RuntimeError,match='conservation'):
        audit_metering(tasks,attempts,[{'prompt_tokens':128,'completion_tokens':64}])


@pytest.mark.asyncio
async def test_invalid_archive_qualification_flag_is_rejected_before_database_io():
    from backend.result_archival import ResultArchiver
    archiver=object.__new__(ResultArchiver)
    with pytest.raises(ValueError,match='boolean'):
        await archiver.archive_batch('tenant','batch',purge_payloads='false')
