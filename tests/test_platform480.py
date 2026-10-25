import pytest
import json
from types import SimpleNamespace

from ops.platform480 import experiment_plan, check_capacity, audit_metering, admission_budget


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


def test_kv_study_preserves_policy_and_alternates_only_admission():
    plan=experiment_plan(3, ('kv',))
    assert len(plan)==12 and {p['policy'] for p in plan}=={'fair'}
    assert [p['slots'] for p in plan[:4]]==[8,32,32,8]
    assert [p['adaptive'] for p in plan[:4]]==[False,True,True,False]
    assert admission_budget(172912,32,True)==164256
    assert admission_budget(172912,8,False)==147584
    with pytest.raises(RuntimeError):admission_budget(172912,64,True)
    with pytest.raises(RuntimeError):admission_budget(18000,32,True)
    for studies in [(), ('kv','kv'), ('unknown',)]:
        with pytest.raises(ValueError):experiment_plan(1, studies)


def test_scaled_cohort_keeps_each_cell_and_unique_custom_ids(tmp_path):
    from ops.platform480 import Canary, REPO
    root=tmp_path/'app';run=tmp_path/'run';run.mkdir()
    source=tmp_path/'gpu-takeover-20261025/trial480-v2/workload.jsonl'
    source.parent.mkdir(parents=True)
    originals=[{'custom_id':str(n),'body':{'model':REPO,'max_tokens':32+n,
                  'messages':[{'role':'user','content':'document '+str(n)}]}} for n in range(20)]
    source.write_text(''.join(json.dumps(r)+'\n' for r in originals))
    obj=SimpleNamespace(root=root,run=run,report={},workload_repeats=3)
    path=Canary.workload(obj)
    rows=[json.loads(line) for line in path.read_text().splitlines()]
    assert len(rows)==len({r['custom_id'] for r in rows})==60
    assert [r['body']['messages'] for r in rows]==[r['body']['messages'] for r in originals]*3
    assert all(r['body']['min_tokens']==r['body']['max_tokens'] for r in rows)
    assert 'min_tokens' not in json.loads(source.read_text().splitlines()[0])['body']


def test_evidence_export_refuses_remote_shell_and_path_injection():
    from ops.collect_platform480 import remote_script
    for path in ['/tmp/other','/root/batchforge-runtime/platform480-x;id',
                 '/root/batchforge-runtime/platform480-x/../api-a.env']:
        with pytest.raises(ValueError):remote_script(path)


def test_analysis_weights_phase_walls_and_uses_pooled_observations():
    from ops.analyze_platform480 import summarize
    phases=[]
    for i,treatment in enumerate('ABBA'):
        phases.append({'study':'kv','treatment':treatment,'repeat':0,'slots':8 if treatment=='A' else 32,
            'policy':'fair','adaptive':treatment=='B','elapsed_seconds':10 if treatment=='A' else 5,
            'audit':{'tasks':2,'completion_tokens':100,'task_observations':[
                {'completion_seconds':i+1,'queue_seconds':0,'accepted_attempt_seconds':i+1},
                {'completion_seconds':i+5,'queue_seconds':1,'accepted_attempt_seconds':i+4}]}})
    report={'phases':phases,'plan':phases,'model':'x','revision':'y','passed':True,'finished_at':'qualified'}
    summary=summarize(report)
    assert summary['abba_blocks'][0]['b_over_a_throughput']==2
    assert summary['groups'][0]['task_completion_seconds']['p50']==4.5
    assert summary['groups'][1]['output_tokens_per_second']==20
    assert summary['groups'][1]['allocation_wall_hours_per_million_output_tokens']==pytest.approx(1_000_000/20/3600)
    assert summary['campaign_passed'] is True
    assert not summarize({**report,'cleanup_error_type':'RuntimeError'})['campaign_passed']
    assert not summarize({**report,'finished_at':None})['campaign_passed']


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
