import copy,pytest
from ops.qualify_agent_console import require_closed


def closed():
    return {'phase':'complete','passed':True,'isolated_pg_stopped':True,
        'plan':{'fixture_set':'workflow-contract-heldout-v1'},'task_results':[{'passed':True} for _ in range(20)],
        'child_receipts':[{} for _ in range(44)],'artifact_readbacks':[{} for _ in range(88)]}


def test_console_qualification_requires_closed_real_source_and_cannot_replay_an_incomplete_cohort():
    require_closed(closed())
    for key,value in [('passed',False),('phase','storage_readback'),('isolated_pg_stopped',False),
        ('task_results',[{'passed':True}]*19),('child_receipts',[{}]*43),('artifact_readbacks',[{}]*87)]:
        data=closed();data[key]=value
        with pytest.raises(ValueError):require_closed(data)
    data=closed();data['task_results'][0]['passed']=False
    with pytest.raises(ValueError):require_closed(data)
