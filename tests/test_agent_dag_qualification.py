from datetime import datetime,timezone
import copy,hashlib
import pytest
from agents.workflows import Workflow
from agents.contracts import digest
from ops.qualify_agent_dag import fixtures,plan,grade


def test_dag_plan_uses_original_finite_task_quota_and_disjoint_roles():
    cases=fixtures();result=plan(datetime(2026,10,26,1,tzinfo=timezone.utc),24)
    assert result['max_child_tasks']==24 and result['agent_nodes']==6
    assert all(Workflow.model_validate(c['spec']).max_parallel==2 for c in cases)
    assert len({v for c in cases for v in c['expected'].values()})==6
    assert result['fixture_sha256']==digest(fixtures())
    assert not result['primary_changes'] and not result['new_cloud_resources']


@pytest.mark.parametrize('now,quota',[(datetime(2026,10,26,8,20,tzinfo=timezone.utc),24),
    (datetime(2026,10,26,1,tzinfo=timezone.utc),23)])
def test_no_late_or_over_quota_dag_canary(now,quota):
    with pytest.raises(ValueError):plan(now,quota)


def test_dag_grader_requires_actual_tool_values_and_memory_citation():
    c=fixtures()[0];citation=f"memory:dag-canary:{c['key']}:v1:{hashlib.sha256(c['memory'].encode()).hexdigest()}"
    nodes={n['id']:{'status':'completed','result':{'answer':c['expected'][n['id']],'citations':[citation] if n['id']=='right' else []},
        'state':{'observations':[{'tool':t,'result':{'value':int(c['expected'][n['id']])} if t=='math.calculate' else {}} for t in n['tool_sequence']]}} for n in c['spec']['nodes']}
    assert all(grade(c,nodes).values())
    nodes['left']['state']['observations'][0]['result']['value']=0
    assert not grade(c,nodes)['tools_committed_with_expected_values']
    nodes['right']['result']['citations']=[]
    assert not grade(c,nodes)['memory_citation_verified']
