"""Frozen study selection/contracts; public reference values never enter goals."""
from datetime import datetime,timezone
import pytest
from agents.contracts import canonical,digest
from ops import source_workflow_fixtures as s
from ops.retrieval_squad import extract
from test_retrieval_squad import dataset
from ops import qualify_agent_dag as q


def test_source_plan_is_disjoint_bounded_and_references_are_grader_only(monkeypatch):
    corpus,small=extract(canonical(dataset()));cases=[{**small[0],'id':str(i)} for i in range(40)]
    monkeypatch.setattr(s,'official',lambda raw:(corpus,cases))
    monkeypatch.setattr(s,'old_selected',lambda raw,name:(corpus,cases[:8] if name=='retrieval-squad-v1' else cases[8:16]))
    values=s.fixtures(b'synthetic public fixture');assert len(values)==3
    assert {v['kind'] for v in values}=={'memory','retrieval','mixed'}
    assert all(v['question']['id'] not in {str(i) for i in range(16)} for v in values if v['question'])
    assert sum(sum(n['max_model_calls'] for n in c['spec']['nodes']) for c in values)==28
    for c in values:
        for n in c['spec']['nodes']:
            if n.get('inherit_sources'):assert not n['allowed_tools'] and n['max_tool_calls']==0
        if c['question']:assert 'references' not in str(c['spec']) and '2030' not in str(c['spec'])
    monkeypatch.setattr(q,'selected_fixtures',lambda name:values)
    p=q.plan(datetime(2026,10,26,5,tzinfo=timezone.utc),50,'source-lineage-v1');assert p['graphs']==3 and p['agent_nodes']==10 and p['max_child_tasks']==28
    with pytest.raises(ValueError):q.plan(datetime(2026,10,26,5,tzinfo=timezone.utc),27,'source-lineage-v1')
