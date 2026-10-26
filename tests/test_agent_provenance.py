"""Pure adversarial lineage checks; synthetic fixtures, not GPU evidence."""
import copy,hashlib
from datetime import datetime,timezone
import pytest
from agents.contracts import Policy,canonical,digest
from agents.store import event_document
from agents.workflows import Workflow,projection
from agents.provenance import resolve_graph,terminal_anchor,mapping,PROTOCOL
from agents.memory import excerpt
from test_agent_retrieval import registry
from agents.retrieval import Registry,quote_evidence
from agents.contracts import FinalDecision

TENANT='tenant-a';RID='agrun_'+'a'*32;DEADLINE=datetime(2026,10,26,7,tzinfo=timezone.utc)


def graph(snapshot=''):
    tool='retrieval.search' if snapshot else 'memory.get'
    return {'nodes':[
        {'id':'source','goal':'Read the project code','model':'test','allowed_tools':[tool],'require_citations':True,'planner_protocol':'schema-v1'},
        {'id':'relay','goal':'Forward the code and original citation','model':'test','depends_on':['source'],
         'allowed_tools':[],'max_tool_calls':0,'require_citations':True,'inherit_sources':True,'planner_protocol':'schema-v1'},
        {'id':'reader','goal':'Forward the dependency code and citation','model':'test','depends_on':['relay'],
         'allowed_tools':[],'max_tool_calls':0,'require_citations':True,'inherit_sources':True,'planner_protocol':'schema-v1'}],
        'outputs':['reader'],'allowed_tools':[tool],'memory_namespace':'facts','retrieval_snapshot':snapshot}


def anchor(row):
    payload={'state_sha256':digest(row['state']),'result_sha256':digest(row['result']),'status':'completed'}
    h=digest(event_document(TENANT,row['id'],row['revision'],'run.completed',payload,'0'*64))
    row.update(event_hash=h,terminal_event={'kind':'run.completed','revision':row['revision'],'payload':payload,'event_hash':h,'previous_hash':'0'*64})


def fixture(tmp=None,snapshot=False):
    reg=Registry(registry(tmp)) if snapshot else None
    sid=reg.visible(TENANT)[0]['id'] if reg else '';spec=Workflow.model_validate(graph(sid))
    parent={'id':RID,'tenant_id':TENANT,'model':'batchforge/workflow-v1','goal':canonical(spec.document()).decode(),
        'policy':spec.policy().document(),'status':'running','deadline_at':DEADLINE,
        'state':{'nodes':{n.id:{'run_id':'agrun_'+str(i+1)*32} for i,n in enumerate(spec.nodes)}}}
    rows={n.id:{'id':parent['state']['nodes'][n.id]['run_id'],'tenant_id':TENANT,'goal':n.goal,'model':n.model,
        'policy':n.policy(spec,parent=RID,node=n.id).document(),'status':'completed','state':{},'result':{},'deadline_at':DEADLINE,'revision':2} for n in spec.nodes}
    for r in rows.values():r['policy']['allowed_tools']=list(r['policy']['allowed_tools'])
    if reg:
        value=reg.require(TENANT,'facts',sid).search('Mars mission');tool='retrieval.search';args={'query':'Mars mission'};cite=value['matches'][0]['citation'];answer='2030'
    else:
        text='Project code: NOVA-9071.';value=excerpt({'namespace':'facts','key':'project','version':1,'sha256':hashlib.sha256(text.encode()).hexdigest(),'text':text})
        tool='memory.get';args={'key':'project'};cite=value['citation'];answer='NOVA-9071'
    rows['source']['state']={'observations':[{'tool':tool,'arguments_sha256':digest(args),'result_sha256':digest(value),'result':value}]}
    rows['source']['result']={'type':'final','answer':answer,'citations':[cite]};anchor(rows['source'])
    rows['relay']['state']={'workflow_inputs':[projection(rows['source'])]}
    doc=resolve_graph(parent,rows['relay'],rows,reg)
    rows['relay']['result']={'type':'final','answer':answer,'citations':[cite],'dependency_evidence':doc};anchor(rows['relay'])
    rows['reader']['state']={'workflow_inputs':[projection(rows['relay'])]}
    return parent,rows,reg


@pytest.mark.parametrize('retrieval',[False,True])
def test_transitive_sources_bind_each_committed_node_without_adding_query_permissions(tmp_path,retrieval):
    parent,rows,reg=fixture(tmp_path,retrieval);value=resolve_graph(parent,rows['reader'],rows,reg)
    assert value['protocol']==PROTOCOL and value['consumer_node']=='reader' and len(value['sources'])==1
    source=value['sources'][0];assert {x['node'] for x in source['nodes']}=={'source','relay'}
    assert source['edges']==[{'from':'relay','to':'reader'},{'from':'source','to':'relay'}]
    assert rows['reader']['policy']['allowed_tools']==[] and source['origins'][0]['node']=='source'
    assert 'semantic correctness' in value['scope']


@pytest.mark.parametrize('mutation',[
    lambda p,r:r['reader']['state']['workflow_inputs'][0].update(run_id='agrun_'+'f'*32),
    lambda p,r:r['reader']['state']['workflow_inputs'][0].update(result_sha256='f'*64),
    lambda p,r:r['source'].update(tenant_id='other'),
    lambda p,r:r['source']['state']['observations'][0]['result'].update(namespace='other'),
    lambda p,r:r['source']['state']['observations'][0].update(result_sha256='f'*64),
    lambda p,r:r['relay']['result']['dependency_evidence']['sources'][0]['nodes'][0].update(run_id='agrun_'+'f'*32),
    lambda p,r:r['source']['terminal_event']['payload'].update(state_sha256='f'*64),
    lambda p,r:r['relay']['policy'].update(inherit_sources=False),
])
def test_foreign_or_mutated_commits_cannot_turn_citation_strings_into_authority(mutation):
    parent,rows,reg=fixture();mutation(parent,rows)
    with pytest.raises(ValueError):resolve_graph(parent,rows['reader'],rows,reg)


def test_revocation_and_source_context_work_limits_remain_effective(tmp_path):
    parent,rows,reg=fixture(tmp_path,True)
    with pytest.raises(PermissionError):resolve_graph(parent,rows['reader'],rows,Registry())
    spec=Workflow.model_validate(graph());spec.max_dependency_bytes=256
    parent,rows,_=fixture();parent['goal']=canonical(spec.document()).decode()
    with pytest.raises(ValueError):resolve_graph(parent,rows['reader'],rows,None)


def test_ordinary_agent_cannot_claim_workflow_lineage_or_widen_old_defaults():
    from agents.app import RunRequest
    assert 'inherit_sources' not in Policy().document()
    assert all('inherit_sources' not in n for n in Workflow.model_validate({'nodes':[{'id':'one','goal':'Compute','model':'test'}],'outputs':['one']}).document()['nodes'])
    with pytest.raises(ValueError):RunRequest(goal='Claim another run',model='test',inherit_sources=True)
    with pytest.raises(ValueError):Policy(inherit_sources=True,require_citations=True)
    g=graph();g['nodes'][0]['inherit_sources']=True
    with pytest.raises(ValueError):Workflow.model_validate(g)


def test_quote_inheritance_revalidates_chunk_against_frozen_corpus(tmp_path):
    parent,rows,reg=fixture(tmp_path,True);policy=Policy(**{**rows['reader']['policy'],'allowed_tools':(),'tool_sequence':(),'answer_format':'retrieved_span'})
    doc=resolve_graph(parent,rows['reader'],rows,reg)
    from agents.provenance import retrieval_matches
    matches=retrieval_matches(doc);cite=next(iter(matches));final=FinalDecision(type='final',answer='2030',citations=[cite])
    assert quote_evidence(final,[],reg,TENANT,policy,inherited_matches=matches)['quotes']
    matches[cite]['text']+=' invented2099'
    with pytest.raises(ValueError):quote_evidence(final,[],reg,TENANT,policy,inherited_matches=matches)


def test_memory_get_and_search_share_only_their_committed_common_excerpt():
    from agents.provenance import merge
    parent,rows,_=fixture();from agents.provenance import own_sources
    policy=Policy(**{**rows['source']['policy'],'allowed_tools':('memory.get',)})
    values=own_sources(rows['source'],policy,None);cite=next(iter(values));short=copy.deepcopy(values)
    short[cite]['source'].update(text='Project code:',text_sha256=hashlib.sha256(b'Project code:').hexdigest(),text_truncated=True,full_document_bytes_replayed=False)
    joined=merge(values,short)[cite]['source']
    assert joined['text']=='Project code:' and joined['text_truncated'] and not joined['full_document_bytes_replayed']
    short[cite]['source']['text']='Different'
    with pytest.raises(ValueError):merge(values,short)
