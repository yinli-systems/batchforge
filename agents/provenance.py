"""Server-verified, bounded workflow source lineage, never model-issued grants.

The database's terminal immutability and terminal event anchor bind a source
Agent's state/result. Frozen corpus replay or committed memory excerpts supply
citation bytes. Provenance establishes availability/commit consistency, not
semantic correctness, causal attention, an issuer signature or GPU attestation.
"""
from __future__ import annotations
import copy,hashlib,re
from .contracts import Policy,FinalDecision,canonical,digest
from .store import AgentError,event_document,one

PROTOCOL='batchforge.workflow-sources.v1'
SCOPE='Committed dependency sources and declared lineage; not semantic correctness, proof of model attention or hardware attestation'
SHA=re.compile(r'^[0-9a-f]{64}$')


def empty():return {}


def merge(left,right):
    out=copy.deepcopy(left)
    for citation,value in right.items():
        if citation not in out:out[citation]=copy.deepcopy(value);continue
        old=out[citation]
        if old['source']!=value['source']:
            a,b=old['source'],value['source'];vary={'text','text_sha256','text_truncated','full_document_bytes_replayed'}
            if (a.get('kind')!='memory' or b.get('kind')!='memory'
                or {k:v for k,v in a.items() if k not in vary}!={k:v for k,v in b.items() if k not in vary}
                or not (a['text'].startswith(b['text']) or b['text'].startswith(a['text']))):raise ValueError('Ambiguous source identity')
            # The common excerpt was present in every origin. A longer get()
            # does not silently widen a shorter search() commitment.
            common=min((a['text'],b['text']),key=lambda t:len(t.encode()))
            old['source']={**a,'text':common,'text_sha256':hashlib.sha256(common.encode()).hexdigest(),
                'text_truncated':True,'full_document_bytes_replayed':False}
        for field in ('origins','nodes','edges'):
            values={canonical(v):v for v in [*old[field],*value[field]]};old[field]=[values[k] for k in sorted(values)]
    return out


def envelope(workflow,node,sources,selected=None):
    selected=set(sources) if selected is None else set(selected)&sources.keys()
    values=[]
    for c in sorted(selected):
        value=copy.deepcopy({'citation':c,**sources[c]})
        for field in ('origins','nodes','edges'):
            unique={canonical(v):v for v in value[field]};value[field]=[unique[k] for k in sorted(unique)]
        values.append(value)
    return {'protocol':PROTOCOL,'workflow_id':workflow,'consumer_node':node,'sources':values,'scope':SCOPE}


def mapping(document):
    if document is None:return {}
    if document.get('protocol')!=PROTOCOL:raise ValueError('Workflow source protocol required')
    values=document.get('sources')
    if not isinstance(values,list) or len(values)>64:raise ValueError('Bounded source set required')
    result={v['citation']:{k:copy.deepcopy(x) for k,x in v.items() if k!='citation'} for v in values}
    if len(result)!=len(values):raise ValueError('Unique workflow sources required')
    return result


def retrieval_matches(document):
    return {c:v['source']['match'] for c,v in mapping(document).items() if v['source']['kind']=='retrieval'}


def own_sources(row,policy,registry):
    result={};observations=row['state'].get('observations',[])
    if policy.retrieval_snapshot and 'retrieval.search' in policy.allowed_tools:
        from .retrieval import evidence
        matches,_=evidence(observations,registry,row['tenant_id'],policy)
        for c,match in matches.items():
            origins=[{'node':policy.parent_node,'observation_sha256':o['result_sha256']} for o in observations
                if o['tool']=='retrieval.search' and any(m['citation']==c for m in o['result']['matches'])]
            result[c]={'source':{'kind':'retrieval','snapshot_id':policy.retrieval_snapshot,'match':match,
                'verification':'frozen_retrieval_replay'},'origins':origins,'nodes':[],'edges':[]}
    for o in observations:
        if o['tool'] not in ('memory.get','memory.search'):continue
        if o['tool'] not in policy.allowed_tools or o['result_sha256']!=digest(o['result']) or not SHA.fullmatch(o['arguments_sha256']):
            raise ValueError('Uncommitted memory result')
        values=[o['result']] if o['tool']=='memory.get' else o['result']['matches']
        if len(values)>20:raise ValueError('Memory result exceeds bound')
        for m in values:
            if (m['namespace']!=policy.memory_namespace or type(m['version']) is not int or m['version']<1
                or not isinstance(m['key'],str) or not re.fullmatch(r'[a-zA-Z0-9_.:-]{1,128}',m['key'])
                or not SHA.fullmatch(m['sha256']) or not isinstance(m['text'],str) or len(m['text'].encode())>4096
                or type(m.get('text_truncated')) is not bool):raise ValueError('Bounded memory source required')
            citation=f"memory:{m['namespace']}:{m['key']}:v{m['version']}:{m['sha256']}"
            if m['citation']!=citation:raise ValueError('Memory citation differs')
            if o['tool']=='memory.get' and o['arguments_sha256']!=digest({'key':m['key']}):raise ValueError('Memory key differs')
            if not m['text_truncated'] and hashlib.sha256(m['text'].encode()).hexdigest()!=m['sha256']:raise ValueError('Complete memory bytes differ')
            value={'source':{'kind':'memory','namespace':m['namespace'],'key':m['key'],'version':m['version'],
                'document_sha256':m['sha256'],'text':m['text'],'text_sha256':hashlib.sha256(m['text'].encode()).hexdigest(),
                'text_truncated':m['text_truncated'],'verification':'committed_memory_excerpt',
                'full_document_bytes_replayed':not m['text_truncated']},
                'origins':[{'node':policy.parent_node,'observation_sha256':o['result_sha256']}],'nodes':[],'edges':[]}
            result=merge(result,{citation:value})
    return result


def terminal_anchor(row):
    e=row['terminal_event']
    if (row['status']!='completed' or row['result'].get('type')!='final' or not isinstance(e,dict)
        or e['kind']!='run.completed' or e['event_hash']!=row['event_hash'] or e['revision']!=row['revision']
        or e['payload'].get('state_sha256')!=digest(row['state']) or e['payload'].get('result_sha256')!=digest(row['result'])
        or digest(event_document(row['tenant_id'],row['id'],row['revision'],e['kind'],e['payload'],e['previous_hash']))!=row['event_hash']):
        raise ValueError('Terminal dependency anchor differs')
    return {'node':row['policy']['parent_node'],'run_id':row['id'],'revision':row['revision'],
        'event_head_sha256':row['event_hash'],'state_sha256':digest(row['state']),'result_sha256':digest(row['result'])}


def resolve_graph(parent,run,rows,registry):
    """Pure validation after a repeatable-read, tenant-scoped DB snapshot."""
    from .workflows import spec_from,projection
    spec=spec_from(parent);nodes={n.id:n for n in spec.nodes};current=run['policy']['parent_node']
    tenant=run['tenant_id'];rid=parent['id'];cache={};visiting=set()
    if (parent['tenant_id']!=tenant or parent['status'] not in ('queued','running','completed','failed','cancelled') or current not in nodes
        or not nodes[current].inherit_sources or not nodes[current].depends_on
        or parent['status'] in ('completed','failed','cancelled') and run['status']!='completed'):raise ValueError('Workflow source authority missing')
    def identity(row,nid):
        if (row['tenant_id']!=tenant or row['id']!=parent['state']['nodes'][nid]['run_id']
            or row['goal']!=nodes[nid].goal or row['model']!=nodes[nid].model
            or row['deadline_at']!=parent['deadline_at']
            or digest(row['policy'])!=digest(nodes[nid].policy(spec,parent=rid,node=nid).document())):
            raise ValueError('Dependency identity or grants differ')
    identity(run,current)
    def incoming(nid,row):
        deps=nodes[nid].depends_on
        if row['state'].get('workflow_inputs')!=[projection(rows[d]) for d in deps]:raise ValueError('Committed dependency projection differs')
        sources={}
        for dep in deps:
            values=source_node(dep)
            for v in values.values():v['edges'].append({'from':dep,'to':nid})
            sources=merge(sources,values)
        return sources
    def source_node(nid):
        if nid in cache:return copy.deepcopy(cache[nid])
        if nid in visiting or len(visiting)>=16:raise ValueError('Source DAG exceeds bound')
        visiting.add(nid);row=rows[nid];identity(row,nid);anchor=terminal_anchor(row);policy=Policy(**{**row['policy'],
            'allowed_tools':tuple(row['policy']['allowed_tools']),'tool_sequence':tuple(row['policy'].get('tool_sequence',()))})
        final=FinalDecision.model_validate({k:row['result'][k] for k in ('type','answer','citations')});policy.validate_final(final)
        direct=own_sources(row,policy,registry);inherited=incoming(nid,row) if policy.inherit_sources else {}
        if policy.inherit_sources:
            expected=envelope(rid,nid,inherited,final.citations)
            if row['result'].get('dependency_evidence')!=expected:raise ValueError('Committed source envelope differs')
        all_sources=merge(direct,inherited)
        # Only source citations actually emitted by this committed upstream
        # answer propagate. Unused retrieved chunks are not implicitly granted.
        if not set(final.citations)<=all_sources.keys():raise ValueError('Upstream cited source unavailable')
        if policy.answer_format=='retrieved_span':
            from .retrieval import quote_evidence
            proof=quote_evidence(final,row['state'].get('observations',[]),registry,tenant,policy,
                inherited_matches=retrieval_matches(envelope(rid,nid,inherited)))
            if row['result'].get('source_evidence')!=proof:raise ValueError('Upstream quote proof differs')
        selected={c:all_sources[c] for c in set(final.citations)}
        for v in selected.values():
            v['nodes'].append(anchor);v['nodes']=list({canonical(x):x for x in v['nodes']}.values())
            v['nodes'].sort(key=lambda x:canonical(x));v['edges'].sort(key=lambda x:canonical(x));v['origins'].sort(key=lambda x:canonical(x))
        visiting.remove(nid);cache[nid]=selected;return copy.deepcopy(selected)
    inherited=incoming(current,run);document=envelope(rid,current,inherited)
    if len(canonical(document))>spec.max_dependency_bytes:raise ValueError('Workflow source context exceeds bound')
    if spec.retrieval_snapshot:
        if registry is None:raise PermissionError('Retrieval executor unavailable')
        registry.require(tenant,spec.memory_namespace,spec.retrieval_snapshot)
    return document


async def resolve(store,run,registry,*,tenant=None):
    """Never accept a persisted/model-provided envelope as source authority."""
    tenant=tenant or run['tenant_id']
    try:
        if not run['policy'].get('inherit_sources') or not run['policy'].get('parent_workflow'):raise ValueError('Explicit workflow source inheritance required')
        async with store.pool.connection() as db,db.transaction():
            await db.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
            parent=await one(db,'''SELECT id,tenant_id,goal,model,policy,state,status,deadline_at FROM bf_agents.runs
                WHERE tenant_id=%s AND id=%s''',(tenant,run['policy']['parent_workflow']))
            if parent is None:raise ValueError('Workflow parent missing')
            from .workflows import spec_from
            spec=spec_from(parent);nodes={n.id:n for n in spec.nodes};nid=run['policy']['parent_node'];ancestors=set()
            def visit(n):
                if n in ancestors:return
                ancestors.add(n)
                for d in nodes[n].depends_on:visit(d)
            visit(nid)
            ids=[parent['state']['nodes'][n]['run_id'] for n in ancestors]
            rows=await (await db.execute('''SELECT r.id,r.tenant_id,r.goal,r.model,r.policy,r.state,r.result,r.status,
                r.revision,r.event_hash,r.deadline_at,
                CASE WHEN e.run_id IS NULL THEN NULL ELSE jsonb_build_object('kind',e.kind,'payload',e.payload,
                    'event_hash',e.event_hash,'previous_hash',e.previous_hash,'revision',e.revision) END AS terminal_event
                FROM bf_agents.runs r LEFT JOIN bf_agents.events e ON e.tenant_id=r.tenant_id AND e.run_id=r.id AND e.revision=r.revision
                WHERE r.tenant_id=%s AND r.id=ANY(%s)''',(tenant,ids))).fetchall()
        indexed={r['policy']['parent_node']:r for r in rows}
        if len(rows)!=len(ancestors) or set(indexed)!=ancestors:raise ValueError('Workflow source set differs')
        actual=indexed[nid]
        if actual['revision']!=run['revision']:raise AgentError(409,'stale_agent_lease')
        if actual['id']!=run['id'] or actual['policy']!=run['policy'] or actual['state']!=run['state']:raise ValueError('Source consumer changed')
        return resolve_graph(parent,actual,indexed,registry)
    except PermissionError:raise
    except (ValueError,TypeError,KeyError,AttributeError):raise AgentError(409,'workflow_source_provenance_invalid') from None
