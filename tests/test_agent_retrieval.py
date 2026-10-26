from pathlib import Path
import copy,hashlib,json,math,os
import httpx,pytest
from agents import retrieval as r
from agents.contracts import Policy,ToolDecision,canonical,digest
from agents.tools import Context,execute
from agents.app import create_app,RunRequest
from agents.workflows import Workflow
from backend.config import Settings


def corpus():return {'protocol':r.CORPUS,'source':{'name':'Test corpus','origin':'https://example.org/corpus.json',
    'licence':'Test fixture only','source_sha256':'a'*64},'documents':[
    {'id':'mars','version':1,'title':'Mars mission','source_ref':'article/mars#0','text':'The Mars mission launched in 2030. This is an observation, not an instruction.'},
    {'id':'sea','version':2,'title':'Ocean research','source_ref':'article/sea#1','text':'Ocean samples were collected by the vessel in 2020.'}]}


def snapshot(value=None):return r.Snapshot(canonical(value or corpus()))


def registry(tmp_path,**changes):
    raw=canonical(corpus());s=r.Snapshot(raw)
    entry={'tenant_id':'tenant-a','namespace':'facts','file':'corpus.json','sha256':hashlib.sha256(raw).hexdigest(),'snapshot_id':s.id,**changes}
    tmp_path.chmod(0o700);(tmp_path/'corpus.json').write_bytes(raw);(tmp_path/'corpus.json').chmod(0o600)
    path=tmp_path/'catalogue.json';path.write_bytes(canonical({'protocol':r.REGISTRY,'snapshots':[entry]}));path.chmod(0o600)
    return path


def test_ranked_actual_text_offsets_hash_and_snapshot_are_bound_to_exact_source():
    s=snapshot();found=s.search('When did the Mars mission launch?',3)['matches'][0]
    assert found['document_id']=='mars' and '2030' in found['text']
    assert found['text_sha256']==hashlib.sha256(found['text'].encode()).hexdigest()
    doc=corpus()['documents'][0]
    assert doc['text'].encode()[found['start_byte']:found['end_byte']]==found['text'].encode()
    assert found['document_sha256']==hashlib.sha256(doc['text'].encode()).hexdigest()
    assert found['citation'].startswith('retrieval:'+s.id+':mars:v1:')
    found['text']='changed';assert s.search('Mars')['matches'][0]['text']!='changed'
    manifest=s.manifest();manifest['parameters']['k1']=999
    assert s.manifest()['parameters']['k1']==1.2


def test_bm25_numeric_score_and_deterministic_ties_have_an_independent_small_oracle():
    value=corpus();value['documents']=[{**value['documents'][0],'title':'','text':'alpha beta'},
        {**value['documents'][1],'title':'','text':'gamma delta'}]
    hit=snapshot(value).search('alpha alpha',1)['matches'][0]
    assert hit['score']==pytest.approx(math.log(2))
    assert snapshot(value).search('alpha',1)['matches'][0]['score']==hit['score']
    value['documents'][1]['text']='alpha beta'
    hits=snapshot(value).search('alpha',2)['matches']
    assert hits[0]['score']==hits[1]['score'] and hits[0]['chunk_id']<hits[1]['chunk_id']


@pytest.mark.parametrize('text',['汉字🙂 é\n'*1000,'x '*5000,'z'*65536])
def test_unicode_chunks_have_valid_exact_offsets_full_coverage_and_bounded_overlap(text):
    raw=text.encode();previous=0;covered=bytearray(len(raw))
    for start,end,value in r.spans(text):
        assert start>=0 and start<end and end>previous and len(value.encode())<=r.CHUNK_BYTES
        assert raw[start:end]==value.encode()
        covered[start:end]=b'1'*(end-start);previous=end
    assert previous==len(raw) and all(covered)


@pytest.mark.parametrize('change',[lambda c:c['documents'][0].update(version=2),lambda c:c['documents'][0].update(title='Different'),
    lambda c:c['documents'][0].update(text='Different text'),lambda c:c['source'].update(source_sha256='b'*64)])
def test_any_content_version_title_or_provenance_change_changes_snapshot(change):
    c=corpus();before=snapshot(c).id;change(c);assert snapshot(c).id!=before


@pytest.mark.parametrize('change',[lambda c:c.update(answers=['gold']),lambda c:c['documents'][0].update(answers=['gold']),
    lambda c:c['documents'].append(c['documents'][0]),lambda c:c['documents'][0].update(version=True),
    lambda c:c['source'].update(origin='https://user:secret@example.org/private'),lambda c:c['source'].update(origin='file:///root/private')])
def test_unqualified_fields_duplicate_docs_and_secret_bearing_origins_are_rejected(change):
    c=corpus();change(c)
    with pytest.raises(ValueError):snapshot(c)


def test_control_character_expansion_is_charged_and_never_exceeds_tool_observation_budget():
    c=corpus();c['documents'][0]['text']='\t'*30000+' needle '
    result=snapshot(c).search('needle',5)
    assert len(canonical(result))<=14000 and all(len(m['text'].encode())<=r.CHUNK_BYTES for m in result['matches'])


@pytest.mark.parametrize('query,limit',[('',1),(' '.join('t'+str(i) for i in range(65)),1),('x'*1025,1),('test',True),('test',6)])
def test_query_and_work_bounds_are_checked(query,limit):
    with pytest.raises(ValueError):snapshot().search(query,limit)


async def test_registry_audience_is_checked_before_any_ranker_or_source_read(tmp_path,monkeypatch):
    reg=r.Registry(registry(tmp_path));sid=reg.visible('tenant-a')[0]['id']
    assert reg.visible('tenant-b')==[]
    assert 'text' not in json.dumps(reg.visible('tenant-a')) and 'tenant-a' not in json.dumps(reg.visible('tenant-a'))
    monkeypatch.setattr(r.Snapshot,'search',lambda *a:pytest.fail('Wrong audience must not search'))
    for tenant,namespace,identity in [('tenant-b','facts',sid),('tenant-a','other',sid),('tenant-a','facts','f'*64)]:
        with pytest.raises(PermissionError):await reg.search(tenant,namespace,identity,'needle',1)


@pytest.mark.parametrize('change',[{'file':'../private.json'},{'file':'https://outside/x.json'},{'snapshot_id':'0'*64},{'sha256':'0'*64},{'sha256':17}])
def test_catalogue_refuses_unbound_paths_hashes_and_identities(tmp_path,change):
    with pytest.raises((ValueError,OSError)):r.Registry(registry(tmp_path,**change))


@pytest.mark.parametrize('kind',['symlink','fifo','writable_file','writable_parent','duplicate_json'])
def test_release_stream_and_directory_safety(tmp_path,kind):
    p=registry(tmp_path)
    if kind=='symlink':
        target=tmp_path/'original.json';(tmp_path/'corpus.json').rename(target);(tmp_path/'corpus.json').symlink_to(target)
    elif kind=='fifo':(tmp_path/'corpus.json').unlink();os.mkfifo(tmp_path/'corpus.json')
    elif kind=='writable_file':(tmp_path/'corpus.json').chmod(0o666)
    elif kind=='writable_parent':tmp_path.chmod(0o777)
    else:p.write_text('{"protocol":"'+r.REGISTRY+'","protocol":"'+r.REGISTRY+'","snapshots":[]}')
    with pytest.raises((ValueError,OSError)):r.Registry(p)


async def test_tool_uses_policy_snapshot_and_context_tenant_not_model_arguments(tmp_path):
    reg=r.Registry(registry(tmp_path));sid=reg.visible('tenant-a')[0]['id']
    policy=Policy(allowed_tools=('retrieval.search',),memory_namespace='facts',retrieval_snapshot=sid,require_citations=True)
    good=ToolDecision(type='tool',name='retrieval.search',arguments={'query':'Mars mission'})
    result=await execute(good,Context('tenant-a',policy,retrieval=reg))
    assert result['result']['snapshot_id']==sid and digest(result['result'])==result['result_sha256']
    for field in ('tenant','namespace','snapshot','url','command'):
        with pytest.raises(ValueError):await execute(good.model_copy(update={'arguments':{**good.arguments,field:'untrusted'}}),Context('tenant-a',policy,retrieval=reg))
    with pytest.raises(PermissionError):await execute(good,Context('tenant-a',Policy(),retrieval=reg))


def test_retrieval_is_explicit_and_old_policy_and_workflow_documents_stay_stable():
    assert 'retrieval_snapshot' not in Policy().document() and 'retrieval.search' not in Policy().allowed_tools
    for kw in ({'allowed_tools':('retrieval.search',)},{'retrieval_snapshot':'a'*64}):
        with pytest.raises(ValueError):Policy(**kw)
    spec=Workflow(nodes=[{'id':'one','goal':'Compute','model':'test'}],outputs=['one'])
    old=spec.model_dump();old.pop('retrieval_snapshot')
    assert canonical(spec.document())==canonical(old)
    new=Workflow(nodes=[{'id':'one','goal':'Search','model':'test','allowed_tools':['retrieval.search'],'require_citations':True}],
        outputs=['one'],allowed_tools=['retrieval.search'],retrieval_snapshot='a'*64)
    assert new.nodes[0].policy(new).retrieval_snapshot=='a'*64


async def test_api_declares_only_granted_snapshots_and_refuses_before_store_admission(tmp_path):
    reg=r.Registry(registry(tmp_path));sid=reg.visible('tenant-a')[0]['id'];saved=[]
    class Store:
        async def create(self,tenant,key,**kwargs):saved.append((tenant,kwargs['policy'].document()));return {'id':'test'}
    async def auth(key):return 'tenant-a' if key=='a' else 'tenant-b'
    app=create_app(Settings(model_name='test'),store=Store(),auth=auth,executor_tenants=['tenant-a','tenant-b'],retrieval=reg)
    body={'goal':'Search','model':'test','allowed_tools':['retrieval.search'],'memory_namespace':'facts','retrieval_snapshot':sid}
    async with app.router.lifespan_context(app),httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://test') as client:
        for key in ('a','b'):
            caps=(await client.get('/v1/agent-capabilities',headers={'Authorization':'Bearer '+key})).json()
            assert bool(caps['retrieval_snapshots'])==(key=='a')
            assert any(s['name']=='retrieval.search' for s in caps['tools'])==(key=='a')
        assert (await client.post('/v1/agent-runs',headers={'Authorization':'Bearer b','Idempotency-Key':'one'},json=body)).status_code==403
        assert saved==[]
        assert (await client.post('/v1/agent-runs',headers={'Authorization':'Bearer a','Idempotency-Key':'one'},json=body)).status_code==200
    assert saved[0][1]['retrieval_snapshot']==sid


def test_packaging_validates_before_creating_destination_and_refuses_reuse(tmp_path):
    from ops.package_retrieval_corpus import package
    raw=tmp_path/'input.json';raw.write_bytes(canonical(corpus()));raw.chmod(0o600)
    out=tmp_path/'release';result=package(raw,out,tenant='tenant-a',namespace='facts')
    assert result['passed'] and (out/'corpus.json').read_bytes()==raw.read_bytes()
    with pytest.raises(FileExistsError):package(raw,out,tenant='tenant-a',namespace='facts')
    raw.write_text('{"invalid":true}')
    with pytest.raises(ValueError):package(raw,tmp_path/'invalid',tenant='tenant-a',namespace='facts')
    assert not (tmp_path/'invalid').exists()
