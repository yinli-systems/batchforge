import copy,hashlib
from datetime import datetime,timezone
import pytest
from agents.contracts import canonical,digest
from agents.retrieval import Snapshot
from ops import retrieval_squad as r
from ops import qualify_agent_runtime as q


def dataset():return {'version':'1.1','data':[{'title':'Mars','paragraphs':[{'context':'The mission launched in 2030.',
    'qas':[{'id':'q1','question':'When did the mission launch?','answers':[{'text':'2030','answer_start':24}]}]}]}]}


def test_corpus_extraction_excludes_question_ids_queries_gold_and_reference_offsets():
    corpus,cases=r.extract(canonical(dataset()));text=corpus.decode()
    assert 'When did' not in text and 'answer_start' not in text and 'qas' not in text and 'references' not in text
    assert cases[0]['references'][0]['text']=='2030'
    assert Snapshot(corpus).search(cases[0]['question'])['matches'][0]['document_id']==cases[0]['document_id']
    with pytest.raises(ValueError):r.official(canonical(dataset()))


def test_wrong_public_reference_span_and_duplicate_cases_are_not_repaired():
    data=dataset();data['data'][0]['paragraphs'][0]['qas'][0]['answers'][0]['answer_start']=0
    with pytest.raises(ValueError):r.extract(canonical(data))
    data=dataset();p=data['data'][0]['paragraphs'][0];p['qas'].append(p['qas'][0])
    with pytest.raises(ValueError):r.extract(canonical(data))


def test_grader_requires_actual_retrieval_replay_and_a_cited_chunk_covering_reference(monkeypatch):
    corpus,cases=r.extract(canonical(dataset()));s=Snapshot(corpus);case=cases[0]
    monkeypatch.setattr(r,'inputs',lambda:(corpus,cases))
    found=s.search(case['question']);observed={'tool':'retrieval.search','arguments_sha256':digest({'query':case['question']}),
        'result':found,'result_sha256':digest(found)}
    run={'status':'completed','state':{'observations':[observed]},'result':{'answer':'2030','citations':[found['matches'][0]['citation']]}}
    assert r.grade(case,run)['passed']
    missing=copy.deepcopy(run);missing['result']['citations']=[]
    assert not r.grade(case,missing)['passed']
    tampered=copy.deepcopy(run);tampered['state']['observations'][0]['result']['query']='another query'
    tampered['state']['observations'][0]['result_sha256']=digest(tampered['state']['observations'][0]['result'])
    assert not r.grade(case,tampered)['retrieval_replay_verified']
    wrong=copy.deepcopy(run);wrong['result']['answer']='2021'
    assert r.grade(case,wrong)['citation_verified'] and not r.grade(case,wrong)['passed']
    assert r.grade(case,{'status':'failed'})['exact_match']==0


def test_real_qualification_freezes_grants_and_snapshot_but_never_adds_gold_to_request(monkeypatch):
    corpus,cases=r.extract(canonical(dataset()));s=Snapshot(corpus)
    case={**cases[0],'goal':'Search the corpus for the mission launch date','snapshot_id':s.id,'tools':['retrieval.search'],
        'kind':'retrieval-squad','planner_protocol':'schema-v1','require_citations':True,'tool_sequence':['retrieval.search']}
    monkeypatch.setattr(q,'selected_fixtures',lambda name:[case]);requests=[]
    class Response:
        def raise_for_status(self):pass
        def json(self):return {'id':'agrun_test'}
    class Client:
        def post(self,path,**kwargs):requests.append(kwargs['json']);return Response()
    q.create_runs_http(Client(),'unique-run','retrieval-squad-v1')
    assert len(requests)==2 and requests[0]==requests[1]
    body=requests[0];assert body['allowed_tools']==['retrieval.search'] and body['retrieval_snapshot']==s.id
    assert 'references' not in str(body) and '2030' not in str(body)
    plan=q.plan(datetime(2026,10,26,3,tzinfo=timezone.utc),4,'retrieval-squad-v1')
    assert plan['max_new_tasks']==4 and plan['fault_reclaim_gate_seconds']==30
    with pytest.raises(ValueError):q.plan(datetime(2026,10,26,3,tzinfo=timezone.utc),3,'retrieval-squad-v1')


def test_independent_api_and_cluster_lifetime_cannot_outlive_final_cleanup_margin():
    command=['/fixed/python','-m','agents','api']
    early=q.finite_command(command,datetime(2026,10,26,3,tzinfo=timezone.utc))
    assert early[0]=='/usr/bin/timeout' and early[3]=='2100' and early[4:]==command
    late=q.finite_command(command,datetime(2026,10,26,8,58,tzinfo=timezone.utc))
    assert late[3]=='90'
    with pytest.raises(ValueError):q.finite_command(command,datetime(2026,10,26,8,59,tzinfo=timezone.utc))
    with pytest.raises(ValueError):q.finite_command(command,datetime(2026,10,26,3))
    with pytest.raises(ValueError):q.finite_command(command,datetime(2026,10,26,3,tzinfo=timezone.utc),seconds=True)


def test_span_validation_is_disjoint_by_id_before_any_retrieval_or_model_answers(monkeypatch):
    corpus,cases=r.extract(canonical(dataset()));cases=[{**cases[0],'id':str(i)} for i in range(32)]
    monkeypatch.setattr(r,'official',lambda raw:(corpus,cases))
    _,old=r.selected(b'fixture');_,new=r.selected(b'fixture','retrieval-span-v1')
    assert len(old)==len(new)==8 and not {c['id'] for c in old}&{c['id'] for c in new}
    assert all(c['answer_format']=='retrieved_span' for c in new)
    assert all('answer_format' not in c for c in old)
    assert digest(r.selected(b'fixture','retrieval-span-v1')[1])==digest(new)


def test_abstention_scores_zero_and_quote_commitment_is_required_for_new_contract(monkeypatch):
    corpus,cases=r.extract(canonical(dataset()));case={**cases[0],'answer_format':'retrieved_span'};s=Snapshot(corpus)
    monkeypatch.setattr(r,'inputs',lambda:(corpus,cases))
    found=s.search(case['question']);m=found['matches'][0]
    observed={'tool':'retrieval.search','arguments_sha256':digest({'query':case['question']}),'result':found,'result_sha256':digest(found)}
    run={'status':'completed','state':{'observations':[observed]},'result':{'type':'abstain','reason':'insufficient_evidence'}}
    value=r.grade(case,run);assert value['abstained'] and value['completed'] and not value['answer_delivered'] and value['f1']==0
    run['result']={'type':'final','answer':'2030','citations':[m['citation']]}
    assert not r.grade(case,run)['passed']
    start=m['text'].encode().index(b'2030')+m['start_byte']
    run['result']['source_evidence']={'protocol':'batchforge.quoted-answer.v1','snapshot_id':s.id,'quotes':[{
        'citation':m['citation'],'document_id':m['document_id'],'document_version':m['document_version'],'document_sha256':m['document_sha256'],
        'start_byte':start,'end_byte':start+4,'answer_sha256':hashlib.sha256(b'2030').hexdigest()}]}
    assert r.grade(case,run)['passed']
    run['result']['source_evidence']['quotes'][0]['start_byte']+=1
    assert not r.grade(case,run)['passed']
