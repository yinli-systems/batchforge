"""Immutable, operator-published BM25 corpora with exact excerpt commitments.

Not a vector embedding service, a network fetcher or the VectorMatch matching
engine. An Agent chooses a bounded query; its immutable policy and the local
audience registry select the only corpus it may read. No answer/reference is
part of an index. Citation consistency does not prove semantic entailment.
"""
from __future__ import annotations
from collections import Counter,defaultdict
from pathlib import Path
from types import MappingProxyType
import copy,hashlib,math,os,re,unicodedata,stat
from pydantic import Field
from backend.evaluations import private_release_bytes
from .contracts import Strict,canonical,digest,protocol_json,OutputContractError

CORPUS='batchforge.retrieval-corpus.v1'
REGISTRY='batchforge.retrieval-catalogue.v1'
INDEX='batchforge.bm25-utf8-chunks.v1'
MAX_BYTES=64*1024**2
CHUNK_BYTES=1536
OVERLAP_BYTES=128
PARAMETERS={'k1':1.2,'b':0.75,'chunk_bytes':CHUNK_BYTES,'overlap_bytes':OVERLAP_BYTES,
    'tokenizer':'unicode-nfkc-casefold-word-v1','query_term_frequency':'binary','fields':'title+chunk',
    'score':'idf*tf*(k1+1)/(tf+k1*(1-b+b*length/avg_length))','tie_break':'chunk_id_ascending'}
SHA=re.compile(r'^[0-9a-f]{64}$')
NAME=re.compile(r'^[A-Za-z0-9_.:-]{1,128}$')
NAMESPACE=re.compile(r'^[a-zA-Z0-9_-]{1,64}$')


class Source(Strict):
    name:str=Field(min_length=1,max_length=256)
    origin:str=Field(max_length=512,pattern=r'^https://[A-Za-z0-9.-]+/[A-Za-z0-9_./~-]*$')
    licence:str=Field(min_length=1,max_length=256)
    source_sha256:str=Field(pattern=r'^[0-9a-f]{64}$')


class Document(Strict):
    id:str=Field(pattern=r'^[A-Za-z0-9_.:-]{1,128}$')
    version:int=Field(ge=1,le=2147483647)
    title:str=Field(max_length=256)
    source_ref:str=Field(pattern=r'^[A-Za-z0-9_.:/#-]{1,256}$')
    text:str=Field(min_length=1,max_length=65536)


class Corpus(Strict):
    protocol:str=Field(pattern=r'^batchforge\.retrieval-corpus\.v1$')
    source:Source
    documents:list[Document]=Field(min_length=1,max_length=20000)


def tokens(text):return re.findall(r'[^\W_]+',unicodedata.normalize('NFKC',text).casefold())


def spans(text):
    """UTF8 byte windows; boundaries never cut a codepoint or invent offsets."""
    raw=text.encode();start=0
    while start<len(raw):
        end=start+len(raw[start:start+CHUNK_BYTES].decode('utf-8',errors='ignore').encode())
        if end<len(raw):
            space=raw.rfind(b' ',start,end)
            if space>start+CHUNK_BYTES//2:end=space
        if end<=start:raise ValueError('UTF8 chunk did not advance')
        yield start,end,raw[start:end].decode('utf-8')
        if end==len(raw):break
        next_start=max(start+1,end-OVERLAP_BYTES)
        while raw[next_start]&0xc0==0x80:next_start+=1
        start=next_start


class Snapshot:
    def __init__(self,raw):
        if not isinstance(raw,bytes) or not 1<=len(raw)<=MAX_BYTES:raise ValueError('Bounded corpus bytes required')
        corpus=Corpus.model_validate(protocol_json(raw));seen=set();chunks=[];postings=defaultdict(dict);lengths=[];total_terms=0
        corpus_sha=hashlib.sha256(raw).hexdigest()
        self._identity={'protocol':INDEX,'corpus_sha256':corpus_sha,'parameters':copy.deepcopy(PARAMETERS)}
        self.id=digest(self._identity);self.source=MappingProxyType(corpus.source.model_dump())
        documents=[]
        for doc in sorted(corpus.documents,key=lambda d:d.id):
            if doc.id in seen or len(doc.text.encode())>65536:raise ValueError('Unique bounded document identities required')
            seen.add(doc.id);full_sha=hashlib.sha256(doc.text.encode()).hexdigest()
            documents.append({'id':doc.id,'version':doc.version,'sha256':full_sha,'source_ref':doc.source_ref,'bytes':len(doc.text.encode())})
            for start,end,text in spans(doc.text):
                value={'document_id':doc.id,'document_version':doc.version,'document_sha256':full_sha,
                    'title':doc.title,'source_ref':doc.source_ref,'start_byte':start,'end_byte':end,'text':text,
                    'text_sha256':hashlib.sha256(text.encode()).hexdigest()}
                value['chunk_id']=digest({k:v for k,v in value.items() if k not in ('text','title')})
                value['citation']=f"retrieval:{self.id}:{doc.id}:v{doc.version}:{start}:{end}:{value['text_sha256']}"
                counts=Counter(tokens(doc.title+' '+text));lengths.append(sum(counts.values()));total_terms+=len(counts)
                if len(chunks)>=50000 or total_terms>5000000:raise ValueError('Corpus index exceeds work bound')
                number=len(chunks);chunks.append(MappingProxyType(value))
                for term,count in counts.items():postings[term][number]=count
        self._chunks=tuple(chunks);self._lengths=tuple(lengths)
        self._citations=MappingProxyType({c['citation']:c for c in self._chunks})
        self._postings=MappingProxyType({k:MappingProxyType(v) for k,v in postings.items()})
        self._average=sum(lengths)/len(lengths) or 1
        self._manifest={'protocol':INDEX,'id':self.id,'corpus_sha256':corpus_sha,'parameters':copy.deepcopy(PARAMETERS),
            'source':dict(self.source),'document_count':len(documents),'chunk_count':len(chunks),
            'documents_sha256':digest(documents)}

    def manifest(self):return copy.deepcopy(self._manifest)

    def search(self,query,limit=3):
        if not isinstance(query,str) or not 1<=len(query.encode())<=1024 or type(limit) is not int or not 1<=limit<=5:
            raise ValueError('Bounded query and top-k required')
        terms=sorted(set(tokens(query)))
        if len(terms)>64:raise ValueError('Query term work bound exceeded')
        score=defaultdict(float);n=len(self._chunks)
        for term in terms:
            postings=self._postings.get(term,{})
            idf=math.log1p((n-len(postings)+.5)/(len(postings)+.5))
            for number,tf in postings.items():
                score[number]+=idf*tf*2.2/(tf+1.2*(.25+.75*self._lengths[number]/self._average))
        ranked=sorted(score,key=lambda i:(-score[i],self._chunks[i]['chunk_id']))
        result={'protocol':'batchforge.retrieval-result.v1','backend':'bm25','snapshot_id':self.id,
            'corpus_sha256':self._manifest['corpus_sha256'],'query_sha256':hashlib.sha256(query.encode()).hexdigest(),
            'query':query,'requested_limit':limit,
            'matches':[],'truncated':False}
        for i in ranked[:limit]:
            item={**self._chunks[i],'score':score[i]}
            if len(canonical({**result,'matches':[*result['matches'],item]}))>14000:result['truncated']=True;break
            result['matches'].append(item)
        result['truncated']=result['truncated'] or len(ranked)>len(result['matches'])
        return result

    def verify_match(self,match):
        if not isinstance(match,dict) or self._citations.get(match.get('citation'))!={k:v for k,v in match.items() if k!='score'}:
            raise ValueError('Source chunk differs from pinned corpus')
        return True

    def verify(self,result):
        if not isinstance(result,dict) or result.get('snapshot_id')!=self.id:raise ValueError('Wrong retrieval snapshot')
        if self.search(result.get('query'),result.get('requested_limit'))!=result:raise ValueError('Retrieval result differs from frozen index')
        return True


class Registry:
    def __init__(self,path=''):
        self._grants={};self._snapshots={}
        if not path:return
        path=Path(path).absolute()
        parent=path.parent.stat()
        if path.resolve()!=path or not stat.S_ISDIR(parent.st_mode) or parent.st_uid!=os.geteuid() or parent.st_mode&0o022:
            raise ValueError('Owned non-writable resolved catalogue directory required')
        config=protocol_json(private_release_bytes(path,1024**2))
        if not isinstance(config,dict) or set(config)!= {'protocol','snapshots'} or config['protocol']!=REGISTRY or not isinstance(config['snapshots'],list) or len(config['snapshots'])>8:
            raise ValueError('Bounded retrieval audience catalogue required')
        total=0
        for entry in config['snapshots']:
            if (not isinstance(entry,dict) or set(entry)!={'tenant_id','namespace','file','sha256','snapshot_id'}
                or not isinstance(entry['tenant_id'],str) or not NAME.fullmatch(entry['tenant_id'])
                or not isinstance(entry['namespace'],str) or not NAMESPACE.fullmatch(entry['namespace'])
                or not isinstance(entry['file'],str) or not re.fullmatch(r'[A-Za-z0-9_-]+\.json',entry['file'])
                or not isinstance(entry['sha256'],str) or not SHA.fullmatch(entry['sha256'])
                or not isinstance(entry['snapshot_id'],str) or not SHA.fullmatch(entry['snapshot_id'])):raise ValueError('Explicit retrieval audience and pinned basename required')
            key=(entry['tenant_id'],entry['namespace'],entry['snapshot_id'])
            if key in self._grants:raise ValueError('Duplicate retrieval grant')
            raw=private_release_bytes(path.parent/entry['file'],MAX_BYTES);total+=len(raw)
            if total>MAX_BYTES or hashlib.sha256(raw).hexdigest()!=entry['sha256']:raise ValueError('Pinned bounded corpus bytes required')
            snapshot=self._snapshots.get(entry['sha256']) or Snapshot(raw)
            if snapshot.id!=entry['snapshot_id']:raise ValueError('Index identity differs from corpus/analyzer')
            self._snapshots[entry['sha256']]=snapshot;self._grants[key]=snapshot

    def require(self,tenant,namespace,snapshot):
        value=self._grants.get((tenant,namespace,snapshot))
        if value is None:raise PermissionError('Retrieval snapshot is not granted')
        return value

    def visible(self,tenant):
        return [{'namespace':namespace,**snapshot.manifest()} for (tid,namespace,_),snapshot in sorted(self._grants.items()) if tid==tenant]

    async def search(self,tenant,namespace,snapshot,query,limit):
        return self.require(tenant,namespace,snapshot).search(query,limit)


def evidence(observations,registry,tenant,policy):
    """Validate committed source observations before using them as authority."""
    if registry is None:raise PermissionError('Retrieval executor unavailable')
    snapshot=registry.require(tenant,policy.memory_namespace,policy.retrieval_snapshot)
    matches={};queries=[]
    for observation in observations:
        if observation['tool']!='retrieval.search':continue
        value=observation['result'];snapshot.verify(value)
        permitted={digest({'query':value['query'],'limit':value['requested_limit']})}
        if value['requested_limit']==3:permitted.add(digest({'query':value['query']}))
        if observation['arguments_sha256'] not in permitted or observation['result_sha256']!=digest(value):
            raise ValueError('Committed retrieval arguments or bytes differ')
        queries.append(value['query_sha256'])
        for match in value['matches']:
            source={k:v for k,v in match.items() if k!='score'}
            if match['citation'] in matches and matches[match['citation']]!=source:raise ValueError('Ambiguous committed source')
            matches[match['citation']]=source
    return matches,queries


def quote_evidence(decision,observations,registry,tenant,policy,*,inherited_matches=None):
    if policy.answer_format!='retrieved_span':raise ValueError('Explicit extractive answer contract required')
    policy.validate_final(decision)
    matches,queries=evidence(observations,registry,tenant,policy)
    if inherited_matches:
        if not policy.inherit_sources:raise ValueError('Explicit source inheritance required')
        snapshot=registry.require(tenant,policy.memory_namespace,policy.retrieval_snapshot)
        for cite,match in inherited_matches.items():
            snapshot.verify_match(match)
            if cite!=match['citation'] or cite in matches and matches[cite]!=match:raise ValueError('Inherited source identity differs')
            matches[cite]=match
    if not queries and not inherited_matches:raise OutputContractError('retrieval_required')
    if not set(decision.citations)<=matches.keys():raise OutputContractError('retrieved_span')
    quotes=[];raw=decision.answer.encode()
    for citation in decision.citations:
        match=matches[citation];offset=match['text'].encode().find(raw)
        if offset<0:continue
        quotes.append({'citation':citation,'document_id':match['document_id'],'document_version':match['document_version'],
            'document_sha256':match['document_sha256'],'start_byte':match['start_byte']+offset,
            'end_byte':match['start_byte']+offset+len(raw),'answer_sha256':hashlib.sha256(raw).hexdigest()})
    if not quotes:raise OutputContractError('retrieved_span')
    return {'protocol':'batchforge.quoted-answer.v1','snapshot_id':policy.retrieval_snapshot,'quotes':quotes,
        'scope':'Exact answer bytes occur in a cited retrieved source; semantic correctness is not established'}
