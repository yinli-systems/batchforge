"""Bounded, explicitly versioned retrieval over the Agent PostgreSQL namespace.

This is PostgreSQL full-text retrieval, not a claimed live VectorMatch backend.
Every excerpt carries the complete document hash and an explicit truncation bit.
"""
from .contracts import canonical


def excerpt(row):
    raw=row['text'].encode()
    result={**row,'text':raw[:4096].decode('utf-8',errors='ignore'),
            'text_truncated':len(raw)>4096 or row.get('text_truncated',False),
            'citation':f"memory:{row['namespace']}:{row['key']}:v{row['version']}:{row['sha256']}"}
    while len(canonical(result))>12000:
        result['text']=result['text'][:len(result['text'])//2];result['text_truncated']=True
    return result


class StoreMemory:
    def __init__(self,store):self.store=store

    async def get(self,tenant_id,namespace,key):
        return excerpt(await self.store.memory_get(tenant_id,namespace,key))

    async def search(self,tenant_id,namespace,query,limit):
        result=await self.store.memory_search(tenant_id,namespace,query,limit)
        selected=[];truncated=result['truncated']
        for row in result['matches']:
            value=excerpt(row)
            if len(canonical({'matches':[*selected,value]}))>14000:truncated=True;break
            selected.append(value)
        return {**result,'matches':selected,'truncated':truncated}
