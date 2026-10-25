"""BatchForge adapter: one real Batch task for each durable Agent model step.

Inference admission stays on the existing HTTP boundary, so primary maintenance
gates and tenant quotas apply. Read-only artifact lookup uses the same database.
Keys are executor configuration, never model output or workflow state.
"""
from __future__ import annotations
import asyncio,hashlib
from urllib.parse import urlsplit
import httpx
from backend.receipts import verify as verify_receipt
from .contracts import canonical,digest,protocol_json as strict_json
from .runtime import RetryLater,DeliveryError


class BatchAdapter:
    def __init__(self,pool,*,base_url,tenant_keys,client=None):
        parsed=urlsplit(base_url)
        if (parsed.scheme not in ('http','https') or parsed.username or parsed.password or parsed.query or parsed.fragment
            or parsed.path not in ('','/') or (parsed.scheme=='http' and parsed.hostname not in ('127.0.0.1','localhost','::1'))):
            raise ValueError('Pinned HTTPS Batch endpoint or loopback HTTP required')
        self.pool=pool;self.base_url=base_url.rstrip('/');self.tenant_keys=dict(tenant_keys)
        if not self.tenant_keys or any(not isinstance(k,str) or not isinstance(v,str) or not v for k,v in self.tenant_keys.items()):
            raise ValueError('Explicit executor tenant credentials required')
        self.client=client or httpx.AsyncClient(timeout=httpx.Timeout(15,connect=5),follow_redirects=False)

    @staticmethod
    def input_bytes(run,state):
        if digest(state['request'])!=state['request_sha256']:raise DeliveryError('Input intent drift')
        return canonical({'custom_id':f"agent-step-{state['step']}",'method':'POST','url':'/v1/chat/completions','body':state['request']})+b'\n'

    @staticmethod
    def filename(run,state):return f"agent-{run['id']}-{state['step']}-{state['request_sha256']}.jsonl"

    async def request(self,run,method,path,**kwargs):
        key=self.tenant_keys.get(run['tenant_id'])
        if not key:raise PermissionError('Tenant executor not configured')
        headers={'Authorization':'Bearer '+key,**kwargs.pop('headers',{})}
        try:response=await self.client.request(method,self.base_url+path,headers=headers,**kwargs)
        except httpx.TransportError:raise RetryLater() from None
        if response.status_code in (408,429,500,502,503,504):raise RetryLater()
        if response.status_code>=300:raise DeliveryError('Batch request refused')
        if len(response.content)>262144:raise DeliveryError('Batch response exceeds bound')
        return response

    async def find_input(self,run,state):
        raw=self.input_bytes(run,state);expected=hashlib.sha256(raw).hexdigest()
        async with self.pool.connection() as conn:
            rows=await (await conn.execute('''SELECT f.id,f.bytes,
                COALESCE(b.sha256,encode(sha256(f.content),'hex')) AS sha256
                FROM public.files f LEFT JOIN public.blob_objects b ON b.id=f.blob_id AND b.tenant_id=f.tenant_id
                WHERE f.tenant_id=%s AND f.filename=%s AND f.kind='input' AND f.storage_deleted_at IS NULL
                AND (f.storage_expires_at IS NULL OR f.storage_expires_at>%s)
                AND (f.blob_id IS NULL OR b.state='ready') ORDER BY f.created_at LIMIT 2''',
                (run['tenant_id'],self.filename(run,state),run['deadline_at']))).fetchall()
        if len(rows)>1:raise DeliveryError('Ambiguous named artifact')
        if not rows:return None
        row=rows[0]
        if row['bytes']!=len(raw) or row['sha256']!=expected:raise DeliveryError('Named artifact differs from durable input')
        # Verify actual bytes as well as catalogue metadata before reuse.
        response=await self.request(run,'GET',f"/v1/files/{row['id']}/content")
        if response.content!=raw:raise DeliveryError('Input byte readback mismatch')
        return row['id']

    async def upload(self,run,state):
        response=await self.request(run,'POST','/v1/files',data={'purpose':'batch'},
            files={'file':(self.filename(run,state),self.input_bytes(run,state),'application/jsonl')})
        document=strict_json(response.content)
        if not isinstance(document.get('id'),str) or not document['id'].startswith('file_'):raise DeliveryError('Invalid input file ACK')
        return document['id']

    async def create(self,run,state):
        response=await self.request(run,'POST','/v1/batches',headers={'Idempotency-Key':state['child_key']},
            json={'input_file_id':state['file_id'],'endpoint':'/v1/chat/completions','completion_window':'24h',
                  'metadata':{'agent_run_id':run['id'],'agent_step':str(state['step']),'request_sha256':state['request_sha256']}})
        document=strict_json(response.content)
        if not isinstance(document.get('id'),str) or not document['id'].startswith('batch_'):raise DeliveryError('Invalid Batch ACK')
        return document['id']

    async def poll(self,run,state):
        bid=state['batch_id'];batch=strict_json((await self.request(run,'GET',f'/v1/batches/{bid}')).content)
        if batch['status']=='in_progress':return None
        if batch['status']!='completed' or batch['request_counts']['completed']!=1 or not batch.get('output_file_id'):
            raise DeliveryError('Child Batch did not deliver one accepted output')
        envelope=strict_json((await self.request(run,'GET',f'/v1/batches/{bid}/receipt')).content)
        if not verify_receipt(envelope):raise DeliveryError('Receipt checksum mismatch')
        receipt=envelope['receipt']
        if (receipt['batch_id']!=bid or receipt['model']!=run['model'] or receipt['ledger_state']!='consistent_ledger'
            or receipt['counts']['total']!=1 or not all(receipt['checks'].values())):raise DeliveryError('Child ledger inconsistent')
        raw=(await self.request(run,'GET',f"/v1/files/{batch['output_file_id']}/content")).content
        lines=[strict_json(line) for line in raw.splitlines() if line.strip()]
        if len(lines)!=1:raise DeliveryError('One child response required')
        row=lines[0]
        if row.get('custom_id')!=f"agent-step-{state['step']}" or row.get('error') or row['response']['status_code']!=200:
            raise DeliveryError('Child response identity differs')
        body=row['response']['body'];choices=body.get('choices')
        if body.get('model')!=run['model'] or not isinstance(choices,list) or len(choices)!=1:
            raise DeliveryError('Ambiguous planner response')
        usage=body['usage']
        if {k:usage[k] for k in ('prompt_tokens','completion_tokens')}!=receipt['accepted_usage']:
            raise DeliveryError('Child response and ledger usage differ')
        return {'content':choices[0]['message']['content'],'usage':receipt['accepted_usage'],
                'receipt_sha256':envelope['receipt_sha256'],
                'invalid_code':None if choices[0].get('finish_reason')=='stop' else 'model_output_truncated'}
