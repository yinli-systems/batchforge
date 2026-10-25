"""Read-only child-ledger lookup, scoped by the caller's committed Agent run."""
from urllib.parse import urlsplit
import httpx
from backend.receipts import verify
from .contracts import protocol_json
from .store import AgentError


def endpoint(value):
    p=urlsplit(value)
    if (p.scheme not in ('http','https') or p.username or p.password or p.query or p.fragment
        or p.path not in ('','/') or (p.scheme=='http' and p.hostname not in ('127.0.0.1','localhost','::1'))
        or not p.hostname):
        raise ValueError('Operator-pinned HTTPS or loopback Batch endpoint required')
    return value.rstrip('/')


class HTTPReceipts:
    def __init__(self,url,keys):self.url=endpoint(url);self.keys=keys
    async def __aenter__(self):
        self.client=httpx.AsyncClient(timeout=httpx.Timeout(10,connect=3),trust_env=False,follow_redirects=False)
        return self
    async def __aexit__(self,*_):await self.client.aclose()
    async def read(self,tenant,bid):
        key=self.keys.get(tenant)
        if not key:raise AgentError(503,'tenant_executor_not_configured')
        try:
            async with self.client.stream('GET',self.url+'/v1/batches/'+bid+'/receipt',headers={'Authorization':'Bearer '+key}) as r:
                if r.status_code!=200:raise AgentError(503,'agent_child_receipt_unavailable')
                raw=bytearray()
                async for part in r.aiter_bytes():
                    raw.extend(part)
                    if len(raw)>262144:raise AgentError(503,'agent_child_receipt_unavailable')
            return protocol_json(raw)
        except (httpx.HTTPError,ValueError):raise AgentError(503,'agent_child_receipt_unavailable') from None


def reconcile(run,committed,envelope):
    receipt=envelope.get('receipt',{}) if isinstance(envelope,dict) else {}
    try:checksum_valid=isinstance(receipt,dict) and verify(envelope)
    except (ValueError,TypeError,KeyError,AttributeError):checksum_valid=False
    if (not checksum_valid or receipt.get('batch_id')!=committed['batch_id'] or receipt.get('model')!=run['model']
        or receipt.get('ledger_state')!='consistent_ledger' or not isinstance(receipt.get('counts'),dict) or receipt.get('counts',{}).get('total')!=1
        or receipt.get('counts',{}).get('completed')!=1 or not isinstance(receipt.get('checks'),dict) or not receipt.get('checks')
        or not all(value is True for value in receipt['checks'].values()) or receipt.get('accepted_usage')!=committed['usage']):
        raise AgentError(409,'agent_child_ledger_drift')
    return {'protocol':'batchforge.agent-child-receipt.v1','run_id':run['id'],'batch_id':committed['batch_id'],
        'committed_receipt_sha256':committed['receipt_sha256'],'accepted_usage_reconciled':True,'current':envelope,
        'scope':'Current Batch ledger matches the committed Agent child identity/model/usage; archive metadata may change the full receipt hash. Checksums are not signatures.'}
