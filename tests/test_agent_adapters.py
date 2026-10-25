import hashlib,json
from pathlib import Path
import httpx,pytest
from agents.batch import BatchAdapter
from agents.contracts import digest
from agents.memory import excerpt
from agents.runtime import RetryLater,DeliveryError
from agents.__main__ import credentials


def test_memory_excerpt_is_bounded_even_for_json_escaped_control_bytes():
    row={'namespace':'workspace','key':'facts','version':3,'sha256':'b'*64,'text':'\x00'*32768}
    result=excerpt(row)
    assert result['text_truncated'] and result['sha256']==row['sha256']
    assert len(json.dumps(result).encode())<=12000 and result['citation'].endswith('b'*64)


@pytest.mark.parametrize('url',['http://example.com','https://user:secret@example.com','http://127.0.0.1?token=x','file:///tmp/x','https://example.com/path'])
def test_batch_endpoint_not_model_controlled_or_credential_bearing(url):
    with pytest.raises(ValueError):BatchAdapter(None,base_url=url,tenant_keys={'tenant':'secret'})


def test_executor_registry_rejects_readable_or_symlinked_credentials(tmp_path):
    p=tmp_path/'registry.json';p.write_text('{"tenant":"test-only-key-at-least16"}');p.chmod(0o600)
    assert list(credentials(p))==['tenant']
    link=tmp_path/'link';link.symlink_to(p)
    with pytest.raises(ValueError):credentials(link)
    p.chmod(0o644)
    with pytest.raises(ValueError):credentials(p)
    p.chmod(0o600);p.write_text('{"tenant":"test-only-valid-key-1111","tenant":"test-only-valid-key-2222"}')
    with pytest.raises(ValueError):credentials(p)


async def test_adapter_uses_identical_idempotency_key_and_never_logs_tenant_key():
    seen=[]
    def handler(request):
        seen.append(request)
        if len(seen)==1:return httpx.Response(503)
        return httpx.Response(200,json={'id':'batch_one'})
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        adapter=BatchAdapter(None,base_url='http://127.0.0.1:8080',tenant_keys={'tenant':'private-key'},client=client)
        run={'id':'agrun_test','tenant_id':'tenant'}
        state={'file_id':'file_one','child_key':'agent.agrun_test.1','step':1,'request_sha256':'a'*64}
        with pytest.raises(RetryLater):await adapter.create(run,state)
        assert await adapter.create(run,state)=='batch_one'
    assert len(seen)==2 and seen[0].content==seen[1].content
    assert all(r.headers['Idempotency-Key']==state['child_key'] for r in seen)
    assert all(r.headers['Authorization']=='Bearer private-key' for r in seen)


def test_input_intent_hash_cannot_change_on_resume():
    request={'model':'test-model','messages':[]}
    state={'request':request,'request_sha256':digest(request),'step':1}
    raw=BatchAdapter.input_bytes({'id':'run'},state)
    assert json.loads(raw)['custom_id']=='agent-step-1'
    request['model']='different'
    with pytest.raises(DeliveryError):BatchAdapter.input_bytes({'id':'run'},state)
