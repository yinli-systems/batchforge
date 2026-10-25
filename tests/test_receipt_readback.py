import hashlib
import httpx
import pytest
from scripts.verify_receipt import check_artifact

FID='file_'+'a'*32


def artifact(content):
    return {'id':FID,'bytes':len(content),'sha256':hashlib.sha256(content).hexdigest()}


def test_readback_checks_actual_bytes_and_lengths_without_logging_content():
    secret=b'private response, never recorded'
    transport=httpx.MockTransport(lambda r:httpx.Response(200,content=secret))
    with httpx.Client(base_url='http://local',transport=transport) as client:
        result=check_artifact(client,artifact(secret),max_bytes=100)
        assert result['passed'] and secret.decode() not in str(result)
        assert not check_artifact(client,artifact(b'corrupt'),max_bytes=100)['passed']
        with pytest.raises(ValueError,match='exceeds readback bound'):
            check_artifact(client,artifact(b'small'),max_bytes=10)


def test_expired_artifacts_or_unpublished_hashes_do_not_certify_persistence():
    with httpx.Client(base_url='http://local',transport=httpx.MockTransport(lambda r:httpx.Response(410))) as client:
        result=check_artifact(client,artifact(b'content'),max_bytes=100)
        assert not result['passed'] and result['state']=='unavailable'
        assert check_artifact(client,{'id':FID,'bytes':1},max_bytes=100)['state']=='hash_not_published'
