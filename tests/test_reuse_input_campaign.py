import hashlib
import json
import pytest
from ops import reuse_input_campaign as campaign


def test_only_pinned_public_mixture_can_be_reused(monkeypatch):
    rows=[{'custom_id':str(i),'method':'POST','url':'/v1/chat/completions',
        'body':{'model':'test','messages':[{'role':'user','content':str(i)}],'max_tokens':32}}
        for i in range(20)]
    fixture=b'\n'.join(campaign.render(r) for r in rows)+b'\n'
    monkeypatch.setattr(campaign,'FIXTURE_SHA256',hashlib.sha256(fixture).hexdigest())
    for row in rows:row['body']['min_tokens']=32
    data=b'\n'.join(campaign.render(r) for r in reversed(rows))+b'\n'
    assert campaign.verify_public_mixture(data,fixture)==640
    with pytest.raises(RuntimeError):campaign.verify_public_mixture(data,fixture+b' ')
    rows[0]['body']['messages'][0]['content']='different'
    with pytest.raises(RuntimeError):campaign.verify_public_mixture(b'\n'.join(campaign.render(r) for r in rows),fixture)


@pytest.mark.parametrize('state',[{'phase':'failed'},{'phase':'cleanup_failed'},
    {'phase':'complete','passed':True,'primary_published':True,'observed_gpu_identities_unchanged':False}])
def test_failed_or_unverified_release_prevents_gpu_submission(state):
    with pytest.raises(RuntimeError):campaign.ready_release(state)


def test_waits_for_cleanup_before_beginning_the_next_workload():
    state={'passed':True,'primary_published':True,'observed_gpu_identities_unchanged':True}
    assert campaign.ready_release({**state,'phase':'primary_published'}) is False
    assert campaign.ready_release({**state,'phase':'complete'}) is True
