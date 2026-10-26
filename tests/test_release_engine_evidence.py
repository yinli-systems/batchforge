import base64,json
from pathlib import Path
import pytest
from ops import release_engine_evidence as r


def test_release_patch_preserves_secrets_retention_quality_source_and_worker_settings():
    raw=b'# keep\r\nDATABASE_URL=private-test-only\nQUALITY_EVIDENCE_CATALOGUE=/old/catalogue.json\nBATCH_SUBMISSION_ENABLED=true\nBLOB_GC_ENABLED=true\nRESULT_ARCHIVE_SECONDS=120\nUVICORN_APP_DIR=/old/source\n'
    values={'UVICORN_APP_DIR':'/new/source','FRONTEND_DIR':'/new/source/frontend','ENGINE_EVIDENCE_CATALOGUE':'/private/release/catalogue.json'}
    patched=r.patch(raw,values)
    assert patched.startswith(raw.replace(b'UVICORN_APP_DIR=/old/source\n',b'UVICORN_APP_DIR=/new/source\n'))
    assert b'BATCH_SUBMISSION_ENABLED=true\n' in patched
    assert patched.count(b'UVICORN_APP_DIR=')==1
    assert r.gated(patched).replace(b'BATCH_SUBMISSION_ENABLED=false',b'BATCH_SUBMISSION_ENABLED=true')==patched
    with pytest.raises(ValueError):r.patch(raw+b'UVICORN_APP_DIR=duplicate\n',values)
    with pytest.raises(ValueError):r.patch(raw,{**values,'FRONTEND_DIR':'/bad\nSECRET=x'})


class Platform:
    def __init__(self,quiet=True):self.events=[];self.safe=quiet
    def close_gates(self,**kw):self.events.append(('close',kw))
    def quiet(self,**kw):
        self.events.append(('quiet',kw))
        if not self.safe:raise RuntimeError('Unknown accepted work or inference still active')
        return [{'fresh':1},{'fresh':2}]
    def install(self,target,closed):self.events.append(('install',target,closed))
    def gates_closed(self):self.events.append(('gates_verified',))
    def verify(self,target):self.events.append(('verify',target));return [{'ready':True}]


@pytest.mark.parametrize('commit',[True,False])
def test_only_sole_owner_opens_after_two_fresh_drain_and_same_evidence_checks(commit):
    p=Platform();result=r.restore_outcome(p,commit=commit)
    assert p.events==[('close',{'rollback':not commit}),('quiet',{'seconds':180}),('install',commit,True),
        ('gates_verified',),('verify',commit),('install',commit,False),('verify',commit)]
    assert result['outcome']==('published' if commit else 'rolled_back')


def test_unknown_inflight_or_accepted_work_keeps_admission_closed_in_rollback():
    p=Platform(quiet=False)
    with pytest.raises(RuntimeError):r.restore_outcome(p,commit=False)
    assert p.events==[('close',{'rollback':True}),('quiet',{'seconds':180})]


def test_bad_candidate_replica_uses_known_original_gated_only_in_rollback(tmp_path,monkeypatch):
    env=tmp_path/'a.env';env.write_bytes(b'BATCH_SUBMISSION_ENABLED=true\nUVICORN_APP_DIR=/new\n');env.chmod(0o600)
    p=object.__new__(r.ReleasePlatform);api={'env_file':str(env)};p.apis=[api]
    p.original={env:b'BATCH_SUBMISSION_ENABLED=true\nUVICORN_APP_DIR=/old\n'}
    p.check=lambda:None;p.identity=lambda *a,**k:None;p.gates_closed=lambda:None
    changes=[];p.configure_bytes=lambda api,raw:changes.append(raw)
    p.close_gates(rollback=True)
    assert changes==[b'BATCH_SUBMISSION_ENABLED=false\nUVICORN_APP_DIR=/old\n']


def test_restore_loader_refuses_modified_frozen_source_and_private_config(tmp_path,monkeypatch):
    monkeypatch.setattr(r,'owned',lambda p:p)
    source=Path(r.__file__).resolve().parents[1]
    config={'protocol':r.PROTOCOL,'base':str(tmp_path),'operator_source':str(source),'operator_files':{'ops/release_engine_evidence.py':'0'*64}}
    path=tmp_path/'restore-private.json';r.save(path,config)
    with pytest.raises(ValueError,match='commitment'):r.load(path,'0'*64)
    with pytest.raises(ValueError,match='snapshot'):r.load(path,r.sha(path.read_bytes()))


def test_foreign_environment_drift_is_refused_before_any_manager_or_reopen(tmp_path,monkeypatch):
    manifest=tmp_path/'manifest';manifest.write_bytes(b'known');receipt=tmp_path/'receipt';receipt.write_bytes(b'receipt')
    env=tmp_path/'api.env';env.write_bytes(b'BATCH_SUBMISSION_ENABLED=true\nSECRET=foreign\n')
    for pth in (manifest,receipt,env):pth.chmod(0o600)
    p=object.__new__(r.ReleasePlatform)
    p.c={'manifest_path':str(manifest),'manifest_b64':base64.b64encode(b'known').decode(),'managers':{},
        'worker_receipt_path':str(receipt),'worker_receipt_sha256':r.sha(b'receipt'),'worker_source':str(tmp_path),
        'worker_files':{},'catalogue_files':{}}
    p.services=[{'role':'api','env_file':str(env)}]
    p.original={env:b'BATCH_SUBMISSION_ENABLED=true\nSECRET=original\n'};p.target={env:p.original[env]+b'NEW=target\n'}
    monkeypatch.setattr(r,'PROBED_SOURCES',[])
    with pytest.raises(ValueError,match='Foreign'):p.check()


def test_missing_or_live_controller_loss_proof_never_authorizes_publication(tmp_path,monkeypatch):
    monkeypatch.setattr(r,'owned',lambda p:p)
    r.save(tmp_path/'restore-private.json',{})
    monkeypatch.setattr(r,'load',lambda *a:{'operator_commit':'source','operator_files':{'x':'sha'},'controller':{'pid':12}})
    r.save(tmp_path/'recovery-validation.json',{'passed':True,'outcome':'rolled_back'})
    r.save(tmp_path/'dispatch-observation.json',{'intentional_controller_exit':42,'first_api_gate_verified':True})
    monkeypatch.setattr(r,'process_identity',lambda _: {'pid':12})
    with pytest.raises(ValueError):r.require_loss_qualification(tmp_path,{'operator_commit':'source','operator_files':{'x':'sha'}})
    monkeypatch.setattr(r,'process_identity',lambda _:None)
    assert r.require_loss_qualification(tmp_path,{'operator_commit':'source','operator_files':{'x':'sha'}})==r.sha((tmp_path/'recovery-validation.json').read_bytes())
