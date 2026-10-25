import copy
import hashlib
from types import SimpleNamespace

import pytest

from ops.promote480 import Promotion, qualified, verify_file, REPO, REVISION


def qualification():
    phases=[]
    for repeat in range(3):
        for treatment in 'ABBA':
            phases.append({'repeat':repeat,'treatment':treatment,'study':'kv','policy':'fair',
                'slots':32 if treatment=='B' else 8,
                'adaptive':treatment=='B','audit':{'tasks':60,'one_accepted_attempt_per_task':True,
                'batch_task_attempt_metering_match':True}})
    return {'passed':True,'finished_at':'complete','model':REPO,'revision':REVISION,'workload_repeats':3,
        'phases':phases,'plan':phases,'faults':[{'scope':scope,'adaptive':True,'stale_heartbeat_status':409,
        'stale_completion_status':409,'metering':{'tasks':60}} for scope in ['worker_process_loss','engine_and_worker_process_loss']],
        'persistence':{'verified_files_after_api_restart':28,'s3_input_objects':14,
                       'archived_batches':14,'s3_https_readback_verified':True}}, {
        'phase':'platform_canary_passed','finished_at':'complete','restore_failures':[]}


def test_partial_failed_or_static_only_recovery_cannot_promote():
    r,t=qualification();qualified(r,t)
    for mutate in [lambda r:r.update(finished_at=None),lambda r:r.update(cleanup_error_type='error'),
                   lambda r:r['phases'].pop(),lambda r:r['faults'][0].update(adaptive=False),
                   lambda r:r['phases'][1]['audit'].update(one_accepted_attempt_per_task=False),
                   lambda r:r['persistence'].update(verified_files_after_api_restart=26)]:
        bad=copy.deepcopy(r);mutate(bad)
        with pytest.raises(RuntimeError):qualified(bad,t)
    with pytest.raises(RuntimeError):qualified(r,{**t,'restore_failures':['timeout']})


def test_rollout_rechecks_actual_checkpoint_bytes_not_only_download_receipt(tmp_path):
    data=b'known immutable tensor fixture';(tmp_path/'weights').write_bytes(data)
    receipt={'file':'weights','bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()}
    assert verify_file(tmp_path,receipt)==len(data)
    (tmp_path/'weights').write_bytes(b'x'*len(data))
    with pytest.raises(RuntimeError,match='hash_changed'):verify_file(tmp_path,receipt)
    with pytest.raises(RuntimeError,match='unsafe_checkpoint'):verify_file(tmp_path,{**receipt,'file':'../outside'})


def test_rollback_cannot_stop_the_new_engine_while_accepted_480b_work_remains():
    roll=object.__new__(Promotion);calls=[]
    roll.gpu_swapped=True
    roll.apis_config=lambda **kwargs:calls.append(('apis',kwargs))
    roll.assert_admission_closed=lambda:None
    roll.active=lambda model:1
    def wait(predicate,seconds,reason):
        assert not predicate();raise RuntimeError(reason)
    roll.wait=wait
    roll.invoke=lambda service,action:calls.append((service,action))
    with pytest.raises(RuntimeError,match='must_drain'):roll.rollback()
    assert len(calls)==1 and calls[0][0]=='apis' and calls[0][1]['enabled'] is False


def test_rollback_restores_old_catalog_while_admission_stays_closed_before_worker_registers():
    roll=object.__new__(Promotion);calls=[]
    roll.gpu_swapped=True;roll.old_model='235b'
    roll.watchdog='watchdog';roll.worker='new-worker';roll.engine='new-engine'
    roll.old_worker='old-worker';roll.old_engine='old-engine';roll.manifest='manifest';roll.original={}
    roll.report={};roll.active=lambda model:0
    roll.apis_config=lambda **kw:calls.append(('apis',kw))
    roll.assert_admission_closed=lambda:None
    roll.wait=lambda predicate,*args:predicate()
    roll.invoke=lambda service,action:(calls.append((service,action)) or {'running':True})
    roll.private_write=lambda *args:None
    roll.ready=lambda *args:None;roll.event=lambda *args:None
    roll.rollback()
    closed_catalog=('apis',{'enabled':False,'model':'235b'})
    assert calls.index(closed_catalog)<calls.index(('old-worker','start'))
    assert calls.index(('new-engine','stop'))<calls.index(('old-worker','start'))
    assert calls[-2][0]=='apis' and calls[-2][1]['restore'] is True


def test_process_only_watchdog_readiness_uses_owned_identity_not_a_missing_http_port():
    roll=object.__new__(Promotion)
    roll.invoke=lambda *args:{'running':True}
    roll.manager=SimpleNamespace(readiness=lambda service:pytest.fail('watchdog has no HTTP port'))
    roll.wait=lambda predicate,*args:predicate()
    roll.ready({'role':'supervisor','port':None})
    roll.invoke=lambda *args:{'running':False}
    with pytest.raises(RuntimeError,match='exited_during_start'):
        roll.ready({'role':'engine','port':8348})
