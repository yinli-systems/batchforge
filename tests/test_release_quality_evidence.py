from pathlib import Path
import copy
import pytest
from ops.release_quality_evidence import require_closed_endurance,release_environment


def closed():
    return {'configuration':{'tasks':2400},'passed':True,'accepted_tasks':2400,'completed_tasks':2400,
            'unfinished_tasks':0,'accepted_exactly_once':True,'accepted_usage_matches_task_usage':True}


@pytest.mark.parametrize('change',[{'passed':False},{'completed_tasks':2399},{'unfinished_tasks':1},
    {'accepted_exactly_once':False},{'accepted_usage_matches_task_usage':False}])
def test_unfinished_or_failed_real_gpu_run_prevents_api_release(change):
    report=closed();report.update(change)
    with pytest.raises(RuntimeError):require_closed_endurance({'phase':'complete'},report)


def test_successful_report_still_requires_the_controller_to_close():
    with pytest.raises(RuntimeError):require_closed_endurance({'phase':'sustained_running'},closed())
    require_closed_endurance({'phase':'complete'},closed())


def test_release_enables_catalogue_without_changing_models_credentials_or_main_retention_policy():
    original={'MODEL_NAME':'existing-model','DATABASE_URL':'private-test-only','AWS_SECRET_ACCESS_KEY':'private-test-only',
              'BLOB_GC_ENABLED':'true','RESULT_ARCHIVE_ENABLED':'true','LEASE_SECONDS':'15'}
    saved=copy.deepcopy(original);source=Path('/approved/immutable/source')
    env=release_environment(original,source,paused=True)
    assert original==saved and env['MODEL_NAME']==original['MODEL_NAME'] and env['DATABASE_URL']==original['DATABASE_URL']
    assert env['QUALITY_EVIDENCE_CATALOGUE']==str(source/'deploy/quality/catalogue.json')
    assert env['BLOB_GC_ENABLED']=='true' and env['RESULT_ARCHIVE_ENABLED']=='true' and env['LEASE_SECONDS']=='15'
    assert env['BATCH_SUBMISSION_ENABLED']=='false'
    canary=release_environment(original,source,paused=True,canary=True)
    assert canary['BLOB_GC_ENABLED']=='false' and canary['RESULT_ARCHIVE_ENABLED']=='false'
