from pathlib import Path
import pytest
from ops.qualify_agent_schema import canary_environment


def test_private_ingress_cannot_bypass_primary_drain_or_change_data_policies():
    source=Path('/root/candidate/source')
    original={'BATCH_SUBMISSION_ENABLED':'false'}
    with pytest.raises(ValueError):canary_environment(original,source)
    original={'DATABASE_URL':'test-dsn','MODEL_NAME':'test-model','INPUT_RETENTION_SECONDS':'86400',
        'RESULT_ARCHIVE_MIN_AGE_SECONDS':'120','RETENTION_ADMISSION_ENABLED':'true'}
    env=canary_environment(original,source)
    assert all(env[k]==v for k,v in original.items())
    assert env['JSON_SCHEMA_ENABLED']=='true' and env['PYTHONPATH']==str(source)
    assert env['PYTHONSAFEPATH']=='1'
    assert env['BLOB_GC_ENABLED']==env['RESULT_ARCHIVE_ENABLED']=='false'
    assert 'JSON_SCHEMA_ENABLED' not in original
