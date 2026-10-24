import json
import shlex

import pytest

from ops.ha.add_qwen235b_catalog import catalog_bytes
from ops.ha.switch_qwen235b import MODEL


def test_catalog_addition_preserves_all_other_bytes_and_existing_ids():
    source = b"# keep\nMODEL_NAME='old-default'\nMODEL_NAMES_JSON='[\"old1\",\"old2\"]'\nSCHEDULER_POLICY=cost_aware\nRESULT_READERS_ENABLED=true\nRESULT_ARCHIVE_ENABLED=false\nSECRET='literal $() spaces'\n"
    result = catalog_bytes(source)
    unchanged = lambda raw: [line for line in raw.splitlines(keepends=True) if not line.startswith(b'MODEL_NAMES_JSON=')]
    assert unchanged(result) == unchanged(source)
    assignment = next(line for line in result.decode().splitlines() if line.startswith('MODEL_NAMES_JSON='))
    assert json.loads(shlex.split(assignment.split('=', 1)[1])[0]) == ['old1', 'old2', MODEL]
    assert catalog_bytes(result) == result


def test_duplicate_catalog_or_non_list_refused():
    for raw in (b'MODEL_NAMES_JSON=[]\nMODEL_NAMES_JSON=[]\n', b'MODEL_NAMES_JSON={}\n'):
        with pytest.raises(ValueError):
            catalog_bytes(raw)
