"""Campaign evidence must survive failure without making inference replay safe."""
import json
from types import SimpleNamespace

import pytest

from ops.gpu_campaign import Journal, execute, write_json


def test_journal_keeps_unresolved_intents_visible_after_reopen(tmp_path):
    path = tmp_path / 'events.jsonl'
    journal = Journal(path)
    journal({'event': 'intent', 'custom_id': 'unknown-outcome'})
    journal.close()
    assert json.loads(path.read_text())['custom_id'] == 'unknown-outcome'
    with pytest.raises(FileExistsError):
        Journal(path)


def test_existing_or_remote_campaign_is_rejected_before_engine_access(tmp_path):
    out = tmp_path / 'existing'
    out.mkdir()
    (out / 'intent').write_text('preserve')
    args = SimpleNamespace(url='http://127.0.0.1:8300', out=out)
    with pytest.raises(FileExistsError):
        execute(args)
    assert (out / 'intent').read_text() == 'preserve'
    args.url = 'https://example.com'
    with pytest.raises(ValueError, match='Pod-local'):
        execute(args)


def test_atomic_report_preserves_previous_document_until_publication(tmp_path):
    path = tmp_path / 'state.json'
    write_json(path, {'state': 'intent'})
    # Simulate a crashed writer: a pre-existing temporary file must not be
    # silently consumed or overwrite the last published evidence.
    path.with_suffix('.json.tmp').write_text('partial')
    with pytest.raises(FileExistsError):
        write_json(path, {'state': 'complete'})
    assert json.loads(path.read_text()) == {'state': 'intent'}
