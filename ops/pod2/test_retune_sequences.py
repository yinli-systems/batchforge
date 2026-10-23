"""Regression checks for tuning evidence parsing and secret-preserving edits."""
import pytest

from ops.pod2.retune_sequences import kv_capacity, replace_env


def test_only_appended_startup_kv_capacity_counts_and_commas_are_supported():
    assert kv_capacity('INFO GPU KV cache size: 91,520 tokens\n') == 91520
    assert kv_capacity('GPU KV cache size: 91,520 tokens\nGPU KV cache size: 62,000 tokens') == 62000
    with pytest.raises(RuntimeError, match='Fresh startup'):
        kv_capacity('GPU KV cache memory: 5GiB')


def test_tuning_edits_only_named_assignments_and_preserves_private_values():
    raw = b"# private config\nexport WORKER_MAX_INFLIGHT='8'\nBATCHFORGE_WORKER_TOKEN='a secret literal'\nWORKER_TOKEN_BUDGET=32768\n"
    edited = replace_env(raw, {'WORKER_MAX_INFLIGHT': '16', 'WORKER_TOKEN_BUDGET': '65536'})
    assert b"BATCHFORGE_WORKER_TOKEN='a secret literal'" in edited
    assert b"WORKER_MAX_INFLIGHT=16\n" in edited
    assert b"WORKER_TOKEN_BUDGET=65536\n" in edited
    assert edited.count(b'WORKER_MAX_INFLIGHT') == 1
    assert b'32768' not in edited
