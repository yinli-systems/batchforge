import hashlib
import json
from pathlib import Path

import pytest

from ops.ha import profile_qwen235b as probe


def test_probe_keeps_only_frozen_public_inputs_and_exact_four_token_shapes(monkeypatch):
    rows, examples = [], {}
    for prompt in (128, 512, 2048, 8192, 16384):
        for output in (32, 128, 512, 2048):
            name = f'{prompt}-{output}'
            rows.append({'custom_id': name, 'body': {'model': probe.MODEL, 'max_tokens': output}})
            examples[name] = {'rendered_prompt_tokens': prompt, 'output_token_cap': output}
    payload = b''.join(json.dumps(row).encode() + b'\n' for row in rows)
    monkeypatch.setattr(probe, 'WORKLOAD20_SHA256', hashlib.sha256(payload).hexdigest())
    selected, manifest = probe.probe_workload(payload, {'examples': examples})
    assert len(selected.splitlines()) == 4
    assert sum(e['rendered_prompt_tokens'] for e in manifest['examples'].values()) == 26752
    assert sum(e['output_token_cap'] for e in manifest['examples'].values()) == 128
    assert hashlib.sha256(selected).hexdigest() == manifest['workload_sha256']
    with pytest.raises(ValueError, match='changed'):
        probe.probe_workload(payload + b' ', {'examples': examples})


def test_metrics_scope_filters_other_models_and_sums_finish_reasons():
    labels = f'model_name="{probe.MODEL}",engine="0"'
    payload = '\n'.join(f'{name}{{{labels}}} 0' for name in probe.COUNTERS[1:] + probe.GAUGES)
    payload += f'\nvllm:request_success_total{{{labels},finished_reason="stop"}} 3'
    payload += f'\nvllm:request_success_total{{{labels},finished_reason="length"}} 4'
    payload += '\nvllm:request_success_total{model_name="unrelated",engine="0"} 99\n'
    assert probe.engine_metrics(payload)['vllm:request_success_total'] == 7
    with pytest.raises(ValueError, match='absent'):
        probe.engine_metrics(payload.replace('vllm:num_requests_running', 'wrong_gauge'))


def test_rank_mapping_excludes_stale_unowned_pids_and_requires_all_four():
    members = [{'pid': 100 + r, 'start_ticks': str(r)} for r in range(4)]
    current = {m['pid']: m for m in members}
    logs = '\n'.join(f'(Worker_TP{r} pid={100+r}) ready' for r in range(4))
    logs += '\n(Worker_TP0 pid=999) old startup'
    ranks = probe.rank_mapping(logs, members, identity=current.get)
    assert {r: m['pid'] for r, m in ranks.items()} == {r: 100+r for r in range(4)}
    current.pop(102)
    with pytest.raises(ValueError, match='Four live'):
        probe.rank_mapping(logs, members, identity=current.get)


def test_trace_files_must_map_once_to_each_verified_rank_pid():
    ranks = {r: {'pid': 100+r} for r in range(4)}
    paths = {Path(f'host_{100+r}.12345.pt.trace.json.gz') for r in range(4)}
    assert set(probe.map_traces(paths, ranks)) == set(range(4))
    for invalid in (paths | {Path('host_999.12345.pt.trace.json.gz')},
                    paths | {Path('host_100.99999.pt.trace.json.gz')},
                    paths - {Path('host_100.12345.pt.trace.json.gz')}):
        with pytest.raises(ValueError):
            probe.map_traces(invalid, ranks)
