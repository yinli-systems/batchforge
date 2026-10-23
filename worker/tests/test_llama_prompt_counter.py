import json

import httpx
import pytest

from scripts.llama_prompt_counter import LlamaPromptCounter

MODEL = 'openai/gpt-oss-120b'
COMMIT = 'a' * 40
REVISION = 'b' * 40
MESSAGES = [{'role': 'user', 'content': 'public diagnostic'}]


def fixture_transport(*, model=MODEL, prompt_tokens=3, generated=5, cached=0,
                      finish='length', content='', token_ids=None):
    calls = []
    def handler(request):
        body = json.loads(request.content) if request.content else None
        calls.append((request.method, request.url.path, body))
        if request.url.path == '/v1/models':
            return httpx.Response(200, json={'data': [{'id': model}]})
        if request.url.path == '/props':
            return httpx.Response(200, json={'chat_template': 'actual-server-template',
                'default_generation_settings': {'n_ctx': 18688}, 'build_info': 'test'})
        if request.url.path == '/apply-template':
            assert body['model'] == MODEL
            assert body['messages'] == MESSAGES
            assert body['add_generation_prompt'] is True
            return httpx.Response(200, json={'prompt': 'actual server-rendered prompt'})
        if request.url.path == '/tokenize':
            assert body == {'content': 'actual server-rendered prompt',
                'add_special': False, 'parse_special': True}
            return httpx.Response(200, json={'tokens': token_ids if token_ids is not None else [7, 8, 9]})
        if request.url.path == '/v1/chat/completions':
            assert body['cache_prompt'] is False
            assert body['max_tokens'] == 5
            assert not {'min_tokens', 'ignore_eos'} & set(body)
            return httpx.Response(200, json={'model': MODEL,
                'usage': {'prompt_tokens': prompt_tokens, 'completion_tokens': generated,
                    'prompt_tokens_details': {'cached_tokens': cached}},
                'choices': [{'message': {'content': content, 'reasoning_content': 'observed reasoning'},
                    'finish_reason': finish}],
                'timings': {'prompt_ms': 20, 'predicted_ms': 30}})
        raise AssertionError('Unexpected endpoint')
    return httpx.MockTransport(handler), calls


def counter(transport):
    return LlamaPromptCounter('http://engine', model=MODEL, commit=COMMIT,
        revision=REVISION, transport=transport)


def test_counter_uses_live_formatter_and_tokenizer_without_inference():
    transport, calls = fixture_transport()
    with counter(transport) as c:
        assert c.calibrate(MESSAGES, 3)['matched'] is True
        assert c.context_capacity == 18688
        assert c.counter_metadata['llama_cpp_commit'] == COMMIT
        with pytest.raises(ValueError, match='differs from observed'):
            c.calibrate(MESSAGES, 4)
    assert all(path != '/v1/chat/completions' for _, path, _ in calls)
    assert [path for _, path, _ in calls[:4]] == ['/v1/models', '/props', '/apply-template', '/tokenize']


def test_catalog_mismatch_cannot_fallback_to_smaller_model():
    transport, _ = fixture_transport(model='Qwen/Qwen2.5-7B-Instruct')
    with pytest.raises(ValueError, match='exact model'):
        counter(transport)


@pytest.mark.parametrize('changes', [{'cached': 1}, {'prompt_tokens': 4}, {'generated': 6}])
def test_uncached_probe_rejects_observed_cache_or_usage_mismatch(changes):
    transport, _ = fixture_transport(**changes)
    with counter(transport) as c, pytest.raises(ValueError, match='uncached contract'):
        c.probe({'model': MODEL, 'messages': MESSAGES, 'max_tokens': 5}, admission_context=8)


def test_length_limited_reasoning_is_inference_not_a_final_answer():
    transport, calls = fixture_transport()
    with counter(transport) as c:
        report, raw = c.probe({'model': MODEL, 'messages': MESSAGES, 'max_tokens': 5},
            admission_context=8)
    assert report['passed'] is True
    assert report['natural_generation_mode'] is True
    assert report['natural_stop'] is False
    assert report['final_text_nonempty'] is False
    assert report['cached_prompt_tokens'] == 0
    assert 'observed reasoning' not in json.dumps(report)
    assert 'observed reasoning' in json.dumps(raw)
    assert sum(path == '/v1/chat/completions' for _, path, _ in calls) == 1


def test_worker_reservation_is_enforced_even_when_server_has_more_capacity():
    transport, calls = fixture_transport()
    with counter(transport) as c, pytest.raises(ValueError, match='reservation'):
        c.probe({'model': MODEL, 'messages': MESSAGES, 'max_tokens': 5}, admission_context=7)
    assert all(path != '/v1/chat/completions' for _, path, _ in calls)


def test_invalid_tokenizer_ids_are_rejected():
    transport, _ = fixture_transport(token_ids=[7, True, 9])
    with counter(transport) as c, pytest.raises(ValueError, match='integer token IDs'):
        c.apply_chat_template(MESSAGES)


@pytest.mark.parametrize('extra', [{'reasoning_effort': 'low'}, {'response_format': {'type': 'json_object'}}])
def test_formatter_overrides_fail_before_counting_or_inference(extra):
    transport, calls = fixture_transport()
    with counter(transport) as c, pytest.raises(ValueError, match='Unsupported probe keys'):
        c.probe({'model': MODEL, 'messages': MESSAGES, 'max_tokens': 5, **extra}, admission_context=8)
    assert all(path not in {'/apply-template', '/v1/chat/completions'} for _, path, _ in calls)


@pytest.mark.parametrize('changes', [{'finish': None}, {'finish': 'error'}, {'content': []}, {'content': 0}])
def test_failed_completion_or_invalid_visible_text_cannot_pass(changes):
    transport, _ = fixture_transport(**changes)
    with counter(transport) as c, pytest.raises(ValueError):
        c.probe({'model': MODEL, 'messages': MESSAGES, 'max_tokens': 5}, admission_context=8)


def test_cli_marker_survives_uncertain_outcome_and_refuses_duplicate(tmp_path):
    from scripts.llama_prompt_counter import start_probe_marker
    report = tmp_path / 'probe.json'
    marker = start_probe_marker(report, input_sha256='f' * 64)
    assert marker.stat().st_mode & 0o777 == 0o600
    assert json.loads(marker.read_text())['automatic_retry'] is False
    assert not report.exists()
    with pytest.raises(FileExistsError):
        start_probe_marker(report, input_sha256='f' * 64)


def test_server_timing_parser_preserves_computed_suffix_for_cache_detection():
    from scripts.llama_prompt_counter import parse_timing_pairs
    text = ('0.07 I slot print_timing: id  0 | task 35 | prompt eval time = 100.84 ms / 5 tokens (rate)\n'
            '0.08 I slot print_timing: id  0 | task 35 |        eval time = 955.76 ms / 39 tokens (rate)\n')
    assert parse_timing_pairs(text) == [{'task_id': 35, 'prefill_ms': 100.84,
        'prefill_tokens': 5, 'decode_ms': 955.76, 'decode_tokens': 39}]


def test_server_timing_parser_refuses_missing_or_duplicate_request_boundaries():
    from scripts.llama_prompt_counter import parse_timing_pairs
    prompt = 'slot print_timing: id 0 | task 7 | prompt eval time = 10 ms / 80 tokens\n'
    decode = 'slot print_timing: id 0 | task 7 | eval time = 20 ms / 32 tokens\n'
    with pytest.raises(ValueError, match='Incomplete'):
        parse_timing_pairs(prompt)
    with pytest.raises(ValueError, match='Duplicate'):
        parse_timing_pairs(prompt+decode+prompt)
