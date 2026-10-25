import hashlib
import json
from types import SimpleNamespace

import httpx
import pytest

from scripts.engine_matrix import dispatch_order, run
from scripts.gptoss_prompt_counter import GptOssHarmonyCounter
from scripts.token_workload import build, fit_messages


class CharacterTokenizer:
    """Test double with template overhead, not an actual model tokenizer."""
    chat_template = "test-only-character-template"
    def apply_chat_template(self, messages, **kwargs):
        return [0] * (len(messages[0]["content"]) + 3)


def test_token_fitter_verifies_template_and_rejects_insufficient_source():
    messages, count = fit_messages(CharacterTokenizer(), "public text " * 500, 128, "mix-0")
    assert count == 128
    assert len(messages[0]["content"]) + 3 == 128
    with pytest.raises(ValueError, match="Insufficient source"):
        fit_messages(CharacterTokenizer(), "x", 500, "mix-0")


def test_dispatch_policies_change_order_and_preserve_every_request():
    rows = [{"custom_id": key} for key in "abcd"]
    examples = {"a": {"rendered_prompt_tokens": 16384, "output_token_cap": 32, "class": "prefill-heavy"},
        "b": {"rendered_prompt_tokens": 128, "output_token_cap": 2048, "class": "decode-heavy"},
        "c": {"rendered_prompt_tokens": 128, "output_token_cap": 32, "class": "balanced"},
        "d": {"rendered_prompt_tokens": 8192, "output_token_cap": 32, "class": "prefill-heavy"}}
    assert [r["custom_id"] for r in dispatch_order(rows, examples, "fifo")] == list("abcd")
    assert [r["custom_id"] for r in dispatch_order(rows, examples, "shortest")] == list("cdab")
    assert [r["custom_id"] for r in dispatch_order(rows, examples, "class-fair")] == list("cbad")


def arguments(tmp_path):
    row = {"custom_id": "a", "body": {"model": "test-model", "max_tokens": 32,
        "messages": [{"role": "user", "content": "test"}]}}
    payload = (json.dumps(row) + "\n").encode()
    workload, manifest = tmp_path/"work.jsonl", tmp_path/"manifest.json"
    workload.write_bytes(payload)
    manifest.write_text(json.dumps({"model": "test-model", "tokenizer_revision": "test-only",
        "workload_sha256": hashlib.sha256(payload).hexdigest(), "examples": {"a": {
            "rendered_prompt_tokens": 128, "output_token_cap": 32, "class": "balanced"}}}))
    return SimpleNamespace(workload=workload, manifest=manifest, policy="fifo", concurrency=1,
        context_limit=256, request_timeout=1, force_length=False, hourly_cost=2,
        url="http://test-engine", engine_label="test-only")


def transport(*, tokens=128, done=True, fail_http=False, omit_usage=False, deltas=None, finish="stop"):
    async def handler(request):
        if request.url.path == "/v1/models":
            return httpx.Response(200, json={"data": [{"id": "test-model", "max_model_len": 256}]})
        assert json.loads(request.content)["stream_options"]["include_usage"] is True
        if fail_http:
            return httpx.Response(503, text="private engine diagnostics must not appear")
        events = [{"choices": [{"index": 0, "delta": {"role": "assistant"}, "finish_reason": None}]}]
        for delta in deltas if deltas is not None else [{"content": "Hello"}, {"content": " world"}]:
            events.append({"choices": [{"index": 0, "delta": delta, "finish_reason": None}]})
        events[-1]["choices"][0]["finish_reason"] = finish
        if not omit_usage:
            events.append({"choices": [], "usage": {"prompt_tokens": tokens, "completion_tokens": 2}})
        body = ": keepalive\n\n" + "".join("data: " + json.dumps(e) + "\n\n" for e in events)
        if done:
            body += "data: [DONE]\n\n"
        return httpx.Response(200, text=body)
    return httpx.MockTransport(handler)


@pytest.mark.asyncio
async def test_stream_measures_real_usage_and_percentiles_without_treating_role_as_first_token(tmp_path):
    report = await run(arguments(tmp_path), transport=transport())
    assert report["passed"] is True
    assert report["summary"]["prompt_tokens"] == 128
    assert report["summary"]["generated_tokens"] == 2
    assert report["summary"]["completion_seconds"]["p99"] is not None
    assert report["records"][0]["emitted_text_events"] == 2
    assert report["summary"]["cost"]["window_usd"] > 0
    assert report["controlled_fixed_output"] is False
    assert report["schema_version"] == 3
    assert report["summary"]["final_text_present"] == 1
    assert report["summary"]["no_final_text"] == 0
    assert report["records"][0]["inference_success"] is True
    assert report["records"][0]["final_text_codepoints"] == len("Hello world")
    assert "mean_decode_token_seconds" not in report["summary"]
    assert "accepted_tasks_per_second" not in report["summary"]
    assert report["summary"]["successful_inferences_per_second"] > 0


@pytest.mark.asyncio
@pytest.mark.parametrize("options", [{"tokens": 127}, {"done": False}, {"omit_usage": True}, {"fail_http": True}])
async def test_mismatch_truncation_missing_usage_and_http_failure_are_not_counted_success(tmp_path, options):
    report = await run(arguments(tmp_path), transport=transport(**options))
    assert report["passed"] is False
    assert report["summary"]["completed"] == report["summary"]["generated_tokens"] == 0
    assert report["summary"]["errors"] == 1
    assert report["summary"]["completion_seconds"]["samples"] == 0
    assert report["summary"]["all_terminal_completion_seconds"]["samples"] == 1
    assert "private engine diagnostics" not in json.dumps(report)


@pytest.mark.asyncio
async def test_fixed_output_experiment_cannot_claim_cap_as_measured_output(tmp_path):
    args = arguments(tmp_path)
    args.force_length = True
    report = await run(args, transport=transport())
    assert report["passed"] is False  # 2 generated is not the requested 32.


@pytest.mark.asyncio
async def test_hash_and_context_gates_fail_before_inference(tmp_path):
    args = arguments(tmp_path)
    args.context_limit = 159
    with pytest.raises(ValueError, match="context limit"):
        await run(args, transport=transport())
    args.workload.write_text("changed")
    with pytest.raises(ValueError, match="hash differs"):
        await run(args, transport=transport())


@pytest.mark.asyncio
async def test_durable_intent_precedes_inference_and_http_failure_has_a_terminal(tmp_path):
    events = []
    requests = []
    async def handler(request):
        if request.url.path == '/v1/models':
            return httpx.Response(200, json={'data': [{'id': 'test-model', 'max_model_len': 256}]})
        assert events == [{'event': 'intent', 'custom_id': 'a', 'dispatch_index': 0}]
        requests.append(request)
        return httpx.Response(503)
    report = await run(arguments(tmp_path), transport=httpx.MockTransport(handler), event_sink=events.append)
    assert len(requests) == 1  # Uncertain/failed inference is not retried.
    assert not report['passed']
    assert events[-1]['event'] == 'terminal'
    assert events[-1]['record']['status'] == 'error'


@pytest.mark.asyncio
async def test_failed_intent_fsync_prevents_inference(tmp_path):
    def unavailable(event):
        raise OSError('test disk full')
    async def handler(request):
        assert request.url.path == '/v1/models', 'Inference was sent before a durable intent'
        return httpx.Response(200, json={'data': [{'id': 'test-model', 'max_model_len': 256}]})
    with pytest.raises(OSError, match='test disk full'):
        await run(arguments(tmp_path), transport=httpx.MockTransport(handler), event_sink=unavailable)


@pytest.mark.asyncio
async def test_harmony_reasoning_only_is_inference_success_without_a_final_answer(tmp_path):
    report = await run(arguments(tmp_path), transport=transport(
        deltas=[{"reasoning_content": "Think through this"}, {"reasoning_content": "."}], finish="length"))
    row = report["records"][0]
    assert report["passed"] and row["inference_success"]
    assert row["final_text_present"] is False
    assert row["first_final_content_seconds"] is None
    assert row["final_text_codepoints"] == 0
    assert row["reasoning_text_codepoints"] == len("Think through this.")
    assert report["summary"]["no_final_text"] == 1
    assert report["summary"]["first_final_content_seconds"]["samples"] == 0
    assert "quality" in report["passed_scope"]


@pytest.mark.asyncio
async def test_first_final_content_is_separate_from_reasoning_and_aliases_are_not_doubled(tmp_path):
    report = await run(arguments(tmp_path), transport=transport(deltas=[
        {"reasoning_content": "Thinking", "reasoning": "Thinking"}, {"content": "答案"}]))
    row = report["records"][0]
    assert row["first_final_content_seconds"] >= row["ttft_seconds"]
    assert row["final_text_present"] is True
    assert row["reasoning_text_codepoints"] == len("Thinking")
    assert row["final_text_codepoints"] == 2
    assert row["visible_text_codepoints"] == len("Thinking") + 2
    assert row["completion_tokens"] == 2  # Engine usage, not text events/codepoints.
    assert "stream_span_per_reported_output_token_proxy_seconds" in row


@pytest.mark.asyncio
async def test_conflicting_reasoning_aliases_are_explicit_stream_errors(tmp_path):
    report = await run(arguments(tmp_path), transport=transport(deltas=[
        {"reasoning_content": "one", "reasoning": "different"}]))
    assert not report["passed"]
    assert report["records"][0]["inference_success"] is False


def test_harmony_counter_calls_the_same_default_system_developer_and_message_sequence():
    counter = object.__new__(GptOssHarmonyCounter)
    counter.render_date = "2026-10-24"
    calls = []
    def system(**kwargs):
        calls.append(("system", kwargs))
        return "system"
    def developer(**kwargs):
        calls.append(("developer", kwargs))
        return "empty-developer"
    def parse(message):
        calls.append(("parse", message))
        return ["user"]
    def render(messages):
        calls.append(("render", messages))
        return [7, 11, 13]
    counter.harmony = SimpleNamespace(get_system_message=system, get_developer_message=developer,
        parse_chat_input=parse, render_for_completion=render)
    messages = [{"role": "user", "content": "public input"}]
    assert counter.apply_chat_template(messages) == [7, 11, 13]
    assert calls == [("system", {"reasoning_effort": None, "browser_description": None,
        "python_description": None, "start_date": "2026-10-24"}),
        ("developer", {"tools": None}), ("parse", messages[0]),
        ("render", ["system", "empty-developer", "user"])]
    with pytest.raises(ValueError, match="plain text"):
        counter.apply_chat_template([{"role": "user", "content": [], "tools": []}])


def test_harmony_counter_requires_cpu_only_and_pins_installed_formatter_sources(tmp_path, monkeypatch):
    import scripts.gptoss_prompt_counter as module

    monkeypatch.delenv("CUDA_VISIBLE_DEVICES", raising=False)
    with pytest.raises(ValueError, match="Hide GPUs"):
        GptOssHarmonyCounter()
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "")
    monkeypatch.setattr(module.metadata, "version", lambda name: {"vllm": "0.10.2", "openai-harmony": "test-harmony"}[name])
    harmony, serving = tmp_path/"harmony.py", tmp_path/"serving.py"
    harmony.write_text("official helper source placeholder")
    serving.write_text("official server source placeholder")
    monkeypatch.setattr(module.importlib, "import_module", lambda _: SimpleNamespace(__file__=str(harmony)))
    monkeypatch.setattr(module.util, "find_spec", lambda _: SimpleNamespace(origin=str(serving)))
    counter = GptOssHarmonyCounter(render_date="2026-10-24")
    assert counter.counter_metadata["harmony_utils_sha256"] == hashlib.sha256(harmony.read_bytes()).hexdigest()
    assert counter.counter_metadata["serving_chat_sha256"] == hashlib.sha256(serving.read_bytes()).hexdigest()
    assert counter.counter_metadata["render_date"] == "2026-10-24"
    monkeypatch.setattr(module.metadata, "version", lambda _: "0.11.0")
    with pytest.raises(ValueError, match="0.10.2"):
        GptOssHarmonyCounter()


def test_workload_preserves_exact_counter_provenance_and_rendered_token_digest(tmp_path):
    dataset = tmp_path/"public.json"
    dataset.write_text(json.dumps({"data": [{"paragraphs": [{"context": "public text " * 100}]}]}))
    tokenizer = CharacterTokenizer()
    payload, old = build(dataset, tokenizer, model="test-model", revision="test-only",
        inputs=[128], outputs=[32], repeats=1, seed=1, context_limit=256)
    tokenizer.counter_metadata = {"type": "test-harmony-counter", "render_date": "2026-10-24"}
    second_payload, manifest = build(dataset, tokenizer, model="test-model", revision="test-only",
        inputs=[128], outputs=[32], repeats=1, seed=1, context_limit=256)
    assert second_payload == payload  # Metadata does not alter existing request bodies.
    assert old["prompt_counter"]["type"] == "huggingface-chat-template"
    assert manifest["prompt_counter"] == tokenizer.counter_metadata
    assert manifest["chat_template_sha256"] is None  # Never label Harmony as a HF template hash.
    example = next(iter(manifest["examples"].values()))
    assert example["rendered_prompt_token_ids_sha256"] == hashlib.sha256(json.dumps(
        [0] * 128, separators=(",", ":")).encode()).hexdigest()
