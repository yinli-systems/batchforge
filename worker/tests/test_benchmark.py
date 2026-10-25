import asyncio
from email.parser import BytesParser
from email.policy import default
import hashlib
import json
from types import SimpleNamespace

import httpx
import pytest

from scripts.benchmark import distribution, run


def arguments(tmp_path):
    return SimpleNamespace(tasks=4, batches=2, model="test-model", prompt="Classify this example.",
        max_tokens=8, temperature=.25, url="http://test-control", engine_label="test-only",
        deadline=1, request_timeout=1, poll=.001, workload_dir=tmp_path)


def fixture_transport(*, fail_second_create=False, reasoning_only=False, malformed_message=False):
    uploaded, batches = {}, {}
    fast_terminal = asyncio.Event()

    def batch(file_id, terminal=False):
        count = len(uploaded[file_id])
        return {"id": "batch-" + file_id, "status": "completed" if terminal else "in_progress",
            "request_counts": {"total": count, "pending": 0 if terminal else count, "running": 0,
                "completed": count if terminal else 0, "failed": 0, "cancelled": 0},
            "output_file_id": "output-" + file_id if terminal else None}

    async def handle(request):
        path = request.url.path
        if path == "/v1/files" and request.method == "POST":
            message = BytesParser(policy=default).parsebytes(b"Content-Type: " + request.headers["content-type"].encode() + b"\r\n\r\n" + request.content)
            part = next(item for item in message.iter_parts() if item.get_param("name", header="content-disposition") == "file")
            file_id = part.get_filename().removesuffix(".jsonl")
            uploaded[file_id] = [json.loads(line) for line in part.get_payload(decode=True).splitlines()]
            return httpx.Response(200, json={"id": file_id})
        if path == "/v1/batches":
            file_id = json.loads(request.content)["input_file_id"]
            if fail_second_create and file_id.endswith("-1"):
                # A successful batch must already be monitored while a separate
                # create has not yet returned. No timing thresholds needed.
                await asyncio.wait_for(fast_terminal.wait(), .5)
                return httpx.Response(500)
            batches["batch-" + file_id] = file_id
            return httpx.Response(200, json=batch(file_id))
        if path.startswith("/v1/batches/"):
            file_id = batches[path.rsplit("/", 1)[-1]]
            fast_terminal.set()
            return httpx.Response(200, json=batch(file_id, terminal=True))
        if path.startswith("/v1/files/output-"):
            file_id = path.split("/")[3].removeprefix("output-")
            rows = [{"custom_id": row["custom_id"], "response": {"body": {"batchforge_test_only": True,
                "choices": [{"index": 0, "message": "invalid" if malformed_message else {
                    "role": "assistant", "content": None if reasoning_only else "final answer",
                    "reasoning_content": "reasoning without final answer" if reasoning_only else None},
                    "finish_reason": "length" if reasoning_only else "stop"}],
                "usage": {"prompt_tokens": 2, "completion_tokens": 3}}}} for row in uploaded[file_id]]
            return httpx.Response(200, text="\n".join(json.dumps(row) for row in rows) + "\n")
        raise AssertionError(str(request.url))

    return httpx.MockTransport(handle)


@pytest.mark.asyncio
async def test_measured_report_fingerprints_actual_payload_and_records_config(monkeypatch, tmp_path):
    monkeypatch.setenv("BATCHFORGE_API_KEY", "test-only-key")
    report = await run(arguments(tmp_path), transport=fixture_transport())
    actual_bytes = b"".join(path.read_bytes() for path in sorted(tmp_path.glob("*.jsonl")))
    assert report["passed"] is True
    assert report["workload"]["sha256"] == hashlib.sha256(actual_bytes).hexdigest()
    assert report["workload"]["temperature"] == .25
    assert report["workload"]["prompt"] == "Classify this example."
    assert report["workload"]["max_tokens"] == 8
    assert report["submission"]["attempted_http_requests"] == 4
    assert report["submission"]["successful_http_requests"] == 4
    assert report["submission"]["upload_latency_seconds"]["samples"] == 2
    assert report["submission"]["create_latency_seconds"]["samples"] == 2
    assert report["batch_turnaround_seconds"]["samples"] == 2
    assert report["observed_prompt_tokens"] == 8
    assert report["observed_completion_tokens"] == 12
    assert report["test_fixture_detected"] is True
    assert report["output_content"]["final_text_present"] == 4
    assert report["output_content"]["no_final_text"] == 0


@pytest.mark.asyncio
async def test_partial_submission_failure_is_retained_and_does_not_block_batch_monitoring(monkeypatch, tmp_path):
    monkeypatch.setenv("BATCHFORGE_API_KEY", "test-only-key")
    report = await run(arguments(tmp_path), transport=fixture_transport(fail_second_create=True))
    assert report["passed"] is False
    assert report["requested_tasks"] == 4
    assert report["submitted_tasks"] == report["completed"] == 2
    assert report["accepted_batches"] == report["terminal_batches"] == 1
    assert report["submission"]["successful_http_requests"] == 3
    assert report["submission_errors"][0]["http_status"] == 500
    assert report["submission_errors"][0]["create_outcome_uncertain"] is True
    assert report["output_rows_match_completed"] is True


def test_percentiles_are_defined_for_small_and_empty_samples():
    assert distribution([])["p95"] is None
    assert distribution([2])["p95"] == 2
    assert distribution([1, 3])["p50"] == 2
    assert distribution([1, 3])["p95"] == 2.9


@pytest.mark.asyncio
async def test_reasoning_only_length_outputs_remain_durable_completions_without_final_answers(monkeypatch, tmp_path):
    monkeypatch.setenv("BATCHFORGE_API_KEY", "test-only-key")
    report = await run(arguments(tmp_path), transport=fixture_transport(reasoning_only=True))
    assert report["passed"] is True
    assert report["completed"] == 4 and report["observed_completion_tokens"] == 12
    assert report["output_content"]["final_text_present"] == 0
    assert report["output_content"]["reasoning_only"] == report["output_content"]["no_final_text"] == 4
    assert report["output_content"]["length_capped"] == 4
    assert report["output_finish_reasons"] == {"length": 4}
    assert report["output_content"]["durable_completion_distinct_from_final_answer"] is True
    assert report["output_content"]["quality_evaluated"] is False


@pytest.mark.asyncio
async def test_malformed_choice_message_is_explicit_output_validation_failure(monkeypatch, tmp_path):
    monkeypatch.setenv("BATCHFORGE_API_KEY", "test-only-key")
    report = await run(arguments(tmp_path), transport=fixture_transport(malformed_message=True))
    assert report["passed"] is False
    assert report["completed"] == 4  # Database durable state is not rewritten by an observer.
    assert report["output_validation_errors"]
    assert report["output_content"]["rows_with_valid_content_shape"] == 0


@pytest.mark.asyncio
async def test_completion_credit_window_bounds_accepted_nonterminal_batches(monkeypatch, tmp_path):
    monkeypatch.setenv('BATCHFORGE_API_KEY', 'test-only-key')
    args = arguments(tmp_path)
    args.tasks = args.batches = 8
    args.max_inflight_batches = 2
    args.submission_concurrency = 8
    base = fixture_transport()
    active, polls, peak = set(), {}, 0

    async def handle(request):
        nonlocal peak
        if request.url.path == '/v1/batches' and request.method == 'POST':
            response = await base.handle_async_request(request)
            active.add(response.json()['id'])
            peak = max(peak, len(active))
            assert len(active) <= 2
            return response
        if request.url.path.startswith('/v1/batches/'):
            bid = request.url.path.rsplit('/', 1)[-1]
            polls[bid] = polls.get(bid, 0) + 1
            response = await base.handle_async_request(request)
            if polls[bid] == 1:
                row = response.json()
                row['status'] = 'in_progress'
                return httpx.Response(200, json=row)
            active.remove(bid)
            return response
        return await base.handle_async_request(request)

    report = await run(args, transport=httpx.MockTransport(handle))
    assert report['passed'] and report['completed'] == 8
    assert peak == 2 and not active
    assert report['max_inflight_batches'] == 2
    assert all(n == 2 for n in polls.values())


@pytest.mark.asyncio
async def test_unresolved_batch_stops_window_without_releasing_unknown_capacity(monkeypatch, tmp_path):
    monkeypatch.setenv('BATCHFORGE_API_KEY', 'test-only-key')
    args = arguments(tmp_path)
    args.tasks = args.batches = 3
    args.max_inflight_batches = 1
    args.deadline = .01
    base = fixture_transport()
    creates = []

    async def handle(request):
        response = await base.handle_async_request(request)
        if request.url.path == '/v1/batches':
            creates.append(response.json()['id'])
        if request.url.path.startswith('/v1/batches/'):
            row = response.json(); row['status'] = 'in_progress'
            return httpx.Response(200, json=row)
        return response

    report = await run(args, transport=httpx.MockTransport(handle))
    assert report['passed'] is False and len(creates) == 1
    assert len(report['unfinished_batch_ids']) == 1
    assert {e['type'] for e in report['submission_errors']} == {'UnresolvedBatchAwaitingRecovery'}


@pytest.mark.asyncio
async def test_global_deadline_prevents_waiting_uploads(monkeypatch, tmp_path):
    monkeypatch.setenv('BATCHFORGE_API_KEY', 'test-only-key')
    args = arguments(tmp_path)
    args.tasks = args.batches = 3
    args.submission_concurrency = 1
    args.run_deadline = .01
    base = fixture_transport()
    uploads = []

    async def handle(request):
        if request.url.path == '/v1/files' and request.method == 'POST':
            uploads.append(request.url.path)
            await asyncio.sleep(.02)
        return await base.handle_async_request(request)

    report = await run(args, transport=httpx.MockTransport(handle))
    assert report['submitted_tasks'] == 0 and len(uploads) == 1
    assert all(e['type'] == 'RunDeadlineExceeded' and not e['create_outcome_uncertain'] for e in report['submission_errors'])


@pytest.mark.asyncio
async def test_transient_create_reuses_exact_key_and_body_after_lost_ack(monkeypatch, tmp_path):
    monkeypatch.setenv('BATCHFORGE_API_KEY', 'test-only-key')
    args = arguments(tmp_path)
    args.tasks = args.batches = 1
    args.create_retries = 1
    args.max_inflight_batches = 1
    base = fixture_transport()
    calls = []
    committed = None

    async def handle(request):
        nonlocal committed
        if request.url.path == '/v1/batches' and request.method == 'POST':
            calls.append((request.headers['Idempotency-Key'], request.content))
            if committed is None:
                committed = (await base.handle_async_request(request)).json()
                return httpx.Response(503, json={'error': {'code': 'service_unavailable'}})
            return httpx.Response(200, json=committed)
        return await base.handle_async_request(request)

    report = await run(args, transport=httpx.MockTransport(handle))
    assert report['passed'] and report['completed'] == 1
    assert len(calls) == 2 and calls[0] == calls[1]
    assert report['submission']['attempted_http_requests'] == 3
    assert report['batch_observations'][0]['create_retry_errors'][0]['http_status'] == 503


@pytest.mark.asyncio
async def test_retained_payload_429_waits_for_archive_with_same_file_and_key(monkeypatch, tmp_path):
    monkeypatch.setenv('BATCHFORGE_API_KEY', 'test-only-key')
    args = arguments(tmp_path);args.tasks=args.batches=1;args.admission_wait_seconds=3
    base=fixture_transport();calls=[]
    async def handle(request):
        if request.url.path=='/v1/batches' and request.method=='POST':
            calls.append((request.headers['Idempotency-Key'],request.content))
            if len(calls)==1:return httpx.Response(429,json={'error':{'code':'retained_payload_limit'}})
        return await base.handle_async_request(request)
    report=await run(args,transport=httpx.MockTransport(handle))
    assert report['passed'] and calls[0]==calls[1]
    assert report['batch_observations'][0]['create_retry_errors'][0]['quota_wait_retry']==1


@pytest.mark.asyncio
async def test_resume_reobserves_accepted_batches_and_reuses_unaccepted_input_without_upload(monkeypatch,tmp_path):
    monkeypatch.setenv('BATCHFORGE_API_KEY','test-only-key')
    args=arguments(tmp_path);base=fixture_transport();fail=True;creates={};uploads=[]
    async def handle(request):
        if request.url.path=='/v1/files' and request.method=='POST':uploads.append(request.content)
        if request.url.path=='/v1/batches' and request.method=='POST':
            key=request.headers['Idempotency-Key'];creates[key]=creates.get(key,0)+1
            if fail and key.endswith('-1'):return httpx.Response(429,json={'error':{'code':'retained_payload_limit'}})
        return await base.handle_async_request(request)
    transport=httpx.MockTransport(handle)
    prior=await run(args,transport=transport)
    assert prior['completed']==2 and not prior['passed']
    args.resume_from=tmp_path/'prior.json';args.resume_from.write_text(json.dumps(prior))
    fail=False
    resumed=await run(args,transport=transport)
    assert resumed['passed'] and resumed['completed']==4
    assert resumed['run_id']==prior['run_id'] and len(uploads)==2
    assert creates[prior['run_id']+'-0']==1 and creates[prior['run_id']+'-1']==2
    assert resumed['original_measurement_started_at']==prior['measurement_started_at']
    assert resumed['resume']['restored_accepted_batches']==1
    args.prompt='changed prompt'
    with pytest.raises(ValueError,match='identical workload'):await run(args,transport=transport)
