import copy
import json

import pytest

from scripts import compare_sustained_reports as module


def distribution(samples):
    return {"samples": samples, "p50": 10, "p95": 20, "p99": 30, "max": 40, "mean": 15}


def report(job, count):
    config = {"job_id": job, "model": "Qwen/Qwen3-235B-A22B-Instruct-2507-FP8", "tasks": 6,
        "tasks_per_batch": 2, "arrival_rate": .4, "seed": 20261024, "duration_seconds": 1800,
        "drain_seconds": 900, "max_inflight_tasks": 80, "submission_concurrency": 2,
        "hourly_cost": 16.1, "fixed_output": True, "source_sha256": "a" * 64,
        "code_sha256": {"sustained_benchmark.py": "b" * 64, "benchmark.py": "c" * 64, "observe_runtime.py": "d" * 64}}
    tasks = [{"custom_id": f"{job}-{index:08d}", "status": "completed", "accepted_attempts": 1,
        "prompt_tokens": 128, "completion_tokens": 32, "accepted_prompt_tokens": 128,
        "accepted_completion_tokens": 32, "simulated": False} for index in range(count)]
    value = {"job_id": job, "configuration": config, "passed": count == 6, "completed_tasks": count,
        "accepted_tasks": count, "terminal_tasks": count, "failed_tasks": 0, "cancelled_tasks": 0,
        "unfinished_tasks": 0, "duplicate_accepted_attempts": 0, "accepted_exactly_once": True,
        "accepted_usage_matches_task_usage": True, "fixed_output_lengths_exact": True, "tasks": tasks,
        "prompt_tokens": count * 128, "completion_tokens": count * 32, "accepted_batches": count // 2,
        "elapsed_seconds_including_disconnect_and_validation": 200, "cost": {
            "assumed_whole_pod_usd_per_hour": 16.1, "allocation_usd": 16.1 * 200 / 3600},
        "by_class": {"decode_heavy": {"tasks": count, "prompt_tokens": count * 128,
            "completion_tokens": count * 32, "completion_seconds": distribution(count)}},
        "gpu_utilization_percent": {str(rank): distribution(5) for rank in range(4)},
        "sampling_coverage": {"observed_wall_span_seconds": 190, "maximum_sample_or_boundary_gap_seconds": 10,
            "continuous_sampling_segment_span_seconds": 190, "wall_seconds_outside_sampling_segments": 10},
        "observation_samples": 5, "observation_errors": 0,
        "http_rejections": {"upload_429": 0, "create_429": 61 if count < 6 else 0, "uncertain_create_responses": 0}}
    for name in module.LATENCIES: value[name] = distribution(count if name != "scheduled_arrival_to_durable_admission_seconds" else count // 2)
    return value


def proof(report_sha):
    return {"report_sha256": report_sha, "engine_runtime_fingerprint": "sha256:" + "1" * 64,
        "engine_configuration_sha256": "2" * 64, "worker_configuration_sha256": "3" * 64,
        "service_source_sha256": {"worker/runtime.py": "4" * 64, "ops/manage.py": "5" * 64}}


def compare(before=None, after=None, before_proof=None, after_proof=None):
    return module.compare(before or report("baseline", 4), after or report("candidate", 6),
        before_proof or proof("e" * 64), after_proof or proof("f" * 64), before_sha="e" * 64,
        after_sha="f" * 64, input_shas={"baseline_report": "e" * 64, "candidate_report": "f" * 64})


def test_preserves_partial_full_plan_failure_and_reports_population_cost_and_admission_changes():
    result = compare()
    assert result["baseline"]["original_report_passed"] is False
    assert result["baseline"]["partial_plan"] and result["candidate"]["original_report_passed"] is True
    assert result["baseline"]["accepted_tasks"] == 4 and result["candidate"]["accepted_tasks"] == 6
    assert result["changes"]["durable_task_throughput"]["relative_change_percent"] == 50
    assert result["changes"]["create_429"]["absolute_change"] == -61
    assert result["candidate"]["cost"]["allocation_usd_per_completed_task"] < result["baseline"]["cost"]["allocation_usd_per_completed_task"]
    assert result["baseline"]["offered_tasks_within_admission_window"] == 6
    assert result["baseline"]["completion_tokens_per_whole_wall_second"] == 128 / 200
    assert result["candidate"]["by_class"]["decode_heavy"]["tasks"] == 6
    assert "matched-task" in result["limitations"][0]
    assert "daily" in result["limitations"][-1]


@pytest.mark.parametrize("field,value", [
    ("model", "another/model"), ("source_sha256", "9" * 64), ("arrival_rate", .5), ("duration_seconds", 3600),
    ("seed", 2026), ("tasks_per_batch", 1), ("max_inflight_tasks", 64), ("hourly_cost", 20),
    ("fixed_output", False), ("code_sha256", {"sustained_benchmark.py": "9" * 64}),
])
def test_mixed_experiment_configuration_is_rejected(field, value):
    after = report("candidate", 6); after["configuration"][field] = value
    with pytest.raises(module.ComparisonError): compare(after=after)


@pytest.mark.parametrize("field", ["engine_runtime_fingerprint", "engine_configuration_sha256", "worker_configuration_sha256", "service_source_sha256", "report_sha256"])
def test_engine_worker_sources_and_proof_report_binding_cannot_change(field):
    after = proof("f" * 64)
    if field == "engine_runtime_fingerprint": after[field] = "sha256:" + "9" * 64
    elif field == "service_source_sha256": after[field] = {"worker/runtime.py": "9" * 64}
    else: after[field] = "9" * 64
    with pytest.raises(module.ComparisonError): compare(after_proof=after)


@pytest.mark.parametrize("case", ["duplicate", "usage", "fail", "fixed", "cost", "class", "gpu", "gap", "latency"])
def test_unproven_acceptance_metering_or_observation_is_refused(case):
    after = report("candidate", 6)
    if case == "duplicate": after["tasks"][1]["custom_id"] = after["tasks"][0]["custom_id"]
    elif case == "usage": after["tasks"][0]["accepted_prompt_tokens"] = 999
    elif case == "fail": after["failed_tasks"] = 1
    elif case == "fixed": after["fixed_output_lengths_exact"] = False
    elif case == "cost": after["cost"]["allocation_usd"] *= .5
    elif case == "class": after["by_class"]["decode_heavy"]["prompt_tokens"] -= 1
    elif case == "gpu": del after["gpu_utilization_percent"]["3"]
    elif case == "gap": after["sampling_coverage"]["wall_seconds_outside_sampling_segments"] = 0
    else: after["task_completion_seconds"]["samples"] = 2
    with pytest.raises(module.ComparisonError): compare(after=after)


def test_markdown_and_json_summary_contain_aggregates_without_per_task_identifiers():
    result = compare()
    text = module.markdown(result)
    assert "Offered / accepted / completed" in text
    assert "Maximum sampling/boundary gap" in text
    assert "not matched-task latency samples" in text
    assert "candidate-00000000" not in text and "baseline-00000000" not in json.dumps(result)
    assert result["business_state_modified"] is False


def test_private_input_sha_and_exclusive_outputs(tmp_path):
    root = tmp_path / ".runtime"; root.mkdir(mode=0o700)
    source = root / "report.json"
    source.write_text(json.dumps(report("baseline", 4))); source.chmod(0o600)
    data, checksum = module.read_metadata(source)
    assert checksum == module.digest(source.read_bytes()) and data["job_id"] == "baseline"
    source.chmod(0o644)
    with pytest.raises(module.ComparisonError): module.read_metadata(source)
    output = root / "comparison.json"
    module.exclusive_output(output, b'{"safe":"metadata"}\n')
    assert output.stat().st_mode & 0o777 == 0o600
    before = output.read_bytes()
    with pytest.raises(module.ComparisonError): module.exclusive_output(output, b'overwrite')
    assert output.read_bytes() == before
    link = root / "link.json"; link.symlink_to(output)
    with pytest.raises(module.ComparisonError): module.read_metadata(link)


def test_zero_baseline_delta_is_explicitly_undefined_instead_of_infinite():
    assert module.change(0, 3)["relative_change_percent"] is None


def test_proven_pre_forward_rejections_are_not_lost_or_combined_with_429_and_uncertain_creates():
    before, after = report("baseline", 4), report("candidate", 6)
    before["http_rejections"]["upload_not_forwarded"] = 3
    after["http_rejections"].update(upload_not_forwarded=7, upload_429=2, uncertain_create_responses=1)
    result = compare(before=before, after=after)
    assert result["baseline"]["http_rejections"]["upload_not_forwarded"] == 3
    assert result["candidate"]["http_rejections"] == {"upload_not_forwarded": 7, "upload_429": 2,
        "create_429": 0, "uncertain_create_responses": 1}
    assert result["changes"]["upload_not_forwarded"]["absolute_change"] == 4
    text = module.markdown(result)
    assert "Proven pre-forward upload rejections (safe retry) | 3 | 7" in text
    assert "Upload HTTP 429" in text and "Create HTTP 429" in text and "Uncertain create responses" in text


def test_old_report_without_pre_forward_counter_remains_compatible_as_zero():
    result = compare()
    assert result["baseline"]["http_rejections"]["upload_not_forwarded"] == 0
    assert result["candidate"]["http_rejections"]["upload_not_forwarded"] == 0


@pytest.mark.parametrize("invalid", [-1, 1.5, True, "1", None, float("nan")])
def test_present_pre_forward_counter_requires_nonnegative_integer(invalid):
    after = report("candidate", 6)
    after["http_rejections"]["upload_not_forwarded"] = invalid
    with pytest.raises(module.ComparisonError): compare(after=after)
