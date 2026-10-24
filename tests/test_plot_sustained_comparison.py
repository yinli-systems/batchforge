"""Numeric/provenance gates only; never fabricate or publish candidate plots."""
import copy
import json
import pytest

from scripts import plot_sustained_comparison as module


@pytest.fixture
def inputs():
    # Small synthetic unit-test metadata, never rendered/published as evidence.
    distribution = {"samples": 2, "p50": 10, "p95": 20, "p99": 30, "max": 40, "mean": 15}
    config = {"job_id": "unit-baseline", "model": "unit-test/model", "tasks": 4, "tasks_per_batch": 1,
        "arrival_rate": 1, "seed": 1, "duration_seconds": 4, "drain_seconds": 1, "max_inflight_tasks": 1,
        "submission_concurrency": 1, "hourly_cost": 10, "fixed_output": True, "source_sha256": "0" * 64,
        "code_sha256": {"sustained_benchmark.py": "1" * 64}}
    before = {"job_id": "unit-baseline", "configuration": config, "passed": False, "completed_tasks": 2,
        "accepted_tasks": 2, "terminal_tasks": 2, "failed_tasks": 0, "cancelled_tasks": 0, "unfinished_tasks": 0,
        "duplicate_accepted_attempts": 0, "accepted_exactly_once": True, "accepted_usage_matches_task_usage": True,
        "fixed_output_lengths_exact": True, "tasks": [{"custom_id": f"unit-baseline-{index:08d}",
            "status": "completed", "accepted_attempts": 1, "prompt_tokens": 128, "completion_tokens": 32,
            "accepted_prompt_tokens": 128, "accepted_completion_tokens": 32, "simulated": False} for index in range(2)],
        "prompt_tokens": 256, "completion_tokens": 64, "accepted_batches": 2,
        "elapsed_seconds_including_disconnect_and_validation": 5, "cost": {
            "assumed_whole_pod_usd_per_hour": 10, "allocation_usd": 10 * 5 / 3600},
        "by_class": {"balanced": {"tasks": 2, "prompt_tokens": 256, "completion_tokens": 64,
            "completion_seconds": copy.deepcopy(distribution)}},
        "gpu_utilization_percent": {str(rank): copy.deepcopy(distribution) for rank in range(4)},
        "sampling_coverage": {"observed_wall_span_seconds": 4, "maximum_sample_or_boundary_gap_seconds": 1,
            "continuous_sampling_segment_span_seconds": 4, "wall_seconds_outside_sampling_segments": 1},
        "observation_samples": 2, "observation_errors": 0,
        "http_rejections": {"upload_429": 0, "upload_not_forwarded": 1, "create_429": 0, "uncertain_create_responses": 0}}
    before.update({key: copy.deepcopy(distribution) for key in module.comparison.LATENCIES})
    after = copy.deepcopy(before)
    after["job_id"] = after["configuration"]["job_id"] = "synthetic-unit-test-only"
    for task in after["tasks"]:
        task["custom_id"] = "synthetic-unit-test-only-" + task["custom_id"].rsplit("-", 1)[1]
    before_sha, after_sha = "a" * 64, "b" * 64
    proof = {"report_sha256": before_sha, "engine_runtime_fingerprint": "sha256:" + "c" * 64,
        "engine_configuration_sha256": "d" * 64, "worker_configuration_sha256": "e" * 64,
        "service_source_sha256": {"worker/runtime.py": "f" * 64}}
    candidate_proof = {**proof, "report_sha256": after_sha}
    values = {"baseline_report": before, "candidate_report": after,
        "baseline_runtime_proof": proof, "candidate_runtime_proof": candidate_proof}
    shas = dict(zip(module.INPUT_KEYS, (before_sha, after_sha, "1" * 64, "2" * 64)))
    result = module.comparison.compare(before, after, proof, candidate_proof,
        before_sha=before_sha, after_sha=after_sha, input_shas=shas)
    return values, shas, copy.deepcopy(result)


def test_recomputes_all_metadata_and_preserves_partial_plan_false_without_matplotlib(inputs):
    values, shas, result = inputs
    checked = module.verified_data(result, values, shas)
    audit = module.chart_audit(checked, {"comparison": "3" * 64, **shas})
    assert audit["baseline"]["original_report_passed"] is False
    assert audit["baseline"]["completed_tasks"] == 2
    assert audit["baseline"]["planned_tasks"] == 4
    assert audit["baseline"]["http_rejections"]["upload_not_forwarded"] == 1
    assert any("N=1" in bound and "no confidence" in bound for bound in audit["interpretation_bounds"])
    assert "tasks" not in audit["baseline"] and audit["raw_responses_included"] is False


@pytest.mark.parametrize("mutation", ["rate", "tail", "full_plan", "gpu", "source"])
def test_edited_plot_metrics_or_provenance_cannot_reach_renderer(inputs, mutation):
    values, shas, result = inputs
    if mutation == "rate": result["candidate"]["completed_tasks_per_whole_wall_second"] *= 2
    elif mutation == "tail": result["candidate"]["latencies"]["scheduled_arrival_to_finished_seconds"]["p99"] /= 2
    elif mutation == "full_plan": result["baseline"]["original_report_passed"] = 0  # bool/int equality is insufficient.
    elif mutation == "gpu": result["candidate"]["gpu_utilization_percent"]["0"]["mean"] = 100
    else: result["source_inputs_sha256"]["candidate_report"] = "9" * 64
    with pytest.raises(module.comparison.ComparisonError, match="comparison_does_not_match_verified_inputs"):
        module.verified_data(result, values, shas)


def test_candidate_and_bound_runtime_proof_are_required(inputs):
    values, shas, result = inputs
    del values["candidate_report"]
    with pytest.raises(module.comparison.ComparisonError, match="required_metadata_inputs_missing"):
        module.verified_data(result, values, shas)


def test_metadata_read_allows_committed_metadata_but_not_symlinks_or_duplicate_json(tmp_path):
    source = tmp_path / "metadata.json"
    source.write_text('{"metric":42}'); source.chmod(0o644)
    value, checksum = module.read_metadata(source)
    assert value == {"metric": 42} and checksum == module.digest(source.read_bytes())
    link = tmp_path / "link.json"; link.symlink_to(source)
    with pytest.raises(module.comparison.ComparisonError): module.read_metadata(link)
    source.write_text('{"metric":1,"metric":2}')
    with pytest.raises(module.comparison.ComparisonError): module.read_metadata(source)
    source.write_text('{"metric":NaN}')
    with pytest.raises(module.comparison.ComparisonError): module.read_metadata(source)


def test_artifacts_are_exclusive_and_audit_binds_exact_image_bytes(tmp_path, monkeypatch):
    monkeypatch.setattr(module, "ROOT", tmp_path)
    prefix = tmp_path / "docs/benchmarks/unit-test-no-chart"
    audit = {"safe_metadata": True}
    artifacts = {"png": b"not-a-real-chart: numeric test", "svg": b"not-a-real-chart: numeric test"}
    result = module.write_artifacts(prefix, artifacts, audit)
    assert len(result) == 3
    saved = json.loads(prefix.with_suffix(".chart-data.audit.json").read_bytes())
    assert saved["artifacts"]["unit-test-no-chart.png"]["sha256"] == module.digest(artifacts["png"])
    with pytest.raises(module.comparison.ComparisonError, match="chart_output_already_exists"):
        module.write_artifacts(prefix, artifacts, audit)
    assert prefix.with_suffix(".png").read_bytes() == artifacts["png"]
    with pytest.raises(module.comparison.ComparisonError, match="chart_output_path_invalid"):
        module.write_artifacts(tmp_path / "unsafe/output", artifacts, audit)


def test_default_cli_verifies_bound_metadata_without_importing_matplotlib_or_writing_charts(inputs, tmp_path, monkeypatch, capsys):
    values, _shas, _result = inputs
    paths, shas = {}, {}
    for key in module.INPUT_KEYS[:2]:
        paths[key] = tmp_path / (key + ".json")
        raw = json.dumps(values[key], sort_keys=True).encode()
        paths[key].write_bytes(raw); paths[key].chmod(0o644)
        shas[key] = module.digest(raw)
    for key, report_key in zip(module.INPUT_KEYS[2:], module.INPUT_KEYS[:2]):
        values[key]["report_sha256"] = shas[report_key]
        paths[key] = tmp_path / (key + ".json")
        raw = json.dumps(values[key], sort_keys=True).encode()
        paths[key].write_bytes(raw); paths[key].chmod(0o644)
        shas[key] = module.digest(raw)
    result = module.comparison.compare(values["baseline_report"], values["candidate_report"],
        values["baseline_runtime_proof"], values["candidate_runtime_proof"],
        before_sha=shas["baseline_report"], after_sha=shas["candidate_report"], input_shas=shas)
    comparison = tmp_path / "comparison.json"
    comparison.write_text(json.dumps(result, sort_keys=True)); comparison.chmod(0o644)
    monkeypatch.setattr(module, "render", lambda _: pytest.fail("Validation tried to render"))
    arguments = ["--comparison", str(comparison)]
    for key in module.INPUT_KEYS:
        arguments.extend(["--" + key.replace("_", "-"), str(paths[key])])
    assert module.main(arguments) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["validated"] is True and output["artifacts_written"] == {}
    assert output["baseline_full_plan_passed"] is False and output["candidate_full_plan_passed"] is False
    assert not list(tmp_path.rglob("*.png")) and not list(tmp_path.rglob("*.svg"))


def test_optional_plotting_dependency_has_a_fixed_safe_failure(monkeypatch):
    import builtins
    original = builtins.__import__
    def unavailable(name, *args, **kwargs):
        if name == "matplotlib": raise ImportError("PRIVATE-ENVIRONMENT-DIAGNOSTIC")
        return original(name, *args, **kwargs)
    monkeypatch.setattr(builtins, "__import__", unavailable)
    with pytest.raises(module.comparison.ComparisonError, match="optional_matplotlib_not_installed"):
        module.render({})
