"""Compare terminal offered-load experiments using safe frozen metadata only.

No database, HTTP, GPU, raw model output, policy change or capacity forecast.
Different accepted cohorts are never described as matched-task latency samples.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import sys

SHA = re.compile(r"[0-9a-f]{64}\Z")
PROOF_KEYS = {"report_sha256", "engine_runtime_fingerprint", "engine_configuration_sha256",
              "worker_configuration_sha256", "service_source_sha256"}
REQUIRED_CONFIG = {"model", "tasks", "tasks_per_batch", "arrival_rate", "seed", "duration_seconds", "drain_seconds",
    "max_inflight_tasks", "submission_concurrency", "hourly_cost", "fixed_output", "source_sha256", "code_sha256"}
LATENCIES = ("scheduled_arrival_to_finished_seconds", "scheduled_arrival_to_durable_admission_seconds",
             "task_completion_seconds", "task_first_queue_seconds")


class ComparisonError(ValueError):
    def __init__(self, code): self.code = code; super().__init__(code)


def digest(raw): return hashlib.sha256(raw).hexdigest()


def read_metadata(path):
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            info = os.fstat(descriptor)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) & 0o077
                    or not 0 < info.st_size <= 16 * 1024 * 1024):
                raise ComparisonError("unsafe_metadata_file")
            with os.fdopen(descriptor, "rb", closefd=False) as handle: raw = handle.read(info.st_size + 1)
            after = os.fstat(descriptor)
            if len(raw) != info.st_size or any(getattr(info, field) != getattr(after, field) for field in ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")):
                raise ComparisonError("metadata_changed")
        finally: os.close(descriptor)
        def reject(_): raise ComparisonError("invalid_metadata_json")
        def unique(pairs):
            value = {}
            for key, item in pairs:
                if key in value: raise ComparisonError("invalid_metadata_json")
                value[key] = item
            return value
        value = json.loads(raw.decode("utf-8"), parse_constant=reject, object_pairs_hook=unique)
        json.dumps(value, allow_nan=False, ensure_ascii=False).encode("utf-8")
        if not isinstance(value, dict): raise ComparisonError("invalid_metadata_json")
        return value, digest(raw)
    except ComparisonError: raise
    except Exception: raise ComparisonError("metadata_read_failed") from None


def number(value, *, positive=False):
    if type(value) not in (int, float) or not math.isfinite(value) or value < 0 or positive and value <= 0:
        raise ComparisonError("invalid_numeric_metric")
    return value


def integer(value, *, positive=False):
    if type(value) is not int: raise ComparisonError("invalid_integer_metric")
    return number(value, positive=positive)


def statistics(value, expected=None):
    if not isinstance(value, dict): raise ComparisonError("invalid_distribution")
    count = integer(value.get("samples"))
    if expected is not None and count != expected: raise ComparisonError("distribution_population_mismatch")
    keys = ("p50", "p95", "p99", "max", "mean")
    if not count:
        if any(value.get(key) is not None for key in keys): raise ComparisonError("invalid_distribution")
    else:
        for key in keys: number(value.get(key))
        if not value["p50"] <= value["p95"] <= value["p99"] <= value["max"] or value["mean"] > value["max"]:
            raise ComparisonError("invalid_distribution")
    return {"samples": count, **{key: value.get(key) for key in keys}}


def validate_proof(proof, report_sha):
    if (set(proof) != PROOF_KEYS or proof["report_sha256"] != report_sha
            or not re.fullmatch(r"sha256:[0-9a-f]{64}", proof.get("engine_runtime_fingerprint", ""))
            or not SHA.fullmatch(proof.get("engine_configuration_sha256", ""))
            or not SHA.fullmatch(proof.get("worker_configuration_sha256", ""))
            or not isinstance(proof.get("service_source_sha256"), dict) or not proof["service_source_sha256"]):
        raise ComparisonError("runtime_proof_invalid")
    for path, checksum in proof["service_source_sha256"].items():
        if (not isinstance(path, str) or not 1 <= len(path) <= 512 or any(ord(char) < 32 for char in path)
                or not isinstance(checksum, str) or not SHA.fullmatch(checksum)):
            raise ComparisonError("runtime_proof_invalid")


def validate_report(report):
    config = report.get("configuration")
    if (not isinstance(config, dict) or not REQUIRED_CONFIG.issubset(config)
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", report.get("job_id", ""))
            or config.get("job_id") != report["job_id"] or not isinstance(config.get("model"), str)
            or not re.fullmatch(r"[A-Za-z0-9_/.-]{1,256}", config["model"])
            or not isinstance(config.get("source_sha256"), str) or not SHA.fullmatch(config["source_sha256"])
            or not isinstance(config.get("code_sha256"), dict) or not config["code_sha256"]
            or any(not isinstance(value, str) or not SHA.fullmatch(value) for value in config["code_sha256"].values())
            or type(config.get("fixed_output")) is not bool):
        raise ComparisonError("runner_configuration_invalid")
    planned = integer(config["tasks"], positive=True)
    batch_size = integer(config["tasks_per_batch"], positive=True)
    if planned > 20000 or batch_size > planned: raise ComparisonError("runner_configuration_invalid")
    for key in ("arrival_rate", "duration_seconds", "drain_seconds"): number(config[key], positive=True)
    for key in ("max_inflight_tasks", "submission_concurrency"): integer(config[key], positive=True)
    integer(config["seed"]); number(config["hourly_cost"])
    count = integer(report.get("completed_tasks"), positive=True)
    if (count > planned or any(report.get(key) != count for key in ("accepted_tasks", "terminal_tasks"))
            or any(report.get(key) != 0 for key in ("failed_tasks", "cancelled_tasks", "unfinished_tasks", "duplicate_accepted_attempts"))
            or report.get("accepted_exactly_once") is not True or report.get("accepted_usage_matches_task_usage") is not True
            or type(report.get("passed")) is not bool or report["passed"] != (count == planned)
            or config["fixed_output"] and report.get("fixed_output_lengths_exact") is not True):
        raise ComparisonError("terminal_cohort_not_proven")
    tasks = report.get("tasks")
    if not isinstance(tasks, list) or len(tasks) != count: raise ComparisonError("task_population_mismatch")
    identifiers = set()
    for task in tasks:
        identifier = task.get("custom_id", "")
        if (not re.fullmatch(re.escape(report["job_id"]) + r"-[0-9]{8}", identifier) or identifier in identifiers
                or int(identifier.rsplit("-", 1)[1]) >= planned or task.get("status") != "completed"
                or task.get("accepted_attempts") != 1 or task.get("simulated") is True
                or task.get("prompt_tokens") != task.get("accepted_prompt_tokens")
                or task.get("completion_tokens") != task.get("accepted_completion_tokens")):
            raise ComparisonError("task_acceptance_or_usage_invalid")
        integer(task["prompt_tokens"]); integer(task["completion_tokens"])
        identifiers.add(identifier)
    for key in ("prompt_tokens", "completion_tokens"):
        if sum(task[key] for task in tasks) != report.get(key): raise ComparisonError("token_total_mismatch")
    elapsed = number(report.get("elapsed_seconds_including_disconnect_and_validation"), positive=True)
    cost = report.get("cost", {})
    allocation = config["hourly_cost"] * elapsed / 3600
    if (cost.get("assumed_whole_pod_usd_per_hour") != config["hourly_cost"]
            or not math.isclose(number(cost.get("allocation_usd")), allocation, rel_tol=1e-9, abs_tol=1e-6)):
        raise ComparisonError("whole_pod_allocation_mismatch")
    latencies = {key: statistics(report.get(key), count if key != "scheduled_arrival_to_durable_admission_seconds" else None) for key in LATENCIES}
    batch_count = integer(report.get("accepted_batches"), positive=True)
    if latencies["scheduled_arrival_to_durable_admission_seconds"]["samples"] != batch_count:
        raise ComparisonError("admission_population_mismatch")
    classes = {}
    for label, group in report.get("by_class", {}).items():
        if label not in {"prefill_heavy", "decode_heavy", "balanced"}: raise ComparisonError("unknown_workload_class")
        members = integer(group.get("tasks"), positive=True)
        classes[label] = {"tasks": members, "prompt_tokens": integer(group.get("prompt_tokens")),
            "completion_tokens": integer(group.get("completion_tokens")), "completion_seconds": statistics(group.get("completion_seconds"), members),
            "whole_cohort_prompt_tokens_per_second": group["prompt_tokens"] / elapsed,
            "whole_cohort_completion_tokens_per_second": group["completion_tokens"] / elapsed}
    if (sum(group["tasks"] for group in classes.values()) != count
            or any(sum(group[key] for group in classes.values()) != report[key] for key in ("prompt_tokens", "completion_tokens"))):
        raise ComparisonError("workload_class_total_mismatch")
    gpu = report.get("gpu_utilization_percent", {})
    if set(gpu) != {"0", "1", "2", "3"}: raise ComparisonError("four_gpu_samples_required")
    gpu = {key: statistics(value) for key, value in gpu.items()}
    if any(not value["samples"] or value["max"] > 100 for value in gpu.values()):
        raise ComparisonError("invalid_gpu_samples")
    gaps = report.get("sampling_coverage", {})
    gap_keys = ("observed_wall_span_seconds", "maximum_sample_or_boundary_gap_seconds",
        "continuous_sampling_segment_span_seconds", "wall_seconds_outside_sampling_segments")
    gaps = {key: number(gaps.get(key)) for key in gap_keys}
    if not math.isclose(gaps["continuous_sampling_segment_span_seconds"] + gaps["wall_seconds_outside_sampling_segments"], elapsed, abs_tol=1e-5):
        raise ComparisonError("sampling_coverage_mismatch")
    rejected = report.get("http_rejections", {})
    rejected = {**{key: integer(rejected.get(key)) for key in ("upload_429", "create_429", "uncertain_create_responses")},
        "upload_not_forwarded": integer(rejected.get("upload_not_forwarded", 0))}
    offered = sum(min(batch_size, planned - offset) for offset in range(0, planned, batch_size)
                  if offset / config["arrival_rate"] < config["duration_seconds"])
    if count > offered: raise ComparisonError("accepted_exceeds_offered_schedule")
    return {"job_id": report["job_id"], "original_report_passed": report["passed"], "partial_plan": count < planned,
        "planned_tasks": planned, "offered_tasks_within_admission_window": offered,
        "configured_arrival_rate_tasks_per_second": config["arrival_rate"], "accepted_tasks": count, "completed_tasks": count,
        "accepted_fraction_of_offered": count / offered if offered else None, "accepted_batches": batch_count,
        "elapsed_seconds_including_disconnect_and_validation": elapsed, "http_rejections": rejected,
        "completed_tasks_per_whole_wall_second": count / elapsed, "prompt_tokens": report["prompt_tokens"],
        "completion_tokens": report["completion_tokens"], "prompt_tokens_per_whole_wall_second": report["prompt_tokens"] / elapsed,
        "completion_tokens_per_whole_wall_second": report["completion_tokens"] / elapsed, "cost": {
            "assumed_whole_pod_usd_per_hour": config["hourly_cost"], "allocation_usd": allocation,
            "allocation_usd_per_completed_task": allocation / count, "allocation_usd_per_million_completed_tasks": allocation / count * 1000000},
        "latencies": latencies, "by_class": classes, "gpu_utilization_percent": gpu, "sampling_coverage": gaps,
        "observation_samples": integer(report.get("observation_samples")), "observation_errors": integer(report.get("observation_errors"))}


def change(before, after):
    return {"baseline": before, "candidate": after, "absolute_change": after - before,
            "relative_change_percent": (after / before - 1) * 100 if before else None}


def compare(baseline, candidate, before_proof, after_proof, *, before_sha, after_sha, input_shas):
    validate_proof(before_proof, before_sha); validate_proof(after_proof, after_sha)
    left, right = validate_report(baseline), validate_report(candidate)
    left_config = {key: value for key, value in baseline["configuration"].items() if key != "job_id"}
    right_config = {key: value for key, value in candidate["configuration"].items() if key != "job_id"}
    if left_config != right_config: raise ComparisonError("runner_configuration_changed")
    if baseline["job_id"] == candidate["job_id"]: raise ComparisonError("distinct_experiment_ids_required")
    if any(before_proof[key] != after_proof[key] for key in PROOF_KEYS - {"report_sha256"}):
        raise ComparisonError("engine_worker_or_service_runtime_changed")
    return {"comparison_version": 1, "comparison_source_sha256": digest(Path(__file__).read_bytes()), "source_inputs_sha256": input_shas,
        "configuration_matched_except_job_id": True, "runtime_proofs_matched": True,
        "runtime_fingerprint": before_proof["engine_runtime_fingerprint"], "fixture_sha256": left_config["source_sha256"],
        "model": left_config["model"], "baseline": left, "candidate": right,
        "changes": {"completed_tasks": change(left["completed_tasks"], right["completed_tasks"]),
            "completion_token_throughput": change(left["completion_tokens_per_whole_wall_second"], right["completion_tokens_per_whole_wall_second"]),
            "durable_task_throughput": change(left["completed_tasks_per_whole_wall_second"], right["completed_tasks_per_whole_wall_second"]),
            "allocation_usd_per_completed_task": change(left["cost"]["allocation_usd_per_completed_task"], right["cost"]["allocation_usd_per_completed_task"]),
            "create_429": change(left["http_rejections"]["create_429"], right["http_rejections"]["create_429"]),
            "upload_not_forwarded": change(left["http_rejections"]["upload_not_forwarded"], right["http_rejections"]["upload_not_forwarded"]),
            "latencies": {key: {quantile: change(left["latencies"][key][quantile], right["latencies"][key][quantile])
                for quantile in ("p50", "p95", "p99")} for key in LATENCIES}},
        "comparison_scope": "offered-load admission and completion comparison; accepted populations can differ",
        "business_state_modified": False, "limitations": [
            "Descriptive serial experiments, not randomized matched-task tail or GPU kernel speedup evidence",
            "Latency percentiles use each run's completed population; admission latency uses accepted batches",
            "Whole-wall throughput and cost include admission backoff, idle, drain, validation and disconnect time",
            "Offered tasks refer to the persisted virtual cohort release schedule, not acknowledged API creates",
            "NVML values are sample means of device activity, not useful FLOPs, Tensor Core occupancy or time-weighted utilization",
            "Sampling gaps remain explicit; missing periods are not zero-filled",
            "Cost uses an operator whole-Pod allocation assumption, not invoice or marginal inference cost",
            "The response-policy intervention requires separate operator policy/audit evidence; it is not inferred from these reports",
            "No daily capacity, fleet scaling, high-availability or future-model guarantee"]}


def markdown(result):
    before, after = result["baseline"], result["candidate"]
    rows = [("Original full-plan passed", before["original_report_passed"], after["original_report_passed"]),
        ("Offered / accepted / completed", f"{before['offered_tasks_within_admission_window']} / {before['accepted_tasks']} / {before['completed_tasks']}",
         f"{after['offered_tasks_within_admission_window']} / {after['accepted_tasks']} / {after['completed_tasks']}"),
        ("Upload HTTP 429 responses", before["http_rejections"]["upload_429"], after["http_rejections"]["upload_429"]),
        ("Create HTTP 429 responses", before["http_rejections"]["create_429"], after["http_rejections"]["create_429"]),
        ("Proven pre-forward upload rejections (safe retry)", before["http_rejections"]["upload_not_forwarded"], after["http_rejections"]["upload_not_forwarded"]),
        ("Uncertain create responses (same-key retry)", before["http_rejections"]["uncertain_create_responses"], after["http_rejections"]["uncertain_create_responses"]),
        ("Whole wall seconds", round(before["elapsed_seconds_including_disconnect_and_validation"], 3), round(after["elapsed_seconds_including_disconnect_and_validation"], 3)),
        ("Completed tasks / whole wall second", round(before["completed_tasks_per_whole_wall_second"], 6), round(after["completed_tasks_per_whole_wall_second"], 6)),
        ("Completion tokens / whole wall second", round(before["completion_tokens_per_whole_wall_second"], 3), round(after["completion_tokens_per_whole_wall_second"], 3)),
        ("Whole-Pod allocation USD / completed task", round(before["cost"]["allocation_usd_per_completed_task"], 6), round(after["cost"]["allocation_usd_per_completed_task"], 6))]
    for name in LATENCIES:
        rows.append((name + " P50 / P95 / P99 (s)",
            " / ".join(str(before["latencies"][name][key]) for key in ("p50", "p95", "p99")),
            " / ".join(str(after["latencies"][name][key]) for key in ("p50", "p95", "p99"))))
    for rank in ("0", "1", "2", "3"):
        rows.append((f"GPU{rank} NVML sample mean (%)", before["gpu_utilization_percent"][rank]["mean"], after["gpu_utilization_percent"][rank]["mean"]))
    rows.append(("Maximum sampling/boundary gap (s)", before["sampling_coverage"]["maximum_sample_or_boundary_gap_seconds"], after["sampling_coverage"]["maximum_sample_or_boundary_gap_seconds"]))
    return ("# Sustained offered-load comparison\n\n" + f"Model: `{result['model']}`. Runtime and fixture match.\n\n"
        + "| Metric | Baseline | Candidate |\n| --- | ---: | ---: |\n"
        + "\n".join(f"| {name} | {left} | {right} |" for name, left, right in rows)
        + "\n\nThis compares admission and completion under the same offered schedule. Different accepted cohorts are not matched-task latency samples or GPU kernel speedup evidence.\n\n"
        + "\n".join("- " + item for item in result["limitations"]) + "\n\nInput SHA256 values:\n\n"
        + "\n".join(f"- `{key}`: `{value}`" for key, value in sorted(result["source_inputs_sha256"].items())) + "\n")


def exclusive_output(path, raw):
    parent = path.parent
    if ".runtime" not in parent.resolve().parts: raise ComparisonError("private_output_required")
    info = parent.lstat()
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) & 0o077):
        raise ComparisonError("private_output_required")
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, "wb") as handle: handle.write(raw); handle.flush(); os.fsync(handle.fileno())
    except OSError: raise ComparisonError("exclusive_output_failed") from None


def main():
    class SafeArgumentParser(argparse.ArgumentParser):
        def error(self, _message):
            print(json.dumps({"ok": False, "error_code": "invalid_arguments"}))
            raise SystemExit(2)
    parser = SafeArgumentParser(description=__doc__)
    for side in ("baseline", "candidate"):
        parser.add_argument(f"--{side}-report", type=Path, required=True)
        parser.add_argument(f"--{side}-runtime-proof", type=Path, required=True)
    parser.add_argument("--json", type=Path); parser.add_argument("--markdown", type=Path)
    args = parser.parse_args()
    try:
        inputs, shas = {}, {}
        for key in ("baseline_report", "candidate_report", "baseline_runtime_proof", "candidate_runtime_proof"):
            inputs[key], shas[key] = read_metadata(getattr(args, key))
        result = compare(inputs["baseline_report"], inputs["candidate_report"], inputs["baseline_runtime_proof"], inputs["candidate_runtime_proof"],
            before_sha=shas["baseline_report"], after_sha=shas["candidate_report"], input_shas=shas)
        raw = json.dumps(result, sort_keys=True, separators=(",", ":"), allow_nan=False).encode() + b"\n"
        if args.json: exclusive_output(args.json, raw)
        if args.markdown: exclusive_output(args.markdown, markdown(result).encode())
        print(raw.decode(), end="")
    except Exception as error:
        print(json.dumps({"ok": False, "error_code": error.code if isinstance(error, ComparisonError) else "comparison_failed"}))
        raise SystemExit(1) from None


if __name__ == "__main__": main()
