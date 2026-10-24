"""Optional matplotlib plots of two verified terminal sustained experiments.

Validate without writing by default. Install matplotlib in a separate plotting
environment, then add --write to create PNG, SVG and a metadata-only audit.
Both real terminal reports and their runtime proofs are mandatory; this tool
does not draw forecasts, a placeholder candidate, confidence intervals or raw
responses. It makes no network, database or GPU calls.

Optional dependency setup (separate from the inference/service environment):
    python3 -m venv .runtime/plot-venv
    .runtime/plot-venv/bin/python -m pip install matplotlib

Pass --comparison, --baseline-report, --candidate-report and both matching
--baseline-runtime-proof/--candidate-runtime-proof files. Default: validate only.
Add --write --output-prefix docs/benchmarks/qwen235b-two-30m only after both
real runs are terminal. This writes .png, .svg and .chart-data.audit.json without
overwriting existing files. Raw model output files are never inputs.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import stat
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from scripts import compare_sustained_reports as comparison

INPUT_KEYS = ("baseline_report", "candidate_report", "baseline_runtime_proof", "candidate_runtime_proof")
BOUNDS = [
    "Two serial experiments: N=1 run per configuration; no confidence intervals",
    "End-to-end admission comparison, not matched-task tail or GPU kernel speedup evidence",
    "Full-plan passed=false is preserved even when every accepted task completed",
    "Whole-wall rates and assumed allocation cost include admission, drain, validation and disconnects",
    "NVML sample means are unweighted device activity; missing periods are not zero-filled",
    "No invoice, daily capacity, fleet scaling, high-availability or future-model guarantee",
    "Response-policy attribution requires the separate operator policy/audit evidence",
]


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def read_metadata(path):
    """Allow committed nonsecret metadata as well as private runtime exports."""
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            before = os.fstat(descriptor)
            if (not stat.S_ISREG(before.st_mode) or before.st_uid != os.geteuid()
                    or before.st_mode & 0o022 or not 0 < before.st_size <= 16 * 1024 * 1024):
                raise comparison.ComparisonError("unsafe_metadata_file")
            with os.fdopen(descriptor, "rb", closefd=False) as handle:
                raw = handle.read(before.st_size + 1)
            after = os.fstat(descriptor)
            if len(raw) != before.st_size or any(getattr(before, key) != getattr(after, key)
                    for key in ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")):
                raise comparison.ComparisonError("metadata_changed")
        finally:
            os.close(descriptor)

        def reject(_):
            raise comparison.ComparisonError("invalid_metadata_json")

        def unique(pairs):
            value = {}
            for key, item in pairs:
                if key in value:
                    raise comparison.ComparisonError("invalid_metadata_json")
                value[key] = item
            return value

        value = json.loads(raw.decode("utf-8"), object_pairs_hook=unique, parse_constant=reject)
        json.dumps(value, allow_nan=False, ensure_ascii=False).encode("utf-8")
        if not isinstance(value, dict):
            raise comparison.ComparisonError("invalid_metadata_json")
        return value, digest(raw)
    except comparison.ComparisonError:
        raise
    except Exception:
        raise comparison.ComparisonError("metadata_read_failed") from None


def verified_data(result, inputs, shas):
    if set(inputs) != set(INPUT_KEYS) or set(shas) != set(INPUT_KEYS):
        raise comparison.ComparisonError("required_metadata_inputs_missing")
    expected = comparison.compare(inputs["baseline_report"], inputs["candidate_report"],
        inputs["baseline_runtime_proof"], inputs["candidate_runtime_proof"],
        before_sha=shas["baseline_report"], after_sha=shas["candidate_report"], input_shas=shas)
    # Recompute all values with the frozen comparator. A plausible but edited
    # throughput, tail, cost, population or source checksum cannot reach a plot.
    canonical = lambda value: json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    if canonical(result) != canonical(expected):
        raise comparison.ComparisonError("comparison_does_not_match_verified_inputs")
    return expected


def chart_audit(result, input_shas):
    return {"chart_version": 1, "plot_source_sha256": digest(Path(__file__).read_bytes()),
        "comparison_source_sha256": result["comparison_source_sha256"], "source_inputs_sha256": input_shas,
        "model": result["model"], "runtime_fingerprint": result["runtime_fingerprint"],
        "fixture_sha256": result["fixture_sha256"], "baseline": result["baseline"], "candidate": result["candidate"],
        "comparison_scope": result["comparison_scope"], "interpretation_bounds": BOUNDS,
        "raw_responses_included": False, "business_state_modified": False}


def render(audit):
    try:
        import matplotlib
        matplotlib.use("Agg")
        from matplotlib import pyplot as plt
        from matplotlib.ticker import FuncFormatter
    except ImportError:
        raise comparison.ComparisonError("optional_matplotlib_not_installed") from None

    colors = ("#245B9E", "#D56820")
    names = ("Baseline", "Candidate")
    runs = (audit["baseline"], audit["candidate"])
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10, "text.usetex": False,
        "svg.fonttype": "none", "axes.spines.top": False, "axes.spines.right": False,
        "axes.titleweight": "bold", "axes.labelcolor": "#283444", "text.color": "#283444"})
    figure = plt.figure(figsize=(15, 10.4), facecolor="white")
    grid = figure.add_gridspec(2, 6, left=.065, right=.975, top=.83, bottom=.20, hspace=.94, wspace=1.05)
    axes = [figure.add_subplot(grid[0, :2]), figure.add_subplot(grid[0, 2:4]),
        figure.add_subplot(grid[0, 4:]), figure.add_subplot(grid[1, :3]), figure.add_subplot(grid[1, 3:])]

    def bars(axis, values, labels, formatter=lambda value: f"{value:,.2f}"):
        axis.bar(range(len(values)), values, color=colors, width=.57)
        axis.set_xticks(range(len(values)), labels)
        axis.set_ylim(0, max(values) * 1.22 if max(values) else 1)
        for index, value in enumerate(values):
            axis.annotate(formatter(value), (index, value), xytext=(0, 5), textcoords="offset points",
                ha="center", fontsize=10, fontweight="bold")

    def tail(axis, field, title, note):
        for side, run in enumerate(runs):
            values = [run["latencies"][field][key] for key in ("p95", "p99")]
            positions = [index + (side - .5) * .36 for index in range(2)]
            axis.bar(positions, values, width=.34, color=colors[side], label=names[side])
            for position, value in zip(positions, values):
                axis.annotate(f"{value:.1f}", (position, value), xytext=(0, 4), textcoords="offset points",
                    ha="center", fontsize=9)
        axis.set_xticks([0, 1], ["P95", "P99"])
        axis.set_ylim(0, max(run["latencies"][field]["p99"] for run in runs) * 1.24 or 1)
        axis.set_ylabel("Seconds")
        axis.set_title(title, loc="left", pad=12)
        axis.text(0, -.24, note, transform=axis.transAxes, va="top", fontsize=8.5)

    rates = [run["completed_tasks_per_whole_wall_second"] for run in runs]
    bars(axes[0], rates, names, lambda value: f"{value:.5f}")
    axes[0].set_ylabel("Completed tasks / whole wall second")
    axes[0].set_title("A  Durable task throughput", loc="left", pad=12)
    axes[0].text(0, -.24, "\n".join(f"{name}: offered {run['offered_tasks_within_admission_window']}; "
        f"accepted/completed {run['accepted_tasks']}/{run['completed_tasks']}\n"
        f"  full-plan passed={str(run['original_report_passed']).lower()}; "
        f"output {run['completion_tokens_per_whole_wall_second']:.1f} tokens/s\n"
        f"  upload429 {run['http_rejections']['upload_429']}; proven pre-forward {run['http_rejections']['upload_not_forwarded']}; "
        f"create429 {run['http_rejections']['create_429']}"
        for name, run in zip(names, runs)), transform=axes[0].transAxes, va="top", fontsize=8.5)
    populations = "/".join(str(run["completed_tasks"]) for run in runs)
    tail(axes[1], "scheduled_arrival_to_finished_seconds", "B  Scheduled arrival -> finished",
        f"Includes admission/backpressure + queue + execution.\nCompleted-task populations: {populations}.")
    tail(axes[2], "task_completion_seconds", "C  DB created -> finished",
        f"Includes admitted queue + execution; excludes admission.\nCompleted-task populations: {populations}.")

    costs = [run["cost"]["allocation_usd_per_million_completed_tasks"] for run in runs]
    bars(axes[3], costs, names, lambda value: f"${value:,.0f}")
    axes[3].yaxis.set_major_formatter(FuncFormatter(lambda value, _position: f"${value:,.0f}"))
    axes[3].set_ylabel("Assumed USD / million completed tasks")
    axes[3].set_title("D  Whole-Pod allocation cost", loc="left", pad=12)
    price = runs[0]["cost"]["assumed_whole_pod_usd_per_hour"]
    axes[3].text(0, -.24, f"Assumed ${price:.2f}/Pod-hour; whole observed wall time.\n"
        "Observed cohort unit-cost ratio, not a million-task capacity claim or invoice.",
        transform=axes[3].transAxes, va="top", fontsize=8.5)

    for side, run in enumerate(runs):
        means = [run["gpu_utilization_percent"][str(rank)]["mean"] for rank in range(4)]
        positions = [rank + (side - .5) * .36 for rank in range(4)]
        axes[4].bar(positions, means, width=.34, color=colors[side], label=names[side])
        for position, value in zip(positions, means):
            axes[4].annotate(f"{value:.1f}", (position, value), xytext=(0, 4),
                textcoords="offset points", ha="center", fontsize=9)
    axes[4].set_xticks(range(4), [f"GPU {rank}" for rank in range(4)])
    axes[4].set_ylim(0, 115)
    axes[4].set_ylabel("NVML utilization sample mean (%)")
    axes[4].set_title("E  Sampled four-GPU device activity", loc="left", pad=12)
    gaps = [run["sampling_coverage"] for run in runs]
    axes[4].text(0, -.24, "Max sample/boundary gap (s): " + " / ".join(
        f"{name} {gap['maximum_sample_or_boundary_gap_seconds']:.2f}" for name, gap in zip(names, gaps))
        + "\nWall outside sampling segments (s): " + " / ".join(
        f"{name} {gap['wall_seconds_outside_sampling_segments']:.2f}" for name, gap in zip(names, gaps))
        + "\nNVML samples, GPU0..3: " + " | ".join(f"{name} " + "/".join(
            str(run['gpu_utilization_percent'][str(rank)]['samples']) for rank in range(4)) for name, run in zip(names, runs))
        + "\nUnweighted sample means; gaps not zero-filled; not useful FLOPs or kernel occupancy.",
        transform=axes[4].transAxes, va="top", fontsize=8.5)
    for axis in axes:
        axis.set_axisbelow(True)
        axis.grid(axis="y", color="#DCE2E8", linewidth=.6)

    figure.text(.065, .955, "Sustained offered-load admission comparison", fontsize=20, fontweight="bold")
    figure.text(.065, .920, audit["model"], fontsize=11)
    figure.legend(*axes[1].get_legend_handles_labels(), loc="upper right", bbox_to_anchor=(.975, .950),
        ncol=2, frameon=False, fontsize=10)
    figure.text(.065, .888, "Same single-Pod runtime | Serial N=1 run/configuration | No CIs | "
        "Different accepted populations; not GPU kernel speedup evidence", fontsize=10)
    figure.text(.065, .085, "No daily/fleet/HA guarantee. Full-plan failure flags remain unchanged. "
        "Response-policy attribution needs the separate policy/audit record.", fontsize=8.5)
    checksums = audit["source_inputs_sha256"]
    figure.text(.065, .057, "SHA256 prefixes (full values in chart audit): "
        f"plot {audit['plot_source_sha256'][:12]} | compare source {audit['comparison_source_sha256'][:12]} | "
        f"comparison JSON {checksums['comparison'][:12]}", fontsize=8)
    figure.text(.065, .036, " | ".join(f"{key.replace('_', ' ')} {checksums[key][:12]}" for key in INPUT_KEYS), fontsize=7.5)
    try:
        artifacts = {}
        for format_name in ("png", "svg"):
            buffer = io.BytesIO()
            figure.savefig(buffer, format=format_name, dpi=160, metadata={"Creator": "BatchForge verified metadata plot"})
            artifacts[format_name] = buffer.getvalue()
        return artifacts
    finally:
        plt.close(figure)


def write_artifacts(prefix, artifacts, audit):
    prefix = prefix.absolute()
    allowed = ROOT / "docs" / "benchmarks"
    if (prefix.resolve() != prefix or not prefix.name or len(prefix.name) > 100
            or any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for char in prefix.name)
            or not prefix.is_relative_to(allowed)):
        raise comparison.ComparisonError("chart_output_path_invalid")
    # Refuse symlink components even when their final target is inside docs.
    current = ROOT
    for component in prefix.parent.relative_to(ROOT).parts:
        current = current / component
        if current.is_symlink():
            raise comparison.ComparisonError("chart_output_path_invalid")
    files = {prefix.with_suffix("." + key): raw for key, raw in artifacts.items()}
    audit["artifacts"] = {path.name: {"sha256": digest(raw), "bytes": len(raw)} for path, raw in files.items()}
    files[prefix.with_suffix(".chart-data.audit.json")] = (json.dumps(audit, indent=2, sort_keys=True,
        allow_nan=False) + "\n").encode()
    if any(path.exists() or path.is_symlink() for path in files):
        raise comparison.ComparisonError("chart_output_already_exists")
    prefix.parent.mkdir(parents=True, exist_ok=True)
    created = []
    try:
        for path, raw in files.items():
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o644)
            created.append(path)
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(raw); handle.flush(); os.fsync(handle.fileno())
        descriptor = os.open(prefix.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    except Exception:
        for path in created:
            path.unlink(missing_ok=True)
        raise comparison.ComparisonError("chart_output_failed") from None
    return {path.name: digest(raw) for path, raw in files.items()}


def main(argv=None):
    class SafeParser(argparse.ArgumentParser):
        def error(self, _message):
            print(json.dumps({"ok": False, "error_code": "invalid_arguments"})); raise SystemExit(2)
    parser = SafeParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    for key in ("comparison", *INPUT_KEYS):
        parser.add_argument("--" + key.replace("_", "-"), type=Path, required=True)
    parser.add_argument("--output-prefix", type=Path, default=ROOT / "docs/benchmarks/sustained-comparison")
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args(argv)
    try:
        inputs, shas = {}, {}
        for key in INPUT_KEYS:
            inputs[key], shas[key] = read_metadata(getattr(args, key))
        result, comparison_sha = read_metadata(args.comparison)
        result = verified_data(result, inputs, shas)
        audit = chart_audit(result, {"comparison": comparison_sha, **shas})
        files = write_artifacts(args.output_prefix, render(audit), audit) if args.write else {}
        print(json.dumps({"ok": True, "validated": True, "artifacts_written": files,
            "baseline_full_plan_passed": result["baseline"]["original_report_passed"],
            "candidate_full_plan_passed": result["candidate"]["original_report_passed"],
            "source_inputs_sha256": audit["source_inputs_sha256"]}, sort_keys=True))
        return 0
    except Exception as error:
        print(json.dumps({"ok": False, "error_code": error.code if isinstance(error, comparison.ComparisonError) else "plot_failed"}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
