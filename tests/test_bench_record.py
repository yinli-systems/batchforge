import pytest

from ops.bench.record import Record, percentile, validate
from ops.bench.run_bench import run


def fake_engine(fail_every=0):
    calls = {"n": 0}

    def engine(prompt, max_tokens):
        calls["n"] += 1
        if fail_every and calls["n"] % fail_every == 0:
            raise TimeoutError("simulated")
        return max_tokens, 0.010 * calls["n"]

    return engine


def base(**kw):
    return Record(model="simulated-7b", precision="fp16", gpu_model="none-simulated", gpu_count=1,
                  tensor_parallel=1, input_tokens=128, output_tokens=64, requests=0,
                  command="python -m ops.bench.run_bench", **kw)


def test_nearest_rank_percentiles():
    values = list(range(1, 101))
    assert percentile(values, 50) == 50 and percentile(values, 95) == 95 and percentile(values, 99) == 99


def test_failures_are_recorded_by_type_and_success_rate_is_computed():
    summary = run(fake_engine(fail_every=4), record=base(), requests=8)
    assert summary["failures"] == ["TimeoutError", "TimeoutError"]
    assert summary["success_rate"] == pytest.approx(0.75)
    assert summary["validation_problems"] == []


def test_cost_requires_a_declared_source():
    rec = base(hourly_cost_usd=2.5)
    rec.requests = 1
    rec.latency_ms = [10.0]
    assert "cost given without a source" in validate(rec)


def test_missing_command_and_bad_tensor_parallel_are_flagged():
    rec = base()
    rec.command = ""
    rec.gpu_count = 1
    rec.tensor_parallel = 4
    rec.requests = 1
    problems = validate(rec)
    assert "missing reproduction command" in problems
    assert "tensor parallel larger than gpu count" in problems


def test_cost_per_million_tokens_uses_measured_rate():
    rec = base(hourly_cost_usd=3.6, cost_source="operator price sheet 2026-10-25")
    rec.requests = 1
    rec.latency_ms = [10.0]
    rec.output_tokens_per_s = 1000.0
    assert rec.summary()["usd_per_million_output_tokens"] == pytest.approx(1.0)
