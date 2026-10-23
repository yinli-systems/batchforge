"""Histogram evidence must preserve counters and disclose quantile uncertainty."""
from scripts.observe_runtime import parse_metrics, quantile_bucket, summarize


def metrics(count, total, first, second, start=100):
    text = f'''# TYPE process_start_time_seconds gauge
process_start_time_seconds {start}
# TYPE batchforge_http_request_duration_seconds histogram
batchforge_http_request_duration_seconds_bucket{{method="POST",route="/internal/tasks/{{tid}}/complete",le="0.1"}} {first}
batchforge_http_request_duration_seconds_bucket{{method="POST",route="/internal/tasks/{{tid}}/complete",le="0.5"}} {second}
batchforge_http_request_duration_seconds_bucket{{method="POST",route="/internal/tasks/{{tid}}/complete",le="+Inf"}} {count}
batchforge_http_request_duration_seconds_count{{method="POST",route="/internal/tasks/{{tid}}/complete"}} {count}
batchforge_http_request_duration_seconds_sum{{method="POST",route="/internal/tasks/{{tid}}/complete"}} {total}
'''
    return {"name": "api-a", **parse_metrics(text)}


def sample(api):
    return {"started_at": "start", "finished_at": "end", "apis": [api], "database": {"queue": {"pending": 1}}}


def test_cumulative_histograms_remain_exact_and_summary_uses_deltas():
    before, after = metrics(100, 20, 40, 98), metrics(120, 25, 48, 117)
    result = summarize([sample(before), sample(after)])
    assert after["http_duration_cumulative"][0]["count"] == 120
    operation = result["operations"][0]
    assert operation["count"] == 20
    assert operation["sum_seconds"] == 5
    assert operation["mean_seconds"] == .25
    assert operation["bucket_deltas"] == {"0.1": 8, "0.5": 19, "+Inf": 20}
    assert operation["p95_bucket"] == {"lower_exclusive_seconds": .1, "upper_inclusive_seconds": .5, "upper_unbounded": False}


def test_restart_intervals_are_excluded_and_unbounded_quantiles_are_honest():
    before = metrics(100, 20, 40, 98)
    restarted = metrics(2, 1, 0, 1, start=200)
    next_sample = metrics(4, 2, 0, 2, start=200)
    result = summarize([sample(before), sample(restarted), sample(next_sample)])
    assert result["counter_resets_excluded_intervals"] == {"api-a": 1}
    assert result["operations"][0]["count"] == 2
    assert result["operations"][0]["p95_bucket"]["upper_unbounded"] is True
    assert quantile_bucket({"+Inf": 0}, 0, .95) is None


def test_failed_scrapes_preserve_observation_gap_without_counter_fabrication():
    result = summarize([sample(metrics(10, 2, 4, 9)), sample({"name": "api-a", "error": {"type": "TimeoutError"}}), sample(metrics(15, 3, 6, 14))])
    assert result["api_scrape_failures"] == {"api-a": 1}
    assert result["operations"][0]["count"] == 5
    assert result["sample_count"] == 3
