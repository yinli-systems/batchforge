"""Run a benchmark against an engine callable and produce a validated record.

The engine is any callable(prompt_tokens, max_tokens) -> (output_tokens, seconds) or raises.
Real GPU engines are plugged in separately; the tests in this repo use a fake engine only.
"""
from __future__ import annotations

import time
from typing import Callable

from .record import Record, validate


def run(engine: Callable[[int, int], tuple[int, float]], *, record: Record, requests: int) -> dict:
    record.requests = requests
    started = time.monotonic()
    total_output = 0
    for _ in range(requests):
        try:
            output, seconds = engine(record.input_tokens, record.output_tokens)
            record.latency_ms.append(round(seconds * 1000, 3))
            total_output += output
        except Exception as exc:  # record every failure with its type, never drop it
            record.failures.append(type(exc).__name__)
    elapsed = time.monotonic() - started
    if elapsed > 0 and total_output:
        record.output_tokens_per_s = round(total_output / elapsed, 3)
    problems = validate(record)
    summary = record.summary()
    summary["validation_problems"] = problems
    return summary
