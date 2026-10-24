"""Operator-calibrated scheduling inputs, separate from tokenizer admission.

The UTF-8 work estimate is a ranking heuristic, never a KV-cache reservation or
an actual token usage measurement. Rates and price have no implicit defaults:
operators must supply a calibration label and cost source for cost-aware work.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping


PROFILE_FIELDS = frozenset({
    "profile_id", "runtime_fingerprint", "prefill_tokens_per_second",
    "decode_tokens_per_second", "hourly_cost_usd", "calibration_label", "cost_source",
})
IDENTITY = re.compile(r"[A-Za-z0-9._:-]{1,256}\Z")


def runtime_fingerprint(descriptor: Mapping) -> str:
    """Hash a caller-supplied revision/quantization/tokenizer/runtime descriptor.

    Hardware/profile identity belongs outside this descriptor: different devices
    may execute the same compatible numerical runtime. This does not establish
    semantic compatibility by itself; operators must pin all relevant settings.
    """
    if not isinstance(descriptor, Mapping) or not descriptor:
        raise ValueError("Runtime descriptor must be a nonempty object")
    encoded = json.dumps(dict(descriptor), sort_keys=True, separators=(",", ":"),
                         ensure_ascii=False, allow_nan=False).encode()
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _label(value, name: str, maximum: int = 256) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > maximum or any(ord(c) < 32 for c in value):
        raise ValueError(f"{name} must be a nonempty printable string of at most {maximum} characters")
    return value


def validate_profile(profile: Mapping) -> dict:
    if not isinstance(profile, Mapping) or set(profile) != PROFILE_FIELDS:
        raise ValueError("scheduling_profile must contain exactly: " + ", ".join(sorted(PROFILE_FIELDS)))
    result = dict(profile)
    for key in ("profile_id", "runtime_fingerprint"):
        if not isinstance(result[key], str) or not IDENTITY.fullmatch(result[key]):
            raise ValueError(f"{key} must be an opaque 1-256 character identity without spaces")
    for key in ("calibration_label", "cost_source"):
        result[key] = _label(result[key], key)
    for key in ("prefill_tokens_per_second", "decode_tokens_per_second", "hourly_cost_usd"):
        raw = result[key]
        if isinstance(raw, bool) or not isinstance(raw, (int, float)):
            raise ValueError(f"{key} must be a finite number")
        value = float(raw)
        if not math.isfinite(value) or value < 0 or (key != "hourly_cost_usd" and value == 0):
            raise ValueError(f"{key} must be finite and {'nonnegative' if key == 'hourly_cost_usd' else 'positive'}")
        lower, upper = (0, 1e9) if key == "hourly_cost_usd" else (1e-9, 1e12)
        if not lower <= value <= upper:
            raise ValueError(f"{key} must be between {lower:g} and {upper:g} to keep prediction arithmetic bounded")
        result[key] = value
    return result


def profile_from_metadata(metadata: Mapping | None) -> dict | None:
    """Read only authenticated worker configuration, never tenant metadata."""
    if not isinstance(metadata, Mapping):
        raise ValueError("Worker metadata must be an object")
    profile = metadata.get("scheduling_profile")
    return None if profile is None else validate_profile(profile)


def estimate_work(body: Mapping) -> dict:
    messages = body.get("messages", [])
    encoded = json.dumps(messages, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()
    output = body.get("max_tokens", body.get("max_completion_tokens", 256))
    if isinstance(output, bool) or not isinstance(output, int) or not 1 <= output <= 10_000_000:
        raise ValueError("max_tokens must be a positive bounded integer")
    # Includes role/content serialization overhead; multilingual and reasoning
    # outputs may deviate substantially. The output estimate is the requested cap.
    return {"estimated_input_tokens": max(1, (len(encoded) + 3) // 4),
            "estimated_output_tokens": output,
            "estimate_source": "utf8_json_bytes_div4_and_output_cap_v1"}


def predict(work: Mapping, profile: Mapping) -> dict:
    profile = validate_profile(profile)
    prefill = work["estimated_input_tokens"] / profile["prefill_tokens_per_second"]
    decode = work["estimated_output_tokens"] / profile["decode_tokens_per_second"]
    return {"predicted_prefill_seconds": prefill, "predicted_decode_seconds": decode,
            "predicted_service_seconds": prefill + decode,
            "predicted_cost_usd": (prefill + decode) * profile["hourly_cost_usd"] / 3600}
