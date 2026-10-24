#!/usr/bin/env python3
"""Audit observed JSONB response sizes without exposing text or changing policy.

Only the integer/bool/null/string/list/dict JSON subset is supported. The byte
calculation matches PostgreSQL jsonb::text spacing for that subset, not floats.
Default invocation prints safe metadata; --report creates a new private file.
No database, HTTP client, prompt/output copy, or policy mutation is performed.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import sys

MAX_FILE_BYTES = 16 * 1024 * 1024
MAX_TOTAL_BYTES = 64 * 1024 * 1024


class AuditError(ValueError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def reject_number(_value):
    raise AuditError("unsupported_float_or_nonfinite")


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise AuditError("duplicate_json_key")
        result[key] = value
    return result


def integer_subset(value, depth=0):
    if depth > 128:
        raise AuditError("unsupported_nesting")
    if value is None or type(value) in (bool, int):
        return
    if type(value) is str:
        if "\x00" in value:
            raise AuditError("unsupported_unicode")
        try:
            value.encode("utf-8", errors="strict")
        except UnicodeError:
            raise AuditError("unsupported_unicode") from None
        return
    if type(value) is list:
        for item in value:
            integer_subset(item, depth + 1)
        return
    if type(value) is dict:
        for key, item in value.items():
            if type(key) is not str:
                raise AuditError("unsupported_json_type")
            integer_subset(key, depth + 1)
            integer_subset(item, depth + 1)
        return
    raise AuditError("unsupported_float_or_nonfinite" if isinstance(value, float) else "unsupported_json_type")


def canonical_bytes(value):
    """Return only the length. PostgreSQL key reordering does not change it."""
    integer_subset(value)
    try:
        return len(json.dumps(value, ensure_ascii=False, separators=(", ", ": "),
                              allow_nan=False).encode("utf-8", errors="strict"))
    except (TypeError, ValueError, UnicodeError, RecursionError):
        raise AuditError("canonical_encoding_failed") from None


def distribution(values):
    ordered = sorted(values)
    def quantile(fraction):
        if not ordered:
            return None
        position = (len(ordered) - 1) * fraction
        low, high = math.floor(position), math.ceil(position)
        return round(ordered[low] + (ordered[high] - ordered[low]) * (position - low), 6)
    return {"count": len(ordered), "p50": quantile(.5), "p95": quantile(.95), "p99": quantile(.99),
            "min": ordered[0] if ordered else None, "max": ordered[-1] if ordered else None,
            "total": sum(ordered), "mean": sum(ordered) / len(ordered) if ordered else None}


def private_input(path):
    try:
        if ".runtime" not in path.resolve(strict=True).parts or not path.name.endswith("-output.jsonl"):
            raise AuditError("unsafe_input_path")
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except AuditError:
        raise
    except OSError:
        raise AuditError("input_unavailable") from None
    try:
        before = os.fstat(descriptor)
        if (not stat.S_ISREG(before.st_mode) or before.st_uid != os.geteuid()
                or stat.S_IMODE(before.st_mode) & 0o077):
            raise AuditError("input_not_private_regular_file")
        if not 0 < before.st_size <= MAX_FILE_BYTES:
            raise AuditError("input_size_limit")
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            raw = handle.read(MAX_FILE_BYTES + 1)
        after = os.fstat(descriptor)
        if len(raw) > MAX_FILE_BYTES:
            raise AuditError("input_size_limit")
        attributes = ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")
        if any(getattr(before, key) != getattr(after, key) for key in attributes) or len(raw) != before.st_size:
            raise AuditError("input_changed_during_audit")
        return raw, (before.st_dev, before.st_ino)
    finally:
        os.close(descriptor)


def audit(paths, *, expected_count, run_id, candidate_limit_bytes=65536,
          original_limit_bytes=1048576, expected_output_caps=None):
    if (type(expected_count) is not int or not 1 <= expected_count <= 1000000
            or not isinstance(run_id, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", run_id)
            or type(candidate_limit_bytes) is not int or type(original_limit_bytes) is not int
            or not 1024 <= candidate_limit_bytes <= original_limit_bytes <= MAX_FILE_BYTES
            or not 1 <= len(paths) <= 4096):
        raise AuditError("invalid_audit_bounds")
    if expected_output_caps is not None and (not expected_output_caps or any(type(value) is not int or not 1 <= value <= 1000000000 for value in expected_output_caps)):
        raise AuditError("invalid_output_cap_set")
    source_bytes = 0
    identifiers, identities, sizes, tokens, files, groups = set(), set(), [], [], [], {}
    for index, path in enumerate(paths):
        raw, identity = private_input(Path(path))
        if identity in identities:
            raise AuditError("duplicate_input_file")
        identities.add(identity)
        source_bytes += len(raw)
        if source_bytes > MAX_TOTAL_BYTES:
            raise AuditError("total_input_size_limit")
        file_sizes, file_tokens = [], []
        try:
            lines = raw.decode("utf-8", errors="strict").split("\n")
            if lines[-1] == "":
                lines.pop()
        except UnicodeError:
            raise AuditError("invalid_utf8") from None
        for line in lines:
            if not line.strip():
                raise AuditError("empty_jsonl_row")
            try:
                row = json.loads(line, parse_float=reject_number, parse_constant=reject_number, object_pairs_hook=unique_object)
            except AuditError:
                raise
            except (ValueError, TypeError, RecursionError):
                raise AuditError("invalid_json") from None
            if not isinstance(row, dict):
                raise AuditError("invalid_output_shape")
            custom_id = row.get("custom_id")
            if not isinstance(custom_id, str) or not 1 <= len(custom_id) <= 256 or not custom_id.startswith(run_id + "-"):
                raise AuditError("run_identity_mismatch")
            if custom_id in identifiers:
                raise AuditError("duplicate_custom_id")
            response = row.get("response")
            if (row.get("error") is not None or not isinstance(response, dict)
                    or response.get("status_code") != 200 or not isinstance(response.get("body"), dict)):
                raise AuditError("invalid_output_shape")
            body = response["body"]
            usage = body.get("usage")
            completion = usage.get("completion_tokens") if isinstance(usage, dict) else None
            if type(completion) is not int or not 0 <= completion <= 1000000000:
                raise AuditError("invalid_completion_usage")
            if expected_output_caps is not None and completion not in expected_output_caps:
                raise AuditError("observed_output_not_in_declared_caps")
            size = canonical_bytes(body)
            identifiers.add(custom_id)
            sizes.append(size); tokens.append(completion)
            file_sizes.append(size); file_tokens.append(completion)
            groups.setdefault(completion, []).append(size)
            if len(identifiers) > expected_count:
                raise AuditError("expected_count_mismatch")
        files.append({"file_index": index, "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw),
                      "count": len(file_sizes), "response_jsonb_text_bytes": distribution(file_sizes),
                      "completion_tokens": distribution(file_tokens)})
    if len(identifiers) != expected_count:
        raise AuditError("expected_count_mismatch")
    maximum = max(sizes)
    covered = sum(size <= candidate_limit_bytes for size in sizes)
    return {"audit_version": 1, "observed_at": datetime.now(timezone.utc).isoformat(), "run_id": run_id,
        "source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), "expected_count": expected_count,
        "observed_count": len(identifiers), "custom_ids_unique_and_run_prefix_bound": True,
        "input_bytes": source_bytes, "input_files": files,
        "response_jsonb_text_bytes": distribution(sizes), "completion_tokens": distribution(tokens),
        "completion_token_groups": {str(cap): {"count": len(values), "response_jsonb_text_bytes": distribution(values),
            "candidate_covered_count": sum(value <= candidate_limit_bytes for value in values),
            "candidate_minimum_headroom_bytes": candidate_limit_bytes - max(values)} for cap, values in sorted(groups.items())},
        "group_basis": "actual completion_tokens; only a separately proven fixed-output fixture makes these requested-cap groups",
        "declared_output_caps": sorted(expected_output_caps) if expected_output_caps is not None else None,
        "candidate": {"limit_bytes": candidate_limit_bytes, "covered_count": covered,
            "coverage_fraction": covered / expected_count, "all_observed_responses_fit": covered == expected_count,
            "minimum_headroom_bytes": candidate_limit_bytes - maximum,
            "response_reservation_bytes_for_observed_count": candidate_limit_bytes * expected_count},
        "original": {"limit_bytes": original_limit_bytes, "covered_count": sum(size <= original_limit_bytes for size in sizes),
            "minimum_headroom_bytes": original_limit_bytes - maximum,
            "response_reservation_bytes_for_observed_count": original_limit_bytes * expected_count},
        "reservation_comparison": {"response_reservation_reduction_bytes": (original_limit_bytes - candidate_limit_bytes) * expected_count,
            "response_reservation_reduction_fraction": 1 - candidate_limit_bytes / original_limit_bytes,
            "scope": "response allowance only; input bodies, error allowances and total tenant quota are separate"},
        "canonical_definition": "UTF8 bytes of json.dumps(response.body,ensure_ascii=False,separators=(', ',': ')); key order cannot change length for the accepted integer-only JSON subset",
        "percentile_method": "linear interpolation at (n-1)*p",
        "preview_only": True, "database_read": False, "http_requests": False, "policy_changed": False,
        "limitations": ["Observed cohort evidence only; no guarantee for future responses, other workloads or models",
            "Floats, nonfinite numbers, duplicate keys, NUL and unpaired surrogates are refused",
            "This checks response.body JSONB textual accounting, not PostgreSQL heap/TOAST/WAL storage",
            "Completion token groups do not independently establish each request's configured output cap",
            "A smaller response allowance can reject a larger future completion; no policy is applied by this tool"]}


def write_report(path, report):
    path = Path(path)
    try:
        parent = path.parent.resolve(strict=True)
        info = parent.stat()
        if (".runtime" not in parent.parts or not stat.S_ISDIR(info.st_mode)
                or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) & 0o077):
            raise AuditError("report_parent_not_private")
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    except AuditError:
        raise
    except FileExistsError:
        raise AuditError("report_already_exists") from None
    except OSError:
        raise AuditError("report_unavailable") from None
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(json.dumps(report, sort_keys=True, separators=(",", ":"), allow_nan=False).encode() + b"\n")
            handle.flush(); os.fsync(handle.fileno())
        descriptor = os.open(parent, os.O_RDONLY)
        try: os.fsync(descriptor)
        finally: os.close(descriptor)
    except (OSError, ValueError, TypeError):
        # Preserve any partial exclusive report; never remove/overwrite evidence.
        raise AuditError("report_write_failed") from None


class SafeArgumentParser(argparse.ArgumentParser):
    def error(self, _message):
        print(json.dumps({"ok": False, "error_code": "invalid_arguments"}))
        raise SystemExit(2)


def main(argv=None):
    parser = SafeArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, action="append", required=True)
    parser.add_argument("--expected-count", type=int, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--candidate-limit-bytes", type=int, default=65536)
    parser.add_argument("--original-limit-bytes", type=int, default=1048576)
    parser.add_argument("--expected-output-caps", help="Optional comma-separated allowed actual completion token counts")
    parser.add_argument("--report", type=Path, help="Create one NEW owner-only metadata file under an existing private .runtime directory")
    args = parser.parse_args(argv)
    try:
        caps = None
        if args.expected_output_caps is not None:
            try: caps = {int(value) for value in args.expected_output_caps.split(",")}
            except ValueError: raise AuditError("invalid_output_cap_set") from None
        report = audit(args.input, expected_count=args.expected_count, run_id=args.run_id,
            candidate_limit_bytes=args.candidate_limit_bytes, original_limit_bytes=args.original_limit_bytes, expected_output_caps=caps)
        if args.report:
            write_report(args.report, report)
        print(json.dumps(report, sort_keys=True, separators=(",", ":"), allow_nan=False))
        return 0
    except AuditError as error:
        print(json.dumps({"ok": False, "error_code": error.code}))
        return 1
    except Exception:
        print(json.dumps({"ok": False, "error_code": "audit_failed"}))
        return 1


if __name__ == "__main__":
    sys.exit(main())
