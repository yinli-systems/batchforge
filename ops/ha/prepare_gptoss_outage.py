"""Prepare eight bounded recovery requests from a frozen public-text fixture.

No HTTP, database access, generation, model download, or process operation.
Prompt-token provenance belongs to the source vLLM Harmony formatter only;
the llama.cpp fallback has its own formatter and actual token usage.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
from pathlib import Path


def prepare(workload: Path, manifest: Path, output: Path) -> dict:
    data = workload.read_bytes()
    metadata = json.loads(manifest.read_bytes())
    if metadata.get("model") != "openai/gpt-oss-120b" or metadata.get("workload_sha256") != hashlib.sha256(data).hexdigest():
        raise ValueError("Frozen GPT-OSS source model or workload digest mismatch")
    candidates = []
    for line in data.splitlines():
        row = json.loads(line)
        example = metadata.get("examples", {}).get(row.get("custom_id"), {})
        body = row.get("body", {})
        if (example.get("target_prompt_tokens") == 128 and example.get("output_token_cap") == 512
                and type(example.get("rendered_prompt_tokens")) is int and 120 <= example["rendered_prompt_tokens"] <= 136
                and row.get("method") == "POST" and row.get("url") == "/v1/chat/completions"
                and body.get("model") == "openai/gpt-oss-120b" and body.get("max_tokens") == 512
                and "reasoning_effort" not in body and body.get("stream", False) is False):
            candidates.append((row, example))
    if not candidates:
        raise ValueError("No frozen natural 128-input/512-output-cap request exists")
    source, example = sorted(candidates, key=lambda item: item[0]["custom_id"])[0]
    rows = []
    for index in range(8):
        row = copy.deepcopy(source)
        row["custom_id"] = f"gptoss-recovery-{index}"
        rows.append(row)
    payload = b"".join((json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n").encode() for row in rows)
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    os.chmod(output, 0o700)
    report = {"schema_version": 1, "submitted": False, "tasks": 8,
        "model": "openai/gpt-oss-120b", "max_output_tokens_per_task": 512,
        "maximum_requested_generated_tokens_without_retries": 4096,
        "source_workload_sha256": hashlib.sha256(data).hexdigest(),
        "source_manifest_sha256": hashlib.sha256(manifest.read_bytes()).hexdigest(),
        "source_example_id": source["custom_id"], "source_example": example,
        "source_prompt_counter": metadata.get("prompt_counter"),
        "workload_sha256": hashlib.sha256(payload).hexdigest(),
        "same_body_repeated": True, "force_minimum_tokens": False, "ignore_eos": False,
        "limitations": ["Only a recovery exercise, not a throughput or answer-quality benchmark",
            "Eight requests repeat one public source body; no independent-workload diversity claim",
            "Source rendered prompt-token count applies only to its frozen vLLM Harmony formatter/date, not llama.cpp",
            "A lost lease can cause repeated physical inference; unique accepted usage is checked by the probe",
            "Submission and fault execution both require a separately released operator command"]}
    for name, value in (("workload.jsonl", payload), ("preparation.json", (json.dumps(report, indent=2)+"\n").encode())):
        descriptor = os.open(output / name, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, "wb") as file:
            file.write(value)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workload", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True, help="A new private directory; never overwrites an existing run")
    args = parser.parse_args()
    print(json.dumps(prepare(args.workload, args.manifest, args.output), indent=2))


if __name__ == "__main__":
    main()
