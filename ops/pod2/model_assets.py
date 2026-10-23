#!/usr/bin/env python3
"""Download the public, revision-pinned 7B model into BatchForge-owned storage.

No Hugging Face token is used. Downloaded Python code is excluded, and each LFS
weight is verified against the official repository's SHA256 metadata.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict, is_dataclass
import datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import time

MODEL = "Qwen/Qwen2.5-7B-Instruct"
REVISION = "a09a35458c702b33eeacc393d103063234e8bc28"
RUNTIME = Path("/root/batchforge-runtime")
PATTERNS = ["*.safetensors", "*.safetensors.index.json", "config.json", "generation_config.json",
            "tokenizer.json", "tokenizer_config.json", "special_tokens_map.json", "added_tokens.json",
            "vocab.json", "merges.txt", "chat_template.jinja"]


def utc() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("resolve", "download"))
    parser.add_argument("--revision", default=REVISION)
    args = parser.parse_args()
    if len(args.revision) != 40 or any(c not in "0123456789abcdef" for c in args.revision):
        parser.error("revision must be a full lowercase commit SHA")
    os.environ["HF_HOME"] = str(RUNTIME / "hf-home")
    from huggingface_hub import HfApi, snapshot_download

    info = HfApi().model_info(MODEL, revision=args.revision, token=False, files_metadata=True)
    if info.sha != args.revision:
        raise RuntimeError("Official repository returned a different revision")
    weights = []
    for item in info.siblings:
        if not item.rfilename.endswith(".safetensors"):
            continue
        lfs = asdict(item.lfs) if is_dataclass(item.lfs) else item.lfs
        weights.append({"name": item.rfilename, "bytes": item.size, "sha256": lfs["sha256"]})
    report = {"model": MODEL, "revision": info.sha, "resolved_at_utc": utc(),
              "weight_files": weights, "weight_bytes": sum(w["bytes"] for w in weights),
              "download_uses_authentication": False, "trust_remote_code": False,
              "storage_free_bytes_before": shutil.disk_usage(RUNTIME).free}
    state = RUNTIME / "app/.runtime/pod2-qwen7b-model.json"
    state.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    state.write_text(json.dumps(report, indent=2) + "\n")
    state.chmod(0o600)
    print(json.dumps({"event": "model_resolved", "model": MODEL, "revision": info.sha,
                      "weight_bytes": report["weight_bytes"],
                      "storage_free_bytes_before": report["storage_free_bytes_before"]}), flush=True)
    if args.action == "resolve":
        return
    if report["storage_free_bytes_before"] < report["weight_bytes"] + 4 * 1024**3:
        raise RuntimeError("Not enough app filesystem headroom for model download")
    started = time.monotonic()
    target = RUNTIME / "models/Qwen2.5-7B-Instruct" / info.sha
    snapshot_download(MODEL, revision=info.sha, token=False, local_dir=target,
                      allow_patterns=PATTERNS, max_workers=4)
    verified = []
    for weight in weights:
        path = target / weight["name"]
        if path.stat().st_size != weight["bytes"]:
            raise RuntimeError("Weight file length differs from official metadata")
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(8 * 1024**2), b""):
                digest.update(chunk)
        if digest.hexdigest() != weight["sha256"]:
            raise RuntimeError("Weight file checksum differs from official metadata")
        verified.append(weight["name"])
    report.update({"model_path": str(target), "download_completed_at_utc": utc(),
                   "download_and_checksum_seconds": round(time.monotonic() - started, 3),
                   "sha256_verified_weight_files": verified, "status": "verified"})
    state.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"event": "model_verified", "model_path": str(target), "revision": info.sha,
                      "files": len(verified), "elapsed_seconds": report["download_and_checksum_seconds"]}), flush=True)


if __name__ == "__main__":
    main()
