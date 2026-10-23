"""Build exact chat-template token lengths from public text, without remote code.

The output is ordinary BatchForge JSONL; metadata lives in a separate manifest.
Output lengths are caps, never promises that a model will generate that many.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import random


def chat_tokens(tokenizer, messages):
    return tokenizer.apply_chat_template(messages, tokenize=True, add_generation_prompt=True)


def fit_messages(tokenizer, text, target, identifier, *, tolerance=8):
    """Search real text boundaries, then verify the actual rendered chat length.

    BPE length is not strictly monotonic at a character boundary. Keep the best
    observed candidate and fail instead of silently mislabelling a short input.
    """
    prefix = f"Document batch {identifier}. Summarize the following documents accurately.\n\n"
    def render(length):
        messages = [{"role": "user", "content": prefix + text[:length]}]
        return messages, len(chat_tokens(tokenizer, messages))
    messages, minimum = render(0)
    if minimum > target + tolerance:
        raise ValueError("Requested prompt shorter than chat template and instruction")
    best = (abs(minimum-target), messages, minimum)
    high = min(len(text), target * 8)
    while render(high)[1] < target - tolerance:
        if high == len(text):
            raise ValueError("Insufficient source text for requested token length")
        high = min(len(text), high * 2)
    low = 0
    while low <= high:
        middle = (low + high) // 2
        messages, count = render(middle)
        if abs(count-target) < best[0]:
            best = (abs(count-target), messages, count)
        if count == target:
            break
        if count < target:
            low = middle + 1
        else:
            high = middle - 1
    if best[0] > tolerance:
        raise ValueError(f"Cannot fit prompt within tolerance: target={target}, actual={best[2]}")
    return best[1], best[2]


def build(dataset, tokenizer, *, model, revision, inputs, outputs, repeats, seed,
          context_limit, tolerance=8):
    if repeats < 1 or not inputs or not outputs or min(inputs + outputs) < 1:
        raise ValueError("Positive input/output sizes and repeats required")
    if max(inputs) + tolerance + max(outputs) > context_limit:
        raise ValueError("Context must fit the largest actual prompt plus output cap")
    data = dataset.read_bytes()
    paragraphs = [p["context"] for a in json.loads(data)["data"] for p in a["paragraphs"]]
    if not paragraphs:
        raise ValueError("No public document paragraphs found")
    rng = random.Random(seed)
    rows, examples = [], {}
    for repeat in range(repeats):
        for input_size in inputs:
            for output_size in outputs:
                identifier = f"mix-{repeat}-{input_size}-{output_size}"
                # A different real-document order per request avoids an identical
                # shared prefix; prefix caching must also be disabled in engine.
                order = list(paragraphs)
                rng.shuffle(order)
                text = "\n\n".join(order)
                messages, actual = fit_messages(tokenizer, text, input_size, identifier, tolerance=tolerance)
                body = {"model": model, "messages": messages, "max_tokens": output_size,
                        "temperature": 0, "seed": seed}
                rows.append({"custom_id": identifier, "method": "POST", "url": "/v1/chat/completions", "body": body})
                examples[identifier] = {"target_prompt_tokens": input_size, "rendered_prompt_tokens": actual,
                    "rendered_prompt_token_ids_sha256": hashlib.sha256(json.dumps(
                        chat_tokens(tokenizer, messages), separators=(",", ":")).encode()).hexdigest(),
                    "output_token_cap": output_size,
                    "class": "prefill-heavy" if actual >= output_size * 8 else "decode-heavy" if output_size >= actual * 2 else "balanced"}
    rng.shuffle(rows)
    payload = "".join(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n" for row in rows).encode()
    counter = getattr(tokenizer, "counter_metadata", None)
    template_hash = hashlib.sha256(str(tokenizer.chat_template).encode()).hexdigest() if counter is None else None
    return payload, {"schema_version": 1, "model": model, "tokenizer_revision": revision,
        "dataset": "SQuAD 1.1 development", "license": "CC BY-SA 4.0",
        "source_url": "https://rajpurkar.github.io/SQuAD-explorer/", "dataset_sha256": hashlib.sha256(data).hexdigest(),
        "workload_sha256": hashlib.sha256(payload).hexdigest(), "seed": seed, "context_limit": context_limit,
        "input_sizes": inputs, "output_caps": outputs, "repeats": repeats, "tasks": len(rows),
        "prompt_tolerance_tokens": tolerance, "chat_template_sha256": template_hash,
        "prompt_counter": counter or {"type": "huggingface-chat-template", "chat_template_sha256": template_hash},
        "examples": examples, "limitations": ["Real public text, not customer traffic or a quality evaluation",
            "Output values are maximum lengths; actual generated tokens must come from engine usage",
            "Input counts include rendered chat template and generation prefix; server usage must agree",
            "Prefill/decode labels use requested input and output cap, not separate hardware timing"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--tokenizer", type=Path, required=True, help="Existing local pinned tokenizer directory")
    parser.add_argument("--prompt-counter", choices=("hf-template", "gptoss-vllm-0.10.2-harmony"), default="hf-template")
    parser.add_argument("--harmony-date", help="Formatter date YYYY-MM-DD; default is this host's local current date")
    parser.add_argument("--revision", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--inputs", type=int, nargs="+", default=[128, 512, 2048, 8192, 16384])
    parser.add_argument("--outputs", type=int, nargs="+", default=[32, 128, 512, 2048])
    parser.add_argument("--repeats", type=int, default=2)
    parser.add_argument("--context-limit", type=int, required=True)
    parser.add_argument("--seed", type=int, default=20261024)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    args = parser.parse_args()
    if args.prompt_counter == "gptoss-vllm-0.10.2-harmony":
        from scripts.gptoss_prompt_counter import GptOssHarmonyCounter
        config = (args.tokenizer / "config.json").read_bytes()
        if json.loads(config).get("model_type") != "gpt_oss":
            parser.error("Harmony counter requires a local pinned GPT-OSS config")
        tokenizer = GptOssHarmonyCounter(render_date=args.harmony_date)
        tokenizer.counter_metadata["model_config_sha256"] = hashlib.sha256(config).hexdigest()
        tokenizer.counter_metadata["model_revision"] = args.revision
    else:
        if args.harmony_date:
            parser.error("--harmony-date requires the explicit Harmony counter")
        from transformers import AutoTokenizer
        tokenizer = AutoTokenizer.from_pretrained(str(args.tokenizer), local_files_only=True, trust_remote_code=False)
    payload, manifest = build(args.dataset, tokenizer, model=args.model, revision=args.revision,
        inputs=args.inputs, outputs=args.outputs, repeats=args.repeats, seed=args.seed, context_limit=args.context_limit)
    for path in (args.output, args.manifest):
        path.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(payload)
    args.manifest.write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps({key: value for key, value in manifest.items() if key != "examples"}, indent=2))


if __name__ == "__main__":
    main()
