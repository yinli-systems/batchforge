"""Request workload for the open-loop engine test, built only from real text.

Three classes, each with a target prompt size measured in tokens:

* short  - job listing, yes/no early-career classification, real recorded label
* medium - consecutive prose paragraphs, summary, format checked
* long   - consecutive prose paragraphs, JSON extraction, format checked

Prompt sizes are measured by a token counter supplied by the caller. The
engine's own /tokenize endpoint is used on the pod; a character estimate is
used only for dry runs on other machines.
"""
from __future__ import annotations

import random
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

from .corpus import Job

Counter = Callable[[list[dict]], int]

SHORT_SYSTEM = (
    "Answer only yes or no: is this job listing suitable for early-career applicants "
    "(graduate, intern or junior)?"
)
MEDIUM_SYSTEM = "Summarize the user's text in at most two sentences. Do not add commentary."
LONG_SYSTEM = (
    "Extract the fields from the user's text as a JSON object with exactly these keys: "
    "title, date, amount. Use null for any field that is not stated. Output only the JSON object."
)


@dataclass(frozen=True)
class ClassSpec:
    name: str
    share: float
    max_tokens: int
    prompt_min: int
    prompt_max: int
    json_object: bool = False


SPECS = {
    "short": ClassSpec("short", 0.45, 8, 40, 120),
    "medium": ClassSpec("medium", 0.30, 160, 800, 2500),
    "long": ClassSpec("long", 0.25, 200, 6000, 16000, json_object=True),
}


@dataclass
class Item:
    id: str
    kind: str
    messages: list[dict]
    max_tokens: int
    json_object: bool
    label: dict = field(default_factory=dict)
    prompt_tokens: int = 0
    source_ids: tuple[int, ...] = ()


def estimate_tokens(messages: list[dict]) -> int:
    """Character-based estimate. Dry runs only; the pod uses exact counts."""
    return sum(len(m["content"]) for m in messages) // 4 + 8 * len(messages)


def _short_items(jobs: Sequence[Job], rng: random.Random, n: int, count: Counter) -> list[Item]:
    items = []
    pool = list(jobs)
    rng.shuffle(pool)
    for index in range(n):
        job = pool[index % len(pool)]
        messages = [{"role": "system", "content": SHORT_SYSTEM}, {"role": "user", "content": job.text}]
        size = count(messages)
        spec = SPECS["short"]
        if size > spec.prompt_max:
            continue
        items.append(Item(
            id=f"short-{index:05d}", kind="short", messages=messages, max_tokens=spec.max_tokens,
            json_object=False, label={"answer": "yes" if job.early_career else "no"},
            prompt_tokens=size, source_ids=(index % len(pool),),
        ))
    return items


def _windowed_items(paragraphs: Sequence[str], rng: random.Random, kind: str, n: int,
                    system: str, count: Counter) -> list[Item]:
    """Consecutive runs of real paragraphs, sized to fall inside the class range."""
    spec = SPECS[kind]
    items = []
    attempts = 0
    while len(items) < n and attempts < n * 50:
        attempts += 1
        start = rng.randrange(len(paragraphs))
        chosen: list[int] = []
        text = ""
        index = start
        while True:
            chosen.append(index)
            text = "\n\n".join(paragraphs[i] for i in chosen)
            messages = [{"role": "system", "content": system}, {"role": "user", "content": text}]
            size = count(messages)
            if size >= spec.prompt_min:
                break
            index = (index + 1) % len(paragraphs)
            if len(chosen) > len(paragraphs):
                break
        if size < spec.prompt_min or size > spec.prompt_max:
            continue
        items.append(Item(
            id=f"{kind}-{len(items):05d}", kind=kind, messages=messages, max_tokens=spec.max_tokens,
            json_object=spec.json_object, prompt_tokens=size, source_ids=tuple(chosen),
        ))
    if len(items) < n:
        raise ValueError(f"Could not build {n} {kind} items inside {spec.prompt_min}-{spec.prompt_max} tokens; "
                         f"built {len(items)}. Add more prose.")
    return items


def build_pools(jobs: Sequence[Job], paragraphs: Sequence[str], seed: int, per_kind: int,
                count: Counter = estimate_tokens) -> dict[str, list[Item]]:
    """One pool per class. Each pool has `per_kind` distinct items."""
    rng = random.Random(seed)
    return {
        "short": _short_items(jobs, rng, per_kind, count),
        "medium": _windowed_items(paragraphs, rng, "medium", per_kind, MEDIUM_SYSTEM, count),
        "long": _windowed_items(paragraphs, rng, "long", per_kind, LONG_SYSTEM, count),
    }


def reuse_report(pools: dict[str, list[Item]], paragraph_count: int) -> dict:
    """How often each real paragraph appears across the prose requests."""
    uses = [0] * paragraph_count
    for kind in ("medium", "long"):
        for item in pools.get(kind, []):
            for index in item.source_ids:
                uses[index] += 1
    total = sum(uses)
    return {
        "paragraphs": paragraph_count,
        "paragraph_uses": total,
        "max_uses_one_paragraph": max(uses) if uses else 0,
        "unused_paragraphs": sum(1 for u in uses if u == 0),
    }
