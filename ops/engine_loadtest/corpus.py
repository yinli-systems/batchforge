"""Real text for the engine load test.

Two sources, both on disk and both real:

* Job listings (CSV) with a recorded `early_career` flag. The flag is a real
  label, so short classification requests can be scored for correctness.
* Prose paragraphs from the project's own Markdown docs. They have no labels,
  so long and medium requests are scored for format validity only.

Nothing here is generated or paraphrased.
"""
from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from pathlib import Path

csv.field_size_limit(10**7)

SKIP_PREFIXES = ("#", "|", "```", "- ", "* ", "<", "!", "[")
MIN_PARAGRAPH_CHARS = 200


@dataclass(frozen=True)
class Job:
    text: str
    early_career: bool


def load_jobs(path: Path, limit: int | None = None) -> list[Job]:
    """Rows with a recorded early_career flag, de-duplicated on title and company."""
    seen: set[tuple[str, str]] = set()
    jobs: list[Job] = []
    with path.open(encoding="utf-8", errors="replace", newline="") as handle:
        for row in csv.DictReader(handle):
            flag = (row.get("early_career") or "").strip()
            title = (row.get("title") or "").strip()
            company = (row.get("company") or "").strip()
            if flag not in {"0", "1"} or not title or not company:
                continue
            key = (title.lower(), company.lower())
            if key in seen:
                continue
            seen.add(key)
            location = (row.get("location") or "").strip()
            text = f"{title} at {company}" + (f" ({location})" if location else "")
            jobs.append(Job(text=text, early_career=flag == "1"))
            if limit and len(jobs) >= limit:
                break
    return jobs


def load_paragraphs(paths: list[Path]) -> list[str]:
    """Prose paragraphs from Markdown: blank-line separated, no headings, tables or code."""
    paragraphs: list[str] = []
    for path in sorted(paths):
        in_code = False
        block: list[str] = []
        for line in path.read_text(encoding="utf-8").splitlines() + [""]:
            if line.strip().startswith("```"):
                in_code = not in_code
                continue
            if in_code:
                continue
            if line.strip():
                block.append(line.strip())
                continue
            text = " ".join(block)
            block = []
            if len(text) >= MIN_PARAGRAPH_CHARS and not text.startswith(SKIP_PREFIXES):
                paragraphs.append(re.sub(r"\s+", " ", text))
    return paragraphs
