"""Open-loop sender: each request leaves at its planned time, whatever the server is doing.

Responses are streamed so time-to-first-token is measured. A request that would
exceed `max_inflight` is recorded as dropped rather than skipped, so overload
shows up in the results instead of disappearing.
"""
from __future__ import annotations

import asyncio
import json
import time
from dataclasses import asdict, dataclass
from typing import Callable

import httpx

from .schedule import Planned

Clock = Callable[[], float]


@dataclass
class Record:
    id: str
    kind: str
    planned_at: float
    sent_at: float | None = None
    lag: float | None = None
    dropped: bool = False
    status: int | None = None
    ttft: float | None = None
    latency: float | None = None
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    text: str = ""  # full output; classes cap at 200 tokens, so this stays small
    error: str | None = None

    def as_json(self) -> dict:
        return asdict(self)


async def _send(http: httpx.AsyncClient, url: str, model: str, planned: Planned, record: Record,
                clock: Clock, start: float) -> None:
    item = planned.item
    body = {
        "model": model,
        "messages": item.messages,
        "max_tokens": item.max_tokens,
        "temperature": 0,
        "stream": True,
        "stream_options": {"include_usage": True},
    }
    if item.json_object:
        body["response_format"] = {"type": "json_object"}
    pieces: list[str] = []
    try:
        async with http.stream("POST", url, json=body) as response:
            record.status = response.status_code
            if response.status_code != 200:
                await response.aread()
                record.error = f"HTTP {response.status_code}"
                return
            async for line in response.aiter_lines():
                if not line.startswith("data: ") or line == "data: [DONE]":
                    continue
                chunk = json.loads(line[6:])
                for choice in chunk.get("choices") or []:
                    delta = (choice.get("delta") or {}).get("content")
                    if delta:
                        if record.ttft is None:
                            record.ttft = clock() - record.sent_at
                        pieces.append(delta)
                usage = chunk.get("usage")
                if usage:
                    record.prompt_tokens = usage["prompt_tokens"]
                    record.completion_tokens = usage["completion_tokens"]
    except (httpx.HTTPError, ValueError) as exc:
        record.error = type(exc).__name__
    finally:
        record.latency = clock() - record.sent_at
        record.text = "".join(pieces)


async def run_open_loop(schedule: list[Planned], *, base_url: str, model: str,
                        max_inflight: int = 512, timeout: float = 900.0,
                        http: httpx.AsyncClient | None = None,
                        clock: Clock = time.monotonic, sleep=asyncio.sleep) -> list[Record]:
    url = base_url.rstrip("/") + "/v1/chat/completions"
    owns_client = http is None
    client = http or httpx.AsyncClient(timeout=httpx.Timeout(timeout, connect=10),
                                        limits=httpx.Limits(max_connections=max_inflight))
    records: list[Record] = []
    tasks: list[asyncio.Task] = []
    inflight = 0
    start = clock()

    async def tracked(planned: Planned, record: Record) -> None:
        nonlocal inflight
        try:
            await _send(client, url, model, planned, record, clock, start)
        finally:
            inflight -= 1

    try:
        for planned in schedule:
            target = start + planned.at
            delay = target - clock()
            if delay > 0:
                await sleep(delay)
            record = Record(id=planned.item.id, kind=planned.item.kind, planned_at=planned.at)
            records.append(record)
            if inflight >= max_inflight:
                record.dropped = True
                continue
            record.sent_at = clock()
            record.lag = record.sent_at - target
            inflight += 1
            tasks.append(asyncio.create_task(tracked(planned, record)))
        await asyncio.gather(*tasks)
    finally:
        if owns_client:
            await client.aclose()
    return records
