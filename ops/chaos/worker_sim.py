"""Simulated inference workers that speak the real worker HTTP contract.

Only inference is simulated: service time and token counts come from a seeded
generator. Registration, claim, heartbeat and completion use the production
endpoints and payloads, so the database sees the same SQL it sees in service.
"""
from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass, field

import httpx


@dataclass
class Ledger:
    """What the simulated workers believe they delivered, for reconciliation."""
    accepted: dict[str, dict] = field(default_factory=dict)   # task id -> usage
    duplicates: int = 0
    stale: int = 0
    errors: int = 0
    claims: int = 0


async def run_worker(index: int, *, base_url: str, token: str, model: str, slots: int,
                     service_ms: tuple[float, float], seed: int, ledger: Ledger,
                     stop: asyncio.Event, poll_s: float = 0.05) -> None:
    rng = random.Random(seed * 100003 + index)
    headers = {"Authorization": f"Bearer {token}"}
    name = f"sim-{index:04d}"
    async with httpx.AsyncClient(base_url=base_url, headers=headers, timeout=30,
                                 limits=httpx.Limits(max_connections=slots + 4)) as http:
        registration = await http.post("/internal/workers/register", json={
            "name": name, "model": model, "concurrency": slots,
            "metadata": {"engine_count": 1, "ready_engine_count": 1},
        })
        registration.raise_for_status()
        worker_id = registration.json()["worker_id"]
        inflight: set[asyncio.Task] = set()

        async def serve(task: dict) -> None:
            await asyncio.sleep(rng.uniform(*service_ms) / 1000)
            completion = rng.randint(1, 64)
            prompt = rng.randint(1, 256)
            response = {
                "id": f"sim-{task['id']}", "object": "chat.completion", "model": model,
                "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": prompt, "completion_tokens": completion},
            }
            body = {"worker_id": worker_id, "attempt_token": task["attempt_token"], "response": response}
            try:
                reply = await http.post(f"/internal/tasks/{task['id']}/complete", json=body)
            except httpx.HTTPError:
                ledger.errors += 1
                return
            if reply.status_code == 409:
                ledger.stale += 1
                return
            if reply.status_code != 200:
                ledger.errors += 1
                return
            if reply.json().get("duplicate"):
                ledger.duplicates += 1
                return
            ledger.accepted[task["id"]] = {"prompt_tokens": prompt, "completion_tokens": completion,
                                           "attempt_token": task["attempt_token"], "worker_id": worker_id}

        while not stop.is_set():
            inflight = {t for t in inflight if not t.done()}
            free = slots - len(inflight)
            if free > 0:
                try:
                    claimed = await http.post("/internal/tasks/claim", json={
                        "worker_id": worker_id, "model": model, "limit": free})
                    claimed.raise_for_status()
                except httpx.HTTPError:
                    ledger.errors += 1
                    await asyncio.sleep(poll_s)
                    continue
                tasks = claimed.json()["tasks"]
                ledger.claims += len(tasks)
                for task in tasks:
                    inflight.add(asyncio.create_task(serve(task)))
                if not tasks:
                    await asyncio.sleep(poll_s)
            else:
                await asyncio.wait(inflight, return_when=asyncio.FIRST_COMPLETED)
        if inflight:
            await asyncio.gather(*inflight, return_exceptions=True)
