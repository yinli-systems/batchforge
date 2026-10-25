"""Simulated inference workers that speak the real worker HTTP contract.

Only inference is simulated: service time and token counts come from a seeded
generator. Registration, claim, heartbeat and completion use the production
endpoints and payloads, so the database sees the same SQL it sees in service.

Each in-flight task is heartbeated at the interval the API advertises at
registration, as the real worker does. Every failure is recorded with the
operation and either the HTTP status or the transport exception, so an error
count can always be traced to what produced it.
"""
from __future__ import annotations

import asyncio
import random
import time
from collections import Counter
from dataclasses import dataclass, field

import httpx


@dataclass
class Ledger:
    """What the simulated workers believe they delivered, for reconciliation."""
    accepted: dict[str, dict] = field(default_factory=dict)   # task id -> usage
    duplicates: int = 0
    stale: int = 0
    errors: int = 0
    errors_by_kind: Counter = field(default_factory=Counter)  # "<op>:<status|exception>" -> count
    stale_by_code: Counter = field(default_factory=Counter)   # "<op>:<error code>" -> count
    claims: int = 0
    heartbeats_ok: int = 0
    heartbeats_failed: int = 0
    latency_ms: dict[str, list[float]] = field(default_factory=dict)  # op -> request latencies

    def fail(self, op: str, kind: str) -> None:
        self.errors += 1
        self.errors_by_kind[f"{op}:{kind}"] += 1

    def timed(self, op: str, started: float) -> None:
        elapsed = (time.monotonic() - started) * 1000
        self.latency_ms.setdefault(op, []).append(round(elapsed, 3))


def _error_code(reply: httpx.Response) -> str:
    try:
        return str(reply.json().get("error", {}).get("code", "unknown"))
    except ValueError:
        return "unparsed"


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
        heartbeat_s = float(registration.json()["heartbeat_interval_seconds"])
        inflight: set[asyncio.Task] = set()

        async def heartbeat(task_id: str, attempt_token: str, done: asyncio.Event) -> None:
            while True:
                try:
                    await asyncio.wait_for(done.wait(), timeout=heartbeat_s)
                    return
                except asyncio.TimeoutError:
                    pass
                try:
                    reply = await http.post(f"/internal/tasks/{task_id}/heartbeat",
                                            json={"worker_id": worker_id, "attempt_token": attempt_token})
                    if reply.status_code == 200:
                        ledger.heartbeats_ok += 1
                    else:
                        ledger.heartbeats_failed += 1
                except httpx.HTTPError:
                    ledger.heartbeats_failed += 1

        async def serve(task: dict) -> None:
            done = asyncio.Event()
            beat = asyncio.create_task(heartbeat(task["id"], task["attempt_token"], done))
            try:
                await asyncio.sleep(rng.uniform(*service_ms) / 1000)
                completion = rng.randint(1, 64)
                prompt = rng.randint(1, 256)
                response = {
                    "id": f"sim-{task['id']}", "object": "chat.completion", "model": model,
                    "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}],
                    "usage": {"prompt_tokens": prompt, "completion_tokens": completion},
                }
                body = {"worker_id": worker_id, "attempt_token": task["attempt_token"], "response": response}
                started = time.monotonic()
                try:
                    reply = await http.post(f"/internal/tasks/{task['id']}/complete", json=body)
                except httpx.HTTPError as exc:
                    ledger.fail("complete", type(exc).__name__)
                    return
                ledger.timed("complete", started)
                if reply.status_code == 409:
                    ledger.stale += 1
                    ledger.stale_by_code[f"complete:{_error_code(reply)}"] += 1
                    return
                if reply.status_code != 200:
                    ledger.fail("complete", str(reply.status_code))
                    return
                if reply.json().get("duplicate"):
                    ledger.duplicates += 1
                    return
                ledger.accepted[task["id"]] = {"prompt_tokens": prompt, "completion_tokens": completion,
                                               "attempt_token": task["attempt_token"], "worker_id": worker_id}
            finally:
                done.set()
                await beat

        while not stop.is_set():
            inflight = {t for t in inflight if not t.done()}
            free = slots - len(inflight)
            if free > 0:
                started = time.monotonic()
                try:
                    claimed = await http.post("/internal/tasks/claim", json={
                        "worker_id": worker_id, "model": model, "limit": free})
                except httpx.HTTPError as exc:
                    ledger.fail("claim", type(exc).__name__)
                    await asyncio.sleep(poll_s)
                    continue
                if claimed.status_code != 200:
                    ledger.fail("claim", str(claimed.status_code))
                    await asyncio.sleep(poll_s)
                    continue
                ledger.timed("claim", started)
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


def merge(ledgers: list[Ledger]) -> Ledger:
    """Combine ledgers from worker processes into one reconciliation view."""
    merged = Ledger()
    for ledger in ledgers:
        merged.accepted.update(ledger.accepted)
        merged.duplicates += ledger.duplicates
        merged.stale += ledger.stale
        merged.errors += ledger.errors
        merged.errors_by_kind.update(ledger.errors_by_kind)
        merged.stale_by_code.update(ledger.stale_by_code)
        merged.claims += ledger.claims
        merged.heartbeats_ok += ledger.heartbeats_ok
        merged.heartbeats_failed += ledger.heartbeats_failed
        for op, values in ledger.latency_ms.items():
            merged.latency_ms.setdefault(op, []).extend(values)
    return merged


def run_worker_group(first: int, count: int, options: dict, stop_flag, results) -> None:
    """Entry point for one worker process: runs `count` simulated workers until `stop_flag` is set.

    Simulated workers are split across processes so that the load generator does not
    become the bottleneck on one interpreter. The parent records each process's CPU time.
    """
    import resource

    ledger = Ledger()
    overrun_ms: list[float] = []

    async def main() -> None:
        stop = asyncio.Event()
        sims = [asyncio.create_task(run_worker(i, ledger=ledger, stop=stop, **options))
                for i in range(first, first + count)]

        async def bridge() -> None:
            # Event-loop overrun of this process: how late a 100 ms sleep returns while the simulated
            # workers run. Above the stop-rule limit, the generator, not the control plane, is the bottleneck.
            while not stop_flag.is_set():
                before = time.monotonic()
                await asyncio.sleep(0.1)
                overrun_ms.append(max(0.0, time.monotonic() - before - 0.1) * 1000)
            stop.set()

        await asyncio.gather(bridge(), *sims)

    asyncio.run(main())
    usage = resource.getrusage(resource.RUSAGE_SELF)
    results.put({"ledger": ledger, "cpu_s": round(usage.ru_utime + usage.ru_stime, 3), "workers": count,
                 "loop_overrun_ms": overrun_ms})
