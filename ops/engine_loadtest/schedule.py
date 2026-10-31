"""Poisson arrival schedules with a fixed class mix. Deterministic for a given seed."""
from __future__ import annotations

import random
from dataclasses import dataclass

from .workload import SPECS, Item


@dataclass(frozen=True)
class Planned:
    index: int
    at: float          # seconds from step start
    item: Item


def poisson_times(rate: float, duration: float, rng: random.Random) -> list[float]:
    if rate <= 0 or duration <= 0:
        raise ValueError("rate and duration must be positive")
    times, t = [], 0.0
    while True:
        t += rng.expovariate(rate)
        if t >= duration:
            return times
        times.append(t)


def build_schedule(pools: dict[str, list[Item]], rate: float, duration: float, seed: int) -> list[Planned]:
    """Arrivals at `rate` req/s for `duration` seconds.

    Each arrival picks a class by its share, then the next item of that class's
    pool, cycling through the pool in a seeded order. Items are reused only when
    the step needs more of a class than its pool holds, and that is reported.
    """
    rng = random.Random(seed)
    times = poisson_times(rate, duration, rng)
    kinds = list(SPECS)
    weights = [SPECS[k].share for k in kinds]
    orders = {}
    for kind in kinds:
        order = list(range(len(pools[kind])))
        rng.shuffle(order)
        orders[kind] = (order, 0)
    planned = []
    for index, at in enumerate(times):
        kind = rng.choices(kinds, weights=weights)[0]
        order, cursor = orders[kind]
        item = pools[kind][order[cursor % len(order)]]
        orders[kind] = (order, cursor + 1)
        planned.append(Planned(index=index, at=at, item=item))
    return planned


def mix_report(schedule: list[Planned], pools: dict[str, list[Item]]) -> dict:
    counts = {kind: 0 for kind in SPECS}
    for planned in schedule:
        counts[planned.item.kind] += 1
    total = len(schedule) or 1
    reused = {kind: max(0, counts[kind] - len(pools[kind])) for kind in SPECS}
    return {"requests": len(schedule),
            "share": {k: round(v / total, 3) for k, v in counts.items()},
            "pool_reuse_count": reused}
