# Scale & Chaos Lab evidence (control plane, simulated workers)

What this is: the BatchForge API, its SQL claim/complete/recovery paths and its
migrations, run against an isolated local PostgreSQL, driven by **simulated
workers** that speak the real worker HTTP contract. Inference is a timer with
seeded service times and seeded token counts.

What this is **not**: any GPU measurement. No GPU, NVLink, NCCL or model
inference was used or measured here. The "workers" are processes on one Mac
(10 CPU cores), not machines or GPU nodes. Throughput numbers describe this
lab, not a managed database, not a GPU fleet, and not production.

## Runs

| Run | Workers | Tasks | Verdict | Wall | Tasks/s | Worker-side errors |
|---|---|---|---|---|---|---|
| R0 | 10 | 10,000 | PASS | 31.6 s | 316 | 0 |
| R1 | 100 | 100,000 | PASS | 192.3 s | 520 | 0 |

R1 was run twice on the final harness. The first attempt returned **FAIL**:
all 100,000 tasks completed and every invariant held, but 2 completions failed
on the client with `ReadError` (transport read failure). Those 2 attempts
expired in the database, were re-claimed (100,002 claims for 100,000 tasks),
and completed once each. The invariant "worker-side errors = 0" therefore
failed. The threshold was not changed. The rerun was clean. The first attempt's
pack was overwritten by the rerun, so only its numbers are recorded here.
The cause of the two `ReadError`s is **not established**: `api.log` at warning
level contains nothing about them. Treat them as an open item, not as explained.

## Invariants checked in SQL and by the simulated workers

Every task ends terminal; no task has two completed attempts; the accepted set
equals the completed set; batch counters reconcile; token totals match worker
usage; no stale or duplicate acceptance; no worker-side errors; idempotent
replay of 50 accepted completions. Verdicts are PASS only if all hold and the
generator stop rule passes (see below).

## Stop rule and measurement validity

The design (`docs/design/scale-chaos-lab.md`, section 8) says a run is invalid
if the load generator cannot keep up. The harness measures the simulated
fleet's event-loop overrun (how late a 100 ms sleep returns inside each worker
process) and marks the run `INVALID` above 50 ms p99. The first version of this
harness ran all workers in one process and measured observer lag; that measured
API latency, not the generator, and one 10-worker run was INVALID at 111 ms.
Moving the fleet to 4 processes and measuring the generator directly brought
the overrun down (R0 p99 3.1 ms, R1 p99 11.2 ms). R1 is valid under the stop rule.

## Latency (worker-side, milliseconds, local loopback)

| Run | claim p50 / p99 | complete p50 / p99 |
|---|---|---|
| R0 | see `summary.json` | see `summary.json` |
| R1 | 50.5 / 161.9 | 48.4 / 172.6 |

These include the time spent on a loaded single-host machine (10 cores shared
by 4 simulator processes, the API, and PostgreSQL). They are not isolated
service times.

## Deviations from production defaults (recorded in each `summary.json`)

- Retention payload budget: production 1 GiB. The default refused the first
  10,000-task batch with `429 retained_payload_limit`. R0 used 16 GiB, R1 used
  256 GiB. Observed charge in R1: about 118 GB charged for 100,000 tasks
  (about 1.18 MB per task), against a request body of a few hundred bytes per
  task. Whether that charge is intended is **an open question**, not a verdict.
- Retained-task and queued-task limits: production 100,000, lab 1,000,000.
- The PostgreSQL bundled with `pgserver` is **16.2**, not the PostgreSQL 17 the
  design names. The config records the version actually used.

## Reproduce

```bash
python -m ops.chaos.harness --rung R0 --workers 10 --processes 4 --tasks 10000 --retention-payload-gib 16 --out docs/evidence/scale-chaos/R0
python -m ops.chaos.harness --rung R1 --workers 100 --processes 4 --tasks 100000 --retention-payload-gib 256 --out docs/evidence/scale-chaos/R1
```

Needs `pgserver`, `psycopg`, `httpx`, `fastapi`, `uvicorn` (see `requirements.txt`).
Each pack's `config.json` records the code SHA-256 over `ops/chaos` and `backend`,
the git HEAD and whether that tree was dirty. Both packs in this commit were
produced from a clean tree at `485658b`; the code hash is `a65b3a09ebae…`.
