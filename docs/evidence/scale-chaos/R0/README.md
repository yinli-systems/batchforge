# R0: harness correctness, 10 simulated workers, 10,000 tasks

Verdict: **passed** (all five invariants and the replay check).

## What was real and what was simulated

- Real: BatchForge API (uvicorn subprocess), PostgreSQL 17 (isolated, local),
  migrations 1 to 11, the SQL claim, completion and recovery paths, the worker
  HTTP contract, and the SQL invariant checks.
- Simulated: the workers (10 asyncio clients, 4 slots each) and the inference
  step (service time 5 to 40 ms, token counts from a seeded generator).
- Not used: any GPU, NCCL, or interconnect. Nothing here describes GPU execution.

## Result

- 10,000 tasks completed in 22.2 s, 450 tasks/s through the control plane.
- 10,000 claims, 0 stale results, 0 duplicates, 0 worker-side errors.
- 50 replayed completions: all idempotent.
- Completion tokens 324,227 and prompt tokens 1,288,389 match on both the worker and database sides.

## Deviation from production defaults (important)

Under the shipped retention defaults (migration 0009), the 10,000-task batch was
**refused** with HTTP 429 `retained_payload_limit`. Each task reserves 1 MiB of
response space plus error space, against a 1 GiB per-tenant payload budget.
The harness recorded that refusal (`default_limit_probe`), then raised the
lab tenant's budget to 16 GiB and resubmitted. The raise is recorded under
`retention_limits_raised` with `deviation_from_default: true`.

This is a finding about the product, not about the lab. Any large batch is refused
under defaults. Whether reservation should be per task or per batch is a design
decision that needs an owner.

## Reproduce

```bash
python -m ops.chaos.harness --workers 10 --tasks 10000 --out docs/evidence/scale-chaos/R0
```

Requires the pinned dependencies (`requirements.lock`) and `pgserver==0.1.4`.
