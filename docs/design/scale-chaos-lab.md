# Scale & Chaos Lab: design

Status: design only. Nothing here has been run.
Owner: BatchForge control plane. Branch: `feat/gpu-inventory-evidence`.

## 1. Purpose

Measure the BatchForge control plane under many workers, large task counts and
injected faults, without claiming anything about GPU execution. The lab answers
three questions:

1. Where does the PostgreSQL-backed control plane stop being fast enough, and
   which operation hits the limit first (claim, heartbeat, completion, recovery)?
2. Do the safety invariants (exactly-once acceptance, fencing, token accounting)
   hold under overload and faults at every scale we can reach?
3. How long does recovery take after worker loss, lease storms and database
   interruptions, and does it degrade gracefully?

## 2. What is and is not simulated

| Component | Real or simulated | Notes |
|---|---|---|
| BatchForge API, claim/complete/fail/recover SQL | **Real** | Same code and migrations as production |
| PostgreSQL 17 | **Real**, local | Isolated instance (as in `devtools/run_checks.py`), never the managed Neon database |
| Workers | **Simulated** | Speak the real worker HTTP contract; inference replaced by a timer with seeded service times |
| Inference engine | **Simulated** | Token counts and latencies drawn from fixed, recorded distributions |
| GPUs, NVLink, NCCL | **Not used** | The lab makes no claim about them |
| Network | Real loopback, plus optional fault proxy | Partitions and latency are injected, not measured on real links |

Every result must carry the label `simulated-workers` in its evidence pack.

## 3. Architecture

- **Control plane under test**: one or two API processes against one PostgreSQL
  primary, same settings as production (`DB_POOL_MAX`, timeouts, lock order).
- **Worker fleet**: N asyncio worker processes. Each registers, claims bounded
  batches, sends heartbeats at the advertised interval, and completes or fails
  tasks after a sampled service time. Workers run on the Mac, not on the GPU pod.
- **Load generator**: open-loop arrival schedule (seeded Poisson), reusing the
  schedule code from `ops/engine_loadtest`, so arrival is independent of completions.
- **Fault injector**: a scheduled list of faults applied at fixed offsets:
  worker kill, heartbeat blackhole for a named worker set, lease-expiry storm
  (all leases for a fraction of workers stop renewing at once), database
  connection drop (pool reset), and slow-query injection via `pg_sleep` inside a
  controlled session. No fault touches the production database.
- **Observer**: samples `pg_stat_activity`, lock waits, `pg_stat_statements`,
  API latency histograms and queue depth every second.

## 4. Scale ladder

Each rung runs only after the previous one passes its invariants.

| Rung | Workers | Tasks per run | Duration | Purpose |
|---|---|---|---|---|
| R0 | 10 | 10,000 | 10 min | Harness correctness |
| R1 | 100 | 100,000 | 30 min | Baseline control-plane throughput |
| R2 | 1,000 | 1,000,000 | 60 min | Overload and lock contention |
| R3 | 1,000 | 1,000,000 | 60 min + faults | Recovery under chaos |
| R4 | 10,000 | 1,000,000 | 60 min | Stop-point search; expected to fail somewhere |

The ladder is about the control plane. Reaching R2 does not mean "1,000 GPUs".
The worker count is simulated concurrency, and the document must say so.

## 5. Invariants (hard failures)

A run fails if any of these is violated, regardless of throughput:

1. **Exactly-once acceptance**: each task has at most one accepted completion.
2. **Fencing**: no completion or failure with a superseded or expired attempt
   token is accepted.
3. **No loss**: every submitted task ends in `completed`, `failed` or
   `cancelled` once the run is drained; none stays `pending` or `running`
   without a live lease.
4. **Reconciliation**: batch counters equal the number of task rows in each
   state, and tenant token totals equal the sum of accepted usage.
5. **Idempotent replay**: a replayed completion with the same token returns the
   same result and changes nothing.

Invariant checks run as SQL after every drain, and the results are part of the
evidence pack.

## 6. Metrics

Per operation (claim, heartbeat, complete, fail, recover):

- request rate, p50/p95/p99 latency, error and 409 (stale) rates;
- database time per call, lock-wait time, and connection-pool wait.

Whole run:

- accepted completions per second, queue depth and oldest pending age;
- expired-lease count and recovery time (time from fault to the last affected
  task returning to a live state);
- duplicate-acceptance count (must be 0) and stale-result rejection count.

## 7. Fault scenarios

| Scenario | Injection | Expected behaviour | Pass condition |
|---|---|---|---|
| F1 Worker crash | kill 5% of workers mid-run | Leases expire, tasks re-queue | No loss; recovery within 2 lease periods |
| F2 Heartbeat blackhole | drop heartbeats for 10% of workers for 3 lease periods | Tasks reassigned; original worker's late completion rejected | No duplicate acceptance; stale results 409 |
| F3 Lease storm | expire all leases for 20% of workers at once | Recovery sweep stays bounded | Sweep latency and queue depth stay bounded; invariants hold |
| F4 DB connection reset | reset the pool every 30 s for 5 min | Retries succeed, no double accept | Invariants hold; 503 rate matches retries |
| F5 Slow query | 2 s lock hold on batch rows | Claims back off, no deadlock | No deadlock errors; p99 claim latency reported |

Each scenario runs against R1 first, then R3.

## 8. Stop rules and invariants of the experiment itself

- The generator must keep up: client lag p99 below 50 ms, or the run is invalid.
- Stop a run early if the oldest pending task is older than its deadline by more
  than 10 minutes, and record the point where that happened.
- A run that shows a hard-invariant violation is not reported as a throughput
  result. It becomes a bug report.

## 9. Evidence pack per run

Each run writes, into `docs/evidence/scale-chaos/<run-id>/`:

- `config.json`: rung, seed, worker count, arrival rate, fault list, git SHA,
  migration version, PostgreSQL version and settings;
- `summary.json`: the metrics in §6 and the invariant results in §5;
- `observer.jsonl`: one-second samples;
- `README.md`: what was simulated, what was not, and the pass or fail verdict.

Raw traces stay local. Summaries and configs are committed.

## 10. Prerequisites and blockers

- **Database**: the lab needs a local PostgreSQL 17. The managed Neon database
  is not used because it is over quota and must not receive test load.
- **Host**: run on the Mac. The GPU pod's CPUs are busy with the inference engine,
  and mixing them would contaminate both measurements.
- **Schema**: use the current migrations, applied to the lab database only.

## 11. Timeline

1. Harness and invariant checks, R0 (about 1 week).
2. R1 baseline and tuning of the worker simulator so it is not the bottleneck.
3. R2 overload, then the fault scenarios F1–F5 on R1 and R3.
4. R4 stop-point search. Report the first failing operation and the limit.

## 12. Known risks

- The Python worker simulator may become the bottleneck before the database does.
  The harness must report its own CPU use and client lag to rule this out.
- Local PostgreSQL behaves differently from managed PostgreSQL in I/O and
  connection limits. Results describe this lab, not Neon.
- "1,000 workers" on one machine is 1,000 concurrent clients, not 1,000
  machines. Network and failure-domain effects are not represented.

## 13. Out of scope

- Any GPU measurement, NCCL or interconnect claim.
- Multi-region behaviour and consensus.
- Tenant fairness across real customers.
