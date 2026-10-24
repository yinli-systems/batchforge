# Qwen235B policy160: complete work, invalid arrival comparison

Both real cohorts completed160 tasks in8 homogeneous20-task batches with one accepted attempt per task, no retries/failures, identical174,080 input and12,800 output tokens. The cost-aware run failed its admission-arrival protocol, so these results do not establish a scheduling speedup or cost saving. Both rounds observed a committed L/S overlap in a read-only sample; the candidates visible and lock-eligible to a later claim remain unobserved.

The evidence is the original-byte [fair report](../../ops/reports/qwen235b-policy-fair160-v1-report-2026-10-24.json) and [cost-aware report](../../ops/reports/qwen235b-policy-cost160-v1-report-2026-10-24.json), their [fair plan](../../ops/reports/qwen235b-policy-fair160-v1-plan-2026-10-24.json), [fair journal](../../ops/reports/qwen235b-policy-fair160-v1-journal-2026-10-24.json), [cost plan](../../ops/reports/qwen235b-policy-cost160-v1-plan-2026-10-24.json) and [cost journal](../../ops/reports/qwen235b-policy-cost160-v1-journal-2026-10-24.json). The [source-origin inventory](../../ops/reports/qwen235b-policy160-source-origin-2026-10-24.json) records all six SHA/size/path bindings and the two externally supplied transport-manifest SHAs. The strict6063 metadata validator rechecked the actual bytes locally; this analysis made no production calls. The earlier operator-source inventory describes its preparation stage; these later reports are separate execution evidence.

The runtime was one Pod with four H100 GPUs, TP4, Qwen3-235B-A22B-Instruct-2507-FP8 (MoE, not dense235B), eager execution, prefix caching off,64 claimed-task capacity and a298,384-token shared admission budget. The source/config/profile/price controls match between rounds apart from the selected API scheduling policy. The protected engine/worker/gateway generation stayed fixed; API/watchdog generations changed with the policy roll. The controls do not establish identical connection-pool warmth, query plans or background maintenance state. The profile retains projected static16 rates and a$1.00625 lane-hour proxy;64 claims are not64 independently priced execution slots. With one compatible same-price profile, normal nonurgent/nonaged cost ordering is monotonic with predicted service time. This experiment cannot validate heterogeneous placement.

## Offered trace and the failed gate

Each round offered four L batches at0.0/0.1/0.2/0.3s, followed by four S batches at2.0/2.1/2.2/2.3s. L tasks used2,048 input/128 output tokens; S tasks128/32. All8 inputs were preuploaded. No POST was retried and no extra tasks were added to repair the trace.

Recorded start lateness stayed below0.25s in both rounds. The additional gate required **all four L admission ACKs before the first S request start**. Fair passed: last L ACK1.355001s < first S start2.040465s. Cost-aware failed: last L ACK6.836817s > first S start2.040149s. Its individual `arrival_protocol_valid` and `pair_arrival_comparable` are both false. Fair's true field is an individual-round result and does not make the overall pair comparable.

| Batch | Offered s | Fair start s | Fair ACK s | Fair start→ACK s | Cost start s | Cost ACK s | Cost start→ACK s |
|---|---:|---:|---:|---:|---:|---:|---:|
| L0 | 0.0 | 0.042 | 0.553 | 0.511 | 0.045 | 2.579 | 2.534 |
| L1 | 0.1 | 0.137 | 0.832 | 0.695 | 0.139 | 4.630 | 4.491 |
| L2 | 0.2 | 0.236 | 1.079 | 0.842 | 0.255 | 6.157 | 5.902 |
| L3 | 0.3 | 0.336 | 1.355 | 1.019 | 0.339 | 6.837 | 6.498 |
| S0 | 2.0 | 2.040 | 2.485 | 0.445 | 2.040 | 8.100 | 6.060 |
| S1 | 2.1 | 2.138 | 2.832 | 0.695 | 2.138 | 9.178 | 7.040 |
| S2 | 2.2 | 2.236 | 3.041 | 0.805 | 2.238 | 7.453 | 5.215 |
| S3 | 2.3 | 2.358 | 3.251 | 0.893 | 2.338 | 9.782 | 7.444 |

These monotonic sender intervals include arrival-journal persistence plus network/server processing. They are not isolated server lock waits or pure HTTP transport time. Cost S2 acknowledged before S0/S1 despite the offered order. A batch `created_at` is transaction-start `now()` and cannot establish insertion, COMMIT, acceptance order or the candidate set seen by another transaction.

## Observed completion and allocation

The following values describe these two runs; they are not a valid causal policy comparison. Whole-Pod allocation uses the explicit$16.10/hour scenario, not an invoice. Full-operation time starts before preuploads and ends after output verification/final drain; it excludes preparation, policy rollout and final report writes.

| Recorded quantity | Fair | Cost-aware |
|---|---:|---:|
| T0_to_terminal_seconds | 29.967055 | 32.869298 |
| full_operation_seconds | 44.116643 | 53.647244 |
| T0_to_terminal_usd | 0.134019 | 0.146999 |
| full_operation_usd | 0.197299 | 0.239922 |
| Tasks/s from T0 to terminal observation | 5.339197 | 4.867765 |
| Tasks/s across full operation | 3.626749 | 2.982446 |

DB batch-transaction-start→task SQL-finish tails follow. Each cell uses80 tasks/class/round. They include admission/dispatch/execution/reporting effects and stop before durable COMMIT/ACK; they are not pure pending-queue, GPU, kernel or TTFT timings.

| Class | Policy | P50 s | P95 s | P99 s | Task samples |
|---|---|---:|---:|---:|---:|
| L | fair | 18.426172 | 28.123428 | 28.230007 | 80 |
| L | cost_aware | 22.580326 | 28.246495 | 28.348947 | 80 |
| S | fair | 19.938955 | 22.138624 | 22.404758 | 80 |
| S | cost_aware | 9.803622 | 15.185292 | 15.352762 | 80 |

Client batch timings use only four batches/class/round, a different population:

| Class | Client quantity | Fair P95 s | Cost P95 s | Population |
|---|---|---:|---:|---|
| L | scheduled_to_client_admission_ACK_seconds | 1.028543 | 6.449864 | 4/class/round |
| L | scheduled_to_client_batch_terminal_observation_seconds | 28.353781 | 32.472860 | 4/class/round |
| S | scheduled_to_client_admission_ACK_seconds | 0.934506 | 7.421768 | 4/class/round |
| S | scheduled_to_client_batch_terminal_observation_seconds | 23.289418 | 20.130566 | 4/class/round |

The shorter cost S DB tail (P9515.185292s versus22.138624s) does not establish a causal benefit because the acceptance timeline differed. Nor does sampled overlap establish why a particular later claim selected its batch. The SQL function first bounds each tenant's oldest64 candidates, selects one batch and then claims within that batch; it neither globally sorts every batch nor preempts already claimed work or the worker's FIFO KV admission queue.

## What the admission evidence does and does not explain

The slowdown began before this workload created any batch. Confirmed-upload event gaps were roughly0.74–0.84s throughout fair preupload, while the last four cost gaps were1.413s,2.059s,2.191s and2.900s. Those gaps include the upload and surrounding source/readiness/DB checks; they are not upload-only latency. Cost's first recorded batch request also took2.533569s from sender start to ACK. A successful claim on the new workload alone cannot explain a preupload slowdown when no workload tasks had yet been admitted.

The cost preparation ledger retained238,205,802 charged bytes versus205,947,482 for fair, a difference of **32,258,320 bytes**, exactly the prior fair160 reservation. The retained-task count also rose by exactly160. No net refund is visible in those aggregate preparation counters. They do not identify per-batch charges or establish when individual purge work ran. Archive/purge activity is a concrete potential competing workload, alongside shared database latency, pool wait, object I/O and query planning. These metadata do not record whether any of those occurred during the admission stall; none is asserted as its measured cause. The final read-only proof transaction took0.106690s in fair and0.325268s in cost; that bracket includes its multiple SQL statements and network/transaction work, so it is further timing context, not an isolated RTT measurement.

The code and a new synthetic Unix-only PG16 test establish a possible lock convoy: claim holds worker→queue→batch→task; admission takes the tenant umbrella and later waits for that queue. While waiting, admission can block other same-tenant admission/input-lease operations. Claim alone did not block a tenant `NO KEY UPDATE` in that test. This demonstrates the mechanism, not its occurrence or duration in either real run. Synthetic local calls were8–29ms and the packaged local server had no JIT available; those results cannot rule in/out production JIT or WAN effects.

A separate, not-deployed observability proposal measures fixed stages for input read/validation, pool acquisition, BEGIN, first idempotency tenant lock, retention/queue work, task insertion and commit/rollback. All timings include awaits and DB roundtrips; actual PG lock-wait attribution still needs a bounded read-only server observer. No tenant/quota/idempotency/history/reader fence was removed, no migration was written, and the policy v1 workloads were not replayed. A future controlled investigation must preserve these invalid v1 flags and collect stage/background evidence before choosing a performance fix.

The experiment remains N=1, serial and one-Pod. Fixed output lengths measure controlled work rather than answer quality. It proves accepted-result/token integrity under both policies and exposes an admission bottleneck; it does not prove compute HA, multi-GPU heterogeneity, global SJF/fairness guarantees, exactly-once GPU execution or daily-scale capacity.
