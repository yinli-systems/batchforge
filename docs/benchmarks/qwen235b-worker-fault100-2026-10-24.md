# Real Qwen235B worker-loss recovery

A real worker SIGKILL during a fresh 100-task, single-batch Qwen3-235B-A22B-Instruct-2507-FP8 workload recovered all **100 tasks, with zero failures**. The 64 captured leases expired and became retryable pending work in **93.122 seconds**. Every task then had one durable accepted result and matching usage. Attempts can execute more than once; this result does not establish exactly-once GPU execution.

| Observed metric | Result |
| --- | ---: |
| Accepted / completed / failed | 100 / 100 / 0 |
| Accepted input / output tokens | 545,280 / 68,000 |
| Expired attempts / total attempt decisions | 64 / 164 |
| Runner wall time | 323.839 seconds |
| Task completion P50 / P95 / P99 | 147.160 / 292.518 / 321.921 seconds |
| Assumed whole-Pod allocation at $16.10/hour | $1.448280 |
| Old heartbeat / completion writeback | 409 / 409 |

Both stale requests returned `stale_attempt`; the accepted task state remained unchanged. All 64 captured tasks were reclaimed under new attempt tokens and accepted once. The final SQL snapshot reports zero pending/running/failed/cancelled tasks, zero duplicate accepted tasks, and equal task/attempt/batch usage totals. The original engine, APIs and gateway remained protected. The replacement worker and restored watchdog passed the probe's final checks.

This isolates leased-task recovery with the watchdog deliberately paused. The probe waited for captured leases to expire, checked engine quiescence, then restarted the worker. At that checkpoint the engine was already idle; the measured quiescence check took 0.067 seconds. That does **not** establish when old engine requests first stopped. The 93.122-second interval is an observed lease-recovery bound for this run, not an automatic watchdog restart time or a GPU execution duration.

Four physical GPUs were observed throughout the runner's sampling window: 297 samples per device, with mean NVML activity of 62.8%, 55.4%, 64.8% and 39.5%. These unweighted activity samples include the deliberate outage and lease wait; they are not useful FLOPs, kernel occupancy or steady serving utilization. The cost above includes the runner window, including recovery, retries, idle periods and validation. It is an allocation scenario, not an invoice or marginal inference cost.

A new no-fault **single-batch** reference is still required before attributing a throughput or cost delta to the fault. The earlier 250.149-second result used five batches and different source history, so it is not treated as a matched control here. The fixed work, complete output lengths, schema11, actual runtime fingerprint, six owned process identities, configuration/source hashes and exact original report links are preserved in the [source-origin inventory](../../ops/reports/qwen235b-worker-fault100-v1-source-origin-2026-10-24.json).

This is one controlled application worker-loss experiment on one four-H100 Pod. It does not test a hardware GPU reset, whole-Pod loss, independent compute failover or cross-node migration. The 235B model is an MoE with approximately 22B active parameters per token. Raw prompts, outputs, credentials, configuration and database dumps remain outside the report package.
