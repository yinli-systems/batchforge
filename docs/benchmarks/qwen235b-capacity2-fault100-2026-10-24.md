# Real Qwen235B worker capacity reduction and recovery

A fresh, single-batch 100-task workload completed after a worker SIGKILL and a controlled change in worker admission slots from **64 to 2 and back to 64**. All 100 tasks completed without failure, with one durable accepted result per task and matching usage. The same four-H100 inference engine stayed alive. This changes application admission capacity; it does not remove GPUs or reduce the model's physical capacity.

| Observed metric | Result |
| --- | ---: |
| Accepted / completed / failed | 100 / 100 / 0 |
| Accepted input / output tokens | 545,280 / 68,000 |
| Expired attempts / total attempt decisions | 64 / 164 |
| Runner wall time | 331.671180 seconds |
| Captured leases becoming retryable pending | 89.768 seconds |
| Registered admission slots | 64 → 2 → 64 |
| Positive reduced-capacity observations with pending work | 3 |
| In-flight attempts accepted during graceful drain | 2 |
| Graceful drain duration | 1.393774 seconds |
| Tasks pending before capacity restoration | 98 |
| Task completion P50 / P95 / P99 | 151.028 / 313.393 / 329.874 seconds |
| Assumed whole-Pod allocation at $16.10/hour | $1.483307 |
| Old heartbeat / completion writeback | 409 / 409 |

The replacement worker registered two slots. Three sampled observations showed pending work and between one and two running tasks, while the probe checked that registration remained two and running work never exceeded that bound. This is a short observed reduction window, not a sustained two-slot throughput benchmark. The probe then stopped that worker gracefully; both captured valid attempts were accepted. With 98 tasks still pending, it restored the original worker configuration and observed more than two running tasks again. The original 64-slot registration was restored, and the temporary worker configuration was removed.

All 64 leases captured before the crash expired and were reclaimed once under new attempt tokens. Both stale-token writes returned 409, and accepted state remained unchanged. The final snapshot contains 100 completions, zero pending/running/failed/cancelled tasks, no duplicate accepted tasks and equal task/attempt/batch usage. All fixed output lengths were retained. This establishes unique durable acceptance, not exactly-once execution on the GPU.

The inference engine, API processes and gateway retained their protected identities. Before restarting admission, the probe checked engine quiescence; recovered leases alone were not treated as proof that orphaned inference had released its KV allocation. The watchdog was deliberately paused for this controlled experiment and restored after recovery. These results do not measure automatic watchdog recovery or whole-Pod availability.

The broader sampling window was 332.567 seconds including output validation, with 303 NVML observations per device. Mean physical activity was 34.6%, 67.5%, 68.8% and 67.0%; those unweighted samples include the intentional interruption and lease wait. They do not measure useful FLOPs, steady serving utilization or exclusive workload attribution. The allocation scenario uses the 331.671180-second runner window and is neither an invoice nor marginal inference cost.

The fixed Qwen3-235B-A22B-Instruct-2507-FP8 workload, complete output lengths and schema11 match the finite workload used by the [worker-only](qwen235b-worker-fault100-2026-10-24.md) and [engine-loss](qwen235b-engine-fault100-2026-10-24.md) experiments. A fresh no-fault single-batch reference was still running when this package was created. No matched overhead or performance improvement is inferred here, and the older five-batch reference is not used as a control.

The [source-origin inventory](../../ops/reports/qwen235b-capacity2-fault100-v1-source-origin-2026-10-24.json) preserves all four original metadata files, their SHA256/byte counts, frozen source/fixture bindings and the externally anchored transport manifest. Local validation used the reviewed metadata validator v2, whose sole schema addition permits the existing producer's `recovery_before_original_worker` phase. No production query or runtime source change was performed while packaging these results.

This is one controlled application experiment on one Pod, not hardware GPU reset recovery, engine loss, cross-node migration or independent peer failover. The 235B model is an MoE with approximately 22B active parameters per token. Raw prompts, outputs, credentials, configuration and database dumps are excluded.
