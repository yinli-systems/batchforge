# Real Qwen235B engine-loss recovery

A controlled loss of the owned engine and worker on one four-H100 Pod recovered **100 real tasks, with zero failures**. The experiment used Qwen3-235B-A22B-Instruct-2507-FP8, a single batch and the frozen 20-cell mixed workload repeated five times. The worker was SIGKILLed; the engine was stopped through its existing managed lifecycle and reloaded. All 64 captured valid leases expired, became retryable, and were accepted under replacement attempts. Every task had one durable accepted result and matching token usage. GPU inference can execute more than once.

| Observed metric | Result |
| --- | ---: |
| Accepted / completed / failed | 100 / 100 / 0 |
| Accepted input / output tokens | 545,280 / 68,000 |
| Expired attempts / total attempt decisions | 64 / 164 |
| Runner wall time | 396.904711 seconds |
| Captured leases becoming retryable pending | 89.562 seconds |
| Engine load request to observed ready | 59.6866 seconds |
| Observed engine unavailability after confirmed stop | 153.524 seconds |
| Task completion P50 / P95 / P99 | 216.611 / 388.225 / 388.920 seconds |
| Assumed whole-Pod allocation at $16.10/hour | $1.775046 |
| Old heartbeat / completion writeback | 409 / 409 |

The lease-recovery and model-load intervals are contained within the observed unavailability interval; they are not separate downtime totals to add together. Unavailability starts after the probe confirmed engine loss and ends when readiness was observed. It includes the deliberate lease wait, checks and model reload. This is a sampled application recovery measurement, not a hardware failure timestamp or automatic watchdog failover time. The watchdog was deliberately paused and restored after the pair passed its checks.

Both stale-token writebacks were rejected with 409, and the accepted state remained unchanged. The final proof reports all 100 tasks completed, one accepted result per task, matching task/attempt usage and no simulated responses. All fixed output lengths were retained. The two API processes and gateway kept their protected identities; the probe reported no HTTP polling errors. These checks establish sampled continuity, not a continuous availability guarantee. Engine and worker resumed with new owned identities, and the watchdog was restored.

At the sampled pair-down checkpoint, all four H100s reported 0 MiB of used device memory. The benchmark's broader sampler window was 405.608 seconds, including output validation; its per-device NVML statistics contain 225 observations each. Their mean activity was 83.9%, 81.0%, 53.7% and 74.0%. Missing device observations are not filled with zero, and these unweighted means do not establish sustained occupancy, useful FLOPs or exclusive workload attribution. The cost scenario uses the 396.904711-second runner window, including deliberate outage, recovery and retries; it is not an invoice or marginal inference cost.

The [worker-only loss experiment](qwen235b-worker-fault100-2026-10-24.md) kept the engine alive. This run separately tests loss of application serving capacity and model reload. A fresh no-fault single-batch reference is still required before attributing any throughput, latency or cost difference to either fault. The earlier five-batch result is not treated as a matched control. No such overhead comparison is claimed here.

The exact four original metadata artifacts, their crosslinks, frozen source/fixture hashes and the externally anchored transport manifest are preserved in the [source-origin inventory](../../ops/reports/qwen235b-engine-fault100-v1-source-origin-2026-10-24.json). Local validation re-ran the frozen metadata validator and checked all task totals, 164 decisions, 64 expirations, original byte counts and SHA256 values. No production query was performed while creating this package.

This is one controlled experiment on one Pod. It does not establish hardware GPU reset recovery, whole-Pod availability, independent peer failover or cross-node migration. The model is an MoE with approximately 22B active parameters per token, rather than a dense 235B model. Raw prompts, outputs, credentials, configuration and database dumps are excluded.
