# Preregistered post-Fleet real workload

After the guarded native Fleet release and PostgreSQL17 CI for `748dc10`, run
one finite 800-task / 40-batch real Batch API workload on the same existing
480B-total / 35B-active TP4 engine and four H100s. Reuse the byte-verified retained
20-cell public input, with zero uploads, natural input128–16K and forced
output32–2K. Planned output is544,000 tokens. Source/runtime/model, 32-slot
exact-token admission, KV budget164,256, fair scheduling and retention stay fixed.

Offered rate1task/s, maximum128 in flight, one submitting request, five-second
GPU/engine/API/Azure samples. Admission bound3,600s plus1,800s drain; controller
lifetime5,700s. Existing retained-task quota must fit all800 without alteration.
Use operator-confirmed $16.10 whole-Pod/hour allocation, never invoice cost.
Do not compare this ordered observation to a different offered load as a causal
speedup. The failed 3,600-task run remains failed continuity evidence.

Preconditions include the completed native release receipt, exact managed
identities and current module hashes, fresh worker/API readiness, empty durable
active queue and retained input TTL covering the entire bound. The campaign
holds the existing product lock, never changes services, cancels accepted work,
cleans history, raises quotas, changes models or creates paid resources.
Record all pauses, errors, retries, missing samples and identity changes.
A worker KV latch requires bounded, independently quiescent recovery; process
liveness or a successful health request cannot authorize clearing it. Automatic
live-latch recovery is still an open reliability task. Observe/recover the same
job; never resubmit it under a fresh identity after an uncertain acceptance.

This is compute/observability qualification, not an answer-quality score,
24-hour capacity guarantee, physical GPU reset or whole-Pod HA. Preserve the
exact derived operator controller alongside the immutable runtime receipt.

The fixed run `/root/batchforge-runtime/soak480-fleet-20261025-v1` is now running,
controller PID607317/start47342118 from immutable runtime748dc10. Its exact
derived controller SHA is9b80d25948cb4fafb0eb84a4f46be7cc25d61b65fe158624fa02a5db2be50496.
The [started snapshot](platform480-2026-10-25/soak480-fleet-20261025-v1-started/evidence-manifest.json)
is not a completion report. At17:41:19UTC the sampled four-device activity was
95/96/95/81%; this is one device-wide sample, not sustained saturation proof.
[Fresh worker/managed identity health](platform480-2026-10-25/evidence-api-fleet-20261025-v2-post-release/post-rollout-health.json)
confirmed an unlatched ready worker,31 active tasks and159,520 reserved tokens.
