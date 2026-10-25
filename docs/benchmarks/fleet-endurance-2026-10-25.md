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

The fixed run `/root/batchforge-runtime/soak480-fleet-20261025-v1` was started,
controller PID607317/start47342118 from immutable runtime748dc10. Its exact
derived controller SHA is9b80d25948cb4fafb0eb84a4f46be7cc25d61b65fe158624fa02a5db2be50496.
The [started snapshot](platform480-2026-10-25/soak480-fleet-20261025-v1-started/evidence-manifest.json)
is not a completion report. At17:41:19UTC the sampled four-device activity was
95/96/95/81%; this is one device-wide sample, not sustained saturation proof.
[Fresh worker/managed identity health](platform480-2026-10-25/evidence-api-fleet-20261025-v2-post-release/post-rollout-health.json)
confirmed an unlatched ready worker,31 active tasks and159,520 reserved tokens.

## Closed result

The fixed study closed successfully: **800 accepted/completed, zero failed**,
all40 batches, exactly one accepted attempt per task, conserved usage and exact
forced output lengths. Its [original report](platform480-2026-10-25/soak480-fleet-20261025-v1-complete/run/report.json)
retains1,270.536 whole-wall seconds,4,362,240 prompt /544,000 output tokens,
**428.166 output token/s**, and DB-acceptance completion P50/P95/P99
**154.884 /250.846 /258.444 seconds**. Parent continuity checks also passed;
engine, worker, API and gateway identities remained fixed throughout.

Each GPU had198 observations: means92.146/96.217/95.035/87.955%, all max100.
There were zero observation errors, maximum sample/boundary gap7.157s and no
gap over twice the five-second target. Whole-Pod allocation at the confirmed
$16.10/h was **$5.6821**, approximately$0.00710 per delivered compute task;
it is not an invoice or a quality-qualified answer price. Forced outputs have
no reference-answer quality grade. These device-wide samples are not FLOP or
Tensor Core saturation measurements.

This offered1task/s workload and duration differ from the earlier0.5task/s run
and failed3,600-task study. Its higher observed rate is not a controlled speedup,
long-term reliability proof or daily capacity guarantee. Source748dc10 was fixed.
A separate read-only audit verifies normal S3 archival/purge and byte samples;
its report is independent of this closed performance result.

Scheduled-arrival to completion P95/P99 were **431.547 /488.814 seconds**.
These include admission/backpressure and are not the shorter DB-acceptance
latencies above. All800 attempt records were accepted once, with zero retries.

The independent [closed storage audit](platform480-2026-10-25/evidence-api-fleet-readback-20261025-v2-complete/validation.json)
found40 ready S3 archives, all800 database response payloads normally purged,
and all800 task identities retained. It verified byte counts/SHA and strict
receipts for input/output at cohort indices0/20/39: six reads, with the same
retained input reused, not six distinct objects or an all-output byte scan.
An earlier [audit failure](platform480-2026-10-25/evidence-api-fleet-readback-20261025-v1-failed/initial-failure.json)
completed no atomic report because PostgreSQL SUM(bigint) produced Decimal,
which JSON could not serialize. Its partial temp file/source/log remain on the
Pod, failed evidence is preserved in Git, and a distinct v2 normalized counters
to integers before publishing. Neither audit changed services or submitted work.
