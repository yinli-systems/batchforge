# Real480B short-action lease qualification

Immutable source **8ddc2ca23243b9fc1e960b2738a6152bccdd28be**, candidate-v9,
passed the preregistered Batch-ACK process-loss gate on the existing four-H100
480B-total/~35B-active MoE service. The ordinary Agent coordinator now has a
15-second lease, independent5-second renewal and a45-second action deadline.
These settings and the30-second reclaim gate were frozen before execution.

Four new `lease-contract-heldout-v1` goals cover calculation and
memory→calculation with actual tool order, result values and retrieved citations.
All4 passed, reserving/delivering10 real child model tasks with6,778 prompt and
391 completion tokens. No output-contract corrections were needed. Four separate
decoder calibration tasks passed with1,170 prompt/137 completion tokens. They
are not four more Agent goals.

The injected coordinator exited after a real Batch ACK before committing that
ACK to Agent state. Application fault timestamp00:50:11.890482UTC to the next
native PostgreSQL lease event measured **13.580339seconds**, passing the30-second
gate. The successor used the original child key and Batch ID. This interval is
the remaining lease plus scheduling/reclaim time, not a full15-second timeout
starting at the ACK. It is one finite process-loss observation, not a GPU fault,
hard real-time guarantee, P99 or a matched throughput improvement.

The four event histories/173 events verified from genesis. Agent API restart
preserved state/results/event heads. All20 distinct Agent input/output artifacts
passed byte/SHA readback after normal S3 archival and DB response purge.
The separate decoder archive's two objects were independently read after closure.
Independent Azure SQL reconciled **14 unique tasks/14 attempts**, all accepted
usage (**7,948 prompt/528 completion tokens**) and11 ready/purged batch archives.
All14 task identities remain while their response payloads were purged normally.
Existing retained-task headroom changed104→100 after decoder admission→90 after
Agent work. No quota, retention policy or old-history deletion was used.

Agent state was native Unix-only PostgreSQL on container disk, not Azure Agent
persistence. This cohort restarted the API; native database-process restart
evidence belongs to the preceding DAG qualification and is not recounted here.
All temporary coordinators/APIs/native PG stopped, ports18170/18172 closed and
the six primary identities/source/configurations remained unchanged. Independent
closeout verified all four ready endpoints and the same armed finite guard.
The guard waits at the unknown campaign boundary; automatic guard coverage
during this qualification is not claimed.

The complete wrapper took **196.890569seconds**, allocating **$0.880538** at the
operator-confirmed$16.10/whole-Pod-hour. The nested Agent interval186.498849seconds
and decoder interval7.465655seconds overlap the wrapper. Their raw reports retain
their own allocations; **do not add those windows to the wrapper allocation**.
This is not an invoice, marginal cost or production per-qualified-answer price.
No GPU utilization/saturation or daily-capacity conclusion is drawn from this
small functional workload.

Evidence:

- [Wrapper report](platform480-2026-10-25/evidence-api-agent-schema-lease-20261026-v1-complete/validation.json), SHA `c98dab8282b303b2050af43b05b3bf3fb805816fd433a3c8f8fdf7d5ebf0746b`.
- [Agent report](platform480-2026-10-25/soak480-agent-runtime-schema-lease-20261026-v1-complete/report.json), SHA `35ee75cd4765b40d125e65b888f0e5df0fa9410f246e927909b1bb0c389dbab8`.
- [Decoder report](platform480-2026-10-25/soak480-agent-runtime-grammar-lease-20261026-v1-complete/report.json), SHA `7de669c68f7eaf77ab559ebe6a1a07d919d02f10852fd88594d914d2261a08f0`.
- [Independent closeout](platform480-2026-10-25/evidence-api-agent-lease-closeout-20261026-v1-complete/validation.json) and exact validator source; all transfer manifests verify exported bytes.
- [Source receipt](platform480-2026-10-25/evidence-api-agent-runtime-20261026-v9-source/source-receipt.json),318 files; [actual CI](../../ops/reports/github-native-ci-8ddc2ca-2026-10-26.json),1,727 Python/zero skips,39 Node tests and compileall, no remaining runner.

Native CPU tests separately exercise slow I/O that requires successful renewals,
cancellation/deadline stopping, accepted-upload reconciliation and final-clock
fences rolling back expired transactions. Those deterministic-adapter results
are not relabeled as additional GPU tasks. Production Agent admission remains
off pending owner-authenticated migration and a compatible managed rollout.
