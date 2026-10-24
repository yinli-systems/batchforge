# Production history retirement verification — October 24, 2026

Schema11 is deployed on the existing private control plane. A freshly generated
isolated test tenant completed the entire real-GPU verification flow; the
default tenant's history retirement remains disabled. This establishes a finite
working storage lifecycle, not unlimited storage or a second compute failure
domain.

## Deployment and provenance

The [guarded rollout report](../ops/reports/history-v11-production-rollout-2026-10-24.json)
binds the exact six-source migration bundle, fresh schema10 snapshot, actual
restore and existing archived QA20 result. Both APIs served the schema10 prefix
with the compatible code before migration, then were rolled onto schema11.
The model, worker and gateway identities were preserved. The old 17,841-byte
QA20 archive remained byte-identical through both APIs and the gateway.
All eight accounting/constraint checks stayed at zero mismatches. Runtime-role
checks confirmed no history DELETE/TRUNCATE, policy-write, retirement-execute or
owner-membership privilege.

The committed source snapshot `23a6c0ac27d367d0bd859bd7859af11debfec067` passed
[1,159 tests with zero skips](../ops/reports/linux-pg17-23a6c0a-validation-2026-10-24.json)
on an isolated PostgreSQL17.11 server. Its test cluster was stopped and removed;
production credentials and database access were excluded. This validation is
separate from GPU timing, the console's Node checks and hosted GitHub CI, which
was blocked by account billing before running any steps.

Before migration, a new 128,137,665-byte schema10 snapshot was restored on the
same Pod and checked against its original ledger. The
[backup metadata](../ops/reports/history-v11-preflight-v3-schema10-backup-2026-10-24.json)
and [restore result](../ops/reports/history-v11-preflight-v3-restore-2026-10-24.json)
share snapshot SHA256
`aa8b271093a210fee744937d39830a30c5145fab8e246b1128ed6a8858115e09`.
The database dump was not exported. A same-Pod restore does not qualify
independent disaster recovery, role/ACL recovery or an object-store backup.

## Actual verification flow

The [production verification report](../ops/reports/history-v11-real-retirement-verification-2026-10-24.json)
records 12 passing checks. The model was Qwen3-235B-A22B-Instruct-2507-FP8,
235B total / 22B active, on all four H100 80GB devices using TP4. The private
fixture contained 20 natural-generation requests selected from the earlier
public QA100 input, SHA256
`1160867847ed599a6045094864c36e8cd68880658f116e20f2e7e94b114f7566`.
No synthetic completion, modified timestamp or forced output length was used.

| Step | Observed result |
| --- | --- |
| Submit first 20, with a 20-task tenant cap | All 20 completed, each accepted once |
| Submit another 20 before retirement | HTTP 429; tenant counts unchanged |
| Automatic archive and payload purge | Payload charge 4,039,150 →0 bytes; retained tasks stayed 20 |
| Owner maintenance: three bounded pages | 7 / 7 / 6 tasks and 20 attempts retired; task quota 20 →13 →6 →0 |
| Download before archive, after purge and after retirement | Same 17,410 bytes and SHA256 at every stage |
| Metering and batch usage after detail retirement | Preserved; UTC overview unchanged |
| Physically compact the expired versioned key | Original idempotency row absent; replay returns 410 |
| Retry the retired batch | HTTP 410; no extra work created |
| Submit second 20 with a fresh key | All 20 completed, each accepted once |
| Default tenant, global ledgers and service identities | Unchanged default; zero invariant mismatches; identical owned processes |

Each 20-request execution used 22,485 prompt and 308 generated tokens, with zero
repeated accepted attempts. Accepted-once refers to durable result acceptance;
it does not promise exactly-once GPU execution after a fault. The post-retirement
download SHA256 was
`31c04ecef5f0f4118193d4aff829d1ddc833dc8b04302fac4994a5084dd4fa70`.
The second execution has its own result file and identity; it is not a replay
of the first output.

The test tenant used explicit 60-second minimum age and replay TTL, and one
finite three-cycle maintenance run. Those settings do not enable early
retirement for the default tenant. The existing automatic publisher retained
its 120-second terminal-age gate. Raw input, responses and the generated tenant
key remain private on the serving Pod.

## Remaining boundaries

The implementation preserves batch totals and token usage while removing
verified task/attempt detail. Legacy idempotency keys have no automatic replay expiry unless
an explicit versioned policy applies; auxiliary row and byte limits still apply. Default-tenant maintenance needs a new
explicit cutoff, minimum age and finite execution; no periodic janitor was
silently installed.

Files, batches, ready archive objects, stored input bytes and auxiliary metadata
still have independent finite quotas. Logical credit release is not a claim
that PostgreSQL heap/TOAST/index/WAL allocation immediately shrinks. This
20-task test does not establish sustained reclamation throughput or resolve
all long-term metadata/object lifecycle requirements. The fresh matched
response-reservation benchmark and the later fault experiments are separate
measurements.
