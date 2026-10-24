# Auxiliary metadata admission (schema v10 candidate)

This is a local implementation and integration contract, registered in the
candidate source but not migrated or enabled on the live Pod. It complements schema v9's
retained-task and task-payload admission; it does not replace result archival.

`0010_auxiliary_metadata_admission.sql` adds owner-managed per-tenant limits
and authoritative usage. Every state counts, including expired input file
metadata, retired blob generations and deleted archive chunks. Only an actual
SQL row deletion returns that row's credit. The migration deletes no history,
idempotency keys, fences or object-store data. It backfills existing counts;
an already over-limit tenant can read history, renew existing leases and finish
accepted work, while new allocations fail with `BQ429`.

| Resource | Initial limit | What consumes it |
| --- | ---: | --- |
| Input file credits | 10,000 | Retained input file rows **plus** upload reservations, each reserving one future file row |
| Upload reservations | 32 | All unremoved reservation rows, including expired reservations awaiting cleanup |
| Idempotency | 10,000 keys, 256 MiB reserved responses | Each new key reserves 64 KiB before its side effect; the byte budget binds at 4,096 default-size keys, before the 10,000 row cap |
| Blob generations | 20,000 | All states, including deleting/deleted fences |
| Archive runs | 10,000 | All states, including failed/retired generation history |
| Archive chunks | 100,000 | All states, including writing/deleting/deleted chunks |
| Input readers | 128 | Input read-lease rows, including expired rows until cleanup |
| Result readers | 128 | Result read-lease rows, independently of input readers |

These are configurable owner policies, not measured storage footprints or a
capacity claim. Input content/declared bytes have a 16 MiB initial SQL ceiling;
input filename bytes are capped at 1,024 and other file metadata at 8 KiB.
Other generation metadata has an 8 KiB row ceiling, lease metadata 4 KiB,
idempotency key/identity metadata 2 KiB. A new idempotency response cannot
exceed its original reserved allowance. Policy changes affect future keys;
they do not shrink existing per-key reservations. Existing historical response
reservations are backfilled to at least their actual canonical response size.
Usage measures logical rows and reserved bytes, not heap/TOAST/index size,
MVCC bloat, WAL, object-provider billing or an exact physical disk bound.

The 64 KiB key reservation covers the existing API's maximum 16 metadata
entries with 64-character keys and 512-character values. Even six-byte JSON
control-character escaping gives at most `16 * (64 + 512) * 6 = 55,296` bytes,
plus JSON punctuation and a batch-view wrapper of less than 4 KiB. Four-byte
Unicode is smaller. Real PostgreSQL tests store the maximum Unicode and
escaped-control responses and check their canonical JSONB text against the
reservation. A future larger metadata or response contract must revisit this
bound before accepting its side effects.

Before-insert triggers take the tenant umbrella lock and validate row shape,
but do **not** spend quota. Only an actual successful insert charges usage.
Consequently `INSERT ... ON CONFLICT DO NOTHING` can replay an existing key
at quota capacity without allocating another credit. All counters and policies
are owner-managed, including a database guard against accidentally broad table
grants. Private security-definer accounting functions must never be granted
to the runtime role. No API may set session replication role or disable triggers.

External upload reservation precedes object PUT. Publication deletes the
consumed reservation before inserting the input file, inside the same
tenant-locked transaction; one reserved file credit becomes one retained file
credit. Failure rolls back that transfer. Abort and expiry return reservation
credits, but do not pretend that retained file/generation metadata disappeared.
Quota rejection before generation insertion cannot leave an untracked PUT.

Output/error finalization has **no auxiliary quota check or auxiliary-ledger
lock**. A unique tenant/batch/kind result-file identity and deferred batch FK
bound output/error rows to two per retained batch. Supported creation paths
always create at least one task per batch, so v9's retained-task bound also
bounds retained batches. Archive file identities are limited to one of each
kind per run and must refer to that run's actual batch. Each chunk ordinal must
be below its file's expected result count. Existing work therefore does not
lose its completion merely because a tenant filled its upload or archive
metadata quota. When new archive metadata is refused, accepted SQL results
remain available; the publisher must back off and must not purge them.

Reader pools are separate from submission pools. A full reader pool may reject
a **new** download before headers, but does not deny renewals or release of an
existing lease. No live lease is evicted for another reader. Bounded cleanup
can reclaim expired leases; storage outages can delay cleanup, so a configured
TTL is not a cleanup-completion SLA. A renewal/state transition does not spend
new credits or lock the auxiliary ledger.

## Integration and lock order

For a new metadata allocation, lock the tenant `FOR NO KEY UPDATE` before any
existing metadata row, then the tenant's auxiliary ledger, then resource rows.
All auxiliary ledger rows are locked in resource-name order. Existing v9 and
blob/result byte ledgers share that same tenant umbrella; their relative order
cannot create a same-tenant cycle when every writer first owns the umbrella.
Normal GPU completion keeps worker → queue → batch → task order and never
requests the auxiliary tenant/ledger locks. Server-generated result-file
inserts return from the auxiliary guard before taking those locks.

`backend/auxiliary_admission.py` supplies `tenant_prelock`,
`release_read_lease` and `collect_expired_read_leases`. The latter selects a
bounded candidate set without row locks, then, per candidate transaction,
locks tenant → lease and rechecks expiry using `FOR UPDATE SKIP LOCKED`.
This prevents the inverse lease → tenant order in a global DELETE sweep.
The helper performs no S3/client I/O in a database transaction and works
against the existing v8 tables before v10 is installed.

Required root-owned integration before migration/enforcement:

1. Preserve the tenant-before-key order in `Store.idempotency`; perform replay
   lookup before staging any new retry/upload side effect. `remember` remains
   in the same transaction as batch creation and within its reserved allowance.
2. In `ResultArchiver.open_file` cleanup, replace the raw lease DELETE with
   `release_read_lease(pool, 'result_read_leases', tenant, lease)`. Replace its
   global expired-read DELETE with `collect_expired_read_leases` before the
   existing expired-run sweep. Blob reader cleanup already uses those helpers.
3. Map `BQ429` to HTTP 429, `BP413` to 413 and `BR409` to 409 for **all** routes,
   including BlobCatalog calls outside Store's task methods. Return a stable
   resource code, not raw SQL messages/credentials.
4. Register v10 and require it in readiness when this feature is enabled;
   retain the immutable checksums for v1–v9. Grant runtime access only to the
   existing data-plane tables and optional SELECT on the new policy/usage
   tables; do not grant EXECUTE on private accounting functions. All replicas
   must use the tenant-first cleanup paths before activating the triggers.
5. Before applying the structural result-file indexes/FK, inspect historical
   duplicate result kinds and missing batch identities. The migration must
   fail for operator repair rather than discard ambiguous history. The final
   CHECK is validated, rather than hiding an unvalidated historical constraint.
   Run only in a planned quiescent migration window. V10 takes a tenants-table
   EXCLUSIVE NOWAIT gate, followed by batches/tasks/attempts and all counted
   metadata relation write gates NOWAIT. Any held writer immediately refuses
   migration; roll back the entire transaction before retrying. Bootstrap then
   initializes counters directly without a tenant tuple lock. This is a short
   operator write gate, not a claim of uninterrupted online migration. For a
   combined v8→v9→v10 rollout, the migration runner must acquire the same full
   fail-fast gate before entering immutable v9, whose own relation locks precede
   the v10 SQL file. Both migrations must remain in that same transaction.

No automatic idempotency/history expiry is implemented. A replay guarantee
requires an explicit published horizon before expired keys can be removed or
reused. Generation fences are deliberately not aged out: an SDK cancellation
does not prove its late PUT can no longer arrive. A clock-only grace period is
insufficient proof for deleting the fence. Safe fence retirement needs a
separately proven writer/provider lifecycle, not an assumed timer.

The initial behavior is finite, explicit capacity: a tenant eventually receives
429 for new work instead of accumulating metadata forever. This is not a
claim of indefinite operation or ten million tasks per day. Sustained operation
still needs an approved history/metering-retirement contract and measured
maintenance throughput. Operator-created tenants/keys/workers, filesystem logs,
PostgreSQL/WAL maintenance and object-store billing need separate management.
