# Input artifact storage

New uploads can use `BLOB_STORAGE_BACKEND=s3` instead of storing another copy of the input JSONL in PostgreSQL `files.content`. The database retains file metadata and each parsed task body. Existing inline inputs remain readable. The `inline` compatibility mode retains the old upload path and does not enforce the artifact catalog's byte reservations; enable an external backend to use the controls below.

`local` uses an owner-only directory with fsynced, atomic publication and checksum verification. It survives application process restarts on the same filesystem; it does **not** provide durability after Pod or disk loss. S3 mode requires an independently durable configured service. Credentials remain in operator environment variables, never file metadata, URLs returned to tenants, or source control. No bucket is created by the adapter.

## Admission and accounting

Each external upload first reserves its full logical byte count and validated task count in PostgreSQL. The transaction locks the tenant before updating usage. Concurrent uploads must satisfy both bounds:

- `stored_bytes + reserved_bytes + incoming_bytes <= MAX_STORED_INPUT_BYTES_PER_TENANT` (default 256 MiB).
- `pending_and_running_tasks + held_upload_tasks + incoming_tasks <= MAX_QUEUED_TASKS_PER_TENANT` (default 100,000).

A successful upload transfers reserved bytes to stored bytes and keeps its task credit until the first batch submission. The submission consumes that credit in the same transaction as batch and task creation. Reusing an input file for another batch requires fresh queue capacity. An unsubmitted retry staging file can be discarded without touching a committed batch, including when a commit outcome is initially uncertain. Expired reservations release both credits.

Identical content is deduplicated only within one tenant's active object generation. Each file still consumes its full logical byte allowance, so repeated references do not bypass admission. Migration 5 counts existing inline input bytes against the tenant's starting stored-byte balance, but does not move or delete those files. An existing tenant over its configured limit keeps its data and cannot upload more external input until the operator changes its allowance or implements an explicit legacy retention policy.

These are per-tenant logical admission bounds, not a global database or physical bucket size guarantee. In-flight writes, retired generations awaiting collection, and verified download spools can consume additional space. Tenant provisioning remains an operator responsibility.

## Publication, downloads, and collection

External I/O occurs outside database transactions. A committed upload reservation and object-generation row precede each write. Publication adds a file row with `content=NULL` only after the write succeeds. A write or database-publication failure leaves a known, collectable generation and releases its reservation. Abandoned upload reservations expire by default after 300 seconds; an active upload renews its lease.

Objects use a tenant-derived namespace, SHA-256 digest and unique generation ID. Local writes publish atomically without overwriting an existing object. S3 writes include Content-MD5 and SHA-256 metadata; downloads also verify the complete length and SHA-256 before exposing bytes. S3 downloads spool to a temporary file after 8 MiB instead of retaining an unbounded object in RAM. A download holds a renewable database read lease without holding a database connection while a client consumes chunks.

With `BLOB_GC_ENABLED=true`, each API performs bounded, jittered maintenance. New external inputs become eligible for expiry after `INPUT_RETENTION_SECONDS` (default 24 hours); active batches keep their referenced input. File metadata is tombstoned and its quota credit released. Collection skips live file references, upload reservations and read leases, and waits a 300-second orphan grace period. Legacy inline inputs are excluded from automatic expiry.

Retired object keys are never reused. Their metadata remains as a deletion fence, and deletes are retried, so a delayed SDK write or delete cannot target a subsequently uploaded generation of the same content. This deliberately retains small object tombstones; metadata compaction is not implemented. A crash after reservation, external write, publication, or deletion is recoverable by the same bounded maintenance operations. The configured object service must still supply its own availability and durability.

## Capacity limits still present

Task bodies remain JSONB in `tasks`; model responses and errors also remain JSONB. Streaming output downloads avoid assembling an entire batch response in API memory, but do not archive or remove those database rows. Output byte admission, result retention, output object archival, global physical-storage quotas and tombstone compaction are not implemented here. This change reduces duplicated input BYTEA growth; it does not establish a daily ten-million-task storage capacity. Database footprint still depends on actual input, output, attempts, indexes and retained history, and must be measured for the intended workload.

The targeted tests in `tests/test_blob_storage.py` exercise real isolated PostgreSQL transactions for concurrent byte/task admission, rollback, tenant isolation, retention, renewable read leases, orphan cleanup, cancelled SDK I/O and generation-safe deletion. Injected S3 adapter tests verify checksums and bounded spooling; live provider compatibility is a separate deployment check.
