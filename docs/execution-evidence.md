# Execution evidence and admission headroom

Authenticated, tenant-scoped routes add an inspectable execution boundary to the
Batch API. No schema migration or GPU engine change is required on schema 11.

- `GET /v1/batches/{id}/receipt`: one PostgreSQL statement reconciles batch/task
  counters, accepted attempts and token usage, with retained-history counters.
  It returns recorded runtime identities and input/output/error artifact hashes.
- `GET /v1/batches/{id}/tasks?limit=25&after={task_id}`: bounded keyset pagination,
  including status, attempt count, accepted token usage and completion time.
  A cursor must belong to this tenant and batch. No prompt, response, error body
  or fencing token is returned.
- `GET /v1/capacity`: logical retained-task, payload, input-storage and queue
  headroom. Upload-held task credits count against queue headroom. This is an
  advisory statement snapshot; admission transactions remain authoritative.

The console's batch drawer exposes evidence inspection, task pages and receipt
JSON download. WebCrypto recomputes the domain-separated canonical SHA-256
before displaying “checksum verified.” Switching/closing a drawer invalidates
in-flight responses. Integer values beyond JavaScript's exact range fail
verification rather than silently rounding. The create dialog shows current
headroom without changing any limits.

## Reservation lifetime

Each task reserves its request's PostgreSQL canonical JSON bytes, a bounded
response, and `(max_attempts + 1) × max_error_bytes`. Completion does **not**
release this reservation. Verified S3 publication followed by database payload
purge releases it. A client can hit `retained_payload_limit` with an empty GPU
queue; it must wait for archival and reuse the same upload and idempotency key.
The existing 120-second archival age and 24-hour input retention are unchanged.

## Independent byte readback

Download a receipt, then use an authenticated existing endpoint:

```sh
python -m scripts.verify_receipt --receipt batch-receipt.json \
  --url http://127.0.0.1:8080 --out new-readback.json
```

Supply `BATCHFORGE_API_KEY` through the environment. The verifier streams at most
64 MiB per artifact, follows no redirect, compares byte count and SHA-256, and
saves only the readback report. Unpublished hashes, expired objects, corruption
and excessive streams cannot pass. It never saves or prints payload bytes.

A receipt checksum is not an issuer signature. Worker-reported runtime identity
is not hardware attestation. Database reconciliation is not answer quality, and
recorded object metadata is not independent byte readback. Legitimately retired
history is explicitly incomplete. A single Pod remains a single GPU failure
domain; these features do not create multi-node HA.
