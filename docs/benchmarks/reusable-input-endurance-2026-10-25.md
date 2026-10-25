# Bounded endurance without repeated input uploads

The next endurance protocol addresses a measured storage constraint: each new
input carries a 24-hour retention policy, and rewriting custom IDs prevents
content reuse. Repeating input uploads is unnecessary because the Batch API
already accepts the same tenant input file in distinct batches.

`scripts.sustained_benchmark --reuse-input-file file_… --input-jsonl private.jsonl`
now verifies a read-only, tenant-bound database reference, object state, bytes,
SHA-256 and expiry beyond the persisted admission plus drain deadline. It then
independently streams the authenticated content endpoint and checks its exact
size and digest before any create. It sends **zero input uploads**. An absent,
expired, changed or cross-tenant artifact fails closed. Only whole, unchanged
fixtures with unique per-file custom IDs are supported; fixed-output mode
requires existing `min_tokens` to equal the output caps.

Distinct batches legitimately reuse custom IDs. Reconciliation therefore checks
`(batch_id, custom_id)` membership plus globally unique database task IDs, one
accepted attempt per completed task, token conservation and actual output length.
It preserves the existing fsynced journal, identical immutable resume settings,
24-hour idempotency horizon, bounded same-key retries and backpressure. A resumed
closed run does not reread inputs or create more work. Its telemetry may refresh.
No quota increase, retention extension, payload deletion or service change occurs.

## Preregistered successor

`ops.reuse_input_campaign` waits for a successful, fully cleaned-up quality API
release and acquires the same exclusive campaign lock. It verifies live managed
identities, readiness, an empty old queue and sufficient retained-task headroom.
The predecessor input must match the pinned public 20-cell SQuAD mixture after
forcing output lengths; prompts are never exported as evidence.

- Same existing four H100s, 480B total / 35B active AWQ MoE, same engine/worker.
- 3,600 tasks in 180 batches of 20; 2,448,000 requested output tokens.
- One existing input, offered rate 1 task/s, maximum 128 in flight.
- Admission bounded to 10,800 s; drain 1,800 s; five-second target sampling.
- No new input upload; price and invoice stay unmeasured (`null`).
- Every failure is retained. New submissions cannot begin without enough time
  to finish admission and drain before 2026-10-26 09:00 UTC.

At 15:04 UTC, immutable successor source `9415736` passed real Azure read-only
schema-11 compatibility, exact input metadata/expiry checks and independent
authenticated S3 byte readback. The public mixture contains 13,600 requested
output tokens per 20-task batch. The
[qualification and launcher](platform480-2026-10-25/evidence-api-reuse-20261025-v1-qualified/evidence-manifest.json)
are exported with SHA-256 verification. One successor is queued in
`waiting_release`; qualification did not submit any GPU task or change services.

This is a new endurance and artifact-reuse experiment. Fixed order and offered
load differ from the original shuffled 0.5 task/s study, so its latency and
throughput cannot establish a controlled improvement over that study. No real
successor results are claimed in this preregistration.

## Console follow-up

A separately reviewed console change adds **Use existing input** to Create batch.
It accepts an existing input ID, skips upload, and creates new tasks through the
same API. It preserves a submission key when retrying a lost create response in
the open dialog; changing inputs starts a new attempt. This in-memory create
attempt does not survive a page reload. Workspace changes stop later submission
steps and discard stale responses. The server remains authoritative for ownership,
expiry, model support and quota checks. This frontend change is not part of either
immutable running/queued snapshot and is **not deployed** with source `831b839`.
Twenty Node regression checks passed; browser visual QA remains unavailable.
