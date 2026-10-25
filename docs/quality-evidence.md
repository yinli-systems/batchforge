# Published quality evidence

Current publication uses immutable **`748dc10`**, after the guarded Fleet/quality
release. Both APIs and gateway reconcile two reports: the failed original
10,570-question full-dev baseline and the passing frozen 1,000-question JSON-span
holdout. The console and API expose answer-contract/cohort hashes, invalid-format
counts and separately pinned operator-rate allocations. Cost bytes, arithmetic,
wall interval, exact counts and quality gate must match the source report; any
ledger mismatch suppresses both scores and cost. These are not invoices. The
[release receipt](benchmarks/platform480-2026-10-25/evidence-api-fleet-release-20261025-v2-complete/release.json)
retains checks before and after reopening. The earlier `831b839` rollout below is
historical publication evidence.


This follow-up adds an authenticated Quality evidence view and two read-only
routes, without new cloud services, credentials, database tables or migrations:

- `GET /v1/evaluations`: the current tenant's published reference evaluations.
- `GET /v1/evaluations/{id}`: the same safe projection plus its batch IDs.

Configure `QUALITY_EVIDENCE_CATALOGUE` with an immutable release catalogue path,
for example `/approved/release/deploy/quality/catalogue.json`. The default is
empty, preserving existing deployments. The included catalogue pins the original 10,570-task report and frozen
1,000-task holdout; the original failed 70%/80% gate remains failed.
Immutable source `831b839` passed independent read-only Azure qualification at
14:36:39 UTC on October 25: schema 11, TLS hostname verification, actual tenant
authentication, 166-batch linkage, other-owner hiding and exact response-body
checksums. The [qualification evidence](benchmarks/platform480-2026-10-25/quality-api-review-20261025-v1-qualified/validation.json)
is exported with SHA-256 verification. This did not deploy the feature.
The guarded release completed **2026-10-25 15:20:51 UTC**, after the successful
closed 2,400-task endurance report. A private canary passed, both API enqueue
paths drained, and both API replicas and the gateway passed validation before
canary cleanup. That release used immutable **`831b839`** with its pinned
catalogue. The 480B worker and engine identities stayed unchanged. The
[actual release receipt](benchmarks/platform480-2026-10-25/quality-api-release-20261025-v1-complete/release.json)
is retained. Failed checks would restore compatible API configuration; no
retention, schema, model or cloud resource changed. The separate input-reuse
console and physical-fleet follow-ups were not in that earlier revision.

## Ownership and reconciliation

The server reads at most 16 hash-pinned reports, with at most 256 batch IDs each.
Files must be owned regular release files, not writable by other users; basenames,
byte counts, checksums, protocol, dataset identity, scope and finite metrics are
validated. Symlinks, FIFOs, non-integral exact counts, altered bytes and conflicting
gate states are rejected. Use the approved container filesystem, not the broken
`/workspace` FUSE mount. Preserve catalogue/report bytes in private Git for
redeployment; this does not create whole-Pod compute failover.

Every API read queries tenant-bound batch rows in one statement. All cohort IDs
must belong to the authenticated tenant. Partial ownership reveals no report,
and detail reads return the same 404 as unknown IDs. The query retrieves only
batch identity, model, status, counters, usage and runtime fingerprint. It never
loads prompts, responses, errors, fencing tokens or credentials.

For a visible report, model/runtime, total/completed/failed/cancelled counts and
token sums must match the durable batch ledger. A mismatch suppresses quality
metrics and marks the evidence unverified. This remains valid after supported
task-history retirement because the batch ledger is retained. It is a batch
linkage check, not a fresh per-attempt audit or hardware attestation.

## Console behavior

Delivery, quality and deadline appear separately. A fully delivered cohort may
fail its quality gate. The view shows EM, F1, thresholds, on-time exact-answer
rate, model/runtime, dataset hash and report hash. It offers authenticated JSON
evidence download. Workspace switches invalidate in-flight fetches and downloads;
unknown metadata fields never become public response fields.

These are operator-published experimental reports with checksums. They are not
issuer signatures, hidden benchmarks, contamination audits or guarantees of
production accuracy. The current publication protocol supports pinned public
SQuAD 1.1 complete or seeded-subset reports. Customer-owned evaluators and an
independent evidence-signing trust chain remain separate work.

The source-report SHA pins the original operator publication. An API download is
an owner-safe projection, not those original report bytes. The detail endpoint
also returns `X-BatchForge-Evidence-SHA256` over its exact JSON response body;
the console verifies those bytes with WebCrypto and saves them without
reformatting. Missing or corrupt checksums stop the download. Both evidence
routes use `Cache-Control: no-store`, so current ledger linkage is not
served from a shared or stale cache. These checksums are not issuer signatures.
