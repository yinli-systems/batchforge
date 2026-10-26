# Agent workspace and receipt verification

The candidate console is served by the Agent service at `/console/`. An opt-in
mount at `/agents/console/` integrates it with the Batch workspace. The mount
shares the parent's existing database pool and tenant authentication, validates
the independent Agent schema, and verifies an operator-owned executor registry
before startup. It creates no tables and launches no coordinator or GPU worker.
`AGENT_API_ENABLED=false` is the default; current primary748dc10 remains unchanged.
Only an enabled, enrolled workspace receives an Agent link in its overview.

After the owner migration and qualified deployment, configure
`AGENT_API_ENABLED=true` and an owner-only `AGENT_EXECUTOR_KEYS_FILE`. A compatible
coordinator must also be running. Enabled admission means the durable queue
accepts work; it does not measure coordinator or GPU availability. Standalone
`python -m agents api --executor-keys ... --batch-url ...` serves the same console
and reads child receipts from the pinned Batch endpoint. The shared mount reads
the ledger directly through its parent store and pool. Keys never enter goals,
run state, URLs, console assets or exported evidence.

## Operations

The workspace exposes goal submission, caller-declared required tool order,
model/tool/token/deadline budgets, run summaries, final answers, actual tool
observations, child model receipts, event verification and optimistic-version
memory. API capabilities determine available models/protocols, mutation buttons
and receipt lookup. Schema-v1 admission fails before creating Agent state unless
this deployment explicitly enables its qualified schema ingress. Console labels
separate reserved steps, executed tools and delivered completion tokens; there
is no fabricated monetary cost or GPU-utilization widget.

Creation records a cryptographic request key and exact body in this browser
tab before sending. A missing/uncertain acknowledgement reuses that saved body
and key across reloads; changing the unresolved goal cannot allocate another
key. Pending records are tenant-scoped and bounded. Switching workspace keys
clears visible data and invalidates old async responses without discarding
another tenant's unresolved identity. No automatic model task is submitted on
page load or refresh. If session storage cannot preserve a key, creation fails
before network I/O while authenticated reads remain usable.

A definite first validation refusal allows correction under that same request
key. An earlier lost acknowledgement keeps the body frozen even if a later
retry encounters a validation error; it cannot establish that the first attempt
was never accepted. The CLI rejects `--read-only` with coordinator mode before
reading credentials or opening a database, rather than silently starting work.

Cancellation reads the server's resulting state because completion can win.
Already accepted model work remains in the Batch ledger. Memory writes use the
explicit expected version; a conflict does not silently increase that version
and overwrite concurrent work. Lists project bounded goal previews and counters,
excluding full model input, memory observations and results from list queries.

## Evidence boundary

Verification fetches bounded event pages from genesis through the selected run's
revision, verifies tenant/run identity, every event hash and the captured head,
then binds creation's goal/model/policy and transition counters. A newer head on
a later page does not substitute another snapshot. Terminal cancellation and
deadline results bind to their own event payloads.

The snapshot route supplies the server's exact canonical `state_json` and
`result_json` strings. Browser JSON re-serialization changes values such as
`42.0` to `42`; therefore the client hashes the provided bytes, checks their
parsed values equal the displayed objects, and compares against the verified
event commitment. It never substitutes a repaired model answer. Absence of a
state commitment remains explicit. Hashes show consistency, not an issuer
signature, hardware attestation or answer quality.

Child receipt lookup first resolves the authenticated tenant's run and requires
the requested child in its committed receipt list. The backend and browser
verify the current Batch receipt checksum, one completed child, model identity,
consistency checks and accepted usage against the recorded step. S3 archival can
change full receipt metadata/hash while accepted identity/usage remain fixed;
the original and current receipt hashes are both exposed. This lookup reads
ledger metadata, not fresh S3 payload bytes. Prior byte-readback evidence stays
separate. Drift is refused rather than displayed as verified.

Assets use a fixed whitelist and same-origin CSP; untrusted content is inserted
with DOM text nodes. Authenticated fetches refuse redirects, omit cookies and
never forward keys to an editable URL. Node regressions cover lost ACK/reload,
workspace/selection races, read-only and unsupported modes, tampering, float-byte
commitments, cancellation and child checksum/usage checks. Native PostgreSQL
tests cover shared-pool authentication, disabled routing, scoped child lookup,
snapshot bytes and reduced list projection. These are not visual browser tests.

## Native qualification plan

`ops.qualify_agent_console` is prepare-only unless `--apply`. It requires the
closed20/20 ordered-tool cohort,44 model children/88 prior artifact readbacks,
stopped isolated native PG, immutable candidate and unchanged Agent SQLv1.
It holds the campaign lock, restarts only that native cluster read-only, and
starts an extra loopback18174 Agent API with all mutations disabled. Independent
900s OS limits and pidfd cleanup bind those owned canary processes.

`ops/verify_agent_console.mjs` executes the actual client module against all20
real snapshots/event histories and44 current Azure child receipts, checks exact
asset hashes/CSP, pagination, unauthenticated refusal, mutation refusal and an
unassociated-child404. No new GPU task, upload, production schema/configuration
change or raw user-output export is permitted. Native records remain container
data, not Azure Agent deployment or Pod-loss durability. Actual completion must
be recorded separately; a prepared candidate is not a passed rollout. Browser
visual QA is unavailable under the current browser restriction.

## Closed native result

Candidate2f6bf7c passed [actual CI40247586664](https://github.com/yinli-systems/batchforge/actions/runs/40247586664):
1,673 Python tests, zero skips,39 Node tests and compileall, with independent
PostgreSQL17.11/Node24.22.1/UID65534. Its one-job runner deregistered. This includes
same-key correction after a definite first refusal, conservative handling of an
earlier lost ACK, read-only coordinator refusal and browser-storage denial.

The same source then passed the finite real-record HTTP qualification:
**20 snapshots,846 events and44 current Azure child receipts** verified through
the actual shared client module. Exact asset hashes/CSP, tenant authentication,
reduced list projection, mutation refusal and unassociated-child404 passed.
All temporary API/PG/Node children stopped; their port and native postmaster
record were independently checked closed, with the private primary still ready
and its finite guard armed. Primary identities/configurations remained unchanged.

[Closed lifecycle/readback](benchmarks/platform480-2026-10-25/evidence-api-agent-console-20261026-v2-complete/validation.json)
and [exact candidate source receipt](benchmarks/platform480-2026-10-25/evidence-api-agent-runtime-20261026-v7-source/source-receipt.json)
are exported with transport SHA verification. The first console source dff5139
also passed the same readback and1,672 Python/35 Node tests; its evidence remains
separate. The model-producing cohort is source0909c61, while these are UI/client
readback qualifications. No new GPU task or fresh S3 payload download was made.
This is not a second20-goal inference result, browser visual QA or production
Agent deployment. The Azure owner migration and enabled production mount remain
pending; the current primary still serves748dc10.
