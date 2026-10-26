# Engine evidence publication — October26

The existing private gateway8080 and APIs8082/8083 now serve the corrected full
engine-quality evidence and updated console from immutable **5a0a446**. Worker
source748dc10,480B MoE/TP4 engine and gateway identities were preserved.
This publication submitted **zero inference tasks** and created no cloud resource.
The full Agent platform remains unfinished; its Azure namespace migration still
needs owner authentication.

The API returns corrected full-dev **EM81.996216% / F190.940007%**,8667 exact
answers, the explicit one-record correction and original score history. The
original failed full Batch report and separate passed1000-case Batch holdout
remain visible. Engine evidence has no fabricated Batch IDs or ledger claim.
Exact download bytes hash to
`bd580e453973815abc0e865e2ea0e8d95b88616b32378c6c83d36bf2b09d8fff`.

## Closed acceptance sequence

1. [Exact-head native CI](https://github.com/yinli-systems/batchforge/actions/runs/40261699502)
   passed **1862 Python,45 Node, zero skipped**, PG17.11/Node24.22.1/UID65534
   and compileall. The one-job runner deregistered. [Authenticated receipt](../../ops/reports/github-native-ci-5a0a446-2026-10-26.json).
2. [Read-only Azure canary](platform480-2026-10-25/evidence-api-engine-product-qualification-20261026-v2-complete/validation.json)
   verified transaction read-only, schema11, actual tenant auth/401/404, old Batch
   states, corrected engine download SHA and served frontend hash. Its custom
   lifespan starts no reaper/archiver/GC/Agent; mutations are blocked before body
   parsing or storage access. The actual console functions and WebCrypto passed
   real HTTP checks. Both candidate processes stopped; original pins stayed fixed.
3. A real controller-loss qualification closed only the first API gate, verified
   create/retry503, then exited. The wrapper actually observed
   [exit42](platform480-2026-10-25/evidence-api-engine-release-dispatch-loss-20261026-v1-complete/validation.json).
   Its independent restoration process acquired both locks, closed the other
   gate, obtained two fresh DB/worker/engine idle observations and restored the
   original APIs. [Handler](platform480-2026-10-25/soak480-engine-product-release-loss-20261026-v1-complete/recovery-validation.json)
   elapsed **9.619137seconds**; the original profiles/config bytes and zero-action
   finite guard were restored. The interrupted parent retains its interrupted
   phase, not a forged completed campaign. This is an app-controller failure,
   not a physical GPU/whole-Pod outage or general RTO guarantee.
4. [Primary publication](platform480-2026-10-25/soak480-engine-product-release-primary-20261026-v1-complete/campaign.json)
   completed at**03:03:34UTC**. A separate restoration owner was armed before
   stopping the old finite guard/supervisor or changing API files. Both create
   and retry gates closed, accepted work drained, target APIs were verified while
   gated, then a committed sole handler reverified and opened them. It rearmed a
   new source-qualified finite guard with the same09:00 deadline and persistent
   recovery budget. No worker, engine or gateway restart occurred.
5. [Independent post-release audit](platform480-2026-10-25/evidence-api-engine-product-published-audit-20261026-v3-complete/validation.json)
   verified actual API process environments/source paths, normal writable
   readiness, authenticated evidence/invalid-key401/missing404 on all three
   endpoints, original Batch states, fresh four-H100 Fleet, exact frontend bytes
   and actual console download verification. The current guard was live with
   zero recovery invocations. The capacity API matched read-only Azure counters:
   **49910/50000 workspace retained tasks,90 headroom**, not physical disk/GPU
   capacity. Zero task rows were inserted after the full engine run closed.

All linked snapshots were allowlisted, exported to the Mac and SHA checked.
[Summary](../../ops/reports/engine-product-publication-2026-10-26.json) records
the distinct scopes and correction history. Private service profiles, credentials,
tenant outputs and model weights are excluded.

## Preserved failures and limits

The first read-only canary failed because its authenticated readiness route
followed the SPA catch-all and returned HTML. It changed no primary/SQL/GPU
state; [failed receipt](platform480-2026-10-25/evidence-api-engine-product-qualification-20261026-v1-failed/validation.json)
remains unchanged. The fix prioritizes that route and has an actual HTTP test.
An empty create-body probe would have produced422 before exercising admission;
the final source uses a valid body and tests both actual gate paths.

The first post-release audit launcher had a placeholder-substitution SyntaxError
before remote execution; its local failed launcher/log were retained. Auditv2
correctly qualified publication but labelled the all-database49930 task rows as
workspace retention. Its original report remains unchanged. Auditv3 uses the
authenticated workspace capacity and matching SQL ledger; the20 other rows were
not new submissions. Do not use auditv2's70-headroom calculation for admission.

These are CPU/HTTP/SQL/client checks, with no browser visual QA. Publication adds
no new answer-quality measurement, hidden test, invoice validation, hardware
attestation or whole-Pod failover. Existing container fsync remains ephemeral;
the persisted inference ledger is Azure and payload archives remain existing S3.
Normal rollout changes only API source/frontend/engine catalogue; the existing
Batch catalogue, model, worker source, quotas and retention policy stay fixed.
