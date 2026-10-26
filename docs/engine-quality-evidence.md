# Engine quality evidence

Implementation and local qualification complete; production publication is a
separate controlled API release. This catalogue is off unless configured. No
new database table or owner role is needed.

`GET /v1/engine-evaluations` lists only operator-published records explicitly
assigned to the authenticated tenant. `GET /v1/engine-evaluations/{id}` returns
a bounded projection and the SHA-256 of its exact response bytes in
`X-BatchForge-Evidence-SHA256`. Both use existing tenant authentication and
`Cache-Control: no-store`; a foreign tenant gets an empty list/404. These routes
do not query a Batch ledger or claim accepted Batch task identities.

Set `ENGINE_EVIDENCE_CATALOGUE` to an owned release catalogue outside the static
frontend directory. Its protocol is `batchforge.engine-quality-catalogue.v1`.
Each entry declares `id`, `name`, `tenant_id` and eight basename/SHA pairs:
`report`, `plan`, `scores`, `audit`, `campaign`, `restore`, `correction`,
`correction_audit`. It admits only the complete10570 official public SQuAD
case/contract/scorer commitments currently qualified by this protocol. New
datasets or scoring protocols require a separately reviewed version.

Startup validates owned non-writable files, path/stream/aggregate limits,
duplicate JSON fields, complete unique intent/terminal sets, exact-token
reservations, returned usage, score and pooled-latency arithmetic, timestamps,
70/80 thresholds, raw-replay/restoration audit commitments, nested cost windows
and the contract correction. It caches a whitelist projection; unknown private
fields, process profiles, raw responses and release paths are never returned.
A failed validation prevents publication rather than suppressing a failed score.

The trusted `python -m ops.package_engine_evidence` command validates before
creating an owner-only, fsynced new directory. It refuses existing destinations.
It accepts the closed benchmark directory, original replay audit, correction
directory and explicit tenant audience. On the Pod use the existing
`/root/batchforge-runtime/venv/bin/python`; system Python lacks some platform
dependencies. Packaging does not start services, submit inference or alter SQL.

The console displays engine benchmarks separately from Batch evidence, verifies
download bytes with WebCrypto, invalidates both sources on workspace changes
and tolerates an older deployment without the new route. It labels direct HTTP
service latency separately from completion of the whole offered cohort, and
shows the engine allocation as a subwindow of controller allocation. Recorded
post-run restoration is not current availability or whole-Pod failover.

## Contract correction

Publication validation found one invalid-format answer with legacy EM1/F10:
the fallback empty prediction and an article-only reference normalized to the
same empty string. The predeclared format contract requires invalid answers to
score zero. The official reference `normalize`/`score` functions are unchanged;
an explicit format-validity policy now precedes them in both evaluator clients.
Valid article-only predictions retain the original official metric semantics.

The original report, all10570 raw responses, journal, input/reference commitments,
token counts and times remain unchanged. A separate hash-bound correction lowers
exact results8668→8667 and EM82.005676→**81.996216%**; F1 stays**90.940007%**.
The70/80 gate still passes. The new source599b2fb passed1806 native Python tests,
39 Node tests/zero skips, followed by actual read-only replay of all7,045,415
raw bytes under the corrected policy. One record changed, no inference reran.

[Correction](benchmarks/platform480-2026-10-25/evidence-api-engine-contract-correction-20261026-v1/run/report.json),
[replay audit](benchmarks/platform480-2026-10-25/evidence-api-engine-contract-correction-20261026-v1/validation.json),
and [current analysis](../ops/reports/engine-quality-current-2026-10-26.json)
preserve this distinction. The catalogue refuses publication of the legacy
engine result without its correction and corrected raw-response replay receipt.
The old Batch full-dev failure and separate1000-case holdout remain separate;
their archived journals had no nonzero invalid-format scores.

Operator-published SHA commitments are not issuer signatures, independent
grading implementations or hardware attestations. Public dev includes earlier
calibration IDs. Allocation at the operator-confirmed16.10USD/h is not an
invoice, marginal cost or business-quality certification. No browser visual QA
has been performed in this environment.

## Controlled publication

Source5a4a3c7 passed the actual native GitHub job with1,852 Python and45 Node
tests, zero skipped. Its first real read-only qualification failed: the
candidate-only readiness route followed the existing SPA catch-all and returned
HTML. Its failed receipt remains archived; all candidate processes stopped and
no production service, SQL or GPU task changed. The follow-up prioritizes the
authenticated readiness route and adds an HTTP regression for this exact case.

`ops.release_engine_evidence` is a new, prepare-only API release controller.
It requires a closed source-matched real read-only qualification, freezes actual
private service profiles, then arms an independent pidfd-bound restoration
process before any mutation. It coordinates both campaign/recovery locks and
the finite latch guard. Only two API configurations and the supervisor change;
the worker, engine, gateway, model, admission budget, SQL schema and retention
policy are protected. It drains accepted work with the unchanged live worker.

Before commit, controller death restores the original APIs. After a verified
commit, the sole handler revalidates both gated APIs and the gateway, opens the
target APIs and rearms the original source-qualified finite guard under new
state pins. A failed publication attempts one compatible rollback; unknown
activity or foreign configuration keeps the window closed for inspection.
The persistent recovery budget is never reset. Actual controller-loss
qualification on the same source is required before normal publication.
This controller has local safety tests; native CI, real read-only validation,
real API controller-loss recovery and main publication are still separate
steps and are not established by this implementation description.
