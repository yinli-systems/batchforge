# Agent value-contract holdout preregistration

Baseline remains closed `quality_failed`:20/20 delivered,16/20 exact answer
successes. All four poisoned-document answers contained extra text. Its
controller, artifacts and grades remain unchanged. No expected-code substring
is extracted or promoted into an accepted answer.

The candidate runtime adds caller-declared generic `identifier`/`number`
contracts and required retrieved citations. Rejection is deterministic shape
validation, not reference-answer matching. A fresh model step receives a fixed
validation hint, costs real accepted tokens and consumes the same4-call/2,048
token run budget. Raw original child outputs remain in S3; rejection hashes and
receipts are retained. Incorrect values that satisfy the generic shape still
fail independent task grading.

Freeze `value-contract-heldout-v1` in `ops.qualify_agent_runtime` before running:
20 new synthetic cases, disjoint IDs and expected answers from baseline,
8 arithmetic/4 memory/4 memory→calculation/4 poisoned documents. All numeric
cases declare `number`; code cases declare `identifier`;12 retrieved-fact cases
require memory citations. Policy fields are included in the fixture SHA. Gold
values are used only in grading; the coordinator never receives gold data.
Required-tool checks still require the actual correct arithmetic result and
versioned memory citation, not merely a model's claim to have used a tool.

Maximum80 new existing-tenant Batch tasks. Recheck fresh retained-task, payload
and metadata headroom. No quota changes or alternate production tenant. Keep
the20-run immutable-source protocol, one coordinator post-ACK death, natural
120s lease expiry, private API restart/event-head readback and every child S3
artifact byte check. This explicitly isolated native Agent control database is
still not an Azure Agent deployment; actual model work/usage remains on the
existing Batch API/Azure/S3/480B service. Only canary application processes stop.

Add per-run DB-created→terminal duration and output-rejection counts to this
successor report. Its20 correlated synthetic cases are descriptive quality and
recovery evidence, not a production P99/SLO or controlled throughput comparison.
The changed inputs/contracts prevent treating it as a matched performance gain.
Whole-controller cost includes setup, fault wait and audit at$16.10/Pod-hour;
invoice and marginal cost remain unmeasured. Stop/recover before09:00UTC on
2026-10-26. A preregistration or passed CPU test is not a passed GPU result.
