# Cross-Agent source lineage: transport verified, answer quality failed

The frozen three-graph real480B qualification on2026-10-26 is **quality_failed**.
Source `bda7eb5` completed all10 Agent roles but only **1/3 workflow goals** was
reference-correct: synthetic memory passed; both public retrieval roots failed
exact match. Source provenance and delivery do not establish semantic quality.
The [preregistered plan](../../ops/reports/source-workflow-preregistration-2026-10-26.json)
and [structured summary](../../ops/reports/source-workflow-summary-2026-10-26.json)
retain the fixed gates and failed result. No answer or gate was repaired.

## What the real run established

- Four existing H100 GPUs served the480B-total/~35B-active MoE through the existing
  Batch API, Azure task/usage ledger and S3.14 unique tasks/14 attempts consumed
 21,584 prompt and1,538 output tokens. No additional cloud resource was created.
- All six receiving roles independently verified upstream source commitments;
  the actual JS client verified10 roles,466 events and14 Batch receipts. Answers
  and citations propagated without rewriting and without downstream query grants.
- A coordinator exited after committing dependency activation and before returning
  its acknowledgement. The same committed child identities completed. This is a
  workflow activation failure, not a physical GPU failure or Batch-ACK lease study.
- Native PostgreSQL/API restart readback passed. An independent audit confirmed
  all14 ready/purged S3 archives,14 retained Azure identities after payload purge,
  and **all28 distinct input/output artifact bytes and SHA256**. All trial
  processes stopped, primary source/configuration/PIDs stayed unchanged and the
  queue was empty. The audit passed while explicitly preserving failed quality.

The controller wall was239.908470s, allocating **$1.072924** at the user-confirmed
whole-Pod$16.10/hour. This includes control/archive waiting, is not an invoice,
not marginal cost and not a qualified-cohort answer price. Only11 GPU samples
cover the workflow loop; they exclude most archival wait. This small functional
study is not evidence of sustained GPU saturation or a capacity improvement.

## Failure diagnosis

SHA-verified S3 artifacts and the frozen corpus show both actual retrieved and
cited excerpts contained reference spans. Answers scored EM0 with F1**80%** and
**85.7143%**. One contained the normalized reference plus extra words; the other
selected a different answer despite an available reference span. These two
observations motivate an answer-extraction adapter; they do not prove all retrieval
is sufficient or establish a causal mechanism.

The goal text requested five search results, but both actual model tool calls
omitted that argument and used the default **three** with rewritten queries.
That textual goal was not enforced as a typed argument contract. We do not claim
five-result search was achieved. Both questions are now development cases and
must be excluded from future independent retrieval-answer qualifications.

## Evidence and limits

[Original failed report](platform480-2026-10-25/soak480-agent-runtime-dag-source-lineage-20261026-v1-quality-failed/report.json),
[independent closure/ledger/S3 audit](platform480-2026-10-25/evidence-api-source-workflow-audit-20261026-v1-complete/validation.json),
and [read-only failure diagnostics](platform480-2026-10-25/evidence-api-source-workflow-diagnostics-20261026-v1-complete/validation.json)
are exported with transfer manifests and exact validator sources/hashes.

The stopped native control database has an owner-only off-Pod Mac copy preserving
all1147 files and27 directories. Its complete archive is SHA-verified, but **this
specific source-workflow snapshot has not been restored**. The earlier RAG
terminal-state restore remains separately qualified. Private database contents,
raw model outputs, environments, keys and weights are not committed.

Exact-head native [CI40271674578](https://github.com/yinli-systems/batchforge/actions/runs/40271674578)
passed1955 Python/zero skips,50 Node tests and compileall, PG17.11/Node24.22.1,
non-root UID65534, with zero remaining runners. Its preceding88dc2a9 check failed
one fixture lacking a test-only worker token; that failure remains preserved.
The corrected check does not turn the real semantic qualification into a pass.

The primary API remains5a0a446, worker748dc10,480B serving unchanged. This Agent
candidate has not been deployed against Azure: runtime lacks DDL privileges and
the owner credential is not stored. Registry grants are loaded per process;
hot file revocation/reload has not been implemented or qualified. Source proof
checks availability/commit consistency, not semantic truth, signatures, GPU
attestation, whole-Pod durability or production high availability.
