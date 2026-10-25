# Public ground-truth quality campaign and admission failures

The complete public SQuAD 1.1 dev set contains 10,570 questions. The exact
4,854,279-byte dataset is pinned by SHA-256
`95aa6a52d5d6a735563366753ca50492a658031da74f301ac5238b03966972c9`.
The model is the already-serving Qwen3-Coder 480B-total/35B-active AWQ MoE on
four H100s. No model judge or paid inference API is involved.

Each request asks for the shortest answer span, temperature zero, cap 128 tokens,
with ordinary EOS. Exact match and multiset F1 follow the official SQuAD 1.1
normalization and maximum across reference answers. This is a public dev set,
not a hidden test or contamination audit. The predeclared complete-cohort gate
is EM >=70%, F1 >=80% within 3,600 seconds. Missing delivery invalidates the
quality result; a delivery failure must not be called a model accuracy score.

Closed failures are retained under
`platform480-2026-10-25/quality-squad-20261025-v{1,2,3}` with verified export
manifests, plans and original reports:

| Segment | Actual acceptance | Outcome |
| --- | ---: | --- |
| v1 | 0 | 11 oversized creates rejected by the 320 MiB logical payload limit |
| v2 | 64 | Concurrent create received 503; private API diagnostic was LockNotAvailable; later admission stopped |
| v3 | 1,406 | Small batches and identical-key transient retries worked; later 429s were archival backpressure |

The original v1 JSON contains zero EM/F1 from zero predictions. That is a
reporting defect: **no valid quality score was measured**. The original is kept;
current code publishes `evaluation_valid: false` and null scores when delivery
is incomplete. None of these failed complete-cohort checks demonstrate 0%
model accuracy.

The underlying schema deliberately retains response/error reservations after
completion until verified object archival and payload purge. Reducing the
in-flight batch count alone cannot handle that lag. The client now waits, with
bounded backoff, for `retained_payload_limit` or `queue_limit`; permanent history
limits still require operator action. Transient creates reuse the exact body
and idempotency key. Unknown upload outcomes never trigger automatic re-upload.

v4 resumes v3's **same** `bench-457e33f0db9f` workload: accepted batches are
re-observed, rejected submissions reuse their known input files and keys. The
source/runtime/dataset hashes and original deadline remain bound; resumed
segment timings are separate from complete-cohort elapsed time. Closed v3
results remain unchanged. The complete quality result is pending until the
resumed report closes and its byte/hash/ID checks pass.

## Next bounded endurance protocol

`ops.product_campaign` waits for that quality report, checks actual primary
managed identities, qualifies the immutable API candidate, closes admission on
both primary replicas and drains accepted work. API rollout preserves original
private configuration, never switches the model, and restores compatible API
configuration on failure. GPU worker/engine identities must remain unchanged.

After successful API rollout, the same four H100s run a preregistered 2,400-task
balanced 20-cell input/output mixture through the real private Batch API.
Offered arrival rate is 0.5 tasks/s, maximum in-flight 128, cohorts 20,
admission cap 7,200 seconds and drain cap 1,800 seconds. Forced output lengths
make this a compute/endurance workload, **not** another answer-quality score.
GPU/engine/HTTP/database samples occur every five seconds, with gaps reported.
No unmeasured hourly price is substituted: costs stay null. Results are pending.

Reference metric:
[official SQuAD v1.1 evaluator](https://github.com/allenai/bi-att-flow/blob/master/squad/evaluate-v1.1.py).
Dataset/license:
[Stanford SQuAD](https://rajpurkar.github.io/SQuAD-explorer/).

## Candidate and queued launch evidence

The immutable `398db6f` candidate at private port 18150 passed real SQL/HTTP
qualification and independent byte readback of both archived input and output.
[Candidate evidence](platform480-2026-10-25/evidence-api-20261025-v2-launch/evidence-manifest.json)
includes actual managed identities, source hashes and the first name-collision
preflight rejection, which did not stop the existing canary or alter primary.
The old v1 canary was stopped only after the distinct v2 qualified.

The [queued campaign snapshot](platform480-2026-10-25/soak480-20261025-v1-queued/campaign.json)
records independent controller PID 580284, started at 13:29:51 UTC on October 25.
This snapshot is **waiting_quality**, not a completed rollout or endurance result.
The launcher binds `/proc` start ticks and cwd; passive preflight verified all
five primary service identities plus watchdog and the actual 62,259,600-byte /
1,632,000-output-token plan. The controller and source snapshot survive client
SSH disconnection but do not survive losing the entire container disk.
