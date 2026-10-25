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
results remain unchanged. The resumed cohort is now closed: **10,570/10,570 delivered**, no missing
predictions, 2,365,342 prompt tokens and 122,168 completion tokens. Recovered
segment timings remain distinct from the 2,628.085-second original-cohort
terminal-observation span. The valid measured result is **EM 53.8221%, F1
75.8213%**: both miss the unchanged 70%/80% gates. There are 5,689 exact answers;
on-time exact-answer goodput is 2.16469/s over that cohort span. Only four outputs
hit the 128-token cap; 10,566 ended normally. This is not a successful quality
qualification, even though durable delivery and deadline checks passed.
[Original complete report](platform480-2026-10-25/quality-squad-20261025-v4-complete/run/report.json)
and the full hash-bound score rows are preserved. The next capacity workload
measures forced generation, not a replacement quality result.

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

## Short quality-window GPU observation

A separate read-only window at 13:53:10–13:54:11 UTC sampled all four cards 30
times. Arithmetic sample means were 15.40%, 16.07%, 15.83% and 13.50%; maxima
were 100%, 98%, 97% and 99%. Model residency was high, but this short-answer
quality phase did **not** sustain full GPU computation. The observation is not
whole-campaign utilization, SM occupancy, energy or a performance A/B.
The older running primary exposed no admission-stage histogram series, so the
empty stage-delta array does not mean zero admission latency. New candidate
instrumentation must be checked after real creates reach it.
[Raw transferred observation and source binding](platform480-2026-10-25/quality-squad-20261025-v4-observation-v1/evidence-manifest.json)
are preserved without payloads or credentials. A subsequent real read-only
status aggregation showed retained historical completed payloads plus the fresh
cohort's pre-archival reservations, not a large failed-task payload backlog.
The existing archival cutoff and 120-second age were left unchanged.


## Product rollout and real endurance start

The product rollout passed at **13:59:37 UTC** on October 25. Both API replicas
now load immutable `398db6f`; worker and engine identities stayed unchanged.
A separate private gateway 8080 probe verified auth, receipt checksums, task
pagination, capacity, the exact frontend source hash and historical input/output
byte readback under real sustained load. The HTTP/S3 probe is a functional check,
not a latency SLA. The original source backups remain private on the Pod.

The 2,400-task endurance job actually started at 13:59:37 UTC and is still
running. In the first reported 20-sample window (14:00:48–14:02:47 UTC), GPU
sample means were 90.6%, 96.3%, 94.5% and 77.6%, with each reaching a sampled
100%. This establishes active four-card compute during that window, not 100%
continuous utilization or a completed endurance pass. Final throughput, P95/P99,
usage conservation, sampling gaps and cost remain subject to the closed report.
[Started campaign snapshot](platform480-2026-10-25/soak480-20261025-v1-started/evidence-manifest.json).

## CPU-only quality diagnosis

An offline analysis independently recomputed the same EM/F1 from all 10,570
original predictions and pinned references, without any new inference or score
change. Of the 4,881 non-exact predictions, 4,528 contain a complete normalized
reference span plus extra text; 200 have token overlap without full containment,
and 153 have no overlap. A contained gold string does **not** prove the entire
answer correct (it can include contradictory or irrelevant alternatives), so
this diagnostic is not an improved score or a deployable answer selector.
Only four outputs were capped; simply raising the token limit does not address
most of these errors. A future prompt/output-contract experiment needs its own
fixed cohort, preregistered scoring and actual measurements after the current
GPU campaign closes. Raw predictions and benchmark thresholds remain unchanged.

## Endurance closure and quality-product publication

The separate fixed compute study subsequently completed **2,400/2,400** tasks
at 15:20:28 UTC, with unique acceptance, conserved metering and exact output
lengths. Its [closed report and analysis](endurance480-2026-10-25.md) retain
336.499 output tokens/s, both durable and scheduled-arrival latency, all 758
NVML samples per device and sampling gaps. This does not change the failed
SQuAD quality gate above.

After that closure, API source `831b839` published the authenticated Quality
evidence page and stronger task-history receipts at 15:20:51 UTC. Worker and
engine identities stayed unchanged. A 3,600-task reusable-input saturation study
then started at 15:20:53 UTC; it has a separate protocol and no final result yet.
