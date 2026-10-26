# Preregistered full-dev engine quality evaluation

Status: client and independent restoration-trigger prerequisites implemented;
the platform-specific maintenance/restore handler and its real qualification
are not yet complete. **No full engine evaluation has started or passed.**
Serving primary748dc10, its worker and guard remain unchanged.

The target is the complete10,570-question public SQuAD1.1 dev set on the existing
480B-total/~35B-active AWQ MoE/four H100 engine. The earlier full **Batch API**
quality gate53.8221/75.8213 remains failed. The improved1,000-question public
holdout82.8/91.429 is separate. This proposed full run uses the frozen
`json-span-v1` synthetic few-shot contract, natural EOS, temperature0, max128
output tokens, the original reference-independent parser and official EM/F1
normalization. Gates stay70% EM/80% F1. Full dev includes the old calibration
IDs and is neither a new hidden holdout nor a contamination audit.

The Pod's existing dataset was independently read and hashed, without inference:

- Dataset bytes:`95aa6a52d5d6a735563366753ca50492a658031da74f301ac5238b03966972c9`.
- Canonical ID/context/question/reference manifest for all10,570 cases:`3df3ccf00f3d7c9610cacdf4d53980320777b9516a748c60e2cacbd82bb8ca66`.

The client rejects altered input bytes or a forged dataset label over synthetic
cases. Workload hashes exclude gold references; references have a separate
commitment. A100-case maintenance/recovery canary may select from the same
verified source corpus but cannot become a full-cohort pass. CPU fixtures remain
explicitly detected and never publish a full quality pass.

`scripts.engine_quality.Evaluator` requires a live exclusive-window authority
callback, durable intent/terminal journal and a separate private raw-response
sink. It validates vLLM0.10.2, the sole served model,18440 context and initial
engine quiescence. Each request uses the actual chat-tokenizer protocol, padded
to16-token blocks, reserving prompt+128 output against164256 tokens and at most
32 concurrent requests. The engine's actual fresh physical KV capacity must be
verified by the maintenance controller before that budget is granted.

There is one HTTP inference attempt per question. A timeout/transport error,
identity/usage mismatch or revoked authority latches admission paused. Unknown
outcomes are retained, not replayed or reported as zero usage. In-flight requests
may finish; final restoration must independently prove engine quiescence. Missing
or incomplete delivery hides aggregate EM/F1. Invalid JSON, duplicate fields,
extra fields or invented spans score zero without gold-assisted repair. Finish
reasons remain reported using the frozen parser's scoring semantics. Intent,
raw response and scored-answer hashes are distinct.

The measurement window includes exact tokenization/admission/inference; HTTP
service latency and offered-cohort completion latency have separate pooled
distributions. Engine throughput/goodput is not Batch acceptance throughput/P99.
Engine-only outcomes have no new Azure task identity, Batch accepted-usage
receipt or platform failure-recovery claim. Cost uses the confirmed16.10USD/h
whole-Pod wall allocation and is not an invoice or marginal cost. Maintenance
and restoration cost belongs to the outer controller window; overlapping windows
must not be added.

`ops.maintenance_guard` supplies a two-phase pidfd-bound trigger. Before BEGIN,
a failed setup handshake causes no restoration or controller signal. After
BEGIN, normal RESTORE, controller exit, pipe failure, guardian TERM or a fixed
deadline calls one trusted restore command. Deadline/invalid-pipe handling stops
the exact controller with TERM then bounded KILL before invoking restoration.
Broken reporting/log creation does not skip the handler. Handler failure or
timeout remains failed and is not automatically replayed. **Handler exit0 alone
is not proof that actual platform restoration succeeded.** This primitive is
not a shell tool capability and never accepts commands from a model or public API.

Required next deployment qualification before full evaluation:

1. Bind the current immutable worker/API source, manifest/config bytes, real
   managed identities, model shards and fresh physical KV evidence.
2. Acquire campaign/recovery ownership, stop the finite guard safely without
   resetting its action allowance, arm independent restoration, then close
   create/retry admission on both primary APIs and prove drain twice using
   fresh Azure/worker/engine observations.
3. Stop only the owned idle worker/watchdog required for exclusive inference.
   Preserve the engine/gateway identities; no cloud, driver or model restart.
4. The independently owned restore handler must first terminate the exact
   evaluation client, wait for fresh engine quiescence, restore only known
   original/gated environment bytes, verify source/configuration and new live
   service identities, then reopen admission and rearm a compatible finite guard
   with the remaining lifetime/persistent budget.
5. Qualify controller loss/deadline restoration on isolated native CPU service
   fixtures and a bounded real100-case engine canary. Preserve the intentionally
   interrupted run separately; it is not a quality pass. Only then run the full
   cohort in a new directory, with a fixed completion/restoration margin before
   2026-10-26 09:00UTC.
6. Sample actual four-card NVIDIA/engine state with explicit missing values and
   gaps; export allowlisted plan/scores/journals/samples/failures to new Mac
   directories, verify hashes and push. Raw responses stay private. Container
   fsync does not establish Pod-loss durability.

No retained-task quota change, new tenant, history deletion, new paid resource
or paid inference API is authorized or needed for this engine-only study.
