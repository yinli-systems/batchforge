# Preregistered full-dev engine quality evaluation

Status: client, independent restoration trigger and platform-specific exclusive
controller/restore handler implemented. Native checks and real100-case
controller-loss qualification are required before full execution.
**No full engine evaluation has started or passed.**
Serving source remains748dc10. Service process epochs may change during guarded
maintenance; the480B engine and gateway identities remain protected.

Prerequisite source1bfada3 passed actual native
[CI40252909143](https://github.com/yinli-systems/batchforge/actions/runs/40252909143):
1,759 Python/zero skips,39 Node tests and compileall on PG17.11/Node24.22.1,
UID65534; the one-job runner deregistered. This includes actual isolated Linux
guardian/controller processes, including TERM-ignoring controller KILL, parent
exit, guardian TERM, handler failure/timeout and log failure. These tests use
dummy restore handlers and do not qualify restoration of the primary services.

The endpoint-fenced source5dd826d subsequently passed
[CI40253395659](https://github.com/yinli-systems/batchforge/actions/runs/40253395659):
**1,765 Python/zero skips,39 Node tests and compileall**, with zero remaining
runner registrations. A separate actual Pod CPU preparation parsed and verified
all10,570 cases and the100-case public-source canary, matching the old frozen
contract/scorer hashes. [Prepared source and plans](platform480-2026-10-25/evidence-api-engine-quality-20261026-v1-prepared/validation.json)
explicitly record zero HTTP/DB/GPU calls and an unqualified exclusive window/
platform restore handler. They are not an inference quality result.

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
engine quiescence. The client must target only loopback8348, with ambient
proxies/redirects disabled; standalone module execution refuses instead of
silently submitting work or exiting as a successful evaluation. Each request
uses the actual chat-tokenizer protocol, padded
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

Source27a0d30 adds the trusted `ops.engine_quality_campaign` operator roles.
The independent handler takes both global locks after the caller relinquishes
them or dies; only that handler restores. The exact runner is blocked on START
until its identity/config commitment is fsynced. Before inference, both API
replicas reject create/retry and two fresh Azure/worker/engine observations prove
drain. After direct inference, an exact pidfd-bound runner stop and two fresh
engine-quiescence observations precede reopening. Before runner START, restoration
preserves an already-running Batch worker and its accepted work. Foreign source/
configuration changes refuse actions. Four-card samples target5 seconds; raw
responses remain private. Full execution refuses without a closed, same-source
real100-case controller-loss restoration receipt. A new live restoration-handler
boundary allows the original qualified finite guard source to rearm without
resetting its persistent budget or zero-action lifetime allowance.

A passive actual-Pod preflight of immutable27a0d30 verified all six owned live
process environments, their source/profile pins, the current engine's startup-
bound172912-token physical cache and exactly four H100. It made zero HTTP or DB
requests, submitted no inference and changed no primary services. Passing this
preflight is not qualification of actual restoration.

## First real controller-loss qualification: failed restoration

The first100-case qualification, immutablef7edd48, produced32 fsynced inference
intents, a fresh nonquiescent engine observation and an actual four-H100 sample
before the controller deliberately exited42. It has zero terminal records;
those32 requests remain outcome-unknown, without retry or a quality score. The
independent restorer stopped the exact runner and restored worker/API service,
but failed to start the watchdog. The original interrupted campaign and failed
restoration report are retained unchanged.

Root cause: the operator's `PYTHONSAFEPATH=1` and candidate `PYTHONPATH` leaked
through the existing process manager into a supervisor env file that declares
neither setting. The root supervisor's script-relative fallback import then
failed. This actual-primary failure was not covered by the earlier toy-handler
process tests. Source0657482 captures private actual per-service environments,
starts each manager subprocess with that frozen profile, rejects undeclared live
Python source controls, and preserves the qualified legacy guard's clean import
environment. A real subprocess regression test reproduces the safe-path issue.
Private process profiles and raw responses are explicitly excluded from public
evidence export.

An independent immediate watchdog repair passed. A subsequent clean-source
repair attempt used system Python and failed on its missing psycopg dependency;
it is preserved. The new v2 repair used the existing platform venv, proved two
fresh Azure/worker/engine drain observations, rolled only APIs with a clean
import environment, restored all original configuration bytes and verified
API/worker/engine/gateway readiness. It rearmed the same qualified finite guard
source with zero lifetime recovery invocations, preserving the persistent budget.
Engine/gateway and worker were not restarted by that repair.

[Original failure](platform480-2026-10-25/soak480-engine-quality-controller-loss-20261026-v1-failed/recovery-validation.json)
and [clean-source repair](platform480-2026-10-25/evidence-api-engine-quality-source-repair-20261026-v2/validation.json)
are separate records. A new same-source real controller-loss qualification is
required before the full cohort; no failed run directory is replayed.

## New same-source qualification and full-run start

Source0657482 passed native CI40255880147:1,787 Python/zero skips,39 Node tests,
compileall, zero remaining runners. The new real100-case controller-loss run v2
then passed independent restoration. It recorded24 inference intents and no
known completed prediction:23 intents have no terminal record and one has a
conservative AuthorityLost unknown outcome; eight other cases ended AdmissionPaused
before dispatch. These are deliberately incomplete quality data, without replay.
Dispatch proof to verified restoration was8.779006 seconds, including8.605475
seconds in the handler. No engine/cloud action was performed.

A separate read-only audit verified all actual per-service profiles, exact original
configuration bytes, new owned identities/readiness, unchanged engine/gateway,
two fresh schema11/TLS/least-privilege DB-worker-engine idle observations and a
clean rearmed finite guard with zero recovery invocations.
[Qualification summary](../../ops/reports/engine-quality-restore-qualification-2026-10-26.json)
and [audit](platform480-2026-10-25/evidence-api-engine-quality-restore-audit-20261026-v1/validation.json).

The new full10570 engine-only run is
`/root/batchforge-runtime/soak480-engine-quality-full-20261026-v1`, controller735026,
immutable0657482. It has started real inference; this paragraph is not a final
quality pass. During its exclusive window both API create/retry gates are
intentionally closed and worker/watchdog/finite guard are intentionally stopped.
The480B engine and gateway remain running. The same independently armed
restoration handler must reopen and verify services after the run. Do not
interfere with its fixed source or start another load/client.

The actual Pod launcher disconnected and both interrupted controllers were
reaped. Their deaths and dispatch proofs are independently observed; the
intended os._exit(42) is source-pinned, but the OS wait status was not captured.
Native isolated CPU tests do capture the exit status. No actual Pod exit-code
observation is claimed in the qualification summary.
