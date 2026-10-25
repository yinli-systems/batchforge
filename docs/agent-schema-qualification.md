# Agent schema-v1: decoder qualification and rollout boundary

The two closed real-model Agent cohorts remain failed: baseline16/20 and the
disjoint generic-value-contract cohort15/20. The latter exposed three invalid
tool envelopes and two final answers citing evidence never retrieved. Rewriting
these outputs or accepting an invented citation would make the evidence weaker.

`schema-v1` projects server-granted tools and the current remaining tool budget
into a JSON-schema union. A final answer can cite only exact strings already
returned by retrieved memory; without required evidence the final branch is
absent. Identifier/number fields retain their generic runtime contracts. No
reference answer enters the grammar or parser. Typed parsing, actual tool
authorization, memory scope, citation checks and accounting remain mandatory if
an engine ignores a constraint. Protocol/citation rejection records the original
content/receipt hashes and uses a new metered step within the existing budget.
Default `json-v1` policy and request bytes remain unchanged.

The installed engine's versioned [vLLM0.10.2 structured-output documentation](https://docs.vllm.ai/en/v0.10.2/features/structured_outputs.html)
describes the standard `response_format.json_schema` wire format. Our primary
Batch API currently admits only text/json_object. The new ingress feature is
therefore **off by default** (`JSON_SCHEMA_ENABLED=false`), with a bounded subset:
32KiB,256 nodes,12 levels,8 union branches, closed objects, finite enums/bounds
and three qualified fixed patterns. References, remote resolution, recursive
definitions and arbitrary regexes are rejected before task admission. This is
not support for every JSON Schema draft or a generic output-verification service.

## Preregistered finite qualification

`python -m ops.qualify_agent_schema --run <new-owned-directory> --apply` starts
one extra API on loopback18172 from a SHA-verified immutable candidate. It copies
the existing runtime configuration, requires both primary admission gates open,
preserves quotas/model/retention and disables duplicate GC/archive maintainers.
The same existing worker and engine consume the shared Azure queue. No second
GPU worker/KV budget, new tenant, owner DDL or primary restart is involved.
An independent OS timeout limits the extra API to2700 seconds. The controller
verifies its actual process environment and primary identities before/after,
then stops only its own process group. A primary epoch/config change fails the
qualification; it is not a reason to replay the original directory.

First, four real Batch requests calibrate tool/number/memory-envelope/citation
decoding. One citation observation is explicitly synthetic and no proposed
memory tool is executed in this calibration. Strict receipts and accepted usage
must reconcile. Only a passing decoder report permits the next stage.

Second,20 fresh `schema-contract-heldout-v1` goals exercise arithmetic, stored
memory, memory-to-arithmetic joins and malicious retrieved text. The same
coordinator post-ACK crash, DB-lease reconciliation, event-chain/API-restart
checks, all child receipt checks and archived byte readbacks from the prior
Agent controller apply. Agent control state remains isolated native PostgreSQL
on the existing container; child inference/usage/artifacts are real Azure/S3/
480B-total35B-active MoE. This is not an Azure Agent rollout or Pod-loss durability.

The absolute maximum is84 new retained tasks (4 calibration +20×4 Agent calls),
subject to fresh existing headroom and the final09:00UTC experiment boundary.
No quota widening or history deletion. Previous fixture IDs, answers and failed
reports are preserved; the new holdout is disjoint. A passed tiny synthetic
cohort would establish these harness contracts, not general Agent competence,
SQuAD full-set quality, security against every attack or whole-platform completion.

Azure Agent migration still requires owner authentication. The user confirmed
the owner password is not saved; the one-time native hidden-input prompt timed
out without capturing a credential. No owner migration or password reset has
been attempted. Independent implementation and native/GPU qualification continue.

## Closed first schema cohort:8/20, not a quality pass

Source da4ad45 passed actual native CI40243462130:1,656 Python tests, zero
skips and23 Node tests. The four decoder calibration requests passed. The
20-case schema cohort then completed all20 runs but passed only8. All8 pure
arithmetic runs bypassed the requested calculator;4 also returned incorrect
numeric values. All4 memory→calculation joins returned correct answers/citations
but skipped the required calculator. The8 memory/poisoned-memory cases passed.
The independent grader required actual correct tool results and did not relax
that requirement to accept direct model computation.

There were32 accepted model steps and64 artifact byte readbacks. Coordinator
post-ACK recovery, Agent API restart/event heads, all primary epochs and child
usage/artifact checks passed; isolated PG and private ingress stopped. The
Agent report stays `quality_failed` and its parent stays failed. Shape-constrained
decoding does not establish completion of required business operations. Different
cases/contracts prevent treating these cohort scores as a matched improvement.

## Next bounded contract: ordered tool prerequisites

The caller may declare an immutable `tool_sequence`, at most8 entries, drawn
only from existing grants and within the same tool-call budget. It contains
tool names, never expected values. The grammar permits only the next required
tool until the sequence is complete; the runtime independently rejects a
premature final or out-of-order allowed tool before execution, preserving the
original accepted usage and spending a new budgeted model step. Actual persisted
tool observations advance this contract; model claims do not. Default empty
sequences preserve previous policy documents and request hashes. A coordinator
that does not understand a nonempty sequence is not a compatible rollback.

`workflow-contract-heldout-v1` freezes20 further disjoint IDs/values. Arithmetic
requires math.calculate; memory cases require memory.get; joins require
memory.get then math.calculate. Existing independent answer/tool/citation grading
is unchanged. Fresh headroom after the first schema cohort is166, sufficient
for at most84 new tasks including four decoder checks. Recheck before admission.
Run a new immutable source/CI and a new controller directory; never modify the
first failed cohort, its source or grades. This is a sequential workflow
prerequisite contract, not a general DAG or multi-Agent implementation.

## Closed ordered-tool qualification:20/20

Source0909c61 passed native CI40244273293 with1,660 Python/zero skips and23 Node
tests. A fresh private-ingress controller ran four decoder requests, then all
20 workflow-contract goals passed exact answer, actual required tool-result
and retrieved-citation checks. The44 child model tasks had44 observed attempts,
28,447 prompt and1,926 completion tokens. The original acknowledged child was
reconciled after coordinator death and natural lease expiry; no replacement
child identity was accepted. Agent API restart preserved result/event heads.

All88 distinct input/output artifact IDs passed byte readback after normal S3
archive/purge. All primary epochs/configuration stayed unchanged; private API,
coordinators and isolated PG stopped. The355.055s controller window allocates
$1.587885 at the operator-confirmed whole-Pod rate, including fault wait and
audit. This is not an invoice, a production per-answer price or a controlled
speedup over different earlier cases. Fresh retained-task headroom is118.

An extra independent audit first attempted to connect after the controller had
already stopped its native PG; that OperationalError is retained. A new owned
audit restarted only that isolated cluster in read-only mode and verified all20
actual tool sequences, full event chains and committed state/result hashes
against the closed report, then stopped its cluster. No GPU tasks or production
database mutation occurred. This demonstrates process-restart readback on the
container, not Pod-loss durability or an Azure Agent rollout.

Sources: [unchanged cohort report](benchmarks/platform480-2026-10-25/soak480-agent-runtime-schema-20261025-v2-complete/report.json),
[source-bound summary](../ops/reports/agent-workflow-v2-summary-2026-10-25.json),
[private-ingress lifecycle](benchmarks/platform480-2026-10-25/evidence-api-agent-schema-20261025-v2-complete/validation.json),
[independent read-only restart audit](benchmarks/platform480-2026-10-25/evidence-api-agent-tool-order-audit-20261025-v2-complete/validation.json).
The original16/20,15/20 and8/20 failed cohorts remain failed. The20 synthetic
cases establish this bounded workflow contract, not general model reliability,
full SQuAD qualification, a complete DAG/multi-Agent platform or production HA.
