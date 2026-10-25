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
