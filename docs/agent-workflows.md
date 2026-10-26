# Bounded multi-Agent DAG execution

This application candidate adds static, typed DAGs to the existing Agent
execution store. It is not enabled on the serving API. The Azure Agent namespace
still requires owner authentication; no administrator password was saved and
the runtime role has not been elevated. Agent SQLv1 and serving schema11 stay
unchanged. Native PostgreSQL qualification and real model execution must be
reported separately from implementation or deterministic adapter tests.

The first [real480B qualification](benchmarks/agent-dag-2026-10-26.md) closed
successfully: two graphs/six Agent roles,14 unique model tasks/14 attempts,
coordinator loss after committed activation, native PG/API restart and28 artifact
byte readbacks. This does not deploy the Agent namespace on Azure or establish
large-scale throughput, broad task quality or whole-Pod availability.

`POST /v1/agent-workflows` admits at most16 named Agent nodes, declared
dependencies and output nodes, with at most8 active child Agents. Admission
rejects cycles, unknown or duplicate dependencies, unused branches, unserved
models, tools outside parent grants, and child allocations exceeding the total
model-call, tool-call or generated-token budget. The graph is immutable. A model
cannot add a node, change a namespace, grant tools or spawn recursive delegates.

The parent and **all** child execution rows are created in one tenant-locked
transaction. Existing database triggers reserve active/retained quota for the
whole graph, including blocked nodes. A quota failure rolls back the parent,
children, events and usage together. Same-key replay returns the same graph and
child IDs. There is one absolute database deadline, including dependency waits.
This requires up to17 active slots; reservations can reduce concurrent admission.

A separate `workflow-coordinator` claims only workflow control rows. Ordinary
coordinators claim only Agent rows whose dependency gate is open. Graph parents
use the explicit program identifier `batchforge/workflow-v1`, which is not an
inference model. They never submit inference. Ordinary Agents retain their
existing durable Batch outbox and fenced model/tool execution.

Dependency activation locks tenant, parent and children. It records the exact
completed child ID, event revision/head, full-result hash and a separately hashed
answer/citation projection. This bounded data enters the downstream planner as
untrusted workflow results; it does not authorize a tool or count as a directly
retrieved memory citation. Context overflow fails the graph without truncation.
Activation events and the parent checkpoint commit atomically, with a final
database-clock lease check. Lost ACKs reuse the committed child identities.

Parent cancellation or deadline cancels every nonterminal child in the same
transaction used for activation. A failed/cancelled child fails the graph and
cancels remaining work; completed sibling records stay immutable. An accepted
Batch can still finish after Agent cancellation. Its usage stays in the original
Batch ledger. This is not external exactly-once execution or a physical GPU
attestation.

Final outputs reference child results and event heads. Parent model/tool counters
remain zero because the parent does no inference; `result.usage` explicitly sums
child execution counters. These counters are orchestration budgets, not a second
billing ledger. Never sum parent aggregation and child usage as separate charges.

Admission is off by default (`AGENT_WORKFLOWS_ENABLED=false`). Enabling requires
compatible API **and ordinary and workflow coordinators**, enrolled executor
credentials and the owner-applied Agent schema. Older coordinators do not
understand immutable lineage or blocked children and are not a compatible
rollback while graphs are active. Stop admission and drain accepted workflows
before downgrading. The existing same-origin Agent inspection endpoints expose
parent/child snapshots and events; the console does not yet have a DAG editor.

Example (explicitly a bounded two-role graph, not a dynamic planner):

```json
{
  "nodes": [
    {"id":"subtotal","goal":"Calculate 21*2 using math.calculate; return only the number.",
     "model":"QuantTrio/Qwen3-Coder-480B-A35B-Instruct-AWQ","answer_format":"number","tool_sequence":["math.calculate"]},
    {"id":"review","depends_on":["subtotal"],
     "goal":"Check the dependency answer by calculating 21*2, then return the number.",
     "model":"QuantTrio/Qwen3-Coder-480B-A35B-Instruct-AWQ","answer_format":"number","tool_sequence":["math.calculate"]}
  ],
  "outputs":["review"],
  "max_parallel":2,
  "max_model_calls":8,
  "max_tool_calls":4,
  "max_generated_tokens":4096
}
```

Use an actually served model identifier from the authenticated capability API.
The current implementation has no arbitrary-code executor, effectful tool
approval/resumption, graph mutation, distributed transaction with external
services, recursive delegation, workflow UI editor or verified production
database deployment. Those remain separate platform work.
