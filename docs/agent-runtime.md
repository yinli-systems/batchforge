# Durable Agent runtime: current implementation and rollout contract

The Agent runtime is an opt-in, additive control-plane service. Its model steps
use the existing BatchForge queue and 480B-total/35B-active MoE worker; it does
not start another inference engine or call a paid provider. This first runtime
implements a sequential typed planner/tool loop. It is not the entire Agent
platform, an arbitrary-code sandbox, or an already deployed Azure feature.

## Persistence and authority

`bf_agents` is an independent versioned PostgreSQL namespace. Its migration
leaves the serving `public.batchforge_schema_migrations` ledger at version11.
Startup only verifies both ledgers. It never attempts owner DDL or widens role
privileges. `python -m agents.migrate --plan --runtime-role bf_runtime` prints a
connection-free, checksum-bound plan; an owner executes the actual migration
using a protected `DATABASE_URL`. The command does not log a connection string.
Existing tenants receive database-owned defaults:32 active runs,1,000 retained
runs and16MiB memory each. New tenants require owner policy enrollment. Runtime
credentials cannot update those limits/counters, delete retained runs/events,
or turn a terminal run back into active work.

Each run has a tenant-scoped request key, immutable goal/model/policy, bounded
model/tool/generated-token budgets, a database deadline, a revision, an event
hash head, and a DB-clock coordinator lease with a fresh fencing token/epoch.
Admission, cancellation, quota accounting and state transitions lock the tenant
first. Lost-ACK transition replay does not charge counters twice. Late lease
holders cannot commit after expiration, reassignment or cancellation. Terminal
state is immutable. Hash-chained events are append-only for runtime roles;
checksums demonstrate consistency, not an issuer signature or hardware proof.

Model output is strict JSON data. Duplicate names (including nested names),
markdown extraction, additional authority fields and unobserved memory citations
are rejected. Invalid planner output still charges its accepted generated tokens.
The model cannot choose a tenant, executor key, memory namespace, endpoint or
new capability. The opt-in [schema-v1 candidate](agent-schema-qualification.md)
also constrains legal tool envelopes and observed citations at decoding, while
retaining runtime enforcement and bounded metered correction. Currently
available tools are bounded literal arithmetic and
read-only tenant/namespace memory get/search. The calculator uses a bounded AST,
not Python evaluation. Shell, network, arbitrary Python and effectful tools are
unavailable. This is capability enforcement over trusted tools; it is **not** a
process isolation backend.

Memory writes are explicit authenticated API operations with optimistic versions.
Reads return versioned citations and the full document SHA; excerpts identify
truncation. Search is PostgreSQL full-text retrieval. The user's current
VectorMatch repository implements order matching, journals and replicated
execution. Its future integration needs an observed domain-data read adapter;
dense-vector retrieval needs its own qualified backend and index identity.
See [integration boundaries](vector-retrieval-integration.md). Retrieved
text is marked untrusted. A model following malicious text still cannot acquire
ungranted tool capabilities; this alone does not guarantee semantic correctness.

## Model-step outbox and uncertain I/O

The candidate output-contract extension lets the caller request a generic
`text`, `identifier` or `number` answer string and require retrieved citations.
Validation never receives a reference answer and does not extract a substring,
strip prose or normalize the model's answer into a passing result. A rejected
value records its original content/receipt hashes and requests a new model step
with a fixed runtime validation hint. That step has a fresh child identity,
consumes the same remaining budget and retains all accepted token usage. It can
fail the run when the budget runs out; there is no unlimited repair loop.

Default text/no-required-citation requests keep the exact v1 policy document
and idempotency hash. New non-default contracts are explicitly immutable run
policy. A deployment must keep a coordinator that understands them until those
accepted runs finish; an older coordinator is not a compatible rollback for an
active identifier/number-policy run. The serving Batch schema11 and Agent SQLv1
checksums are unchanged by this additive application contract.

An optional immutable `tool_sequence` declares caller-required tool prerequisites.
It is bounded by the original grants and run budget. For example,
`["memory.get","math.calculate"]` requires a committed memory observation then
a committed calculation before finalization. Model claims or a correct-looking
answer cannot substitute for execution. Schema-v1 permits only the next required
tool, and the runtime separately enforces the order even if the provider ignores
the grammar. A refusal keeps all original accepted usage and requests a new
budgeted decision. This is a sequential contract, not a general DAG or delegation
system. Empty sequences keep old request hashes; older coordinators cannot safely
process accepted nonempty sequences.

Schema-v1 also requires a Batch ingress qualified for `response_format.json_schema`.
The currently serving primary still leaves that new feature off. Qualification
uses the bounded private ingress18172; a durable production Agent rollout needs
the owner migration and a separately reviewed compatible ingress deployment.

Before any external action, persist a model-input intent and content SHA.
Before the single upload attempt, persist `upload_attempted`. After a crash,
reconcile an exact tenant/filename/size/SHA match and independently read its
bytes. If no matching published artifact exists, enter `waiting_approval` and
do not automatically upload again. The first version supports inspection and
cancellation, not an automated approval bypass or an input-repair endpoint.

Each child Batch has the stable key `agent.<run-id>.<step>`. An uncertain Batch
ACK retries that exact request/key and file, never another task identity. All
admission goes through the existing HTTP endpoint, preserving its gate and
quotas. The coordinator reserves a model-call budget before dispatch; that is
an orchestration budget, not a second billing ledger. Before using output it
requires exactly one completed child, matching model/custom ID, a nontruncated
response, consistent strict receipt, and matching accepted token usage. Persist
delivery and charge usage before interpreting the next model decision.

Agent cancellation fences subsequent Agent commits. It cannot retract GPU work
already accepted by the Batch API, or make its usage disappear. A child accepted
in the cancellation/HTTP uncertainty window can finish after cancellation; its
original Batch ledger remains authoritative. External side-effect tools and
exactly-once external effects are deliberately not claimed. Memory read tools
may be re-executed before a durable result commit; committed observations stay
fixed across subsequent model steps.

## Private service boundary

Install the `agents` optional dependency. A root-only regular JSON file maps
explicit tenant IDs to their existing executor API keys; it is never included
in run state or exported evidence. The CLI rejects symlinks, wrong ownership,
group/other-readable files, duplicates and unbounded registries. Coordinator
startup verifies every key belongs to the configured tenant. No owner credential
is passed to a GPU worker.

```
python -m agents api --executor-keys /protected/agent-executors.json --port 18170
python -m agents coordinator --executor-keys /protected/agent-executors.json \
  --batch-url http://127.0.0.1:8080 --seconds 3600
```

The API binds loopback. Routes cover create/list/read/cancel runs, paged events,
bounded30s SSE streams and versioned memory. Run creation requires a stable
`Idempotency-Key`, an operator-configured executor and a served model. Paging
queries only bounded event pages, verifies continuity from the stored anchor,
and includes the current head. A client independently verifies the full stream
by fetching from revision0; SSE does not repeatedly download the whole history.
Public process liveness is distinct from database/schema/writability readiness.

## Validation and outstanding rollout work

The tests use actual isolated PostgreSQL for concurrent admission, lost-ACK
replay, lease expiry/reclaim, cancellation, budgets, tenant isolation, immutable
events, memory accounting and restricted-role quota enforcement. Coordinator
crash tests inject ambiguous upload/Batch outcomes and prove that the accepted
child identity is reused. HTTP integration exercises the actual Batch API,
database ledger and receipt with a clearly synthetic model worker. These are
CPU correctness tests, not real-GPU Agent quality evidence.

The existing Azure runtime role lacks CREATE privilege. No Azure owner
credential is persisted in the known restricted configurations. Owner credential
is not saved, as the user confirmed. A native hidden-input prompt expired without
capturing it; no password guessing, privilege escalation, public11
ledger change or production cutover is attempted. Continue native qualification
and a reviewable migration while resolving that requirement. The first real
Agent canary must preserve this distinction if using an isolated test control
database while its child Batch tasks run on Azure/S3/real480B.

Before rollout: immutable source receipt; exact schema/grant verification;
private canary; real planner/tool task-success evaluation; coordinator restart
and stale-epoch checks; child receipt and artifact readback; bounded cost/latency
measurement; current task headroom; and a rollback preserving accepted children.
The live finite GPU guard pins existing services, so stop/requalify it before
changing pinned model/API/worker configurations. No resource upgrade is needed.
