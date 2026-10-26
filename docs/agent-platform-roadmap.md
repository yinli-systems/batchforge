# Agent-native BatchForge: implementation map

The goal is a complete Agent infrastructure platform. A working module below
does not mean the entire goal is complete. Existing GPU evidence remains useful,
but it does not establish Agent task success or every advertised capability.

| Layer | Current concrete state | Next acceptance evidence |
|---|---|---|
| Production inference | Live private Batch API, Azure identity/metering, S3 artifacts,480B MoE/4H100 | Preserve existing receipts and quota policy for Agent children |
| Agent harness | Typed loop and immutable ordered-tool prerequisites;20/20 fresh real480B goals qualified with44 children, post-ACK recovery and88 artifact reads; earlier failed cohorts retained | Azure owner migration and durable production rollout; larger independent task-success evaluation |
| Durable workflows | Atomic whole-graph reservations, fenced dependencies/cancellation; two real DAGs qualified; short Agent leases/independent renewal/action limits native-tested with real Batch-ACK recovery at13.58s | Owner-authenticated deployment, larger failure/latency studies, approval resolution and workflow inspection |
| Multi-Agent | Bounded static graphs of separate Agent roles; immutable lineage, grants and total allocations; six real480B roles qualified | Broader independent evaluations, safe approval/resolution and qualified runtime adapters; dynamic/recursive delegation remains unavailable |
| Memory/context/RAG | Versioned tenant/namespace PostgreSQL memory and full-text adapter | Qualified vector backend/index identity, retrieval/citation checks, bounded context and frozen retrieval eval; VectorMatch currently supplies matching/replication patterns and needs a distinct domain-data read adapter |
| GPU serving | Real exact-token admission, fair/cost-aware scheduling, Fleet, qualified finite latch guard | Agent interactive vs batch workload SLO/goodput under a preregistered matched study |
| Evaluation | Original full Batch SQuAD failure retained; separate improved1000-case Batch holdout and full10570 engine-only82.01/90.94 pass with raw replay/off-Pod copy; native Agent correctness tests | Publish engine evidence with explicit scope; frozen Agent success/tool-correctness/security cases and accepted-cost accounting |
| Observability | Existing GPU/queue/Fleet evidence; new paged Agent events/SSE | Workflow spans, step-level goodput/P99, explicit missing telemetry and receipt-linked costs |
| Permissions/security | Known capability whitelist, tenant scoping, read-only data tools | Human approval/resolution protocol and independently qualified code sandbox/egress policy before effectful tools |
| Multimodal/realtime | Text planner only; bounded event stream is not a multimodal model | Runtime capability checks, real image/audio/video fixtures, streaming cancellation/backpressure and latency evidence |
| Post-training/RL | Not implemented | Consent/sanitization-aware trajectory export, deterministic reward contracts, frozen holdout, reproducible bounded training/eval on existing GPUs |
| Product delivery | Existing batch console, receipts, quality/cost/Fleet pages; Agent console candidate qualified on20 real snapshots/846 events/44 current receipts, same-origin integration and client verification | Owner-authenticated deployment, safe approval, richer workflow inspection and browser visual QA when permitted |

Execution order is a real durable Agent loop first; then workflow composition and
retrieval; then isolation and runtime adapters; then multimodal and learning
loops. Each layer needs source, tests, an actual runtime qualification and an
honest evidence state. Do not manufacture tool results, nodes, metrics, quality
scores, user adoption, GPU measurements or billing evidence to fill the table.
Do not new-tenant or change retained-task limits to squeeze in a benchmark.

Current resource authorization is unchanged: the already running fourH100 Pod,
existing container disk, Azure and existing S3 only. No paid provider calls or
new cloud resources. GPU experiments must finish and recover before09:00UTC on
2026-10-26. Ephemeral control-plane experiments do not turn container storage
into whole-Pod durable storage. The real deployment must keep its durable
workflow state in the independently verified persistent store.
