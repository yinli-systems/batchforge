# Workflow sources: explicit inheritance and independently inspected commits

The candidate adds opt-in `inherit_sources` to workflow nodes. Ordinary Agent
requests cannot set it or assign parent lineage. Old default policy/workflow
request documents remain unchanged. A receiving node has explicit declared
dependencies and citation requirements; it can have **no tool/query permission**
and still cite verified evidence from those dependencies.

`agents.provenance` reads a repeatable-read, tenant-scoped snapshot of the frozen
workflow and its bounded ancestor set. It checks actual server-assigned IDs,
model/goal/policy/deadline, dependency projections, immutable terminal state and
result hashes, and the terminal event anchor. Only citations actually emitted
by committed upstream answers propagate. Transitive provenance deduplicates
nodes/edges, limits the graph to16 roles, and respects `max_dependency_bytes`;
it does not enumerate exponentially many paths.

Pinned BM25 chunks replay against the audience/namespace/corpus registry.
Memory sources bind the committed version/full hash and returned excerpt. A
truncated historical memory excerpt is explicitly **not** full-document replay;
merging get/search excerpts retains their common bytes. Updating current memory
does not rewrite an already committed historical citation. Source text remains
untrusted. A source commit is not semantic correctness or proof of model attention.

The runtime resolves fresh source authority before planning and new uploads/
model dispatch, and again before terminal output. Cached/model-supplied source
JSON is never used as authority. A lost Batch acknowledgement can be reconciled
read-only from its tenant/body-bound stable key after source revocation. Its
already accepted output is still charged before the Agent fails authorization.
A previously attempted POST whose acceptance remains unknown enters explicit
`waiting_approval`; the same step is not posted again after revocation. Manual
resolution of that unresolved state remains separate unfinished work.

Final results include the used `batchforge.workflow-sources.v1` envelope. Source
inheritance does not weaken the opt-in verbatim-span contract: inherited BM25
chunks are rechecked against the frozen corpus before an extracted answer can
be accepted. A general cited synthesis can combine sources without claiming its
entire sentence occurs verbatim in one document.

`GET /v1/agent-runs/{id}/sources` revalidates available sources for the authenticated
tenant. The console's **Verify upstream sources** independently checks the parent
workflow, every relevant node's full event/state/result commitments, declared
edges, emitted citations and actual origin tool observations. It refuses drift
and stale workspace/selection responses. Its label does not certify answer
quality. No browser visual QA has been performed.

[The frozen three-graph protocol](../ops/reports/source-workflow-preregistration-2026-10-26.json)
uses synthetic memory, two new public questions disjoint from both prior RAG
cohorts, transitive relays and mixed-source fan-in. Maximum allocation is28 tasks
in the original workspace. Native PostgreSQL, actual client and real480B runtime
qualification are required before claiming this candidate qualified or deployed.
The primary service and Azure Agent owner migration are unchanged.
