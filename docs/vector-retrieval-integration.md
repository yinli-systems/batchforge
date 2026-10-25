# VectorMatch and Agent retrieval integration boundaries

Repository inspection on2026-10-25 found that
[yinli-systems/vectormatch](https://github.com/yinli-systems/vectormatch/blob/main/README.md)
implements deterministic limit-order matching, a journal, Aeron replicated
execution and PostgreSQL event projection. The observed README blob SHA was
`5493513f0f15b7763f44a3f063a7cb2f5b747c77`. Those are useful engineering patterns
for Agent execution: original request identity, unknown-outcome reconciliation,
bounded outbox, replay and verified restoration.

BatchForge currently retrieves tenant memory through PostgreSQL full-text
search and returns versioned citations/full-document SHA with bounded excerpts.
A dense-vector integration needs a separately qualified retrieval backend,
embedding/index version, exact tenant filter, query deadline, stale/unavailable
semantics and frozen retrieval/answer evaluation. No live VectorMatch vector
search route, embedding model or index has been observed.

A future VectorMatch Agent tool can expose an authenticated, read-only domain
snapshot/projection after its actual protocol is inspected and qualified. It
must identify the source lineage/frontier and stale state, preserve original
request identity and keep tool output as untrusted data. This is a domain-data
adapter. The current task does not authorize changing the separate VectorMatch
project, deploying another server, sending trading commands or substituting
fabricated vector results.

The Agent platform map retains both work items with these explicit boundaries.
No dependency on a nonexistent vector API is added to current Agent execution.
