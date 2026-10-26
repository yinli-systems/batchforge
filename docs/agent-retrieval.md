# Pinned Agent retrieval

The candidate passed native correctness tests and a real480B delivery/recovery
study, but its **answer-quality gate failed**: eight completed runs, EM0% and
F122.12%. It is not enabled on the primary Agent API; that mount remains off
pending the separate Azure owner migration. [Closed evidence and failure analysis](benchmarks/agent-rag-2026-10-26.md)
distinguish retrieval, answer quality, storage and client consistency.

`retrieval.search` searches an operator-published immutable corpus with BM25.
The corpus contains documents, versions and provenance; it accepts no question,
reference-answer or executable fields. This is lexical retrieval. It is not an
embedding/vector service or the VectorMatch replicated matching engine.

The versioned index pins the exact corpus bytes, Unicode NFKC/casefold word
tokenizer, title+chunk field,1536-byte chunks/128-byte overlap, binary query
term frequency, BM25 k1=1.2/b=0.75 and deterministic chunk-ID tie breaks.
The IDF uses log(1+(N-df+0.5)/(df+0.5)); the parameter/IDF conventions are
documented by [Lucene BM25Similarity](https://lucene.apache.org/core/10_5_2/core/org/apache/lucene/search/similarities/BM25Similarity.html).
This implementation does not claim Lucene index/ranking compatibility: its
tokenization, title field and chunking are explicitly different.

A snapshot ID hashes corpus bytes and index parameters. Every match includes
full-document SHA/version, chunk ID/text SHA, original UTF8 byte interval,
source reference and a snapshot-bound citation. Windows never cut codepoints;
their byte ranges can be checked against the source. Query/result commitments
permit exact replay against the pinned index. Replay uses the same ranking
implementation and is not independent semantic verification.

## Authority and bounds

An explicit `AGENT_RETRIEVAL_CATALOGUE` assigns each snapshot to a tenant and
namespace. It defaults off. The registry accepts only owned, non-writable,
resolved local releases and pinned basenames/hashes; no HTTP fetch, SQL, file
path or command is accepted from the planner. Foreign audiences see no metadata
and admission refuses before creating Agent state. New runs and each new model
step require the declared registry grant; revocation fails future planning
without erasing already accepted child usage.

The immutable `Policy.retrieval_snapshot` and `retrieval.search` capability must
be declared together. Existing default policy documents/request hashes stay
unchanged. A workflow's children inherit the parent snapshot only where that
child actually has the retrieval grant; they cannot select another snapshot.
Existing workflow requests omit the new empty field when computing their key.
Coordinators and ingress must receive the same frozen registry during rollout.

The model supplies only a query and top-k≤5. Its tenant, namespace, index and
permissions come from trusted execution context. Query UTF8 bytes≤1024, unique
terms≤64, documents≤20000, index chunks≤50000 and postings≤5million. Aggregate
catalogue input is≤64MiB. Returned matches fit a14000-byte canonical observation
budget; truncation is explicit. Source passages and tool results remain untrusted
data even when an attacker writes instructions inside them.

The console offers only audience-visible snapshots, retains their identity in
the run policy, and does not add retrieval to default grants without a selected
snapshot. Definite first admission refusal may be corrected using the same key;
an earlier lost acknowledgement remains unresolved and cannot change the body.
The actual client tests cover selection and refusal. No browser visual QA has
been performed. Citation membership/byte consistency does not prove that an
answer follows from a source; that is measured separately by an external grader.

## Frozen qualification

[Preregistration](../ops/reports/retrieval-rag-preregistration-2026-10-26.json)
pins the official public SQuAD1.1 dev SHA and all2067 passages. Queries and
references never enter the corpus. A full10570-query CPU retrieval study reports
correct-paragraph recall@1/3/5, answer-span coverage and latency with an explicit
CPU scope. The preliminary local Mac run observed91.807% paragraph recall@5.
Frozen Pod source independently reproduced the same rankings over all10570
queries in18.792seconds. This is CPU retrieval, not GPU answer quality or a
matched hardware speedup.

Eight Agent questions are selected by SHA256(seed:public case ID) order using
seed2026102603 before observing their RAG answers. All failures remain in the
denominator. Gold references are confined to the external scorer; prompts ask
the original question and require a real `retrieval.search` before final output.
The policy binds the corpus, its source citations and at most four model calls.
Maximum allocation is32 Agent children plus four independent decoder cases,
all under the existing workspace quotas, with no new tenant or limit increase.

The real study must reconcile the existing480B/4H100 Batch path with Azure
unique acceptance/usage, S3 byte readback, retrieval result replay, exact source
citations and task success. Predeclared cohort gates are EM75/F180,
supported-citation75 and joint task-success75 percent, plus all eight completed,
all retrieval observations replayed and coordinator post-Batch-ACK recovery
within30seconds. It restarts the inspection API and verifies durable event
heads/result hashes, then exercises actual console client history/child receipts.
Native Agent state is explicitly temporary PostgreSQL on container disk; it
does not establish an Azure Agent rollout or whole-Pod durability.

The primary API/worker/engine/gateway and guard are preserved. An unknown
campaign lock makes the existing finite guard wait; the trial does not claim
that guard provides automatic GPU recovery coverage during this study. Cost
uses actual whole-Pod wall allocation at the confirmed16.10USD/h, not an invoice
or marginal cost. Public dev has been seen in prior engine evaluations and is
not hidden testing. The full platform's vector adapters, cross-Agent citation
provenance, owner migration, sandboxing, multimodal and learning layers remain
separate unfinished work.

## Optional exact source excerpts and abstention

The additive `retrieved_span` contract requires a pinned, audience-granted
snapshot, `retrieval.search`, a nonzero tool budget and citations. It leaves old
policy hashes and old failed reports unchanged. The runtime replays committed
search arguments/results against the frozen index before admitting a terminal
answer. An answer must be an unchanged, contiguous UTF8 span in at least one
actually cited chunk, at most128bytes/16words with no surrounding whitespace.
Duplicate/unobserved citations fail. Invalid output consumes its accepted model
usage and a retry consumes another model call within the original budget; there
is no substring extraction, reference-assisted repair or free retry.

Successful answers include a `batchforge.quoted-answer.v1` receipt with original
document/version/hash, byte offsets and answer SHA. This proves byte inclusion,
not semantic relevance or correctness. Decoder JSON-schema constraints assist
reliability; byte/word limits, source membership and citation uniqueness are
runtime checks. The generic `text` contract keeps its existing behavior.

Only this explicit contract permits `{"type":"abstain","reason":
"insufficient_evidence"}`, after a verified search even if no matches were
found. Its lifecycle is terminal/completed but its disposition is **abstained**,
not an answered or correct task. It has a distinct `run.abstained` event and
retains all child usage/receipts. The declaration does not prove the corpus has
no answer. A required workflow child abstention fails the parent with
`workflow_child_abstained` and cancels unstarted descendants rather than
silently treating missing evidence as dependency data.

[The second preregistration](../ops/reports/retrieval-span-preregistration-2026-10-26.json)
freezes eight new public questions by seed2026102604, excluding all eight prior
RAG development IDs. No question is selected using a retrieval hit or a model
answer. It retains EM75/F180/citation75/joint75 gates; abstentions and failed
runs score zero. This is a new cohort, not a matched-question improvement or a
hidden test. Native tests and an independent real qualification must close
before this candidate can be described as qualified or deployed. Agent Azure
schema creation still requires an owner credential that is not saved.

[The real disjoint eight-question qualification](benchmarks/agent-rag-span-2026-10-26.md)
subsequently passed EM75/F185.26/citation100/joint75, with all original source
proofs and independent Azure/S3 closure. Two answers remain wrong. The old failed
cohort is retained. This candidate has not been deployed as an Azure Agent service.
