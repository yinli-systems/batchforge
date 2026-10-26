# Pinned Agent retrieval

Candidate implementation and local tests are complete. It is not enabled on
the primary Agent API: that mount remains off pending the separate Azure owner
migration. A source-frozen native PostgreSQL and real480B qualification follows
the preregistered protocol below; implementation alone is not that evidence.

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
CPU scope. The preliminary local Mac run observed91.807% paragraph recall@5;
the formal frozen-source Pod run is a separate measurement.

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
