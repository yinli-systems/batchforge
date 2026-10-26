# Source-bound RAG answers: qualified finite candidate, not production rollout

The new disjoint eight-question public RAG cohort passed its frozen gates:
**EM75%, F185.256%, supported-reference citation100%, joint task success75%**.
All eight answers are verified verbatim source spans, but two answers are still
wrong by the external reference scorer. Byte inclusion does not establish
semantic correctness. The old eight-question EM0/F122.115 failure stays intact.
Neither study is a hidden test or a large-sample quality/capacity claim.

## Source and contract

Candidate **dc139375570339b4b109ed801887ca74756133be** is immutable at
`/root/batchforge-runtime/evidence-api-agent-runtime-20261026-v21/source`.
Its [native CI](https://github.com/yinli-systems/batchforge/actions/runs/40267208834)
passed1928 Python/zero skips,48 Node tests and compileall on PostgreSQL17.11,
Node24.22.1 and UID65534; the ephemeral runner was removed.
[Source receipt](platform480-2026-10-25/evidence-api-agent-runtime-20261026-v21-span-source/source-receipt.json)
and [CI receipt](../../ops/reports/github-native-ci-dc13937-2026-10-26.json)
pin this check. It did not alter the primary API/worker/engine/gateway/guard.

The opt-in `retrieved_span` contract requires real audience-granted retrieval,
exact observed citations, an unchanged contiguous answer of at most128UTF8bytes
and16words, and a server-produced document/version/hash/byte-range receipt.
Rejected output consumes its accepted usage and any retry is a new metered
model call within the same budget. No gold-assisted repair/extraction is used.
Typed insufficient-evidence abstention is separate from an answered result and
requires actual search. Native PostgreSQL tests cover that path; **the real
cohort had zero abstentions**, so no real-model abstention calibration is claimed.

[Preregistration](../../ops/reports/retrieval-span-preregistration-2026-10-26.json)
freezes seed2026102604 and fixture SHA
`38386fa3b8f75819a119f0025cfc4e17afec7aa25a37f2f6fda90d707df2eb52`.
Selection excludes all eight previous RAG development IDs, without ranking-hit
selection or observed answers. Passages contain no question/reference fields;
gold remains in the external scorer. The entire public dev corpus was already
used in earlier engine evaluations. The new questions and contract prevent a
controlled, matched-question improvement claim against the previous eight.

## Real execution and independent closeout

The existing480B-total/~35B-active MoE on fourH100s executed16 Agent children
plus four independent decoder cases: **20 unique tasks,20 attempts**,26753
prompt and1664 output tokens. No cloud resource, quota, tenant or retention
policy was added/changed. Retained task headroom decreased from70 to50 out of
the fixed50000 limit.

After a coordinator exited after the Batch acceptance acknowledgement but before
its Agent state commit, another DB lease was acquired in **14.038092seconds**,
under the predeclared30-second gate. The accepted child identity was preserved.
This is Agent coordinator process loss, not a physical GPU failure.

Inspection API restart retained the state/event commitments. The actual Node
client independently checked eight snapshots,385 events,16 current Azure child
receipts, quoted source bytes and exact served asset hashes. No browser visual
QA was performed. Eight-run completion P50/P95/P99 were44.669/47.241/47.791s;
these are descriptive small-cohort numbers, not a population P99 or engine
service latency.

The [independent read-only audit](platform480-2026-10-25/evidence-api-agent-rag-span-audit-20261026-v1-complete/validation.json)
verified17 ready/purged archives,20 retained Azure identities and matching usage,
all34 distinct input/output artifact byte/SHA readbacks, unchanged primary
source/config/process identities, healthy services/guard and no leftover trial
APIs/coordinators/native PostgreSQL. Response bodies are normally purged from
Azure after verified S3 publication; their task identities and usage remain.

[Parent](platform480-2026-10-25/evidence-api-agent-schema-retrieval-span-20261026-v1-complete/validation.json),
[Agent cohort](platform480-2026-10-25/soak480-agent-runtime-schema-retrieval-span-20261026-v1-complete/report.json),
[decoder](platform480-2026-10-25/soak480-agent-runtime-grammar-retrieval-span-20261026-v1-complete/report.json)
and [structured summary](../../ops/reports/agent-rag-span-v1-summary-2026-10-26.json)
retain the raw public evidence. All allowlisted transfers were SHA-checked.

## Cost, utilization and persistence limits

The whole293.829-second wrapper allocates **$1.314069** at the operator-confirmed
$16.10/whole-Pod-hour. Its nested280.838-second Agent interval allocates$1.255972;
these windows overlap and must not be added. Dividing the whole wrapper by six
reference-exact/source-supported results yields$0.219012 each for this tiny
control/archive-heavy experiment, not an invoice, marginal or business result
price. Storage readback occupied about205seconds after Agent completion and
includes the required120-second age plus eligible archival/verification work;
it does not mean a205-second S3 request.

Each card has52 samples, zero sampling errors and maximum internal gap5.269s.
Whole sampled means are3.673/2.154/2.942/3.596% busy, including control/archival
idle time. **This cohort did not saturate the GPUs** and cannot establish serving
throughput or daily task capacity. The earlier high-load studies remain separate.

The stopped native control database was copied privately off-Pod to the Mac:
1147 files/43,910,550 bytes,6,060,994-byte compressed archive SHA
`dec00c525ef073b06730592444f9643a6026591eda23472f603c146083c3a2f3`.
Every file was reread from the local archive and verified; directories0700,
files0600 and file/directory fsync were checked. Only
[metadata](../../ops/reports/agent-rag-span-private-control-export-2026-10-26.json)
was committed. Raw model outputs, database archive, keys/environments and weights
remain outside Git. Actual restore is **not yet qualified**.

Agent production persistence/DDL is still pending Azure owner authentication;
`bf_runtime` lacks CREATE and the user confirmed the owner password is not saved.
The existing Batch production service remains on Azure/S3. Next work is
cross-Agent citation provenance, bounded backup restore qualification and
measured control/archive overhead, followed by safe approval/sandbox/runtime
adapters. The complete platform goal remains unfinished.
