# Source-bound answer extraction candidate

`answer_adapter: "source-span-v1"` is explicit opt-in for `retrieved_span` with
`schema-v1`. Default policy and workflow documents omit the new field, keeping
old request hashes and planner behavior compatible. Production source is unchanged.

After real pinned retrieval and all required tools complete, the model sees the
question and the same original source excerpts with short zero-based ordinals.
It returns exactly `{"answer":"short literal phrase","source":0}` or
`{"answer":null,"source":null}`. The runtime freezes the complete ordinal-to-source
mapping in its durable model intent. It alone maps the selected ordinal to the
original full citation; it never rewrites or repairs the answer or uses references.

Before new upload/dispatch and final acceptance the mapping is replayed against
the granted snapshot and committed own/inherited evidence. A changed mapping or
revoked loaded authority blocks new I/O. Accepted lost-ACK work reconciles read-only
and retains its usage even if subsequent authorization fails. Unknown accepted
work stays explicitly unresolved; this does not implement manual approval.

Unknown/negative/bool ordinals, nonliteral text, duplicate/extra fields, malformed
JSON and answers over128 UTF8 bytes/16 words are refused. An invalid accepted
output remains charged. A correct literal quote can still be a wrong answer.
Null abstention is typed and scores zero in answer-required evaluations.

The frozen follow-up compares the original final Agent JSON/full citations with
this compact extraction envelope on identical top5 BM25 excerpts/original
questions. Its ABBA blocks are correlated repetitions. Prompt packing and ordinal
encoding change together, so the experiment cannot isolate their separate causal
effects. It measures the post-retrieval engine subsystem, not Agent tool selection,
Batch acceptance/durability or whole-platform SLOs. Canary100 development cases
and all18 prior RAG cases are excluded from the separate500-case study. Public
SQuAD dev is not a hidden benchmark and prior full engine work has seen it.

The exclusive maintenance controller preserves source-aware per-service profiles,
closes both APIs' create/retry gates, drains accepted work, stops only the owned
worker/guard and arms independent bounded restoration before direct inference.
A new source-matched controller-loss canary must close before the full study.
No engine/model/cloud resource is restarted, no quota or retention policy changes.

Pre-inference Mac/Pod comparison found nine of100 full-citation bodies differed
only in floating BM25 scores and their result digest (maximum absolute delta
7.11e-15). All ordinal requests and score-stripped source/citation bodies matched.
The exact-plan assertion refused before controller launch. The [actual Pod plan](../ops/reports/answer-adapter-study-pod-plan-2026-10-26.json)
was then frozen read-only, before any answer was observed; the initial Mac draft
and both refused launcher attempts remain preserved. This does not qualify
cross-architecture replay of numeric BM25 metadata. Both production service
sources and the published older results remain unchanged.

The original500-case v1 comparison stopped at a120s HTTP timeout (209 known,
1 unknown outcome), before the ordinal phases. It remains failed. Follow-up v2
uses250 disjoint questions excluding all618 prior reserved RAG/canary/study IDs,
and an explicit300s lifetime for both arms. Production serving timeouts are
unchanged. The original engine QA evaluator keeps its120s default. A passed v2
would not establish the failed v1's120s service target. The new source, requests,
cohort and recovery qualification must be frozen independently before inference.
