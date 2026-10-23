# Validation

Measurements below were observed on 2026-10-23 UTC. They are short-run results,
not a daily capacity forecast or a production availability claim.

## Correctness

The complete local suite currently passes **34 tests**, using PostgreSQL 16.2
for database tests. Coverage includes concurrent claim capacity, tenant isolation,
key rotation/revocation, cancellation races, deadline fencing, expired leases,
idempotency conflicts and replay, concurrent admission limits, result streaming,
transactional usage, endpoint failure handling, and event-driven worker refill.
Three additional persisted console regressions pass in Node/V8: cancellation
versus completion, confirmed cancellation, and retry after a committed-but-lost
acknowledgement including recovery after tab reload. They run with
`node --test frontend/tests/*.test.js` and are included in the CI configuration.

The HTTP chaos test launches its own API, test engine and worker subprocesses,
SIGKILLs its worker after execution starts, restarts with a new identity, and
drops a committed claim acknowledgement through a test proxy. It recovered four
expired attempts and produced ten unique outputs for ten tasks. Late completion
was rejected with 409. Its responses are explicitly synthetic and are never used
as GPU performance evidence. See [observed report](../ops/reports/http-chaos-2026-10-23.json).

The separate [protocol probe](../ops/reports/failure-probe-2026-10-23.json) verifies
cancellation, expiry, stale-result rejection and idempotent accepted completion.

## Real GPU inference

Hardware: four shared NVIDIA H100 80GB HBM3 devices, four independent TP1 engines,
FP16, eager execution, eight sequences per GPU, 2,048-token engine context cap,
and a conservative 0.06 GPU-memory allocation target. Other host workloads were
left running. Model and runtime versions are recorded in
[hardware-primary](../ops/reports/hardware-primary-2026-10-23.json).

- Model: `Qwen/Qwen2.5-0.5B-Instruct`.
- Model revision: `7ae557604adf67be50417f59c2c2f167def9a775`.
- vLLM 0.10.2, PyTorch 2.8.0 / CUDA 12.8.
- Transformers 4.55.2, tokenizers 0.21.4, huggingface-hub 0.34.4.
- Short classification prompt, temperature 0, output limit 64 tokens.
- Every measured output contained 14 generated tokens; input lengths varied with
  request IDs. This is not a long-output, long-context or model-quality test.

| Run | Tasks / batches | Client location | Observed elapsed | Completed/s | Failed |
| --- | --- | --- | --- | --- | --- |
| Smoke | 32 / 4 | Laptop through SSH | 3.353 s | 9.543 | 0 |
| Initial polling | 1,024 / 32 | Native Pod client | 24.844 s | 41.217 | 0 |
| Event-driven refill | 1,024 / 32 | Native Pod client | 13.126 s | 78.014 | 0 |
| Two-Pod execution | 512 / 16 | Native primary Pod client | 9.930 s | 51.563 | 0 |

Every run downloaded and checked results: unique custom IDs, known inputs,
output counts matching durable completed counts, no unfinished batches and no
test-fixture marker. The large runs exercised all four GPU workers.

The two-Pod run added one Blackwell RTX PRO 6000 `MIG 1g.24gb` worker connected
through the private SSH application relay. Database attempt records attribute
496 accepted completions to the H100 Pod and 16 to the MIG Pod. This proves
cross-Pod task execution; adding a remote worker through a laptop relay is not
an established throughput improvement. See the [measurement](../ops/reports/gpu-multipod-2026-10-23.json)
and [database attribution](../ops/reports/multipod-distribution-2026-10-23.json).
Only one of the second Pod's six CUDA-visible MIG devices was used.

Reports: [smoke](../ops/reports/gpu-smoke-2026-10-23.json),
[initial polling](../ops/reports/gpu-load-2026-10-23.json),
[event-driven refill](../ops/reports/gpu-refill-2026-10-23.json).
Reports include exact workload hashes, token counts, upload/create latencies,
observed batch turnaround and measurement boundaries.

The runs used the same workload shape and settings, with different random IDs
and slightly different input token totals. Shared hardware load was not isolated.
The observed before/after result supports keeping the refill change but does not
establish a universal speedup. Submission acceptance and completed inference are
reported separately. The refill run accepted 1,024 tasks in about 0.50 seconds;
that is a short batch-ingestion observation, not sustained task throughput.

## Browser acceptance

The deployed console was exercised through a private SSH tunnel: authentication,
live metrics, JSONL sample submission, batch details, token usage, metadata and
authenticated JSONL download. The sample batch completed three actual GPU tasks.
Responsive checks at widths 1,280 and 390 pixels showed no document-level
horizontal overflow. The console displayed five connected/engine-ready workers
and 2,603 accepted completions after the measured runs.
One model result used Markdown fences despite the plain JSON instruction;
transport completion must not be confused with task-quality acceptance.

The second Pod's first generation probe timed out during initial startup; later
real generation succeeded without an engine change. Its cause was not established.
An idle relay disconnect/reconnect test verified remote listener cleanup and
automatic worker re-registration. No active inference was interrupted in this
test, so it does not establish cross-Pod lease-expiry recovery. See
[second-Pod evidence](../ops/reports/pod2-deployment-2026-10-24.json).

## Backup restoration

A private snapshot of the generated benchmark/catalog database was restored into
a new temporary PostgreSQL cluster: 70 batches, 2,091 tasks, 169,144 metered tokens,
zero task-count discrepancies, matching task/batch usage totals. The temporary
cluster was stopped and removed; the live database was not a restore target.
The [restore report](../ops/reports/restore-check-2026-10-23.json) includes the
snapshot SHA256. The snapshot is ignored by Git and stored with mode 600.

UI rendering checks cover liveness versus engine readiness, missing legacy
readiness, degraded nodes, escaped user content and batch states. Browser QA
caught a nested-template formatter regression; it was fixed and render output
was compared against the original before deployment.

## Failures discovered and fixed

Concurrent idempotent submissions initially deadlocked: foreign-key KEY SHARE
locks on a tenant conflicted with an admission FOR UPDATE upgrade. Admission
now uses FOR NO KEY UPDATE, which retains serialization and permits FK readers.
Regressions reproduce four simultaneous eight-task submissions over real TCP
HTTP and concurrent queue-limit competition.

The old vLLM release installed a newer, incompatible Transformers tokenizer API
without an upper bound. The tested engine dependencies are pinned explicitly.

## Verification limits

GitHub Actions is configured, but the hosted run did not start because GitHub
reported account payment/spending-limit restrictions. This is separate from the
passing local tests. [Blocked run](https://github.com/yinli-systems/batchforge/actions/runs/40013353844).

The Compose configuration parses, but container builds were not executed in the
initial environment. The native deployment is the exercised path. The database
is a single local PostgreSQL instance; no failover or multi-region durability has
been established. Payments, invoices, object-storage retention, customer quality
gates and ten-million-task sustained load remain outside the first release's
verified capabilities.
