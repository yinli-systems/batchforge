# Capacity and the ten-million-task target

The multi-Pod 7B and 120B configurations below are historical. The current run
has one four-H100 Pod; see [single-Pod validation](single-pod-validation-2026-10-24.md)
for the latest equal-work policy comparison and larger-token capacity scenario.

Ten million completed tasks per day means **115.74 accepted completions/second
for a full day**. With a 70% average utilization target, provisioned sustainable
capacity would need to exceed **165.34 tasks/second**. These are arithmetic
targets, not measured BatchForge capacity. A batch upload acknowledgement, an
engine-only token benchmark and an accepted durable AI result are different
measurements.

## What this deployment measures

The earlier throughput baseline uses Qwen2.5-7B-Instruct, revision
`a09a35458c702b33eeacc393d103063234e8bc28`, BF16, independent TP1 engines, eager
execution, 4,096-token context, prefix caching explicitly disabled. Four H100s
are shared with existing workloads; BatchForge's memory allocation is 0.24 per
H100. The other Pod exposes sixteen 24 GB Blackwell MIG instances across six
physical parents (slice counts 1/1/4/4/2/4). Four H100s plus sixteen MIG instances
are twenty inference devices, not twenty full GPUs. Each MIG engine has a 0.82
memory fraction. The measured baselines use eight sequences per engine, worker
inflight eight and a 32,768-token reservation. Sixteen-sequence tuning keeps the
same memory fractions and requires a free-memory gate, proven KV capacity of at
least 65,536 tokens and sixteen real simultaneous generations before activation.
Device UUIDs, model revision, software and actual generation are recorded.

The deployment has since added real GPT-OSS-120B execution on both Pods and a
separate 18K-context 7B pool. Primary H100s now form one 120B TP4 engine; the
secondary retains fourteen short-context 7B engines, one long-context 7B engine
and one 120B CPU-expert/MIG engine. The historical twenty-engine rates below
are not current 120B capacity. See [100B validation](100b-validation.md) for the
new configuration and mixed-length results.

Public-document inputs use distinct SQuAD development questions and real passages
packed to a target of 700 words. This corpus may occur in training data; it is
not customer demand or an independent explanation-quality evaluation. The
end-to-end timer covers submission through client observation of all terminal
batches; download/integrity checks are separate. Attempt duration includes
scheduling and database commit, so it is not isolated GPU execution latency.

## Measured real inference

Each following run completed all 2,048 tasks with actual 7B inference, distinct
public questions/passages, temperature zero, output cap 512 and prefix caching
off. Input token total was 2,369,454 (mean 1,156.96; p50 1,153; p95 1,303.65;
maximum 1,510). The input corpus SHA256 is
`8f81023c1ee309f945c1f5f2b0c15cbe2f17e7e6306a6d8b761f77396ca3f6e2`.

| Deployed path | Inference devices | Elapsed seconds | Accepted tasks/s |
| --- | ---: | ---: | ---: |
| Python claim/completion | 4 H100 + 6 MIG | 746.013 | 2.745 |
| SQL v2 completion/heartbeat | 4 H100 + 6 MIG | 233.624 | 8.766 |
| SQL v2, expanded fleet | 4 H100 + 16 MIG | 262.548 | 7.800 |
| SQL v3 claim + v2 completion | 4 H100 + 16 MIG | 86.950 | 23.554 |
| Same SQL paths, sixteen sequences/engine | 4 H100 + 16 MIG | 59.803 | 34.246 |

The v3 observation spans 2026-10-23 21:39:18.247531–21:40:45.197500 UTC.
At the same twenty devices it reached 3.02 times the v2 rate. The previous
ten-to-twenty-device expansion reduced throughput by 11.0%; adding GPU devices
did not remove the measured control-plane bottleneck. This is a short shared
hardware observation, not a promise of linear scaling. Multi-origin worker
routing also changed between the first two runs, although no origin failovers
were observed. The separate sixteen-sequence run reached 34.245613/s over
59.803280 seconds, a further 45.4% observation. All 2,048 tasks and accepted
attempts matched durable output/usage, with no retries and contributions from
all twenty workers. See its [run](../ops/reports/squad7b-analysis-2048-sql20-v3-seq16-2026-10-24.json)
and [attribution](../ops/reports/squad7b-analysis-2048-sql20-v3-seq16-attribution-2026-10-24.json).
Memory fractions, model, corpus and token limits stayed fixed. Each engine first
passed a real sixteen-running-request probe; configured fleet capacity is 320
slots. The secondary mid-run snapshot observed 255/256 inflight tasks, all
sixteen workers already contributing accepted results.
The tuning restart also reset worker origin selection; the previous run had
one worker on direct API-a. This is an ordered configuration comparison.

See the [initial real load](../ops/reports/squad7b-analysis-2048-baseline-2026-10-24.json),
[SQL10 load](../ops/reports/squad7b-analysis-2048-sql10-2026-10-24.json),
[SQL20 v2 load](../ops/reports/squad7b-analysis-2048-sql20-v2-2026-10-24.json) and
[SQL20 v3 load](../ops/reports/squad7b-analysis-2048-sql20-v3-2026-10-24.json).
Their generated token means were 191.50/191.58/191.05; medians 183/181/181;
p95 288/289/286; maximum 512. Each had one length-limited response; the other
2,047 stopped naturally. A cap of 512 does not mean each result generated 512.
The v3 run generated 391,296 tokens (mean 191.06; median 181; p95 284;
maximum 512), with 2,046 natural stops and two length limits.

## The first bottleneck

Holding a tenant queue lock across many remote SQL round trips serializes work
even with spare GPU memory. The additive v2 migration moves completion/heartbeat
into SECURITY INVOKER PostgreSQL functions; v3 moves bounded claim scheduling
and leases into one call. Both preserve worker/queue/batch/task lock order,
server-clock lease checks, idempotent replay, cancellation fences and atomic
accepted usage. This does not guarantee physical inference exactly once.
Recovery, cancellation, admission and multi-tenant fairness still need load tests.

A synthetic control-plane test uses its own empty database and explicit synthetic
results. It can isolate state-machine cost but cannot establish HTTP throughput,
GPU capacity or daily AI completions. Never mix this load into the real inference
comparison on the same managed compute. A separate sequential v2 comparison of
512 tasks per mode, one tenant, sixteen workers and pool size sixteen measured
9.802 → 38.339 synthetic accepted results/s (3.91 times). Both modes had zero
observed retries/errors and verified exact usage. These direct Store calls used
30-second leases, a 25-second report retry budget and no heartbeat; production
uses 90-second leases with heartbeats. Ingestion and validation were outside the
timer; temporary database and role removal were verified. This test did not
measure HTTP or GPUs. See its [state-only report](../ops/reports/controlplane-v2-comparison-2026-10-24.json).

## Sizing from observed tokens

For a measured task rate `R`, mean input tokens `I` and mean generated tokens `O`,
the fleet must sustain at least `R * I` prompt tokens/second and `R * O` generated
tokens/second for that workload. Prefill and decoding share the same GPU; the two
requirements cannot be independently divided by unrelated peak figures.
Using the SQL10 measured means, 115.74 tasks/second requires approximately
133,907 prompt tokens/second and 22,174 generated tokens/second. That arithmetic
does not establish how many additional GPUs to buy. The twenty-device v3 result
is still 4.91 times below the required average task rate. The sixteen-sequence
rate is 3.38 times below that average, or 4.83 times below the provisioned
70%-utilization target, before applying customer quality requirements. A simple
full-day multiplication of 34.245613/s is 2.96 million tasks; no such sustained
day has been measured, so this is arithmetic, not capacity evidence.

Shared H100 capacity does not predict an isolated full H100. Sustained capacity
needs a long run at the required arrival rate, with queue-age limits, token
distributions, retries, quality checks and injected failures.

The observed Runpod allocation prices were $16.10/hour for the primary Pod and
$11.18/hour for the second. They cover the existing complete allocations,
including unrelated primary GPU workloads and all sixteen configured MIG slices.
Together they cost $654.72 per day for the whole existing allocations. These are
not the application's incremental GPU cost or a quote for new capacity.
No additional GPU Pod was purchased. Public ingress remains private by owner
choice; the proposed $14/month Render service was not created. The managed Neon
project uses its free plan and has no production SLA.

## Useful output, storage and billing

Successful state transitions alone do not establish useful answers. Across the
four runs, JSON field-contract success was 98.14%/98.00%/97.90%/98.00%; custom
answer exact match was 65.77%/65.58%/65.43%/65.58%, token F1 approximately
80.10%/80.01%/79.94%/79.93%. This is not the official SQuAD evaluator or an
independent factuality test. Valid analyses averaged about 93 words despite
requesting 180–220; only 1/0/1/2 results met that request. Analysis inputs have no
`response_format`, so the JSON guard does not change the fixed analysis A/B
workload. See the [v3 quality report](../ops/reports/squad7b-analysis-2048-sql20-v3-quality-2026-10-24.json).

The tuned analysis run generated a mean 191.12 tokens with two length limits;
36 outputs failed the field contract (98.242% success over all expected tasks).
Custom answer exact match was 65.381%, token F1 80.007%, and only 1/2,048
analyses met the word range. Throughput improvement did not resolve that
application-level quality gap.

A direct eight-input translation pilot produced strict JSON in 0/8 outputs.
Adding only `response_format={"type":"json_object"}` yielded 7/8, not 8/8: one
response contained an unterminated string despite a natural stop. Reports retain
that failure ([unconstrained](../ops/reports/primary-translation-pilot-8-2026-10-24.json),
[structured](../ops/reports/primary-structured-translation-pilot-8-2026-10-24.json)).
An observed EOS/grammar configuration difference is a diagnostic hypothesis,
not a confirmed root cause. These engine-only pilots do not measure platform
capacity or translation fidelity.

During this 7B phase, the tested worker JSON postcondition was enabled on all
twenty workers. The current 120B deployment retains the same guard. Exact
`json_object` requests require a strict
top-level object before completion. The [primary tuning report](../ops/reports/primary-retune-16seq-2026-10-24.json)
records all four sixteen-sequence KV/probe/readiness gates at unchanged memory.
Malformed output follows durable retry/fence handling; accepted usage excludes
that rejected attempt. Separate process-lifetime counters observe rejected
prompt/generated tokens when engine usage exists. They are not durable billing
records and cannot account for unobserved responses. JSON syntax does not
guarantee required fields, factual correctness or a good translation.

The 2,048-task source JSONL is 11,669,640 bytes: at ten million identical-size
tasks that is 56.98 GB/day of raw input alone, before output, attempts, indexes,
WAL and duplicated file/task payloads. This is not physical database growth or
a storage quote. Real retention, compression and object-storage measurements
must replace extrapolation. A PostgreSQL 17 logical backup was restored into a
separate empty database and checked against per-batch state/usage; its SHA256
verified private off-host copy exists. That proves one restoration, not an
automatic retention schedule or a recovery SLA; see [backup](backup.md).

## Work required before a daily-scale claim

For the newer **120B equal-cell mixed workload**, use the separate
[measured capacity and cost arithmetic](100b-validation.md#cost-and-the-daily-target):
primary 0.42766 and secondary 0.03001 engine inferences/s. Their separately
measured sum at an assumed 70% duty is about 27,680/day, roughly 361× below
ten million/day. The larger-model workload has different lengths, precision and
active parameter count from the 7B analysis run above. Its frozen primary JSONL
contains 530,026 bytes for 20 requests: input alone extrapolates to
265.013 GB/day at ten million requests, before outputs, indexes, WAL or retries.

The next measured engineering steps are:

- Reduce idle queue traffic with model-scoped wake-up hints and bounded fallback
  polling, while retaining database claims as the source of truth. During the
  121.85-second engine-loss window, the second Pod recorded 1,062 claim HTTP
  calls and eight completion calls across all its model pools. These are call
  counters, not unique tasks or evidence of slow SQL; the mostly idle pools
  still consume control-plane work and keep the free database active.
  [Observed secondary window](../ops/reports/pod2-gptoss120b-engine-down-secondary-observation-2026-10-24.json).
- Keep PostgreSQL responsible for task/attempt fences and accepted usage, but
  move large immutable input/output bodies to checksummed object storage with
  explicit lifecycle and streaming exports. Count duplicated payloads, WAL and
  retained failed attempts when sizing storage.
- Benchmark a fully available device in an isolated runtime before translating
  shared TP4 measurements into hardware counts. Measure prefill, decode,
  communication and memory separately; changing quantization/runtime requires
  fresh answer-quality validation.
- Evaluate token-cost-aware tenant scheduling under steady arrivals. The current
  SQL scheduler is dispatch-turn fairness; the three measured client dispatch
  orders are not a production scheduler implementation. Long requests, cheap
  requests and tenant deadlines need explicit starvation and admission tests.
- Define model-revision and precision compatibility for failover. The current
  120B lanes have different non-expert precision; recovery re-executes a fenced
  task on the second worker. It does not migrate a live KV cache or guarantee
  the same generated token sequence.

The general validation requirements remain:

1. Repeat a fixed corpus with device count, model, concurrency and cache settings
   held constant; report device expansion separately.
2. Measure steady arrivals, queue age, retries, KV pressure, operation latency,
   lock waits, WAL and storage growth; reserve capacity for a Pod outage.
3. Profile failure reporting and lease reaping under retry storms. They still use
   client-driven multi-statement transactions; the original translation run
   observed high failure latency, failed scrapes and 450 origin failovers. This
   supports further profiling, not an unmeasured causal claim. Preserve fences,
   bounded work and atomic counters when optimizing these paths.
4. Measure multi-tenant fairness and recovery before sharding, preserving tenant
   admission limits and fenced accounting.
5. Add checksummed object storage, streaming export, funded retention and cleanup.
6. Establish quota alerts, scheduled off-host backup/restoration, Pod replacement
   and ingress recovery. Watchdogs do not remove the shared database dependency.
7. Define customer acceptance rules for schema, factuality, truncation and retry
   cost before treating accepted completions as useful billable work.

See [validation](validation.md), [operations](operations.md) and the report JSON
files for observed results and their limits. No ten-million-task daily run has
been demonstrated.
