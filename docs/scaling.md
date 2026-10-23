# Capacity and the ten-million-task target

Ten million completed tasks per day means **115.74 accepted completions/second
for a full day**. With a 70% average utilization target, provisioned sustainable
capacity would need to exceed **165.34 tasks/second**. These are arithmetic
targets, not measured BatchForge capacity. A batch upload acknowledgement, an
engine-only token benchmark and an accepted durable AI result are different
measurements.

## What this deployment measures

The current model is Qwen2.5-7B-Instruct, revision
`a09a35458c702b33eeacc393d103063234e8bc28`, BF16, independent TP1 engines, eager
execution, 4,096-token context, prefix caching explicitly disabled. Four H100s
are shared with existing workloads; BatchForge's memory allocation is 0.24 per
H100. The other Pod exposes sixteen 24 GB Blackwell MIG instances. A MIG instance
is a fraction of a physical GPU, not an additional full RTX PRO 6000. Device
reports record actual UUID mappings and generation evidence.

The public-document workload uses deterministic, distinct SQuAD development
questions and real passages packed to a target of 700 words. An answer profile
caps output at 96 tokens; an analysis profile caps it at 512 tokens. Source hashes,
model identity, actual input/output token distributions, finish reasons,
quality checks, observation boundaries and hardware settings accompany the
reports. Unsuccessful JSON contracts and output truncation remain visible.
This public benchmark may occur in training data and is not customer demand or
an independent evaluation of explanation quality.

Use accepted completions per wall-clock second from submission through client
observation of all terminal batches for the end-to-end number. Download and
integrity checking are timed separately. Attribute results through durable task
and attempt rows; compare task usage, batch usage and downloaded output usage.
An attempt duration includes scheduling and database commit and cannot be
reported as isolated GPU execution latency.

## The first bottleneck

With a remote durable database, an application transaction that holds a tenant
queue lock while issuing many SQL statements pays network round-trip time while
blocking other work for that tenant. Low GPU occupancy can therefore coexist
with spare engine memory and a deep pending queue. Adding GPUs alone cannot
remove that serialization.

The additive v2 migration puts completion and heartbeat validation, state changes
and accepted usage accounting in SECURITY INVOKER PostgreSQL functions. One
client/server call keeps worker, queue, batch and task locking on the database
side. The functions preserve server-clock lease checks after lock acquisition,
idempotent replay, cancellation fences and atomic output metadata. No physical
inference exactly-once guarantee is implied. Claim, recovery, cancellation and
admission remain additional paths to measure, including one busy tenant and many
tenants.

A synthetic control-plane test uses its own empty database and explicit synthetic
results. It can isolate state-machine cost but cannot establish HTTP throughput,
GPU capacity or daily AI completions. Never mix this load into the real inference
comparison on the same managed compute.

## Sizing from observed tokens

For a measured task rate `R`, mean input tokens `I` and mean generated tokens `O`,
the fleet must sustain at least `R * I` prompt tokens/second and `R * O` generated
tokens/second for that workload. Prefill and decoding share the same GPU; the two
requirements cannot be independently divided by unrelated peak figures.
For illustration only, 1,800 input and 400 generated tokens at 115.74 tasks/second
would require about 208,333 prompt and 46,296 generated tokens/second. Replace
those illustrative lengths with actual report distributions before sizing.

Do not extrapolate a memory-constrained share of an already busy H100 into the
capacity of an isolated full H100. Do not multiply one warmed engine's peak by
the number of GPUs without measuring scheduler fill, database cost, prompt
lengths, output lengths, retries and quality. A short run supports a bounded
observation; sustained capacity needs a long run at the required arrival rate,
with queue-age limits and injected failures.

The observed Runpod allocation prices were $16.10/hour for the primary Pod and
$11.18/hour for the second. They cover the existing complete allocations,
including unrelated primary GPU workloads and all sixteen configured MIG slices.
They are not the application's incremental GPU cost or a quote for new capacity.
No additional GPU Pod was purchased. Public ingress remains private by owner
choice; the proposed $14/month Render service was not created. The managed Neon
project uses its free plan and has no production SLA.

## Work required before a daily-scale claim

1. Run the same fixed input corpus before and after each hot-path change. Keep
   device count, concurrency, model, output contract and cache settings fixed;
   report device expansion as a separate comparison.
2. Measure steady arrival rates, completed results, queue age, failure/retry rate,
   engine occupancy, KV pressure, claim/heartbeat/complete latency, lock waits,
   database WAL and storage growth. Retain enough headroom for a Pod outage.
3. Remove any measured remaining claim or tenant-counter serialization. Assess
   bounded multi-tenant fairness and recovery load before sharding. Partitioned
   scheduling must preserve tenant admission limits and fenced accounting.
4. Move large input/output objects to durable object storage with checksums,
   streaming export, explicit retention and cleanup. Ten million task rows/day
   plus attempts, duplicated JSON input/output and WAL exceed a 1 GB free database
   quickly. For example, just 10 KB/task is 100 GB/day before indexes and replicas.
5. Establish database quota alerts, production recovery guarantees, off-host
   backup retention, regular restoration, Pod replacement and private ingress
   failover. Process watchdogs and four API processes do not eliminate a shared
   database failure domain.
6. Treat useful-task quality as a service constraint. Chargeable work needs
   validated schemas, customer-specific acceptance rules and explicit handling
   of truncation/retry costs, in addition to successful transport.

See [validation](validation.md), [operations](operations.md) and the report JSON
files for observed results and their limits. No ten-million-task daily run has
been demonstrated.
