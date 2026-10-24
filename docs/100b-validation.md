# Real 100B deployment and workload evidence

Both existing Pods now execute GPT-OSS-120B, whose checkpoint contains
116,829,156,672 total parameters (about 5.1B active per token). The primary runs
native MXFP4 experts with BF16 remaining tensors through vLLM 0.10.2 / Marlin on
four shared H100s. The secondary runs the runtime-maintained GGUF conversion
through pinned llama.cpp on one Blackwell MIG with CPU expert execution. Its
non-expert tensors use Q8_0, so the nodes are not numerically identical models.
No additional GPU allocation was purchased.

## Inference and platform acceptance

| Evidence | Actual result | Boundary |
| --- | --- | --- |
| Primary natural request | Correct final `42`, 82 input / 47 generated tokens, 1.258 s | One engine request |
| Primary full-context request | 16,384 actual input / 2,048 generated, 38.445 s, final content present | Output reached its cap; not a quality score |
| Primary durable batch | 3/3 accepted, final answers `42`, `437`, `Paris`; 242 input / 169 generated | Real authenticated upload, queue, worker, output and database commit |
| Post-recovery natural mixed batch | 20/20 durably completed in 43.884 s; 109,050 input / 9,292 generated | 10/20 have final text; 10 are reasoning-only; 15 hit the cap |
| Explicit low-effort platform smoke | Correct final `42`, 80 input / 16 generated, natural stop | One accepted CPU/MIG task; choice persisted and forwarded |
| Secondary natural request, cap 32 | 80 input / 32 generated, no final answer | Reasoning consumed the cap; retained as a failed answer probe |
| Secondary natural request, cap 128 | Correct final `4`, 80 input / 39 generated | 75 input tokens reused; 1.076 s is a warm-cache observation |

Sources: [primary direct requests](../ops/reports/primary-gptoss120-natural-long-2026-10-24.json),
[primary platform batch](../ops/reports/primary-gptoss120-durable3-2026-10-24.json),
[secondary launch and both answer probes](../ops/reports/pod2-gptoss120b-launch-2026-10-24.json).
The report format separates durable completion, final text presence, reasoning
only responses and truncation. A valid protocol response does not establish a
useful answer or customer acceptance.

The post-recovery mixed batch used the frozen 20 source bodies and original
32–2,048 output caps, with natural EOS and no forced minimum. The primary
accepted 17 tasks and the secondary three. Downloaded output usage matches
the task, attempt and batch ledgers, with one accepted attempt per task and no
failed, cancelled or unfinished tasks. Actual rendered input differs between
the two runtime templates; primary-only manifest counts are not imposed on
the fallback. [Natural batch](../ops/reports/primary-gptoss120-natural-durable20-2026-10-24.json),
[per-task attribution](../ops/reports/primary-gptoss120-natural-durable20-attribution-2026-10-24.json).

The output audit found final text in 0/5 requests capped at 32 tokens, 1/5 at
128, 4/5 at 512 and 5/5 at 2,048. Two of the last group still reached their
cap. Final text presence does not establish correctness or completeness.
The actual executing benchmark was an older pinned version without these
content counters; its source and original report are preserved. A separate
read-only audit used the current helper against the downloaded original bytes,
matched each custom ID to its frozen manifest cap, and joined each result to
its accepted worker and usage. No inference was replayed to fill this
observation gap. [Output audit](../ops/reports/primary-gptoss120-final-content-audit-2026-10-24.json),
[executed benchmark source](../ops/reports/primary-gptoss120-durable-benchmark-executed-2026-10-24.py),
[low-effort smoke](../ops/reports/primary-gptoss120-low-effort-durable1-attribution-2026-10-24.json).

The first primary configuration failed after loading weights because its
2,048-token prefill chunk left insufficient KV memory. A 1,024-token chunk kept
the complete 18,440-token context inside the same 0.24 memory fraction. Initial
primary capacity was one full-context slot; its worker reserved 18,440 tokens
and maintains leases independently of the 1,800-second inference timeout.
Secondary requested context 18,440 is internally padded to 18,688 by llama.cpp;
the platform still admits the smaller configured bound.

The secondary subsequently passed an **uncached 16,384-input / 2,048-output**
request in 113.396 seconds, with final content present and zero cached input
tokens. Its server-side timings separate 59.612 seconds of prefill
(274.85 tokens/s) from 53.673 seconds of decode (38.14 tokens/s). This is a
CPU/GPU configuration measurement, not an isolated GPU hardware comparison.

Moving seven more expert layers onto this same MIG (CPU expert layers 35 → 28,
16 inference threads unchanged) reduced the same uncached request to
**98.640 seconds**: prefill 50.969 s / 321.45 tokens/s, decode 47.560 s /
43.04 tokens/s. Peak device allocation was 16,956,915,712 bytes with
8,410,234,880 bytes minimum sampled free. The 13.0% wall-time reduction is one
ordered paired observation, not a repeated confidence interval. All 33 other
managed service identities stayed unchanged. See the [tuning record](../ops/reports/pod2-gptoss120b-moe28-tuning-2026-10-24.json),
[before](../ops/reports/pod2-gptoss120b-long-moe35-2026-10-24.json) and
[after](../ops/reports/pod2-gptoss120b-long-moe28-2026-10-24.json).

## Completed 120B H100 matrix and concurrency tuning

Each row below processes the same 20 cells: 109,056 exact rendered input tokens
and 13,600 generated tokens, with fixed-length EOS suppression. It measures
engine HTTP, not durable database acceptance or natural answer quality.
Completion latency starts at the common burst arrival and includes client
queueing. The sequence-1 configuration used memory fraction 0.24; sequence-4
used 0.26. Context 18,440, prefill chunk 1,024, weights, precision and prefix
cache setting stayed the same. The change is a configuration comparison on
shared hardware, not an isolated causal estimate of the sequence limit alone.

| Slots / client dispatch | Seconds | Generated tokens/s | Completion P50 / P95 / P99, s | Final text present |
| --- | ---: | ---: | --- | ---: |
| 1 / FIFO | 261.58 | 51.99 | 185.71 / 252.20 / 259.71 | 9/20 |
| 1 / shortest | 254.71 | 53.39 | 36.11 / 217.42 / 247.25 | 9/20 |
| 1 / class round robin | 248.57 | 54.71 | 202.88 / 239.46 / 246.75 | 10/20 |
| 4 / FIFO | 88.23 | 154.13 | 46.02 / 67.40 / 84.07 | 10/20 |
| 4 / shortest | 91.03 | 149.40 | 12.63 / 63.85 / 85.59 | 10/20 |
| 4 / class round robin | 78.92 | 172.33 | 48.23 / 66.58 / 76.45 | 9/20 |
| 8 / FIFO | 46.77 | 290.81 | 12.70 / 42.12 / 45.84 | 9/20 |

All seven runs completed 20/20 streams without an inference error. Their reports
are [seq1 FIFO](../ops/reports/primary-gptoss120-seq1-fifo-2026-10-24.json),
[seq1 shortest](../ops/reports/primary-gptoss120-seq1-shortest-2026-10-24.json),
[seq1 class](../ops/reports/primary-gptoss120-seq1-class-fair-2026-10-24.json),
[seq4 FIFO](../ops/reports/primary-gptoss120-seq4-fifo-2026-10-24.json),
[seq4 shortest](../ops/reports/primary-gptoss120-seq4-shortest-2026-10-24.json),
[seq4 class](../ops/reports/primary-gptoss120-seq4-class-fair-2026-10-24.json) and
[seq8 FIFO](../ops/reports/primary-gptoss120-seq8-fifo-2026-10-24.json).
Sequence-4 FIFO was 2.96× the initial FIFO generated throughput. Shortest-first
reduced median waiting; class round robin had the highest observed throughput
in the four-slot burst. Neither establishes a repeated-run policy winner.
Reasoning can consume a 32/128-token cap before final content starts, so the
final-text counts remain separate from complete-stream success.

The final eight-slot FIFO reached **2,331.94 input and 290.81 generated tokens/s**,
5.59× the original one-slot FIFO and 1.89× the four-slot FIFO. Its offered
20-request burst completed at 0.42766 inferences/s. HTTP TTFT P50/P95/P99 was
0.500/1.223/1.581 seconds, measured from each request's dispatch, not burst arrival.
There was no eight-slot comparison of all three policies.

| Eight-slot FIFO workload class | Requests | Completion P50 / P95 / P99, s | HTTP service P50, s | HTTP TTFT P95, s |
| --- | ---: | --- | ---: | ---: |
| Prefill-heavy | 10 | 11.33 / 33.96 / 40.26 | 2.80 | 1.432 |
| Decode-heavy | 3 | 41.88 / 46.28 / 46.67 | 40.25 | 1.097 |
| Balanced | 7 | 12.15 / 41.71 / 41.84 | 10.24 | 1.200 |

These are classes within the same mixed burst, not isolated prefill/decode
kernel benchmarks. In particular, three decode-heavy samples cannot establish
a tail-latency SLO. The class rule and per-request counts are in the raw report.

The four-slot startup logged 10.77 full-context requests of hybrid KV capacity.
It then passed **four simultaneous 16,384-input / 2,048-output natural requests**
in 38.3–39.3 seconds, observed running peak four and 21,984 MiB resident per
owned rank before enabling four worker slots. See the [admission and concurrency
proof](../ops/reports/primary-gptoss120-seq4-retune-2026-10-24.json).

The final tuning step retained fraction 0.26 and admitted eight slots only after
a fresh per-rank `fraction × total memory + 1 GiB` gate. Startup logged 10.63
full-context capacity. **Eight simultaneous 16,384-input / 2,048-output natural
requests completed in 44.113 seconds**, with running peak eight and minimum
sampled shared-device free memory 2,103 MiB. The worker now advertises eight
slots and reserves 147,520 tokens. The healthy four-slot configuration remains
available for rollback; no further sequence-limit increase was attempted.
[Eight-slot proof](../ops/reports/primary-gptoss120-seq8-retune-2026-10-24.json).
The final watchdog now requires `fraction × total memory + 2,048 MiB` on
**every** primary rank before restarting. The former 512-MiB margin could
admit less memory than the observed resident allocation. The new margin raises
the per-rank threshold to 23,253.34 MiB; the read-only recovery preflight found
at least 24,069 MiB of current free plus this engine's owned resident memory.
That estimate is not a promise of future availability: recovery must pass a
fresh physical-free-memory check after the old engine exits. The worker and
watchdog rollout preserved the engine and its inference configuration.
[Restart-margin rollout](../ops/reports/primary-gptoss120-worker-effort-margin-2026-10-24.json).

Owned-process SM-active samples are retained separately from shared device-wide
utilization. NCCL spin/communication kernels can count as active, so they are
not useful-math efficiency. During final FIFO, each H100 rank had 36 valid owned
process samples: mean SM-active was 78.44%, 79.69%, 74.58% and 43.31% for devices
0–3. The engine's sampled running peak was eight and KV usage peak 28.24%.
Startup selected Marlin on all four ranks and
reported NCCL 2.27.3; it skipped the P2P test and trusted the driver. Neither that
configuration nor these logs establish the kernel path of every collective.
See [communication evidence](../ops/reports/primary-gptoss120-communication-2026-10-24.json).

## Completed 120B CPU/MIG matrix on the secondary

The same five input lengths crossed with four output lengths completed **20/20**
in 666.36 seconds, with 109,056 input and 13,600 generated tokens. The runtime's
own template/tokenizer generated a separate exact-count corpus from the same
public source. Its prompt bytes and precision differ from the primary, so this
is a deployment comparison, not a controlled GPU hardware ranking.

| Configuration | Input tokens/s | Generated tokens/s | Completion P50 / P95 / P99, s | Final text present |
| --- | ---: | ---: | --- | ---: |
| One MIG + 28 CPU expert layers, 16 threads, one slot, FIFO | 163.66 | 20.41 | 422.19 / 605.72 / 654.23 | 10/20 |

All 20 appended server timing pairs processed the full manifest input count,
matching returned usage with `cache_prompt=false`. The earlier warm calibration
processed only five tokens of an 80-token prompt, demonstrating why usage alone
is insufficient to prove no prefix reuse. Actual output counts matched every
cap. The pinned server implements `ignore_eos`; its generation minimum-token
setting is not relied upon. Each request's prefill/decode durations are retained
in the [matrix report](../ops/reports/pod2-gptoss120b-matrix-fifo-2026-10-24.json).
Across this single-slot run, server timings sum to 359.803 seconds of prefill
and 303.189 seconds of decode, or 303.10 prompt tokens/s during prefill and
44.86 reported generated tokens/s during decode. These phase rates explain
the lower 20.41 generated tokens/s over the full mixed run; they cannot be
added as independent fleet capacity. Native transition timing and the
reported-token denominator distinction are preserved in the [stage analysis](../ops/reports/pod2-gptoss120b-matrix-stage-analysis-2026-10-24.json).

The run collected 2,626 memory samples: peak device use 17,140,482,048 bytes,
minimum free 8,226,668,544 bytes. The engine averaged 7.85 CPU cores with 16
threads inside a 108.8-vCPU cgroup quota. Per-MIG SM utilization remained
unavailable. All 35 managed service identities predated the run and were still
ready afterward. This lane provides real large-model execution and independent
recovery capacity; its long-prompt prefill latency is a substantial throughput
constraint.

## Completed 7B mixed-length comparison on the secondary GPU

One MIG ran the same 20 requests for each policy: inputs
128/512/2,048/8,192/16,384 crossed with outputs 32/128/512/2,048, four in flight.
Each run generated exactly 109,056 input and 13,600 output tokens. Prefix
caching was disabled; fixed output lengths used explicit EOS suppression.

| Client dispatch | Success | Input tokens/s | Output tokens/s | Completion P50 / P95 / P99, seconds |
| --- | ---: | ---: | ---: | --- |
| FIFO | 20/20 | 523.37 | 65.27 | 117.52 / 162.57 / 199.21 |
| Shortest requested work | 20/20 | 484.92 | 60.47 | 35.80 / 160.79 / 212.07 |
| Round robin by workload class | 20/20 | 552.60 | 68.91 | 122.86 / 169.51 / 191.78 |

These completion times start at the common offered burst and include client
queue delay. The engine and production SQL scheduling policies did not change.
The small samples, sequential run order, shared hardware and overlapping CPU
build/download/hash work prevent attributing all variation to policy. In this
burst, shortest-first improved median waiting but had the largest tail; that
does not establish a production policy winner or P99 SLO.

[FIFO raw report](../ops/reports/pod2-matrix7b-18k-fifo-2026-10-24.json),
[shortest raw report](../ops/reports/pod2-matrix7b-18k-shortest-2026-10-24.json),
[class-fair raw report](../ops/reports/pod2-matrix7b-18k-class-fair-2026-10-24.json).
MIG SM utilization returned `NOT_SUPPORTED`; memory samples and engine KV
occupancy are reported separately and are not substituted for SM utilization.

## Natural answer quality and reasoning effort

A paired pilot used the same eight predetermined public questions, a 512-token
cap, JSON object output and concurrency four. Both phases returned final text,
valid JSON and natural stops for 8/8 requests; neither suppressed EOS nor forced
a minimum length.

| Effort | Generated tokens | Elapsed seconds | Answer exact match | Answer token F1 |
| --- | ---: | ---: | ---: | ---: |
| Omitted, runtime default | 1,302 | 9.046 | 7/8 | 0.975 |
| Explicit `low` | 411 | 2.910 | 5/8 | 0.781 |

The low-effort phase generated 68.4% fewer tokens but had worse answer scores
in this small sample. The platform therefore preserves the runtime default
when effort is omitted; callers can explicitly request `low`, `medium` or
`high`. Validation and all workers preserve that choice. This is a diagnostic
pilot with sequential phases on shared hardware, not an official SQuAD score,
a statistical quality conclusion or a customer acceptance threshold. Public
questions may also have appeared in training. The evaluator rejects missing
final text, duplicate JSON keys and nonfinite values separately from transport
success. [Paired natural-output evidence](../ops/reports/primary-gptoss120-natural-quality-paired8-2026-10-24.json).

A subsequent durable comparison reused all 20 frozen mixed-length request
bodies and their output caps, adding only `reasoning_effort: "low"`. Both
cohorts used natural EOS and no forced minimum. All 40 tasks had one accepted
attempt; downloaded output usage matched the task, attempt and batch ledgers.

| Effort | Durable completion | Final text present | Length capped | Generated tokens | Elapsed seconds |
| --- | ---: | ---: | ---: | ---: | ---: |
| Omitted, runtime default | 20/20 | 10/20 | 15/20 | 9,292 | 43.884 |
| Explicit `low` | 20/20 | 20/20 | 14/20 | 8,742 | 46.289 |

Each output-cap group (32, 128, 512 and 2,048) had final text in 5/5 low-effort
responses. All five 32-token responses still hit their cap. This measures
final-text presence, not correctness or completeness. The low cohort used
5.9% fewer generated tokens but took 5.5% longer in this ordered observation.
Placement changed from 17 primary / 3 secondary tasks to 19 / 1: only 16 pairs
used the same runtime; four are explicitly placement-confounded. The audit
retains that split instead of attributing the whole change to effort alone.
Within the 16 same-runtime pairs, all on the primary, final text increased
from 9/16 to 16/16; the four changed-runtime pairs increased from 1/4 to 4/4.
These results do not overturn the eight-question quality regression above or
justify changing the default.

See the [low-effort batch](../ops/reports/primary-gptoss120-natural-durable20-low-2026-10-24.json),
[launcher input comparison](../ops/reports/primary-gptoss120-natural-durable20-low-variant-2026-10-24.json),
[accepted-task attribution](../ops/reports/primary-gptoss120-natural-durable20-low-attribution-2026-10-24.json)
and [paired content audit](../ops/reports/primary-gptoss120-final-content-audit-2026-10-24.json).

## Cost and the daily target

The previously recorded complete allocation rates are $16.10/hour for the
primary Pod and $11.18/hour for the secondary. At those rates, keeping both
allocations running costs $654.72/day, including unrelated primary workloads
and other secondary inference lanes. This is an allocation assumption, not a
fresh invoice, a single-MIG price, or marginal BatchForge cost.

For this equal-cell matrix the mean request is 5,452.8 input and 680 generated
tokens. Ten million such requests per day requires an average **115.74
completions/s**, **631,111 input tokens/s** and **78,704 generated tokens/s**.
At 70% average utilization, provisioned sustainable capacity must exceed
**165.34 completions/s**, before retries and quality rejection. Prefill and
decode share resources; independent peak token rates cannot be added together.

| Measured 120B lane | Inferences/s | Rate × 86,400 | At assumed 70% duty |
| --- | ---: | ---: | ---: |
| Shared H100 TP4, eight-slot FIFO | 0.42766 | 36,950/day | 25,865/day |
| CPU + one MIG, single-slot FIFO | 0.03001 | 2,593/day | 1,815/day |
| Sum of separately measured rates | 0.45767 | 39,543/day | 27,680/day |

These are **arithmetic extrapolations of short engine bursts**, not measured
daily accepted tasks. Summing the lanes also assumes both retain those rates
simultaneously. At the assumed 70% duty, ten million tasks/day is about **361×**
this combined rate, before quality rejection, retries and control-plane costs.
It is not evidence for a particular number of dedicated GPUs.

Assigning each complete allocation's recorded price entirely to its 120B lane
gives $10,457 per million engine inferences for the primary and $103,471 for the
secondary at the measured rates. These gross allocations include unrelated
primary work and the secondary's other fifteen 7B lanes; marginal 120B costs
are unavailable. Blind linear replication at 70% duty would require 387 copies
of the measured primary configuration ($149,536.80/day at that assumed rate),
or 362 pairs of both allocations ($237,008.64/day). Neither is a purchase plan
or a quote. Dedicated-device benchmarks, model/runtime selection, prefill and
decode profiling, and workload economics must precede procurement.
[Inputs, formulas and source hashes](../ops/reports/100b-capacity-arithmetic-2026-10-24.json).

The old 34.25 accepted tasks/s measurement used shorter 7B tasks and the
previous 20-engine deployment. It is not current 120B throughput and must not
be extrapolated into a 120B GPU count. A dedicated H100 with the full memory
available is also a different configuration from this shared TP4 allocation.

The frozen primary 120B mixed-length JSONL contains 530,026 bytes for 20 requests,
with SHA-256 `ffe741d9c87bd52ecf571f4b5c6a8ae5c5eac3d46f997399f49b2ff4dcc3f634`.
At ten million requests of that byte distribution, raw input alone would be
**265.013 GB/day**, before output, duplicate storage, indexes, WAL and retries.
The current free database cannot serve as that payload archive. Object storage,
streaming input/output, retention and a funded database capacity plan are
required in addition to inference throughput; multiplying GPU count alone does
not resolve this storage limit.

## Reproduction and recovery scope

The [workload guide](mixed-benchmarks.md) describes hashes, exact model-specific
prompt rendering, usage equality, timeout/failure accounting and final-text
measurements. The [selection assessment](100b-model-feasibility.md) preserves
weight size, precision, runtime and failed startup evidence. Private prompts,
credentials and model weights are excluded from the repository.

Managed engines have per-rank memory admission. TP4 additionally enables
observed-child identity tracking and pidfd cleanup; the CPU-expert lane also
checks actual cgroup headroom before each restart. Linux tests prove TP4 owned
orphan cleanup without signaling an unrelated process. Single-GPU engines
retain leader identity checks; descendant cleanup after their leader dies
has not been exercised.

The real engine-loss exercise captured **seven valid primary leases** in the
eight-task run `bench-45c4387884b3`. The owned primary engine and worker were
unavailable from 23:30:38.245804 through 23:32:40.099379 UTC on October 23:
**121.853536 seconds**. All seven captured tasks expired with `lease_expired`
and were accepted exactly once by `secondpod-gptoss120b-cpu-expert`, with the
last accepted by 23:32:33.910420. The full batch completed 8/8, zero failures or
cancellations, with 1,008 accepted input and 1,336 generated tokens. These are
the fallback runtime's actual counts, not eight times a primary template target.

The primary restart passed its fresh 2,048-MiB margin gate, and engine, worker
and watchdog restoration completed at 23:33:36.651825 UTC. Both primary API
processes and its gateway retained their identities throughout the sampled
exercise. Lease recovery was automatic; primary restoration was controlled by
the fault operator. This proves **task re-execution and accepted-result
fencing**, not live KV-cache migration, physical GPU reset recovery, whole-Pod
replacement or database failover. [Probe evidence](../ops/reports/primary-gptoss120-engine-outage-2026-10-24.json),
[durable batch](../ops/reports/primary-gptoss120-engine-outage-benchmark-2026-10-24.json).
An explicit follow-up selected one of those reclaimed tasks, read its original
expired primary attempt token into memory, and sent an old heartbeat. It returned
`409 stale_attempt`; the task still had exactly one accepted secondary result
and unchanged usage. The report records the task ID and attempt number without
persisting the token. [Same-task fence supplement](../ops/reports/primary-gptoss120-engine-outage-same-task-fence-2026-10-24.json).
The managed database remains a free Neon instance; private ingress and process
watchdogs do not establish a production SLA.

The current schema-v3 backup was actually restored into a uniquely named
temporary database, checked, and removed. The 103,379,878-byte archive contains
343 batches, 25,134 tasks and 25,691 attempts. All three migration checksums,
SQL function security settings, accounting invariants and accepted-attempt
uniqueness passed. Pure restore took 8.452 seconds; restore, checks and cleanup
took 26.509 seconds. A private off-host copy matches the archive SHA-256.
This tests the archive on the same Neon branch; it does not restore runtime
roles/secrets or prove regional failover. See the [backup and capacity limits](backup.md).

The final post-cohort snapshot at 23:52:13 UTC on October 23 found 17 fresh,
ready workers across three model pools, zero pending/running tasks and no
in-progress batches. All five primary managed services were ready; its API
and gateway process identities survived the engine-loss exercise. The runtime
database connection used client TLS with `sslmode=verify-full` and the
least-privilege `bf_runtime` role. The separately observed Neon server-side
`pg_stat_ssl` leg was false; it is not the client connection's TLS state.
[Final readiness](../ops/reports/primary-gptoss120-final-readiness-2026-10-24.json).
The [release evidence index](../ops/reports/100b-release-evidence-2026-10-24.json)
records source hashes, executed-versus-observer versions and verification scope.

At 00:11:50 UTC on October 24, an approved owner-only credential rotation
remediated an administrative password accidentally displayed during a tool-log
configuration check. Fresh connections rejected the old password through both
direct and pooled endpoints. The new owner connection and unchanged runtime
connection passed; 17 workers remained fresh with no unfinished tasks. The
primary owner configuration and local owner credential are mode 600, and no
service restart was required. [Rotation verification](../ops/reports/owner-credential-rotation-2026-10-24.json).
