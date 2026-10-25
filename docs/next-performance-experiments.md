# Next four-H100 performance experiments

> Historical design and experiments: the current October 25 deployment uses
> one four-H100 Pod, 480B-total / 35B-active AWQ MoE, Azure PostgreSQL 17/schema 11
> and existing S3. See [current operations](operations.md). Old Neon, 120B/235B,
> two-Pod and migration-version examples below are not current settings or safe
> restart commands for the 480B primary. No whole-Pod compute HA is established.


This is a read-only review of the `71629cf` evidence, dated October 24, 2026.
It neither changes the live engine nor establishes another performance result.
The pinned model is **Qwen3-235B-A22B-Instruct-2507-FP8**, revision
`e156cb4efae43fbee1a1ab073f946a1377e6b969`: **235B total / 22B active**,
not a dense 235B model. The [official model card](https://huggingface.co/Qwen/Qwen3-235B-A22B-Instruct-2507-FP8)
also specifies 128 experts, eight activated experts, and four KV heads.

## What the present evidence says

The [final production-config fixed100](../ops/reports/primary-qwen235b-final-source-platform-analysis-2026-10-24.json)
completed in 250.149091 seconds, with P95/P99 228.293969/246.166256 seconds,
545,280 prompt tokens and 68,000 output tokens. The four NVML utilization
means were 59.38%, 83.51%, 87.90%, and 90.97%. These are device activity
samples, not useful FLOPs or Tensor Core utilization.

GPU0's lower activity is not evidence that it received less model work. The
earlier, separately captured [four-request trace](../ops/reports/primary-qwen235b-trace-offline-analysis-v2-2026-10-24.json)
contains exactly 120,717 CUDA kernels per rank and nearly equal GEMM,
attention and MoE interval unions. The main rank difference is custom
collective time:

| TP rank | Any-kernel union (s) | Custom collective union (s) | MoE/expert union (s) |
| --- | ---: | ---: | ---: |
| 0 | 2.7710 | 0.9192 | 0.6984 |
| 1 | 5.5975 | 3.7340 | 0.7003 |
| 2 | 4.7559 | 2.8942 | 0.7007 |
| 3 | 4.8766 | 3.0246 | 0.7011 |

Each rank's first-to-last kernel span was approximately 6.123 seconds. The
[v0.10.2 custom collective source](https://github.com/vllm-project/vllm/blob/v0.10.2/csrc/custom_all_reduce.cuh)
has device-side flag-polling barriers. **Inference:** other ranks may spend
more activity time waiting for rank0's next launch. CPU dispatch gaps are a
plausible lead; the trace does not establish their cause, and the short,
profiled static16 capture cannot explain every sample in the later adaptive64
run. Making the other three cards spin longer would increase utilization
without increasing useful throughput. The measured NCCL fallback was already
4.90% slower, so switching collectives again is not the leading experiment.

The final100 engine histograms contain 6,547.777 seconds of summed per-request
decode stage time, versus 6,595.392 seconds of summed inference stage time.
These overlapping request durations are not a wall-time decomposition, but
they make decode dispatch a better first lead than assuming prefill is the
main bottleneck. Physical KV usage peaked at 83.02%; worker reservations
approached the conservative 298,384-token limit, with up to 22 reservation
waiters. This is consistent with admission pressure and does not prove that
more memory alone improves throughput.

## Shared measurement contract

Freeze model/revision, generation settings, TP4, custom collective,
BF16 KV, context 18,440, prefix caching off, exact task fixtures and token
totals. Keep schema, archival policy, tenant quota, source hashes and cost
basis identical between compared runs. Each candidate needs its own runtime
fingerprint. Profile captures are separate from timed runs.

Before any restart, bind the owned manifest, PID/start/cwd identities and
environment hashes; drain accepted work and require zero engine requests.
Use the existing owned-process recovery pattern, a unique durable operation
ID, a recorded baseline and an executable exact restoration. Recheck API,
gateway and tenant-ledger invariants afterwards. Transport uncertainty,
duplicate accepted attempts, wrong token totals, NaNs, an admission pause,
OOM or unproven recovery fail the candidate; do not silently retry its label.

Use the unchanged fixed100 for continuity, then a **separately labelled**
rolling-arrival fixture made from whole repeats of the same 20-cell grid.
Freeze arrival timestamps/seed and preserve each cell's share. A bounded
open-loop arrival schedule avoids a client completion loop hiding overload.
Report offered versus accepted versus completed rates, backlog slope,
admission429s, queue wait, TTFT, TPOT, completion P50/P95/P99, and per-cell and
per-tenant tails. Include warmup, steady observation and drain costs; never
exclude drain time from whole-cohort cost. A short rolling test is not a
24-hour capacity proof. Respect the current retained-history and idempotency
caps until the separately reviewed history-retirement implementation exists.

For a promoted candidate, use repeated bounded A/B windows, preferably an
A/B/A sequence, with raw run values. One old serial baseline and one new run
cannot support statistical significance. Pre-register a practical acceptance
rule, for example at least 5% median whole-cohort improvement across three
runs per configuration, no cell's completion P95 worse by more than 10%, no
correctness or recovery regression. Those are engineering acceptance limits,
not confidence intervals; report individual runs even when the rule fails.

## Ranked controlled experiments

### 1. Decode CUDA graphs without a simultaneous compiler change

**Best expected benefit/risk ratio.** The deployed `71629cf` process manager
unconditionally adds `--enforce-eager`; the report's null
`ENGINE_ENFORCE_EAGER` value does not mean graphs are enabled. The new optional
path in `ops/manage.py` preserves eager execution by default and accepts only
the bounded experiment below. Source support and argument tests are not an
executed graph deployment or a throughput result.

For the first candidate, retain compilation level0 and explicitly request
`FULL_DECODE_ONLY`, with a bounded capture set no greater than64, such as
`[1,2,4,8,16,24,32,48,64]`. This is a proposed configuration to validate against
the installed package, not a command already run. The pinned
[CompilationConfig source](https://github.com/vllm-project/vllm/blob/v0.10.2/vllm/config/compilation.py)
defines this mode and allows full graphs without compilation; the
[configuration initialization](https://github.com/vllm-project/vllm/blob/v0.10.2/vllm/config/__init__.py)
disables graphs under enforced eager execution. Removing eager alone would
also choose default piecewise compilation, confounding the first experiment.

The explicit candidate environment is:

```dotenv
ENGINE_ENFORCE_EAGER=0
ENGINE_MAX_NUM_SEQS=64
ENGINE_COMPILATION_CONFIG='{"level":0,"cudagraph_mode":"FULL_DECODE_ONLY","cudagraph_capture_sizes":[1,2,4,8,16,24,32,48,64]}'
```

The manager rejects unknown or duplicate JSON keys, another compilation
level or graph mode, noninteger sizes, unsorted/duplicate sizes, a missing
1 or final sequence-limit size, and any size/sequence limit above64. A
compilation config with eager mode is rejected rather than silently ignored.
The JSON becomes one canonical argv element, without shell evaluation.
`engine_execution_configuration(env)` returns the normalized descriptor for
the rollout's new runtime fingerprint. The controller must still bind the
installed version, actual resolved configuration and post-capture memory;
the process-manager function does not inspect a running GPU.

Require actual installed source/CLI validation, resolved config, successful
capture logs on every TP rank and a separate untimed trace demonstrating
replay. The [v0.10.2 FlashAttention backend](https://github.com/vllm-project/vllm/blob/v0.10.2/vllm/v1/attention/backends/flash_attn.py)
has different graph support depending on its actual FA implementation; the
generic startup message does not identify that version. No backend swap is
part of this experiment.

**Memory gate:** capture consumes memory. Remeasure physical KV capacity
after startup/capture. Do not retain 298,384 admitted tokens merely because
the old eager run fit. First compare both modes with the same budget at most
95% of the smaller observed capacity, rounded down to16. If the budget must
change, run a new eager control at that shared budget. Keep claim limit64
and chunk size4,096. Compare full-grid throughput and long-output tails,
not just a graph microbenchmark. Natural QA20 is a gross regression check;
it is not a general quality guarantee.

### 2. Single-variable prefill chunk budget: 4,096 versus 8,192

**Lower implementation risk, less certain overall benefit.** Run under the
winning execution mode, with matching admission budget and all other
settings unchanged. Start at8,192 rather than jumping to the full context.
The [version-specific optimization guide](https://docs.vllm.ai/en/v0.10.2/configuration/optimization.html)
describes the TTFT/ITL tradeoff and recommends larger token batches especially
for smaller models; that recommendation is not proof of a benefit for this
235B MoE. The pinned
[scheduler configuration](https://docs.vllm.ai/en/v0.10.2/api/vllm/config/scheduler.html)
also validates batching and context constraints.

Require a fresh startup-memory/KV gate and a shared safe budget for A/B.
Preserve the exact mixed fixture, and additionally publish its prefill-heavy,
decode-heavy and balanced subsets. Larger chunks may improve prefill while
delaying the decode iterations that dominate the long-output tail. A throughput
win that violates the declared tail limit is not automatically a platform win.
Do not change `max_num_partial_prefills`, attention implementation, prefix
caching or compiler mode in the same experiment.

### 3. Admission fairness under rolling mixed arrivals

**Potential tail benefit; requires measured head-of-line evidence first.**
The local `WeightedLimiter` is FIFO. A request that does not fit blocks smaller
requests behind it, even when some headroom remains. This is intentional
starvation prevention. Log only bounded aggregate counters: head-ticket
size, available tokens, oldest wait duration, and the count/total tokens of
later fitting tickets. Correlate those values with real engine occupancy and
accepted-task queue time during the frozen rolling fixture. Merely seeing22
waiters does not establish avoidable head-of-line delay.

If confirmed, test bounded backfilling with both a bypass-count cap and a
maximum oldest-ticket age, then a drain-for-head phase that stops bypasses.
Preserve exact block-rounded reservations, cancellation cleanup, heartbeat
leases, the latched uncertainty pause and the original total budget. Require
adversarial tests where a large request faces indefinitely arriving small
requests, cancellation races at grant, pause races, and multiple tenants.
Measure each tenant's normalized slowdown and oldest wait, as well as total
throughput. Never promote unrestricted shortest-job-first admission based on
its aggregate P50 alone. The existing SQL scheduler's tenant rotation and aging
do not by themselves prevent starvation inside a later local reservation queue.

## Cost calibration and postponed changes

The live adaptive64 profile explicitly preserves a static16 HTTP fit. Its
11.7753 output tokens/s is a per-request stream proxy, not the final cohort's
271.838 aggregate output tokens/s. Replacing one with the other would make
cost prediction less meaningful. Whole-Pod cost stays $16.10/hour as an
assumed allocation scenario, not an invoice, and64 claim slots are not64
independently priced execution slots.

A read-only join of final100 task rows and attempt decisions shows median
observed-attempt/predicted-service ratios of **2.80–6.19** across the five
32-output cells, versus **1.15–1.16** across the five 2,048-output cells. The
overall median ratio is1.594. Attempt duration includes reservation and report
time, so this is operational prediction error, not isolated model-kernel
error. The input byte/4 estimates also have per-cell median estimated/actual
prompt ratios between0.969 and1.297. The worker's exact tokenizer reservation
does not retroactively make the SQL scheduler's earlier input estimate exact.
These figures are reproducible from
[the final100 raw metadata](../ops/reports/primary-qwen235b-final-source-fixed100-2026-10-24.json),
joining `tasks.id` to `attempt_decisions.task_id`, then grouping by actual
`prompt_tokens`/`completion_tokens`.

Keep the old fit labelled projected until new measured calibration and holdout
validation exist. A later predictor should record execution profile,
concurrency/load range, token lengths, calibration date, uncertainty and
out-of-range behavior. Distinguish service prediction from end-to-end occupancy.
With only one physical worker, a refreshed fit cannot demonstrate heterogeneous
routing savings; use multi-profile simulations only as simulation evidence.

FP8 KV is postponed behind the above experiments. The
[v0.10.2 KV guide](https://docs.vllm.ai/en/v0.10.2/features/quantization/quantized_kvcache.html)
emphasizes memory/concurrency gains and representative scale calibration.
Its general latency wording should not be read as a hardware-specific law:
the pinned FlashAttention source has capability-gated FP8 paths. Check the
installed backend's actual support and Q/K/V scaling first. FP8 KV changes
numerics and fingerprints; it needs independent long-context/long-output
quality comparisons, scale provenance and a fresh memory/admission test.
QA20 alone is too small for that decision. Doubling KV capacity is neither a
promise of twice the task throughput nor a reason to bypass the worker's
uncertainty fence.
