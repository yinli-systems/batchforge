# Four-H100 large-model validation

The running model is the official
[Qwen3-235B-A22B-Instruct-2507-FP8](https://huggingface.co/Qwen/Qwen3-235B-A22B-Instruct-2507-FP8),
revision `e156cb4efae43fbee1a1ab073f946a1377e6b969`, on four H100 80GB devices.
The initial deployment completed at 02:04:28 UTC on October 24, 2026, with tensor
parallelism 4, eight sequence slots, an 18,440-token context limit, 4,096 batched
tokens and a 0.90 GPU memory fraction. Its model card describes
235B total parameters and 22B active parameters. This distinction matters when
comparing compute demand with another mixture-of-experts model.

Each rank loaded 55.1856 GiB of model state and reported 14.17 GiB available for
KV cache. The engine reported 316,144 cache tokens and 17.14 complete contexts.
This establishes memory admission headroom; it does not establish Tensor Core
saturation. The direct 20-cell fixed-output calibration completed all 20
requests in 199.077 seconds, with 109,056 prompt tokens and 13,600 generated
tokens (68.315 generated tokens/s across this burst). All final streams contained
text. This direct calibration is separate from durable platform benchmarks and
does not evaluate answer quality. See
[deployment evidence](../ops/reports/primary-qwen235b-switch-complete-2026-10-24.json)
and [raw calibration](../ops/reports/primary-qwen235b-direct-fixed20-seq8-2026-10-24.json).

The previous GPT-OSS-120B deployment used TP4, 64 slots, the same context and
batched-token limits, and a 0.85 memory fraction. Its exact manifest and private
environments remain the initial switch's rollback target.

## Switch contract

`ops/ha/switch_qwen235b.py` defaults to preparation only. It creates private
candidates and an owner-readable report under `.runtime/qwen235b-switch-v1/`.
The `--apply` operation requires separate coordination after the 120B performance
and fault experiments. The API catalogs must already advertise the Qwen model;
the controller does not modify API or gateway configuration.

The controller snapshots the current 64-slot manifest and environments, checks
that existing 120B work has drained, stops the owned watchdog and worker, checks
the drain again, then stops the owned engine. Each engine start uses a fresh
four-rank GPU memory gate. The Qwen engine uses TP4, eight slots, 18,440 context,
4,096 batched tokens, eager execution, no prefix cache and a 0.90 memory fraction.
Its own startup logs must report at least eight complete contexts. Subsequent
retunes also require the exact reported cache-token count to cover every full
context; a rounded concurrency value cannot bypass this check.

During calibration, the live manifest contains the new engine but no new worker.
Only after all 20 direct requests pass validation does the controller write the
new scheduling profile, expose the worker in the live manifest, start it and
resume the watchdog. The worker has a new ID and model-specific runtime
fingerprint. Its environment copies an explicit allowlist of worker credentials,
URLs and operational options; tenant keys, database credentials and object-store
credentials are excluded.

An engine startup, KV admission or calibration error restores the original
manifest, checks that the original environment files have not changed, and
restarts the original engine and worker with readiness checks. A failed rollback
leaves a `rollback_blocked` report for operator intervention. API and gateway
processes are never deliberately restarted by this controller. A non-prepared
report prevents re-running the switch under the same stable identity after an
ambiguous interruption.

`ops/ha/retune_qwen235b.py` prepares a separate guarded eight-to-sixteen-slot
candidate. It preserves the numerical runtime fingerprint and the original
worker's operational settings, reserves 295,040 full-context tokens, and runs a
fresh direct 20-cell calibration at concurrency sixteen before starting the
profiled worker. No eight-slot rate is relabeled as a sixteen-slot measurement.
Startup or calibration failure restores the exact eight-slot manifest and
environment files. Preparation has no engine, worker, API or gateway side effects.

## Calibration and evidence limits

The frozen SQuAD 1.1 public-text workload contains the full Cartesian product of
input lengths 128, 512, 2,048, 8,192 and 16,384 with output lengths 32, 128, 512
and 2,048. Pinned-tokenizer prompt counts must equal the server's actual counts.
Direct calibration forces the output lengths, validates every stream and usage
record, and runs at concurrency eight. This is a capacity experiment, not a
natural-output quality evaluation.

The initial profile uses two separate nonnegative least-squares fits through the
origin: actual prompt tokens against HTTP time to first visible text, and actual
output tokens minus one against the visible stream span. Both rates must be
positive and finite. The first phase includes queueing and the first decode
token; the second is affected by transport buffering. These are measured HTTP
phase proxies for this specific burst and concurrency, not isolated prefill and
decode kernel rates. The report retains residual errors and raw timing/usage
metadata. GPT-OSS rates are never reused as Qwen measurements.

The runtime fingerprint includes the pinned model/config/tokenizer hashes,
FP8 quantization configuration, BF16 compute and KV assumptions, TP4 execution,
generation settings, actual runtime package versions and observed attention
backend messages. The observed message, `Using Flash Attention backend on V1
engine`, means the FlashAttention backend under the vLLM V1 engine; it does not
identify FlashAttention's algorithm version. A matching fingerprint is a routing constraint, not a claim
that semantic quality was evaluated.

Allocation cost remains the operator-supplied whole-Pod scenario divided by
eight nominal occupied slots. It is not an invoice, a measured marginal task
cost or proof of savings. There is one physical GPU type in this experiment;
logical scheduling profiles must not be presented as measured heterogeneous
hardware results.

## Subsequent profiling

The read-only inventory found Nsight Compute 2025.1.1 and CUPTI 12.8.90, but no
Nsight Systems or DCGM executable. Driver profiling is restricted to privileged
users, and the container lacks both `CAP_SYS_ADMIN` and `CAP_PERFMON`; therefore
hardware-counter access is expected to require additional host permissions.
This is a permission inference, not a completed profiler run. NVIDIA documents
the applicable [performance-counter permissions](https://developer.nvidia.com/nvidia-development-tools-solutions-err_nvgpuctrperm-permission-issue-performance-counters).

For the installed vLLM 0.10.2, the
[version-specific profiling guide](https://docs.vllm.ai/en/v0.10.2/contributing/profiling.html)
supports PyTorch tracing through `VLLM_TORCH_PROFILER_DIR`. The sixteen-slot
candidate configures an owner-only directory under the application's `.runtime`.
The installed worker and API implementation hashes were checked: initialization
constructs the profiler, and only explicit `/start_profile` and `/stop_profile`
RPCs start and finish a capture. The retune controller never calls those routes.
A later separate, short probe can test CUPTI activity tracing and inspect GEMM/MoE, attention and
NCCL kernel intervals on every rank. Profiling adds overhead and must not overlap
the throughput comparison. High NVML GPU activity can include communication or
spin kernels; memory allocation and GPU utilization alone do not prove that
GEMM compute is saturated. Overlapping kernel durations must not be summed and
reported as exclusive wall time.

The initial engine also warned that its exact H100 block-FP8 MoE configuration
file was absent (`E=128,N=384`, block shape `[128,128]`), so it used the default
configuration. It reported that `CutlassBlockScaledGroupedGemm` was unsupported
on the current platform. These are concrete optimization leads, not evidence
that either is the measured bottleneck. Kernel configuration remains unchanged
during the baseline comparison. The trace analyzer reports per-rank interval
unions, exclusive category intervals and overlapping intervals; it never sums
the four ranks into a global elapsed time or infers hardware utilization from
kernel names.

After the 235B experiment, two independent 120B TP2 replicas are a possible
follow-up. A simple weight-size projection is about 34.44 GiB per rank instead
of 17.22 GiB for TP4, before KV and runtime buffers. The potential tradeoff is
less communication per replica against duplicated weights and smaller batches.
Only a fresh memory gate and equal-work measurement can establish whether this
improves throughput; no TP2 deployment or measurement is included here.
