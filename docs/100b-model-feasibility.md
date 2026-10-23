# Serving a model above 100B total parameters on the existing Pods

Updated 2026-10-24. The primary TP4 deployment now serves real GPT-OSS-120B requests. The official-weight and compatibility assessment below records why this model was selected; measured results are distinguished from planning bounds. The second Pod uses a separately validated CPU-expert runtime and precision format.

The first candidate is **OpenAI GPT-OSS-120B in its original MXFP4 format, tensor parallel size 4 on the four shared H100s**. Its 116.83B total parameters satisfy the requested size, but only about 5.1B parameters are active per token. This is a sparse MoE model, not a dense 120B computation per token. OpenAI documents H100 support and a multi-GPU route. [OpenAI model documentation](https://developers.openai.com/api/docs/models/gpt-oss-120b), [OpenAI deployment guidance](https://developers.openai.com/cookbook/articles/gpt-oss/run-transformers).

## Official weights and memory floor

Bytes below were calculated by summing `size` for **root-level `.safetensors` files** in each publisher's public Hugging Face API response (`?blobs=true`). They include shard headers, exclude tokenizer/config files, and exclude duplicate alternative weight layouts such as GPT-OSS `original/` and `metal/`. GiB = bytes / 2^30. Disk size is a lower bound, not the allocated GPU memory or loading peak.

| Official model | Total / active parameters | Format | Weight bytes | GiB | Shared-H100 assessment |
|---|---|---|---:|---:|---|
| GPT-OSS-120B | 116.829B / about 5.1B | native MXFP4 experts, BF16 remaining tensors | 65,248,893,184 | 60.768 | Best first TP4 candidate; 15.192 GiB/rank before runtime overhead |
| Qwen3-235B-A22B | 235.094B / 22B | BF16 | 470,191,875,096 | 437.900 | Does not fit |
| Qwen3-235B-A22B | same | FP8 | 239,044,149,304 | 222.627 | Does not fit |
| Qwen3-235B-A22B | same | official GPTQ INT4 | 124,548,922,936 | 115.995 | Does not fit in roughly 80 GiB available across four ranks |
| Qwen3.5-122B-A10B | LM 122B / 10B; full checkpoint 125.086B | BF16 | 250,173,250,864 | 232.992 | Does not fit |
| Qwen3.5-122B-A10B | same | FP8 | 127,163,011,832 | 118.430 | Does not fit |
| Qwen3.5-122B-A10B | same | official GPTQ INT4 | 78,864,326,320 | 73.448 | Very tight even before workspace/KV; requires a different runtime version |

The Qwen3.5 model is real: its official card describes 122B language-model parameters, 10B activated, and native 262,144-token context. The full published multimodal checkpoint has extra tensors; do not confuse its 125.086B metadata count with the language-only 122B description. Its official GPTQ card also offers `--language-model-only`, which skips the vision encoder, but the resulting resident footprint has not been measured here. Qwen3-235B has native 32,768 context, extendable to 131,072 with YaRN. [Qwen3.5 model](https://huggingface.co/Qwen/Qwen3.5-122B-A10B), [Qwen3.5 official INT4 deployment](https://huggingface.co/Qwen/Qwen3.5-122B-A10B-GPTQ-Int4), [Qwen3 official INT4 card](https://huggingface.co/Qwen/Qwen3-235B-A22B-GPTQ-Int4).

Pinned metadata revisions, in the table's order:

| Repository | Revision |
|---|---|
| `openai/gpt-oss-120b` | `b5c939de8f754692c1647ca79fbf85e8c1e70f8a` |
| `Qwen/Qwen3-235B-A22B` | `8efa61729e24bd65b1d152b5ab5409052aa80e65` |
| `Qwen/Qwen3-235B-A22B-FP8` | `39eb2b067ea6b8e3e1dd97d3cd0c7ffeaf3e1a35` |
| `Qwen/Qwen3-235B-A22B-GPTQ-Int4` | `dff138951f3898772fc0d795a4b5b30f417577f4` |
| `Qwen/Qwen3.5-122B-A10B` | `dc4d348443bc740c68e2d77492492c11606384d5` |
| `Qwen/Qwen3.5-122B-A10B-FP8` | `a099dee70ccfcd8d5dda56aaa0b60cb8ecadabc9` |
| `Qwen/Qwen3.5-122B-A10B-GPTQ-Int4` | `30cd92cba9707a9aba09d1e490ed4b66b78e9606` |

The read-only size calculation is reproducible with `GET https://huggingface.co/api/models/{repository}/revision/{revision}?blobs=true`, summing `siblings[*].size` where `rfilename` ends with `.safetensors` and contains no `/`. [GPT-OSS pinned files](https://huggingface.co/openai/gpt-oss-120b/tree/b5c939de8f754692c1647ca79fbf85e8c1e70f8a), [Qwen3 INT4 pinned files](https://huggingface.co/Qwen/Qwen3-235B-A22B-GPTQ-Int4/tree/dff138951f3898772fc0d795a4b5b30f417577f4), [Qwen3.5 INT4 pinned files](https://huggingface.co/Qwen/Qwen3.5-122B-A10B-GPTQ-Int4/tree/30cd92cba9707a9aba09d1e490ed4b66b78e9606).

## Current vLLM 0.10.2 and GPU compatibility

GPT-OSS is registered in the fixed v0.10.2 source. Its MXFP4 implementation has an SM90 FlashInfer/BF16 path, a Triton path when the package is present with Torch >=2.8, and a Marlin fallback. This is more specific evidence than applying a current-version installation recipe to the existing environment. H100 SM90 is the first target. Do not force newer-version environment flags such as `VLLM_MXFP4_USE_MARLIN` without checking that they exist in 0.10.2. [Model registration](https://github.com/vllm-project/vllm/blob/v0.10.2/vllm/model_executor/models/registry.py#L81), [fixed-version MXFP4 backend selection](https://github.com/vllm-project/vllm/blob/v0.10.2/vllm/model_executor/layers/quantization/mxfp4.py#L48-L116).

TP4 is structurally plausible: 64 attention heads become 16/rank, eight KV heads become two/rank, 2,880 expert intermediate elements divide by four, and the loader explicitly handles 32-element MXFP4 block rounding and per-rank slices. The quantization backend also pads dimensions. The loader temporarily moves full source tensors to CUDA before slicing, and swizzling creates transient allocations; neither the 15.192 GiB average nor `--gpu-memory-utilization` proves startup fits. [Attention/TP implementation](https://github.com/vllm-project/vllm/blob/v0.10.2/vllm/model_executor/models/gpt_oss.py#L69-L110), [MXFP4 loading](https://github.com/vllm-project/vllm/blob/v0.10.2/vllm/model_executor/models/gpt_oss.py#L257-L308).

At context 18,440 and four sequences, a conservative BF16 KV bound treating all 36 layers as full attention is `2 × 36 × 2 KV heads/rank × 64 head_dim × 2 bytes × 18,440 × 4 = 1,359,544,320 bytes`, or 1.266 GiB/rank. Half the layers actually use a 128-token window. With the measured 19.115-GiB/rank 0.24 budget, average checkpoint plus that KV bound leaves only about 2.66 GiB/rank for padding, activation/workspace, CUDA and transient allocations. This arithmetic is a planning bound, not a capacity measurement. [Published configuration](https://huggingface.co/openai/gpt-oss-120b/blob/b5c939de8f754692c1647ca79fbf85e8c1e70f8a/config.json).

Qwen3 MoE/GPTQ is supported by the current model family, but its INT4 memory floor excludes it from the first fully resident experiment. Qwen3.5 uses a different hybrid Gated DeltaNet architecture; its publisher asks for a newer/main-branch vLLM in a fresh environment. It is **not** a model-name substitution inside this 0.10.2 environment. A newer vLLM recipe lists minimum 0.17.0 for the Qwen3.5 MoE family; that is not evidence that every GPTQ/SM120 combination is validated. [Current Qwen3 registry](https://docs.vllm.ai/en/v0.10.2/models/supported_models.html), [publisher's Qwen3.5 requirements](https://huggingface.co/Qwen/Qwen3.5-122B-A10B-GPTQ-Int4#serving), [vLLM Qwen3.5 recipe](https://github.com/vllm-project/recipes/blob/main/models/Qwen/Qwen3.5-397B-A17B.yaml).

The second Pod exposes sixteen roughly 23.63-GiB MIG instances on six physical RTX PRO 6000 Blackwell parents. Those are not sixteen independent full GPUs. The existing app-local MIG platform intentionally supports TP1/DP1; its successful BF16 7B inference does not validate MXFP4 kernels or distributed collectives. In 0.10.2, the generic capability >=9 Triton selection does not distinguish SM120 from SM90/SM100. Later fixed-version code explicitly documents SM120 Triton limitations. SM120 native low-bit support must therefore be tested independently in an isolated runtime, not inferred from the word “Blackwell.” [0.10.2 selection](https://github.com/vllm-project/vllm/blob/v0.10.2/vllm/model_executor/layers/quantization/mxfp4.py#L82-L90), [later compatibility guard and explanation](https://docs.vllm.ai/en/v0.14.1/api/vllm/model_executor/layers/quantization/mxfp4/).

There is a version boundary in NVIDIA's own documentation: the MIG guide says NCCL is unsupported for the older stack, whereas **NCCL >=2.31 adds experimental MIG communicators and requires CUDA driver >=13.0**, with MNNVL disabled. That does not make the current Torch2.8/CUDA12.x stack or app plugin TP-capable. Repartitioning the shared physical GPUs is outside this plan. [MIG limitations](https://docs.nvidia.com/datacenter/tesla/mig-user-guide/latest/deployment-considerations.html), [new experimental NCCL support](https://docs.nvidia.com/deeplearning/nccl/user-guide/docs/usage/communicators.html#using-mig-instances).

## Initial execution plan and measured results

The numbered plan below records the initial rollout constraints. Subsequent
measured tuning is documented separately in [100B validation](100b-validation.md);
use the canonical live manifest for the active configuration.

1. Keep the existing secondary 7B fleet serving. Drain only the four application-owned primary 7B workers/engines in a released maintenance window, retaining the exact original configs for rollback. Do not stop other tenants' processes.
2. Verify free memory on **each** selected H100 after the owned engines exit, plus CPU/cgroup RAM, disk quota, topology, NCCL version and a small collective. Four roughly 20-GiB budgets are not interchangeable with one dedicated 80-GiB card.
3. Download only pinned root Transformers shards/config/tokenizer, not the whole repository's alternative `original/` and `metal/` layouts. Maintain the existing 0.24 fraction of total GPU memory; do not claim ownership of the full cards.
4. First bounded config: TP4, BF16 activation dtype, eager execution, max context 18,440, max sequences <=4, max batched tokens 2,048, explicit chunked prefill, prefix caching off. Native model context is 131,072, but that is not an appropriate initial shared-memory reservation. Start with one request before increasing concurrency; a bounded high-context requirement still needs its KV capacity proven by startup logs.
5. Require readiness, one real generated answer, a long-context request, a simultaneous-request probe, and GPU memory measurements across all four ranks before declaring the model served. GPT-OSS uses Harmony and reasoning channels; verify final-answer extraction/JSON behavior separately rather than carrying over Qwen assumptions. No tasks/day or latency estimate is established by the weight calculation.

CPU offload is a fallback, not free capacity. vLLM's `--cpu-offload-gb` is **weight** offload per GPU, with host-to-device transfers on each forward; it is not the same as `--swap-space` for KV/cache-related host allocation. Quantized GPT-OSS loading/offload combinations remain unverified here, so no manager offload switch is enabled. [v0.10.2 flag semantics](https://docs.vllm.ai/en/v0.10.2/cli/serve.html#cpu-offload-gb).

The process manager now records kernel-observed descendants while the managed leader is alive. After leader loss it validates each recorded PID/start-time/process-group/session/cwd identity and signals through Linux pidfds, without enumerating unrelated processes or signaling a stale process group. A real Linux regression killed an owned leader, reaped its TERM-resistant child, and preserved an unrelated process ([evidence](../ops/reports/engine-members-linux-regression-2026-10-24.json)). Unobserved or escaped-session descendants are deliberately not guessed at; every GPU rank still must pass the free-memory gate before restart. This CPU process regression is separate from a real distributed inference recovery drill.

A separate **llama.cpp CPU-expert-offload** lane provides >=100B execution on one MIG. The runtime maintainer publishes GPT-OSS-120B CPU-MoE examples and an official runtime conversion repository. It keeps experts in CPU RAM and executes selected experts there, rather than requiring all quantized weights to fit in a single 24-GiB MIG. The restart gate requires checkpoint file bytes plus an additional 80 GiB available in both host and visible cgroup memory accounting; this is a conservative admission rule, not a measured minimum. Only one replica is admitted, avoiding sixteen copies of the host-memory/CPU workload. The pinned executable compiled for SM120, the complete GGUF checksum passed ([asset evidence](../ops/reports/pod2-gptoss120b-ready-assets-2026-10-24.json)), and separate natural and long-context inference subsequently passed ([measured results](100b-validation.md)). [Maintainer's CPU offload guide](https://github.com/ggml-org/llama.cpp/discussions/15396), [runtime-maintained GGUF conversion](https://huggingface.co/ggml-org/gpt-oss-120b-GGUF).

For that isolated preparation, the runtime-maintained GGUF revision is `7200b74bea78b0a2964ef9f3897eefa401e890ff`; download only `gpt-oss-120b-MXFP4.gguf` (63,387,346,208 bytes; SHA256 `582bd40f6886200101f4c4ed9f25f3fe80cc14c86e9e2b37746cd8904a0c622d`), not its optional Eagle3 drafts. An inspected runtime source pin is `ggml-org/llama.cpp@0ae0e1976670fac25f4e0ed864df30021accc764` (2026-10-23). A build or download alone establishes neither SM120/MIG compatibility nor successful inference.


The initial 2,048-token prefill chunk exhausted the configured KV budget after
weights loaded. Reducing the chunk to 1,024, while keeping memory fraction 0.24,
TP4, context 18,440 and one sequence, produced a healthy engine. Actual resident
weights were about 17.22 GiB per rank, exceeding the checkpoint-bytes/4 estimate.
That initial runtime reported 1.36 simultaneous full-context requests and
therefore admitted one slot. Later 0.26-fraction configurations passed four and
eight actual simultaneous 16K-input / 2K-output requests. The raw hybrid KV-token count must
not be interpreted as a full-attention context limit.

A natural arithmetic request returned final answer `42` with 82 input and 47
generated tokens in 1.258 seconds. The public-text long request returned
**16,384 measured input + 2,048 generated tokens** in **38.445 seconds**, with
final content present and `finish_reason=length`. Neither used forced minimum
output or ignored EOS. These are individual engine requests, not a sustained
throughput or answer-quality evaluation. [Exact natural/long-context proof](../ops/reports/primary-gptoss120-natural-long-2026-10-24.json),
[successful startup](../ops/reports/primary-gptoss120-startup-batch1024-2026-10-24.json),
[retained failed first startup](../ops/reports/primary-gptoss120-startup-batch2048-failed-2026-10-24.json).

The secondary GGUF has the same 116,829,156,672 logical parameters, with MXFP4
experts, Q8_0 non-expert tensors and F32 auxiliary tensors. It is not bit-identical
to the primary HF checkpoint's precision. Its separate llama.cpp backend initially kept
35 of 36 MoE layers on CPU; measured tuning moved seven more layers to the same
MIG, leaving 28 on CPU and eight on the selected GPU slice. This
this avoids claiming unsupported tensor parallelism across the existing MIG
stack. Cross-node recovery validates durable work continuity across these
implementations, not identical token sequences or identical model quality.
