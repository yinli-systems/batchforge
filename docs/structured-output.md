# Structured output: acceptance, stopping and observed cost

For an explicit `response_format={"type":"json_object"}` request, a worker only
accepts a strict top-level JSON object. It rejects fences, malformed syntax,
duplicate keys, non-finite numbers and missing content. Rejection follows the
normal durable failure/retry/fencing protocol; it does not regenerate inside
one attempt. A JSON object alone does not guarantee required fields, translation
fidelity or correct answers. Rejected response usage is excluded from accepted
task/batch usage; process counters separately observe rejected compute.

The original 2,048-task translation load accepted 1,872 results and ended with
176 failures. Its 666 rejected responses consumed an observed 335,734 prompt and
337,090 generated tokens. Mean rejected generation was about 506 tokens, below
the 1,024-token cap. Treating every failure as a length limit would be wrong.
The isolated worker window also recorded 450 origin failovers, and the secondary
observer recorded 25 failed metric scrapes and high failure-report latency.
These performance findings are retained independently of output quality.

## A concrete stopping-set mismatch

The pinned Qwen2.5-7B tokenizer uses EOS 151645. Its original generation config
also lists 151643 (`<|endoftext|>`) as EOS. In vLLM 0.10.2, extra generation EOS
IDs become sampling stop IDs, while the HF xgrammar backend does not pass that
expanded set to xgrammar. xgrammar 0.1.23 defaults to the tokenizer's single EOS.
The scheduler stops on an extra stop ID without checking grammar termination.
See the fixed-version [backend construction](https://github.com/vllm-project/vllm/blob/v0.10.2/vllm/v1/structured_output/backend_xgrammar.py#L68-L72),
[xgrammar default](https://github.com/mlc-ai/xgrammar/blob/v0.1.23/python/xgrammar/tokenizer_info.py#L198-L223),
[sampling stop update](https://github.com/vllm-project/vllm/blob/v0.10.2/vllm/sampling_params.py#L423-L445)
and [scheduler stopping](https://github.com/vllm-project/vllm/blob/v0.10.2/vllm/v1/core/sched/utils.py#L52-L63).

A CPU-only experiment with the real pinned tokenizer reproduced the mismatch.
After the prefix `{"translation_zh":"`, the default grammar's mask allows
151643 and accepting it leaves the matcher unterminated. Supplying both EOS IDs
to the grammar correctly forbids it in that state. String-prefix and tokenized
prefix paths agreed, with CUDA hidden and never initialized. This proves the
synthetic mechanism, rather than explaining every failed production sample.
The [report includes the exact executable snippet](../ops/reports/controlplane-eos-cpu-mask-2026-10-24.json).

An initial same-GPU control using eight original pilot inputs and 32 sorted
public IDs from actually failed tasks produced 11/40 strict translation objects.
Of the 29 invalid outputs, 18 naturally stopped with `stop_reason=151643`, six
hit the 1,024-token cap and five had other natural-stop behavior. The classes
must be assessed separately; the stopping-set mismatch is not a universal
explanation of model errors.

The version-specific supported configuration path is a separate, complete
`generation_config.json` directory, selected with `ENGINE_GENERATION_CONFIG`.
The candidate changes only EOS to `[151645]`; weights, tokenizer and other
sampling defaults remain unchanged. The original model revision directory is
never edited. In 0.10.2, an EOS entry in `--override-generation-config` does not
reach the relevant stop-loading path, and `--generation-config vllm` still reads
model EOS for stopping. These are not equivalent substitutes. See the actual
[versioned implementation](https://github.com/vllm-project/vllm/blob/v0.10.2/vllm/config/__init__.py#L1500-L1573).

Promotion requires measured improvement on paired identical requests, separate
classification of cap/field failures, no regression in previously valid cases,
and another full public-corpus run. Syntax repair, hidden output rewriting and
changed token caps must not be mixed into the EOS-only comparison.

The paired candidate did **not** pass that gate. Strict objects rose from 11/40
to 13/40, but generated tokens rose from 19,695 to 30,272 (53.7%). Of the eighteen
old extra-EOS failures, two became valid and sixteen hit the output cap. All
eleven previously valid outputs stayed valid. Length-limited outputs rose from
six to twenty-two. The five other natural-stop cases contained duplicate keys;
the strict guard correctly rejected them. The GPU0 controller restored the
original engine configuration and watchdog, preserving other service identities.
See the [paired report](../ops/reports/primary-json-eos-paired40-2026-10-24.json).

A separate eleven-request diagnostic increased the cap to 2,048 for the six old
length cases and five duplicate-key cases. It produced only one strict object;
six responses again hit the cap. Increasing output budget alone is therefore
not a demonstrated remedy. No fleet-wide EOS change was released. The acceptance
guard and bounded retries remain necessary, and the original failed corpus
results remain failed measurements.
