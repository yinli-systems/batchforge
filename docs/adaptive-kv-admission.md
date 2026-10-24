# Exact-token worker admission

`WORKER_ADAPTIVE_KV_ENABLED` is **off by default**. The existing whole-context
reservation and `min(max_inflight, token_budget // context)` capacity remain the
default. The opt-in implementation is limited to one worker owning one private
engine URL for the exact model ID
`Qwen/Qwen3-235B-A22B-Instruct-2507-FP8`. GPT-OSS, multiple engine URLs and other
model names are refused. This is a worker admission bound, not a distributed or
physical GPU KV allocator.

With the feature enabled, the operator sets `WORKER_MAX_INFLIGHT` as the maximum
outstanding claimed tasks and `WORKER_TOKEN_BUDGET` as this worker's conservative
KV token budget. The operator must validate that budget against the actual
owned engine's measured cache capacity and block size. A possible experiment
is 64 outstanding claims and a 300,000-token budget against the observed
316,144-token Qwen cache, with a 16-token block size; those numbers are not a
default, a successful load test or permission to change a running engine.
Direct benchmark clients, profilers and another worker must not share the
engine during this mode's validation or use.

## Counting and reservation

At startup, the private engine must advertise only the exact model ID, report
vLLM `0.10.2`, return a valid `/tokenize` response with the configured context,
and report zero running/waiting requests. Only then may the worker register
`max_inflight` capacity. This check runs before the worker's first admission;
it does not certify that another future caller cannot use the engine.

Every claimed task starts its normal lease heartbeat before tokenization or
budget waiting. The worker sends the exact text messages to the same engine's
`/tokenize`, with `add_generation_prompt=true`,
`continue_final_message=false`, and `add_special_tokens=false`. There is no
byte heuristic, remote Hugging Face request, tokenizer installation or fallback
estimate. The response must contain consistent integer `count`/`tokens` and
the exact context bound. Token IDs remain in memory and are not logged.

For this ordinary Qwen chat path, both the
[chat endpoint](https://github.com/vllm-project/vllm/blob/v0.10.2/vllm/entrypoints/openai/serving_chat.py)
and [tokenization endpoint](https://github.com/vllm-project/vllm/blob/v0.10.2/vllm/entrypoints/openai/serving_tokenization.py)
call the same `_preprocess_chat` implementation with the same template
defaults. This applies only to the supported text message shape without tools,
template overrides or multimodal inputs. GPT-OSS takes a separate Harmony path
and is deliberately unsupported here.

The worker rejects `exact_prompt_count + requested_maximum_output > context`,
then reserves `ceil((prompt + maximum_output) / 16) * 16` tokens before starting
inference. A FIFO weighted limiter keeps the sum of its active reservations
within the operator's budget and prevents smaller requests indefinitely
bypassing an older waiter. FIFO can leave a small amount of headroom unused.
Budget waits are cancellable and share a bounded timeout with invocation once
an engine becomes eligible. Waiting claims retain heartbeats and still count
toward `max_inflight`; they are not GPU-running requests.

Before accepted completion, the actual response model and usage prompt count
must equal the model and exact tokenizer preflight. The normal output/context,
JSON-object and fenced-report checks still apply. Reservations are released
after the engine response, before potentially repeated durable reporting; an
ambiguous report never causes re-inference.

## Fail-closed uncertainty

Tokenization/protocol failure never admits a guessed request. An uncertain
inference transport failure, timeout, remote cancellation or tokenizer/usage
mismatch latches admission pause. Queued budget waiters are released with a
bounded retryable failure, new claims pause and worker readiness becomes false.
A successful health probe does not clear this state. Operator intervention is
required: stop the old owner, confirm its private engine is drained, then let
the replacement pass startup validation. A pause does not automatically reset
on completion of the remaining requests.

Local accounting is released on cancellation. That does **not** prove the
remote engine has synchronously freed KV: disconnect/abort propagation can lag.
The pause prevents reusing the newly released local budget for further requests
under that uncertainty. Worker restart is not a global allocation fence; sole
ownership and startup quiescence remain deployment requirements.

`batchforge_worker_reserved_context_tokens` reports active block-rounded
reservations in this mode. Additional gauges expose adaptive budget, waiters,
peak reservations and latched pause. `batchforge_worker_inflight` includes
claimed work waiting for tokens or durable reporting. Observe the engine's own
running/waiting/cache metrics separately. Neither worker accounting nor NVML
utilization establishes physical KV allocation or Tensor Core saturation.

Concurrency changes require a separately labelled experiment and matching
runtime profile. Earlier prefill/decode fits or per-slot allocation prices are
not automatically recalibrated by this feature. No throughput improvement is
claimed from implementation tests alone.
