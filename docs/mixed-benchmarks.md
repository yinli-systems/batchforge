# Token mixtures, dispatch policies and larger-model validation

`scripts/token_workload.py` builds ordinary batch JSONL from real SQuAD passages.
It uses an existing local tokenizer, includes the model's chat template and
generation prefix, and verifies each input length. Its default matrix has inputs
128/512/2,048/8,192/16,384 and output caps 32/128/512/2,048. The largest request
needs at least 18,440 context tokens with the default fitting tolerance. Exact
source bytes, tokenizer revision, chat-template hash and per-request rendered
counts are preserved. No remote tokenizer code is enabled.

Different context capacities use different queue identities. The explicit
`Qwen/Qwen2.5-7B-Instruct-18k` serving alias uses the same pinned 7B weights but
an 18,440-token engine. It prevents a 16K-input request from being claimed by a
4,096-token engine. `MODEL_NAMES_JSON` adds operator-approved model IDs to the
default `MODEL_NAME`; each batch contains one model. Batch rows, task rows,
tenant queues and retries retain the selected ID. A worker cannot turn an
unavailable 120B request into a 7B request. The catalog lists configured models;
worker readiness still determines actual available capacity.

Create a matrix inside the engine environment, where the pinned tokenizer is
already present:

```sh
python -m scripts.token_workload --dataset /private/path/dev-v1.1.json \
  --tokenizer /private/path/pinned-model --revision FULL_MODEL_REVISION \
  --model Qwen/Qwen2.5-7B-Instruct-18k --context-limit 18440 \
  --repeats 1 --output .runtime/mixed.jsonl --manifest .runtime/mixed-manifest.json
```

`scripts/engine_matrix.py` measures streaming engine HTTP independently of
durable platform results. Use the same source bytes and manifest on both nodes.
Warm up the selected engine before the measured run. Ensure prefix caching is
off and preserve engine memory, chunked-prefill, sequence and software settings.
The concurrency must fit the **observed** KV capacity, reserving a full context
per slot. A configured context length does not prove its concurrent capacity.

```sh
python -m scripts.engine_matrix --url http://127.0.0.1:8215 \
  --workload .runtime/mixed.jsonl --manifest .runtime/mixed-manifest.json \
  --engine-label 'actual GPU UUID, model format and runtime' \
  --context-limit 18440 --concurrency 4 --policy fifo --force-length \
  --report .runtime/matrix-fifo.json
```

The three policies are bounded **client dispatch** of the same offered burst:
FIFO, shortest requested work (`prompt_tokens + 16 * output_cap`), and round
robin between prefill-heavy/decode-heavy/balanced classes. This does not change
vLLM's internal scheduler or the production SQL tenant fairness policy. Compare
them with the same concurrency and repeat runs before inferring a stable win.
Twenty samples per policy describe a small measured burst; they do not establish
a production P99 SLO.

`--force-length` is an explicit synthetic capacity control on real text and real
model inference: vLLM ignores EOS and requires the requested minimum token count.
Every actual output count must match the cap or that request fails measurement.
This mode does not measure natural completion quality. Without it, actual output
lengths may be shorter and are reported from usage. Server input counts must
match the tokenizer manifest. Missing usage, interrupted streams, HTTP errors,
hash mismatch and insufficient context never count as success. There is one
inference request per input, with no replay after an uncertain outcome.

Reports include measured prompt/generated tokens per wall second, HTTP TTFT,
stream-span / reported-output-token proxy, client queue delay and completion P50/P95/P99.
Successful completion percentiles are accompanied by failure counts and the
all-terminal distribution. SSE data chunks are not individual tokens; stream intervals can be affected by
transport buffering and usage includes reasoning/control tokens. Schema 3 reports
first final-content arrival and final/reasoning character counts separately, so
a reasoning-only capped response is not mistaken for a useful final answer.
Earlier schema 1 reports use the historical field `accepted_tasks_per_second`
for successful engine requests, not accepted database completions. Class throughput uses the
whole shared run window and must not be summed with separately measured peaks.

SM utilization must come from device/process telemetry. Primary H100 device
utilization includes unrelated workloads; only owned-process measurements can
be attributed to BatchForge. MIG telemetry may be unavailable without host
permissions. Engine request counts and KV-cache occupancy are useful separate
metrics, but do not substitute for GPU SM utilization. If `--hourly-cost` is
provided, it is an explicit operator-supplied allocation cost over the measured
window, including idle and failed work. A whole-Pod price is not the marginal
price of one MIG slice.

Durable batch performance is still measured with `scripts/benchmark.py` and
`scripts/report_run.py`. Those include upload, queueing, leases and accepted
database commits; engine-only throughput cannot replace them. Engine failure,
capacity removal and cross-node recovery require their own exact task/attempt
evidence. The earlier API-outage experiment is not a GPU-hardware-failure test.

The [100B assessment](100b-model-feasibility.md) explains the selected model's
total versus active parameters, pinned weights, serving compatibility and memory
gates. A downloaded checkpoint is not a successful deployment. The failed
[JSON experiment](structured-output.md) remains a separate quality limitation.


For GPT-OSS on vLLM 0.10.2, use `scripts/gptoss_prompt_counter.py` with CUDA hidden.
The chat endpoint automatically renders Harmony through vLLM's own system and
message helpers; the generic `/tokenize` chat endpoint is not an equivalent
oracle. The counter records the engine helper hashes, Harmony version, local
render date, model revision and per-request token-ID digest. Actual response
usage must still match. The separate llama.cpp backend requires its own applied
chat template/tokenizer check; do not reuse a vLLM Harmony count manifest.

`scripts/llama_prompt_counter.py` reads the live server's `/props` template,
formats plain text messages through `/apply-template`, and counts `/tokenize`
with `add_special=false, parse_special=true`. Calibration must match a real
response's input usage. Record the full runtime/model revisions and independently
verified binary/model hashes; supplied hashes alone do not attest the server.
The natural probe disables prompt reuse and requires `cached_tokens=0`. Its
exclusive `.started` marker prevents an uncertain CLI outcome being replayed
under the same report name. Per-request formatter overrides are rejected.

The llama.cpp matrix sets `cache_prompt=false`. The frozen streaming harness
does not capture cached-token details, so its single-slot exclusive FIFO run
additionally matches every appended server timing pair, in dispatch order, to
the full input count in the manifest and returned usage. Missing, duplicate or
partial timing pairs invalidate that evidence. Server prefill/decode durations
include CPU/GPU execution; they are not isolated CUDA kernel measurements.
Fixed-length support is separately calibrated against actual generated usage;
do not assume that every runtime implements the same request fields as vLLM.

For reasoning models, also run ordinary requests without forced lengths.
`scripts/public_workload.py` evaluator version 2 counts reasoning-only responses
as failed answers and rejects duplicate JSON keys and non-finite JSON constants.
The eight-question omitted-versus-low reasoning pilot is a paired diagnostic,
not an official SQuAD score or a production quality guarantee. Keep its raw
responses private and publish aggregates and source hashes.
