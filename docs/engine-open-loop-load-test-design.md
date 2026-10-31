# Open-loop load test for the Qwen235B engine: design

Status: design only, not run. Target: the private engine on `127.0.0.1:8300`
(TP4 on four H100 80GB, `--max-num-seqs 64`, `--max-model-len 18440`,
`--enforce-eager`, chunked prefill, prefix caching disabled).

## 1. Why this test, and what the baseline does not show

The 2026-10-25 baseline was a **closed-loop** run: 130 requests with a fixed
concurrency of 16 or 32. Each worker waits for its previous request before
sending the next, so the offered load adapts to the server. It cannot show
saturation:

| Concurrency | Req/s | Output tok/s | p50 | p95 |
|---|---|---|---|---|
| 16 | 8.4 | 172 | 2.3 s | 4.3 s |
| 32 | 15.6 | 320 | 2.3 s | 4.3 s |

Latency stayed flat from 16 to 32 clients, which means the engine was far from
its limit at that load and the test had no queueing. It also used only short
outputs (≤200 tokens) and short prompts. Real batch traffic is different: long
prompts, long outputs, bursts, and sustained arrivals.

An open-loop test sends requests at a **target arrival rate** that does not
depend on completions. When the offered rate exceeds capacity, the queue grows,
and that growth is what the test measures.

## 2. Questions the test must answer

1. What is the sustainable request rate and output-token rate for the workload
   mix below, and where does the queue start to grow?
2. How do TTFT, per-token latency and end-to-end latency behave below, at, and
   above that point?
3. Does the engine keep producing valid outputs under sustained load?
4. How much GPU KV memory is used at each load level, and does it approach the
   point where requests wait for cache?
5. Goodput: how much work finishes within its deadline, not just how much
   finishes.

## 3. Workload

Three request classes, drawn from a fixed-seed generator so every run sees the
same items in the same order:

| Class | Share | Input tokens | Output cap | Template |
|---|---|---|---|---|
| Short classify | 45% | 40–120 | 8 | sentiment |
| Medium summary | 30% | 800–2,500 | 160 | summary |
| Long extract | 25% | 6,000–16,000 | 200 | extract (JSON object) |

Notes:

- The long class exists because `max_model_len` is 18440 and batch users will
  submit long documents. The baseline had no inputs above a few hundred tokens.
- Input lengths come from real-looking, varied text (concatenated sentences
  from a pool, not one repeated passage). Prefix caching is disabled on this
  engine, so repetition would not produce cache hits, but a repeated passage
  still makes the workload unrealistic and must be avoided.
- Inputs are pre-tokenized with the engine's own tokenizer
  (`/tokenize`), so the requested lengths are exact, not estimated.
- **Labels must be unambiguous.** The baseline sentiment score of 90% was
  partly label noise (30% of items concatenated two sentiments). Sentiment
  items here carry one polarity only. Extraction checks use the stated amount
  and date, normalized as in the baseline scorer (`re.sub(r"[^0-9.]", …)`).
- Quality is checked on a 5% sample of completed requests, at every load step,
  so quality under load is measured, not assumed.

## 4. Arrival process

- **Poisson arrivals** with a fixed seed. Inter-arrival times are drawn ahead of
  time and written to a schedule file, so the schedule is identical across runs.
- Each **step** has a fixed target request rate λ. Rates are expressed as a
  fraction of a **calibrated capacity** C, not as absolute numbers, so the test
  stays meaningful after the engine or model changes.
- Calibration (step 0): ramp λ from 0.5 to 2.0 req/s in steps, 3 minutes each,
  until the p95 end-to-end latency for the short class exceeds 10 s or the
  queue depth stays above 20 for a minute. C is the last rate that met both
  conditions, minus 10%.
- Measurement steps: λ = 0.5C, 0.75C, 0.9C, 1.0C, 1.1C.

Each step: **5 min warm-up (discarded), 25 min measurement, 5 min drain**.
Total: about 35 min per step, about 3.5 hours for the five steps plus
calibration. Repeat the whole sequence once more on a different day before
trusting any number.

## 5. Client design (the part that usually makes load tests lie)

- **Open loop.** The sender schedules each request at its planned send time and
  does not wait for responses before sending the next one.
- **Bounded in-flight.** Cap in-flight requests at 512. If the cap is hit, the
  request is **counted as dropped**, never silently skipped. A dropped request
  is a failed request for goodput.
- **Client lag is measured.** For each request, record `actual_send − planned_send`.
  If the p99 lag exceeds 50 ms, the client is the bottleneck and the step is
  invalid. Run the client on a separate machine if possible; on the pod it
  shares CPU with the engine's API process.
- **Timeouts.** Per-request timeout 900 s (longer than any SLO). Timeouts count
  as failures.
- **Records.** One line per request: class, planned and actual send time,
  time-to-first-token (streaming enabled for this test only), completion time,
  prompt and completion tokens, HTTP status, the first 200 characters of output
  (for the quality sample), and the request's index in the schedule.

## 6. Metrics

Sampled every 5 s from the engine's `/metrics` endpoint and nvidia-smi:

- `vllm:num_requests_running`, `vllm:num_requests_waiting`
- `vllm:kv_cache_usage_perc` (or the version's equivalent name, checked at setup)
- `vllm:request_queue_time_seconds`, `vllm:time_to_first_token_seconds`,
  `vllm:time_per_output_token_seconds` (histogram deltas per step)
- GPU utilization and memory per device

Per step, computed from the request records:

- Offered rate, completed rate, dropped count, error count
- Output tok/s and prompt tok/s
- p50/p95/p99 end-to-end latency, per class
- TTFT p50/p95 per class
- **Goodput**: completed requests within their deadline, per class. Deadlines:
  short 10 s, medium 60 s, long 180 s. These are proposals; the product owner
  must set them before the test.
- Quality on the 5% sample: format validity and label correctness

## 7. Stability and pass criteria

A step is **stable** if all of these hold over the measurement window:

1. The queue is not growing: the slope of `num_requests_waiting` over the last
   10 minutes is within ±0.5 requests/min.
2. Completed rate is within 2% of offered rate.
3. Error rate (HTTP ≥ 400 or timeout) is below 0.1%.
4. Client lag p99 below 50 ms.
5. p99 end-to-end latency for each class is below its deadline.

The **sustainable rate** is the highest stable step. Report it with the step
below it and the first unstable step, so the knee is visible.

## 8. Outputs and reproducibility

Every run writes a directory containing:

- `config.json`: seed, workload mix, λ values, deadlines, durations, engine
  arguments, model revision, the test script's git revision, and a hash of
  the schedule file.
- `schedule.jsonl`: every planned request with its class and planned time.
- `requests.jsonl`: one record per request, as in §5.
- `metrics.csv`: the 5-second samples from §6.
- `summary.json`: the per-step table, the knee, and the quality sample.
- `report.md`: the one-page result, with charts generated from the CSVs.

Raw files stay on the pod. Only the summary and report leave it.

## 9. What this test does not cover

- **BatchForge.** This measures the engine directly. Batch orchestration,
  leases, retries and the database are excluded. A second test through the
  BatchForge API is needed once the database is restored; its arrival unit is
  a job (a batch of N items), not a single request.
- **Failures.** Engine crash, GPU reset and whole-pod loss are separate tests.
  Only a controlled engine restart is in scope for the first run, and only
  after the baseline steps are complete.
- **Multi-tenant fairness.** Single tenant here. Fairness belongs to the
  scheduler A/B test, which needs BatchForge.
- **Long duration.** A 24-hour soak is planned only after this test is stable
  and reproducible.

## 10. Risks and safeguards

- **Pod shared with other work.** Before the run, confirm no other GPU job is
  running (the `/root/h3` incident showed this can change). Abort the run if
  GPU memory use by non-engine processes appears.
- **KV exhaustion.** Long prompts at high rate can fill the KV cache and stall
  the engine. The stop rule: if `num_requests_waiting` exceeds 200, end the
  step early and mark it unstable. Do not let it run to the end.
- **Engine state.** Restarting the engine takes minutes (model load plus CUDA
  graph/eager warm-up). The test should not start until the engine has been
  idle for 10 minutes and `/v1/models` is healthy.
- **Disk.** Streaming records for 130,000 requests per run need about 200 MB;
  check free space first.

## 11. Order of work

1. Build the schedule and the workload generator, with a dry run that tokenizes
   inputs and checks the length buckets. No GPU load.
2. Run calibration (§4) and record C.
3. Run the five measurement steps in order, one at a time.
4. Write the report. Do not rerun a step to get a better number; if a step is
   invalid, mark it invalid and say why.
5. Repeat on a second day to check reproducibility.
