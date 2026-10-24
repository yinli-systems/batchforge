# Bounded sustained arrival experiments

`scripts/sustained_benchmark.py` submits real inference tasks through the existing
private API. It does not provision resources, change policy, cancel tasks,
invoke archive/purge, delete history, or alter quotas. An operator must choose a
finite task count, admission duration, arrival rate, cohort size and in-flight
limit before launching it. The default admission window is 30 minutes, followed
by at most 20 minutes of drain; accepted tasks continue on the platform if the
observer stops.

## Proposed first 235B experiment

Use the existing frozen **20-row** fixture containing the full 5×4 input/output
grid. Every 20-row cycle is a seeded permutation, so each cohort contains every
fixture cell once. Input token counts are measured from actual accepted usage;
the runner does not infer token count from text length. `--fixed-output` sets
`min_tokens` to each row's requested output cap and verifies every completed
task's actual completion tokens. This is a controlled compute experiment, not an
answer-quality benchmark.

The current 320 MiB payload quota has approximately 129 MiB free. Keep the
in-flight bound at **80 tasks**, cohort size 20, offered rate 0.4 tasks/second,
720 planned tasks, and admission duration 1,800 seconds. Backpressure and the
background publisher's cleanup delay are part of the experiment; do not loosen
quotas to force the run through. The scheduled last cohort arrives at 1,750
seconds. Actual completion may need the drain window. A partial plan remains a
failed full-plan result with its accepted/completed counts preserved.

Run only on the inference host, detached using the existing process manager or
an operator-owned `nohup` process. Environment and fixture paths below are
placeholders for existing owner-only files; secrets are never command arguments:

```sh
PYTHONPATH=. /root/batchforge-runtime/venv/bin/python -m scripts.sustained_benchmark \
  --job-id qwen235b-soak30m-v1 \
  --env-file .runtime/scheduler-v6/benchmark.env \
  --input-jsonl .runtime/scheduler-v6/frozen-grid20.jsonl \
  --model Qwen/Qwen3-235B-A22B-Instruct-2507-FP8 \
  --state-dir .runtime/sustained/qwen235b-soak30m-v1 \
  --tasks 720 --tasks-per-batch 20 --seed 20261024 --fixed-output \
  --arrival-rate 0.4 --duration-seconds 1800 --drain-seconds 900 \
  --max-inflight-tasks 80 --submission-concurrency 2 \
  --poll-seconds 2 --sample-seconds 10 --hourly-cost 16.10
```

The $16.10/hour value is an explicit whole-Pod allocation assumption, not an
invoice or a current provider quote. Allocation includes idle time, backoff,
drain, validation, and any disconnect gap. It is not marginal inference cost.
Optional `--api api-a=http://127.0.0.1:8082` and the corresponding api-b endpoint
enable API/recovery histogram observations only when the private environment
also contains `BATCHFORGE_WORKER_TOKEN`. The token is never written to evidence.

## Identity, recovery and request bounds

Each job binds its ID, exact source SHA256, seed, plan, rate, all bounds, model,
local endpoints, runner source SHA256 and the benchmark/observer dependency
SHA256s. Restart with the **identical command and code**. Changing code or an
immutable parameter requires a new job ID. A controller lock prevents two
writers to the same state directory. It does not coordinate multiple machines;
the operator must run only one controller for a job.

Owner-only JSONL journal entries are fsynced before atomic checkpoint updates.
Every submission records its stable key before upload. The key has the form
`bf1.<persisted job epoch>.<32 deterministic hex characters>` and has a 24-hour
recovery horizon. Before new submissions and on restart, a read-only database
query reconciles batches by tenant, job, index and immutable input SHA256. A
duplicate batch, changed identity, missing accepted batch, revoked/wrong tenant
key, or conflicting source fails closed.

Create requests rejected with 429 or returning an uncertain timeout/5xx are
retried with bounded full jitter, the **same file, body and idempotency key**.
The controller never creates a replacement key for a retry. An upload timeout,
5xx, malformed acknowledgement, or interruption between upload send and durable
file-ID journaling has an uncertain non-idempotent outcome. That condition
fails closed and preserves the journal for operator inspection; it never
blindly uploads another copy. A confirmed 429 upload rejection can be retried.

There is one narrower additional safe retry: the released loopback gateway at
exactly `http://127.0.0.1:8080` can prove it rejected an upload **before selecting
or sending to any upstream**. Before every attempt the runner fsyncs a fresh
32-hex upload nonce and request count, then sends the nonce in
`X-BatchForge-Upload-Attempt`. It retries only a 503 with all of:

- `X-BatchForge-Outcome: not-forwarded`;
- `X-BatchForge-Outcome-Version: 1`;
- exactly one `X-BatchForge-Upload-Attempt` matching that journaled nonce;
- JSON error code `gateway_not_forwarded`, stage `select_upstream`, and reason
  `no_ready_upstream`.

The gateway emits this proof only for an empty ready-upstream set before any
send, and strips these reserved proof headers from every forwarded response.
Direct API origins, missing/duplicate proof fields, a different nonce/version,
upstream 503s, and transport failures do not establish non-forwarding. A timeout
or crash while `stage=uploading` still refuses automatic replay on resume,
including when an earlier attempt had a valid non-forwarding proof. A received
and durably recorded `upload_not_forwarded` stage can resume; each retry keeps
the same item, filename and eventual batch idempotency key. Its backoff is
bounded by the original persisted admission deadline.

`http_rejections.upload_not_forwarded` counts these proven routing rejections
separately from upload/create 429s and uncertain creates. Private journal events
and CLI failure metadata include an allowlisted reason code, stage and HTTP
status when available. Server bodies and arbitrary exception text are never
copied into diagnostics. A failed upload observer does not cancel accepted
tasks or turn them into failed tasks. Preserve that failed experiment; source
changes require fresh matched comparison runs rather than relabeling it as a
completed 30-minute candidate.

`max-inflight-tasks` counts accepted pending/running work plus local submissions
not yet observed in the database. The separate submission-concurrency bound
also covers creates waiting in backoff. Arrival times are cohort release times,
not an assertion that all tasks reached the API at that instant. Submission may
lag because of local admission and 429 responses; this lag is reported.

The admission deadline persists across restart and is not extended by a
disconnect. It stops new submissions; drain never cancels accepted work. A
request already in flight can take the configured request timeout, and the last
bounded backoff can delay process exit by up to 30 seconds. A torn journal is
preserved and refused, never silently truncated. Stale checkpoint temporary
files do not prevent recovery; each atomic update uses a new exclusive name.

## Evidence and interpretation

All state is under ignored `.runtime`, with a 0700 directory and 0600 files.
The runner records metadata, IDs, hashes, token counts, status counts and safe
error types, fixed reason codes and bounded HTTP status metadata only. It writes neither original prompts nor response text, and
never prints remote diagnostics, database connection strings, or tokens.
Raw fixtures remain private and are not copied into reports. Preserve the
journal, observations, checkpoint and versioned reports after a failure.

The report distinguishes:

- planned, durably accepted, terminal, completed, failed, cancelled, and
  unfinished tasks; completed attempt records and retries are separate counts;
- accepted completion **exactly once**, checked against task/attempt rows,
  unique expected custom IDs and matching accepted token usage;
- scheduled-arrival-to-finished latency, including client delay, upload, 429
  backoff, database queue and inference; database-created-to-finished latency is
  reported separately, as is scheduled-to-durable-admission delay;
- P50/P95/P99 use linear interpolation at `(n-1)*p`; completion percentiles
  include completed tasks only, so the accompanying failed/unfinished counts
  must always be read with them;
- actual accepted prompt/completion tokens, fixed-output checks, prefill-heavy,
  decode-heavy and balanced classes, and completion counts per 60-second window;
- per-device NVML utilization, aggregate engine running/waiting/KV gauges,
  cumulative prefill/generation token counters, durable queue, retention and
  auxiliary quota counters, and oldest terminal cohort awaiting purge.

GPU metrics include all device-visible work and do not measure Tensor Core
utilization. Engine counters describe aggregate stage activity, not exact
per-request prefill/decode boundaries. Sampling gaps, observed span, joined
sampling-segment span and time outside those segments are explicit. A segment
joins only samples at most twice the target interval apart; no missing data is
filled with zero, and snapshots never imply continuous observation. API HTTP
percentiles are Prometheus bucket bounds from counter deltas, distinct from
exact database task completion percentiles.

Client and database wall clocks must be synchronized for scheduled latency.
No daily capacity, multi-node recovery, fleet scheduling, quality improvement,
or continuous availability follows from a single finite arrival experiment.
Long-term task-history/idempotency retirement remains a separate storage
capability; this runner deliberately does not perform it.

## Validation

Focused tests cover a complete bounded loop, same-job restart without new POSTs,
balanced deterministic plans, actual output-length verification, HTTP 429 and
ambiguous-create replay identity, uncertain-upload refusal, durable journal
replay/locking, torn journal preservation, stale checkpoint recovery,
credential-safe telemetry, latency decomposition and missing-sample accounting.
Proof tests cover the bound gateway origin, exact nonce/version/error shape,
duplicates and unknown proofs, durable safe-rejection recovery, interruption
with an uncertain current attempt, and the persisted admission deadline.
An additional real PostgreSQL test creates a disposable schema10 database,
executes all observer queries and verifies the connection refuses writes.

```sh
PYTHONPATH=. .venv/bin/python -m pytest -q tests/test_sustained_benchmark.py
```

The real PostgreSQL test requires `TEST_DATABASE_URL` pointing at an isolated
disposable cluster. It never runs against production by default.
