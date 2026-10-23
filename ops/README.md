# Operating BatchForge

For the running two-Pod Qwen2.5-7B deployment, managed database, private console
and process watchdogs, use the current [operations runbook](../docs/operations.md).
The small-model container configuration below is a development example. It is
not the configuration of the measured 7B fleet.

The API is a control plane. A worker calls configured, separately managed
OpenAI-compatible model engines. Neither service falls back to generated fixture
responses. The `worker.fake_engine` service is explicitly test-only, refuses
normal startup without `BATCHFORGE_TEST_ENGINE=1`, and marks every result.

## Local containers

Create `.env` with strong, distinct `POSTGRES_PASSWORD`, `BATCHFORGE_API_KEY`, and
`BATCHFORGE_WORKER_TOKEN`. Use URL-safe generated passwords because Compose places
the database password in a connection URL. Do not commit `.env`. A generated
worker token must have at least 24 characters. Then:

```sh
docker compose --profile gpu up -d --build
```

This starts one vLLM replica on `GPU_DEVICE_ID` (default 0). Check existing GPU
tenants with `nvidia-smi` before startup. The initial model is intentionally small;
`ENGINE_GPU_MEMORY_UTILIZATION=0.06` caps vLLM's allocation target at 6% of total
GPU memory. This is an allocation target, not proof of available headroom or an
isolation guarantee. Keep `ENGINE_MAX_MODEL_LEN` identical in worker and engine.

The API binds localhost. Use an authenticated TLS reverse proxy for remote
clients. PostgreSQL and inference ports are never published by Compose. Inspect
`/readyz` and `/metrics` locally; do not publish control/worker metrics publicly.
The optional inference API key is for existing external engines; the bundled
vLLM service is isolated on the Compose network and does not configure API auth.

For existing private engines, set comma-separated `INFERENCE_BASE_URLS` and run
`docker compose up -d --build`. Each URL excludes `/v1`. Only operator
configuration can select destinations. Model ID must exactly match `/v1/models`.
Use distinct `WORKER_ID`s for independent worker processes. A worker restarts
with a new generated ID by default; abandoned leases are reclaimed by the API.

The worker never claims more than
`min(WORKER_MAX_INFLIGHT, WORKER_TOKEN_BUDGET / ENGINE_MAX_MODEL_LEN)` sequences.
Each active sequence reserves a full model context instead of estimating tokens
from characters. The engine enforces the exact tokenizer/context limit.
Multiple endpoints must serve the same model and context cap. Increase capacity
only after measuring memory, engine health, accepted quality, and task latency.

## Shared Runpod host without Docker

The native deployment uses `/root/batchforge-runtime/app`, with API/worker
dependencies in `/root/batchforge-runtime/venv` and the model server in
`/root/batchforge-runtime/engine-venv`. The pod's `/workspace` FUSE mount did not
support the runtime's file-copy/permission operations, so keep application runtime
and virtual environments on the pod's native filesystem. Do not install into the
global interpreter. Keep secrets in mode-600 `.env`; runtime files are private.

The exercised engine stack pins vLLM 0.10.2, Transformers 4.55.2, Tokenizers
0.21.4, and Hugging Face Hub 0.34.4. The explicit dependency pins avoid the
incompatible Transformers 5 tokenizer API. `ops/Dockerfile.engine` carries the
same pins. The container recipe has not been built in the current environment.

```sh
cd /root/batchforge-runtime/app
# First apply migrations with the owner's DATABASE_URL in the environment.
# --bootstrap is only for an explicitly configured new development tenant.
/root/batchforge-runtime/venv/bin/python -m backend.migrate --bootstrap
# API processes use the restricted runtime DATABASE_URL afterward.
/root/batchforge-runtime/venv/bin/python ops/manage.py start api --python /root/batchforge-runtime/venv/bin/python
/root/batchforge-runtime/venv/bin/python ops/manage.py start engine --name gpu0 --gpu 0 --port 8001 --python /root/batchforge-runtime/engine-venv/bin/python
/root/batchforge-runtime/venv/bin/python ops/manage.py start worker --python /root/batchforge-runtime/venv/bin/python
/root/batchforge-runtime/venv/bin/python ops/manage.py status worker
/root/batchforge-runtime/venv/bin/python ops/manage.py logs engine --name gpu0
/root/batchforge-runtime/venv/bin/python ops/manage.py stop worker
```

`ops/manage.py` is Linux-only. It checks the process start time and working
directory recorded in an app-local PID file before signaling the process group
it created. It never calls `pkill`, kills a discovered GPU process, stops a system
service, or edits global services. A shared host's existing processes belong to
other workloads. Add engines on other GPUs with different `--name`/`--port`
values; point the worker at their local URLs. Every endpoint is an independent
replica, not tensor parallelism.

The current fleet has app-local watchdogs. Stop its watchdog before intentionally
stopping a managed child, then restore it after maintenance; otherwise it will
restart that child. API unready responses alone never cause watchdog restarts.
The watchdog is not whole-Pod boot/replacement orchestration. Follow the exact
manifest names and private configurations in the current runbook.

SIGTERM stops new claims and drains existing work while renewing leases, up to
`WORKER_SHUTDOWN_SECONDS` (default 240 seconds). Supervisor grace is 250 seconds.
After a hard crash, the API expires leases and issues a fresh attempt token.
Stale completions cannot overwrite the fresh result. A lost completion
acknowledgement is retried with the same token, without another model call.
This provides durable at-least-once execution and a fenced accepted result;
model computation may repeat across crashes and partitions.

Configured engines are probed in parallel with an absolute five-second timeout,
independently of the claim loop. One stalled probe cannot delay a healthy replica.
An engine advertising the wrong model is ineligible. Generation connection
errors, timeouts, 429, and 5xx responses open an exponential endpoint cooldown
(2–60 seconds), so a broken endpoint with a working `/v1/models` route cannot
immediately consume every task retry. Other healthy replicas remain eligible.
Already received claims keep their heartbeat while waiting for a healthy engine.

Busy workers wait for capacity events instead of sleeping between successful
claims. Completing work wakes the claim loop immediately; a partial nonempty
claim immediately fills remaining capacity from other ready batches. Only empty
queues use jittered adaptive polling, starting at `WORKER_POLL_SECONDS` and
doubling up to `max(WORKER_POLL_SECONDS, 2 seconds)`. Engine-readiness changes also
wake the loop. Registration metadata includes `engine_count` and
`ready_engine_count`, refreshed immediately when readiness changes as well as
periodically. Worker liveness and model readiness are separate observations.

## Reproducible observed measurements

Run real inference first and keep the resulting JSON report:

```sh
python scripts/benchmark.py --tasks 1000 --batches 8 --max-tokens 64 --temperature 0 --engine-label 'Qwen2.5-0.5B / H100 / vLLM0.10.2' --workload-dir .runtime/benchmark-inputs --report .runtime/real-benchmark.json
python -m pytest worker/tests -q
```

The benchmark reads `BATCHFORGE_API_KEY` from the environment. It uploads JSONL,
submits idempotent batches, waits for terminal states, downloads outputs, checks
unique submitted IDs, and records actual token usage and observed wall-clock
throughput. The schema-version-2 report includes client-observed upload and
batch-create p50/p95/max latencies; attempted and successful submission requests
per second; batch turnaround distributions; exact prompt/output-limit/temperature
configuration; and SHA256 of the concatenated generated JSONL bytes in batch
index order. `--workload-dir` saves those exact files for audit or replay.

Submission RPS counts only upload and create requests, including failed attempts;
polling and result downloads are excluded. Each acknowledged batch is monitored
immediately while other submissions continue. Turnaround starts before its create
request and ends at the first terminal response observed by that client, so the
poll interval adds observation delay. It is not per-task inference latency.
Percentiles use linear interpolation and include their sample count.

Failed creates retain safe status/type/phase diagnostics and any successfully
observed batches in the report. The report records an uncertain create outcome
when a transport error or server error could follow a commit; it never counts
that task as acknowledged submission. `passed` requires every requested task to
have a verified successful output. Partial, failed, and timed-out runs still
write JSON and exit nonzero. Output-validation download time is reported
separately from the submission-to-terminal throughput interval. The benchmark
does not project a small test into daily capacity and does not evaluate quality.

For repeatable public-document workloads, build exact JSONL separately and pass
it with `--input-jsonl`. Random run IDs modify custom IDs, not model messages:

```sh
python -m scripts.public_workload build --dataset .runtime/public-data/dev-v1.1.json \
  --profile analysis --tasks 2048 --output .runtime/analysis.jsonl \
  --manifest .runtime/analysis-manifest.json
python scripts/benchmark.py --model Qwen/Qwen2.5-7B-Instruct --tasks 2048 \
  --batches 32 --max-tokens 512 --input-jsonl .runtime/analysis.jsonl \
  --output-dir .runtime/analysis-outputs --report .runtime/analysis-report.json
python -m scripts.public_workload evaluate --manifest .runtime/analysis-manifest.json \
  --outputs .runtime/analysis-outputs --report .runtime/analysis-quality.json
```

The dataset is [SQuAD 1.1 development](https://rajpurkar.github.io/SQuAD-explorer/),
CC BY-SA 4.0, with the source SHA recorded in the manifest. `answer` uses a
96-token cap; `analysis` uses 512 and reports requested versus actual explanation
length; `translate` uses 1,024 and checks the JSON envelope without claiming
translation fidelity. Use a fresh output directory for each run. Different
profiles, GPU counts or output lengths are separate capacity observations.

For deterministic HTTP integration tests only:

```sh
INFERENCE_BASE_URLS=http://test-engine:8000 docker compose --profile test up -d --build
python scripts/benchmark.py --engine-label test-only --report .runtime/test-benchmark.json
```

Set `TEST_ENGINE_FAIL_FIRST=1` to force one transient failure for every unique
prompt. `TEST_ENGINE_DELAY_SECONDS=3` makes cancellation/lease experiments easy
to observe. Worker tests cover dropped completion acknowledgement, stale lease
cancellation, transient engine errors, and endpoint-parameter rejection. The
full control-plane integration suite exercises PostgreSQL lease recovery.

To record cancellation and lease recovery as an actual JSON protocol report,
start a separate API with a new empty test database and no worker process, then:

```sh
python scripts/failure_probe.py --isolated-empty-test-database --report .runtime/failure-probe.json
```

It deliberately writes marked synthetic completions. It refuses a database with
any batches or registered workers, waits for an actual lease expiry, verifies
old-token rejection and accepted-completion replay, and checks exactly one
durable output row. This is a protocol/failure test, never a GPU benchmark.
The [observed protocol report](reports/failure-probe-2026-10-23.json) records a
successful run with a five-second lease.

For an actual HTTP worker crash/restart and lost-claim-acknowledgement test,
export `TEST_DATABASE_URL` pointing to a disposable PostgreSQL server whose test
role can create databases, then run:

```sh
python scripts/chaos_http.py --report .runtime/http-chaos.json
```

This creates and drops a uniquely named test database, starts its own API,
fixture engine, proxy, and workers on ephemeral localhost ports, kills its own
worker with SIGKILL after inference begins, restarts with a new worker identity,
and verifies recovery plus stale-result fencing. A second stage forwards a
claim to the API, discards the accepted response, and returns 503 to the worker.
The committed but unknown leases must recover without duplicate durable output.
Only child processes created by this invocation are signaled. An observed run
completed 10 tasks with 10 unique output rows and 4 recovered expired attempts;
the [saved report](reports/http-chaos-2026-10-23.json) records the measurement.
All responses in this test are marked synthetic; no GPU performance is measured.

The pinned vLLM image and engine command follow [vLLM's official Docker docs](https://docs.vllm.ai/en/v0.10.2/deployment/docker.html).
Compose starts PostgreSQL/API in healthcheck order using [Docker's documented dependency semantics](https://docs.docker.com/reference/compose-file/services/#depends_on).
