# Operating BatchForge

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

Use an app-owned directory such as `/workspace/batchforge` and virtual
environments. Do not install into the global interpreter. Install API/worker
dependencies with `pip install -e '.[worker]'` inside `.venv`. Install the engine
in a separate `.venv-vllm` with a tested pinned version (container example uses
vLLM 0.11.1). Keep secrets in mode-600 `.env`; runtime files are private.

```sh
.venv/bin/python ops/manage.py start api
.venv/bin/python ops/manage.py start engine --name gpu0 --gpu 0 --port 8001 --python /workspace/batchforge/.venv-vllm/bin/python
.venv/bin/python ops/manage.py start worker
.venv/bin/python ops/manage.py status worker
.venv/bin/python ops/manage.py logs engine --name gpu0
.venv/bin/python ops/manage.py stop worker
```

`ops/manage.py` is Linux-only. It checks the process start time and working
directory recorded in an app-local PID file before signaling the process group
it created. It never calls `pkill`, kills a discovered GPU process, stops a system
service, or edits global services. A shared host's existing processes belong to
other workloads. Add engines on other GPUs with different `--name`/`--port`
values; point the worker at their local URLs. Every endpoint is an independent
replica, not tensor parallelism.

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

## Reproducible observed measurements

Run real inference first and keep the resulting JSON report:

```sh
python scripts/benchmark.py --tasks 1000 --batches 8 --max-tokens 64 --engine-label 'Qwen2.5-0.5B / H100 / vLLM' --report .runtime/real-benchmark.json
python -m pytest worker/tests -q
```

The benchmark reads `BATCHFORGE_API_KEY` from the environment. It uploads JSONL,
submits idempotent batches, waits for terminal states, downloads outputs, checks
unique submitted IDs, and records actual token usage and observed wall-clock
throughput. Failed or timed-out runs exit nonzero. It does not project a small
test into a claimed daily capacity and does not evaluate task quality.

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

The pinned vLLM image and engine command follow [vLLM's official Docker docs](https://docs.vllm.ai/en/v0.11.1/deployment/docker/).
Compose starts PostgreSQL/API in healthcheck order using [Docker's documented dependency semantics](https://docs.docker.com/reference/compose-file/services/#depends_on).
