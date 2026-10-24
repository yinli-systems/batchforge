# BatchForge

Durable AI batch processing with a PostgreSQL control plane, fenced GPU workers,
an OpenAI-shaped batch API, and an operations console backed by live data.

The current experiment uses **one Pod with four dedicated H100 80GB GPUs**.
The official FP8 Qwen3-235B-A22B-Instruct-2507 checkpoint (235B total, 22B active
parameters) is running with TP4 across all four GPUs. Its initial eight-slot
direct calibration completed the 20-cell real-token matrix; platform tuning is
recorded separately. The preceding GPT-OSS-120B (116.83B total) 64-slot
isolated 400-task mixed-length run completed every task in 143.27 seconds
(2.79 tasks/s), with exactly 2,181,120 input and 272,000 generated tokens.
This is a finite burst, not daily capacity. See the
[single-Pod validation](docs/single-pod-validation-2026-10-24.md) for policy A/B,
Prefill/Decode observations, failure and storage evidence, costs and limitations.

Two API replicas and a private gateway use managed Neon PostgreSQL 17 and
verified private S3 artifacts. Workers use the local API; inference does not
depend on an operator laptop relay. An app-owned watchdog uses process identity
checks, bounded restart backoff and per-rank GPU memory gates. The transaction
state machine includes cost-aware scheduling, bounded SQL lease recovery and
runtime-fenced retries. Leased result downloads permit verified, bounded
archival and completed-payload purge.

The earlier two-Pod 7B/120B deployment, 17-worker snapshot and cross-Pod
re-execution experiment are historical; the second Pod is unavailable for this
run. Those measurements remain in [100B validation](docs/100b-validation.md),
[earlier validation](docs/validation.md) and its
[evidence index](ops/reports/100b-release-evidence-2026-10-24.json).
The current single Pod has no whole-Pod compute failover. Physical GPU reset,
whole-Pod replacement and live KV migration remain untested.
The managed database is currently on its free plan, without a production SLA.
Public paid ingress was prepared and deferred by the operator; access remains
private. See [availability](docs/ha.md), [ingress](docs/ingress.md), and
[second-Pod deployment](docs/pod2.md).

## Platform screenshots

Captured from the running private deployment on October 24, 2026 (Asia/Dubai).
The overview includes historical runs across models; it is not a 120B-only
throughput measurement. [View all five screenshots](docs/screenshots/README.md).

![Live workspace overview](docs/screenshots/overview-2026-10-24.jpg)

![Completed GPT-OSS-120B batch](docs/screenshots/batch-120b-completed-2026-10-24.jpg)

## Features

- Upload JSONL chat-completion requests and submit a batch.
- Follow progress, queue depth, worker health and measured token usage.
- Cancel work, retry terminal failures and download result/error JSONL.
- Run multiple GPU workers against independent OpenAI-compatible engines.
- Route each batch by its requested model ID or explicit long-context alias.
- Recover expired leases and reject stale results after retry or cancellation.
- Rank work using calibrated Prefill/Decode cost profiles, with tenant rotation,
  aging, deadline overrides and numerical-runtime compatibility fencing.
- Reserve input artifact quotas before I/O, checksum private objects and archive
  terminal outputs with reader leases and resumable bounded payload purge.
- Gate restarts on per-rank GPU memory and CPU-expert cgroup headroom; clean up
  previously observed engine descendants using verified Linux process identities.
- Validate explicitly requested JSON objects before accepting results, with
  bounded durable retries and separate rejected-compute observations.

## Components

| Path | Responsibility |
| --- | --- |
| `backend/` | API, authentication, PostgreSQL transactions and scheduling |
| `worker/` | Bounded inference concurrency, heartbeats and fenced reporting |
| `frontend/` | Buildless console with live operational data |
| `ops/`, `scripts/` | Deployment and reproducible checks |
| `tests/` | Database-backed correctness and API tests |
| `docs/` | Architecture, operations and measured validation |

Python 3.12 and PostgreSQL are required. GPU workers connect to an existing
OpenAI-compatible inference server; the API itself does not require CUDA.
The HTTP contract is in [backend/CONTRACT.md](backend/CONTRACT.md).

## Start with containers

Copy `.env.example` to `.env`, generate independent random secrets, and set the
database password. Passwords used in connection URLs must be URL safe.

```sh
python3 -c 'import secrets; print(secrets.token_urlsafe(36))'
docker compose --profile gpu up -d --build
```

Open `http://127.0.0.1:8000` and connect with your tenant API key. The default
model is `Qwen/Qwen2.5-0.5B-Instruct`; it is a small integration model, not a
quality recommendation for every workload. The optional GPU profile starts one
replica. Existing engines can be configured with `INFERENCE_BASE_URLS` instead.
The native Runpod deployment has been exercised; the Compose file is provided
but its image build has not been run in the initial environment.
Compose runs a one-shot migration before starting the API. For a native
deployment, run `python -m backend.migrate --bootstrap` with the schema-owner
database connection before API rollout. API startup verifies schema and performs
no DDL; use a separate low-privilege runtime login.

## Develop and verify

```sh
uv sync --locked --extra test --extra worker
uv pip install pgserver==0.1.4
uv run --no-sync python devtools/run_checks.py
```

The helper starts an isolated real PostgreSQL cluster, runs database/worker tests,
then stops it. It does not discover or signal other PostgreSQL processes.
With Node 22 or newer installed for development, run `node --test frontend/tests/*.test.js`
for cancellation-race and retry-acknowledgement regressions. Node is not required
to serve or use the console.
Alternatively set `TEST_DATABASE_URL` to a **disposable** database: integration
tests truncate the BatchForge tables there. Never point tests at a live database.
`requirements.txt` and `requirements.lock` include dependency hashes for runtime
and test environments respectively; `uv.lock` is the project lockfile.

## Submit work

Create `requests.jsonl`, one request per line:

```json
{"custom_id":"item-001","method":"POST","url":"/v1/chat/completions","body":{"model":"Qwen/Qwen2.5-0.5B-Instruct","messages":[{"role":"user","content":"Classify this review as positive or negative: works well."}],"max_tokens":32,"temperature":0}}
```

Export the tenant key locally, upload the file, then submit its returned ID:

```sh
curl -H "Authorization: Bearer $BATCHFORGE_API_KEY" \
  -F purpose=batch -F file=@requests.jsonl http://127.0.0.1:8000/v1/files
curl -H "Authorization: Bearer $BATCHFORGE_API_KEY" \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: catalog-run-001' \
  -d '{"input_file_id":"<returned-file-id>","endpoint":"/v1/chat/completions","completion_window":"24h"}' \
  http://127.0.0.1:8000/v1/batches
```

Poll `/v1/batches/<id>`. Download `/v1/files/<output_file_id>/content` when terminal.
Failed requests are exported separately; retrying creates a new batch from failed
requests only. Reusing an idempotency key with a different request returns 409.

Use `GET /v1/models` for the configured catalog. Set `MODEL_NAMES_JSON` to add
model IDs alongside `MODEL_NAME`; a batch must contain exactly one model.
The deployed 120B model ID is `openai/gpt-oss-120b`. Reasoning tokens consume its
output budget, so a very small cap can produce no final answer. Token throughput,
durable completion and useful final output are reported separately.
Requests may explicitly set `reasoning_effort` to `low`, `medium` or `high`;
omitting it preserves the runtime default. The small paired quality pilot found
fewer generated tokens but worse answer scores with `low`, so the platform does
not apply it globally. See the [measured quality tradeoff](docs/100b-validation.md#natural-answer-quality-and-reasoning-effort).

## Tenant administration

On the trusted control-plane host with `DATABASE_URL` configured:

```sh
python -m backend.admin create-tenant --name example-customer
python -m backend.admin add-key --tenant-id <tenant-id>
python -m backend.admin revoke-key --key-hash <sha256-digest>
```

New keys are printed once. Only digests are stored in the database. Token usage
is metered transactionally; this release does not include payments or invoices.

## Security and access

Use independent random values for the tenant API key and internal worker token.
The console asks for a tenant key and retains it only in browser session storage.
Keep keys out of URLs, source files and screenshots. The source repository is
private. A running deployment separately requires HTTPS and access controls.

See [architecture](docs/architecture.md), [operations](docs/operations.md), and
[worker operations](ops/README.md) for lease semantics, deployment and recovery.
The [second-Pod guide](docs/pod2.md) records all sixteen MIG slices, real model execution,
private connectivity, and the retired laptop relay. Management SSH is separate
from the inference data path.
