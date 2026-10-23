# BatchForge

Durable AI batch processing with a PostgreSQL control plane, fenced GPU workers,
an OpenAI-shaped batch API, and an operations console backed by live data.

The current deployment runs fixed-revision Qwen2.5-7B BF16 replicas on four
shared H100 devices and sixteen Blackwell MIG 24GB slices. Each Pod has two
independent API processes and a private replica router, backed by managed Neon
PostgreSQL 17. Workers use their own local API; inference no longer depends on
an operator laptop or the other GPU Pod. An app-owned watchdog restarts crashed
processes with identity checks, bounded backoff and GPU memory gates.
Fault tests cover API/worker death, lost acknowledgements, stale attempts,
read-only database rejection and transactional usage.
See [validation](docs/validation.md) for workloads, measurements and limits.
The managed database is currently on its free plan, without a production SLA.
Public paid ingress was prepared and deferred by the operator; access remains
private. See [availability](docs/ha.md), [ingress](docs/ingress.md), and
[second-Pod deployment](docs/pod2.md).

## Features

- Upload JSONL chat-completion requests and submit a batch.
- Follow progress, queue depth, worker health and measured token usage.
- Cancel work, retry terminal failures and download result/error JSONL.
- Run multiple GPU workers against independent OpenAI-compatible engines.
- Recover expired leases and reject stale results after retry or cancellation.
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
