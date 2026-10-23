# BatchForge

Durable AI batch processing with a PostgreSQL control plane, fenced GPU workers,
an OpenAI-shaped batch API, and an operations console backed by live data.

This repository contains the first release. It does not claim ten-million-task
daily throughput or production availability without measured evidence.
See [validation](docs/validation.md) for the actual tested environment.

## Features

- Upload JSONL chat-completion requests and submit a batch.
- Follow progress, queue depth, worker health and measured token usage.
- Cancel work, retry terminal failures and download result/error JSONL.
- Run multiple GPU workers against independent OpenAI-compatible engines.
- Recover expired leases and reject stale results after retry or cancellation.

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

## Security and access

Use independent random values for the tenant API key and internal worker token.
The console asks for a tenant key and retains it only in browser session storage.
Keep keys out of URLs, source files and screenshots. The source repository is
private. A running deployment separately requires HTTPS and access controls.

Deployment commands and tested examples are recorded alongside the first
successful end-to-end run. See [architecture](docs/architecture.md) and
[operations](docs/operations.md).
