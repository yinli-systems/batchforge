# BatchForge HTTP contract (v1)

Public API uses `Authorization: Bearer <tenant API key>`. The migration command's
bootstrap key in `BATCHFORGE_API_KEY` owns tenant `default`. Every public resource
is tenant-scoped. API replicas do not need the bootstrap key after initialization.
Worker endpoints use a separate `Authorization: Bearer <worker token>`, accepted
from `BATCHFORGE_WORKER_TOKEN` and/or the bounded `BATCHFORGE_WORKER_TOKENS_JSON`
overlap set for rolling rotation.
Timestamps in public batch resources use Unix seconds. IDs are opaque strings.
Errors: `{ "error": { "message": "...", "type": "invalid_request_error", "code": "..." } }`.

## Public API
- `POST /v1/files` multipart `file` and `purpose=batch`. JSONL rows:
  `{custom_id,method:"POST",url:"/v1/chat/completions",body:{model,messages:[{role,content}],max_tokens?,temperature?}}`.
  Returns `{id,object:"file",bytes,created_at,filename,purpose,status:"processed"}`.
- `GET /v1/files/{id}/content` streams original input or generated result/error JSONL.
- `POST /v1/batches` JSON `{input_file_id,endpoint:"/v1/chat/completions",completion_window:"24h",metadata?:{}}`.
  Optional `Idempotency-Key` binds a key to a body; conflict returns 409.
- `GET /v1/batches?limit=20&after=<batch-id>` returns `{object:"list",data:[batch],has_more,last_id}` newest first.
- `GET /v1/batches/{id}` returns batch.
- `POST /v1/batches/{id}/cancel` stops claims and fences outstanding attempts; returns batch.
- `POST /v1/batches/{id}/retry` creates a new batch from failed tasks only; returns new batch (409 if no failures, source nonterminal, or source model no longer served). Historical output remains available after a model upgrade.
- `GET /v1/models` returns `{object:"list",data:[{id,object:"model",owned_by:"batchforge"}]}`.
- `GET /v1/overview` returns `{stats:{total_batches,total_tasks,completed_tasks,failed_tasks,cancelled_tasks,queued_tasks,running_tasks,prompt_tokens,completion_tokens},throughput:[{timestamp,completed,failed,tokens}],workers:[{id,name,model,concurrency,status,last_seen_at,engine_count,ready_engine_count}],recent_batches:[batch]}`.

Batch: `{id,object:"batch",endpoint,input_file_id,completion_window,status,created_at,in_progress_at,completed_at,failed_at,cancelled_at,output_file_id,error_file_id,request_counts:{total,completed,failed,cancelled,running,pending},metadata,errors?}`.
Status: `in_progress|completed|failed|cancelled`. Results become available at terminal state. Partial completed results survive cancellation.
Result JSONL: `{id,custom_id,response:{status_code:200,request_id,body:<OpenAI completion>},error:null}`.
Error JSONL: `{id,custom_id,response:null,error:{code,message}}`.

## Worker API
`POST /internal/workers/register` JSON `{worker_id?:str,name:str,model:str,concurrency:int,metadata?:{}}` → `{worker_id,heartbeat_interval_seconds,lease_seconds}`.
`POST /internal/tasks/claim` JSON `{worker_id,model,limit:int}` → `{tasks:[{id,batch_id,custom_id,body,attempt_token,lease_expires_at,attempt_number}],lease_seconds}`.
`POST /internal/tasks/{id}/heartbeat` JSON `{worker_id,attempt_token}` → `{ok:true,lease_expires_at}`.
`POST /internal/tasks/{id}/complete` JSON `{worker_id,attempt_token,response:<OpenAI chat completion>}` → `{ok:true,duplicate?:true}`.
`POST /internal/tasks/{id}/fail` JSON `{worker_id,attempt_token,error:{code,message},retryable?:true}` → `{ok:true,status:"pending"|"failed"}`.
Lease expiry, cancellation, or a superseding attempt returns 409 `stale_attempt`; workers must discard stale results. Completion replay using the same accepted token is idempotent. Worker must send usage.prompt_tokens and usage.completion_tokens (nonnegative integers).

## Operational
`GET /healthz` process liveness. `GET /readyz` bounded known schema-prefix/checksum and writable-primary PostgreSQL readiness. `GET /metrics` Prometheus text. Frontend at `/`, static assets and client routing fallback; `/v1/*` and `/internal/*` never fall back to HTML.
Limits configurable: upload bytes, tasks/batch, request bytes, max output tokens, retries, lease seconds. Expiry/retry recovery is bounded and indexed. `DATABASE_URL` required for API startup. The control plane does not run a model; GPU workers connect to a separately managed OpenAI-compatible engine.

Apply `python -m backend.migrate --bootstrap` with migration-owner credentials
before first API startup or adopting an unversioned v0.1 database. API replicas
verify the immutable schema version and execute no DDL/bootstrap writes.
Transient database/pool/read-only/lock/statement-deadline failures return 503
`service_unavailable` and `Retry-After: 1`. Retrying a possibly committed batch
submission must retain its original idempotency key; gateway POST replay is
not part of the contract. See `docs/ha.md` for rollout, least-privilege grants,
managed database cutover and the distinction between API and database HA.

`SQL_COMPLETION_ENABLED=false` is the compatibility default: schema v1 and
known additive v2/v3 prefixes are accepted, while completion/heartbeat use Python transactions. With
the flag true, v2 is required and each operation uses one SECURITY INVOKER SQL
function call/statement transaction. Both modes preserve the HTTP contract,
attempt fencing, duplicate accepted-result semantics, lock order and usage.
Roll every API to the compatibility binary before applying additive migration
2; only then enable the flag. Rollback disables the flag without dropping v2.

`SQL_CLAIM_ENABLED=false` independently retains Python claims. When true it
requires schema v3 and calls one SECURITY INVOKER SQL statement transaction,
preserving worker capacity locking, eight-queue dispatch-turn fairness, SKIP
LOCKED task selection, server-clock leases, attempt metadata and durable counts.
Deploy compatibility code everywhere before migrating v3; then enable the flag.

## Operational guarantees and current limits

- `/metrics` requires the **worker** Bearer token. Tenant credentials cannot read global metrics. Health endpoints expose no database details.
- Batch resources also expose `usage:{prompt_tokens,completion_tokens}`. Metering counts accepted successful results exactly once. Computation can run more than once after a lost lease; external side effects are not exactly-once.
- Supported response formats in v1 are `{type:"text"}` and `{type:"json_object"}`. Tool calls, image input, JSON Schema constrained output, and streaming are rejected. Exact context/tokenizer limits are enforced by the worker's inference engine, while API admission bounds UTF-8 bytes and output token budgets.
- Scheduling gives the ready tenant with the oldest dispatch timestamp the next bounded claim. This is fairness by dispatch turn, not weighted fairness by actual GPU time. The implementation intentionally serializes a tenant queue row and a batch row for transactional counters and cancellation; one very hot tenant/batch can become a PostgreSQL contention point. No claimed million-QPS or multi-region benchmark exists.
- Input JSONL is stored in PostgreSQL with a bounded upload size. Output JSONL streams keyset pages without retaining a connection while a consumer is slow. Large object-store ingestion, retention policies, and partitioned history are future work.
- The 24-hour deadline is real: new claims and attempt heartbeats/reports are fenced at expiry; a bounded background sweep marks unfinished tasks cancelled with `batch_expired`, while the batch becomes failed. Already committed results remain downloadable.
- Worker identity is trusted inside a cluster because workers share the active deployment secret generations. Use a private network and TLS at the ingress; per-worker mTLS identity and remote attestation are not implemented.
- `/v1/overview` task/usage totals are tenant-scoped. Worker inventory exposes only deployment names, models, configured concurrency and liveness, never private worker metadata.
- Bootstrap key insertion never re-enables revoked keys and does not revoke previous keys during rotation. Operator tooling: `python -m backend.admin create-tenant --name NAME`, `add-key --tenant-id ID`, `revoke-key --key-hash SHA256`. Generated key material appears once on stdout; only digests are stored.

Worker `status` is process liveness (`online|offline`), not engine availability. `engine_count` and `ready_engine_count` are separately reported readiness counts, sanitized to integer 0..4096 or null when absent/invalid. No other worker metadata is returned.
