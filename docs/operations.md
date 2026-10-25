# Operations

## Current deployment — October 25, 2026

There is **one existing Pod with four H100 80GB GPUs**, serving
`QuantTrio/Qwen3-Coder-480B-A35B-Instruct-AWQ` on TP4: 480B total / about
35B active MoE parameters. The second Pod and Neon database configurations
below are historical. Do not run their restart examples against the current
primary. Azure PostgreSQL 17, schema 11, holds task identities, attempts and
metering; the existing private S3 endpoint holds verified input/result archives.

| Service | Active configuration |
| --- | --- |
| API replicas | `api-a` / `api-b`, loopback 8082 / 8083, immutable API source `748dc10` |
| Private gateway | `gateway-edge`, loopback 8080 |
| Engine | `engine-qwen480bench`, loopback 8348, TP4 |
| Worker | `worker-qwen480`, health/metrics 9105, exact-token admission, 32 slots |
| KV admission | 164,256-token budget; last qualified physical KV 172,912 tokens |
| Context / durable lease | 18,440 tokens / 15 seconds |
| Supervision | `supervisor-primary`, exact PID/start-time/cwd checks |
| GPU Fleet reporting | **Enabled**; four selected H100s, Azure metadata, fresh views on both APIs and gateway |

Worker and API source selection differ: `UVICORN_APP_DIR` selects API source,
not `python -m worker`. A worker candidate requires the source-origin probe,
`PYTHONSAFEPATH=1` and a single immutable `PYTHONPATH`; never copy over a live
package. Check the live manifest and source receipts before maintenance.

Connect through the already authorized RunPod SSH proxy:

```sh
ssh -tt -i ~/.ssh/id_ed25519 -o BatchMode=yes \
  4x7ej43hrqznjn-64410eb2@ssh.runpod.io
```

Send commands through interactive stdin and read their actual output: this proxy
can ignore command arguments. Historical direct host/port mappings and local
SSH tunnels below are not verified current access paths. Tenant keys belong in
the console key field, never URLs or logs. Inference and database traffic stay
on the Pod-to-service path when the laptop disconnects.

Before changing an application service, coordinate with the existing campaign
lock, pause both API create/retry admission paths, drain accepted work, verify
fresh engine/worker quiescence and exact managed identities, and preserve a
compatible rollback. Do not re-execute a historical experiment directory.

A live PID or API HTTP 200 is insufficient. Probe worker `/readyz` and
`batchforge_worker_adaptive_kv_paused`, active/reserved/waiting work, engine
running/waiting and durable completion progress. The current worker needed a
[bounded recovery from a KV safety latch](benchmarks/reuse480-latch-incident-2026-10-25.md).
The watchdog does not recover a live but latched worker automatically. Preserve
accepted identities and metering across recovery; never simply clear its latch.

`/workspace` is an unhealthy FUSE mount (observed ENOMEM). Mutable source,
virtual environments, logs and locks use the existing container disk. Export
allowlisted evidence to the Mac and private repository with SHA verification.
Container fsync does not survive Pod loss. Do not export environments, keys,
weights or raw tenant responses. Input retention remains 24h and output
archiving starts after the existing 120-second interval; do not silently change
retention or quotas. There is **no whole-Pod compute failover**.

The operator confirmed **$16.10 per whole-Pod hour** on October 25. This is not
an invoice. See the source-bound [quality allocation](benchmarks/quality480-cost-allocation-2026-10-25.json)
and [endurance allocation](benchmarks/endurance480-cost-allocation-2026-10-25.json).
No paid upgrade, new GPU, public ingress or other paid resource is authorized.

## Historical runbook — earlier two-Pod Neon / 120B deployment

The following record describes the earlier deployment only. It is preserved for
reproducing old evidence, **not as a current maintenance procedure**. Its names,
paths, model settings, lease lengths and network mappings must not be applied
to the current 480B deployment.

## Runtime inventory

Both Pods use `/root/batchforge-runtime/app` as their application directory,
`/root/batchforge-runtime/venv/bin/python` for API/worker/operations commands and
`/root/batchforge-runtime/engine-venv/bin/python` for vLLM. Local runtime files are
private and excluded from source archives and Git. Existing unrelated GPU
workloads must remain untouched.

| Service | Primary Pod | Second Pod |
| --- | --- | --- |
| API replicas | `api-a`, `api-b`, ports 8082/8083 | `api-a`, `api-b`, ports 8082/8083 |
| Private gateway | `gateway-edge`, port 8080 | `gateway-edge`, port 8080 |
| Engines | `engine-gptoss120`, port 8300, TP4 | 14 short 7B engines, one long 7B engine, one CPU-expert 120B engine |
| Workers | `worker-gptoss120`, metrics 9105 | 16 model-specific workers; see [second-Pod inventory](pod2.md) |
| Supervisor | `supervisor-primary` | `supervisor-main` |
| Service manifest | `.runtime/services-primary.json` | `.runtime/pod2-services.json` |
| Supervisor configuration | `../supervisor.env` | `.env.supervisor-pod2` |
| API configuration | `../api-a.env`, `../api-b.env` | `.env.api-a`, `.env.api-b` |

The primary now serves `openai/gpt-oss-120b` on four shared H100s with tensor
parallelism. Its context is 18,440 tokens, prefill chunk 1,024, prefix cache off,
and native MXFP4 experts / BF16 remaining tensors use the pinned vLLM Marlin
path. The canonical manifest selects the active sequence limit and matching
worker token budget. Check the [current measured configuration](100b-validation.md)
and manifest before maintenance; dated tuning controllers are execution records,
not routine restart commands. A worker reserves the full configured context
per slot, and its inference timeout is 1,800 seconds with independent lease
heartbeats. The final measured configuration uses
`.env.engine-gptoss120-tp4-seq8` and `.env.worker-gptoss120-tp4-seq8`: eight slots,
token budget 147,520 and memory fraction 0.26. Its healthy four-slot rollback
is `.runtime/primary-gptoss120-seq8-tune/`, containing `original-manifest.json`,
`engine-seq4.env` and `worker-seq4.env`.

The old four-engine 7B deployment and its 16-sequence measurements are historical.
Its rollback manifest is preserved privately under
`.runtime/primary-tp4-transition-v1/original-services.json`. Do not start those
engines alongside the 120B engine. Restoring that topology is an explicit drained
maintenance operation with fresh per-device memory gates, not automatic failover.
The second Pod's 120B lane uses a separately pinned native llama.cpp binary and
GGUF with CPU experts; it does not use the vLLM interpreter.

Consult the manifest and model/device reports for the actual deployed engine
count and UUID mapping. A physical GPU UUID is not a unique MIG instance ID.
`torch.cuda.device_count()` alone undercounts the second Pod's 16 accessible MIG
instances in the tested Torch/NVML environment; explicit MIG UUID discovery is
required. Each worker/engine is independently bounded; the primary uses only its
reserved share of four already shared H100s.

The canonical primary database configurations are `../neon-runtime.env` and
`../neon-owner.env`, both mode 600. Applications receive only `bf_runtime`, the
restricted runtime role. Migrations and backup operations use the separate owner
configuration. The second Pod receives runtime credentials, not owner credentials.
The legacy `../access.env` still holds the operator tenant key but its local
PostgreSQL URL is retired. Do not use that URL for the current live database.

## Private console access

The current operator tunnel forwards local port 8787 to primary loopback 8080.
Open `http://127.0.0.1:8787`; provide the tenant key through the console's key
field, never a URL. The laptop's ignored `.runtime/access.env` contains that key.

```sh
ssh -N -L 127.0.0.1:8787:127.0.0.1:8080 -p 13253 \
  -o StrictHostKeyChecking=yes \
  -o UserKnownHostsFile=/private/tmp/batchforge_known_hosts \
  -i ~/.ssh/id_ed25519 root@91.199.227.82
```

Runpod can change the direct mapping after lifecycle changes. Verify the new
host key and authorized mapping before reconnecting. Neither engine endpoints
nor internal worker routes are published by this setup. The second Pod also
serves its own console through its private gateway.

## Service supervision and maintenance

Run commands from the deployed app directory. The manager checks the PID's
working directory and Linux process-start identity before signaling it.

```sh
/root/batchforge-runtime/venv/bin/python ops/manage.py status api --name a
/root/batchforge-runtime/venv/bin/python ops/manage.py logs worker --name gptoss120
/root/batchforge-runtime/venv/bin/python ops/manage.py status supervisor --name primary
```

The supervisor restarts dead owned processes with bounded backoff, records
restart state, and checks free GPU memory before starting an engine. Database
unreadiness alone does not kill API processes. It refuses ambiguous process
identity and ambiguous timed-out engine startup; resolve those conditions
manually instead of launching duplicate model copies. Inspect the corresponding
`.runtime/supervision-<manifest-stem>.json` and bounded event log.

TP engines additionally record only children observed while their owned leader
is live. After leader loss, cleanup validates boot ID, process start time,
working directory and launch identity before using pidfds to terminate those
observed members. Unknown or escaped children are not guessed. Admission must
still require the exact device count and free memory of at least the configured
fraction plus `ENGINE_GPU_MEMORY_MARGIN_MIB` on every device. This margin
defaults to 512 MiB and accepts integers from 512 through 16,384; invalid
settings fail before a device probe. The current primary TP4 deployment uses
2,048 MiB because the original margin was below its observed resident memory;
the secondary retains 512 MiB. The CPU-expert lane also requires the
checkpoint size plus 80 GiB headroom against both host and visible cgroup limits.
Direct `manage.py start` does not itself enforce these resource gates: maintenance
and recovery operators must call the supervisor's `Manager.start_gate` first.

Stop the supervisor before deliberate maintenance, otherwise it can restart the
service being changed. Stopping the watchdog preserves its children. Restart it
after validating all intended services:

```sh
/root/batchforge-runtime/venv/bin/python ops/manage.py stop supervisor --name primary
# Change only the selected app-owned service; use its manifest's exact env/port/GPU.
/root/batchforge-runtime/venv/bin/python ops/manage.py start supervisor --name primary \
  --env-file /root/batchforge-runtime/supervisor.env \
  --python /root/batchforge-runtime/venv/bin/python
```

For API upgrades, deploy compatible code to all replicas first, apply explicit
owner-run migrations, then enable optional new execution paths. Roll one API at
a time and require `/readyz` before replacing the next. API startup verifies the
schema; it does not run DDL or create tenant keys. Preserve token overlap while
rotating workers. A gateway forwards a mutation once; it never silently retries
an uncertain POST on another replica.

A live process is not proof of useful GPU work. Check engine readiness, worker
readiness, durable accepted results, claim/completion latency and lease churn.
Inference can repeat after failure; fenced result acceptance and usage accounting
must remain unique. Preserve the same attempt token when retrying an uncertain
completion acknowledgement.

## Database protection and recovery

The old local PostgreSQL 16 source is fenced with database-default read-only
transactions after a verified, drained, atomic copy to Neon. It is historical
migration evidence, not an automatic fallback. Never route live writers back to
it during a Neon outage: that would create diverging histories.

Use TLS `verify-full` with the direct Neon endpoint. The pooler does not accept
the API's PostgreSQL startup `options`. Client-side TLS is the relevant check;
`pg_stat_ssl` behind a provider TLS proxy does not describe that client hop.
Database errors return bounded 503 responses. Workers retain their completion
fence while reconnecting; new inference must not be started merely because an
acknowledgement was lost.

See [backup instructions](backup.md) for the version-matched PostgreSQL 17 dump
and isolated restore procedure. A local dump or provider snapshot alone is not
an off-host retention policy. Keep owner secrets outside worker configurations.
The Neon project currently uses its free plan, without a production SLA. Storage,
compute quota, point-in-time retention, alerts and paid production durability
still require sizing and an explicit service decision.

The application watchdog is not an OS boot service, whole-Pod replacement
controller, cross-region database consensus system or guarantee of provider
availability. Recreating a lost Pod requires restoring its app configuration,
model files and service manifest. The other Pod can keep its own workers running
against Neon, but private client access must be redirected explicitly if the
primary SSH endpoint is unavailable.
