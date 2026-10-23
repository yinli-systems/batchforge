# Operations

The exercised deployment is native Linux on two existing Runpod Pods. Both run
independent loopback API replicas against the same managed Neon PostgreSQL 17
instance in Frankfurt. GPU workers call their own Pod's API gateway. Disconnecting
the operator laptop does not interrupt this path. The selected access policy is
private SSH ingress; the paid public ingress proposal was deferred by the owner.

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
| Engines | `engine-gpu0` through `engine-gpu3`, 8100–8103 | `engine-pod2-7b-gpuN`, 8200+N |
| Workers | `worker-gpu0` through `worker-gpu3`, metrics 9101–9104 | `worker-pod2-7b-migN`, metrics 9200+N |
| Supervisor | `supervisor-primary` | `supervisor-main` |
| Service manifest | `.runtime/services-primary.json` | `.runtime/pod2-services.json` |
| Supervisor configuration | `../supervisor.env` | `.env.supervisor-pod2` |
| API configuration | `../api-a.env`, `../api-b.env` | `.env.api-a`, `.env.api-b` |

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
/root/batchforge-runtime/venv/bin/python ops/manage.py logs worker --name gpu0
/root/batchforge-runtime/venv/bin/python ops/manage.py status supervisor --name primary
```

The supervisor restarts dead owned processes with bounded backoff, records
restart state, and checks free GPU memory before starting an engine. Database
unreadiness alone does not kill API processes. It refuses ambiguous process
identity and ambiguous timed-out engine startup; resolve those conditions
manually instead of launching duplicate model copies. Inspect the corresponding
`.runtime/supervision-<manifest-stem>.json` and bounded event log.

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
