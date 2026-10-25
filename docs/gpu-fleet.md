# Physical GPU inventory transport

This follow-up adds an opt-in worker reporter, a bounded read-only
`GET /v1/fleet` endpoint and a physical-observations panel in Workers. It is
independently qualified; it is not part of the immutable active endurance,
queued quality release or queued reusable-input campaign. No deployment or
new worker registration is claimed here.

## Independent qualification

Immutable source `5cca488` passed source-isolated, read-only Azure/ASGI
qualification at 15:13:47 UTC on October 25. Schema 11, verified TLS 1.3,
authentication, cache policy, actual fleet SQL and the existing quality
reconciliation passed. An independent NVML/topology observation then found
the actual four H100s: each 81,559 MiB total / 74,125 MiB used, with all six
GPU pairs reporting `NV18`. Utilization at that instant was 52%, 97%, 86%, 97%;
temperature 49–50 °C. These are instantaneous device-wide observations, not
sustained utilization, measured bandwidth or an allocation certificate.

The [qualification report](benchmarks/platform480-2026-10-25/evidence-api-fleet-20261025-v1-qualified/validation.json)
and full source receipt were exported with SHA-256 verification. The real
database API query returned **zero reported inventory nodes**, because the
running immutable worker has not enabled the new reporter. The independently
observed four GPUs were not inserted as synthetic registrations. Local
verification: 148 relevant Python tests passed, one external PostgreSQL test
skipped; 22 frontend Node checks passed. Browser visual QA remains unavailable.

The reporter needs both `WORKER_INVENTORY_NODE_ID` and
`WORKER_INVENTORY_GPU_INDICES`, for example `0,1,2,3`. The operator explicitly
selects the observed devices. It never assumes every GPU visible to NVIDIA is
assigned to a worker. With neither configured, existing workers run unchanged.
A background task samples read-only NVIDIA commands every 30 seconds; inference
claims and lease heartbeats do not wait for sampling. Each command has a
three-second timeout. Sampling failure removes the observation instead of
inventing zero utilization. No CUDA context, NCCL test or GPU workload is created.

The existing authenticated worker registration transports the latest report
into Azure's existing `workers.metadata`. No schema migration, new database,
agent credential, external endpoint or paid resource is required. Node labels
and GPU UUIDs are SHA-256 identities; hostnames, raw UUIDs, NIC names, process
arguments, environment, driver stderr and unrelated metadata are not exposed.
Reports include physical names, memory, utilization, power, temperature and
bounded symmetric topology labels. Missing measurements remain `null`.

The authenticated API exposes shared workers for configured serving models,
consistent with the existing shared-worker overview. It projects only the
inventory field, limits the database read to 100 workers and 64 KiB per report,
and validates protocol, device identities, finite measurements, unique indices,
topology edges and timestamps. It deduplicates physical devices within each
node, retains shared worker memberships and flags conflicting host/device
identities. Freshness requires an online worker and an inventory age no greater
than 60 seconds; a future timestamp over five seconds is stale. The console
hides stale utilization, memory-used, power and temperature, escapes labels,
and shows at most 20 nodes/eight devices from one report per node.

**Inventory is not resource ownership.** `placement_capacity` deliberately stays
`null`: observing free memory does not establish that a GPU is unreserved or
available to a gang. Worker metadata is not hardware attestation. NVLink labels
are not measured bandwidth or peer-access validation. MIG partitions and
multi-node gang reservations need a separate placement protocol. This is the
discovery/observability boundary on which that protocol can be built.

Enablement must follow a drained, identity-checked worker rollout after the
fixed GPU studies close. API/worker source and every physical qualification
snapshot must be exported and pinned separately. Do not alter running studies
to make a new feature appear active.

## Worker module origins before enablement

The existing manager starts `python -m worker` with the live application root
as its working directory. **`PYTHONPATH` alone does not select a new worker**:
Python normally searches that working directory first. `UVICORN_APP_DIR` selects
API source and does not affect the worker. A real local `-m` regression reproduces
the old-package resolution with a shadow package.

`ops.worker_source.source_environment` prepares, without writing any file,
an environment with `PYTHONSAFEPATH=1` and exactly one approved `PYTHONPATH`.
Python 3.11+ safe-path mode removes the implicit working-directory entry, while
the process cwd remains the live root for managed PID checks. Python treats any
nonempty safe-path variable as enabled; disable it by unsetting it, not by setting
`0`. The helper preserves all existing credentials, model, explicit worker ID,
KV budget, slots, origins and metrics settings. Missing fixed model/worker
identities or an ambiguous device selection are rejected.

The definition-only probe checks safe-path support, actual module origins and
nine entrypoint/runtime/configuration/metrics/KV/inventory/backend file hashes
against a qualified source receipt. It reads `worker/__main__.py` bytes without
importing that executable entrypoint. It validates Settings and returns only a
fixed projection, including the unchanged worker/model identity and runtime
fingerprint. It never constructs or runs Worker, registers, claims tasks, opens
an application port, samples NVIDIA or performs inference. A changed file,
unexpected origin or failed configuration stops preparation. Local focused
verification: **67 passed**. This is preflight support, not a rollout controller;
no running worker/source/manifest has been changed by these helpers.

The same definition-only probe passed on the actual Pod at **15:55:50 UTC**:
Python safe-path mode selected all nine pinned `5cca488` files from the Fleet
candidate while retaining the live root cwd. The parsed configuration preserved
the existing worker ID, 480B model, 32 slots, 164,256-token budget, 18,440-token
context and runtime fingerprint. All managed service identities were unchanged.
The [native preflight receipt](benchmarks/platform480-2026-10-25/evidence-api-fleet-20261025-v1-worker-preflight/passive-preflight.json)
pins helper `e1900dd` and its SHA-256. The probe wrote its evidence receipt;
no service environment file, registration, inference or running study changed.
This qualifies the source-selection mechanism, not Fleet rollout or gang
placement. Production enablement still waits for the current fixed study.

## Release and rollback ordering

`ops.fleet_release` adds a tested ordering primitive, not an installed controller
or an automatic launch. A service-I/O adapter must still bind it to the existing
exclusive campaign lock, owned PID identities, fsynced phase events and private
configuration backups. Its admission guard rejects incomplete/failed 3,600-task
reports, non-unique acceptance, inconsistent usage, incorrect output lengths or
unexpected input uploads.

The forward path pauses both enqueue gates, drains accepted work, installs and
checks the worker, then installs/checks the APIs, reopens admission and checks
again. Engine/model actions are excluded. The drain callback must verify durable
pending/running tasks **and** worker inflight **and** engine quiescence.

A failure after reopening may coexist with newly accepted tasks. Rollback first
installs known-compatible APIs with admission closed, ensures a live draining
worker, and drains again **before** replacing the worker. Original readiness is
checked before and after reopening. A failed rollback leaves admission closed
where possible and records any inability to close it explicitly; it never
claims success or discards accepted tasks. Watchdog recovery is attempted even
after a failed shutdown. Journal failures abort forward mutation but cannot
block recovery actions; even a final journal failure prevents a success claim.

Failure-injection tests cover partial reopen, post-reopen validation, failed
rollback drain, failed original validation after reopen, source/preflight
failure and disk/event failure. This preparation changes no running service.
