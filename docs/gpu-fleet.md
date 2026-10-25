# Physical GPU inventory transport

This follow-up adds an opt-in worker reporter, a bounded read-only
`GET /v1/fleet` endpoint and a physical-observations panel in Workers. It is
prepared for qualification; it is not part of the immutable active endurance,
queued quality release or queued reusable-input campaign. No deployment or
new worker registration is claimed here.

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
