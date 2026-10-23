# Availability, migrations, and database failover

API processes are stateless replicas of one PostgreSQL state machine. Killing
one API process must not lose an accepted batch, accepted result, usage row, or
lease. More API processes on one Pod improve process availability; they do not
survive the loss of that Pod. Database HA, API HA, ingress HA and GPU capacity
are separate availability requirements.

## Rollout and credentials

Run migrations with the schema owner's connection before starting replicas:

```sh
python -m backend.migrate --bootstrap
```

`DATABASE_URL` and, for `--bootstrap`, `BATCHFORGE_API_KEY` come from an
operator-only environment file. Migrations take a transaction-scoped advisory
lock, record an immutable version/checksum, and atomically commit schema and
bootstrap. Concurrent migration commands serialize. An unversioned v0.1 database
is adopted once using its original idempotent schema. `--bootstrap` adds the
default key digest without re-enabling a revoked key. The command without this
flag does no tenant/key work.

API startup verifies a known version/checksum prefix and performs no DDL, tenant initialization,
or key insertion. It can omit `BATCHFORGE_API_KEY`. Use a separate runtime role
with CONNECT, schema USAGE, SELECT on the migration ledger, and required
SELECT/INSERT/UPDATE permissions on application tables. The supplied
`ops/ha/runtime_grants.sql` grants only SELECT on tenant/API-key tables plus
UPDATE on the tenant name column because PostgreSQL admission row locks require
an UPDATE privilege. Do not grant it
schema CREATE, ownership, replication or superuser privileges. The separate
operator CLI needs its own permissions to issue/revoke tenant keys.

Keep version 1's `backend/schema.sql` immutable. Version 2 adds only the
completion/heartbeat SQL functions in
`backend/migrations/0002_sql_completion.sql`. Version 3 adds only the claim
function in `backend/migrations/0003_sql_claim.sql`. The compatibility release
accepts exactly a known v1, v1+v2, or v1+v2+v3 checksum prefix; gaps,
checksum drift and unknown future versions fail readiness. It does not claim a
general online migration framework.

For the additive SQL hot-path rollout, use this order:

1. Deploy the compatibility application and both SQL resources to **every** API
   replica with `SQL_COMPLETION_ENABLED=false` (the default). All replicas must
   be ready against the existing v1 database before continuing. Earlier binaries
   that only recognize v1 must no longer serve traffic.
2. Run `python -m backend.migrate --target-version 2` once using the migration
   owner. It serializes on the existing advisory lock and records the v2
   checksum in the same transaction as function creation. Compatibility APIs
   remain ready during/after this additive change.
3. Roll replicas to `SQL_COMPLETION_ENABLED=true`, one at a time, checking
   readiness and draining each replaced process. Enabled replicas require v2;
   enabling the flag against v1 fails startup/readiness. Python and SQL paths
   interoperate against the same queue and may run concurrently during rollout.
4. Rollback uses `SQL_COMPLETION_ENABLED=false` with the compatibility binary.
   Leave the additive v2 functions/ledger in place; downgrading the schema or
   rolling back to an earlier v1-only binary is not supported.

The two functions are SECURITY INVOKER with fixed search paths and preserve
existing table ACL requirements; they do not obtain schema-owner privileges.
Python still validates response JSON and bounded usage before completion. One
autocommit `SELECT` invokes a full statement transaction containing worker,
queue, batch and task locks, server-clock fencing, accepted response/attempt/
usage updates and terminal output/error file metadata. There are no extra
client BEGIN/COMMIT round trips. Heartbeat follows the same lock order and
clock checks. Lost-response replay remains duplicate-safe. Physical inference
can still execute more than once.

The optimization reduces completion/heartbeat network round trips, not their
consistency model. Failures, admission and cancellation retain their existing
Python transaction implementations. Claims use their separate flag described
below. The tenant queue remains a
serialization point: sustained-load measurements must establish whether more
work is needed after this change. Existing PostgreSQL state-machine cases run
in both modes, including cancellation/completion races, lease/deadline fencing,
usage, partial output and retries. Additional tests race mixed-mode duplicate
completion, expire a lease while a heartbeat waits on the queue lock, check
rollback of failed worker touches, verify two-stage rollout and exercise both
paths with the actual minimum runtime grants.

The v3 claim rollout follows the same compatibility-first procedure while
leaving the v2 SQL completion flag enabled: deploy every API with the new
compatibility binary and `SQL_CLAIM_ENABLED=false`, check all replicas ready
against schema v2, apply `backend.migrate --target-version 3` with the owner,
then roll replicas to `SQL_CLAIM_ENABLED=true`. Enabling that flag against v1/v2
is refused; disabling it safely restores the Python claim path while leaving
the additive function/ledger in place. The v1 and v2 SQL resources are immutable.

The claim function is SECURITY INVOKER, with no new runtime table privileges.
It locks the worker before counting valid active leases and deriving capacity,
materializes and locks the oldest **eight** ready tenant queues with SKIP LOCKED,
selects the first eligible unexpired batch in those candidates, and locks/claims
its bounded pending task set. Attempt creation, lease timestamps, batch/queue
counters, next-ready time and dispatch-turn advancement commit together in one
statement. It returns the same task metadata/body/lease contract. It does not
widen past eight candidates when all eight batches are expired, or change
dispatch-turn fairness to GPU-time fairness. Existing mixed-mode code may claim
concurrently without exceeding one worker's configured capacity.

The full PostgreSQL state-machine suite runs all four independent flag
combinations. Additional claim tests cover mixed Python/SQL callers on one
worker, locked task skipping, the exact eight-candidate bound, and injected
attempt-insert failure rolling back task state, capacity, worker timestamp and
queue counts. Runtime-role lifecycle tests exercise all four combinations.

Worker token rotation uses a bounded overlap of at most four credentials:

```text
BATCHFORGE_WORKER_TOKENS_JSON=["old-worker-token","new-worker-token"]
```

Place actual values in a mode-600 configuration file. First roll every API
replica to accept both, then rotate workers, then remove the old token from every
API. The legacy `BATCHFORGE_WORKER_TOKEN` also participates in the accepted set;
clear it when retiring that generation. No tenant credential is needed on GPU
hosts. Global token rotation is implemented; per-worker scoped identities and
mTLS workload identities remain separate work.

## Readiness and failure semantics

`/healthz` is process liveness. `/readyz` has a configurable two-second total
deadline and verifies the exact schema, `pg_is_in_recovery() = false`, and a
writable transaction mode. A reachable standby or read-only connection is not
ready. This check does not prove disk space, all table privileges, or latency
under sustained workload; those need independent monitoring.

PostgreSQL connection, pool, read-only, lock, statement-deadline, serialization
and deadlock failures return HTTP 503 with `Retry-After: 1`, without database
diagnostics in the response. Statement, lock and pool acquisition deadlines bound
resource occupancy. A 503 or dropped connection can still be ambiguous if a
transaction committed before its response was lost: retain the same
`Idempotency-Key` on batch creation/retry. Gateways must not replay POSTs
automatically. Worker completion uses the original attempt token when retrying;
an accepted replay is duplicate-safe and a superseded attempt gets 409.

Every API replica may run the bounded recovery sweep. Queue/batch/task locking
and attempt fencing are in PostgreSQL, not process memory. Physical model
execution remains at least once; only accepted result/usage commits are
idempotent. For database host failover, libpq multi-host connection strings can
use `target_session_attrs=read-write`, `sslmode=verify-full`, and a short
`connect_timeout`. Existing connections can fail during failover; clients still
need the above replay rules.

## Executable production topology specification

`ops/ha/render.py` generates per-node Patroni JSON (valid YAML) and etcd
environment files; accompanying systemd units provide service definitions. This
is a deployment specification, **not a cluster deployed to the current Pods**.
It requires three provisioned Linux database hosts with durable storage in three
independent failure domains, private routed connectivity, DNS/IP SAN certificates,
and working fencing/watchdog devices. The inventory example's IPs are examples.

```sh
python ops/ha/render.py operator-inventory.json --output /private/operator/bf-ha
```

The renderer refuses duplicate names/IPs/domains and refuses to overwrite
existing credentials. It generates shared PostgreSQL/Patroni passwords directly
into owner-only files and never prints them. Keep its output outside the repo.
Use a reviewed, pinned Patroni 4.1.5 installation with PostgreSQL 17 and etcd 3.6
as the specification baseline; binaries/images and upgrade patches must be
verified in the target environment rather than installed silently by this tool.

For each node, provision these prerequisites before enabling the supplied units:

1. Dedicated `postgres` and `etcd` users, `/var/lib/batchforge-ha/postgres` and
   `/var/lib/batchforge-ha/etcd` owned by their corresponding users, and the
   configured PostgreSQL binaries, etcd binary, and Patroni virtual environment.
2. Copy that node's `patroni.json` and `etcd.env` to `/etc/batchforge-ha`.
   Files must be readable only by the corresponding service owner. Provision
   `tls/etcd-ca.crt`, etcd server/peer/client certificate+key pairs, and a separate
   `tls/ca.crt` with PostgreSQL and Patroni server certificate+key pairs. Include
   each node's private IP/DNS in SANs. Keys are mode 600. Use a dedicated etcd CA
   and narrowly distribute its client certificates.
3. Permit ports 2379/2380 only among DCS/Patroni members; PostgreSQL 5432 only
   from trusted database/app nodes; Patroni HTTPS 8008 only from cluster operators
   and peers. Neither PostgreSQL nor etcd is an internet-facing service.
4. Provide a real `/dev/watchdog` to Patroni's service user and verify that loss
   of keepalive resets/fences the correct host. A container without this device
   cannot safely satisfy `watchdog.mode=required`. This spec deliberately refuses
   leadership without fencing. Test resets on isolated database hosts first.
5. Install/start `etcd.service` as `batchforge-etcd.service` on all three nodes;
   verify all endpoints with authenticated TLS `etcdctl endpoint health` and
   `endpoint status`. Start `patroni.service`, then verify `patronictl list`,
   exactly one primary, one selected synchronous standby and one extra standby.
   `ETCD_INITIAL_CLUSTER_STATE=new` is only for the initial empty deployment;
   replacing/adding a member requires etcd's member-management procedure.
6. Create a schema-owner role/database, run `backend.migrate --bootstrap`, grant
   the runtime role, and point API replicas at the writable database topology.
   Restore existing BatchForge data in a separate maintenance migration; these
   units do not adopt or replace the current live database automatically.

The spec uses strict synchronous replication with one synchronous standby and
`remote_apply`, so acknowledged commits have been replayed on that standby.
With no eligible synchronous replica it blocks writes rather than silently
dropping durability. The third database node provides a replacement standby;
the three etcd voters provide consensus. This durability/availability tradeoff
follows [Patroni replication modes](https://patroni.readthedocs.io/en/latest/replication_modes.html)
and [PostgreSQL synchronous replication](https://www.postgresql.org/docs/16/warm-standby.html).

Leader TTL is 30s, loop wait 5s, retry timeout 5s; a required watchdog with half-TTL
timeout fences a stalled leader before its lease can expire. These are safety
inputs, not a measured RTO guarantee. Validate partition behavior on the target
hardware using [Patroni watchdog guidance](https://patroni.readthedocs.io/en/latest/watchdog.html).
etcd uses authenticated TLS for both client and peer traffic, as described by
[etcd transport security](https://etcd.io/docs/v3.6/op-guide/security/).

Acceptance before claiming production HA must include: kill primary; sever its
DCS path while keeping client traffic alive; stop Patroni while leaving
PostgreSQL running; lose one etcd voter; lose two voters; exhaust a standby disk;
restore a backup plus archived WAL into a clean host; rewind/rejoin the fenced
former primary. Measure observed RTO, retained acknowledged commits, and duplicate
accepted-result/usage rows under real application traffic. No automatic
two-node promotion may bypass fencing. Off-host encrypted backups, WAL archiving,
retention and restore drills remain required even with replication.

## Evidence currently obtained

`tests/test_ha.py` uses real disposable PostgreSQL databases. It checks concurrent
migration, verify-only startup, refusal to create a schema implicitly, schema
drift/read-only readiness, worker credential overlap/retirement, and two actual
TCP API processes. The latter kills one process, replays accepted creation/result
commits through the survivor, recovers an expired attempt, fences its stale
result and verifies unique output and exactly-once accepted usage.

`ops/ha/physical_replication_probe.py` creates two private temporary PostgreSQL
processes, enables physical streaming replication and `remote_apply`, commits 64
rows, crashes its primary, checks all 64 on the read-only standby, confirms the
former primary stopped and explicitly promotes the standby. Its post-promotion
test write locally relaxes synchronous commit because this probe has no third
standby; the production strict configuration intentionally would block without
a replacement. The probe stops/removes only its own clusters.

```sh
python ops/ha/physical_replication_probe.py --report /private/operator/replication.json
```

This proves local PostgreSQL replication and fenced manual promotion mechanics.
It does not prove independent host survival, automatic failover, partitions,
etcd quorum behavior or cross-Pod database HA. The two current GPU Pods alone do
not supply the independently fenced three-node topology required by this spec.

## Managed PostgreSQL cutover

A managed database endpoint in an independent failure domain lets both GPU Pods
run API replicas without relying on either Pod's local database or an operator's
laptop. A free-tier project is useful to validate connectivity and correctness;
its availability guarantees, compute suspension, storage/connection limits and
backup retention must be assessed before treating it as a production SLA.

For Neon, use a **direct** hostname for migration and for this release's API
pool, with `sslmode=verify-full`, a trusted `sslrootcert` path, and optional
`channel_binding=require` if supported by the deployed libpq. Keep the original
DNS hostname so certificate/SNI verification works. On Debian-family Pods the
usual CA file is `/etc/ssl/certs/ca-certificates.crt`; verify the actual file.
Start with a small pool per API replica and budget aggregate connections across
all replicas, workers' API traffic, migrations and operator sessions.

This release sends statement/lock/idle-transaction limits as startup `options`.
Neon's `-pooler` endpoint rejects unsupported startup parameters, so replacing
the direct hostname with the pooler hostname is not a supported deployment
change yet. Its transaction-pooling compatibility needs an explicit alternative
for per-transaction limits and a separate integration test. See
[Neon connection errors](https://github.com/neondatabase/website/blob/main/content/docs/connect/connection-errors.md)
and [Neon connection guidance](https://github.com/neondatabase/website/blob/main/content/docs/get-started/connect-neon.md).

Use a dedicated database `batchforge`, a migration/schema-owner role, and a
separate SQL-created login `bf_runtime` without owner/superuser membership.
Issue its password using a trusted secret manager/operator session; do not paste
it in a command line, chat, Git or report. After schema creation, apply
`ops/ha/runtime_grants.sql` as the schema owner (change its database identifier if
using another name). Review preexisting PUBLIC grants. The low-privilege role
test runs the real batch lifecycle and confirms schema creation, tenant creation
and API-key updates are rejected.

`ops/ha/managed_cutover.py` supports an offline maintenance migration. Both
connection files must be mode 600 and define `DATABASE_URL`; use the target's
schema-owner role for this command, then deploy the runtime credential to API
hosts. The default command is read-only preflight:

```sh
python ops/ha/managed_cutover.py \
  --source-env-file /private/operator/source.env \
  --target-env-file /private/operator/managed-owner.env
```

Drain or explicitly cancel all pending/running tasks, close ingress, and stop
**all** source APIs, worker submissions, recovery loops and operator writers.
Keep the old processes stopped/fenced until cutover is either accepted or
rolled back. The tool requires an explicit operator assertion of this state:

```sh
python ops/ha/managed_cutover.py \
  --source-env-file /private/operator/source.env \
  --target-env-file /private/operator/managed-owner.env \
  --apply --assert-writers-stopped \
  --report /private/operator/cutover-result.json
```

The tool refuses a nonempty destination, mismatched schema, active source tasks,
an unverified remote TLS endpoint and a pooler hostname. It copies a repeatable
read snapshot of all app tables in FK-safe order, checks column definitions,
row counts and ordered COPY SHA-256 for every table, then atomically commits the
target data. It preserves idempotency and accepted attempt records. It never
overwrites a live destination or logs DSNs, passwords or row contents. An empty
schema can remain after a failed copy; application data rolls back.

Before opening ingress, start each Pod's API with its separate runtime-role
credential, check readiness and both Pods' access to historical batches, replay
an existing idempotency key, and run a new real inference batch. Only one source
of truth may accept writes. Rollback to the old database is safe only before new
target writes; after target writes, a reverse controlled migration is needed.
Delete/retire the old runtime credential only after validation and a protected
backup. No online CDC, automatic source fencing or managed-provider HA claim is
made by this copy tool.

## App-owned process supervision

`ops/supervise.py` is a Linux process watchdog for the application, not a Pod
boot service, database election agent or replacement for the deployment
platform. `ops/manage.py` remains the only launcher and PID authority. Its
PID/start-time/cwd checks determine whether a service is alive. Readiness is
observed separately and **never** triggers killing/restarting a live API during
a database outage. The watchdog itself never issues stop or kill commands.

Create a mode-600 JSON manifest directly in the mode-700 app `.runtime` directory:

```json
{
  "services": [
    {"role":"api","name":"replica-a","env_file":".runtime/api.env","python":".venv/bin/python","port":8082},
    {"role":"worker","name":"gpu0","env_file":".runtime/worker-gpu0.env","python":".venv/bin/python","port":9101},
    {"role":"engine","name":"gpu0","env_file":".runtime/engine-gpu0.env","python":".venv/bin/python","port":8200,"gpu":"0"}
  ]
}
```

These are examples: choose each Pod's actual interpreter/environment paths and
unique local service/metrics ports. Supported roles are api, worker, engine,
gateway and link; supervising itself is forbidden. An engine requires one
explicit GPU selector. The manifest accepts no shell commands and at most 64
services. Environment files must be owner-only regular files inside the app;
the existing `/root/batchforge-runtime/*.env` layout is also supported. Interpreter
paths are limited to app-owned `.venv/bin/python*` or that runtime's
`venv/bin/python*` and `engine-venv/bin/python*`, with safe executable ownership
and permissions. Symlinked config paths, duplicate identities/ports, external
config paths and unknown manifest fields are rejected.

Run it directly, or with the app manager's supervisor role when configured:

```sh
python ops/supervise.py --manifest .runtime/services.json
```

The optional environment variable `SUPERVISOR_MANIFEST` supplies the manifest
path. Only one watchdog can hold the app's flock. A dead owned process is
restarted with exponential backoff (default 1s to 60s); repeated startup failures
do not busy-loop. Its failure streak resets only after a minute of observed
stable process liveness. Before restarting an engine it queries CUDA memory on
that engine's selected device, requiring free memory of at least configured
`ENGINE_GPU_MEMORY_UTILIZATION * total + 512 MiB`. Probe failures or insufficient
free memory defer startup with backoff. It never stops another process to make
room. A manager start timeout is ambiguous and blocks further launches until a
valid owned process record returns or an operator investigates the app-owned
state; it cannot blindly launch a second GPU engine.

The watchdog atomically writes mode-600
`.runtime/supervision-<manifest-name>.json` with process IDs, readiness, restart
counts, failed starts and retry time. Counts survive watchdog restarts when the
manifest fingerprint matches. It never copies environment values, credentials,
commands, application output or manager diagnostics into that state. Its event
log contains only these sanitized transitions and rotates at approximately
256 KiB, retaining one previous file. Application stdout/stderr logs belong to
the app manager and require a separate retention policy.

SIGTERM/SIGINT stops **only the watchdog** and records that services were
preserved. A new watchdog attaches to the existing exact process records, so
watchdog upgrades do not restart engines or disrupt in-flight inference. For an
intentional application maintenance stop, stop the watchdog first, then stop
services through `ops/manage.py`; otherwise it will correctly restart a stopped
service. Processes do not survive a Pod/container reboot: manifest/config/data
persistence and launching the watchdog at Pod boot remain responsibilities of
the deployment platform.

`ops/test_supervise.py` checks control-plane backoff, identity-error refusal,
readiness/liveness separation, ambiguous-start quarantine and manifest path and
permission constraints. Its Linux-only subprocess drill creates a separate
temporary app, kills only that app's worker, verifies a new PID/restart counter,
then restarts the watchdog and proves the worker PID is preserved. Run the Linux
case on the target Pod; a skipped case on macOS is not evidence of Linux process
recovery.

## Controlled application control-plane outage

`ops/ha/controlplane_outage_probe.py` is an operator-run failure experiment for
the deployed primary topology, not whole-Pod power loss or database failover.
It defaults to a dry run. Use a separate, real benchmark large enough to retain
work during a 120-second outage; wait until all its batches have been accepted.
The probe requires the exact expected task count, active primary manifest,
existing API-a runtime environment and canonical watchdog environment. It never
creates tasks or reads prompt/output/query text.

```sh
python ops/ha/controlplane_outage_probe.py \
  --manifest /root/batchforge-runtime/app/.runtime/services-primary.json \
  --env-file /root/batchforge-runtime/api-a.env \
  --supervisor-env /root/batchforge-runtime/supervisor.env \
  --run-id bench-REPLACE_WITH_RUNNING_BENCHMARK \
  --expected-tasks 8192 --duration 120 --retire-link pod2 \
  --wait-for-submission-seconds 600 \
  --report /root/batchforge-runtime/app/.runtime/controlplane-outage-report.json
```

Review the dry-run plan, then add `--execute` in the released maintenance
window. The exact topology is API a/b on 8082/8083 and gateway edge on 8080.
Only these services, the watchdog, and an explicitly named obsolete link may
be stopped. GPU worker/engine identities are preserved and compared after the
experiment. All stop/start operations go through the existing app-owned
PID/start-time/cwd manager. A private singleton lock prevents duplicate probes.

The outage timer begins after all three primary URLs stop responding. Every
two seconds, the probe checks those URLs and reads one PostgreSQL MVCC snapshot
of this benchmark's aggregate task/batch/attempt states and accepted token
usage. It requires at least five additional durable completions attributed to
secondary worker IDs while the primary URLs are unavailable. It captures real
primary in-flight lease identities in RAM, requires recovery of expired
attempts, and checks a real old heartbeat receives `409 stale_attempt` after
restoration. It verifies completed tasks equal accepted attempts, no task has
two completed attempts, batch/task counters agree, and task/attempt/batch usage
totals agree. These checks prove accepted-result/usage fencing, not exactly-once
physical inference.

APIs restart sequentially with readiness checks, followed by the gateway and
watchdog. With `--retire-link`, only the named link is removed from the active
manifest after the outage succeeds; it has no task-path dependency. A later
assertion failure restores the original manifest and link. With the watchdog
running again, the probe pins API-a's exact process with a Linux pidfd, kills
only that process and requires a new ready PID while the gateway remains ready
through the surviving API. A private JSON report records bounds, progress,
restoration attempts, preserved PIDs and results without credentials/tokens.

Exceptions and SIGTERM/SIGINT enter restoration even if an assertion or database
read fails. Restoration attempts all services and the watchdog despite an
individual start failure. An interpreter SIGKILL, host loss, or exhausted
underlying platform cannot run `finally`; retain an independent operator
connection and the existing manifest for recovery. HTTP checks are sampled,
not proof of continuous unavailability between polls. The benchmark client's
expected polling errors during this deliberate outage must be reported
separately from durable task failures.

Control-flow tests cover successful retirement, secret-free reporting,
database failure restoration and retry after a partial restore. The real
PostgreSQL observation test uses a disposable database and actual API claims,
completion and lease recovery; it also proves the observer connection rejects
writes and detects an intentionally corrupted duplicate accepted-attempt
ledger. Neither set substitutes for executing the released production
experiment under real inference load.

To detach the released experiment from an SSH client, run this operator block
from the primary app directory using `/root/batchforge-runtime/venv/bin/python`. Set the
literal run ID from the specific 8192-task benchmark being tested. Never select
the latest run automatically. First run the same probe command without
`--execute`; the block below **executes the experiment** and is only for its
explicitly released fault window. Its optional wait is read-only: the selected
run must reach exactly 8192 accepted tasks and have a valid primary lease before
maintenance starts. More tasks, too little remaining work, interruption or a
ten-minute timeout refuse maintenance.

```python
import fcntl, hashlib, json, os, subprocess, sys, time
from pathlib import Path
from ops.manage import process_identity
from ops.supervise import private_write

root = Path('/root/batchforge-runtime/app')
os.chdir(root)
run_id = 'bench-REPLACE_WITH_EXACT_RUN_ID'
assert __import__('re').fullmatch(r'bench-[a-f0-9]{12}', run_id)
metadata = json.loads((root / '.runtime/outage-source.sha256.json').read_text())
assert set(metadata) == {'ops/ha/controlplane_outage_probe.py'}
for relative, expected in metadata.items():
    assert relative == 'ops/ha/controlplane_outage_probe.py'
    assert hashlib.sha256((root / relative).read_bytes()).hexdigest() == expected
prefix = root / '.runtime' / ('controlplane-outage-' + run_id)
report, log, record = (prefix.with_suffix(suffix) for suffix in ('.json', '.log', '.process.json'))
with (root / '.runtime/outage-launch.lock').open('w') as launch_lock:
    fcntl.flock(launch_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    assert not any(path.exists() for path in (report, log, record))
    command = [sys.executable, 'ops/ha/controlplane_outage_probe.py',
        '--manifest', str(root / '.runtime/services-primary.json'),
        '--env-file', '/root/batchforge-runtime/api-a.env',
        '--supervisor-env', '/root/batchforge-runtime/supervisor.env',
        '--run-id', run_id, '--expected-tasks', '8192', '--duration', '120',
        '--retire-link', 'pod2', '--wait-for-submission-seconds', '600',
        '--report', str(report), '--execute']
    descriptor = os.open(log, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, 'wb') as output:
        child = subprocess.Popen(command, cwd=root, stdin=subprocess.DEVNULL,
            stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
    time.sleep(.25)
    identity = process_identity(child.pid)
    assert identity and identity['cwd'] == str(root)
    try:
        private_write(record, {**identity, 'run_id': run_id, 'report': str(report)})
    except BaseException:
        import signal
        pinned = os.pidfd_open(child.pid)
        try:
            if process_identity(child.pid) == identity:
                signal.pidfd_send_signal(pinned, signal.SIGTERM)
        finally:
            os.close(pinned)
        raise
    print(json.dumps({'pid': child.pid, 'report': str(report), 'log': str(log)}))
```

The child remains on the server if the SSH client/laptop disconnects. The
probe's own flock still prevents overlapping experiments. Its private process
record permits an operator to pin that PID with `os.pidfd_open`, recheck
`ops.manage.owned_record(record)` and send SIGTERM through
`signal.pidfd_send_signal`; SIGTERM requests restoration, whereas SIGKILL cannot
run cleanup. The active app watchdog does not supervise the probe itself.
