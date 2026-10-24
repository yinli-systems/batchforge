# Storage admission v8 → v10: executed rollout and operator procedure

The reviewed procedure below was executed on2026-10-24. The
[final deployment report](../ops/reports/storage-admission-live-deployment-2026-10-24.json)
has SHA `1ae36514cfb4dcc6d4e47ba04400c0915fc717d71845040b82bd9d41867e3995`.
It records schema9/10 committed together at03:44:43UTC and watchdog restoration
by03:44:49UTC. Both API replicas are ready with retention enabled, ledger
invariants zero and old20 bytes unchanged on both APIs/gateway. GPU engine and
worker identities were preserved. Publisher remained **OFF** during this storage
roll. It was enabled by the separate, completed policy operation described below.

The stable job was`storage-admission-v10-5448ee8-apply-v1`, PID192529/start33690815,
using operation`storage-admission-v10-5448ee8-v1`; it completed and exited. A fresh
schema8 backup127,281,162 bytes with SHA
`201aa1319a7a2aaee2ac949f82a1b98fdbff7f3b9947164e1a16db50cb1b5917`
was verified off-Pod at0600 before apply. [Capacity](../ops/reports/storage-admission-pre-migration-capacity-2026-10-24.json),
[backup](../ops/reports/storage-admission-pre-migration-backup-2026-10-24.json),
[off-Pod copy](../ops/reports/storage-admission-pre-migration-offpod-2026-10-24.json)
and [actual source ACL](../ops/reports/storage-admission-production-lowpriv-2026-10-24.json)
are separate evidence. The first prepare was correctly refused for group-writable
`ops/`; [the fix](../ops/reports/storage-admission-prepare-permission-fix-2026-10-24.json)
only narrowed0775→0755 while preserving inode/owner/source bytes. API lock modes
were similarly narrowed0644→0600 without replacing their inodes.

Production default-tenant policies are50,000 retained tasks/320MiB charged task
payload/1MiB response/32KiB error, plus input files2,000, uploads32,
idempotency4,096 rows and128MiB/64KiB per key, blob generations2,000, archive
runs2,000, chunks20,000 and128 leases per reader class. Completion retains its
full response/error allowance until verified payload purge converts/releases it.
Limits are finite admission bounds: exhaustion rejects new allocations. There
is no automatic metadata/idempotency/history retirement or global physical
PostgreSQL/WAL quota allocator. Do not describe these limits as indefinite
operation or unbounded storage growth.

## Subsequent current-schema restore and publisher activation

The [fresh schema10 backup](../ops/reports/schema10-fresh-backup-raw-2026-10-24.json)
is 127,944,921 bytes with dump SHA
`302d3f2e7bf4e9b9246b663c4bad9cfc12d5e2b32bd212fc408f96d5ea44b2bd`.
The [actual same-Pod PostgreSQL 17.11 restore](../ops/reports/schema10-postgres17-local-restore-2026-10-24.json)
has report SHA `9ff9e72e7c125e21dad89517ed09fd1337badb141fe2bd579d5ead627e065613`.
It passed schema/checksum, counter, usage and all four storage-ledger checks in
an unprivileged Unix-socket-only temporary cluster, then stopped and removed it.
This restore uses `--no-owner --no-acl`; production permissions are proven by
the separate live ACL inventory, not by the dump restore.

The new schema10 dump has **not** been copied off-Pod. Automatic approval
rejected that export because explicit user authorization was missing; that
authorization is pending. The previous schema8 off-Pod proof above remains
historical evidence, not a current-schema disaster-recovery qualification.

The unique job `final-api-policy-5448ee8-apply-v1` prepared at 03:55:52 UTC and
completed its API policy roll at 03:55:58 UTC. Its
[final report](../ops/reports/final-api-policy-live-deployment-2026-10-24.json)
has SHA `c5ac22ab76ebceed5e310884f6cfbc4b6fefc1b57a83086097f9cfa06c4f983d`.
It made Qwen235 the default while preserving all legacy model IDs and cost-aware
scheduling, leased readers and retention enforcement. The publisher now covers
only batches created after **2026-10-24 03:59:21.961089 UTC**, waits at least 120
seconds after terminal state and purges at most 1,000 completed tasks per page.
Completed tasks retain their response/error allowance until verified purge;
publication eligibility alone does not release it.

API-a/b became PID 195813/start 33759111 and PID 195824/start 33759340; watchdog
PID 195833/start 33759521 was restored. The engine, adaptive worker, gateway,
their configuration bytes and calibrated profile remained unchanged. Both APIs
read the old archived20 output byte-identically after rolling. The
[final readiness observation](../ops/reports/final-api-policy-final-readiness-2026-10-24.json)
records HTTP 200 on 8080/8082/8083 at 03:56:33 UTC. These identities and timestamps
qualify that observation only; they are not instructions to adopt later PIDs.

Future uses of this procedure need a new reviewed checkpoint/bundle/operation
and fresh evidence; **do not rerun the completed operation** or reuse its PID
bindings. The commands below preserve the exact historical execution inputs.

## Frozen inputs

Source checkpoint: `5448ee8aabe2d55b8575894c6a39f0dd6d340221`.

| Artifact | SHA256 |
| --- | --- |
| `.runtime/storage-rollout-prep/storage-backend-5448ee8-v1.tar.gz` | `f344c1420f69826d9a3c8e45ef41aa4e70c3e9ebc20e9f9b4116c739a20e00ee` |
| `ops/ha/deploy_storage_admission.py` | `91bfdd92d96a5f60aa6ede763883589c65f0f92a8f3653efd88497c985714456` |
| Historical `result-archival-pre-purge-restore-2026-10-24.json` | `bf152b0268f56fb94a3af078d9baec23e01d57cf30a0e04bda9975d0f7a366aa` |
| Historical `result-archival-live-production-2026-10-24.json` | `9d9a698179e6ce28dced742929c2374139d6af22faed358bea1476b5afd11a6f` |

The deterministic Python `tarfile` bundle is 39,519 bytes, contains exactly
these eleven regular files, and has already passed the controller's archive
validator locally. The private bundle manifest records each member checksum.

```text
backend/app.py
backend/auxiliary_admission.py
backend/blob_storage.py
backend/config.py
backend/migrate.py
backend/migrations/0009_retention_admission.sql
backend/migrations/0010_auxiliary_metadata_admission.sql
backend/result_archival.py
backend/retention.py
backend/store.py
ops/ha/runtime_grants.sql
```

The helper is staged separately. Do not extract the backend bundle manually or
copy the whole application: the controller validates, snapshots, copies and
evicts only the eight changed backend modules' bytecode under both API role
locks. It preserves unrelated caches. No worker/model source is in this bundle.

## Read-only preparation already completed

[The preparation report](../ops/reports/storage-rollout-readonly-preparation-2026-10-24.json)
records the remote helper hashes/signatures and public workload byte counts.
The new controller was compiled/imported **in RAM** against the existing remote
schema8 import tree with `python -B` and `CUDA_VISIBLE_DEVICES=''`. No constructor,
database connection, HTTP call, GPU call or remote file write was made. The
following existing dependencies import successfully: `manage`, `supervise`,
`engine_members`, `host_memory`, `llama_backend`, `PolicyRoll`, source snapshot
helpers, adaptive controller, final-policy `values`, retune/switch helpers,
`engine_matrix`, worker KV helpers, scheduling and the old schema registry.

`finalize_api_policy` imports the old v8 registry in that running controller
process; only its unchanged environment parser is used. Migration SQL is taken
from the verified bundle plus immutable local v1–v8 files, not from that cached
registry. Each newly started API imports the newly copied v10 registry; scoped
backend cache eviction covers timestamp and unchecked-hash `.pyc` variants.
The source-PID/environment guards bind actual dependencies again at prepare.

The existing API lock files are root-owned regular files with mode **0644**.
The controller intentionally requires private locks. After GO, change just
`app/.runtime/api-a.lock` and `api-b.lock` to **0600**, preserving their inode;
never unlink/replace a live role lock. Both APIs currently use `MAX_ATTEMPTS=3`.
The existing verified PG17 client wrappers are executable at
`/root/batchforge-runtime/postgres-client-17/bin/{pg_dump,pg_restore}`.

## Capacity gate for the next mixed100

Frozen public source `.runtime/qwen235b-v1/workload-20.jsonl` has SHA
`52f84aec744faa37b8d6d2166693536f9f8d8778642b29a58b4e6aa08ac2fb4f`.
For five repetitions, adding `min_tokens=output_cap` exactly as the benchmark
does, its 100 canonical task bodies occupy **2,575,465 bytes**. Natural bodies
without that field occupy 2,573,565 bytes. This fixture has only integer numeric
values; UTF-8 JSON with comma/colon spaces has the same byte length as PostgreSQL
`jsonb::text` for this string/integer/bool/null subset. Key ordering does not
change byte length. A different body/float serialization requires a new count.

At default three attempts, the new cohort reserves:

```text
100 × (1,048,576 response + 4 × 32,768 errors) + 2,575,465 bodies
= 120,540,265 bytes = 114.9561548 MiB
```

Parent's earlier ~02:53 sample had body+response of 199,848,128 bytes and did not
include error bytes. Against 320 MiB (335,544,320 bytes), that leaves only
**15,155,927 bytes / 14.4538 MiB** after this new cohort, before historical errors
and all subsequent cohorts. That historical arithmetic is not current quota.

After the final timed run is terminal and globally drained, execute the
SELECT-only `ops/ha/retention_bootstrap_estimate.sql` through the existing API
runtime credentials, with `default_transaction_read_only=on`. For default tenant:

```text
required = fresh bootstrap_payload_charge_bytes + 120,540,265
require required <= 335,544,320
require fresh retained_tasks + 100 <= 50,000
require global pending/running count == 0
```

Also check all auxiliary observed usage against the proposed limits: input
files 2,000 (including in-flight future-file reservations), uploads32,
idempotency4,096 rows **and** 128 MiB of reserved bytes /64 KiB per key,
blob generations2,000, archive runs2,000, chunks20,000 and each reader class128.
Do not automatically widen limits if a gate fails. The controller rechecks exact
ledgers and minimum100 response/error reservations inside the owner transaction;
the external fixture-body calculation is the stronger gate for this known cohort.
Actual admission remains atomic if concurrent work consumes credits later.

The released read-only preflight at03:30:38–03:30:40UTC subsequently measured
26,823 retained tasks, zero active work, and **205,935,962 bytes** of exact
bootstrap charge (body162,934,113 + response42,902,551 + error99,298). Adding
the reviewed fixed100 reserves326,476,227 bytes, leaving **9,068,093 bytes**.
All six historical structural checks were zero and auxiliary capacities passed
with explicit headroom for the next100. This is enough for that cohort, not for
an additional QA20 while its full response/error allowances remain reserved.
Do not increase quota or purge history to fit the QA. Root must arrange the
new-schema backup/restore and future publisher policy, then wait for verified
cohort archival to release reservation before admitting QA20. Recheck if any
new work is submitted after this observation.

Logical JSON charge is not physical PostgreSQL/TOAST/index/WAL usage. The project
has a separately observed 1 GiB branch logical-size limit; the earlier physical
`pg_database_size` was292,364,288 bytes. Neither value proves future headroom.

## Commands after explicit GO

Use the existing SSH key and known-host file. The following local shell array
is only command plumbing; no credential values are arguments.

```bash
cd '/Users/kevin/Documents/ChatGPT/app 3'
umask 077
BF_SSH=(ssh -p 13253 -o BatchMode=yes -o ConnectTimeout=10 \
  -o StrictHostKeyChecking=yes -o UserKnownHostsFile=/private/tmp/batchforge_known_hosts \
  -o ForwardAgent=no -i /Users/kevin/.ssh/id_ed25519 root@91.199.227.82)
BF_SCP=(scp -P 13253 -o BatchMode=yes -o StrictHostKeyChecking=yes \
  -o UserKnownHostsFile=/private/tmp/batchforge_known_hosts \
  -i /Users/kevin/.ssh/id_ed25519)
```

1. Stage assets into a private **artifact** directory, separate from the
   controller's initially empty **operation** directory. Copy the SQL preview
   and historical reports as assets; only the controller later changes live
   backend source. Set all staged assets to0600. Confirm the archive/controller
   hashes match the table above before any preparation.

```bash
"${BF_SSH[@]}" 'umask 077; mkdir -m 700 /root/batchforge-runtime/app/.runtime/storage-release-v10-5448ee8-v1'
"${BF_SCP[@]}" .runtime/storage-rollout-prep/storage-backend-5448ee8-v1.tar.gz \
  root@91.199.227.82:/root/batchforge-runtime/app/.runtime/storage-release-v10-5448ee8-v1/backend.tar.gz
"${BF_SCP[@]}" .runtime/storage-rollout-prep/deploy_storage_admission-5448ee8.py \
  root@91.199.227.82:/root/batchforge-runtime/app/ops/ha/deploy_storage_admission.py
"${BF_SCP[@]}" ops/ha/retention_bootstrap_estimate.sql \
  root@91.199.227.82:/root/batchforge-runtime/app/.runtime/storage-release-v10-5448ee8-v1/bootstrap-estimate.sql
"${BF_SCP[@]}" ops/reports/result-archival-pre-purge-restore-2026-10-24.json \
  root@91.199.227.82:/root/batchforge-runtime/app/.runtime/storage-release-v10-5448ee8-v1/historical-restore.json
"${BF_SCP[@]}" ops/reports/result-archival-live-production-2026-10-24.json \
  root@91.199.227.82:/root/batchforge-runtime/app/.runtime/storage-release-v10-5448ee8-v1/production20.json
```

   Apply the two role-lock permission changes only after validating each is the
   expected root-owned regular, non-symlink file. Verify source/context/PID
   identity again if another operator changed the selected engine meanwhile.

   Then run this read-only capacity calculation on the Pod, from `app/` using
   `venv/bin/python -B`. It writes only an aggregate private report and refuses
   this known cohort if its exact bodies would exceed policy.

```python
import hashlib, json
from pathlib import Path
import psycopg
from psycopg.rows import dict_row
from ops.supervise import private_read, private_write
from ops.ha.finalize_api_policy import values

root = Path('/root/batchforge-runtime/app')
artifacts = root/'.runtime/storage-release-v10-5448ee8-v1'
api = [values(private_read(root.parent/f'api-{name}.env')) for name in ('a', 'b')]
assert all(int(env.get('MAX_ATTEMPTS', '3')) == 3 for env in api)
public = (root/'.runtime/qwen235b-v1/workload-20.jsonl').read_bytes()
assert hashlib.sha256(public).hexdigest() == '52f84aec744faa37b8d6d2166693536f9f8d8778642b29a58b4e6aa08ac2fb4f'
body_bytes = 0
for line in public.splitlines():
    body = json.loads(line)['body']
    body['min_tokens'] = body.get('max_tokens', body.get('max_completion_tokens'))
    body_bytes += len(json.dumps(body, ensure_ascii=False, sort_keys=True).encode())
body_bytes *= 5
assert body_bytes == 2575465  # requalify serialization/body if source changes
with psycopg.connect(api[0]['DATABASE_URL'], row_factory=dict_row, connect_timeout=5,
        options='-c default_transaction_read_only=on -c statement_timeout=10000') as conn:
    assert conn.execute('SELECT max(version) AS n FROM batchforge_schema_migrations').fetchone()['n'] == 8
    active = conn.execute("SELECT count(*) AS n FROM tasks WHERE status IN ('pending','running')").fetchone()['n']
    estimates = conn.execute(private_read(artifacts/'bootstrap-estimate.sql').decode()).fetchall()
    default = next(row for row in estimates if row['tenant_id'] == 'default')
    auxiliary = conn.execute("""SELECT 'input_files' AS resource,
        (SELECT count(*) FROM files WHERE tenant_id='default' AND kind='input') +
        (SELECT count(*) FROM blob_upload_reservations WHERE tenant_id='default') AS rows,0::bigint AS bytes
      UNION ALL SELECT 'upload_reservations',count(*),0 FROM blob_upload_reservations WHERE tenant_id='default'
      UNION ALL SELECT 'idempotency',count(*),coalesce(sum(greatest(65536,coalesce(octet_length(response::text),0))),0)
        FROM idempotency WHERE tenant_id='default'
      UNION ALL SELECT 'blob_generations',count(*),0 FROM blob_objects WHERE tenant_id='default'
      UNION ALL SELECT 'archive_runs',count(*),0 FROM result_archive_runs WHERE tenant_id='default'
      UNION ALL SELECT 'archive_chunks',count(*),0 FROM result_archive_chunks WHERE tenant_id='default'
      UNION ALL SELECT 'input_read_leases',count(*),0 FROM blob_read_leases WHERE tenant_id='default'
      UNION ALL SELECT 'result_read_leases',count(*),0 FROM result_read_leases WHERE tenant_id='default'""").fetchall()
limits = {'input_files':2000,'upload_reservations':32,'idempotency':4096,
    'blob_generations':2000,'archive_runs':2000,'archive_chunks':20000,
    'input_read_leases':128,'result_read_leases':128}
remaining = 335544320 - int(default['bootstrap_payload_charge_bytes']) - 120540265
passed = (active == 0 and remaining >= 0 and default['retained_tasks'] + 100 <= 50000
    and all(row['rows'] <= limits[row['resource']] for row in auxiliary)
    and next(row['bytes'] for row in auxiliary if row['resource'] == 'idempotency') <= 134217728)
private_write(artifacts/'capacity-preview.json', {'passed':passed,'active_tasks':active,
    'bootstrap':[{key:int(value) if key!='tenant_id' else value for key,value in row.items()} for row in estimates],
    'fixed100_body_bytes':body_bytes,'fixed100_total_new_charge_bytes':120540265,
    'default_remaining_after_exact_fixed100_bytes':remaining,
    'auxiliary':[{key:int(value) if key!='resource' else value for key,value in row.items()} for row in auxiliary]})
if not passed:
    raise RuntimeError('Storage policy cannot admit the reviewed mixed100; do not widen automatically')
```

2. Generate one fresh snapshot using the existing `devtools.backup_postgres`
   and verified client wrappers. Run this Python on the Pod from `app/` using
   `venv/bin/python -B`, with its stdout/stderr redirected to a private log.
   A once marker prevents an ambiguous client disconnect from duplicating work.
   Only print the report/snapshot paths, byte count and SHA; never print `dsn`.

```python
import os
from pathlib import Path
from ops.ha.deploy_storage_admission import bound_owner_dsn
from ops.ha.finalize_api_policy import values
from ops.supervise import private_read
from devtools.backup_postgres import select_bin_dir, preflight, create_backup

os.umask(0o077)
root = Path('/root/batchforge-runtime/app')
artifacts = root / '.runtime/storage-release-v10-5448ee8-v1'
fd = os.open(artifacts/'backup.once', os.O_CREAT|os.O_EXCL|os.O_WRONLY, 0o600)
os.close(fd)
runtime = [values(private_read(root.parent/f'api-{name}.env'))['DATABASE_URL']
           for name in ('a', 'b')]
dsn = bound_owner_dsn(private_read(root.parent/'neon-owner.env'), runtime)
environment = dict(os.environ)  # owner DSN is never added to process env
bins = select_bin_dir(root.parent/'postgres-client-17/bin', environment)
metadata = preflight(dsn, bins, environment)
snapshot, report = create_backup(dsn, bins, root/'.runtime/backups', environment,
    metadata, artifacts/'fresh-backup-raw.json')
```

   A failed or disconnected run is inspected by the same marker/report/PID;
   do not rerun it under another ID. `pg_dump --list` is not a fresh restore.
   The helper separately requires the previously passed historical v8 restore.

3. Copy `fresh-backup-raw.json` and its exact named dump **off the Pod** into a
   local0700 directory under `.runtime/storage-rollout-prep/off-pod/`, with
   `umask077` and files0600. Use the exact snapshot path from the private JSON,
   passed to `scp` as a subprocess argument; do not interpolate DSNs or shell
   command substitution. Verify local dump SHA and size against that JSON.
   The dump remains ignored/private and is never copied into `ops/reports`.

   Build an attested metadata copy locally, preserving the original report:

```python
import hashlib, json, os
from pathlib import Path
from datetime import datetime, timezone
from devtools.backup_postgres import write_private_json

local = Path('.runtime/storage-rollout-prep/off-pod')
report = json.loads((local/'fresh-backup-raw.json').read_bytes())
snapshot = local / Path(report['snapshot_path']).name
assert snapshot.stat().st_mode & 0o777 == 0o600
with snapshot.open('rb') as stream:
    sha = hashlib.file_digest(stream, 'sha256').hexdigest()
assert sha == report['snapshot_sha256']
assert snapshot.stat().st_size == report['snapshot_bytes']
report.update(off_pod_copy_verified=True, off_pod_copy_mode='0o600',
    off_pod_copy_sha256=sha,
    off_pod_copy_verified_at=datetime.now(timezone.utc).isoformat())
write_private_json(local/'fresh-backup.json', report)
```

   Copy only this attested JSON back to the artifact directory as
   `fresh-backup.json`; retain both raw and attested metadata. The deployment
   helper checks the attestation, age≤1h, actual private on-Pod dump SHA/size and
   TLS/tool compatibility again. Refresh backup if this expires before apply.

4. Write `proof-spec.json` privately on the Pod. All three records are explicitly
   hash pinned; the fresh hash is generated only after actual off-Pod verification.

```python
import hashlib
from pathlib import Path
from ops.supervise import private_read, private_write

artifacts = Path('/root/batchforge-runtime/app/.runtime/storage-release-v10-5448ee8-v1')
files = {'fresh_backup': 'fresh-backup.json',
         'historical_restore': 'historical-restore.json', 'production': 'production20.json'}
spec = {name: {'path': str(artifacts/file),
              'sha256': hashlib.sha256(private_read(artifacts/file, 2*1024**2)).hexdigest()}
        for name, file in files.items()}
assert spec['historical_restore']['sha256'] == 'bf152b0268f56fb94a3af078d9baec23e01d57cf30a0e04bda9975d0f7a366aa'
assert spec['production']['sha256'] == '9d9a698179e6ce28dced742929c2374139d6af22faed358bea1476b5afd11a6f'
private_write(artifacts/'proof-spec.json', spec)
```

5. Prepare from the Pod's application directory. This writes only private
   snapshots/report. It binds schema8, global active0, exact current owned
   PID/start/cwd identities, environment/manifest bytes, immutable sources,
   dependencies and proofs. Do not put artifacts inside its operation directory.

```bash
cd /root/batchforge-runtime/app
umask 077
CUDA_VISIBLE_DEVICES='' /root/batchforge-runtime/venv/bin/python -B \
  -m ops.ha.deploy_storage_admission \
  --archive .runtime/storage-release-v10-5448ee8-v1/backend.tar.gz \
  --sha256 f344c1420f69826d9a3c8e45ef41aa4e70c3e9ebc20e9f9b4116c739a20e00ee \
  --proof-spec .runtime/storage-release-v10-5448ee8-v1/proof-spec.json \
  --access-env .runtime/scheduler-v6/benchmark.env \
  --operation storage-admission-v10-5448ee8-v1
```

6. Inspect the prepared private report, recheck the exact-body capacity gate,
   and obtain root's apply GO. Execute **the identical command plus `--apply`**
   in one detached, owner-recorded job with an exclusive once marker and private
   logs. Record PID/start/cwd and inspect that same job after client interruption;
   do not launch another apply process. Prepared replay refuses all PID/source,
   environment, proof or snapshot drift rather than overwriting its baseline.

   The apply pauses only the owned watchdog, copies reviewed backend files under
   both API role locks, rolls A then B with the flagfalse and readiness under
   schema8, and preacquires the full NOWAIT DDL window. One owner transaction
   holds the migration advisory lock, validates exact v1–v8 checksums, executes9
   and10, applies runtime grants, sets default-tenant policy and checks ledgers
   before COMMIT. It rolls A/B with flagtrue under exact schema10, verifies old20
   bytes through both APIs and gateway, and restores the watchdog. Engine,
   worker and gateway PID/config identities remain guarded.

7. If precommit work fails, source/env restoration is permitted; it is not
   reported successful unless rollback and readiness finish. Once COMMIT is
   attempted, including an uncertain acknowledgment, keep the new compatible
   registry/source and set flagsfalse for **forward repair**. Never downgrade
   schema or reintroduce a v8-only registry. Foreign source/PID changes or unsafe
   recovery leave explicit manual-recovery status and may intentionally keep the
   watchdog stopped. Inspect the private report before any further service action.

8. After success, save sanitized report/hash evidence, currentAPI schema/roles,
   retention/auxiliary ledger checks and protected identities. Root then creates
   a new v10 snapshot/off-Pod copy and isolated PG17 restore qualification. The
   final default-model/future-cutoff policy is a separate reviewed operation;
   neither the historical v8 restore nor this migration is relabeled as that
   future v10 restore/publisher proof.
