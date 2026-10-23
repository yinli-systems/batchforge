# Operations

The native deployment runs in `/root/batchforge-runtime/app` on the first Pod.
The workspace network volume rejected required file-mode and copy operations,
so virtual environments and live database data use local app-owned directories.
Existing workloads are not stopped or reconfigured.

| Resource | Location |
| --- | --- |
| API + worker interpreter | `/root/batchforge-runtime/venv/bin/python` |
| GPU engine interpreter | `/root/batchforge-runtime/engine-venv/bin/python` |
| Private runtime configuration | `/root/batchforge-runtime/access.env`, `gpu1.env` to `gpu3.env` |
| API listener | `127.0.0.1:8080` |
| Engine listeners | `127.0.0.1:8100` through `8103` |
| Worker metrics | `127.0.0.1:9101` through `9104` |
| PostgreSQL environment and data | `/opt/batchforge-pg/` |
| PostgreSQL Unix socket | `/opt/batchforge-pg/socket` |
| App process identity files and logs | `app/.runtime/` |

Configuration files are mode 600; app runtime directories are private. Database
data and its Unix socket belong to the dedicated `batchforgepg` OS user.
PostgreSQL does not listen on TCP. Secrets and runtime data are ignored by Git
and excluded from container build contexts.

## Access

The initial deployment binds loopback and is accessed using SSH forwarding.
The desktop session forwards its local port 8787 to the Pod's API port 8080.
Open `http://127.0.0.1:8787`; the tenant key is in the laptop's ignored private
`.runtime/access.env`. Do not place the key in a URL.

To recreate the tunnel using the Pod's verified direct mapping:

```sh
ssh -N -L 127.0.0.1:8787:127.0.0.1:8080 -p 13253 \
  -i ~/.ssh/id_ed25519 root@91.199.227.82
```

Runpod may change the direct IP/port after lifecycle changes. Obtain current
mapping from the user-authorized Pod before reconnecting. The HTTP API and GPU
engine ports are not publicly exposed by this setup.

## Manage the first Pod

From the deployed app directory, operate only managed app instances:

```sh
/root/batchforge-runtime/venv/bin/python ops/manage.py status api
/root/batchforge-runtime/venv/bin/python ops/manage.py logs worker --name gpu0
/root/batchforge-runtime/venv/bin/python ops/manage.py stop worker --name gpu0
/root/batchforge-runtime/venv/bin/python ops/manage.py start worker --name gpu0 \
  --env-file /root/batchforge-runtime/access.env \
  --python /root/batchforge-runtime/venv/bin/python
```

Use `gpu1.env` etc for other workers. Engine instances use the corresponding
`--name gpuN --gpu N --port 810N`, the engine interpreter, and the same context
and memory configuration. PID records include Linux process start identity and
working directory; the manager refuses to signal a mismatched process.

The initial manager is not an OS boot service or a high-availability supervisor.
Disconnecting SSH does not stop its detached app children, but Pod replacement
or local-disk loss requires rebuilding services and restoring data. Use a
managed PostgreSQL deployment with appropriate backups for customer production.

Use `/healthz` for liveness, `/readyz` for database readiness, and the authenticated
console for operational state. A live process alone does not establish that an
engine is ready or that queued tasks are progressing.

## Back up and restore

Install the optional `pgserver==0.1.4` development utility for its PostgreSQL
binaries. The backup helper uses `pg_dump` directly, never the package's global
process discovery/cleanup manager.

```sh
python devtools/backup_postgres.py --env-file /root/batchforge-runtime/access.env
```

It creates an owner-only custom-format snapshot in `.runtime/backups` and checks
it with `pg_restore --list`. This path has been exercised on the running Pod.
Creating a local snapshot is not an off-host disaster-recovery policy. Retention,
off-host transfer and regular restore checks must be configured for the deployment.
The initial test-only snapshot was additionally saved in the operator's private
`.runtime/backups` after auditing that it contained only generated fixtures.

On a non-root development machine, verify a snapshot in an entirely new temporary
cluster:

```sh
python devtools/verify_backup.py .runtime/backups/<snapshot>.dump
```

This path was exercised and verified 2,091 restored test tasks and matching usage.
Restore customer data only into an explicitly selected empty database with
`pg_restore --no-owner --exit-on-error`; the backup helper never restores or
overwrites a database.
