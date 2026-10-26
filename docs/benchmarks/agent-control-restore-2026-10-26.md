# Agent control state: complete cold backup and actual restore

A complete owner-only Mac archive of the stopped native Agent qualification
cluster was transferred back to the existing Pod, restored into a new isolated
PostgreSQL17.11 data directory, started, checked and stopped successfully.
All **eight run states/results,385 events,16 child receipts and source quote
proofs** match the original closed cohort. The same answer scores replayed
without new inference. This qualifies a restore of terminal native qualification
state; Azure Agent migration, an Azure backup, interrupted workflow resumption
and whole-Pod HA remain unproven.

## A file hash check was insufficient to qualify the first copy

The first physical archive contained1147 database files and a manifest, with no
directory entries. The source had26 directories below its root, including13
empty directories. The restore preflight refused to reconstruct this missing
layout from assumptions; **no PostgreSQL startup was attempted for that copy**.
The old archive and its original unqualified metadata are preserved.
[Refusal receipt](../../ops/reports/agent-rag-span-restore-v1-preflight-2026-10-26.json).

PostgreSQL documents a filesystem backup as a complete cluster backup made with
the server shut down. The complete cluster layout and commit/WAL state must
remain together. [PostgreSQL17 documentation](https://www.postgresql.org/docs/17/backup-file.html).

The corrected v2 exporter records directories, including the root, as well as
all regular files. It refuses symlinks/special files, wrong ownership, unsafe
paths, changed files, a live postmaster marker and oversized input. All1147
file hashes equal the prior cold snapshot. The27 directory entries include the
root. The archive contains no executor environment, service configuration or
SSH/API plaintext key. Its database state remains private.

## Actual Mac-to-native restore

The owner-only Mac archive has6,061,650 compressed bytes and SHA
`02c7c80fd9560d77020ba0cd902d994e1ca6156d2e07ae9adbc6c4ac1a704fd1`.
Every file was reread from actual local archive bytes; files0600/directories0700
and local file/directory fsync were verified.
[Complete backup metadata](../../ops/reports/agent-rag-span-private-control-export-v2-2026-10-26.json).

The exact Mac archive bytes were returned through the existing authorized SSH
connection. The initial120-second transport observation timed out. The same
stable job directory/launcher was inspected again, and it was confirmed complete;
**the restore was not resubmitted**. Observation expiry was not treated as process
termination. The private archive and all restored files stay outside Git.

`evidence-api-agent-control-restore-20261026-v2` verified the received archive
SHA, member set/types, all directory entries, every file SHA and PG17 version.
`pg_controldata` reported a cleanly shut-down cluster before startup. The copied
data ran as UID65534 with a private Unix socket, no TCP listener and default
read-only transactions. A separately spawned120-second timeout bounds PostgreSQL
if its operator disappears; the operator also has a180-second outer timeout.
The child was held through a pidfd and stopped after verification. Its full
`/proc` identity helper returned null for this non-root process, so no managed
PID/cwd identity claim is made for the temporary PostgreSQL child.

The restored application's checks covered:

- Both migration ledgers remain at public11/Agent1; no migration or DDL ran.
- All eight original run IDs, revisions, result/state canonical SHA values and
  event heads match the pre-backup client evidence.
- Every event chain replays, totaling385 events; tenant isolation rejects an
  unrelated audience.
- Sixteen child receipts retain25583 accepted prompt and1521 output tokens.
  Generated-token sums, two calls/one tool per run and eight retained/zero active
  control counters reconcile.
- The original frozen public corpus replays all eight retrieval/source proofs
  and the same external scores. This is restoration verification of already
  produced answers, not an independent new quality experiment.
- Public task rows in this isolated control database remain zero. The restore
  program opens no external database connection and submits no model task.

The copied PostgreSQL stopped and its postmaster PID file was absent afterward.
Primary BatchForge source/configuration and all managed process identities
remained unchanged. The measured3.836-second operator interval excludes the
prior transfer; it is not an end-to-end disaster-recovery RTO.

[Exact executed validator and report](platform480-2026-10-25/evidence-api-agent-control-restore-20261026-v2-complete/validation.json),
[summary](../../ops/reports/agent-control-restore-v2-2026-10-26.json)
and the SHA-checked allowlisted transfer preserve this evidence. Raw database
archives, private logs and model response text were not committed.

## Remaining durability work

The existing serving Batch identity/usage remains in Azure and payloads in S3.
The Agent namespace still needs owner-authenticated Azure deployment. Native
qualification backups and this terminal-state restore do not replace that
production persistence. Recovery of accepted in-flight work after database
restore, scheduled backup retention/restore drills, cross-version migration and
independent-failure-domain service recovery need separate qualifications.
