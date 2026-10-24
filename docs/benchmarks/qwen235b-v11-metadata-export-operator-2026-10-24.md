# Private matched metadata-export operator

This is a local preparation artifact for the root operator. It has not run on the serving Pod or consumed the real terminal candidate. All 17 local tests pass. Tests use private synthetic metadata and mock exporter children, plus harmless local pipe-bound tests, an actual threaded different-label exclusion test, and unsafe-mode/hardlink lock refusal cases. No production service, frozen controller/exporter/comparer/receiver source, model profile or database business state was changed.

The tool is `.runtime/export_matched_v11_operator.py`. Its reviewed source and tests are preserved beside this note as `.operator-source.py.txt` and `.operator-tests.py.txt`.

| Binding | Value |
| --- | --- |
| Operator source SHA256 | `36336209d22979e88be63bebe8318a6536c553095a9a0d99aba998adaffb9438` |
| Local synthetic tests SHA256 | `2a2f2f73f4d1bd535d31ad7d3eea5b7b282cd337f7cf651f388927cce3930b29` |
| Matched operation | `qwen235b-v11-matched-v1` |
| Matched prepared plan SHA256 | `8d1b3745c9dfb9a12f11394fcc3d57af7150d33c3a46414646f13c2b02f7a45e` |
| Frozen exporter | `a73b14599f3a0d5656fcaf2aede81e372a4540b5a3e177011701b539549f3f03` |
| Approved receiver | `5534f19cb83c5408af41cd6497d3c6307039ca6748cb93f8c6c3512d6da592d6` |

## Paths and scope

The original five-file export locations are retained for existing consumers:

```text
/root/batchforge-runtime/app/.runtime/sustained-matched/qwen235b-v11-matched-v1/exports/baseline/
/root/batchforge-runtime/app/.runtime/sustained-matched/qwen235b-v11-matched-v1/exports/candidate/
```

The new operator's private state is separate:

```text
/root/batchforge-runtime/app/.runtime/sustained-matched/qwen235b-v11-matched-v1/metadata-export-operators/metadata-export-v1/
  plan.json
  journal.json
  operation.lock
  execution-marker.json                 # execute only
  validation-receipts/                  # preview stdout, not transported
  receipts/baseline-export-receipt.json
  receipts/candidate-export-receipt.json
  payload/comparison.json
  payload/pair-controller-journal.json
  transport-manifest.json
```

Each round's five-file directory contains only its original job-prefixed JSON files. Stdout receipts never enter those directories. All new operator directories are exclusive mode0700; state, receipts, payload copies and the transport manifest are mode0600 with file and parent-directory fsync. A pair-wide `metadata-export.lock` holds a nonblocking exclusive flock across the complete prepare/execute critical section for every operation label. The existing per-operation flock is also retained. Both locks require an owned, regular, single-link mode0600 file opened with `O_NOFOLLOW`; different labels cannot race to write the shared export directories. Existing export directories, operator state or output files are never silently overwritten or reused.

The script invokes the frozen exporter using its default output locations. The exporter retains its original source/environment/PID/runtime/ledger guards. The receiver's schema-only pair-journal loader imports the pinned frozen exporter definitions; the Pod does not need the Mac-only plotting script. The comparison is recomputed with the frozen comparer and its exact SHA must match the terminal pair journal.

## Prepare after both rounds are terminal

Keep the original source/environment/PID freeze until this export finishes. The pair journal must already have `phase=complete`, no pending action, the exact operation/plan/jobs, consistent cost and preserved initial/final cleanup proof. Both real reports and their collection/audit artifacts must exist. A partial 720-task plan is acceptable only when its original `passed=false` is retained and all accepted work completed exactly once.

Use the serving parent virtual environment, not an app-local `.venv`. The root operator stages only the independently reviewed new private helper and approved receiver if needed, without modifying the frozen serving source.

```sh
BF_APP=/root/batchforge-runtime/app
BF_PY=/root/batchforge-runtime/venv/bin/python

"$BF_PY" "$BF_APP/.runtime/export_matched_v11_operator.py" \
  --export-operation metadata-export-v1
```

Default preparation performs two exporter default validations and exclusively creates the private operator plan, journal and validation receipts. It creates **no five-file export directories**, submits no model task and changes no business policy. It is not a zero-filesystem-write action. Retain the printed `operator_prepared_sha256` through the trusted root channel and review the prepared metadata binding before execution.

## Execute once with the exact reviewed operator plan SHA

```sh
"$BF_PY" "$BF_APP/.runtime/export_matched_v11_operator.py" \
  --export-operation metadata-export-v1 \
  --execute \
  --prepared-sha256 TRUSTED_OPERATOR_PREPARED_SHA256
```

Execution requires the unchanged prepared source/input/interpreter binding and clean prepared journal. It durably records an execute intent and exclusive marker. It repeats both default validations; both must succeed before the first exporter `--write` child runs. It then writes baseline followed by candidate, retaining each successful stdout as exact original receipt bytes. Exported filenames, five-file populations and actual file hashes are verified before that write intent is confirmed.

Before each external write/copy, a pending action is fsynced. The operator then copies the recursively whitelisted terminal pair journal through the approved receiver and preserves exact comparison bytes. Only known metadata is copied; no plan, environment, key, prompt, raw JSONL, checkpoint or database dump enters the transport set.

A child timeout, nonzero result, invalid/oversized stdout, partial output set, hash mismatch, storage failure or unknown commit/copy outcome stops the operation. The journal retains `manual_recovery_required` and its unresolved pending intent. There is no automatic retry, resume, compensating write, export-directory cleanup or selection of a replacement operation ID. Root must review existing evidence before any separately authorized recovery. Already accepted model work is unaffected.

Stdout/stderr pipes are each bounded to 64KiB. Stderr plaintext is neither printed nor saved. Only successful strict metadata stdout becomes a receipt. Errors expose fixed codes or exception types, never raw server/driver messages or arbitrary stdout.

## Fourteen-file transport allowlist

On success, stdout includes the transport manifest SHA, original terminal pair-journal/comparison SHA and both exact exporter receipt SHAs. The manifest contains exactly:

- Five original baseline JSON exports.
- Five original candidate JSON exports.
- Two successful exporter stdout receipts.
- One exact comparison JSON.
- One recursively whitelisted, exact original terminal pair journal.

The relative layout is the layout expected by the local receiver: `baseline/<job>-<suffix>.json`, `candidate/<job>-<suffix>.json`, two top-level exporter receipts, `comparison.json` and `pair-controller-journal.json`. The manifest records each source path, transport-relative path, exact byte length and SHA256. Original export paths remain compatible with the existing backup/preparation gates.

The root operator performs transport separately, using this allowlist only, and verifies all fourteen hashes/lengths against the retained trusted-channel manifest SHA. Do not transport the operator plan/journal/validation receipts merely because they exist nearby. The transport manifest is metadata; its hashes are integrity anchors, not signatures or independent observations of production.

After verified reception, construct the local receiver manifest with the trusted exporter receipt, comparison and pair-journal SHAs. Follow `qwen235b-v11-receiving-and-plot-audit-2026-10-24.md`. The approved receiver preserves both partial flags, verifies all five artifacts per round and separately audits pair wall/allocation including collection and cleanup. Rendering remains local and requires both real terminal cohorts. This operator does not render, retire history, delete archives or release task capacity.
