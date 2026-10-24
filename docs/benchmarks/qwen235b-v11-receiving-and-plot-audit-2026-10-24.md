# Exact-byte receiving and plot audit for the schema11 pair

This supplements the original matched operator notes. The serving controller, exporter, comparer and plotting source remain frozen. This procedure has only been tested locally with explicitly synthetic, private fixtures. It does not contain a real candidate result or start a workload.

## Gaps and their closure

The frozen plot helper verifies two reports, two runtime proofs and the comparison JSON. It preserves both partial-plan `passed=false` flags and distinguishes scheduled-arrival latency from DB-created cohort latency. It does not, by itself, verify the other three metadata artifacts, successful exporter receipts, response-policy commit receipts or whole-pair cost.

The separate receiver `.runtime/verify_matched_v11_receipt.py` checks all five original artifacts against each successful `--write` exporter receipt and an operator-supplied receipt SHA256. It verifies policy, collection, audit, accepted usage and original-byte cross-links; each received round directory must contain exactly its five known files. Its terminal pair-journal whitelist is recursive and rejects unknown fields before an exact-byte export. Pair wall and allocation are checked separately from runner wall and allocation, including an additive overhead identity.

The frozen matplotlib renderer can emit trailing whitespace in SVG paths. The new wrapper leaves its source untouched, normalizes SVG lines before final artifact hashing, and records the original renderer SVG SHA, normalized SVG SHA and wrapper SHA. PNG is not postprocessed. This is explicitly modified renderer output, not a claim that the frozen helper emitted the published SVG unchanged.

| Candidate dependency | SHA256 |
| --- | --- |
| Receiver source | `5534f19cb83c5408af41cd6497d3c6307039ca6748cb93f8c6c3512d6da592d6` |
| Synthetic fixture tests | `32c770723bc43a71f99424f586bdcfbc9020554094406f47f555981cc3784afa` |
| Frozen exporter | `a73b14599f3a0d5656fcaf2aede81e372a4540b5a3e177011701b539549f3f03` |
| Frozen controller | `6c5e5895d3cc8ef1826ae75752143ced24f127422a0e612c19b6ad6bb2dd457b` |
| Frozen comparer | `90df1ebbb26a40d7288fb230ec9ddc484e3ece2414c691b6d993936bd17862c3` |
| Frozen renderer | `3a71d665e9d9d3c3569911530da748e7be141c0b6fdbc03378a9bb91cdbad1ff` |

The source and test copies alongside this note are review artifacts. They are not production service modules. Place the reviewed receiver at the named `.runtime` path to run it. No database, HTTP, SSH, GPU or model calls exist in this tool.

## Collect the original evidence

After both rounds, collect the five-file exports using the original exporter and retain the exact single-JSON stdout from each successful `--write` invocation as `baseline-export-receipt.json` and `candidate-export-receipt.json`. Preview-only receipts have an empty written-file map and are rejected by the receiver. Preserve bytes and SHA256 values from the trusted operator channel; hashing a received bundle alone does not prove that it came from the serving host.

The operation must be `qwen235b-v11-matched-v1`, plan SHA256 `8d1b3745c9dfb9a12f11394fcc3d57af7150d33c3a46414646f13c2b02f7a45e`, with exact jobs `qwen235b-soak30m-v11-response1m-v1` and `qwen235b-soak30m-v11-response64k-v1`. No old schema10 baseline, interrupted pilot, QA cohort or fabricated replacement is accepted.

The five exports omit the operation-level journal, where whole-pair cost is recorded. The receiver provides a separate read-only export mode for that journal. Only the operator may run it on the serving host, after the full operation is terminal. Use the parent runtime interpreter `/root/batchforge-runtime/venv/bin/python`, not an `app/.venv` path. The journal SHA below must come from the operator's exact terminal source file, not a rewritten projection.

```sh
BF_APP=/root/batchforge-runtime/app
BF_PY=/root/batchforge-runtime/venv/bin/python
BF_PAIR_JOURNAL="$BF_APP/.runtime/sustained-matched/qwen235b-v11-matched-v1/controller-journal.json"

"$BF_PY" "$BF_APP/.runtime/verify_matched_v11_receipt.py" \
  --export-pair-journal "$BF_PAIR_JOURNAL" \
  --pair-journal-sha256 TRUSTED_TERMINAL_JOURNAL_SHA256
```

That command validates only. Add `--write --pair-output PRIVATE_EXISTING_DIRECTORY/pair-controller-journal.json` to copy exact original bytes to a new private file. The parent directory must already be an owned, mode0700 directory below the app's `.runtime`; the output file is exclusive mode0600. A nonterminal phase, pending/unknown field, wrong operation/plan, changed legacy charge, invalid initial/final cleanup or inconsistent cost refuses export. This export mode pins and imports only the frozen metadata-schema exporter definitions; it does not require the Mac-only plotting file on the Pod. Missing or changed exporter source refuses. Normal local receiving/rendering still pins all four local dependencies. The historical runtime proof requires the actually deployed controller/exporter/comparer and controller pins, and never fabricates or requires a historical plotting entry. Do not alter the timed serving source tree to satisfy checks during the freeze.

Transfer only the ten validated round JSONs, the two successful exporter stdout receipts, the comparison JSON and the separately whitelisted terminal pair journal. Preserve the original checksums in the trusted operator record. Do not transfer raw JSONL outputs, environment files, credentials, checkpoints or database dumps. Keep all local receive directories mode0700 and their files mode0600 under the repository's `.runtime`.

## Bind the local manifest

Create a private manifest only when both real cohorts and all required evidence exist. The example contains deliberately invalid placeholders; it is not a result and cannot pass validation. All paths must be absolute, resolve without symlinks and remain within the local repository's `.runtime`.

```json
{
  "version": 1,
  "operation": "qwen235b-v11-matched-v1",
  "prepared_plan_sha256": "8d1b3745c9dfb9a12f11394fcc3d57af7150d33c3a46414646f13c2b02f7a45e",
  "baseline": {
    "directory": "/ABSOLUTE_LOCAL_REPO/.runtime/sustained-v11-export/baseline",
    "export_receipt": "/ABSOLUTE_LOCAL_REPO/.runtime/sustained-v11-export/baseline-export-receipt.json",
    "export_receipt_sha256": "TRUSTED_BASELINE_WRITE_RECEIPT_SHA256"
  },
  "candidate": {
    "directory": "/ABSOLUTE_LOCAL_REPO/.runtime/sustained-v11-export/candidate",
    "export_receipt": "/ABSOLUTE_LOCAL_REPO/.runtime/sustained-v11-export/candidate-export-receipt.json",
    "export_receipt_sha256": "TRUSTED_CANDIDATE_WRITE_RECEIPT_SHA256"
  },
  "comparison": {
    "path": "/ABSOLUTE_LOCAL_REPO/.runtime/sustained-v11-export/comparison.json",
    "sha256": "TRUSTED_COMPARISON_SHA256"
  },
  "pair_journal": {
    "path": "/ABSOLUTE_LOCAL_REPO/.runtime/sustained-v11-export/pair-controller-journal.json",
    "sha256": "TRUSTED_TERMINAL_JOURNAL_SHA256"
  }
}
```

The receipt SHA inputs and manifest SHA are operator trust anchors, not digital signatures or independent observations of production. The tool requires exact original bytes: even a whitespace-only report edit invalidates its transport receipt. It recomputes the frozen comparison after validating all five artifacts in both rounds. Raw outputs are not read locally; observed response-size/token facts remain attributed to the exact source-bound collection and audit.

## Validate first; render only real terminal data

From the local repository root, run:

```sh
.runtime/plot-venv312/bin/python .runtime/verify_matched_v11_receipt.py \
  --manifest .runtime/sustained-v11-export/receive-manifest.json \
  --manifest-sha256 TRUSTED_LOCAL_MANIFEST_SHA256
```

On success, repeat with `--write --receipt-output` pointing to a new file in an existing private runtime directory. To additionally render the verified real comparison, add:

```text
--render
--output-prefix docs/benchmarks/qwen235b-v11-response-policy-two-30m-2026-10-24
```

One complete invocation is:

```sh
.runtime/plot-venv312/bin/python .runtime/verify_matched_v11_receipt.py \
  --manifest .runtime/sustained-v11-export/receive-manifest.json \
  --manifest-sha256 TRUSTED_LOCAL_MANIFEST_SHA256 \
  --write \
  --receipt-output .runtime/sustained-v11-export/local-verification-receipt.json \
  --render \
  --output-prefix docs/benchmarks/qwen235b-v11-response-policy-two-30m-2026-10-24
```

Existing receipts or chart files are never overwritten. Rendering creates PNG, normalized SVG, a chart-data audit and a separate allocation-context Markdown file. The chart audit binds final artifact bytes, source identities, both exporter receipt SHAs and original pair-journal SHA; its SVG postprocessing record identifies both renderer-original and normalized SVG bytes.

The cost panel remains the frozen renderer's **per-runner** cost view. The companion allocation-context table separately displays the sum of runner windows, additional pair wall/allocation and whole-pair wall/allocation. Additional pair wall includes controller/process overhead, collection/audit, inter-round/final cleanup and comparison. No individual overhead stage is inferred. Preparation, later metadata export/transport, local rendering and later idle time are excluded from the pair timestamp.

Inspect the PNG/SVG at readable size, including every population, full-plan flag, unit and footnote. Verify all chart artifact SHA/size entries and run `git diff --check` as a separate successful gate after staging the selected files; do not allow a shell command after a failing gate to conceal its exit status. Do not publish fixture renderings or replace data to repair a layout issue.

Both full-plan flags can remain false while every accepted task completed exactly once. Offered 720 is the virtual arrival schedule, not accepted capacity. Scheduled-arrival tails and cohort tails describe different latency scopes; different accepted populations are not matched tasks. NVML samples remain device-wide, unweighted observations with explicit gaps. Neither per-runner nor whole-pair allocation is an invoice, measured marginal cost, daily/fleet forecast, GPU kernel speedup or high-availability proof.
