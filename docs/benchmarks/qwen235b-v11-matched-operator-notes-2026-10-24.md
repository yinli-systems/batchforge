# Final schema11 sustained-result export and plotting

These are operator commands for the existing private serving Pod, followed by local rendering. They have not been executed by the author of these notes. The matched pair was started at approximately 08:28 UTC on 2026-10-24 with parent PID `220658`; this document does not start or restart it.

| Binding | Exact value |
| --- | --- |
| Operation | `qwen235b-v11-matched-v1` |
| Prepared plan SHA256 | `8d1b3745c9dfb9a12f11394fcc3d57af7150d33c3a46414646f13c2b02f7a45e` |
| First round | `qwen235b-soak30m-v11-response1m-v1` |
| Second round | `qwen235b-soak30m-v11-response64k-v1` |
| Controller SHA256 | `6c5e5895d3cc8ef1826ae75752143ced24f127422a0e612c19b6ad6bb2dd457b` |
| Exporter SHA256 | `a73b14599f3a0d5656fcaf2aede81e372a4540b5a3e177011701b539549f3f03` |
| Runner SHA256 | `2285f71fb69bde10325c7f393e225b80fe51d70708cf221fac9c1aa9d5192289` |
| Comparator SHA256 | `90df1ebbb26a40d7288fb230ec9ddc484e3ece2414c691b6d993936bd17862c3` |
| Sustained plot helper SHA256 | `3a71d665e9d9d3c3569911530da748e7be141c0b6fdbc03378a9bb91cdbad1ff` |

Both rounds use schema11, the same fixed 20-cell fixture/seed, 720 offered tasks, 0.4 tasks/second, 1,800-second admission duration, 900-second drain allowance, 20 tasks/batch, 80 maximum in flight and two submission workers. Only the future response allowance changes: 1,048,576 to 65,536 bytes. The task cap remains 50,000, total retained-payload quota 335,544,320 bytes, error allowance 32,768 bytes and maximum attempts three.

The plan records 64 prior experimental batches: 42 controlled batches at or after the existing publisher cutoff and 22 older legacy batches. The publisher cutoff remains `2026-10-24T03:59:21.961089Z`. Legacy charge is preserved; no historical purge, retirement, quota widening or service restart is part of this pair. Retained task rows and auxiliary objects accumulate between rounds and remain an explicit serial-comparison limitation.

## After both rounds terminate

Keep the exact source, service PIDs, environment and configuration frozen through metadata export. Confirm the operation journal is terminal and has no `pending_action`. The successful pair journal has `phase=complete` and hashes the generated `comparison.json`. Each round must have its own terminal report, controller journal, runtime proof, complete output collection and response audit. A partial 720-task plan can still be a valid accepted-cohort observation; its original `passed=false` must remain unchanged.

If the controller instead reports `manual_recovery_required`, preserve the journals and all accepted work. Do not rerun `--execute`, invent a replacement job ID or treat a missing report as a completed run. The controller has no automatic replay. A complete single round can be retained as descriptive evidence, but it does not manufacture the missing matched comparison.

Use the same serving interpreter as the controller/API environment. The observed deployment uses `/root/batchforge-runtime/venv/bin/python`; verify that path against the existing controller launch before use. The following commands validate metadata without writing files, changing policy or issuing inference requests. The exporter performs read-only runtime/ledger/profile checks.

```sh
BF_APP=/root/batchforge-runtime/app
BF_PY=/root/batchforge-runtime/venv/bin/python
BF_PLAN_SHA=8d1b3745c9dfb9a12f11394fcc3d57af7150d33c3a46414646f13c2b02f7a45e

"$BF_PY" "$BF_APP/.runtime/export_sustained_v11_metadata.py" \
  --operation qwen235b-v11-matched-v1 --round baseline \
  --prepared-sha256 "$BF_PLAN_SHA"

"$BF_PY" "$BF_APP/.runtime/export_sustained_v11_metadata.py" \
  --operation qwen235b-v11-matched-v1 --round candidate \
  --prepared-sha256 "$BF_PLAN_SHA"
```

After both validations pass, repeat each command with `--write`. This exclusively creates private metadata files under:

```text
/root/batchforge-runtime/app/.runtime/sustained-matched/qwen235b-v11-matched-v1/exports/baseline/
/root/batchforge-runtime/app/.runtime/sustained-matched/qwen235b-v11-matched-v1/exports/candidate/
```

Each directory contains exactly five intended export artifacts, prefixed by its job ID: `report.json`, `controller.json`, `runtime-proof.json`, `output-collection.json` and `response-audit.json`. The exporter checks recursive field whitelists and rejects unknown fields. It copies exact original bytes; its printed SHA256 values retain the original report/proof/collection/audit bindings. Existing output files are never overwritten. An interrupted partial export requires review rather than silently replacing files.

Transfer only these validated JSON files and the operation's `comparison.json` / `comparison.md` using the existing operator metadata path. Raw JSONL outputs, environment files, credentials, checkpoints and database dumps are outside this export. Record transport hashes and preserve every original byte. Keep the two complete export directories separate locally, for example as `.runtime/sustained-v11-export/baseline` and `candidate`, with the comparison in `.runtime/sustained-v11-export/comparison.json`.

## Validate and render the actual two-window comparison locally

The optional plotting environment already exists at `.runtime/plot-venv312`; it is separate from the serving environment. Do not generate a candidate point before its real terminal metadata exists. From the local repository root, validate the actual comparison with:

```sh
BF_META=.runtime/sustained-v11-export
BF_BEFORE=qwen235b-soak30m-v11-response1m-v1
BF_AFTER=qwen235b-soak30m-v11-response64k-v1

.runtime/plot-venv312/bin/python scripts/plot_sustained_comparison.py \
  --comparison "$BF_META/comparison.json" \
  --baseline-report "$BF_META/baseline/$BF_BEFORE-report.json" \
  --candidate-report "$BF_META/candidate/$BF_AFTER-report.json" \
  --baseline-runtime-proof "$BF_META/baseline/$BF_BEFORE-runtime-proof.json" \
  --candidate-runtime-proof "$BF_META/candidate/$BF_AFTER-runtime-proof.json" \
  --output-prefix docs/benchmarks/qwen235b-v11-response-policy-two-30m-2026-10-24
```

The default is validation only. Repeat that exact command with `--write` after validation passes. It creates exclusive `.png`, `.svg` and `.chart-data.audit.json` files. The helper recomputes the full comparison from both reports and runtime proofs, so edited rates, tails, cost, populations or source hashes fail validation. It does not read the database, call HTTP or use GPUs.

Inspect the PNG at its actual size, then inspect the SVG in a browser/viewer. Check clipping, legend/value overlaps, panel units, report SHA prefixes and the preserved full-plan flags. Review the chart-data audit alongside the two original reports and both per-round response audits. Do not fix a chart by editing measured numbers or original reports; correct presentation in a separate reviewed plotting revision after the production source freeze ends.

The final presentation must retain:

- Offered, accepted and completed populations; HTTP429, proven pre-forward rejections and uncertain create responses as separate counters.
- Scheduled-arrival-to-finish/admission latency separately from DB-created-to-finish cohort latency, including P50/P95/P99 and the observation populations.
- Whole measured runner-wall allocation cost at the assumed $16.10/hour, with separate pair-wall cost including collection/cleanup where available.
- All four GPU sample means, sample coverage and missing-period bounds; no zero-filling.
- Serial N=1 per policy, no confidence intervals, different accepted populations when applicable and original `passed=false` flags.

This is an end-to-end admission/response-reservation comparison. It does not establish GPU kernel speedup, matched-task tail improvement, daily capacity, fleet scaling, high availability, billing accuracy or a bound for arbitrary future model responses. The old schema10 baseline and interrupted 64KiB pilot are historical context and are not inputs to this pair.

## Separate workload-regime figure

`qwen235b-workload-regimes-2026-10-24.png` / `.svg` uses the existing real schema10 prefill100 and decode100 reports. It compares different workload mixes, not response policies or software revisions. Its exact numeric dataset and source/report hashes are stored in the adjacent `.chart-data.audit.json`; the plotting source is preserved in `.plot-source.py.txt`. These descriptive 100-task cohorts must remain separate from the two new 30-minute results.
