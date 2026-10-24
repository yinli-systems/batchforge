# Private operator source evidence — not executed

These are review copies of private operator/helper source and tests, preserved byte for byte. [The inventory](../ops/reports/qwen235b-private-operator-source-inventory-2026-10-24.json) records each original path, review-copy path, SHA256, test scope, and the existing metadata-export/receipt-validator source copies. It contains no actual configuration, credentials, task/input/output data, process environment, or database dump.

The 13 copies cover the exact post-pair source rollout, bounded default matched-history retirement driver and allowlist preparer, same-Pod schema11 backup/isolated restore controller, worker-stage histogram analyzer, and bounded worker-stage snapshot collector. They are **NOT_EXECUTED in production** at preparation of this inventory. Passing local tests does not authorize a production operation or show that these private operators passed Linux/PG17 deployment validation.

The final backup source is `ce129946…`, and the final owner driver is `aeff8fd0…`. They replace the earlier `ab628712…` / `226ca81d…` candidates. The backup now allows only two fixed operation names, reads the completed pair journal with an explicit 2MiB limit, and validates the already-completed metadata-export operation through its original two WRITE receipts and exact 14-file transport manifest. Historical metadata validation does not replay the exporter or require historical live PIDs to remain current. Present owned service identities, configuration, source, schema11, and a drained queue remain separate fresh guards.

[The size analysis](../ops/reports/qwen235b-pair-journal-size-bound-analysis-2026-10-24.json) uses only synthetic valid metadata. Its 112-observation journal is 66,294 bytes: the old 64KiB reader refuses it, while the explicit 2MiB reader accepts it. This demonstrates a bounded compatibility failure; it does not claim an observed production journal size or data loss.

Validation evidence is scoped separately:

- Final owner-driver tests plus backup-proof tests: **56 passed, zero skipped** in a fresh local Unix-only PostgreSQL16.2 cluster; cluster stopped and removed. The 19 backup tests were also independently rerun and approved.
- Post-pair rollout: 16 local pure tests and two real local PostgreSQL16.2 tests; independent combined rerun passed.
- Repaired allowlist preparer: 15 local synthetic-metadata tests; stage analyzer: 22 local tests; snapshot collector: 15 local mocked tests, independently reviewed.
- All copied Python source/tests compile, and their review copies match the approved original byte hashes.

The separate source69 release evidence covers ten Python bundle files, with independent Node console-action tests for the frontend. It does not qualify these private operators on Linux/PG17. Actual prepare/apply, current service preflight, fresh same-Pod backup/restore, exact external evidence pins, and root review remain required. No commit, push, deployment, backup, retirement, inference, or production metrics collection was performed by the packaging agent.
