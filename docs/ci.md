# Correctness checks and CI availability

Hosted run [40213979692](https://github.com/yinli-systems/batchforge/actions/runs/40213979692)
failed before any test step for commit `2872d43`. The authenticated check
annotation reports an account payment/spending-limit block. This is not a test
failure or a green check. [Captured evidence](../ops/reports/github-ci-account-block-2872d43-2026-10-25.json)
records the empty steps and independent local validation: 1,363 passed, five
skipped, PostgreSQL **16.2**, not 17. No billing settings were changed.

The development branch can instead target an explicitly provisioned ephemeral
CPU runner on the already-running Pod. GitHub documents self-hosted Actions
execution as free; no GitHub-hosted spending limit is raised. The workflow only
routes trusted private-branch pushes there, not fork pull requests. The runner
must run as an unprivileged user, use a fresh private directory, receive only a
short-lived repository registration token, deregister after one job and have a
bounded lifetime. Do not copy Azure/S3/SSH credentials or the application venv.
It must wait for the fixed GPU campaign to finish before CPU correctness work.

`devtools/run_native_ci.py` requires non-root Linux, a public operator-staged
PostgreSQL **17** toolchain and Node >=22. It starts a separate Unix-only,
mode-700-socket database with host authentication rejected, runs all correctness
and console tests, compiles source, and stops only its own cluster. It supplies
an explicit test environment with no production credentials and CUDA hidden.
The ephemeral runner uses hash-locked CPU dependencies. A queued self-hosted
job still requires a runner; changing YAML alone is not successful CI.

Documentation/evidence-only pushes are excluded to avoid consuming a second
one-job runner merely to publish its completed evidence. Source pushes require
a newly supervised runner. Other branch/PR hosted jobs remain subject to the
account block. If that block also prevents self-hosted execution, preserve its
actual result and independent PostgreSQL17 proof; do not mark CI passed.

References: [GitHub Actions billing](https://docs.github.com/en/billing/concepts/product-billing/github-actions),
[runner releases](https://github.com/actions/runner/releases).
