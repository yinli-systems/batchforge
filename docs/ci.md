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

## Actual existing-Pod check

[Run40215776424, attempt2](https://github.com/yinli-systems/batchforge/actions/runs/40215776424/attempts/2)
for exact source `5791efe` completed successfully: **1,387 Python tests passed,
zero skipped**, PostgreSQL **17.11**, Node24.22.1, **22 console tests passed** and
compileall passed. The runner OS UID was65534, with production credentials
absent. It ran no GPU inference. The one-job runner exited and GitHub listed
zero registered runners afterwards. [Authenticated result / log hash](../ops/reports/github-native-ci-5791efe-2026-10-25.json)
is retained. Initial bootstrap permission errors and attempt1's incorrect
rootfs/toolchain layout remain failed evidence; they are not relabeled passes.
A clean runner-process exit is distinct from a successful workflow job.

The trusted development PR relies on its head-push check; the hosted integration
job is skipped for that same private branch to avoid the known billing block.
This is not a main-branch merge test. Other branches still use the hosted job.
Every source push requires another short-lived supervised runner; publishing
Markdown/benchmark evidence alone does not queue another job.

The subsequent exact head **`748dc10`** also passed [run40216928687](https://github.com/yinli-systems/batchforge/actions/runs/40216928687):
**1,415 Python tests, zero skips, 23 console tests**, PostgreSQL17.11/Node24.22.1.
A root-owned pre-job hook checked the exact repository, private branch, push
event, full commit SHA and non-root UID before checkout. No runner remained
registered after its one job. [Authenticated result](../ops/reports/github-native-ci-748dc10-2026-10-25.json).

Recovery-command source `b7a1a50` passed [native run40219250865](https://github.com/yinli-systems/batchforge/actions/runs/40219250865):
**1,456 Python tests, zero skipped;23 console tests**, PostgreSQL17.11.
Its command is not deployed; a subsequent endpoint/effective-config safety
follow-up requires its own native check and real fault qualification.
