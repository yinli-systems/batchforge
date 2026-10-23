# Bounded GPT-OSS engine recovery drill

This is a prepared operator procedure. It has not been executed. Wait for the
coordinator's explicit fault release after both Pods' matrices and quality
experiments finish. Do not run the submission merely because the code is ready.

The selected primary service names are `worker-gptoss120` and
`engine-gptoss120`; their worker ID is `primary-gptoss120-tp4`. The exact fallback
worker ID is `secondpod-gptoss120b-cpu-expert`, serving the same
`openai/gpt-oss-120b` alias with a different runtime/precision layout. Recheck the
final active manifest/configs and both workers' actual readiness before release.

## Prepare without submitting

From `/root/batchforge-runtime/app` on the primary, after checking the frozen
source archive and per-file hashes:

```sh
/root/batchforge-runtime/venv/bin/python ops/ha/prepare_gptoss_outage.py \
  --workload .runtime/token-matrix-gptoss120/workload.jsonl \
  --manifest .runtime/token-matrix-gptoss120/manifest.json \
  --output .runtime/gptoss120-engine-loss-8
```

The new private directory contains eight unique IDs with the same frozen public
128-input-target/512-output-cap body. It checks the complete source workload
digest before copying. It neither submits work nor generates text. Requested
generated tokens are bounded to 4,096 before retries; output lengths remain
natural and can be shorter. The historical rendered input count belongs to the
source vLLM Harmony formatter/date. Do not label llama.cpp's prompt length as
128 without its separate actual usage. This repeated fixture is for recovery,
not throughput or answer quality.

## Coordinate submission and lease capture

Use the existing private tenant-key injection for the benchmark; never place
the key in command arguments. Create a fresh empty input directory for this
single run. Arm the detached operator before submission: it must watch this
directory for exactly one new `bench-<id>-0.jsonl`, validate all eight custom IDs
share that exact prefix and model, and immediately launch the probe with that
exact run ID. Do not infer a run ID from the newest database row, an old report,
or a fixed delay. The benchmark writes its private input file before its first
HTTP submission, so this handoff can precede claims.

After the separate explicit release, the load command is:

```sh
/root/batchforge-runtime/venv/bin/python scripts/benchmark.py \
  --url http://127.0.0.1:8080 --model openai/gpt-oss-120b \
  --tasks 8 --batches 1 --submission-concurrency 1 --max-tokens 512 \
  --input-jsonl .runtime/gptoss120-engine-loss-8/workload.jsonl \
  --workload-dir .runtime/gptoss120-engine-loss-8/submitted \
  --output-dir .runtime/gptoss120-engine-loss-8/outputs \
  --deadline 3600 --request-timeout 60 --poll 0.5 \
  --engine-label real:gptoss120-cross-runtime-engine-loss \
  --report .runtime/gptoss120-engine-loss-8/benchmark.json
```

Keep the operator and load server-side and detached from the SSH session. Do
not auto-retry this submission under another run ID if the launcher disconnects.
The exact run ID comes from that fresh private input, not this command's eventual
terminal report.

For that exact ID, the released fault command is below. Replace both `EXACT`
values with the validated `bench-...` identifier. Without `--execute`, it only
validates configuration and prints a dry-run description.

```sh
/root/batchforge-runtime/venv/bin/python ops/ha/engine_outage_probe.py \
  --manifest /root/batchforge-runtime/app/.runtime/services-primary.json \
  --env-file /root/batchforge-runtime/api-a.env \
  --supervisor-env /root/batchforge-runtime/supervisor.env \
  --supervisor-name primary --run-id EXACT --expected-tasks 8 \
  --model openai/gpt-oss-120b --worker-name gptoss120 --engine-name gptoss120 \
  --secondary-worker-id secondpod-gptoss120b-cpu-expert \
  --duration 120 --recovery-timeout 1800 --wait-for-submission-seconds 300 \
  --minimum-reclaimed 1 \
  --report /root/batchforge-runtime/app/.runtime/engine-loss-EXACT.json \
  --execute
```

The probe polls for all eight durably submitted tasks and an unexpired lease
held by the exact primary worker. It then pauses only the primary watchdog,
stops the selected worker before its engine, and verifies a captured lease is
still valid after the pair is unavailable. If the primary finishes too quickly
to capture a qualifying lease, the drill refuses or restores and reports
failure; do not fabricate expiry evidence.

## Required result and restoration

For at least one captured task, require the original attempt's
`lease_expired` cause, a different accepted token on that same task from the
exact secondary worker, exactly one accepted attempt, and an old-token
heartbeat returning `409 stale_attempt`. Keep the selected primary pair down
for at least 120 seconds and until this proof exists, with a bounded additional
1,800 seconds for the CPU-expert inference. The watchdog is paused during this
window; this is not a demonstration that it remains available throughout a
physical Pod failure.

Every observation must preserve the primary API/gateway PIDs and readiness;
the secondary's services must not be changed by the drill. In `finally`, the
engine restoration uses the same tracked-member cleanup and per-rank memory
gate as the watchdog. Start the worker only after the engine is healthy, then
restore the watchdog. A rejected memory gate never permits a worker to start.
Report failed restoration explicitly; do not call the drill passed merely
because the load eventually completed elsewhere.

Retain the probe report, private submission/output files, benchmark terminal
report, exact before/after process identities, and secondary observer window.
Verify final batch counters, accepted attempts and usage conservation across
all eight tasks. Physical generation can repeat after a lease loss. This tests
application-owned inference-engine loss and cross-runtime durable recovery;
it does not reset hardware, power off a Pod, fail PostgreSQL, or promise
bit-identical output from the two precision formats.
