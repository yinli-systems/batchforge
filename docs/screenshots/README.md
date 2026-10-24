# BatchForge platform screenshots

These unedited screenshots were captured from the running private deployment
on October 24, 2026, around 04:35 Asia/Dubai. They show existing persisted
workload results; taking the screenshots did not submit any new inference.

## Workspace overview

25,183 total tasks, 25,004 completed, 29.3M processed tokens, and 17 connected
workers. Totals include earlier models, experiments and 179 historical failed
tasks. The throughput chart shows completions by reporting interval, not RPS.
Registered workers and slots include historical registrations; they are not
the same as currently available inference capacity.

![Workspace overview](overview-2026-10-24.jpg)

## Worker cluster

The visible second-Pod GPT-OSS-120B worker is ready with one configured slot.
Other visible cards serve Qwen2.5-7B. The sidebar reports 17 ready workers
across the deployment. Worker readiness does not measure GPU utilization.

![Worker cluster](workers-2026-10-24.jpg)

## Batch history

The recent 20-request comparisons, one-request smoke test and eight-request
recovery exercise are visible. Older batches may have terminal failures;
the completed/failed column remains visible rather than hiding those runs.

![Batch history](batches-2026-10-24.jpg)

## Completed 120B batch

Batch `batch_a1678e67c4ecc8ddbc4d5eb548a4044d` completed 20/20 tasks, with
zero failed, running, pending or cancelled tasks. Its model is
`openai/gpt-oss-120b`. Durable completion does not establish answer correctness.

![Completed 120B batch](batch-120b-completed-2026-10-24.jpg)

## Actual token usage

The same batch records 109,054 input and 8,742 generated tokens. It explicitly
requested low reasoning effort; generated usage can include reasoning tokens.
See the [validation report](../100b-validation.md) for final-text presence,
length caps, quality limitations and worker attribution.

![120B token usage](batch-120b-usage-2026-10-24.jpg)
