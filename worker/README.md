# Worker control-plane availability

`CONTROL_PLANE_URLS` optionally supplies comma-separated, operator-configured API
origins in priority order. It takes precedence over the backwards-compatible
single `CONTROL_PLANE_URL`. Every origin must share the same durable scheduler
database, worker authentication secret, and model/lease policy. Do not point it
at independent queues and expect fencing or capacity accounting to transfer.

The worker sends each HTTP operation exactly once. A transport error, server
error, or malformed JSON response changes the origin for a subsequent operation;
it does not automatically replay that POST. A claim can have committed before
its acknowledgement was lost. The shared database keeps that claim counted
against worker capacity until lease recovery. A completion report's existing
explicit retry loop retains its identical attempt token and response body when
the next origin is selected, without invoking the model again.

Authentication, validation, admission, and stale-attempt responses do not rotate
origins. Concurrent failures from an older selection cannot override a newer
selection, including after the origin list cycles. Successful operation stays on
the selected origin until it fails; there is no automatic primary failback.
`batchforge_worker_control_origin_index` and
`batchforge_worker_control_failovers_total` expose the observed selection.

These origins are control-plane addresses only. Inference destinations remain
separately configured by `INFERENCE_BASE_URLS`. Worker environment files need no
database credentials. API topology changes should be exercised with real
failure/load tests before treating them as an availability guarantee.

## JSON-object output acceptance

When a task requests exactly `response_format: {"type":"json_object"}`, the
worker validates the unmodified first choice's message content before reporting
completion. It must parse as an object, with no Markdown wrapper, raw control
characters, non-finite constants, or duplicate object keys. This checks JSON
syntax and the top-level object type; it does not require task-specific fields
or establish answer or translation accuracy. Other output formats keep their
existing behavior.

Invalid output is reported through the existing fenced `fail` operation with
code `invalid_json_output` and `retryable: true`. The durable scheduler owns the
retry delay and maximum attempt count. The worker does not repair the content or
invoke inference again within that attempt. No rejected response or token usage
is sent to the completion endpoint.

`batchforge_worker_invalid_outputs_total`,
`batchforge_worker_rejected_prompt_tokens_total`, and
`batchforge_worker_rejected_generated_tokens_total` separately count observed
rejections and the engine-reported tokens used by those rejected responses.
These metrics have no dynamic labels and do not contain prompts, content, task
IDs, or tenant IDs. Accepted prompt/completion metrics increment only after a
completion acknowledgement. Rejection counters are process-lifetime observations,
reset on restart, and cannot account for GPU work whose usage response was lost;
they are not a durable billing ledger.
