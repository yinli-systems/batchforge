# Qwen235B projected-profile prediction error — 2026-10-24

This is a read-only audit of 300 recorded single-attempt completions in three different serial cohorts. The production profile remains projected from static16 and is unchanged. These data do not identify isolated GPU service rates or calibrate heterogeneous hardware.

The same profile appears in every decision: prefill **5,573.560942** tokens/s, decode **11.775314** tokens/s and **$1.00625/slot-hour = $16.10/16**. Adaptive64 is a claim limit; it does not create 64 priced execution slots.

| Cohort | Batches / tasks | Input estimate / actual P50 | Observed attempt / predicted service P50 / P95 / P99 | Service absolute error P50 | Predicted cost proxy sum | Whole-Pod allocation scenario |
| --- | --- | --- | --- | --- | --- | --- |
| mixed_final_adaptive100 | 5 / 100 | 1.170x | 1.594x / 6.199x / 7.642x | 37.3% | $1.6462 | $1.1187 |
| prefill100 | 1 / 100 | 1.174x | 3.816x / 10.496x / 10.714x | 73.8% | $0.2624 | $0.3866 |
| decode100 | 1 / 100 | 1.155x | 0.979x / 1.002x / 1.004x | 2.1% | $3.0402 | $1.1419 |

The UTF8/4 heuristic overestimates prompt tokens on these fixtures. It adds positive prefill prediction time; it cannot explain the much longer prefill-heavy observed attempt intervals. Replacing its input count with the actual count would shorten the arithmetic prediction further. The JSON includes that sensitivity calculation with unchanged coefficients; it is not a fitted or production calibration.

The decode-heavy median happens to lie near 1x. That is one correlated, fixed-output cohort, not a holdout result. Inspect its per-cell tails and queue context before drawing conclusions. The mixed cohort has only five repeated rows in each of 20 cells; its P95/P99 cell estimates are descriptive interpolations over five rows.

## What the time intervals measure

`observed_attempt_seconds` is the database attempt finish minus start. Start defaults to transaction-start `now()`, before worker execution; the interval can include claim/dispatch, engine availability, `/tokenize`, shared-KV reservation waiting, engine queue/execution, response validation and completion reporting through the accepted SQL update before COMMIT/ACK. It is not engine GPU time. The recorded metadata cannot separate those stages.

Pre-attempt database queue and client upload/admission are excluded from that attempt interval. Task completion time includes pre-attempt database queue. The accepted DB finish timestamp excludes later COMMIT/HTTP acknowledgement and post-acceptance duplicate report retry delays. Separate task/attempt clock calls produce small negative timing-join residuals; they are retained without clamping.

## Class and cell evidence

Each row below shows distributions within one cohort; they are not independent run replications. Ratios above 1 mean the observed attempt took longer than the recorded service prediction.

| Cohort / class | n | Input ratio P50 | Attempt / prediction P50 | P95 | P99 |
| --- | --- | --- | --- | --- | --- |
| mixed_final_adaptive100 / balanced | 5 | 1.174x | 1.160x | 1.215x | 1.216x |
| mixed_final_adaptive100 / decode_heavy | 50 | 1.174x | 1.345x | 4.836x | 7.026x |
| mixed_final_adaptive100 / prefill_heavy | 45 | 1.165x | 2.178x | 6.502x | 7.345x |
| prefill100 / prefill_heavy | 100 | 1.174x | 3.816x | 10.496x | 10.714x |
| decode100 / decode_heavy | 100 | 1.155x | 0.979x | 1.002x | 1.004x |

| Cohort / cell (input → output tokens) | n | Input ratio P50 | Attempt / prediction P50 | P95 | P99 |
| --- | --- | --- | --- | --- | --- |
| mixed_final_adaptive100 / 128 → 128 | 5 | 1.148x | 2.441x | 2.570x | 2.574x |
| mixed_final_adaptive100 / 128 → 2048 | 5 | 1.297x | 1.148x | 1.173x | 1.175x |
| mixed_final_adaptive100 / 128 → 32 | 5 | 0.969x | 4.956x | 7.531x | 7.809x |
| mixed_final_adaptive100 / 128 → 512 | 5 | 1.086x | 1.473x | 1.511x | 1.512x |
| mixed_final_adaptive100 / 16384 → 128 | 5 | 1.163x | 1.776x | 2.116x | 2.165x |
| mixed_final_adaptive100 / 16384 → 2048 | 5 | 1.174x | 1.160x | 1.215x | 1.216x |
| mixed_final_adaptive100 / 16384 → 32 | 5 | 1.184x | 2.804x | 3.432x | 3.489x |
| mixed_final_adaptive100 / 16384 → 512 | 5 | 1.165x | 1.384x | 1.470x | 1.479x |
| mixed_final_adaptive100 / 2048 → 128 | 5 | 1.128x | 2.173x | 2.655x | 2.659x |
| mixed_final_adaptive100 / 2048 → 2048 | 5 | 1.188x | 1.146x | 1.172x | 1.173x |
| mixed_final_adaptive100 / 2048 → 32 | 5 | 1.256x | 6.016x | 6.838x | 6.944x |
| mixed_final_adaptive100 / 2048 → 512 | 5 | 1.186x | 1.448x | 1.583x | 1.591x |
| mixed_final_adaptive100 / 512 → 128 | 5 | 1.166x | 2.430x | 2.702x | 2.710x |
| mixed_final_adaptive100 / 512 → 2048 | 5 | 1.182x | 1.153x | 1.173x | 1.174x |
| mixed_final_adaptive100 / 512 → 32 | 5 | 1.174x | 6.194x | 7.422x | 7.596x |
| mixed_final_adaptive100 / 512 → 512 | 5 | 1.129x | 1.470x | 1.589x | 1.594x |
| mixed_final_adaptive100 / 8192 → 128 | 5 | 1.203x | 1.888x | 2.377x | 2.386x |
| mixed_final_adaptive100 / 8192 → 2048 | 5 | 1.197x | 1.156x | 1.205x | 1.213x |
| mixed_final_adaptive100 / 8192 → 32 | 5 | 1.163x | 4.266x | 5.193x | 5.282x |
| mixed_final_adaptive100 / 8192 → 512 | 5 | 1.148x | 1.443x | 1.508x | 1.513x |
| prefill100 / 16384 → 128 | 25 | 1.163x | 3.667x | 5.173x | 5.194x |
| prefill100 / 16384 → 32 | 25 | 1.184x | 4.160x | 7.797x | 7.804x |
| prefill100 / 8192 → 128 | 25 | 1.203x | 3.716x | 4.622x | 4.642x |
| prefill100 / 8192 → 32 | 25 | 1.163x | 5.353x | 10.679x | 11.951x |
| decode100 / 128 → 2048 | 25 | 1.297x | 0.978x | 0.979x | 0.979x |
| decode100 / 128 → 512 | 25 | 1.086x | 0.999x | 1.002x | 1.004x |
| decode100 / 512 → 2048 | 25 | 1.182x | 0.977x | 0.979x | 0.979x |
| decode100 / 512 → 512 | 25 | 1.129x | 0.998x | 1.002x | 1.005x |

The JSON preserves P50/P95/P99/min/max/mean for input bias, absolute token error, absolute percentage error, predicted prefill/decode/service components, observed attempt, signed and absolute service error (including MAE via mean absolute-error seconds), first queue, DB completion, timing residual and actual-input sensitivity in every cohort/class/cell.

## Cost scope

The two cost columns are different arithmetic scopes. The prediction sum multiplies per-task service estimates by the projected static16 slot price. Whole-Pod allocation multiplies one cohort wall interval by $16.10/hour, so overlapping tasks are not charged repeatedly. The JSON reports their ratios as scope diagnostics, not billing error or measured cost savings.

Likewise, summing observed attempt durations counts overlapping claims and their reservation/queue/report intervals. Dividing that sum by cohort wall is not measured executing-stream occupancy, and multiplying it by the slot price is not observed GPU cost. These reports exclude validation and inter-cohort time from the stated allocation; they contain no invoice or rejected/retried work population.

## Next calibration contract

**Binding**

- model repository+revision+quantization, tokenizer/chat-template hashes and max context.
- GPU model/count/memory, TP/PP topology, CPU/driver/CUDA/vLLM versions and collective configuration.
- engine flags including eager/graph/capture sizes, prefix cache/chunked prefill, max seqs/batched tokens and physical KV capacity.
- worker shared KV budget/block size/claim cap, profile source and raw stage-instrumentation version.
- explicit price scope whole-Pod allocation versus marginal provider price; no implicit division by claim capacity.

**Telemetry**

- durable custom-id/attempt-number linkage with safe metadata only; no prompt/response bodies or credentials.
- separate pre-claim DB queue, claim transaction+dispatch, worker receipt/tokenize, KV reservation wait, engine request/response, report-send/durable-accept/ack timers.
- worker monotonic durations and bounded trace IDs; preserve DB timestamps separately instead of treating distributed clocks as identical.
- engine queue/prefill/TTFT/decode stage evidence if instrumented; full-response wall alone must not be labeled TTFT or kernel time.
- concurrent active streams/engine queue/KV used+reserved and workload mix over each interval; explicit sampling coverage/gaps.
- actual tokenizer prompt count, requested output cap, actual generated count, status/retries/rejected work and accepted usage.

**Experimental matrix**

- separate workload cells across prompt 128/512/2048/8192/16384 and output 32/128/512/2048, plus natural-EOS multilingual/structured/long-tail holdouts.
- stratify measured admitted execution by real active-stream bins 1/2/4/8/16/32/64 when feasible, offered load and KV pressure; distinguish 64 claims from 64 executing streams.
- cold/warm prefix-cache and eager/graph settings as separate runtime bindings; never mix into one profile.
- repeat independent serial cohorts, randomize workload/config order and block by time; keep task/profile/runtime hashes fixed.
- repeat the same contract on each actual hardware/topology class before enabling heterogeneous cost routing.

**Fit and validation**

- use stage-specific service estimates conditional on actual tokens, output behavior, concurrency and KV pressure; model reservation/queue separately.
- split training/validation/holdout by run/time and prompt-template family so repeated fixture rows cannot leak across splits.
- report signed bias, MAE, percentage error, observed/predicted factors and P50/P95/P99 residuals per cell/class/concurrency, with independent-run uncertainty.
- choose numeric error/coverage and routing-regret acceptance thresholds before collecting holdout; publish full failed/retried cost scope.
- validate ranking and deadline behavior in a shadow scheduler against the unchanged projected baseline before any production promotion.
- keep compatibility/runtime fingerprint separate from hardware calibration identity; require expiry/drift monitoring, explicit promotion and exact rollback.

**Proposed release gates**

- Planning targets, not achieved results: per hardware/runtime stratum start with 3 independent training runs, 1 validation run and 3 untouched holdout runs; preserve time/prompt-family splits.
- Cover at least 30 heldout tasks per populated workload-cell/concurrency bin spread across 3 holdout runs; leave uncovered bins projected rather than extrapolating calibrated status.
- Proposed heldout service gates per supported class/concurrency stratum: median absolute percentage error<=25%, P95 absolute percentage error<=60%, median signed percentage bias within plus/minus 15%; evaluate engine-service and reservation/queue predictors separately.
- A nominal 90% prediction interval should have heldout empirical coverage 85% to 95% in supported strata; report independent-run uncertainty and withhold promotion if evidence is insufficient.
- Exact-tokenizer accounting must match accepted prompt usage; any fallback UTF8 heuristic remains explicitly tagged and must not reserve KV or impersonate exact tokens.
- Shadow-routing promotion also requires predeclared deadline/fairness nonregression and measured hardware-window cost/ranking utility; a service-error pass alone cannot establish cheaper heterogeneous routing.

**Cost validation**

- score whole-Pod allocation with measured occupied wall and all rejected/retried/idle work; report utilization/occupancy independently.
- test proxy ranking against actual hardware-window allocation across queues and workloads; proxy/allocation ratios are diagnostics, not invoices.
- preserve published projected-static16 defaults until independently reviewed holdout measurements justify a new versioned profile.

The contract is a proposal. No new profile coefficients, production calibration, source changes or aggregate-throughput-to-per-stream conversion are produced here.

## Evidence and reproducibility

Adjacent `.json` contains exact report/journal, code-semantics and analysis-dependency SHA256 values plus all aggregate distributions. Adjacent `.analysis-source.py.txt` is the exact local analysis source. To reproduce, place that source at `.runtime/analyze_prediction_error.py` in this repository and run Python without `--write` for validation. `--write` exclusively creates a new result set; existing evidence is never overwritten.

- `mixed_final_adaptive100`: `c3e5c54980dfc18e5a2e9d7672b92a375239adfaef3da22a5a164697dda63d55`
- `prefill100`: `9b56475ed30d9e70188486b2213272a00fe5f9807b1d9c484fc30c0f3124f061`
- `decode100`: `a6659a1dd28e6ed05adeb51089d3edcc2adcb8555a85454938ac33c183db6e87`
- `mixed_guard_journal`: `24dc13b6d5862012f0f6666c7466c833068abfeac4e19bf43e65131496b1bb25`
- `regime_original_guard_journal`: `6980bad830c5b0ed8884401cee927bae850eee9b2a76bd12355ca262389a7d0b`
- `regime_reconciliation`: `118230f4e37ac0f08107b9bf0486983bc83b9b65bf50e7b64409f4f5c81c0d02`

Analysis source SHA256: `645a8422de04154de4623220276478efab02a49020aced891ade003806536bda`.
