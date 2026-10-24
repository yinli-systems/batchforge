# Scheduling policy and evidence

BatchForge's v6 scheduler makes dispatch decisions inside PostgreSQL. It keeps
the durable lease/attempt transaction and adds explicit work estimates,
operator-calibrated execution profiles, runtime compatibility and a selectable
cost-aware policy. A logical profile in a test is not a physical GPU measurement.
The current deployment has one four-H100 120B engine; heterogeneous policy
correctness is tested with logical profiles against real PostgreSQL.

## Policies

`fair` rotates through eligible tenant/model queues and uses FIFO within the
selected batch. `cost_aware` preserves tenant rotation, then ranks eligible work
using predicted prefill/decode time and cost. Both v6 policies enforce batch and
task runtime fingerprints. Switching from cost-aware to fair retains these
fences. A database trigger also makes legacy claim paths reject already-bound
incompatible work during rolling deployments. Those legacy paths do not select
the next compatible task themselves, so policy rollback should use v6 `fair`.

For estimated input tokens `I`, output budget `O`, prefill rate `P`, decode rate
`D` and hourly execution-profile cost `H`, the predictor is:

```
prefill_seconds = I / P
decode_seconds  = O / D
service_seconds = prefill_seconds + decode_seconds
estimated_usd   = service_seconds * H / 3600
```

These are planning estimates. They omit concurrent batching interference, cache
hits, startup, network delay and queueing. The operator must calibrate rates for
the intended concurrency and interpret the cost as the explicitly supplied
execution-profile cost. Do not add these per-request estimates and present them
as an observed invoice. A full allocation hourly price can overstate the
incremental cost of concurrent requests sharing it.

An idle worker defers a task when a fresh, ready, compatible peer has a free
slot and a lower predicted cost (or equal cost with shorter service time), and
the peer's predicted service fits before batch expiry. Peer readiness and
capacity are advisory snapshots; the scheduler never locks other workers while
holding the claimant's worker lock. Admission remains serialized at each
worker, and task rows are locked before a durable attempt is created.

Cost-based deferral ends when task age reaches `scheduler_aging_seconds`, or
when remaining batch time is at most `1.25 * service_seconds + 1`. Urgent batches
are considered first; aged tasks precede new short tasks within a batch. This
limits the age of the *cost preference*. It does not guarantee a finite completion
latency when arrival load exceeds capacity, the compatible runtime disappears,
or tasks lie outside the candidate window. Batch expiry remains the hard deadline;
it is checked again after worker serialization and immediately before dispatch.

The transaction considers the oldest eight eligible tenant queues, and the
oldest 64 eligible tasks per candidate tenant. The selected batch's final scan
is limited to the oldest `max(64, claim_capacity)` eligible tasks, with claim
capacity at most 256. This deliberately bounds candidate ranking and introduces
head-window bias: it is not a globally optimal assignment. The peer-profile
lookup still grows with the number of workers and requires scale testing before
large-fleet claims. Expired or runtime-incompatible batches cannot hide a live
compatible queue behind the eight-tenant limit.

## Operator profiles and work estimates

`SCHEDULER_PROFILE_JSON` on an authenticated worker supplies exactly these keys:

| Key | Meaning |
| --- | --- |
| `profile_id` | Stable operator identifier for the hardware/concurrency calibration |
| `runtime_fingerprint` | Opaque compatible numerical-runtime identity |
| `prefill_tokens_per_second` | Explicit calibrated prefill rate |
| `decode_tokens_per_second` | Explicit calibrated decode rate |
| `hourly_cost_usd` | Explicit cost of this execution profile |
| `calibration_label` | Source/run/configuration label for the rates |
| `cost_source` | Quote, invoice or stated allocation assumption for the price |

There are no default rates or price. API validation and database constraints
reject nonfinite/negative values; rates must be within `1e-9..1e12` and hourly
cost within `0..1e9` to bound arithmetic. Zero cost is permitted only as an
explicit operator assumption. Tenant metadata cannot select hardware rates.
Profiles and labels are copied into each attempt's `scheduling_decision`.

New task input estimates use serialized messages' UTF-8 byte length divided by
four, rounded up; output estimates use `max_tokens`, then
`max_completion_tokens`, then the API default of 256. This is a labeled heuristic,
not exact tokenizer counting. Migration v6 backfills pending/in-flight work from
the JSONB representation and labels its slightly different estimator separately.
An insertion trigger applies the same fallback to work submitted by old API
replicas after the migration and before the policy switch.
The worker continues reserving a full configured context per slot. Neither the
heuristic input count nor predicted service time reduces its KV-cache admission
bound.

## Compatibility and failure semantics

The first profiled dispatch pins the entire batch's runtime fingerprint under
the batch lock, then pins each task as it is dispatched. Both later work in the
batch and retries require the same fingerprint. Hardware profiles may differ,
but their numerical runtime identity must match. For example, different model
revisions or materially different quantization backends should have different
fingerprints unless the operator explicitly establishes compatibility.

`backend.scheduling.runtime_fingerprint()` hashes a canonical caller-supplied
descriptor. Include model revision, tokenizer/chat-template revision,
quantization, inference backend/version and relevant numerical settings. The
helper does not discover these facts or certify equivalent outputs.

Legacy batches already attempted before v6 are marked `legacy_unverified`;
existing accepted/in-flight results are not retroactively certified. Unprofiled
legacy workers can use the fair policy for unbound work, which also records
`legacy_unverified`. Cost-aware dispatch requires a calibrated profile. A batch
pinned to an absent runtime waits for a compatible worker or expires: age and
price preferences never override its compatibility fence.

The claim transaction locks worker, eligible tenant queues, batch and tasks in
that order; it atomically writes attempt identity, task ownership and counters.
Completion still rejects expired/stale attempt tokens. A PostgreSQL rollback
therefore removes the dispatch, fingerprint update and accounting changes
together. Runtime compatibility describes a restart from the original request;
the scheduler does not migrate in-memory KV state.

## Validation

`tests/test_cost_scheduler.py` exercises real PostgreSQL, including heterogeneous
prefill/decode preferences, cheaper-peer deferral, age/deadline override,
unready/busy/stale peers, tenant fairness, incompatible runtime retries and whole
batch pinning, concurrent claims, transaction rollback, expired-batch selection,
and worker-lock waits crossing a deadline. Profile numbers in those tests are
explicit synthetic arithmetic, with no hardware performance claim.

Production assessment additionally needs repeated fixed-hardware experiments,
open-loop arrival rates above capacity, estimator error by workload class,
tenant starvation/expiry metrics and observed cost per successful useful output.
An actual one-profile deployment can validate the policy path and ordering; it
cannot demonstrate heterogeneous-hardware savings.
