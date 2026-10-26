# Short Agent action leases and bounded renewal

This candidate changes the ordinary Agent coordinator from a fixed120-second
lease to a **15-second lease**, renewed independently every5seconds while one
action is executing. A single action has a **45-second total deadline**. These
are trusted operator settings, not model policy or HTTP request fields. Schema
v1, immutable goals/tool budgets and stable child Batch identities are unchanged.
Serving primary748dc10 and production Agent enrollment are not changed here.

Source8ddc2ca passed **1,727 native Python/zero-skip and39 Node tests**.
Its separate [real480B qualification](benchmarks/agent-action-leases-2026-10-26.md)
passed four new Agent goals. After a real Batch ACK and coordinator exit, the
next DB lease arrived13.580339seconds later and reused the accepted child ID,
passing the predeclared30-second gate. This is one finite fault observation,
not an end-to-end tail-latency or throughput improvement claim.

The action and renewer run as separate asyncio tasks. Any renewal error or
timeout revokes local authority and cancels the action; a successful renewal
cannot extend the total action deadline. Renewal stops after action completion.
Lease/timeout loss leaves the last durable state for a future owner instead of
resetting counters, blindly reuploading or marking uncertain work successful.
The original outbox therefore reconciles an accepted upload or Batch ACK under
its original identity. Accepted GPU usage remains in the Batch ledger even if
the enclosing Agent was cancelled.

Renewal needs an independently available database connection; the provided CLI
uses a separate coordinator pool with up to four connections. A stalled pool or
database does not extend authority: renewal times out and revokes the action.

Local monotonic checkpoints precede model HTTP I/O, lookup, trusted tool dispatch
and state transitions. The conservative local validity interval starts **before**
claim/heartbeat I/O, so a delayed acknowledgement does not grant extra time.
These checks reduce stale local actions; they are not a distributed ownership
authority. PostgreSQL independently checks its actual clock, token and absolute
run deadline at the final claim, transition, renewal and idle-release statements.
An event inserted earlier in a transaction rolls back if the final ownership
condition fails. A heartbeat cannot resurrect a lease that expired while the
transaction was open, or extend ownership beyond the run deadline.

A final release/terminal commit can race a renewal refusal. If the action ACK
already completed, that ACK wins; otherwise the durable state is reconciled by
the next owner. Cancellation itself does not roll back an accepted external
request. Stable outbox keys and the original ledger preserve identity; this is
not external exactly-once execution or a hard real-time lease guarantee.

If an adapter ignores cancellation for two seconds, the guard reports a fatal
`UnstoppableAction`; the provided coordinator CLI and qualification worker stop
instead of continuing to schedule work beside an uncontrolled action. Qualified
HTTP/PostgreSQL adapters must propagate cancellation. Current trusted tools are
bounded/read-only. Arbitrary user code is not supported and needs a separately
qualified process sandbox and termination protocol.

Validation includes cooperative cancellation, noncooperative refusal, independent
renewal, delayed claim ACKs and an absolute action deadline despite successful
heartbeats. Native PostgreSQL cases cover a slow upload outliving its original
lease while a contender is refused; cancellation revoking renewal; an accepted
upload timing out and reconciling once; expiry during a transition rolling back
events/usage; expiry during renewal refusing resurrection; and deadline clipping.
Unit/fake-adapter results are separate from real-GPU process-loss qualification.

The frozen `lease-contract-heldout-v1` qualification has four new arithmetic or
memory→arithmetic goals, at most16 accepted model steps, and a predeclared
30-second Batch-ACK-to-next-DB-lease recovery gate. The existing private schema
wrapper additionally runs four decoder tasks, so total admission is bounded by20.
It uses native Unix-only PostgreSQL for Agent state and real Azure/S3/480B
children. Production Agent migration still needs owner authentication; this
candidate is not an Azure Agent rollout or whole-Pod availability proof.
