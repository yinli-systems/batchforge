# Matched pair receipt and allocation context

The plot cost panel uses each runner window. This separate table includes the parent operation wall, collection and cleanup.

| Scope | Wall seconds | Assumed USD at $16.10/Pod-hour |
| --- | ---: | ---: |
| Sum of runner windows | 3878.975963 | 17.347643 |
| Additional pair wall | 259.830383 | 1.162019 |
| Whole terminal pair | 4138.806346 | 18.509662 |

controller/collection/audit/inter-round and final cleanup/comparison plus process overhead; no individual stage attribution. preparation, later metadata export/transport, local rendering and later idle time. No measured invoice or marginal cost.

- baseline: offered 720, accepted/completed 600/600; full-plan passed=false, partial_plan=true.
- candidate: offered 720, accepted/completed 720/720; full-plan passed=true, partial_plan=false.

SVG bytes were normalized after the unchanged frozen renderer. The chart audit records both pre/post SVG hashes and this wrapper source SHA; it does not claim unmodified original-helper output.

Manifest SHA256: `205f298a12c76cb9ba75fe9e577471fd0fb40c52deec78c1bdd953acf7d224d7`. Original terminal pair-journal SHA256: `9cb0438bd21a46823e09d45fae857796e799bdf327f755a80d34d868ad47ce24`. Receiver SHA256: `5534f19cb83c5408af41cd6497d3c6307039ca6748cb93f8c6c3512d6da592d6`.
