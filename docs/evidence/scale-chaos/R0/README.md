# R0: harness correctness (10 simulated workers, 10,000 tasks)

Verdict: PASS. All invariants held; zero worker-side errors; generator overrun p99 3.1 ms.
Simulated: workers, inference, token counts. Real: API, PostgreSQL 16.2 (local), migrations, SQL paths.
Reproduce: see `../README.md`. Raw structure: `summary.json`, `config.json`, `observer.jsonl`.
Earlier R0 pack (before the harness was made reproducible): commit `8b37338`, in the `feat/gpu-inventory-evidence` branch.
