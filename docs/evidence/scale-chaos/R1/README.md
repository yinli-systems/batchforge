# R1: baseline control-plane throughput (100 simulated workers, 100,000 tasks)

Verdict: PASS on the final clean rerun. Zero worker-side errors; generator overrun p99 11.2 ms; 520 tasks/s on this Mac.
A first attempt on the same harness returned FAIL with 2 client-side `ReadError`s (see `../README.md`); that pack was overwritten.
Simulated: 100 workers (4 processes), inference, token counts. Real: API, local PostgreSQL 16.2, migrations, SQL paths.
Not a GPU result. Not a fleet result. Single host.
