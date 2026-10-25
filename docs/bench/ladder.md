# GPU benchmark ladder (plan; no GPU run has happened)

Status: CPU-side pieces exist and are tested with a fake engine. No real GPU measurement is recorded here.

## Gates before each rung

1. **Cost**: operator-declared hourly price with a source, and approval for the spend. Without it, no GPU run.
2. **Memory**: measured free memory per GPU (nvidia-smi) that passes `backend/placement.fits_on_gpus`. Unknown memory is treated as unavailable.
3. **License**: the model's license is recorded and permits the use.
4. **Parallelism**: tensor-parallel degree no larger than the GPUs allocated to the run.

## Rungs

| Rung | Model | Purpose | Exit criterion |
|---|---|---|---|
| 0 | smallest model that fits | pipeline correctness | record validates; failures recorded |
| 1 | 7B-class, fp16 then int8/int4 | precision and KV cache effect | P50/P95/P99 and peak memory recorded |
| 2 | larger dense model with tensor parallel | parallel split | results stable across repeats |
| 3 | fault injection (worker kill mid-run) | recovery and dedupe | no duplicate accepted results |
| 4 | 300B+ target | only after gates 1-4 pass | not attempted until confirmed |

## Records

Each run writes one record with model, precision, GPU model and count, tensor parallel, input and output lengths, requests, failures by type, latency samples, output tokens/s, peak VRAM, declared hourly price with its source, and the reproduction command. `ops/bench/record.py` refuses incomplete records.
