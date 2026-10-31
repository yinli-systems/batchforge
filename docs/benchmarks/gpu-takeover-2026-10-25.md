# Four-H100 takeover: 235B baselines, storage failure, and a real 480B trial

2026-10-25. All GPU observations are from the existing Pod `4x7ej43hrqznjn`,
with four NVIDIA H100 80GB HBM3 devices. No GPU, database, storage allocation,
paid plan, or cloud endpoint was created or upgraded in this campaign. The
already-running Pod continues to incur its existing provider charges; a current
invoice/rate was not supplied, so the benchmark cost fields remain `null`.

## What actually completed

The pinned `QuantTrio/Qwen3-Coder-480B-A35B-Instruct-AWQ` checkpoint ran on all
four GPUs with TP4. This is approximately **480B total / 35B active MoE
parameters**, not a dense 480B model. The base model's
[official description](https://huggingface.co/Qwen/Qwen3-Coder-480B-A35B-Instruct)
and the quantizer's [model card](https://huggingface.co/QuantTrio/Qwen3-Coder-480B-A35B-Instruct-AWQ)
describe its architecture and quantization.

The revision is `9ce3eaa67fe88609afec235117e97eb03d9b3cda`. All 51 safetensors
shards were verified against the pinned Hugging Face LFS SHA-256 values. The
selected checkpoint and tokenizer files total **252,341,633,360 bytes**. No
repository Python was downloaded or executed. Actual startup confirmed
`trust_remote_code=False` and the **awq_marlin** kernel. The
[download receipt](gpu-takeover-2026-10-25/480b-download-receipt.json) records
each verified shard. It says `verified-local`: the complete checkpoint is on
the existing container disk, and is not certified as an off-Pod durable copy.

The experiment used vLLM 0.10.2, PyTorch 2.8.0, Transformers 4.55.2,
eager execution, FP16 activations, an 18,440-token context, 32 maximum engine
sequences, 4,096 batched tokens, chunked prefill, and disabled prefix caching.
The exact startup extract and serving-source hashes are in
[`runtime-proof.json`](gpu-takeover-2026-10-25/runtime-proof.json).

## Measured requests

Every completed phase used the same 20 input/output cells: inputs
128/512/2,048/8,192/16,384 tokens crossed with outputs 32/128/512/2,048.
Inputs are fitted SQuAD 1.1 public document text. Each phase reported exactly
**109,056 input tokens and 13,600 generated tokens**, HTTP concurrency 16,
complete SSE streams, and 20 final-text-present responses. Outputs were forced
to the requested length with `min_tokens` and ignored EOS. This measures
capacity, not answer quality. Tokenizer manifests and exact workload hashes
are included with each pack; customer prompts and generated text are absent.

| Phase | Successes | Wall s | Output tokens/s | Completion P50 s | P95 s | P99 s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 235B FP8, original FIFO | 20/20 | 177.588 | 76.582 | 32.516 | 177.054 | 177.419 |
| 235B FP8, fresh shortest burst | 20/20 | 271.501 | 50.092 | 39.357 | 270.879 | 271.376 |
| 235B FP8, fresh class-fair burst | 20/20 | 203.271 | 66.906 | 45.689 | 203.270 | 203.270 |
| 235B FP8, fresh FIFO burst | 20/20 | 172.417 | 78.879 | 29.749 | 171.777 | 172.288 |
| 480B AWQ, FIFO | 20/20 | 77.251 | 176.048 | 20.549 | 76.892 | 77.178 |

The three fresh 235B bursts completed **60/60**, after the storage failure
described below. Their dispatch order was shortest, class-fair, FIFO. Background
model download/verification changed during those phases. These are exploratory
client-dispatch measurements, not a controlled causal comparison or a change to
the PostgreSQL scheduler. They do not establish that one dispatch policy is
superior. The 235B and 480B rows use different checkpoints and quantization;
their rate difference is not an algorithmic optimization result.

The 480B pack contains 71 GPU samples. Arithmetic sample-mean utilization for
GPU 0/1/2/3 was **87.59% / 79.68% / 76.59% / 76.59%**. Every device reached
100% in at least one sample. This is not continuous four-device saturation,
SM occupancy, achieved FLOPs, or a proof of optimal utilization. Sampling gaps
are not filled with zeros. The 20 observations give weak evidence about P99.

Raw reports:

- [Original 235B phase](gpu-takeover-2026-10-25/235b-original/00-fifo-report.json).
- [All fresh 235B phases](gpu-takeover-2026-10-25/235b-spooled/result.json).
- [480B phase](gpu-takeover-2026-10-25/480b-trial/00-fifo-report.json).
- [480B GPU/engine samples](gpu-takeover-2026-10-25/480b-trial/samples.jsonl).

## Storage failure retained, not erased

The first campaign's FIFO phase passed. Its subsequent shortest phase failed
while flushing a terminal request record to `/workspace` with `OSError:
[Errno 12] Cannot allocate memory`. The campaign stopped; it was not reported
as a successful three-policy experiment. The
[failed campaign result](gpu-takeover-2026-10-25/235b-original/result.json),
request intents/terminals and original traceback remain in the pack. Requests
with uncertain outcomes were not automatically replayed as successful retries.
The later bursts are separately named experiments.

`findmnt` identified `/workspace` as **fuse.geesefs**, showing an apparent 1 PiB
capacity. That number does not establish available, reserved, or free storage.
An isolated 1 KiB append/flush/fsync probe passed 50/50 on the container disk;
on `/workspace` it passed 24 writes and then returned errno 12. A fresh-file
probe failed too, and a subsequent archive read during model preparation also
failed with errno 12. The failure persisted after stopping the original large
mount writer. The provider/mount allocator's underlying fault was not repaired
or conclusively attributed to download concurrency.

The implemented workaround is explicit:

- POSIX benchmark journals refuse FUSE mounts **before creating a campaign or
  sending inference**. Runtime code, locks, live logs and download staging use
  the existing container disk.
- Checkpoints download with bounded concurrency and verified hashes. Reusing
  an archive is opt-in; an unavailable archive falls back to the pinned public
  source. Object publication is opt-in and refuses the unsupported mount.
- Closed evidence is exported to the Mac and committed to the private repo.
  This protects the exported evidence, not unexported in-flight data or the
  complete model checkpoint. Container-local fsync is not Pod-loss durability.

This matches the distinction in RunPod's
[storage guidance](https://www.runpod.io/blog/global-volumes-arent-network-volumes):
an object filesystem mount is not a safe substitute for a local filesystem
for mutable runtime state. The actual mount's failure remains an operational
issue. Existing data and failed packs were retained.

## A failed rollout exposed stale supervision records

The first 480B attempt stopped before serving inference. The APIs on 8082/8083
were healthy, but `api-a.json` and `api-b.json` referred to dead PIDs. The
watchdog consequently attempted duplicate processes that could not bind those
ports. The trial reached its bounded restart limit and invoked restoration.
The original engine became healthy under the restored watchdog; the failed
attempt's explicit restore-failure flag was retained in
[its receipt](gpu-takeover-2026-10-25/480b-first-failed-trial.json).

The existing live API processes were reconciled only after checking exact
command lines, working directories, session/process-group ownership, readiness,
and equality of their in-memory database environment with private configuration.
Credentials were compared in memory and never exported. Their existing PIDs
475899/475900 were retained; the APIs were not restarted by this repair.
See the [repair receipt](gpu-takeover-2026-10-25/api-identity-repair.json).

The trial controller now refuses a model swap if a control-plane managed record
is stale. Candidate configuration names are unique per trial. SIGTERM/INT enter
bounded restoration, and an unready restored engine cannot receive a restarted
worker. During the normal watchdog pause the controller checks control-plane
process liveness; database readiness failures do not kill live APIs.

The second attempt loaded 480B, completed the 20-request benchmark, stopped the
candidate, and restored the original engine/worker/watchdog with
**`restore_failures: []`**. APIs, gateway and catalog configuration remained
unchanged. The [actual trial receipt](gpu-takeover-2026-10-25/480b-trial.json)
contains timestamps, GPU identities and the executed controller hash, which
matches the committed implementation. Restore completion was
2026-10-25 09:44:30 UTC, or 13:44:30 Asia/Dubai.

Post-trial live checks found API a/b, the Qwen235B worker, original engine and
watchdog running; gateway readiness reported two ready replicas. The serving
database was Azure `batchforge_import_20261025`, using `sslmode=verify-full`;
the pre-trial read-only count was 29,243 tasks and no pending/running tasks.

## Reproduction and remaining work

From a source checkout on the Pod, use a **new** local output directory:

```bash
python -m ops.gpu_campaign \
  --url http://127.0.0.1:8300 \
  --workload /path/to/pinned-workload.jsonl \
  --manifest /path/to/pinned-manifest.json \
  --out /root/batchforge-runtime/new-campaign \
  --engine-label existing-qualified-engine \
  --policies fifo shortest class-fair --concurrency 16
```

The [campaign runner](../../ops/gpu_campaign.py) fsyncs an intent before each
request and preserves each terminal record. Existing output directories are
refused. The [trial controller](../../ops/gpu_model_trial.py) requires a drained
queue and verified checkpoint and runs on the Pod without a local SSH relay.
Related focused tests: **73 passed, 3 Linux-only checks skipped on macOS**.
The real Linux rollout/recovery is recorded separately above.

This proves a real 300B+ total-parameter model experiment on this one allocation.
It does **not** prove 480B routing through the durable Batch API, heterogeneous
hardware savings, independent-node failover, response quality, sustained daily
capacity, or production HA. Next work is an isolated 480B Batch API/worker canary
with durable acceptance and metering checks, then preregistered repeated ABBA
experiments and measured admission bottleneck analysis. Do not infer ten-million
tasks/day from these finite engine bursts.
