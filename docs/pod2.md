# Second Pod deployment

The second user-supplied Pod runs an independent GPU engine and worker attached
to the same primary BatchForge control plane. It does not run a second database
or a separate platform. Tenant traffic and scheduling remain on the primary Pod.

## Observed hardware and inference

On 2026-10-24 (Asia/Dubai), CUDA exposed six NVIDIA RTX PRO 6000 Blackwell Server
Edition MIG devices. Device 0 reported `MIG 1g.24gb`, 25,367,150,592 bytes total
and 25,122,439,168 bytes free before this application's engine started. NVML's
aggregate memory query returned `Insufficient Permissions`; the memory values
above came from CUDA for the application's visible device. These are MIG
devices, not six full H100 GPUs.

Only device 0 is used. The exercised stack is Python 3.12.3, Torch 2.8.0+cu128,
vLLM 0.10.2, Transformers 4.55.2, Tokenizers 0.21.4, and Hugging Face Hub 0.34.4.
The model is `Qwen/Qwen2.5-0.5B-Instruct`, float16, context limit 2048, maximum
eight active sequences, eager execution, and GPU memory utilization target 0.12.
The engine reported approximately 0.93 GiB of weights and 1.82 GiB of KV cache.
No other GPU workload was stopped or altered.

The first eight-token HTTP generation timed out after 60 seconds during initial
startup. Later bounded real requests succeeded without changing the engine:
one output token in 0.032 seconds, then eight output tokens in 0.0874 seconds
(34 input tokens). The initial timeout's cause was not established. These probes
prove that inference ran; they do not measure sustained capacity or model quality.
Do not infer generation health from `/v1/models` alone.

## Private connection

The supplied Runpod SSH gateway supports interactive sessions, but rejected
reverse TCP forwarding. Direct SSH through the Pod's mapped TCP port rejected
the supplied key. No `authorized_keys` or sshd configuration was changed.

The deployment therefore uses an application relay over the existing authorized
interactive SSH connection:

```text
second Pod worker
  -> second Pod 127.0.0.1:18082
  -> encrypted Runpod gateway SSH terminal
  -> operator host relay_local.py
  -> operator host 127.0.0.1:8787
  -> primary Pod's existing private SSH tunnel
  -> primary control plane 127.0.0.1:8080
```

Run the operator-side broker from the repository after establishing the primary
Pod's localhost tunnel:

```sh
python3 ops/pod2/relay_local.py
```

The broker prints only ready events and request/error counts. Never tee the
remote relay's raw stdout: it is a private framed protocol carrying worker
requests and responses, not an operator log. The remote endpoint binds only
localhost. Both relay ends restrict routes to registration, claims, and
`task_<32 lowercase hex>` heartbeat/completion/failure operations. They do not
forward tenant API routes, metrics, arbitrary destinations, or arbitrary headers.
The upstream address is fixed to the existing localhost tunnel. Requests and
responses are bounded to 4 MiB, frames to 6 MiB, remote concurrency to 64, and
local forwarding threads to 32. Upstream calls have an eight-second timeout.

The broker sends five-second liveness pings and an explicit close frame on exit.
The remote relay exits after 45 seconds without incoming frames, including when
a gateway fails to close its terminal input. Its PID/start identity is recorded
in the application's `.runtime/relay-18082.json`. The broker must remain alive;
the operator host and both SSH paths are temporary availability dependencies.
This is a development deployment, not a production service mesh or an HA link.

## Application runtime

All code and runtime state live under `/root/batchforge-runtime`; virtual
environments and model caches do not use the Pod's global Python environment or
other workspaces. `HF_HOME`, vLLM, TorchInductor, and Numba caches have explicit
application-owned paths. The code transfer helper `ops/pod2/proxy_exec.py` uses
the existing SSH gateway, disables terminal echo/history, and accepts only an
operator-provided source archive and script. It copies neither SSH private keys
nor GitHub credentials.

Worker configuration is `/root/batchforge-runtime/app/.env.worker-pod2-gpu0`, mode
600. After the user's explicit authorization, it received only the worker token;
the tenant API key, database credentials, and SSH private key were not copied.
Engine-only settings are in `.env.engine-pod2-gpu0`. Secrets and model caches
must never be included in a source archive or committed to Git.

The worker ID is `secondpod-blackwell-mig0`, with eight slots and a conservative
16,384-token context reservation budget. Its only engine endpoint is
`http://127.0.0.1:8100`, excluding `/v1`. Its metrics/readiness server binds
`127.0.0.1:9101`. The worker's readiness requires registration, a matching ready
engine, and recent successful control-plane communication.

Use the app-local supervisor from the Pod's application directory:

```sh
cd /root/batchforge-runtime/app
/root/batchforge-runtime/venv/bin/python ops/manage.py start engine --name gpu0 --gpu 0 --port 8100 --env-file .env.engine-pod2-gpu0 --python /root/batchforge-runtime/engine-venv/bin/python
/root/batchforge-runtime/venv/bin/python ops/manage.py start worker --name pod2-gpu0 --env-file .env.worker-pod2-gpu0 --python /root/batchforge-runtime/venv/bin/python
/root/batchforge-runtime/venv/bin/python ops/manage.py status worker --name pod2-gpu0
/root/batchforge-runtime/venv/bin/python ops/manage.py stop worker --name pod2-gpu0
```

The supervisor verifies recorded process identity before signaling its own
process group. It never discovers or stops unrelated GPU processes. Install
runtime dependencies from the repository's hashed `requirements.txt`; worker
metrics require FastAPI, Uvicorn, and Prometheus in addition to HTTPX. Engine
dependencies belong in the separate engine virtual environment with the pins
above.

## Validation

```sh
python3 -m unittest discover -s ops/pod2 -p test_relay.py -v
```

This test starts the real loopback HTTP/stdio relay, verifies request-body and
authorization framing, completes a response through the channel, rejects tenant
routes, traversal/query routes and the wrong task ID format, and verifies that
closing the channel terminates the server. It is a transport test with synthetic
payloads, not an inference benchmark.

An actual idle disconnect/reconnect check also passed. Before interruption the
worker had zero inflight tasks and 16 completed tasks. Stopping the operator
broker led to the remote port closing within a 40.527-second observation window.
After a new broker connected, the same worker process automatically registered
again and returned readiness 200; successful registration rose from 11 to 12 and
successful claims from 76 to 83. This test did not interrupt active inference or
exercise lease expiry. Durable lease-recovery tests are recorded separately.

The cross-Pod 512-task benchmark completed successfully. Independent primary
database attempt records attribute 496 accepted results to the H100 Pod and 16
to `secondpod-blackwell-mig0`, with 512 unique downloaded outputs. See the
[run report](../ops/reports/gpu-multipod-2026-10-23.json) and
[per-worker database attribution](../ops/reports/multipod-distribution-2026-10-23.json).
The metric snapshot in the deployment report remains explicitly separate from
this batch-specific attribution.

The initial relay attempt used port 18080 before liveness/PID recording was
added. Its orphan process was subsequently identified with a query restricted
to that port. Cleanup verified the exact application working directory, relay
command/port and process start identity, then sent SIGTERM through a Linux pidfd
to avoid signaling a reused PID. Only that verified BatchForge process was
stopped. Port 18082 is the exercised deployment port.
