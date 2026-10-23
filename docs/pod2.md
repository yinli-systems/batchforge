# Second Pod deployment

The second user-supplied Pod runs fourteen short-context 7B engines, one
18K-context 7B engine and one GPT-OSS-120B CPU-expert/GPU engine, each with its own
worker. It also runs two API replicas and its own private gateway. Its APIs connect directly to the
shared managed PostgreSQL database. Runtime requests require neither the primary
Pod nor the operator laptop. The initial 0.5B deployment and laptop relay are
historical.

## Hardware and model

The initial rollout activated six NVIDIA RTX PRO 6000 Blackwell Server Edition
`MIG 1g.24gb` instances, each with 46 multiprocessors and 25,367,150,592 bytes of
total memory. These are MIG instances, not six full GPUs. Before those engines
started, each selected instance had approximately 23.40 GiB free after stopping
only the recorded BatchForge legacy worker/engine. No unrelated workload was
inspected or stopped.

A subsequent read-only visibility audit found **16** accessible MIG instances
across six physical parent GPUs, distributed 1/1/4/4/2/4. Both `nvidia-smi -L` and
CUDA Driver `cuDeviceGetCount` reported 16, with 16 unique MIG UUIDs from
`cuDeviceGetUuid_v2`. In this environment, Torch 2.8.0+cu128
`torch.cuda.device_count()` reported six, while its device properties returned
parent GPU UUIDs, which repeat across MIG instances. That Torch count must not
be used as the Pod's allocated MIG count or as the deployment discovery source.
Driver 595.91.07 and CUDA runtime 12.8 were observed.

The official Torch 2.8.0 implementation prioritizes its NVML count and treats
numeric selectors as physical indices in that path. A selector beginning with
`MIG-` skips this path and uses the CUDA runtime count. The audit observed
`torch.cuda._device_count_nvml() == 6` and the runtime count equal to 16. See the
[version-pinned Torch implementation](https://github.com/pytorch/pytorch/blob/v2.8.0/torch/cuda/__init__.py#L896-L968).

Matching each verified engine process's numeric CUDA mask in a fresh
metadata-only Driver process confirmed six unique selected MIG UUIDs. The
initially activated masks 0 through 5 selected one instance on physical GPU0,
one on GPU1 and four on GPU2. Ten additional MIG instances were enumerated but
were not loaded during the frozen six-engine baseline. This audit made no GPU
allocation, MIG geometry change or service restart. Additional rollout must use
explicit `MIG-<UUID>` selectors and verify each selected instance's free memory
before loading a model.

After the baseline ended, all ten additional instances passed an allocation-only
CUDA check under their explicit MIG UUIDs: one 4096-byte tensor and a kernel
checksum of 1024 per instance. Each exposed exactly one Torch CUDA device and
had 25,130,827,776 bytes free out of 25,367,150,592 before the check. Each probe
subprocess exited and released its context. No model was loaded by this check.
See the [ten-instance CUDA proof](../ops/reports/pod2-extra-mig-allocation-2026-10-24.json)
and [inactive expansion plan](../ops/reports/pod2-expansion-plan-2026-10-24.json).

NVIDIA documents support for one compute instance per GPU instance in a CUDA
process with R570 and newer; that is not a one-instance-per-physical-GPU limit.
Its UUID-v2 API identifies the subscribed MIG compute instance. See the
[MIG enumeration documentation](https://docs.nvidia.com/datacenter/tesla/mig-user-guide/mig-device-names.html)
and [CUDA Driver UUID-v2 reference](https://docs.nvidia.com/cuda/archive/12.9.0/cuda-driver-api/group__CUDA__DEVICE.html).
The preserved [container visibility audit](../ops/reports/pod2-device-visibility-2026-10-24.json)
contains only GPU metadata, two allowlisted visibility variables, and verified
BatchForge engine identities.

The next compatibility boundary was vLLM 0.10.2 itself: its default NVML platform
converts the CUDA visibility selector to an integer. Starting an engine with a
MIG UUID therefore exited during model inspection with `ValueError`, before
serving any inference. The failed app-owned rollout was stopped and its logs
preserved. The existing six numeric-selector engines continued running.

The application now supplies a narrow opt-in platform entry point from
`ops/pod2/mig_platform`, using vLLM's supported `vllm.platform_plugins` mechanism.
It selects vLLM's own `NonNvmlCudaPlatform`, whose capability, name and memory
methods use the selected CUDA logical device. The overlay is provided only
through each engine's `PYTHONPATH`; no installed vLLM or Torch source was
modified. The selector requires a complete single MIG UUID and exactly vLLM
0.10.2. Spawned worker processes inherit it. During the ten-instance expansion,
original engines received these settings only for future UUID-based watchdog
restarts; their live processes remained unchanged in that phase.

The initial validation observed compute capability 12.0, the correct MIG name,
and 25,367,150,592 bytes of memory. MIG6 then generated eight actual output
tokens from a 1619-token document, with startup/readiness included in a
30.999242-second probe. This is a short execution check, not a completed
analysis or a throughput measurement. See the
[real plugin probe](../ops/reports/pod2-7b-mig-plugin-probe-2026-10-24.json).

This adapter is scoped to the exercised tensor-parallel size 1 and data-parallel
size 1. Built-in multi-GPU data parallelism and Ray require separate mapping
work and are not validated by this result. Relevant official source:
[plugin selection](https://github.com/vllm-project/vllm/blob/v0.10.2/vllm/platforms/__init__.py),
[official non-NVML CUDA platform](https://github.com/vllm-project/vllm/blob/v0.10.2/vllm/platforms/cuda.py),
and [data-parallel device-index handling](https://github.com/vllm-project/vllm/blob/v0.10.2/vllm/v1/engine/utils.py).

The earlier uniform sixteen-engine 7B phase used:

- Model: `Qwen/Qwen2.5-7B-Instruct`, non-quantized bfloat16.
- Revision: `a09a35458c702b33eeacc393d103063234e8bc28`.
- Model directory: `/root/batchforge-runtime/models/Qwen2.5-7B-Instruct/<revision>`.
- Four safetensors shards, totaling 15,231,271,888 bytes, each checked against the
  SHA256 published in the official Hugging Face repository metadata.
- Context limit 4096, sixteen active sequences, 4096 batched prefill tokens,
  eager execution, and explicitly disabled prefix caching.
- GPU memory utilization target 0.82; each engine reported 14.2488 GiB of model
  weights and approximately 4.56 GiB of KV cache. Following the load probe,
  devices retained approximately 3.46–3.58 GiB of free CUDA memory.

The exercised stack is Python 3.12.3, Torch 2.8.0+cu128, vLLM 0.10.2,
Transformers 4.55.2, Tokenizers 0.21.4 and Hugging Face Hub 0.34.4. Downloads use
`token=False` and include only model/tokenizer data, excluding repository Python
code. Engines load the verified local directory with Hugging Face offline mode.

The completed expansion report observed 16 unique actual process-selected MIG
UUIDs, 16 ready workers (128 inference slots), and all 35 supervised services
ready at 21:20:06.058186 UTC. All ten added engines passed a genuine document
generation, each with an eight-token output cap. Their startup/probe pipelines
took approximately 32–34 seconds each, except the already-warm MIG6 at 1.784
seconds. The original six engine PIDs remained unchanged. Their future-restart
configurations now include the application-local MIG platform selector.
Review [the final fleet identity and readiness report](../ops/reports/pod2-16mig-ready-2026-10-24.json)
and [each additional engine's generation proof](../ops/reports/pod2-7b-expansion-2026-10-24.json).

The later sequence rollout completed at 21:53:35.279201 UTC. All sixteen engines
were restarted under their explicit unique MIG UUIDs with the application-local
platform selector, including the original six. Every fresh startup reported
85,440 KV-cache tokens, exceeding the 65,536-token reservation for sixteen
4096-token contexts. Each engine passed sixteen simultaneous real generation
requests, with the engine's running-request metric observed at sixteen. All
256 diagnostic requests completed; their output was not quality-evaluated.
The model revision, precision, memory fraction, context limit, batched-token
limit and disabled prefix caching were preserved.

During that uniform stage, all sixteen workers reserved sixteen contexts each (`WORKER_MAX_INFLIGHT=16`,
`WORKER_TOKEN_BUDGET=65536`), totaling 256 secondary slots. Their frozen source
archive is `a2a3ac2f6f5be7593f13a52cbf9cc3167031c9751c8f9df2e9c1ae47323c428f`,
including JSON output validation. The watchdog resumed as PID 82999 with all
35 services running and ready; both APIs and the gateway retained their process
identities. No device needed rollback. Owner-only eight-slot configurations
remain available for rollback without being committed. See
[per-device tuning and generation evidence](../ops/reports/pod2-16seq-tuning-2026-10-24.json),
[final readiness](../ops/reports/pod2-16seq-ready-2026-10-24.json), and
[the preserved clean-workload baseline](../ops/reports/pod2-analysis-seq16-before-workers-2026-10-24.json).
The matching supervision snapshot is preserved under its own
[sixteen-sequence stage filename](../ops/reports/pod2-16seq-service-supervision-2026-10-24.json).

## Original six-engine 7B workload measurements

The first GPU0 probe completed two 128-output-token requests. Its initial
request took 59.869590 seconds, and the next took 5.234881 seconds. The whole
78.215492-second run included waiting for the engine to become ready. This
records cold-start behavior rather than hiding it in a throughput estimate.

A later concurrent run exercised all six engines: eight requests per engine,
eight concurrent requests per engine, synthetic incident documents averaging
about 1621 input tokens, and an output limit of 512 tokens. All 48 requests
returned actual model-generated responses. The run processed 77,792 prompt and
24,568 completion tokens in 24.455055 seconds including readiness checks,
approximately 1004.6 output tokens/second across the six MIG instances. Individual
request durations ranged from 23.716 to 24.353 seconds.

This is an engine-only load measurement. It excludes platform queues, database
operations, network ingress and result downloads; it is not a platform SLA or a
daily capacity projection. Forty-seven responses reached the output cap and one
stopped naturally. The test verifies execution and token accounting, not summary
completeness or business quality. Prefix caching was disabled in the observed
engine logs, and every engine advertised the pinned local model directory.

Review the preserved evidence:

- [Model identity and per-shard integrity](../ops/reports/pod2-7b-model-assets-2026-10-24.json).
- [GPU0 cold/warm request probe](../ops/reports/pod2-7b-gpu0-probe-2026-10-24.json).
- [48 real requests across all six engines](../ops/reports/pod2-7b-engine-load-2026-10-24.json).
- [Six-device configuration and memory evidence](../ops/reports/pod2-7b-hardware-2026-10-24.json).

## Engine operations

Application code lives under `/root/batchforge-runtime/app`; model data and
virtual environments live under `/root/batchforge-runtime`. Runtime dependencies
are installed from the repository's hash-locked requirements, and model-serving
dependencies remain in a separate virtual environment. Hugging Face, vLLM,
TorchInductor and Numba caches have explicit application-owned paths.

Each engine has its own mode-600 environment file, supervisor record, explicit
CUDA MIG UUID selector, and loopback inference port. The original uniform 7B
pool used `.env.engine-pod2-7b-gpu<N>` and port `8200 + N`; the current mixed
pool's exceptions are listed below.
`ops/manage.py` verifies recorded PID, process start identity and working
directory before signaling an application process group.

```sh
cd /root/batchforge-runtime/app
/root/batchforge-runtime/engine-venv/bin/python ops/pod2/upgrade_engines.py --gpu 0
/root/batchforge-runtime/venv/bin/python ops/manage.py status engine --name pod2-7b-gpu0
/root/batchforge-runtime/engine-venv/bin/python ops/pod2/report_engines.py --report .runtime/pod2-7b-hardware.json
```

These commands reproduce the earlier uniform 7B rollout; do not apply them to
the current MIG14/15 model pools. The controlled upgrade command refuses to
start until the pinned model manifest is verified and requires free GPU memory
of at least `total_memory * 0.82 + 512 MiB`. It never frees memory by terminating
another workload. The common supervisor now monitors all 35 local services, with
bounded restart backoff and this GPU memory gate. It inspects exact app-owned
records and never restarts a live API merely because its database readiness is
unavailable. The initial six-engine rollout had 15/15 running and ready; the
expanded fleet has 35/35. Existing engines and workers were preserved when the
watchdog's manifest expanded. This is process recovery, not
Pod boot orchestration or an external traffic load balancer.

The expansion helpers now discover devices through the CUDA Driver API and use
explicit MIG UUIDs for new starts, including logical indices 6 through 15.
An optional `--mig-uuid MIG-<uuid>` assertion rejects an unexpected mapping.
`plan_expansion.py --prepare-workers` prepares missing mode-600 worker files and
an **inactive** `.runtime/pod2-services-expanded.json` manifest containing 35
services. It neither launches processes nor replaces the active manifest. Every
worker entry includes its distinct metrics/readiness port. The watchdog accepts
up to 64 services; regression tests cover the 35-service topology, the 64-service
boundary, and malformed or duplicate MIG inventories. New engines still require
their individual memory gate and real generation probe before workers start.
`expand_engines.py` limits the additional rollout to two simultaneous model
startup and genuine generation pipelines. It selects only logical devices
6 through 15, refuses less than 48 GiB of container memory headroom, and records
partial failure rather than reporting an incomplete rollout as complete. Its
probe uses the existing synthetic document with at most eight output tokens;
this proves execution, not output completeness or business quality.

To reproduce the recorded engine workload:

```sh
/root/batchforge-runtime/engine-venv/bin/python ops/pod2/probe_models.py --ports 8200 8201 8202 8203 8204 8205 --requests 8 --concurrency 8 --sections 16 --max-tokens 512 --request-timeout 360 --report .runtime/pod2-7b-all-engines-probe.json
```

The probe writes prompt/output hashes, actual token usage, response finish
reasons, individual full-response durations and one generated sample per
engine. `fetch_model_reports.py` copies only allowlisted credential-free model
reports into `ops/reports`. Source transfers use `proxy_exec.py` and unique
mode-600 temporary scripts so concurrent operations cannot overwrite each other.

## Independent local control plane

The original six-engine rollout installed mode-600 configurations as
`.env.worker-pod2-7b-mig<N>`. Worker identities are `secondpod-7b-mig0` through
`secondpod-7b-mig5`. Each initially had one engine, eight slots, a 32,768-token context
reservation budget, and loopback readiness/metrics port `9200 + N`. The inference
URL uses loopback port `8200 + N`, excluding `/v1`.

Workers use `CONTROL_PLANE_URL=http://127.0.0.1:8080` and the ordered
`CONTROL_PLANE_URLS=http://127.0.0.1:8080,http://127.0.0.1:8082,http://127.0.0.1:8083`.
The second and third origins allow the worker to contact a local API directly
while the gateway process recovers. Their local gateway routes
to API-a on port 8082 and API-b on 8083. Both APIs have independent database
connection pools (minimum 1, maximum 8) using only the low-privilege `bf_runtime`
application role, with verified TLS. They verify the existing schema and do not
bootstrap tenants, create API keys or run migrations. The supplied credential is
libpq conninfo, parsed with `psycopg.conninfo.conninfo_to_dict`, rather than a URL.

```text
second Pod worker -> second Pod private gateway:8080
  -> second Pod API-a:8082 or API-b:8083 -> managed PostgreSQL over TLS
```

The local APIs returned readiness 200 and the gateway reported two ready
replicas before the six workers were drained and changed from the temporary
machine link to this local gateway. All six workers subsequently returned
readiness 200 with successful registration and claim requests. Each private
configuration retained mode 0600. The former laptop broker exited with status
zero; neither its old port 18082 nor the machine relay port 18084 is used by the
current worker configurations.

The initial eight-slot rollout used `ops/pod2/start_model_workers.py` to validate
private configurations and model identities before starting workers:

```sh
/root/batchforge-runtime/venv/bin/python ops/pod2/start_model_workers.py --devices 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15
```

That initializer expects eight-slot configurations. The active mixed-model
deployment uses the private supervision manifest and existing tuned environment
files; do not regenerate them with the initial-rollout helper.

The temporary server-side machine link was retired after the 21:58 UTC
control-plane recovery test. Its earlier dedicated user-authorized key allowed
only a fixed relay command, with no PTY, arbitrary commands or forwarding; the
user's private SSH key was never copied. No active relay is required by the
current worker request path.

Worker configurations reuse only the previously authorized worker token, without
printing it. The managed-API deployment transferred only `bf_runtime` connection
details and reused that worker token. No database owner credential, tenant API
key, model-download token or user SSH private key was copied. API configuration
files are mode 0600, and APIs have an empty bootstrap key. Private credentials
must never be committed or included in ordinary source archives.

Review [the local API cutover](../ops/reports/pod2-managed-api-cutover-2026-10-24.json),
[six-worker readiness](../ops/reports/pod2-7b-workers-ready-2026-10-24.json), and
[35-service supervision](../ops/reports/pod2-service-supervision-2026-10-24.json).
The private manifest is `.runtime/pod2-services.json`, watchdog configuration is
`.env.supervisor-pod2`, and state is `.runtime/supervision-pod2-services.json`.

## SQL rollout and observation boundaries

The SQL completion rollout installed the exact frozen backend source archive
`c15ab7572c2c937b71bc7ff3d9efffc46e93e0109108193870652f24540e660a` on both
API replicas, first with `SQL_COMPLETION_ENABLED=false`. Both were ready before
the separately operated database owner applied migration v2. Only after the
confirmed v2 SQL checksum
`68969dd3e9025787fc27e300674f9498f4065c35a8c4e1cf62f7bbfc67a9fc7b` did
the replicas roll to `true`. The six workers rolled to the same multi-origin
source archive as the primary Pod, SHA256
`7802df9b5ae567ca5ebec67808a83e5e80b1667467128de31f053fe0b6bf6001`.
No GPU engine or gateway PID changed. The watchdog returned to 15/15 running and
ready. Before the next workload, all six workers had origin index zero and zero
control failovers, with full inference histograms preserved. Review
[phase 1](../ops/reports/pod2-api-phase1-2026-10-24.json),
[phase 2](../ops/reports/pod2-api-phase2-2026-10-24.json), and
[worker pre-run measurements](../ops/reports/pod2-analysis-sql10-before-workers-2026-10-24.json).

The later claim-path rollout used frozen backend archive
`7afac375ad033c8f920936348ef7db210fa0e0e4ef568886615944c0c0fec849`
with eleven verified source files. Both replicas first ran compatible code with
SQL claim disabled and SQL completion still enabled. After the database owner
confirmed migration v3 checksum
`0e3e483eb061972679ea62bacc5eb1569c334c13c0d553dbe10137ea4fe35003`,
both replicas rolled to SQL claim enabled. Direct flag checks confirmed both
features were true. All sixteen engine and worker PIDs, plus the gateway PID,
were preserved across both API-only maintenance windows, and the 35-service
watchdog was restored. See [claim phase 1](../ops/reports/pod2-api-claim-phase1-2026-10-24.json)
and [claim phase 2](../ops/reports/pod2-api-claim-phase2-2026-10-24.json).

The API maintenance caused pre-run worker failover counters to increase:
MIG9 had three cumulative failovers and returned to origin zero; MIG13 had one
and selected origin one (direct API-a). Other workers remained at zero. These
are recorded in the [claim pre-run snapshot](../ops/reports/pod2-analysis-sqlclaim20-before-workers-2026-10-24.json).
Comparisons must use after-minus-before counters, disclose the different
starting origin, and avoid treating the workload as a perfectly isolated SQL
microbenchmark. No worker was restarted merely to reset these measurements.

The completed 20-engine SQL v3 workload processed all 2048 tasks without terminal
failures in 86.949935 seconds (23.553784 tasks/second across both Pods). Its exact
measurement window was 21:39:18.247531–21:40:45.197500 UTC. The secondary's
43 fully contained samples cover 21:39:19.321759–21:40:43.603139 UTC, with no
API scrape failures or excluded counter-reset intervals. Claim requests averaged
49.50 ms (p95 histogram bucket 25–50 ms); completion requests averaged 49.74 ms
(p95 bucket 50–75 ms). These observations concern this workload and do not prove
a daily capacity or end-to-end task latency. All sixteen workers were ready in
the preserved after snapshot; no worker's failover counter increased during
the comparison. The earlier MIG9 and MIG13 counters and MIG13's direct-API origin
remained unchanged. See the [after snapshot](../ops/reports/pod2-analysis-sqlclaim20-after-workers-2026-10-24.json)
and [strict observation window](../ops/reports/squad7b-analysis-2048-sql20v3-secondary-observation-2026-10-24.json).

The clean sixteen-sequence run completed all 2048 tasks without failures or
retries in 59.80328 seconds (34.245613 tasks/second across both Pods), measured
from 21:53:59.555580 to 21:54:59.358881 UTC. Its
[strict secondary observation](../ops/reports/squad7b-analysis-2048-seq16-secondary-observation-2026-10-24.json)
contains 29 samples, no API scrape failures and no counter-reset exclusions.
The [mid-load worker snapshot](../ops/reports/pod2-analysis-seq16-mid-workers-2026-10-24.json)
at 21:54:22.900492 UTC observed 255 in-flight requests across 256 secondary
slots, and all sixteen workers had completed platform tasks.

The [later worker snapshot](../ops/reports/pod2-analysis-seq16-after-workers-2026-10-24.json)
was captured at 21:55:46.939922 UTC, after the next 8192-task load began at
21:55:24 UTC. Its cumulative counters therefore span both runs; subtracting the
earlier worker baseline cannot establish clean-run task attribution. Use the
exact database batch ID and timestamp-bounded observations for that purpose.

During the subsequent 8192-task run, the primary gateway and both primary APIs
were deliberately unavailable from 21:56:03.156472 to 21:58:03.201409 UTC
(120.044924 seconds). The independently checked batch advanced from 591 to
3196 durable results on the secondary during that interval. All 8192 tasks
eventually completed, with no failed or cancelled tasks; 61 expired attempts
accounted for the 8253 total attempts. See the
[full run](../ops/reports/squad7b-analysis-8192-fault20-v3-seq16-2026-10-24.json),
[batch attribution](../ops/reports/squad7b-analysis-8192-fault20-v3-seq16-attribution-2026-10-24.json), and
[fault evidence](../ops/reports/controlplane-outage-bench-b868274f8fe7-2026-10-24.json).
This exercised an application control-plane outage, not Pod power loss or a
managed-database failover.

The [secondary outage-window observation](../ops/reports/pod2-fault8192-outage-secondary-observation-2026-10-24.json)
contains 58 fully contained samples, spanning 21:56:03.388110–21:58:01.212948 UTC.
It records 2220 claim HTTP calls with mean 70.44 ms and p95 bucket 75–100 ms,
and 2750 completion HTTP calls with mean 582.87 ms and p95 bucket 2.5–5 seconds.
There were two API-a metrics scrape failures and one API-b scrape failure, with
no counter-reset exclusions. HTTP calls include idempotent retries and any
non-success responses; they are not unique durable task counts. The source raw
slice is retained with SHA256
`a887769f413e32667a55cf81bcc16c399186ab89fe0f22cc6d56d7d023e2b170`.
The [full-run secondary observation](../ops/reports/pod2-fault8192-full-secondary-observation-2026-10-24.json)
is separately bounded to 21:55:25.847724–21:59:32.633354 UTC.

The baseline secondary observer started after the inference benchmark began.
Its frozen summary includes 314 samples fully contained in the requested
20:45:04.019575–20:57:30.032685 UTC window; actual observation coverage is
20:46:59.966385–20:57:28.481673 UTC. The beginning is missing and must not be
reconstructed or described as full-run coverage. The original raw stream is
preserved; an additional frozen 2,675,135-byte raw slice has SHA256
`8766f3755745288a4e5b53e2da31693ef290c241fe7c50f21316a85d3fa5bdcf`.
See the [bounded baseline observation](../ops/reports/squad7b-analysis-2048-baseline-secondary-observation-2026-10-24.json).

The original observer ran for 1800 seconds. Its application-owned successor
waited for the verified predecessor identity to exit, then started at
21:17:01.059382 UTC with the same read-only sampler into a new
`controlplane-secondary-ab-20261024.jsonl` file for 3600 seconds.
It did not stop the predecessor, deliberately overlap sampling or
depend on a client SSH session. API counter resets during rolling deployments
are excluded by the sampler's summary calculation.

## Historical validation

The earlier 0.5B cross-Pod run completed 512 tasks, with independent database
attempt records attributing 496 results to the H100 Pod and 16 to the second
Pod. That run is preserved separately and must not be presented as a 7B result:
[run report](../ops/reports/gpu-multipod-2026-10-23.json),
[database attribution](../ops/reports/multipod-distribution-2026-10-23.json).

The earlier idle relay disconnect/reconnect test observed port closure within
40.527 seconds and automatic recovery of the same worker process. It did not
interrupt active inference or exercise lease expiry. The initial failed legacy
relay was later stopped only after exact app path, command and start-identity
verification, using a pidfd to prevent PID reuse. The local HTTP/stdio framing
and route-allowlist tests remain available:

```sh
python3 -m unittest discover -s ops/pod2 -p test_relay.py -v
```

## Long-context and 100B preparation

A read-only check at 22:25:29 UTC found all 35 existing services ready before
any model changes. The container has a 501,999,996,928-byte memory limit
(467.52 GiB), with 443,634,458,624 bytes (413.16 GiB) then available; the host's
larger physical RAM is not the container allocation. The app-owned filesystem
had 1,052,933,414,912 bytes available. See the [resource snapshot](../ops/reports/pod2-100b-capacity-readonly-2026-10-24.json)
and [per-MIG memory/toolchain queries](../ops/reports/pod2-100b-memory-build-metadata-2026-10-24.json).

Native CUDA metadata reports 38 directed peer-access-capable pairs, within
same-parent MIG groups of sizes 1/1/4/4/2/4. Cross-parent pairs return false.
This is capability metadata, not a bandwidth or collective benchmark. The
current NCCL is 2.27.3 and the vLLM application plugin is deliberately TP1/DP1.
[NVIDIA's newer NCCL documentation](https://docs.nvidia.com/deeplearning/nccl/user-guide/docs/usage/communicators.html#using-mig-instances)
introduces experimental MIG communicators beginning with NCCL 2.31 and CUDA
driver 13.0; it does not establish that this deployed stack can pool MIG memory.
Sixteen replicas of the same 7B model are not a single 100B model.

The two APIs now have a tested model catalog containing the existing 7B model,
`Qwen/Qwen2.5-7B-Instruct-18k`, and `openai/gpt-oss-120b`, with a 2,048-token
output cap. The rolling deployment changed no schema and preserved every
non-API service identity. The [rollout report](../ops/reports/pod2-multimodel-api-roll-2026-10-24.json)
verifies readiness and the catalog constructed from each actual private API
environment; it does not claim an authenticated tenant HTTP catalog request.
The long-context alias is necessary to keep 16K inputs out of 4K workers.

## Current model pools

The model-specific worker identities prevent a 16K or 117B request from being
claimed by a short-context 7B engine. The current secondary has 16 engines,
16 workers, two APIs and one gateway: 35 supervised services, plus the watchdog
process itself. It reserves 229 inference slots across three pools:

| MIG instances | Served model | Context reservation | Slots per worker | Inference / metrics ports |
| --- | --- | ---: | ---: | --- |
| 0–13 | `Qwen/Qwen2.5-7B-Instruct` | 4096 | 16 | 8200–8213 / 9200–9213 |
| 15 | `Qwen/Qwen2.5-7B-Instruct-18k` | 18440 | 4 | 8215 / 9215 |
| 14 | `openai/gpt-oss-120b` | 18440 | 1 | 8314 / 9214 |

The long 7B worker is `secondpod-7b-18k-mig15`; its engine and worker files are
`.env.engine-pod2-7b-gpu15-18k` and `.env.worker-pod2-7b-mig15-18k`.
Its model weights remain the pinned original 7B revision. The engine advertises
18440 context tokens, enables chunked prefill with a 2048-token batched limit,
and reported 90,512 KV-cache tokens, exceeding the four-context reservation of
73,760. Prefix caching stays disabled. The short pool retains its earlier
4096-context, sixteen-sequence configuration. See the
[model-pool activation record](../ops/reports/pod2-model-pools-ready-2026-10-24.json).

The 117B worker is `secondpod-gptoss120b-cpu-expert`, with one slot and an
18,440-token reservation. Its engine is `pod2-gptoss120b-mig14`; the current
engine environment is `.env.engine-pod2-gptoss120b-mig14-moe28`. The former
35-CPU-expert configuration is retained privately for rollback. All pools use
the same local API origins and require no primary-Pod inference endpoint.

### Actual 117B model and runtime

The model is the official maintainer's conversion
[`ggml-org/gpt-oss-120b-GGUF`](https://huggingface.co/ggml-org/gpt-oss-120b-GGUF/tree/7200b74bea78b0a2964ef9f3897eefa401e890ff),
pinned at revision `7200b74bea78b0a2964ef9f3897eefa401e890ff`. The sole downloaded
weight file is `gpt-oss-120b-MXFP4.gguf`, 63,387,346,208 bytes, verified with
SHA256 `582bd40f6886200101f4c4ed9f25f3fe80cc14c86e9e2b37746cd8904a0c622d`.
Its parsed GGUF header contains 116,829,156,672 logical parameters, 36 layers,
128 experts and four selected experts per token. Expert tensors use MXFP4,
non-expert quantized tensors use Q8_0, and auxiliary tensors use F32. This is
one model exceeding 100 billion total parameters; its sparse active-parameter
count and quantization must not be described as a dense 120B BF16 model.
See [asset integrity](../ops/reports/pod2-gptoss120b-ready-assets-2026-10-24.json)
and [parsed tensor metadata](../ops/reports/pod2-gptoss120b-gguf-header-2026-10-24.json).

The application-owned CUDA build uses official llama.cpp commit
[`0ae0e1976670fac25f4e0ed864df30021accc764`](https://github.com/ggml-org/llama.cpp/tree/0ae0e1976670fac25f4e0ed864df30021accc764),
CUDA 12.8 and architecture SM120. Its `llama-server` binary has SHA256
`163bc895c6e471380b5e42be3f474ac1b9cc20e6cfdf967339a6c09cbc4dc4dd`.
Source and build output live under `/root/batchforge-runtime/llama-cpp/<commit>`;
no installed vLLM, Torch or system package was changed. The first preparation
attempt rejected a source-archive directory entry before building; its report
is preserved separately from the successful build retry.

The current engine keeps 28 of 36 MoE layers on CPU and eight on the selected
MIG instance, with non-MoE layers offloaded to GPU. It uses 16 generation and
batch threads, one slot, batch size 2048, microbatch size 512, `--fit off`,
`--no-context-shift`, `--jinja`, `--metrics` and `--perf`. Requested context is
18440; llama.cpp pads its actual allocation to 18688. Admission and worker
reservation remain 18440. The CPU allocation query found a 108.8-vCPU cgroup
quota and visible cpuset 0–255 on AMD EPYC 9554 hardware. These are container
bounds, not a dedicated 256-core allocation.

The supervisor requires free GPU memory of at least `0.82 * total + 512 MiB`
before a new engine start. For llama.cpp, 0.82 is an admission gate, not a
runtime GPU-memory-fraction option. Its separate host-memory gate requires both
host available memory and cgroup remaining memory to cover the model file plus
80 GiB. It only stops processes whose saved start identity, application cwd and
owned process tree match; it never frees space by terminating other workloads.

### Uncached expert-placement comparison

The same exact request body, containing 16,384 rendered prompt tokens and a
2,048-token output cap, was measured before and after changing only CPU-expert
placement from 35 to 28 layers. Both requests reported 16,384 prompt and 2,048
completion tokens, zero cached tokens, a nonempty final response and
`finish_reason=length`. They used natural generation mode without forced
minimum length; reaching the cap does not prove the answer was complete.

| CPU MoE layers / GPU MoE layers | Request elapsed | Server prefill | Server decode |
| --- | ---: | ---: | ---: |
| 35 / 1 | 113.396 s | 274.846 tokens/s | 38.138 tokens/s |
| 28 / 8 | 98.640 s | 321.451 tokens/s | 43.041 tokens/s |

This is one paired observation, not a production percentile or a claim about
GPU-only performance. Thirty-three other service process identities remained
unchanged. The 28-layer rollout restored its worker and watchdog; 425 GPU-memory
samples observed a peak of 16,956,915,712 bytes and a minimum of 8,410,234,880
bytes free. Per-MIG SM utilization returned NVML `NOT_SUPPORTED`, so utilization
is explicitly unavailable. Full-parent utilization would include unrelated
slices and is not substituted. See the [35-layer request](../ops/reports/pod2-gptoss120b-long-moe35-2026-10-24.json),
[28-layer request](../ops/reports/pod2-gptoss120b-long-moe28-2026-10-24.json), and
[controlled placement change](../ops/reports/pod2-gptoss120b-moe28-tuning-2026-10-24.json).
The startup 32-token arithmetic response contained reasoning but no final
answer; the subsequent 128-cap response produced the correct numeral with a
warm prompt cache. Those startup checks are not cold throughput measurements.

### Exact token counting and cache policy

The reusable [llama.cpp counter](../scripts/llama_prompt_counter.py) obtains the
running server's `/apply-template` output and counts it through `/tokenize` with
special-token parsing. The pinned official server uses the same request parser
for template application and inference. An actual 80-prompt-token response
calibrates the counter; model alias, server context and runtime/model revisions
are checked before measuring. Formatter-affecting request extensions are
rejected rather than silently using an inexact template. The counter's probe
permits natural generation only, distinguishes `stop` from `length`, and records
response hashes instead of publishing response text. A persistent exclusive
`.started` marker prevents an uncertain request from being automatically
replayed under the same report name.

Direct llama.cpp measurements set `cache_prompt=false` per request, and actual
usage must show zero cached tokens. This field is an engine-only measurement
option, not a newly exposed tenant API field. The 7B engines separately disable
vLLM prefix caching. The fixed-capacity pilot verified that `ignore_eos=true`
with a cap of 128 produced exactly 128 tokens, whereas natural generation on
the same uncached input stopped at 39. The frozen harness also sends
`min_tokens`; actual counted output is the capacity gate, not an assumption
that llama.cpp implements vLLM's minimum-token semantics. Fixed-length output
is not a quality score. See the [compatibility pilot](../ops/reports/pod2-gptoss120b-force-pilot-2026-10-24.json)
and [counter calibration](../ops/reports/pod2-gptoss120b-tokenize-calibration-2026-10-24.json).

The observed provider allocation price was $11.18/hour for this entire Pod.
Any elapsed-time cost derived from it concerns that allocation, including CPU,
RAM and all allocated MIG instances. Dividing it by sixteen is not evidence of
a purchasable dedicated-MIG price or the marginal cost of one inference.

### Long-context 7B dispatch experiment

A separate engine-only run on MIG15 completed all 20 length combinations under
each client dispatch policy, with concurrency four: FIFO 208.372 seconds,
shortest requested work 224.894 seconds, and class-fair 197.351 seconds. Each
processed 109,056 prompt and 13,600 generated tokens. The public text was fitted
to the running model's template; outputs were forced to their capacity caps.
These are bounded client dispatch policies, not a replacement for vLLM's
internal scheduler. See the [manifest](../ops/reports/pod2-matrix7b-18k-workload-manifest-2026-10-24.json)
and [FIFO](../ops/reports/pod2-matrix7b-18k-fifo-2026-10-24.json),
[shortest](../ops/reports/pod2-matrix7b-18k-shortest-2026-10-24.json) and
[class-fair](../ops/reports/pod2-matrix7b-18k-class-fair-2026-10-24.json) reports.

The isolated llama.cpp build used eight CPU jobs during parts of FIFO and
shortest; the class-fair run began after compilation completed. This overlap,
the single repetition and only 20 requests per policy preclude attributing the
observed difference solely to dispatch policy or treating p99 as a stable SLA.
The [end-of-run engine histograms](../ops/reports/pod2-longctx-engine-histograms-2026-10-24.json)
cover 61 requests, including the untimed warmup, without per-policy baselines.
They must not be divided into invented per-policy prefill/decode measurements.

### 117B mixed-length capacity run

The single-slot CPU/GPU lane completed all 20 input/output combinations from
23:04:46.234160 to 23:15:52.596131 UTC: 109,056 prompt and 13,600 generated
tokens in 666.361 seconds, or 20.409 generated tokens/second over the full mixed
run. This used FIFO client dispatch at concurrency one, 28 CPU-expert layers,
16 threads and the pinned llama.cpp/GGUF assets above. It compared the same
length grid as the primary, with runtime-specific chat formatting and fitted
public text; it does not claim byte-identical prompts across runtimes.

All twenty actual prompt counts matched their intended 128/512/2048/8192/16384
lengths, and all twenty completion counts reached the 32/128/512/2048 caps.
Every appended engine timing pair processed the entire reported prompt count
and matched the completion count. The stream harness does not retain the
`cached_tokens` field; this independent exclusive-client log evidence, together
with `cache_prompt=false`, establishes that cached prefixes were not counted
as prefill work. The raw private report and appended-log hash are preserved.

The engine recorded 359.803 seconds of prefill and 303.189 seconds of decode
across the run. These are combined CPU/GPU engine stages, not isolated CUDA
kernel timings. HTTP service p95 was 74.128 seconds and first visible text p95
was 51.827 seconds. Completion p95 was 605.720 seconds because the entire
20-request burst was offered at once to one slot; it includes client FIFO wait.
Only ten streams contained final response text. All-stream capacity success
therefore must not be presented as twenty successful business answers.

The 2626 GPU-memory samples observed a peak of 17,140,482,048 bytes and a minimum
of 8,226,668,544 bytes free. The known llama.cpp process consumed a mean of
7.850 CPU cores over this window; its thread setting remained sixteen. SM
utilization was unavailable through per-MIG NVML. An immediate post-run check
found 35/35 services running and ready, sixteen unique engine MIG selectors,
and all verified process start identities predating the timed run. See the
[full per-cell report](../ops/reports/pod2-gptoss120b-matrix-fifo-2026-10-24.json),
[stage accounting](../ops/reports/pod2-gptoss120b-matrix-stage-analysis-2026-10-24.json),
[token manifest](../ops/reports/pod2-gptoss120b-matrix-workload-manifest-2026-10-24.json),
and [post-run service identities](../ops/reports/pod2-120b-moe28-service-supervision-2026-10-24.json).

The optional `reasoning_effort` parameter was deployed with a worker-first
protocol: all sixteen secondary workers reached the reviewed runtime before
API validation changed, and the primary's sole active worker confirmed the
same source before either secondary API rolled. Omitted values preserve the
existing default; explicit values are `low`, `medium` or `high`. The secondary
worker rollout preserved nineteen nonworker identities; its subsequent API
roll preserved all thirty-three non-API identities. The supervisor's optional
memory-margin setting retained the secondary's default of 512 MiB, without
changing or restarting any engine. See the [worker rollout](../ops/reports/pod2-reasoning-effort-workers-roll-2026-10-24.json)
and [API rollout](../ops/reports/pod2-reasoning-effort-api-roll-2026-10-24.json).

The [final 23:27:46.241758 UTC check](../ops/reports/pod2-final-model-pools-ready-2026-10-24.json) found all 35 services running and ready,
all sixteen actual engine selectors matching unique MIG UUIDs, every service
environment file at mode 0600, and exact process identities matching the
reviewed rollout records. No current runtime request depends on a relay. The
read-only observer continued through a non-overlapping pidfd handoff at
23:17:02.624497 UTC into a separate recovery stream; it did not signal or overlap
its predecessor. Its PID and source hash are preserved in the
[handoff record](../ops/reports/pod2-observer-120b-recovery-handoff-2026-10-24.json).
