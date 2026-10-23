# Operations

Run the control plane and GPU inference engines as separate processes. Keep
database data, model caches, keys and logs outside tracked source.
The initial deployment uses an isolated directory on the user-provided Runpod.
Existing workloads must not be terminated or reconfigured.

Use `/healthz` for liveness, `/readyz` for database readiness, and the authenticated
console for operational state. A live process alone does not establish that an
engine is ready or that queued tasks are progressing.

Tested start/stop commands will be recorded after initial environment validation.
