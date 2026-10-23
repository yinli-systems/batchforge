# Independent HTTPS ingress proposal

Both GPU Pods now run independent API replicas against the same managed Neon
database. Their workers use their own Pod's loopback control plane. This removes
the operator laptop and the other GPU Pod from the inference path. Private
management access still uses SSH.

The owner selected private ingress on 2026-10-24. Do not create or bill the
proposed Render service without a later explicit instruction. An independent,
redundant public HTTPS ingress is a separate deployment step.
The current Runpod Pods have no application HTTP port exposed. The actual Edit
Pod dialog warns that changing their configuration resets the running Pod and
erases container data. No edit was saved and no Pod was reset. Existing model
workloads must not be destroyed to add ingress.

`ops/render.yaml` is a concrete proposal for a stateless API service in Frankfurt
with two fixed instances, managed TLS, readiness routing and no persistent disk.
It uses the existing hashed runtime requirements and the same Neon state machine.
Only the low-privilege database login and worker token enter this service; no
tenant bootstrap key or database owner credential is required. Migrations run
separately before rollout. Automatic deploys are off, so code push alone cannot
silently change the running service.

As checked on 2026-10-24, the `0.5c-512mb` compute plan is $7 per instance/month;
two instances are $14/month compute, on a $0 Hobby workspace. Included bandwidth
is 5 GB/month, then $0.15/GB. This is an initial small ingress deployment, not a
capacity commitment for ten million tasks/day or a contractual availability SLA.
Larger instances and bandwidth must be sized from the actual workload. No paid
service has been created by this proposal.

Before approval, confirm provider/account, private GitHub repository access, the
two-instance compute cost and the permitted bandwidth budget. A new account,
terms or payment step requires the account owner's completion. After deployment:

1. Supply the existing direct, verify-full `bf_runtime` Neon connection through
   the service's secret settings. Supply the separate worker credential without
   printing either value or committing it to Git.
2. Verify both replica health checks and run the ordinary authenticated batch
   contract against the actual HTTPS URL. Restart a replica during active work,
   replay an accepted creation/completion, and compare unique usage/results.
3. Add that HTTPS origin to each Pod worker's `CONTROL_PLANE_URLS`, retaining its
   own local API as a fallback. Token-bearing requests never use cleartext
   public HTTP. Verify operation after stopping the operator SSH tunnel.
4. Keep database migrations and operator credentials outside runtime services.
   Keep the Neon production plan/SLA, backup retention and database RTO as
   separately approved requirements.

Sources: [Render pricing](https://render.com/pricing),
[manual scaling and load balancing](https://render.com/docs/scaling),
[Blueprint fields](https://render.com/docs/blueprint-spec),
[Runpod exposed-port configuration](https://docs.runpod.io/pods/configuration/expose-ports).
