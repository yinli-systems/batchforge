-- Run as the migration/schema owner AFTER backend.migrate has initialized schema.
-- Create a LOGIN role bf_runtime with a securely supplied password separately.
-- This file contains no password and never grants schema ownership or CREATE.
GRANT CONNECT ON DATABASE batchforge TO bf_runtime;
GRANT USAGE ON SCHEMA public TO bf_runtime;
GRANT SELECT ON batchforge_schema_migrations TO bf_runtime;
GRANT SELECT ON tenants,api_keys TO bf_runtime;
-- PostgreSQL row locks require UPDATE on at least one column. Admission takes
-- FOR NO KEY UPDATE on this row; tenant/key creation remains operator-only.
GRANT UPDATE(name) ON tenants TO bf_runtime;
GRANT SELECT,INSERT,UPDATE ON workers,files,batches,tasks,attempts,idempotency,tenant_queues TO bf_runtime;
-- v5 catalog lifecycle deletes only ephemeral reservation/read-lease rows.
GRANT SELECT,INSERT,UPDATE ON tenant_blob_usage,blob_objects TO bf_runtime;
GRANT SELECT,INSERT,UPDATE,DELETE ON blob_upload_reservations,blob_read_leases TO bf_runtime;
GRANT SELECT,INSERT,UPDATE ON tenant_result_usage,result_archive_runs,result_archive_files,result_archive_chunks TO bf_runtime;
GRANT SELECT,INSERT,UPDATE,DELETE ON result_read_leases TO bf_runtime;
-- v9 admission policy is persisted and operator-managed; the runtime can only
-- read limits. Derived usage updates are protected by nested accounting triggers.
GRANT SELECT ON tenant_retention_limits TO bf_runtime;
GRANT SELECT,UPDATE ON tenant_retention_usage TO bf_runtime;
-- v10 private SECURITY DEFINER triggers maintain auxiliary counters. The API
-- has no direct counter/policy mutation or internal-function EXECUTE privilege.
GRANT SELECT ON tenant_auxiliary_limits,tenant_auxiliary_usage TO bf_runtime;
-- v11: preserved live+retired metering and read-only policy. The runtime may
-- validate replay horizons but cannot retire rows, change policy, advance the
-- replay floor or mutate the compacted metering ledger.
DO $history_grants$
BEGIN
 IF EXISTS(SELECT 1 FROM batchforge_schema_migrations WHERE version=11) THEN
  GRANT SELECT ON tenant_history_retirement_policy,retired_attempt_hourly,batchforge_attempt_hourly TO bf_runtime;
  GRANT EXECUTE ON FUNCTION batchforge_idempotency_epoch(text),
   batchforge_idempotency_check(text,text,text),batchforge_idempotency_admit(text,text,text) TO bf_runtime;
 END IF;
END
$history_grants$;
-- Do not grant ALTER/CREATE, tenant/key administration, task/result DELETE or replication.
-- Existing deployment-wide PUBLIC grants must also be reviewed separately.
