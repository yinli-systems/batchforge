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
-- Do not grant ALTER/CREATE, tenant/key administration, DELETE or replication.
-- Existing deployment-wide PUBLIC grants must also be reviewed separately.
