-- Immutable v5: bounded input storage, tenant-scoped CAS and fenced lifecycle.
-- Existing inline files are retained. Only new externally stored inputs expire.
CREATE TABLE tenant_blob_usage (
  tenant_id text PRIMARY KEY REFERENCES tenants(id),
  stored_bytes bigint NOT NULL DEFAULT 0 CHECK(stored_bytes >= 0),
  reserved_bytes bigint NOT NULL DEFAULT 0 CHECK(reserved_bytes >= 0),
  held_tasks bigint NOT NULL DEFAULT 0 CHECK(held_tasks >= 0)
);
INSERT INTO tenant_blob_usage(tenant_id,stored_bytes)
SELECT t.id,COALESCE(sum(f.bytes) FILTER(WHERE f.kind='input'),0)
FROM tenants t LEFT JOIN files f ON f.tenant_id=t.id GROUP BY t.id;

CREATE TABLE blob_objects (
  id text PRIMARY KEY,
  tenant_id text NOT NULL REFERENCES tenants(id),
  sha256 text NOT NULL CHECK(sha256 ~ '^[0-9a-f]{64}$'),
  backend text NOT NULL CHECK(backend IN ('local','s3')),
  object_key text NOT NULL,
  bytes bigint NOT NULL CHECK(bytes > 0),
  state text NOT NULL CHECK(state IN ('writing','ready','deleting','deleted')),
  write_token text,
  write_expires_at timestamptz,
  unreferenced_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  gc_after timestamptz NOT NULL DEFAULT clock_timestamp(),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE(tenant_id,id),
  UNIQUE(backend,object_key)
);
CREATE UNIQUE INDEX blob_objects_active_digest ON blob_objects(tenant_id,sha256)
WHERE state IN ('writing','ready');
CREATE INDEX blob_objects_gc ON blob_objects(gc_after,created_at,id)
WHERE state IN ('writing','ready','deleting','deleted');

ALTER TABLE files ADD COLUMN blob_id text;
ALTER TABLE files ADD COLUMN storage_expires_at timestamptz;
ALTER TABLE files ADD COLUMN storage_deleted_at timestamptz;
ALTER TABLE files ADD COLUMN admission_tasks integer NOT NULL DEFAULT 0 CHECK(admission_tasks >= 0);
ALTER TABLE files ADD CONSTRAINT files_tenant_blob_fk FOREIGN KEY(tenant_id,blob_id)
REFERENCES blob_objects(tenant_id,id);
ALTER TABLE files ADD CONSTRAINT files_external_input_only CHECK(blob_id IS NULL OR (kind='input' AND content IS NULL));
CREATE INDEX files_blob_reference ON files(tenant_id,blob_id) WHERE blob_id IS NOT NULL;
CREATE INDEX files_input_retention ON files(storage_expires_at,id)
WHERE blob_id IS NOT NULL AND storage_deleted_at IS NULL;

CREATE TABLE blob_upload_reservations (
  id text PRIMARY KEY,
  tenant_id text NOT NULL REFERENCES tenants(id),
  sha256 text NOT NULL CHECK(sha256 ~ '^[0-9a-f]{64}$'),
  bytes bigint NOT NULL CHECK(bytes > 0),
  task_count integer NOT NULL CHECK(task_count > 0),
  blob_id text,
  expires_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  FOREIGN KEY(tenant_id,blob_id) REFERENCES blob_objects(tenant_id,id)
);
CREATE INDEX blob_upload_reservations_expiry ON blob_upload_reservations(expires_at,id);
CREATE INDEX blob_upload_reservations_object ON blob_upload_reservations(tenant_id,blob_id);

CREATE TABLE blob_read_leases (
  id text PRIMARY KEY,
  tenant_id text NOT NULL,
  blob_id text NOT NULL,
  expires_at timestamptz NOT NULL,
  FOREIGN KEY(tenant_id,blob_id) REFERENCES blob_objects(tenant_id,id)
);
CREATE INDEX blob_read_leases_object ON blob_read_leases(tenant_id,blob_id,expires_at);
-- Deleted object metadata is deliberately retained as a generation fence.
-- Retried deletes address the retired key, never a subsequently uploaded key.
