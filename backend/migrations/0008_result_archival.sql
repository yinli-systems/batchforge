-- Immutable v8. Terminal result artifacts are opt-in; migration moves/deletes
-- no existing task payload. Deploy leased readers before enabling publication.
ALTER TABLE public.batches ADD CONSTRAINT batches_tenant_identity UNIQUE(tenant_id,id);
ALTER TABLE public.files ADD CONSTRAINT files_tenant_identity UNIQUE(tenant_id,id);
CREATE TABLE public.tenant_result_usage (
  tenant_id text PRIMARY KEY REFERENCES public.tenants(id),
  stored_bytes bigint NOT NULL DEFAULT 0 CHECK(stored_bytes>=0),
  reserved_bytes bigint NOT NULL DEFAULT 0 CHECK(reserved_bytes>=0)
);
CREATE TABLE public.result_archive_runs (
  id text PRIMARY KEY,
  tenant_id text NOT NULL REFERENCES public.tenants(id),
  batch_id text NOT NULL,
  backend text NOT NULL CHECK(backend IN ('local','s3')),
  state text NOT NULL CHECK(state IN ('building','ready','deleting','deleted')),
  writer_token text,
  lease_expires_at timestamptz,
  reserved_bytes bigint NOT NULL DEFAULT 0 CHECK(reserved_bytes>=0),
  bytes bigint NOT NULL DEFAULT 0 CHECK(bytes>=0),
  manifest_sha256 text CHECK(manifest_sha256 ~ '^[0-9a-f]{64}$'),
  purge_started_at timestamptz,
  purge_cursor text,
  rows_purged bigint NOT NULL DEFAULT 0 CHECK(rows_purged>=0),
  purged_at timestamptz,
  gc_after timestamptz NOT NULL DEFAULT clock_timestamp(),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  ready_at timestamptz,
  UNIQUE(tenant_id,id),
  FOREIGN KEY(tenant_id,batch_id) REFERENCES public.batches(tenant_id,id),
  CHECK(state<>'ready' OR (manifest_sha256 IS NOT NULL AND writer_token IS NULL))
);
CREATE UNIQUE INDEX result_archive_active_batch ON public.result_archive_runs(tenant_id,batch_id)
  WHERE state IN ('building','ready');
CREATE INDEX result_archive_gc ON public.result_archive_runs(gc_after,id) WHERE state<>'ready';
CREATE TABLE public.result_archive_files (
  tenant_id text NOT NULL,
  archive_id text NOT NULL,
  file_id text NOT NULL,
  kind text NOT NULL CHECK(kind IN ('output','error')),
  bytes bigint NOT NULL DEFAULT 0 CHECK(bytes>=0),
  line_count integer NOT NULL DEFAULT 0 CHECK(line_count>=0),
  expected_count integer NOT NULL CHECK(expected_count>0),
  sha256 text CHECK(sha256 ~ '^[0-9a-f]{64}$'),
  PRIMARY KEY(archive_id,file_id),
  UNIQUE(tenant_id,archive_id,file_id),
  FOREIGN KEY(tenant_id,archive_id) REFERENCES public.result_archive_runs(tenant_id,id),
  FOREIGN KEY(tenant_id,file_id) REFERENCES public.files(tenant_id,id)
);
CREATE TABLE public.result_archive_chunks (
  id text PRIMARY KEY,
  tenant_id text NOT NULL,
  archive_id text NOT NULL,
  file_id text NOT NULL,
  ordinal integer NOT NULL CHECK(ordinal>=0),
  sha256 text NOT NULL CHECK(sha256 ~ '^[0-9a-f]{64}$'),
  bytes bigint NOT NULL CHECK(bytes>0),
  backend text NOT NULL CHECK(backend IN ('local','s3')),
  object_key text NOT NULL,
  state text NOT NULL CHECK(state IN ('writing','ready','deleting','deleted')),
  gc_after timestamptz NOT NULL DEFAULT clock_timestamp(),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE(archive_id,file_id,ordinal),
  UNIQUE(backend,object_key),
  FOREIGN KEY(tenant_id,archive_id,file_id)
    REFERENCES public.result_archive_files(tenant_id,archive_id,file_id)
);
CREATE INDEX result_archive_chunks_gc ON public.result_archive_chunks(gc_after,id);
CREATE TABLE public.result_read_leases (
  id text PRIMARY KEY,
  tenant_id text NOT NULL,
  batch_id text NOT NULL,
  file_id text NOT NULL,
  mode text NOT NULL CHECK(mode IN ('db','archive')),
  archive_id text,
  expires_at timestamptz NOT NULL,
  FOREIGN KEY(tenant_id,batch_id) REFERENCES public.batches(tenant_id,id),
  FOREIGN KEY(tenant_id,file_id) REFERENCES public.files(tenant_id,id),
  FOREIGN KEY(tenant_id,archive_id) REFERENCES public.result_archive_runs(tenant_id,id),
  CHECK((mode='db' AND archive_id IS NULL) OR (mode='archive' AND archive_id IS NOT NULL))
);
CREATE INDEX result_read_db_leases ON public.result_read_leases(tenant_id,batch_id,expires_at) WHERE mode='db';
CREATE INDEX result_read_archive_leases ON public.result_read_leases(archive_id,expires_at) WHERE mode='archive';
CREATE INDEX result_read_leases_expiry ON public.result_read_leases(expires_at,id);
-- Retired generation/chunk metadata remains as a fence for late SDK writes and
-- delayed deletes. Ready archives have no automatic expiry in this migration.
