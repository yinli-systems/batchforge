CREATE TABLE IF NOT EXISTS tenants (
  id text PRIMARY KEY,
  name text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS api_keys (
  key_hash text PRIMARY KEY,
  tenant_id text NOT NULL REFERENCES tenants(id),
  revoked boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS workers (
  id text PRIMARY KEY,
  name text NOT NULL,
  model text NOT NULL,
  concurrency integer NOT NULL CHECK (concurrency BETWEEN 1 AND 4096),
  metadata jsonb NOT NULL DEFAULT '{}',
  last_seen_at timestamptz NOT NULL DEFAULT now(),
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS files (
  id text PRIMARY KEY,
  tenant_id text NOT NULL REFERENCES tenants(id),
  filename text NOT NULL,
  kind text NOT NULL CHECK (kind IN ('input','output','error')),
  content bytea,
  bytes bigint NOT NULL DEFAULT 0,
  batch_id text,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS batches (
  id text PRIMARY KEY,
  tenant_id text NOT NULL REFERENCES tenants(id),
  input_file_id text NOT NULL REFERENCES files(id),
  model text NOT NULL,
  endpoint text NOT NULL DEFAULT '/v1/chat/completions',
  completion_window text NOT NULL DEFAULT '24h',
  status text NOT NULL CHECK (status IN ('in_progress','completed','failed','cancelled')),
  metadata jsonb NOT NULL DEFAULT '{}',
  total_count integer NOT NULL CHECK (total_count > 0),
  pending_count integer NOT NULL DEFAULT 0 CHECK (pending_count >= 0),
  running_count integer NOT NULL DEFAULT 0 CHECK (running_count >= 0),
  completed_count integer NOT NULL DEFAULT 0 CHECK (completed_count >= 0),
  failed_count integer NOT NULL DEFAULT 0 CHECK (failed_count >= 0),
  cancelled_count integer NOT NULL DEFAULT 0 CHECK (cancelled_count >= 0),
  prompt_tokens bigint NOT NULL DEFAULT 0 CHECK (prompt_tokens >= 0),
  completion_tokens bigint NOT NULL DEFAULT 0 CHECK (completion_tokens >= 0),
  output_file_id text REFERENCES files(id),
  error_file_id text REFERENCES files(id),
  created_at timestamptz NOT NULL DEFAULT now(),
  in_progress_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,
  failed_at timestamptz,
  cancelled_at timestamptz,
  expires_at timestamptz NOT NULL DEFAULT now() + interval '24 hours',
  CHECK (total_count = pending_count + running_count + completed_count + failed_count + cancelled_count)
);
CREATE INDEX IF NOT EXISTS batches_tenant_created ON batches(tenant_id, created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS batches_expiry ON batches(expires_at) WHERE status = 'in_progress';
CREATE TABLE IF NOT EXISTS tasks (
  id text PRIMARY KEY,
  tenant_id text NOT NULL REFERENCES tenants(id),
  batch_id text NOT NULL REFERENCES batches(id),
  custom_id text NOT NULL,
  model text NOT NULL,
  body jsonb NOT NULL,
  status text NOT NULL CHECK (status IN ('pending','running','completed','failed','cancelled')),
  attempts integer NOT NULL DEFAULT 0,
  max_attempts integer NOT NULL CHECK (max_attempts BETWEEN 1 AND 20),
  ready_at timestamptz NOT NULL DEFAULT now(),
  lease_expires_at timestamptz,
  attempt_token text,
  worker_id text REFERENCES workers(id),
  response jsonb,
  error jsonb,
  prompt_tokens bigint NOT NULL DEFAULT 0,
  completion_tokens bigint NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz,
  UNIQUE(batch_id, custom_id)
);
CREATE INDEX IF NOT EXISTS tasks_claim ON tasks(model, ready_at, id) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS tasks_batch_claim ON tasks(batch_id, ready_at, id) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS tasks_expired ON tasks(lease_expires_at, id) WHERE status = 'running';
CREATE INDEX IF NOT EXISTS tasks_batch_status ON tasks(batch_id, status, id);
CREATE INDEX IF NOT EXISTS tasks_worker_running ON tasks(worker_id, lease_expires_at) WHERE status = 'running';
CREATE TABLE IF NOT EXISTS attempts (
  token text PRIMARY KEY,
  task_id text NOT NULL REFERENCES tasks(id),
  batch_id text NOT NULL REFERENCES batches(id),
  tenant_id text NOT NULL REFERENCES tenants(id),
  worker_id text NOT NULL REFERENCES workers(id),
  number integer NOT NULL,
  status text NOT NULL CHECK (status IN ('running','completed','retrying','failed','expired','cancelled')),
  error jsonb,
  prompt_tokens bigint NOT NULL DEFAULT 0,
  completion_tokens bigint NOT NULL DEFAULT 0,
  started_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz,
  UNIQUE(task_id, number)
);
CREATE INDEX IF NOT EXISTS attempts_tenant_finished ON attempts(tenant_id, finished_at) WHERE finished_at IS NOT NULL;
CREATE TABLE IF NOT EXISTS idempotency (
  tenant_id text NOT NULL REFERENCES tenants(id),
  scope text NOT NULL,
  key text NOT NULL,
  body_hash text NOT NULL,
  response jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(tenant_id, scope, key)
);
-- One durable scheduling row per tenant/model. Lock this before a batch row.
CREATE TABLE IF NOT EXISTS tenant_queues (
  tenant_id text NOT NULL REFERENCES tenants(id),
  model text NOT NULL,
  pending_count bigint NOT NULL DEFAULT 0 CHECK(pending_count >= 0),
  next_ready_at timestamptz NOT NULL DEFAULT now(),
  last_dispatched_at timestamptz NOT NULL DEFAULT '1970-01-01 00:00:00+00',
  PRIMARY KEY(tenant_id,model)
);
CREATE INDEX IF NOT EXISTS tenant_queues_fair ON tenant_queues(model,last_dispatched_at,tenant_id) WHERE pending_count > 0;
CREATE INDEX IF NOT EXISTS tasks_tenant_ready ON tasks(tenant_id,model,ready_at,id) WHERE status='pending';

ALTER TABLE tenant_queues ALTER COLUMN last_dispatched_at SET DEFAULT '1970-01-01 00:00:00+00';
UPDATE tenant_queues SET last_dispatched_at='1970-01-01 00:00:00+00' WHERE last_dispatched_at='-infinity';
