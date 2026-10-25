-- Independent Agent schema v1. The serving BatchForge schema11 ledger is not
-- advanced, rewritten or relaxed by this additive migration.
CREATE SCHEMA bf_agents;
REVOKE ALL ON SCHEMA bf_agents FROM PUBLIC;
CREATE TABLE bf_agents.migrations(version integer PRIMARY KEY,checksum text NOT NULL,applied_at timestamptz NOT NULL DEFAULT clock_timestamp());
CREATE TABLE bf_agents.limits(
 tenant_id text PRIMARY KEY REFERENCES public.tenants(id),
 max_active_runs integer NOT NULL DEFAULT 32 CHECK(max_active_runs BETWEEN 1 AND 1024),
 max_retained_runs integer NOT NULL DEFAULT 1000 CHECK(max_retained_runs BETWEEN 1 AND 1000000),
 max_memory_bytes bigint NOT NULL DEFAULT 16777216 CHECK(max_memory_bytes BETWEEN 1024 AND 1073741824)
);
CREATE TABLE bf_agents.usage(
 tenant_id text PRIMARY KEY REFERENCES public.tenants(id),
 active_runs integer NOT NULL DEFAULT 0 CHECK(active_runs>=0),
 retained_runs bigint NOT NULL DEFAULT 0 CHECK(retained_runs>=0),
 memory_bytes bigint NOT NULL DEFAULT 0 CHECK(memory_bytes>=0)
);
INSERT INTO bf_agents.limits(tenant_id) SELECT id FROM public.tenants;
INSERT INTO bf_agents.usage(tenant_id) SELECT id FROM public.tenants;
CREATE TABLE bf_agents.runs(
 id text PRIMARY KEY,tenant_id text NOT NULL REFERENCES public.tenants(id),
 request_key text NOT NULL CHECK(length(request_key) BETWEEN 1 AND 128),
 request_hash text NOT NULL CHECK(request_hash ~ '^[0-9a-f]{64}$'),
 goal text NOT NULL CHECK(octet_length(goal) BETWEEN 1 AND 32768),
 model text NOT NULL,policy jsonb NOT NULL CHECK(octet_length(policy::text)<=8192),
 status text NOT NULL DEFAULT 'queued' CHECK(status IN ('queued','running','waiting_model','waiting_approval','completed','failed','cancelled')),
 state jsonb NOT NULL DEFAULT '{}' CHECK(octet_length(state::text)<=131072),
 result jsonb CHECK(octet_length(result::text)<=65536),
 max_model_calls integer NOT NULL CHECK(max_model_calls BETWEEN 1 AND 32),
 max_tool_calls integer NOT NULL CHECK(max_tool_calls BETWEEN 0 AND 64),
 max_generated_tokens integer NOT NULL CHECK(max_generated_tokens BETWEEN 64 AND 32768),
 model_calls integer NOT NULL DEFAULT 0 CHECK(model_calls BETWEEN 0 AND max_model_calls),
 tool_calls integer NOT NULL DEFAULT 0 CHECK(tool_calls BETWEEN 0 AND max_tool_calls),
 generated_tokens integer NOT NULL DEFAULT 0 CHECK(generated_tokens BETWEEN 0 AND max_generated_tokens),
 revision bigint NOT NULL DEFAULT 0 CHECK(revision>=0),
 event_hash text NOT NULL CHECK(event_hash ~ '^[0-9a-f]{64}$'),
 lease_epoch bigint NOT NULL DEFAULT 0 CHECK(lease_epoch>=0),
 lease_owner text,lease_token text,lease_expires_at timestamptz,
 ready_at timestamptz NOT NULL DEFAULT clock_timestamp(),deadline_at timestamptz NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(tenant_id,id),UNIQUE(tenant_id,request_key),
 CHECK((lease_token IS NULL AND lease_owner IS NULL AND lease_expires_at IS NULL) OR
       (lease_token IS NOT NULL AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL))
);
CREATE INDEX agent_runs_claim ON bf_agents.runs(ready_at,created_at,id)
 WHERE status IN ('queued','running','waiting_model');
CREATE INDEX agent_runs_tenant ON bf_agents.runs(tenant_id,created_at DESC,id DESC);
CREATE TABLE bf_agents.events(
 tenant_id text NOT NULL,run_id text NOT NULL,revision bigint NOT NULL,
 operation_key text NOT NULL CHECK(length(operation_key) BETWEEN 1 AND 128),
 request_hash text NOT NULL CHECK(request_hash ~ '^[0-9a-f]{64}$'),
 kind text NOT NULL CHECK(length(kind) BETWEEN 1 AND 64),
 payload jsonb NOT NULL CHECK(octet_length(payload::text)<=16384),
 previous_hash text NOT NULL CHECK(previous_hash ~ '^[0-9a-f]{64}$'),
 event_hash text NOT NULL CHECK(event_hash ~ '^[0-9a-f]{64}$'),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(tenant_id,run_id,revision),UNIQUE(tenant_id,run_id,operation_key),
 FOREIGN KEY(tenant_id,run_id) REFERENCES bf_agents.runs(tenant_id,id)
);
CREATE TABLE bf_agents.memories(
 tenant_id text NOT NULL REFERENCES public.tenants(id),
 namespace text NOT NULL CHECK(namespace ~ '^[a-zA-Z0-9_-]{1,64}$'),
 key text NOT NULL CHECK(length(key) BETWEEN 1 AND 128),
 text text NOT NULL CHECK(octet_length(text)<=32768),
 sha256 text NOT NULL CHECK(sha256 ~ '^[0-9a-f]{64}$'),
 version bigint NOT NULL CHECK(version>0),
 search tsvector GENERATED ALWAYS AS (to_tsvector('simple',text)) STORED,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(tenant_id,namespace,key)
);
CREATE INDEX agent_memories_search ON bf_agents.memories USING gin(search);

CREATE FUNCTION bf_agents.quota_guard() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,bf_agents,public AS $$
DECLARE lim bf_agents.limits; use_row bf_agents.usage; delta bigint; old_active boolean; new_active boolean;
BEGIN
 -- Same global order as BatchForge: tenant before feature usage before rows.
 PERFORM id FROM public.tenants WHERE id=NEW.tenant_id FOR NO KEY UPDATE;
 SELECT * INTO lim FROM bf_agents.limits WHERE tenant_id=NEW.tenant_id;
 SELECT * INTO use_row FROM bf_agents.usage WHERE tenant_id=NEW.tenant_id FOR UPDATE;
 IF lim.tenant_id IS NULL OR use_row.tenant_id IS NULL THEN
  RAISE EXCEPTION 'Agent tenant policy unavailable' USING ERRCODE='BA429';
 END IF;
 IF TG_TABLE_NAME='runs' THEN
  new_active:=NEW.status NOT IN ('completed','failed','cancelled');
  IF TG_OP='INSERT' THEN
   IF use_row.retained_runs>=lim.max_retained_runs OR (new_active AND use_row.active_runs>=lim.max_active_runs) THEN
    RAISE EXCEPTION 'Agent run quota reached' USING ERRCODE='BA429';
   END IF;
   UPDATE bf_agents.usage SET retained_runs=retained_runs+1,active_runs=active_runs+new_active::integer WHERE tenant_id=NEW.tenant_id;
  ELSE
   IF (NEW.tenant_id,NEW.id,NEW.request_key,NEW.request_hash,NEW.goal,NEW.model,NEW.policy,
       NEW.max_model_calls,NEW.max_tool_calls,NEW.max_generated_tokens,NEW.deadline_at,NEW.created_at)
      IS DISTINCT FROM
      (OLD.tenant_id,OLD.id,OLD.request_key,OLD.request_hash,OLD.goal,OLD.model,OLD.policy,
       OLD.max_model_calls,OLD.max_tool_calls,OLD.max_generated_tokens,OLD.deadline_at,OLD.created_at)
      OR NEW.model_calls<OLD.model_calls OR NEW.tool_calls<OLD.tool_calls
      OR NEW.generated_tokens<OLD.generated_tokens OR NEW.lease_epoch<OLD.lease_epoch
      OR NEW.revision<OLD.revision THEN
    RAISE EXCEPTION 'Immutable agent identity/policy or decreasing counter' USING ERRCODE='BA409';
   END IF;
   old_active:=OLD.status NOT IN ('completed','failed','cancelled');
   IF NOT old_active AND NEW IS DISTINCT FROM OLD THEN
    RAISE EXCEPTION 'Terminal agent run is immutable' USING ERRCODE='BA409';
   END IF;
   IF old_active<>new_active THEN
    IF new_active AND use_row.active_runs>=lim.max_active_runs THEN RAISE EXCEPTION 'Agent quota reached' USING ERRCODE='BA429';END IF;
    UPDATE bf_agents.usage SET active_runs=active_runs+new_active::integer-old_active::integer WHERE tenant_id=NEW.tenant_id;
   END IF;
  END IF;
 ELSE
  IF TG_OP='UPDATE' AND (NEW.tenant_id,NEW.namespace,NEW.key) IS DISTINCT FROM (OLD.tenant_id,OLD.namespace,OLD.key) THEN
   RAISE EXCEPTION 'Memory identity is immutable' USING ERRCODE='BA409';
  END IF;
  delta:=octet_length(NEW.text)-CASE WHEN TG_OP='UPDATE' THEN octet_length(OLD.text) ELSE 0 END;
  IF use_row.memory_bytes+delta>lim.max_memory_bytes THEN RAISE EXCEPTION 'Agent memory quota reached' USING ERRCODE='BA429';END IF;
  UPDATE bf_agents.usage SET memory_bytes=memory_bytes+delta WHERE tenant_id=NEW.tenant_id;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER agent_run_quota BEFORE INSERT OR UPDATE ON bf_agents.runs FOR EACH ROW EXECUTE FUNCTION bf_agents.quota_guard();
CREATE TRIGGER agent_memory_quota BEFORE INSERT OR UPDATE ON bf_agents.memories FOR EACH ROW EXECUTE FUNCTION bf_agents.quota_guard();
CREATE FUNCTION bf_agents.immutable_event() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'Agent events are append-only' USING ERRCODE='BA409';END $$;
CREATE TRIGGER agent_events_immutable BEFORE UPDATE OR DELETE ON bf_agents.events FOR EACH ROW EXECUTE FUNCTION bf_agents.immutable_event();
REVOKE ALL ON ALL TABLES IN SCHEMA bf_agents FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA bf_agents FROM PUBLIC;
