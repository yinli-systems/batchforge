-- Additive v11. No existing history, idempotency key, object, or policy is
-- deleted/reclassified. Retirement is owner-only and disabled until a tenant
-- policy explicitly selects a NEW created-after cohort and retention interval.
-- Fail immediately rather than queueing a DDL upgrade behind API work. The
-- owner must provide a short write-quiescent window; retry the whole migration
-- later if any gate is refused. No prior task rows are rewritten/backfilled.
LOCK TABLE public.tenants IN EXCLUSIVE MODE NOWAIT;
LOCK TABLE public.batches,public.idempotency IN ACCESS EXCLUSIVE MODE NOWAIT;
LOCK TABLE public.tasks,public.attempts IN SHARE ROW EXCLUSIVE MODE NOWAIT;
CREATE TABLE public.tenant_history_retirement_policy (
 tenant_id text PRIMARY KEY REFERENCES public.tenants(id),
 enabled boolean NOT NULL DEFAULT false,
 eligible_created_after timestamptz,
 min_age_seconds integer NOT NULL DEFAULT 86400 CHECK(min_age_seconds BETWEEN 60 AND 315360000),
 replay_floor bigint NOT NULL DEFAULT 0 CHECK(replay_floor BETWEEN 0 AND 9999999999),
 replay_ttl_seconds integer NOT NULL DEFAULT 86400 CHECK(replay_ttl_seconds BETWEEN 60 AND 315360000),
 CHECK(NOT enabled OR eligible_created_after IS NOT NULL)
);
CREATE TABLE public.retired_attempt_hourly (
 tenant_id text NOT NULL REFERENCES public.tenants(id),
 hour timestamptz NOT NULL,
 status text NOT NULL CHECK(status IN ('completed','retrying','failed','expired','cancelled')),
 attempts bigint NOT NULL CHECK(attempts>=0),
 prompt_tokens bigint NOT NULL CHECK(prompt_tokens>=0),
 completion_tokens bigint NOT NULL CHECK(completion_tokens>=0),
 PRIMARY KEY(tenant_id,hour,status)
);
ALTER TABLE public.batches
 ADD COLUMN history_retirement_started_at timestamptz,
 ADD COLUMN history_retired_at timestamptz,
 ADD COLUMN history_retired_tasks bigint NOT NULL DEFAULT 0 CHECK(history_retired_tasks>=0),
 ADD COLUMN history_retired_completed bigint NOT NULL DEFAULT 0 CHECK(history_retired_completed>=0),
 ADD COLUMN history_retired_failed bigint NOT NULL DEFAULT 0 CHECK(history_retired_failed>=0),
 ADD COLUMN history_retired_cancelled bigint NOT NULL DEFAULT 0 CHECK(history_retired_cancelled>=0),
 ADD COLUMN history_retired_prompt_tokens bigint NOT NULL DEFAULT 0 CHECK(history_retired_prompt_tokens>=0),
 ADD COLUMN history_retired_completion_tokens bigint NOT NULL DEFAULT 0 CHECK(history_retired_completion_tokens>=0),
 ADD CONSTRAINT history_retired_totals CHECK(
 history_retired_tasks=history_retired_completed+history_retired_failed+history_retired_cancelled
 AND history_retired_tasks<=total_count AND history_retired_completed<=completed_count
 AND history_retired_failed<=failed_count AND history_retired_cancelled<=cancelled_count
 AND history_retired_prompt_tokens<=prompt_tokens AND history_retired_completion_tokens<=completion_tokens);
-- Nullable, no backfill/table rewrite. Pre-v11 rows are never eligible for
-- automatic key deletion, including a legacy row which resembles this format.
ALTER TABLE public.idempotency ADD COLUMN replay_epoch bigint;
CREATE INDEX idempotency_retirable_epoch ON public.idempotency(tenant_id,replay_epoch)
 WHERE replay_epoch IS NOT NULL;
CREATE INDEX batches_history_retirement ON public.batches(tenant_id,created_at,id)
 WHERE status<>'in_progress' AND history_retired_at IS NULL;

CREATE FUNCTION public.batchforge_history_owner() RETURNS boolean
LANGUAGE sql STABLE SECURITY INVOKER SET search_path=pg_catalog,public AS $$
 SELECT current_user=pg_get_userbyid(relowner) FROM pg_class
 WHERE oid='public.tenant_history_retirement_policy'::regclass
$$;
CREATE FUNCTION public.batchforge_history_admin_guard() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog,public AS $$
BEGIN
 IF NOT public.batchforge_history_owner() THEN
  RAISE EXCEPTION 'History retirement is owner-managed' USING ERRCODE='BR403';
 END IF;
 IF TG_TABLE_NAME='tenant_history_retirement_policy' THEN
  IF TG_OP IN ('DELETE','TRUNCATE') THEN
   RAISE EXCEPTION 'Replay policy cannot be deleted or reset' USING ERRCODE='BR403';
  END IF;
  IF TG_OP='INSERT' THEN
   IF NEW.replay_floor<>0 THEN
    RAISE EXCEPTION 'Initial replay floor must be zero' USING ERRCODE='BR403';
   END IF;
  ELSIF TG_OP='UPDATE' THEN
   IF NEW.tenant_id IS DISTINCT FROM OLD.tenant_id OR NEW.replay_floor<OLD.replay_floor THEN
    RAISE EXCEPTION 'History policy identity and replay floor are immutable/monotonic' USING ERRCODE='BR409';
   END IF;
   IF NEW.replay_floor IS DISTINCT FROM OLD.replay_floor AND
      current_setting('batchforge.replay_floor_tenant',true) IS DISTINCT FROM OLD.tenant_id THEN
    RAISE EXCEPTION 'Advance replay floor through the checked operator function' USING ERRCODE='BR403';
   END IF;
  END IF;
 END IF;
 IF TG_OP='TRUNCATE' THEN RETURN NULL;
 ELSIF TG_OP='DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
END $$;
CREATE TRIGGER history_policy_owner BEFORE INSERT OR UPDATE OR DELETE ON public.tenant_history_retirement_policy
 FOR EACH ROW EXECUTE FUNCTION public.batchforge_history_admin_guard();
CREATE TRIGGER history_policy_owner_truncate BEFORE TRUNCATE ON public.tenant_history_retirement_policy
 FOR EACH STATEMENT EXECUTE FUNCTION public.batchforge_history_admin_guard();
CREATE TRIGGER history_rollup_owner BEFORE INSERT OR UPDATE OR DELETE ON public.retired_attempt_hourly
 FOR EACH ROW EXECUTE FUNCTION public.batchforge_history_admin_guard();
CREATE TRIGGER history_rollup_owner_truncate BEFORE TRUNCATE ON public.retired_attempt_hourly
 FOR EACH STATEMENT EXECUTE FUNCTION public.batchforge_history_admin_guard();

-- bf1 is a newly reserved namespace. Arbitrary legacy keys preserve their
-- unbounded replay contract and are NEVER deleted by this migration's tools.
CREATE FUNCTION public.batchforge_idempotency_epoch(p_key text) RETURNS bigint
LANGUAGE plpgsql IMMUTABLE SECURITY INVOKER SET search_path=pg_catalog,public AS $$
BEGIN
 IF p_key NOT LIKE 'bf1.%' THEN RETURN NULL; END IF;
 IF p_key !~ '^bf1\.[1-9][0-9]{9}\.[0-9a-f]{16,64}$' THEN
  RAISE EXCEPTION 'Invalid time-scoped idempotency key' USING ERRCODE='BH400';
 END IF;
 RETURN split_part(p_key,'.',2)::bigint;
END $$;
CREATE FUNCTION public.batchforge_idempotency_check(p_tenant text,p_scope text,p_key text) RETURNS void
LANGUAGE plpgsql STABLE SECURITY INVOKER SET search_path=pg_catalog,public AS $$
DECLARE epoch bigint; floor_value bigint;
BEGIN
 -- A pre-v11 existing full-PK row retains legacy infinite replay semantics,
 -- even if its arbitrary key happens to resemble the new namespace.
 IF EXISTS(SELECT 1 FROM public.idempotency WHERE tenant_id=p_tenant AND scope=p_scope
   AND key=p_key AND replay_epoch IS NULL) THEN RETURN; END IF;
 epoch=public.batchforge_idempotency_epoch(p_key);
 IF epoch IS NULL THEN RETURN; END IF;
 SELECT replay_floor INTO floor_value FROM public.tenant_history_retirement_policy WHERE tenant_id=p_tenant;
 IF epoch<coalesce(floor_value,0) THEN
  RAISE EXCEPTION 'Idempotency replay horizon expired' USING ERRCODE='BH410';
 END IF;
 IF epoch>floor(extract(epoch FROM statement_timestamp()))::bigint+60 THEN
  RAISE EXCEPTION 'Idempotency timestamp is in the future' USING ERRCODE='BH400';
 END IF;
END $$;
CREATE FUNCTION public.batchforge_idempotency_admit(p_tenant text,p_scope text,p_key text) RETURNS void
LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path=pg_catalog,public AS $$
BEGIN
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION 'History write fences require READ COMMITTED' USING ERRCODE='BR409';
 END IF;
 PERFORM id FROM public.tenants WHERE id=p_tenant FOR NO KEY UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'Tenant not found' USING ERRCODE='BF404'; END IF;
 PERFORM public.batchforge_idempotency_check(p_tenant,p_scope,p_key);
END $$;
CREATE FUNCTION public.batchforge_idempotency_time_guard() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog,public AS $$
BEGIN
 PERFORM public.batchforge_idempotency_admit(NEW.tenant_id,NEW.scope,NEW.key);
 IF EXISTS(SELECT 1 FROM public.idempotency WHERE tenant_id=NEW.tenant_id AND scope=NEW.scope
   AND key=NEW.key AND replay_epoch IS NULL) THEN NEW.replay_epoch=NULL;
 ELSE NEW.replay_epoch=public.batchforge_idempotency_epoch(NEW.key); END IF;
 RETURN NEW;
END $$;
-- BEFORE INSERT protects old API replicas as well as the new Store path.
CREATE TRIGGER history_idempotency_time BEFORE INSERT ON public.idempotency
 FOR EACH ROW EXECUTE FUNCTION public.batchforge_idempotency_time_guard();

CREATE FUNCTION public.batchforge_history_batch_guard() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog,public AS $$
BEGIN
 IF (NEW.history_retirement_started_at,NEW.history_retired_at,NEW.history_retired_tasks,
 NEW.history_retired_completed,NEW.history_retired_failed,NEW.history_retired_cancelled,
 NEW.history_retired_prompt_tokens,NEW.history_retired_completion_tokens)
 IS DISTINCT FROM (OLD.history_retirement_started_at,OLD.history_retired_at,OLD.history_retired_tasks,
 OLD.history_retired_completed,OLD.history_retired_failed,OLD.history_retired_cancelled,
 OLD.history_retired_prompt_tokens,OLD.history_retired_completion_tokens)
 AND current_user<>(SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid='public.tenant_history_retirement_policy'::regclass) THEN
  RAISE EXCEPTION 'History retirement is owner-managed' USING ERRCODE='BR403';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER history_batch_state BEFORE UPDATE ON public.batches
 FOR EACH ROW EXECUTE FUNCTION public.batchforge_history_batch_guard();
CREATE FUNCTION public.batchforge_history_retry_guard() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog,public AS $$
BEGIN
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION 'History write fences require READ COMMITTED' USING ERRCODE='BR409';
 END IF;
 -- Old API replicas must also reject retrying a partly retired source. This
 -- tenant umbrella orders the source check with retirement/admission itself.
 PERFORM id FROM public.tenants WHERE id=NEW.tenant_id FOR NO KEY UPDATE;
 IF (NEW.history_retirement_started_at IS NOT NULL OR NEW.history_retired_at IS NOT NULL
   OR NEW.history_retired_tasks<>0 OR NEW.history_retired_completed<>0 OR NEW.history_retired_failed<>0
   OR NEW.history_retired_cancelled<>0 OR NEW.history_retired_prompt_tokens<>0 OR NEW.history_retired_completion_tokens<>0)
   AND current_user<>(SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid='public.tenant_history_retirement_policy'::regclass) THEN
  RAISE EXCEPTION 'History retirement is owner-managed' USING ERRCODE='BR403';
 END IF;
 IF NEW.metadata ? 'retry_of' AND EXISTS(SELECT 1 FROM public.batches
   WHERE tenant_id=NEW.tenant_id AND id=NEW.metadata->>'retry_of'
   AND history_retirement_started_at IS NOT NULL) THEN
  RAISE EXCEPTION 'Source history is retired' USING ERRCODE='BH411';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER history_batch_retry BEFORE INSERT ON public.batches
 FOR EACH ROW EXECUTE FUNCTION public.batchforge_history_retry_guard();

CREATE FUNCTION public.batchforge_history_task_guard() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog,public AS $$
BEGIN
 IF current_setting('transaction_isolation')<>'read committed' THEN
  RAISE EXCEPTION 'History write fences require READ COMMITTED' USING ERRCODE='BR409';
 END IF;
 PERFORM id FROM public.tenants WHERE id=NEW.tenant_id FOR NO KEY UPDATE;
 IF EXISTS(SELECT 1 FROM public.batches WHERE id=NEW.batch_id AND tenant_id=NEW.tenant_id
   AND history_retirement_started_at IS NOT NULL) THEN
  RAISE EXCEPTION 'Source history is retired' USING ERRCODE='BH411';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER history_task_insert BEFORE INSERT ON public.tasks
 FOR EACH ROW EXECUTE FUNCTION public.batchforge_history_task_guard();

-- Batch aggregate usage remains authoritative and untouched. The time series
-- is an exact partition: retained attempts + atomically retired attempt buckets.
CREATE VIEW public.batchforge_attempt_hourly AS
 SELECT tenant_id,timestamp,sum(completed)::bigint AS completed,
 sum(failed)::bigint AS failed,sum(tokens)::bigint AS tokens FROM (
 SELECT tenant_id,(date_trunc('hour',finished_at AT TIME ZONE 'UTC') AT TIME ZONE 'UTC') AS timestamp,
 count(*) FILTER(WHERE status='completed') AS completed,
 count(*) FILTER(WHERE status='failed') AS failed,
 coalesce(sum(prompt_tokens+completion_tokens) FILTER(WHERE status='completed'),0)::bigint AS tokens
 FROM public.attempts WHERE finished_at IS NOT NULL GROUP BY tenant_id,2
 UNION ALL
 SELECT tenant_id,hour AS timestamp,
 CASE WHEN status='completed' THEN attempts ELSE 0 END,
 CASE WHEN status='failed' THEN attempts ELSE 0 END,
 CASE WHEN status='completed' THEN prompt_tokens+completion_tokens ELSE 0 END
 FROM public.retired_attempt_hourly
 ) s GROUP BY tenant_id,timestamp;

CREATE FUNCTION public.batchforge_retire_history_page(p_tenant text,p_batch text,p_limit integer,
 p_apply boolean DEFAULT false,p_manifest text DEFAULT NULL) RETURNS jsonb
LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog,public AS $$
DECLARE policy public.tenant_history_retirement_policy; b public.batches; r public.result_archive_runs;
 ids text[]; n bigint; a bigint; remaining bigint; ended timestamptz; totals record; page record;
BEGIN
 IF NOT public.batchforge_history_owner() THEN
  RAISE EXCEPTION 'History retirement requires the owner' USING ERRCODE='BR403';
 END IF;
 IF p_apply IS NULL OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 1000 THEN
  RAISE EXCEPTION 'Invalid retirement page bound' USING ERRCODE='BH400';
 END IF;
 PERFORM id FROM public.tenants WHERE id=p_tenant FOR NO KEY UPDATE SKIP LOCKED;
 IF NOT FOUND THEN RETURN jsonb_build_object('eligible',false,'reason','tenant_busy_or_missing'); END IF;
 SELECT * INTO policy FROM public.tenant_history_retirement_policy WHERE tenant_id=p_tenant;
 IF NOT FOUND OR NOT policy.enabled OR policy.eligible_created_after IS NULL THEN
  RETURN jsonb_build_object('eligible',false,'reason','policy_disabled');
 END IF;
 SELECT * INTO b FROM public.batches WHERE id=p_batch AND tenant_id=p_tenant;
 IF NOT FOUND THEN RETURN jsonb_build_object('eligible',false,'reason','batch_missing'); END IF;
 -- Matches worker/queue/batch order after the tenant admission umbrella. A
 -- manually imported terminal batch may have no queue; no row is then needed.
 PERFORM tenant_id FROM public.tenant_queues WHERE tenant_id=p_tenant AND model=b.model FOR UPDATE SKIP LOCKED;
 IF NOT FOUND AND EXISTS(SELECT 1 FROM public.tenant_queues WHERE tenant_id=p_tenant AND model=b.model) THEN
  RETURN jsonb_build_object('eligible',false,'reason','queue_busy');
 END IF;
 SELECT * INTO b FROM public.batches WHERE id=p_batch AND tenant_id=p_tenant FOR UPDATE SKIP LOCKED;
 IF NOT FOUND THEN RETURN jsonb_build_object('eligible',false,'reason','batch_busy'); END IF;
 IF b.history_retired_at IS NOT NULL THEN
  RETURN jsonb_build_object('eligible',false,'reason','already_retired','retired_tasks',b.history_retired_tasks);
 END IF;
 ended=greatest(b.completed_at,b.failed_at,b.cancelled_at);
 IF b.status='in_progress' OR b.pending_count<>0 OR b.running_count<>0 OR ended IS NULL
    OR b.created_at<policy.eligible_created_after
    OR ended>clock_timestamp()-make_interval(secs=>policy.min_age_seconds) THEN
  RETURN jsonb_build_object('eligible',false,'reason','cohort_or_age_or_not_terminal');
 END IF;
 SELECT * INTO r FROM public.result_archive_runs WHERE tenant_id=p_tenant AND batch_id=p_batch
   AND state='ready' AND purged_at IS NOT NULL FOR UPDATE SKIP LOCKED;
 IF NOT FOUND THEN RETURN jsonb_build_object('eligible',false,'reason','verified_archive_required'); END IF;
 IF EXISTS(SELECT 1 FROM public.result_read_leases WHERE tenant_id=p_tenant AND batch_id=p_batch
    AND expires_at>clock_timestamp()) THEN
  RETURN jsonb_build_object('eligible',false,'reason','active_reader');
 END IF;
 IF EXISTS(SELECT 1 FROM public.result_archive_runs WHERE tenant_id=p_tenant AND batch_id=p_batch
    AND state='building') THEN
  RETURN jsonb_build_object('eligible',false,'reason','active_writer');
 END IF;
 IF (SELECT coalesce(sum(expected_count),0) FROM public.result_archive_files WHERE archive_id=r.id)<>b.total_count
 OR EXISTS(SELECT 1 FROM public.result_archive_files WHERE archive_id=r.id
    AND (line_count<>expected_count OR sha256 IS NULL))
 OR EXISTS(SELECT 1 FROM public.result_archive_chunks WHERE archive_id=r.id AND state<>'ready')
 OR EXISTS(SELECT 1 FROM public.tasks WHERE tenant_id=p_tenant AND batch_id=p_batch
    AND (status IN ('pending','running') OR lease_expires_at>clock_timestamp()
      OR (status='completed' AND NOT retention_payload_cleared)))
 OR EXISTS(SELECT 1 FROM public.attempts WHERE tenant_id=p_tenant AND batch_id=p_batch
    AND (status='running' OR finished_at IS NULL)) THEN
  RETURN jsonb_build_object('eligible',false,'reason','archive_or_task_invariant');
 END IF;
 -- Never destroy the evidence of a missing/duplicate accepted completion or
 -- mismatched accepted usage. Batch counters alone cannot prove this invariant.
 IF EXISTS(WITH accepted AS (
   SELECT task_id,count(*) AS n,sum(prompt_tokens) AS prompt,sum(completion_tokens) AS completion
   FROM public.attempts WHERE tenant_id=p_tenant AND batch_id=p_batch AND status='completed' GROUP BY task_id)
   SELECT 1 FROM public.tasks t LEFT JOIN accepted a ON a.task_id=t.id
   WHERE t.tenant_id=p_tenant AND t.batch_id=p_batch AND
   ((t.status='completed' AND (coalesce(a.n,0)<>1 OR t.prompt_tokens IS DISTINCT FROM a.prompt
       OR t.completion_tokens IS DISTINCT FROM a.completion))
    OR (t.status<>'completed' AND coalesce(a.n,0)<>0))) THEN
  RAISE EXCEPTION 'Accepted attempt identity or usage differs from retained tasks' USING ERRCODE='BR409';
 END IF;
 SELECT count(*) AS n,count(*) FILTER(WHERE status='completed') AS completed,
 count(*) FILTER(WHERE status='failed') AS failed,count(*) FILTER(WHERE status='cancelled') AS cancelled,
 coalesce(sum(prompt_tokens),0) AS prompt,coalesce(sum(completion_tokens),0) AS completion INTO totals
 FROM public.tasks WHERE tenant_id=p_tenant AND batch_id=p_batch;
 remaining=totals.n;
 IF remaining+b.history_retired_tasks<>b.total_count
 OR totals.completed+b.history_retired_completed<>b.completed_count
 OR totals.failed+b.history_retired_failed<>b.failed_count
 OR totals.cancelled+b.history_retired_cancelled<>b.cancelled_count
 OR totals.prompt+b.history_retired_prompt_tokens<>b.prompt_tokens
 OR totals.completion+b.history_retired_completion_tokens<>b.completion_tokens THEN
  RAISE EXCEPTION 'History count differs from preserved batch aggregate' USING ERRCODE='BR409';
 END IF;
 SELECT array_agg(id ORDER BY id) INTO ids FROM (SELECT id FROM public.tasks
   WHERE tenant_id=p_tenant AND batch_id=p_batch ORDER BY id LIMIT p_limit FOR UPDATE SKIP LOCKED) selected;
 n=coalesce(cardinality(ids),0);
 IF n=0 AND remaining>0 THEN RETURN jsonb_build_object('eligible',false,'reason','tasks_busy'); END IF;
 SELECT count(*) INTO a FROM public.attempts WHERE tenant_id=p_tenant AND task_id=ANY(coalesce(ids,ARRAY[]::text[]));
 IF NOT p_apply THEN
  RETURN jsonb_build_object('eligible',true,'apply',false,'tasks',n,'attempts',a,
    'remaining_tasks',remaining,'archive_id',r.id,'manifest_sha256',r.manifest_sha256);
 END IF;
 IF p_manifest IS DISTINCT FROM r.manifest_sha256 THEN
  RAISE EXCEPTION 'Fresh verified archive manifest required' USING ERRCODE='BR409';
 END IF;
 SELECT count(*) FILTER(WHERE status='completed') AS completed,
 count(*) FILTER(WHERE status='failed') AS failed,count(*) FILTER(WHERE status='cancelled') AS cancelled,
 coalesce(sum(prompt_tokens),0) AS prompt,coalesce(sum(completion_tokens),0) AS completion INTO page
 FROM public.tasks WHERE tenant_id=p_tenant AND id=ANY(coalesce(ids,ARRAY[]::text[]));
 PERFORM public.batchforge_retention_authorize_delete(p_tenant);
 INSERT INTO public.retired_attempt_hourly(tenant_id,hour,status,attempts,prompt_tokens,completion_tokens)
 SELECT tenant_id,(date_trunc('hour',finished_at AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'),status,count(*),sum(prompt_tokens),sum(completion_tokens)
 FROM public.attempts WHERE tenant_id=p_tenant AND task_id=ANY(coalesce(ids,ARRAY[]::text[])) GROUP BY 1,2,3
 ON CONFLICT(tenant_id,hour,status) DO UPDATE SET
 attempts=retired_attempt_hourly.attempts+EXCLUDED.attempts,
 prompt_tokens=retired_attempt_hourly.prompt_tokens+EXCLUDED.prompt_tokens,
 completion_tokens=retired_attempt_hourly.completion_tokens+EXCLUDED.completion_tokens;
 DELETE FROM public.attempts WHERE tenant_id=p_tenant AND task_id=ANY(coalesce(ids,ARRAY[]::text[]));
 DELETE FROM public.tasks WHERE tenant_id=p_tenant AND id=ANY(coalesce(ids,ARRAY[]::text[]));
 UPDATE public.batches SET history_retirement_started_at=coalesce(history_retirement_started_at,clock_timestamp()),
 history_retired_tasks=history_retired_tasks+n,
 history_retired_completed=history_retired_completed+page.completed,
 history_retired_failed=history_retired_failed+page.failed,
 history_retired_cancelled=history_retired_cancelled+page.cancelled,
 history_retired_prompt_tokens=history_retired_prompt_tokens+page.prompt,
 history_retired_completion_tokens=history_retired_completion_tokens+page.completion,
 history_retired_at=CASE WHEN remaining=n THEN clock_timestamp() ELSE NULL END
 WHERE tenant_id=p_tenant AND id=p_batch;
 RETURN jsonb_build_object('eligible',true,'apply',true,'tasks',n,'attempts',a,
  'remaining_tasks',remaining-n,'archive_id',r.id,'manifest_sha256',r.manifest_sha256);
END $$;

CREATE FUNCTION public.batchforge_retire_idempotency_page(p_tenant text,p_floor bigint,p_limit integer,
 p_apply boolean DEFAULT false) RETURNS jsonb
LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog,public AS $$
DECLARE policy public.tenant_history_retirement_policy; removed bigint; candidate_count bigint;
BEGIN
 IF NOT public.batchforge_history_owner() THEN
  RAISE EXCEPTION 'Idempotency retirement requires the owner' USING ERRCODE='BR403';
 END IF;
 IF p_apply IS NULL OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 1000 OR p_floor IS NULL OR p_floor<0 OR p_floor>9999999999 THEN
  RAISE EXCEPTION 'Invalid bounded replay retirement' USING ERRCODE='BH400';
 END IF;
 PERFORM id FROM public.tenants WHERE id=p_tenant FOR NO KEY UPDATE SKIP LOCKED;
 IF NOT FOUND THEN RETURN jsonb_build_object('eligible',false,'reason','tenant_busy_or_missing'); END IF;
 SELECT * INTO policy FROM public.tenant_history_retirement_policy WHERE tenant_id=p_tenant;
 IF NOT FOUND OR NOT policy.enabled OR policy.eligible_created_after IS NULL THEN
  RETURN jsonb_build_object('eligible',false,'reason','policy_disabled');
 END IF;
 IF p_floor<policy.replay_floor OR p_floor>floor(extract(epoch FROM clock_timestamp()))-policy.replay_ttl_seconds THEN
  RAISE EXCEPTION 'Replay floor must be monotonic and preserve the configured TTL' USING ERRCODE='BR409';
 END IF;
 -- NULL responses may represent uncertain commits; never cross them. A key
 -- associated with a live/missing batch is similarly ineligible, not guessed.
 IF EXISTS(SELECT 1 FROM public.idempotency i LEFT JOIN public.batches b
    ON b.tenant_id=i.tenant_id AND b.id=i.response->>'id'
    WHERE i.tenant_id=p_tenant AND i.replay_epoch<p_floor
    AND (i.response IS NULL OR b.id IS NULL OR b.status='in_progress')) THEN
  RETURN jsonb_build_object('eligible',false,'reason','unresolved_or_active_key');
 END IF;
 SELECT count(*) INTO candidate_count FROM (SELECT key FROM public.idempotency WHERE tenant_id=p_tenant
   AND replay_epoch<p_floor AND created_at>=policy.eligible_created_after ORDER BY replay_epoch,scope,key LIMIT p_limit) candidates;
 IF NOT p_apply THEN RETURN jsonb_build_object('eligible',true,'apply',false,'keys',candidate_count,'replay_floor',p_floor); END IF;
 PERFORM set_config('batchforge.replay_floor_tenant',p_tenant,true);
 UPDATE public.tenant_history_retirement_policy SET replay_floor=p_floor WHERE tenant_id=p_tenant;
 WITH candidates AS (SELECT tenant_id,scope,key FROM public.idempotency WHERE tenant_id=p_tenant
   AND replay_epoch<p_floor AND created_at>=policy.eligible_created_after ORDER BY replay_epoch,scope,key
   LIMIT p_limit FOR UPDATE SKIP LOCKED)
 DELETE FROM public.idempotency i USING candidates c
 WHERE (i.tenant_id,i.scope,i.key)=(c.tenant_id,c.scope,c.key);
 GET DIAGNOSTICS removed=ROW_COUNT;
 RETURN jsonb_build_object('eligible',true,'apply',true,'keys',removed,'replay_floor',p_floor);
END $$;

REVOKE ALL ON public.tenant_history_retirement_policy,public.retired_attempt_hourly,
 public.batchforge_attempt_hourly FROM PUBLIC;
REVOKE ALL ON FUNCTION public.batchforge_history_owner(),public.batchforge_history_admin_guard(),
 public.batchforge_idempotency_epoch(text),public.batchforge_idempotency_check(text,text,text),
 public.batchforge_idempotency_admit(text,text,text),public.batchforge_idempotency_time_guard(),
 public.batchforge_history_batch_guard(),public.batchforge_history_retry_guard(),public.batchforge_history_task_guard(),
 public.batchforge_retire_history_page(text,text,integer,boolean,text),
 public.batchforge_retire_idempotency_page(text,bigint,integer,boolean) FROM PUBLIC;
