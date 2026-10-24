-- Additive v6. Predictions are explicitly operator-calibrated estimates, not
-- invoiced usage or KV admission. Keep the worker -> queue -> batch -> task lock
-- order and the existing attempt fencing/completion transaction unchanged.
ALTER TABLE public.workers
    ADD COLUMN scheduler_profile_id text,
    ADD COLUMN runtime_fingerprint text,
    ADD COLUMN prefill_tokens_per_second double precision,
    ADD COLUMN decode_tokens_per_second double precision,
    ADD COLUMN hourly_cost_usd double precision,
    ADD CONSTRAINT workers_scheduler_profile CHECK (
      (scheduler_profile_id IS NULL AND prefill_tokens_per_second IS NULL
       AND decode_tokens_per_second IS NULL AND hourly_cost_usd IS NULL)
      OR (scheduler_profile_id IS NOT NULL AND length(scheduler_profile_id) BETWEEN 1 AND 256
       AND runtime_fingerprint IS NOT NULL AND length(runtime_fingerprint) BETWEEN 1 AND 256
       AND prefill_tokens_per_second BETWEEN 1e-9 AND 1e12
       AND decode_tokens_per_second BETWEEN 1e-9 AND 1e12
       AND hourly_cost_usd BETWEEN 0 AND 1e9
       AND prefill_tokens_per_second IS NOT NULL AND decode_tokens_per_second IS NOT NULL
       AND hourly_cost_usd IS NOT NULL));
ALTER TABLE public.tasks
    ADD COLUMN estimated_input_tokens bigint NOT NULL DEFAULT 0 CHECK (estimated_input_tokens BETWEEN 0 AND 10000000),
    ADD COLUMN estimated_output_tokens bigint NOT NULL DEFAULT 0 CHECK (estimated_output_tokens BETWEEN 0 AND 10000000),
    ADD COLUMN estimate_source text NOT NULL DEFAULT 'legacy_unknown',
    ADD COLUMN runtime_fingerprint text;
ALTER TABLE public.attempts ADD COLUMN scheduling_decision jsonb NOT NULL DEFAULT '{}';
ALTER TABLE public.batches
    ADD COLUMN runtime_fingerprint text,
    ADD COLUMN runtime_compatibility_scope text NOT NULL DEFAULT 'first_dispatch'
      CHECK (runtime_compatibility_scope IN ('first_dispatch','legacy_unverified'));
-- Claims predating v6 did not record a runtime identity. Never retroactively
-- certify the numerical compatibility of their existing accepted/in-flight work.
UPDATE public.batches b SET runtime_compatibility_scope='legacy_unverified'
  WHERE EXISTS (SELECT 1 FROM public.tasks t WHERE t.batch_id=b.id AND t.attempts>0);
CREATE INDEX workers_scheduler_ready ON public.workers(model,last_seen_at)
  WHERE scheduler_profile_id IS NOT NULL;

-- Existing queued and in-flight work must remain schedulable after a rollout or
-- lease recovery. JSONB spacing differs from the Python estimator: identify this
-- deliberately approximate migration estimator separately in durable evidence.
UPDATE public.tasks SET
    estimated_input_tokens=least(10000000,greatest(1,
      (octet_length(convert_to(coalesce(body->'messages','[]'::jsonb)::text,'UTF8'))::bigint+3)/4)),
    estimated_output_tokens=CASE
      WHEN body->>'max_tokens' ~ '^[0-9]{1,7}$' THEN greatest(1,(body->>'max_tokens')::bigint)
      WHEN body->>'max_completion_tokens' ~ '^[0-9]{1,7}$' THEN greatest(1,(body->>'max_completion_tokens')::bigint)
      ELSE 256 END,
    estimate_source='postgres_jsonb_utf8_bytes_div4_and_output_cap_v1'
  WHERE status IN ('pending','running');

-- Old API replicas may keep inserting during an additive rolling migration.
-- Defaults of zero must not strand their newly queued work after policy enable.
CREATE FUNCTION public.batchforge_estimate_task_insert() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog,public AS $estimate$
BEGIN
  IF NEW.estimated_input_tokens=0 OR NEW.estimated_output_tokens=0 THEN
    NEW.estimated_input_tokens := CASE WHEN NEW.estimated_input_tokens>0 THEN NEW.estimated_input_tokens
      ELSE least(10000000,greatest(1,(octet_length(convert_to(coalesce(NEW.body->'messages','[]'::jsonb)::text,'UTF8'))::bigint+3)/4)) END;
    NEW.estimated_output_tokens := CASE WHEN NEW.estimated_output_tokens>0 THEN NEW.estimated_output_tokens
      WHEN NEW.body->>'max_tokens' ~ '^[0-9]{1,7}$' THEN greatest(1,(NEW.body->>'max_tokens')::bigint)
      WHEN NEW.body->>'max_completion_tokens' ~ '^[0-9]{1,7}$' THEN greatest(1,(NEW.body->>'max_completion_tokens')::bigint)
      ELSE 256 END;
    NEW.estimate_source := 'postgres_jsonb_utf8_bytes_div4_and_output_cap_v1';
  END IF;
  RETURN NEW;
END
$estimate$;
CREATE TRIGGER batchforge_estimate_task_insert BEFORE INSERT ON public.tasks
  FOR EACH ROW EXECUTE FUNCTION public.batchforge_estimate_task_insert();

-- Existing Python/SQL claim implementations already lock worker -> queue ->
-- batch -> task, but do not know v6 fingerprints. Fail closed on bound work even
-- during a rolling release or an accidental legacy-policy rollback. This guard
-- is deliberately not a replacement for the v6 eligibility/ranking query.
CREATE FUNCTION public.batchforge_guard_task_runtime() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog,public AS $guard$
DECLARE v_fingerprint text; v_batch_fingerprint text;
BEGIN
  IF OLD.runtime_fingerprint IS NOT NULL AND NEW.runtime_fingerprint IS DISTINCT FROM OLD.runtime_fingerprint THEN
    RAISE EXCEPTION USING ERRCODE='BF409', MESSAGE='runtime_fingerprint_immutable';
  END IF;
  IF NEW.status='running' AND (OLD.status IS DISTINCT FROM 'running'
    OR NEW.worker_id IS DISTINCT FROM OLD.worker_id OR NEW.attempt_token IS DISTINCT FROM OLD.attempt_token) THEN
    SELECT runtime_fingerprint INTO v_fingerprint FROM public.workers WHERE id=NEW.worker_id;
    SELECT runtime_fingerprint INTO v_batch_fingerprint FROM public.batches WHERE id=NEW.batch_id;
    IF (NEW.runtime_fingerprint IS NOT NULL AND NEW.runtime_fingerprint IS DISTINCT FROM v_fingerprint)
      OR (v_batch_fingerprint IS NOT NULL AND v_batch_fingerprint IS DISTINCT FROM v_fingerprint) THEN
      RAISE EXCEPTION USING ERRCODE='BF409', MESSAGE='runtime_fingerprint_mismatch';
    END IF;
  END IF;
  RETURN NEW;
END
$guard$;
CREATE TRIGGER batchforge_guard_task_runtime BEFORE UPDATE OF status,worker_id,attempt_token,runtime_fingerprint ON public.tasks
  FOR EACH ROW EXECUTE FUNCTION public.batchforge_guard_task_runtime();

CREATE OR REPLACE FUNCTION public.batchforge_claim_scheduled(
    p_worker text, p_model text, p_limit integer, p_lease_seconds integer,
    p_policy text DEFAULT 'fair', p_aging_seconds integer DEFAULT 60,
    p_worker_freshness_seconds integer DEFAULT 60
) RETURNS jsonb LANGUAGE plpgsql SECURITY INVOKER
SET search_path = pg_catalog, public AS $function$
DECLARE
    v_worker public.workers%ROWTYPE;
    v_batch public.batches%ROWTYPE;
    v_task record;
    v_updated public.tasks%ROWTYPE;
    v_active bigint;
    v_capacity integer;
    v_tenants text[];
    v_tenant text;
    v_selected_tenant text;
    v_token text;
    v_decision jsonb;
    v_result jsonb := '[]'::jsonb;
    v_count integer := 0;
    v_now timestamptz := clock_timestamp();
BEGIN
    IF p_limit IS NULL OR p_limit<1 OR p_limit>256
       OR p_lease_seconds IS NULL OR p_lease_seconds<5 OR p_lease_seconds>3600
       OR p_policy IS NULL OR p_policy NOT IN ('fair','cost_aware')
       OR p_aging_seconds IS NULL OR p_aging_seconds<1 OR p_aging_seconds>86400
       OR p_worker_freshness_seconds IS NULL OR p_worker_freshness_seconds<1 OR p_worker_freshness_seconds>3600 THEN
        RAISE EXCEPTION USING ERRCODE='BF400', MESSAGE='invalid_claim_limits';
    END IF;
    SELECT * INTO v_worker FROM public.workers WHERE id=p_worker FOR UPDATE;
    IF NOT FOUND OR v_worker.model IS DISTINCT FROM p_model THEN
        RAISE EXCEPTION USING ERRCODE='BF410', MESSAGE='worker_not_registered';
    END IF;
    -- Worker serialization may have waited: a transaction-start timestamp must
    -- not resurrect an expired lease/deadline or backdate worker freshness.
    v_now := clock_timestamp();
    IF p_policy='cost_aware' AND v_worker.scheduler_profile_id IS NULL THEN
        RAISE EXCEPTION USING ERRCODE='BF400', MESSAGE='scheduler_profile_required';
    END IF;
    UPDATE public.workers SET last_seen_at=v_now WHERE id=p_worker;
    SELECT count(*) INTO v_active FROM public.tasks
        WHERE worker_id=p_worker AND status='running' AND lease_expires_at>v_now;
    v_capacity := least(p_limit,greatest(0,v_worker.concurrency-v_active));
    IF v_capacity=0 OR v_worker.metadata->'ready_engine_count'='0'::jsonb THEN
        RETURN jsonb_build_object('tasks',v_result,'lease_seconds',p_lease_seconds,'policy',p_policy);
    END IF;
    -- Bound each transaction to eight fair tenant candidates and 64 oldest
    -- eligible rows per tenant. Lock peer workers NEVER: their capacity is an
    -- advisory preference; their own claim transaction is the admission fence.
    SELECT array_agg(candidate.tenant_id ORDER BY candidate.last_dispatched_at,candidate.tenant_id)
      INTO v_tenants FROM (
        SELECT q.tenant_id,q.last_dispatched_at FROM public.tenant_queues q
        WHERE q.model=p_model AND q.pending_count>0 AND q.next_ready_at<=v_now
          AND EXISTS (SELECT 1 FROM public.tasks eligible JOIN public.batches batch ON batch.id=eligible.batch_id
            WHERE eligible.tenant_id=q.tenant_id AND eligible.model=q.model
              AND eligible.status='pending' AND eligible.ready_at<=v_now
              AND batch.status='in_progress' AND batch.expires_at>v_now
              AND (batch.runtime_fingerprint IS NULL OR batch.runtime_fingerprint=v_worker.runtime_fingerprint)
              AND (eligible.runtime_fingerprint IS NULL OR eligible.runtime_fingerprint=v_worker.runtime_fingerprint)
              AND (p_policy='fair' OR (eligible.estimated_input_tokens>0 AND eligible.estimated_output_tokens>0)))
        ORDER BY q.last_dispatched_at,q.tenant_id LIMIT 8 FOR UPDATE OF q SKIP LOCKED
      ) candidate;
    FOREACH v_tenant IN ARRAY coalesce(v_tenants,ARRAY[]::text[]) LOOP
        FOR v_task IN
          WITH candidates AS MATERIALIZED (
            SELECT t.id,t.batch_id,t.ready_at,t.created_at,t.estimated_input_tokens,
              t.estimated_output_tokens,b.expires_at,
              CASE WHEN v_worker.scheduler_profile_id IS NOT NULL AND t.estimated_input_tokens>0
                AND t.estimated_output_tokens>0 THEN
                t.estimated_input_tokens/v_worker.prefill_tokens_per_second
                + t.estimated_output_tokens/v_worker.decode_tokens_per_second END AS service_seconds
            FROM public.tasks t JOIN public.batches b ON b.id=t.batch_id
            WHERE t.tenant_id=v_tenant AND t.model=p_model AND t.status='pending' AND t.ready_at<=v_now
              AND b.status='in_progress' AND b.expires_at>v_now
              AND (b.runtime_fingerprint IS NULL OR b.runtime_fingerprint=v_worker.runtime_fingerprint)
              AND (t.runtime_fingerprint IS NULL OR t.runtime_fingerprint=v_worker.runtime_fingerprint)
              AND (p_policy='fair' OR (t.estimated_input_tokens>0 AND t.estimated_output_tokens>0))
            ORDER BY t.ready_at,t.id LIMIT 64
          ), ranked AS (
            SELECT c.*,c.service_seconds*v_worker.hourly_cost_usd/3600 AS cost_usd,
              c.created_at<=v_now-p_aging_seconds*interval '1 second' AS aged,
              extract(epoch FROM c.expires_at-v_now)<=coalesce(c.service_seconds,0)*1.25+1 AS urgent
            FROM candidates c
          )
          SELECT r.* FROM ranked r
          WHERE p_policy='fair' OR r.aged OR r.urgent OR NOT EXISTS (
            SELECT 1 FROM public.workers peer
            CROSS JOIN LATERAL (SELECT r.estimated_input_tokens/peer.prefill_tokens_per_second
              + r.estimated_output_tokens/peer.decode_tokens_per_second AS seconds) prediction
            WHERE peer.id<>p_worker AND peer.model=p_model
              AND peer.scheduler_profile_id IS NOT NULL
              AND peer.runtime_fingerprint=v_worker.runtime_fingerprint
              AND peer.last_seen_at>v_now-p_worker_freshness_seconds*interval '1 second'
              AND peer.metadata->'ready_engine_count' IS DISTINCT FROM '0'::jsonb
              AND (SELECT count(*) FROM public.tasks busy WHERE busy.worker_id=peer.id
                AND busy.status='running' AND busy.lease_expires_at>v_now)<peer.concurrency
              AND prediction.seconds<extract(epoch FROM r.expires_at-v_now)
              AND (prediction.seconds*peer.hourly_cost_usd/3600<r.cost_usd-1e-12
                OR (abs(prediction.seconds*peer.hourly_cost_usd/3600-r.cost_usd)<=1e-12
                    AND prediction.seconds<r.service_seconds))
          )
          ORDER BY CASE WHEN p_policy='fair' THEN 0 WHEN r.urgent THEN 0 WHEN r.aged THEN 1 ELSE 2 END,
            CASE WHEN p_policy='cost_aware' AND r.urgent THEN r.expires_at END,
            CASE WHEN p_policy='cost_aware' AND r.aged THEN r.created_at END,
            CASE WHEN p_policy='cost_aware' THEN r.cost_usd END,
            CASE WHEN p_policy='cost_aware' THEN r.service_seconds END,r.ready_at,r.id
        LOOP
            SELECT * INTO v_batch FROM public.batches
                WHERE id=v_task.batch_id AND status='in_progress' AND expires_at>clock_timestamp()
                  AND (runtime_fingerprint IS NULL OR runtime_fingerprint=v_worker.runtime_fingerprint)
                FOR UPDATE SKIP LOCKED;
            IF FOUND THEN
                v_selected_tenant := v_tenant;
                EXIT;
            END IF;
        END LOOP;
        IF v_selected_tenant IS NOT NULL THEN EXIT; END IF;
    END LOOP;
    IF v_selected_tenant IS NULL THEN
        RETURN jsonb_build_object('tasks',v_result,'lease_seconds',p_lease_seconds,'policy',p_policy);
    END IF;
    -- The selected batch remains locked. Recompute eligibility for every row:
    -- a preferred task must not allow unrelated work to bypass peer preference.
    FOR v_task IN
      SELECT t.*,
        CASE WHEN v_worker.scheduler_profile_id IS NOT NULL AND t.estimated_input_tokens>0
          AND t.estimated_output_tokens>0 THEN t.estimated_input_tokens/v_worker.prefill_tokens_per_second
          +t.estimated_output_tokens/v_worker.decode_tokens_per_second END AS service_seconds
      FROM public.tasks t
      WHERE t.batch_id=v_batch.id AND t.status='pending' AND t.ready_at<=v_now
        AND t.id IN (SELECT oldest.id FROM public.tasks oldest
          WHERE oldest.batch_id=v_batch.id AND oldest.status='pending' AND oldest.ready_at<=v_now
            AND (oldest.runtime_fingerprint IS NULL OR oldest.runtime_fingerprint=v_worker.runtime_fingerprint)
            AND (p_policy='fair' OR (oldest.estimated_input_tokens>0 AND oldest.estimated_output_tokens>0))
          ORDER BY oldest.ready_at,oldest.id LIMIT greatest(64,v_capacity))
        AND (t.runtime_fingerprint IS NULL OR t.runtime_fingerprint=v_worker.runtime_fingerprint)
        AND (p_policy='fair' OR (t.estimated_input_tokens>0 AND t.estimated_output_tokens>0))
        AND (p_policy='fair' OR t.created_at<=v_now-p_aging_seconds*interval '1 second'
          OR extract(epoch FROM v_batch.expires_at-v_now)<=(t.estimated_input_tokens/v_worker.prefill_tokens_per_second
            +t.estimated_output_tokens/v_worker.decode_tokens_per_second)*1.25+1
          OR NOT EXISTS (
            SELECT 1 FROM public.workers peer
            CROSS JOIN LATERAL (SELECT t.estimated_input_tokens/peer.prefill_tokens_per_second
              +t.estimated_output_tokens/peer.decode_tokens_per_second AS seconds) prediction
            WHERE peer.id<>p_worker AND peer.model=p_model AND peer.scheduler_profile_id IS NOT NULL
              AND peer.runtime_fingerprint=v_worker.runtime_fingerprint
              AND peer.last_seen_at>v_now-p_worker_freshness_seconds*interval '1 second'
              AND peer.metadata->'ready_engine_count' IS DISTINCT FROM '0'::jsonb
              AND (SELECT count(*) FROM public.tasks busy WHERE busy.worker_id=peer.id
                AND busy.status='running' AND busy.lease_expires_at>v_now)<peer.concurrency
              AND prediction.seconds<extract(epoch FROM v_batch.expires_at-v_now)
              AND (prediction.seconds*peer.hourly_cost_usd/3600
                <(t.estimated_input_tokens/v_worker.prefill_tokens_per_second
                  +t.estimated_output_tokens/v_worker.decode_tokens_per_second)*v_worker.hourly_cost_usd/3600-1e-12
                OR (abs(prediction.seconds*peer.hourly_cost_usd/3600
                  -(t.estimated_input_tokens/v_worker.prefill_tokens_per_second
                    +t.estimated_output_tokens/v_worker.decode_tokens_per_second)*v_worker.hourly_cost_usd/3600)<=1e-12
                  AND prediction.seconds<t.estimated_input_tokens/v_worker.prefill_tokens_per_second
                    +t.estimated_output_tokens/v_worker.decode_tokens_per_second))
          ))
      ORDER BY
        CASE WHEN p_policy='fair' THEN 0
          WHEN extract(epoch FROM v_batch.expires_at-v_now)<=(t.estimated_input_tokens/v_worker.prefill_tokens_per_second
            +t.estimated_output_tokens/v_worker.decode_tokens_per_second)*1.25+1 THEN 0
          WHEN t.created_at<=v_now-p_aging_seconds*interval '1 second' THEN 1 ELSE 2 END,
        CASE WHEN p_policy='cost_aware' AND t.created_at<=v_now-p_aging_seconds*interval '1 second' THEN t.created_at END,
        CASE WHEN p_policy='cost_aware' THEN t.estimated_input_tokens/v_worker.prefill_tokens_per_second
          +t.estimated_output_tokens/v_worker.decode_tokens_per_second END,t.ready_at,t.id
      LIMIT v_capacity FOR UPDATE OF t SKIP LOCKED
    LOOP
        -- A long selection/lock acquisition must not dispatch past the deadline.
        -- SKIP LOCKED avoids waiting on task rows, but elapsed query time matters.
        IF v_batch.expires_at<=clock_timestamp() THEN EXIT; END IF;
        IF v_count=0 THEN
          UPDATE public.batches SET runtime_fingerprint=coalesce(runtime_fingerprint,v_worker.runtime_fingerprint),
            runtime_compatibility_scope=CASE WHEN v_worker.runtime_fingerprint IS NULL
              THEN 'legacy_unverified' ELSE runtime_compatibility_scope END
            WHERE id=v_batch.id RETURNING * INTO v_batch;
        END IF;
        v_token := 'attempt_' || replace(gen_random_uuid()::text,'-','');
        v_decision := jsonb_build_object('policy',p_policy,'profile_id',v_worker.scheduler_profile_id,
          'runtime_fingerprint',v_worker.runtime_fingerprint,'estimate_source',v_task.estimate_source,
          'estimated_input_tokens',v_task.estimated_input_tokens,'estimated_output_tokens',v_task.estimated_output_tokens,
          'prefill_tokens_per_second',v_worker.prefill_tokens_per_second,
          'decode_tokens_per_second',v_worker.decode_tokens_per_second,'hourly_cost_usd',v_worker.hourly_cost_usd,
          'predicted_prefill_seconds',v_task.estimated_input_tokens/v_worker.prefill_tokens_per_second,
          'predicted_decode_seconds',v_task.estimated_output_tokens/v_worker.decode_tokens_per_second,
          'predicted_service_seconds',v_task.service_seconds,
          'predicted_cost_usd',v_task.service_seconds*v_worker.hourly_cost_usd/3600,
          'calibration_label',v_worker.metadata#>>'{scheduling_profile,calibration_label}',
          'cost_source',v_worker.metadata#>>'{scheduling_profile,cost_source}',
          'runtime_compatibility_scope',v_batch.runtime_compatibility_scope,
          'aging_seconds',p_aging_seconds,'selected_at',v_now);
        UPDATE public.tasks SET status='running',attempts=attempts+1,attempt_token=v_token,
          worker_id=p_worker,lease_expires_at=clock_timestamp()+p_lease_seconds*interval '1 second',error=NULL,
          runtime_fingerprint=coalesce(runtime_fingerprint,v_worker.runtime_fingerprint)
          WHERE id=v_task.id RETURNING * INTO v_updated;
        INSERT INTO public.attempts(token,task_id,batch_id,tenant_id,worker_id,number,status,scheduling_decision)
          VALUES(v_token,v_task.id,v_batch.id,v_task.tenant_id,p_worker,v_updated.attempts,'running',v_decision);
        v_result := v_result || jsonb_build_array(jsonb_build_object(
          'id',v_task.id,'batch_id',v_batch.id,'custom_id',v_task.custom_id,'body',v_task.body,
          'attempt_token',v_token,'lease_expires_at',v_updated.lease_expires_at,'attempt_number',v_updated.attempts,
          'scheduling_decision',v_decision));
        v_count := v_count+1;
    END LOOP;
    UPDATE public.batches SET pending_count=pending_count-v_count,running_count=running_count+v_count WHERE id=v_batch.id;
    UPDATE public.tenant_queues SET pending_count=pending_count-v_count,
      next_ready_at=coalesce((SELECT ready_at FROM public.tasks
        WHERE tenant_id=v_selected_tenant AND model=p_model AND status='pending' ORDER BY ready_at,id LIMIT 1),clock_timestamp()),
      last_dispatched_at=CASE WHEN v_count>0 THEN clock_timestamp() ELSE last_dispatched_at END
      WHERE tenant_id=v_selected_tenant AND model=p_model;
    RETURN jsonb_build_object('tasks',v_result,'lease_seconds',p_lease_seconds,'policy',p_policy);
END
$function$;
