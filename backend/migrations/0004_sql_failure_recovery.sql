-- Additive v4. Failure reports and bounded lease/deadline recovery each use one
-- client/server round trip. Existing v1-v3 migration bytes remain immutable.
-- Every entry point is SECURITY INVOKER: no caller privilege is amplified.

CREATE OR REPLACE FUNCTION public.batchforge_finish_batch_v4(p_batch text)
RETURNS void LANGUAGE plpgsql SECURITY INVOKER
SET search_path = pg_catalog, public AS $function$
DECLARE
    v_source record;
    v_batch public.batches%ROWTYPE;
    v_status text;
    v_output text;
    v_errors text;
BEGIN
    -- This helper changes only batch/file metadata, not a task or worker. Its
    -- callers already hold these locks; direct calls retain queue -> batch order.
    SELECT tenant_id,model INTO v_source FROM public.batches WHERE id=p_batch;
    IF NOT FOUND THEN RETURN; END IF;
    PERFORM tenant_id FROM public.tenant_queues
        WHERE tenant_id=v_source.tenant_id AND model=v_source.model FOR UPDATE;
    SELECT * INTO v_batch FROM public.batches WHERE id=p_batch FOR UPDATE;
    IF v_batch.pending_count<>0 OR v_batch.running_count<>0 THEN RETURN; END IF;
    v_status := v_batch.status;
    IF v_status='in_progress' THEN
        v_status := CASE WHEN (v_batch.cancelled_count>0 AND v_batch.expires_at<=clock_timestamp())
            OR (v_batch.completed_count=0 AND v_batch.failed_count>0)
            THEN 'failed' ELSE 'completed' END;
    END IF;
    v_output := v_batch.output_file_id;
    v_errors := v_batch.error_file_id;
    IF v_batch.completed_count>0 AND v_output IS NULL THEN
        v_output := 'file_' || replace(gen_random_uuid()::text,'-','');
        INSERT INTO public.files(id,tenant_id,filename,kind,batch_id)
            VALUES(v_output,v_batch.tenant_id,v_batch.id||'-output.jsonl','output',v_batch.id);
    END IF;
    IF (v_batch.failed_count>0 OR v_batch.cancelled_count>0) AND v_errors IS NULL THEN
        v_errors := 'file_' || replace(gen_random_uuid()::text,'-','');
        INSERT INTO public.files(id,tenant_id,filename,kind,batch_id)
            VALUES(v_errors,v_batch.tenant_id,v_batch.id||'-errors.jsonl','error',v_batch.id);
    END IF;
    UPDATE public.batches SET status=v_status,output_file_id=v_output,error_file_id=v_errors,
        completed_at=CASE WHEN v_status='completed' THEN COALESCE(completed_at,clock_timestamp()) ELSE completed_at END,
        failed_at=CASE WHEN v_status='failed' THEN COALESCE(failed_at,clock_timestamp()) ELSE failed_at END
        WHERE id=v_batch.id;
END
$function$;

CREATE OR REPLACE FUNCTION public.batchforge_fail(
    p_task text,p_worker text,p_token text,p_error jsonb,p_retryable boolean,
    p_retry_base_seconds integer
) RETURNS jsonb LANGUAGE plpgsql SECURITY INVOKER
SET search_path = pg_catalog, public AS $function$
DECLARE
    v_source record;
    v_task public.tasks%ROWTYPE;
    v_batch public.batches%ROWTYPE;
    v_now timestamptz;
    v_retry boolean;
    v_target text;
    v_delay double precision;
BEGIN
    IF p_retry_base_seconds IS NULL OR p_retry_base_seconds<1 OR p_retry_base_seconds>60
       OR p_retryable IS NULL OR jsonb_typeof(p_error) IS DISTINCT FROM 'object'
       OR jsonb_typeof(p_error->'code') IS DISTINCT FROM 'string'
       OR jsonb_typeof(p_error->'message') IS DISTINCT FROM 'string'
       OR length(p_error->>'code') NOT BETWEEN 1 AND 128
       OR length(p_error->>'message') NOT BETWEEN 1 AND 4096 THEN
        RAISE EXCEPTION USING ERRCODE='BF400', MESSAGE='invalid_failure';
    END IF;
    UPDATE public.workers SET last_seen_at=clock_timestamp() WHERE id=p_worker;
    SELECT batch_id,tenant_id,model INTO v_source FROM public.tasks WHERE id=p_task;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE='BF404', MESSAGE='not_found';
    END IF;
    PERFORM tenant_id FROM public.tenant_queues
        WHERE tenant_id=v_source.tenant_id AND model=v_source.model FOR UPDATE;
    SELECT * INTO v_batch FROM public.batches WHERE id=v_source.batch_id FOR UPDATE;
    SELECT * INTO v_task FROM public.tasks WHERE id=p_task FOR UPDATE;
    -- clock_timestamp(), after waiting for every lock, fences expiry even if
    -- this statement started while the old lease was still valid.
    v_now := clock_timestamp();
    IF v_batch.status<>'in_progress' OR v_batch.expires_at<=v_now
       OR v_task.status<>'running' OR v_task.worker_id IS DISTINCT FROM p_worker
       OR v_task.attempt_token IS DISTINCT FROM p_token
       OR v_task.lease_expires_at IS NULL OR v_task.lease_expires_at<=v_now THEN
        RAISE EXCEPTION USING ERRCODE='BF409', MESSAGE='stale_attempt';
    END IF;
    v_retry := p_retryable AND v_task.attempts<v_task.max_attempts;
    v_target := CASE WHEN v_retry THEN 'pending' ELSE 'failed' END;
    -- Equal jitter spreads a synchronized failure storm without a zero-delay
    -- retry. The delay is [0.5,1.0) times the capped exponential, at most 300s.
    v_delay := least(300.0,p_retry_base_seconds*power(2.0,greatest(0,v_task.attempts-1)))
        * (0.5+random()*0.5);
    UPDATE public.tasks SET status=v_target,error=p_error,lease_expires_at=NULL,
        ready_at=v_now+v_delay*interval '1 second',
        finished_at=CASE WHEN v_retry THEN NULL ELSE v_now END WHERE id=p_task;
    UPDATE public.attempts SET status=CASE WHEN v_retry THEN 'retrying' ELSE 'failed' END,
        error=p_error,finished_at=v_now WHERE token=p_token;
    UPDATE public.batches SET running_count=running_count-1,
        pending_count=pending_count+v_retry::integer,failed_count=failed_count+(NOT v_retry)::integer
        WHERE id=v_task.batch_id;
    UPDATE public.tenant_queues SET pending_count=pending_count+v_retry::integer,
        next_ready_at=COALESCE((SELECT ready_at FROM public.tasks
            WHERE tenant_id=v_task.tenant_id AND model=v_task.model AND status='pending'
            ORDER BY ready_at,id LIMIT 1),clock_timestamp())
        WHERE tenant_id=v_task.tenant_id AND model=v_task.model;
    PERFORM public.batchforge_finish_batch_v4(v_task.batch_id);
    UPDATE public.workers SET last_seen_at=clock_timestamp() WHERE id=p_worker;
    RETURN jsonb_build_object('ok',true,'status',v_target);
END
$function$;

CREATE OR REPLACE FUNCTION public.batchforge_recover(
    p_task_limit integer,p_batch_limit integer,p_retry_base_seconds integer
) RETURNS jsonb LANGUAGE plpgsql SECURITY INVOKER
SET search_path = pg_catalog, public AS $function$
DECLARE
    v_cutoff timestamptz := clock_timestamp();
    v_now timestamptz;
    v_task_ids text[];
    v_workers text[];
    v_locked_workers text[];
    v_queue_tenants text[];
    v_queue_models text[];
    v_batches text[];
    v_task public.tasks%ROWTYPE;
    v_batch public.batches%ROWTYPE;
    v_retry boolean;
    v_delay double precision;
    v_error jsonb;
    v_changes jsonb := '[]'::jsonb;
    v_changed_batch text;
    v_recovered integer := 0;
    v_cancelled integer := 0;
    v_processed integer := 0;
    v_batch_count integer := 0;
BEGIN
    IF p_task_limit IS NULL OR p_task_limit<1 OR p_task_limit>1024
       OR p_batch_limit IS NULL OR p_batch_limit<1 OR p_batch_limit>64
       OR p_retry_base_seconds IS NULL OR p_retry_base_seconds<1 OR p_retry_base_seconds>60 THEN
        RAISE EXCEPTION USING ERRCODE='BF400', MESSAGE='invalid_recovery_limits';
    END IF;
    -- Indexed lease candidates are globally limited BEFORE grouping by batch.
    -- Deadline task reads use bounded index ranges for each of the two states.
    -- The final candidate list and all transitions share ONE global task budget,
    -- not task_limit per batch. Row-scan/IO cost is not a wall-clock guarantee.
    WITH expired_batches AS MATERIALIZED (
        SELECT id,expires_at AS due_at FROM public.batches
        WHERE status='in_progress' AND expires_at<=v_cutoff AND pending_count+running_count>0
        ORDER BY expires_at,id LIMIT p_batch_limit
    ), expired_leases AS MATERIALIZED (
        SELECT t.id,t.batch_id,t.lease_expires_at AS due_at
        FROM public.tasks t JOIN public.batches b ON b.id=t.batch_id
        WHERE t.status='running' AND t.lease_expires_at<=v_cutoff AND b.status='in_progress'
        ORDER BY t.lease_expires_at,t.id LIMIT p_task_limit
    ), chosen_batches AS MATERIALIZED (
        SELECT batch_id,min(due_at) AS due_at FROM (
            SELECT id AS batch_id,due_at FROM expired_batches
            UNION ALL SELECT batch_id,due_at FROM expired_leases
        ) sources GROUP BY batch_id ORDER BY min(due_at),batch_id LIMIT p_batch_limit
    ), deadline_tasks AS (
        SELECT t.id,c.batch_id,c.due_at FROM chosen_batches c
        JOIN public.batches b ON b.id=c.batch_id AND b.expires_at<=v_cutoff
        CROSS JOIN LATERAL (
            (SELECT id FROM public.tasks WHERE batch_id=c.batch_id AND status='pending'
                ORDER BY id LIMIT p_task_limit)
            UNION ALL
            (SELECT id FROM public.tasks WHERE batch_id=c.batch_id AND status='running'
                ORDER BY id LIMIT p_task_limit)
        ) t
    )
    SELECT array_agg(id ORDER BY due_at,id) INTO v_task_ids FROM (
        SELECT id,min(due_at) AS due_at FROM (
            SELECT id,batch_id,due_at FROM deadline_tasks
            UNION ALL
            SELECT e.id,e.batch_id,e.due_at FROM expired_leases e
                JOIN chosen_batches c ON c.batch_id=e.batch_id
        ) candidates GROUP BY id ORDER BY min(due_at),id LIMIT p_task_limit
    ) bounded;
    IF v_task_ids IS NULL THEN
        RETURN jsonb_build_object('recovered',0,'deadline_cancelled',0,'processed',0,'batches',0);
    END IF;

    -- Acquire ALL workers before ANY queue. Locking a new worker inside a task
    -- loop can deadlock with that worker's completion waiting for an older queue.
    SELECT array_agg(DISTINCT worker_id) INTO v_workers FROM public.tasks
        WHERE id=ANY(v_task_ids) AND status='running' AND worker_id IS NOT NULL;
    SELECT array_agg(id) INTO v_locked_workers FROM (
        SELECT id FROM public.workers WHERE id=ANY(coalesce(v_workers,ARRAY[]::text[]))
        ORDER BY id FOR UPDATE SKIP LOCKED
    ) locked;
    SELECT array_agg(tenant_id),array_agg(model) INTO v_queue_tenants,v_queue_models FROM (
        SELECT q.tenant_id,q.model FROM public.tenant_queues q
        WHERE EXISTS(SELECT 1 FROM public.tasks t WHERE t.id=ANY(v_task_ids)
            AND t.tenant_id=q.tenant_id AND t.model=q.model
            AND (t.status<>'running' OR t.worker_id=ANY(coalesce(v_locked_workers,ARRAY[]::text[]))))
        ORDER BY q.tenant_id,q.model FOR UPDATE SKIP LOCKED
    ) locked;
    SELECT array_agg(id) INTO v_batches FROM (
        SELECT b.id FROM public.batches b WHERE b.status='in_progress'
            AND EXISTS(SELECT 1 FROM public.tasks t
                JOIN unnest(v_queue_tenants,v_queue_models) q(tenant_id,model)
                    ON q.tenant_id=t.tenant_id AND q.model=t.model
                WHERE t.id=ANY(v_task_ids) AND t.batch_id=b.id
                    AND (t.status<>'running' OR t.worker_id=ANY(coalesce(v_locked_workers,ARRAY[]::text[]))))
        ORDER BY b.id FOR UPDATE SKIP LOCKED
    ) locked;
    FOR v_task IN
        SELECT t.* FROM public.tasks t WHERE t.id=ANY(v_task_ids)
            AND t.batch_id=ANY(coalesce(v_batches,ARRAY[]::text[]))
            AND (t.status<>'running' OR t.worker_id=ANY(coalesce(v_locked_workers,ARRAY[]::text[])))
        ORDER BY t.id FOR UPDATE SKIP LOCKED
    LOOP
        SELECT * INTO v_batch FROM public.batches WHERE id=v_task.batch_id;
        v_now := clock_timestamp();
        IF v_batch.status<>'in_progress' THEN CONTINUE; END IF;
        IF v_batch.expires_at<=v_now AND v_task.status IN ('pending','running') THEN
            v_error := jsonb_build_object('code','batch_expired','message','24-hour completion window expired');
            IF v_task.status='running' THEN
                UPDATE public.attempts SET status='cancelled',error=v_error,finished_at=v_now
                    WHERE token=v_task.attempt_token AND status='running';
            END IF;
            UPDATE public.tasks SET status='cancelled',error=v_error,finished_at=v_now,
                lease_expires_at=NULL WHERE id=v_task.id;
            v_changes := v_changes || jsonb_build_array(jsonb_build_object(
                'batch_id',v_task.batch_id,'tenant_id',v_task.tenant_id,'model',v_task.model,
                'pending_delta',CASE WHEN v_task.status='pending' THEN -1 ELSE 0 END,
                'running_delta',CASE WHEN v_task.status='running' THEN -1 ELSE 0 END,
                'failed_delta',0,'cancelled_delta',1));
            v_cancelled := v_cancelled+1;
        ELSIF v_task.status='running' AND v_task.lease_expires_at<=v_now THEN
            v_retry := v_task.attempts<v_task.max_attempts;
            v_delay := least(300.0,p_retry_base_seconds*power(2.0,greatest(0,v_task.attempts-1)))
                * (0.5+random()*0.5);
            v_error := jsonb_build_object('code','lease_expired','message','Worker lease expired');
            UPDATE public.tasks SET status=CASE WHEN v_retry THEN 'pending' ELSE 'failed' END,
                error=v_error,lease_expires_at=NULL,ready_at=v_now+v_delay*interval '1 second',
                finished_at=CASE WHEN v_retry THEN NULL ELSE v_now END WHERE id=v_task.id;
            UPDATE public.attempts SET status=CASE WHEN v_retry THEN 'expired' ELSE 'failed' END,
                error=v_error,finished_at=v_now WHERE token=v_task.attempt_token;
            v_changes := v_changes || jsonb_build_array(jsonb_build_object(
                'batch_id',v_task.batch_id,'tenant_id',v_task.tenant_id,'model',v_task.model,
                'pending_delta',v_retry::integer,'running_delta',-1,
                'failed_delta',(NOT v_retry)::integer,'cancelled_delta',0));
            v_recovered := v_recovered+1;
        ELSE
            -- A concurrent heartbeat/claim/completion may have changed a row
            -- after candidate selection. Its current lease and token win.
            CONTINUE;
        END IF;
        v_processed := v_processed+1;
    END LOOP;
    -- Aggregate once per batch and queue instead of writing their hotspot rows
    -- once for every expired task. All target rows were already locked above.
    UPDATE public.batches b SET pending_count=b.pending_count+d.pending_delta,
        running_count=b.running_count+d.running_delta,failed_count=b.failed_count+d.failed_delta,
        cancelled_count=b.cancelled_count+d.cancelled_delta
    FROM (SELECT batch_id,sum(pending_delta)::integer AS pending_delta,
        sum(running_delta)::integer AS running_delta,sum(failed_delta)::integer AS failed_delta,
        sum(cancelled_delta)::integer AS cancelled_delta
        FROM jsonb_to_recordset(v_changes) AS c(batch_id text,pending_delta integer,
            running_delta integer,failed_delta integer,cancelled_delta integer)
        GROUP BY batch_id) d WHERE b.id=d.batch_id;
    UPDATE public.tenant_queues q SET pending_count=q.pending_count+d.pending_delta,
        next_ready_at=COALESCE((SELECT ready_at FROM public.tasks
            WHERE tenant_id=q.tenant_id AND model=q.model AND status='pending'
            ORDER BY ready_at,id LIMIT 1),clock_timestamp())
    FROM (SELECT tenant_id,model,sum(pending_delta)::integer AS pending_delta
        FROM jsonb_to_recordset(v_changes) AS c(tenant_id text,model text,pending_delta integer)
        GROUP BY tenant_id,model) d WHERE q.tenant_id=d.tenant_id AND q.model=d.model;
    FOR v_changed_batch IN SELECT DISTINCT batch_id
        FROM jsonb_to_recordset(v_changes) AS c(batch_id text) ORDER BY batch_id
    LOOP
        PERFORM public.batchforge_finish_batch_v4(v_changed_batch);
        v_batch_count := v_batch_count+1;
    END LOOP;
    RETURN jsonb_build_object('recovered',v_recovered,'deadline_cancelled',v_cancelled,
        'processed',v_processed,'batches',v_batch_count);
END
$function$;
