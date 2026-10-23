-- Additive v2: one client/server round trip per completion or heartbeat.
-- SECURITY INVOKER deliberately preserves the caller's existing table ACLs.
-- No extension, SECURITY DEFINER, autonomous commit, or client wall clock.
CREATE OR REPLACE FUNCTION public.batchforge_heartbeat(
    p_task text, p_worker text, p_token text, p_lease_seconds integer
) RETURNS jsonb LANGUAGE plpgsql SECURITY INVOKER
SET search_path = pg_catalog, public AS $function$
DECLARE
    v_source record;
    v_task public.tasks%ROWTYPE;
    v_batch public.batches%ROWTYPE;
    v_now timestamptz;
    v_expiry timestamptz;
BEGIN
    IF p_lease_seconds IS NULL OR p_lease_seconds < 5 OR p_lease_seconds > 3600 THEN
        RAISE EXCEPTION USING ERRCODE='BF400', MESSAGE='invalid_lease';
    END IF;
    -- Preserve the worker -> queue -> batch -> task lock order used by claims.
    UPDATE public.workers SET last_seen_at=clock_timestamp() WHERE id=p_worker;
    SELECT batch_id,tenant_id,model INTO v_source FROM public.tasks WHERE id=p_task;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE='BF404', MESSAGE='not_found';
    END IF;
    PERFORM tenant_id FROM public.tenant_queues WHERE tenant_id=v_source.tenant_id AND model=v_source.model FOR UPDATE;
    SELECT * INTO v_batch FROM public.batches WHERE id=v_source.batch_id FOR UPDATE;
    SELECT * INTO v_task FROM public.tasks WHERE id=p_task FOR UPDATE;
    v_now := clock_timestamp();
    IF v_batch.status <> 'in_progress' OR v_batch.expires_at <= v_now
       OR v_task.status <> 'running' OR v_task.worker_id IS DISTINCT FROM p_worker
       OR v_task.attempt_token IS DISTINCT FROM p_token
       OR v_task.lease_expires_at IS NULL OR v_task.lease_expires_at <= v_now THEN
        RAISE EXCEPTION USING ERRCODE='BF409', MESSAGE='stale_attempt';
    END IF;
    v_expiry := clock_timestamp() + p_lease_seconds * interval '1 second';
    UPDATE public.tasks SET lease_expires_at=v_expiry WHERE id=p_task;
    UPDATE public.workers SET last_seen_at=clock_timestamp() WHERE id=p_worker;
    RETURN jsonb_build_object('ok',true,'lease_expires_at',v_expiry);
END
$function$;

CREATE OR REPLACE FUNCTION public.batchforge_complete(
    p_task text, p_worker text, p_token text, p_response jsonb,
    p_prompt_tokens bigint, p_completion_tokens bigint
) RETURNS jsonb LANGUAGE plpgsql SECURITY INVOKER
SET search_path = pg_catalog, public AS $function$
DECLARE
    v_source record;
    v_task public.tasks%ROWTYPE;
    v_batch public.batches%ROWTYPE;
    v_now timestamptz;
    v_status text;
    v_output text;
    v_errors text;
BEGIN
    IF p_prompt_tokens IS NULL OR p_completion_tokens IS NULL
       OR p_prompt_tokens < 0 OR p_completion_tokens < 0 OR p_response IS NULL THEN
        RAISE EXCEPTION USING ERRCODE='BF400', MESSAGE='invalid_usage';
    END IF;
    UPDATE public.workers SET last_seen_at=clock_timestamp() WHERE id=p_worker;
    SELECT batch_id,tenant_id,model INTO v_source FROM public.tasks WHERE id=p_task;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE='BF404', MESSAGE='not_found';
    END IF;
    PERFORM tenant_id FROM public.tenant_queues WHERE tenant_id=v_source.tenant_id AND model=v_source.model FOR UPDATE;
    SELECT * INTO v_batch FROM public.batches WHERE id=v_source.batch_id FOR UPDATE;
    SELECT * INTO v_task FROM public.tasks WHERE id=p_task FOR UPDATE;
    -- An already accepted token remains duplicate-safe after terminal/deadline.
    IF v_task.status='completed' AND v_task.attempt_token=p_token AND v_task.worker_id=p_worker THEN
        RETURN jsonb_build_object('ok',true,'duplicate',true);
    END IF;
    v_now := clock_timestamp();
    IF v_batch.status <> 'in_progress' OR v_batch.expires_at <= v_now
       OR v_task.status <> 'running' OR v_task.worker_id IS DISTINCT FROM p_worker
       OR v_task.attempt_token IS DISTINCT FROM p_token
       OR v_task.lease_expires_at IS NULL OR v_task.lease_expires_at <= v_now THEN
        RAISE EXCEPTION USING ERRCODE='BF409', MESSAGE='stale_attempt';
    END IF;
    UPDATE public.tasks SET status='completed',response=p_response,error=NULL,
        prompt_tokens=p_prompt_tokens,completion_tokens=p_completion_tokens,
        finished_at=clock_timestamp(),lease_expires_at=NULL WHERE id=p_task;
    UPDATE public.attempts SET status='completed',prompt_tokens=p_prompt_tokens,
        completion_tokens=p_completion_tokens,finished_at=clock_timestamp() WHERE token=p_token;
    UPDATE public.batches SET running_count=running_count-1,completed_count=completed_count+1,
        prompt_tokens=prompt_tokens+p_prompt_tokens,completion_tokens=completion_tokens+p_completion_tokens
        WHERE id=v_source.batch_id RETURNING * INTO v_batch;
    UPDATE public.workers SET last_seen_at=clock_timestamp() WHERE id=p_worker;
    IF v_batch.pending_count=0 AND v_batch.running_count=0 THEN
        v_status := v_batch.status;
        IF v_status='in_progress' THEN
            v_status := CASE WHEN v_batch.completed_count=0 AND v_batch.failed_count>0 THEN 'failed' ELSE 'completed' END;
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
            completed_at=CASE WHEN v_status='completed' THEN COALESCE(completed_at,now()) ELSE completed_at END,
            failed_at=CASE WHEN v_status='failed' THEN COALESCE(failed_at,now()) ELSE failed_at END
            WHERE id=v_batch.id;
    END IF;
    RETURN jsonb_build_object('ok',true);
END
$function$;
