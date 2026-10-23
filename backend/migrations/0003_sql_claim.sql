-- Additive v3. Preserve Python claim selection, lock order and bounded fairness.
CREATE OR REPLACE FUNCTION public.batchforge_claim(
    p_worker text, p_model text, p_limit integer, p_lease_seconds integer
) RETURNS jsonb LANGUAGE plpgsql SECURITY INVOKER
SET search_path = pg_catalog, public AS $function$
DECLARE
    v_worker public.workers%ROWTYPE;
    v_batch public.batches%ROWTYPE;
    v_task public.tasks%ROWTYPE;
    v_updated public.tasks%ROWTYPE;
    v_active bigint;
    v_capacity integer;
    v_tenants text[];
    v_tenant text;
    v_selected_tenant text;
    v_batch_id text;
    v_token text;
    v_result jsonb := '[]'::jsonb;
    v_count integer := 0;
BEGIN
    IF p_limit IS NULL OR p_limit<1 OR p_limit>256 OR p_lease_seconds IS NULL OR p_lease_seconds<5 OR p_lease_seconds>3600 THEN
        RAISE EXCEPTION USING ERRCODE='BF400', MESSAGE='invalid_claim_limits';
    END IF;
    SELECT * INTO v_worker FROM public.workers WHERE id=p_worker FOR UPDATE;
    IF NOT FOUND OR v_worker.model IS DISTINCT FROM p_model THEN
        RAISE EXCEPTION USING ERRCODE='BF410', MESSAGE='worker_not_registered';
    END IF;
    UPDATE public.workers SET last_seen_at=clock_timestamp() WHERE id=p_worker;
    SELECT count(*) INTO v_active FROM public.tasks
        WHERE worker_id=p_worker AND status='running' AND lease_expires_at>clock_timestamp();
    v_capacity := least(p_limit,greatest(0,v_worker.concurrency-v_active));
    IF v_capacity=0 THEN
        RETURN jsonb_build_object('tasks',v_result,'lease_seconds',p_lease_seconds);
    END IF;
    -- Materialize and lock exactly the same oldest eight ready queue candidates
    -- as the original fetchall(). Do not accidentally lock one row per cursor
    -- iteration or widen this selection past eight during function execution.
    SELECT array_agg(candidate.tenant_id ORDER BY candidate.last_dispatched_at,candidate.tenant_id)
      INTO v_tenants FROM (
        SELECT tenant_id,last_dispatched_at FROM public.tenant_queues
        WHERE model=p_model AND pending_count>0 AND next_ready_at<=clock_timestamp()
        ORDER BY last_dispatched_at,tenant_id LIMIT 8 FOR UPDATE SKIP LOCKED
      ) candidate;
    FOREACH v_tenant IN ARRAY coalesce(v_tenants,ARRAY[]::text[]) LOOP
        SELECT batch_id INTO v_batch_id FROM public.tasks
            WHERE tenant_id=v_tenant AND model=p_model AND status='pending' AND ready_at<=clock_timestamp()
            ORDER BY ready_at,id LIMIT 1;
        IF NOT FOUND THEN CONTINUE; END IF;
        SELECT * INTO v_batch FROM public.batches
            WHERE id=v_batch_id AND status='in_progress' AND expires_at>clock_timestamp()
            FOR UPDATE SKIP LOCKED;
        IF FOUND THEN
            v_selected_tenant := v_tenant;
            EXIT;
        END IF;
    END LOOP;
    IF v_selected_tenant IS NULL THEN
        RETURN jsonb_build_object('tasks',v_result,'lease_seconds',p_lease_seconds);
    END IF;
    FOR v_task IN
        SELECT * FROM public.tasks WHERE batch_id=v_batch.id AND status='pending' AND ready_at<=clock_timestamp()
        ORDER BY ready_at,id LIMIT v_capacity FOR UPDATE SKIP LOCKED
    LOOP
        v_token := 'attempt_' || replace(gen_random_uuid()::text,'-','');
        UPDATE public.tasks SET status='running',attempts=attempts+1,attempt_token=v_token,
            worker_id=p_worker,lease_expires_at=clock_timestamp()+p_lease_seconds*interval '1 second',error=NULL
            WHERE id=v_task.id RETURNING * INTO v_updated;
        INSERT INTO public.attempts(token,task_id,batch_id,tenant_id,worker_id,number,status)
            VALUES(v_token,v_task.id,v_batch.id,v_task.tenant_id,p_worker,v_updated.attempts,'running');
        v_result := v_result || jsonb_build_array(jsonb_build_object(
            'id',v_task.id,'batch_id',v_batch.id,'custom_id',v_task.custom_id,'body',v_task.body,
            'attempt_token',v_token,'lease_expires_at',v_updated.lease_expires_at,'attempt_number',v_updated.attempts));
        v_count := v_count+1;
    END LOOP;
    UPDATE public.batches SET pending_count=pending_count-v_count,running_count=running_count+v_count WHERE id=v_batch.id;
    UPDATE public.tenant_queues SET pending_count=pending_count-v_count,
        next_ready_at=coalesce((SELECT ready_at FROM public.tasks
            WHERE tenant_id=v_selected_tenant AND model=p_model AND status='pending' ORDER BY ready_at,id LIMIT 1),clock_timestamp()),
        last_dispatched_at=clock_timestamp()
        WHERE tenant_id=v_selected_tenant AND model=p_model;
    RETURN jsonb_build_object('tasks',v_result,'lease_seconds',p_lease_seconds);
END
$function$;
