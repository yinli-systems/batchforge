-- Immutable v9: task history and canonical-JSON admission bounds, not a claim
-- about physical heap/TOAST/index/WAL bytes. Existing over-limit data is adopted;
-- only new INSERTs are refused. No task history is automatically deleted.
-- Relation gates do not wait for an admission's tenant ROW SHARE/tuple lock.
-- EXCLUSIVE on tenants would also queue new FK checks behind a waiting DDL gate.
LOCK TABLE public.tenants,public.tasks,public.attempts IN SHARE ROW EXCLUSIVE MODE;
CREATE TABLE public.tenant_retention_limits (
  tenant_id text PRIMARY KEY REFERENCES public.tenants(id),
  max_retained_tasks bigint NOT NULL DEFAULT 100000 CHECK(max_retained_tasks>0),
  max_payload_bytes bigint NOT NULL DEFAULT 1073741824 CHECK(max_payload_bytes>0),
  max_response_bytes integer NOT NULL DEFAULT 1048576 CHECK(max_response_bytes BETWEEN 1024 AND 16777216),
  max_error_bytes integer NOT NULL DEFAULT 32768 CHECK(max_error_bytes BETWEEN 32768 AND 1048576)
);
CREATE TABLE public.tenant_retention_usage (
  tenant_id text PRIMARY KEY REFERENCES public.tenants(id),
  retained_tasks bigint NOT NULL DEFAULT 0 CHECK(retained_tasks>=0),
  charged_payload_bytes bigint NOT NULL DEFAULT 0 CHECK(charged_payload_bytes>=0)
);
ALTER TABLE public.tasks
  ADD COLUMN retention_body_bytes bigint NOT NULL DEFAULT 0 CHECK(retention_body_bytes>=0),
  ADD COLUMN retention_response_bytes bigint NOT NULL DEFAULT 0 CHECK(retention_response_bytes>=0),
  ADD COLUMN retention_error_bytes bigint NOT NULL DEFAULT 0 CHECK(retention_error_bytes>=0),
  ADD COLUMN retention_response_limit integer NOT NULL DEFAULT 1048576 CHECK(retention_response_limit>0),
  ADD COLUMN retention_error_limit integer NOT NULL DEFAULT 32768 CHECK(retention_error_limit>=32768),
  ADD COLUMN retention_payload_cleared boolean NOT NULL DEFAULT false,
  ADD COLUMN retention_errors_sealed boolean NOT NULL DEFAULT false;

CREATE FUNCTION public.batchforge_json_bytes(p_value jsonb) RETURNS bigint
LANGUAGE sql IMMUTABLE SECURITY INVOKER SET search_path=pg_catalog,public
AS $$ SELECT CASE WHEN p_value IS NULL THEN 0 ELSE octet_length(p_value::text)::bigint END $$;

-- Migration owns the write window. Limits need not accommodate historical data.
INSERT INTO public.tenant_retention_limits(tenant_id) SELECT id FROM public.tenants;
UPDATE public.tasks t SET
  retention_body_bytes=CASE WHEN t.body='{}'::jsonb AND t.response IS NULL AND t.status='completed' THEN 0 ELSE public.batchforge_json_bytes(t.body) END,
  retention_response_limit=greatest(l.max_response_bytes,public.batchforge_json_bytes(t.response)),
  retention_error_limit=greatest(l.max_error_bytes,public.batchforge_json_bytes(t.error),
    coalesce((SELECT max(public.batchforge_json_bytes(a.error)) FROM public.attempts a WHERE a.task_id=t.id),0)),
  retention_response_bytes=CASE WHEN t.status IN ('pending','running') THEN greatest(l.max_response_bytes,public.batchforge_json_bytes(t.response)) ELSE public.batchforge_json_bytes(t.response) END,
  retention_error_bytes=CASE WHEN t.status IN ('pending','running') THEN
    (t.max_attempts::bigint+1)*greatest(l.max_error_bytes,public.batchforge_json_bytes(t.error),
      coalesce((SELECT max(public.batchforge_json_bytes(a.error)) FROM public.attempts a WHERE a.task_id=t.id),0))
      +coalesce((SELECT sum(public.batchforge_json_bytes(a.error)) FROM public.attempts a WHERE a.task_id=t.id),0)
    ELSE public.batchforge_json_bytes(t.error)+coalesce((SELECT sum(public.batchforge_json_bytes(a.error)) FROM public.attempts a WHERE a.task_id=t.id),0) END,
  retention_payload_cleared=(t.body='{}'::jsonb AND t.response IS NULL AND t.status='completed'),
  retention_errors_sealed=(t.status NOT IN ('pending','running'))
FROM public.tenant_retention_limits l WHERE l.tenant_id=t.tenant_id;
INSERT INTO public.tenant_retention_usage(tenant_id,retained_tasks,charged_payload_bytes)
SELECT n.id,count(t.id),coalesce(sum(t.retention_body_bytes+t.retention_response_bytes+t.retention_error_bytes),0)
FROM public.tenants n LEFT JOIN public.tasks t ON t.tenant_id=n.id GROUP BY n.id;

CREATE FUNCTION public.batchforge_retention_owner() RETURNS boolean LANGUAGE sql STABLE
SECURITY INVOKER SET search_path=pg_catalog,public AS $$
  SELECT pg_has_role(current_user,(SELECT relowner FROM pg_class WHERE oid='public.tenant_retention_limits'::regclass),'USAGE')
$$;
CREATE FUNCTION public.batchforge_retention_lock(p_tenant text) RETURNS void
LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog,public AS $$
BEGIN
  PERFORM id FROM public.tenants WHERE id=p_tenant FOR NO KEY UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='BF404',MESSAGE='tenant_not_found'; END IF;
  PERFORM tenant_id FROM public.tenant_retention_usage WHERE tenant_id=p_tenant FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='BR409',MESSAGE='retention_ledger_missing'; END IF;
END $$;
CREATE FUNCTION public.batchforge_retention_tenant() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog,public AS $$
BEGIN
  INSERT INTO public.tenant_retention_limits(tenant_id) VALUES(NEW.id);
  INSERT INTO public.tenant_retention_usage(tenant_id) VALUES(NEW.id);
  RETURN NEW;
END $$;
CREATE TRIGGER retention_tenant AFTER INSERT ON public.tenants
FOR EACH ROW EXECUTE FUNCTION public.batchforge_retention_tenant();

-- Counter writes come only from nested task/provisioning triggers. The runtime
-- receives UPDATE on usage, never policy UPDATE or an administrator capability.
CREATE FUNCTION public.batchforge_retention_usage_guard() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog,public AS $$
BEGIN
  IF pg_trigger_depth()<2 THEN RAISE EXCEPTION USING ERRCODE='BR403',MESSAGE='retention_counter_is_derived'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER retention_usage_guard BEFORE INSERT OR UPDATE OR DELETE ON public.tenant_retention_usage
FOR EACH ROW EXECUTE FUNCTION public.batchforge_retention_usage_guard();
CREATE FUNCTION public.batchforge_retention_policy_guard() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog,public AS $$
BEGIN
  IF NOT public.batchforge_retention_owner() THEN
    RAISE EXCEPTION USING ERRCODE='BR403',MESSAGE='retention_policy_operator_only';
  END IF;
  IF TG_OP='UPDATE' AND NEW.tenant_id IS DISTINCT FROM OLD.tenant_id THEN
    RAISE EXCEPTION USING ERRCODE='BR409',MESSAGE='retention_tenant_immutable';
  END IF;
  IF TG_OP='DELETE' THEN RAISE EXCEPTION USING ERRCODE='BR403',MESSAGE='retention_policy_delete_unsupported'; END IF;
  PERFORM id FROM public.tenants WHERE id=NEW.tenant_id FOR NO KEY UPDATE;
  RETURN NEW;
END $$;
CREATE TRIGGER retention_policy_guard BEFORE INSERT OR UPDATE OR DELETE ON public.tenant_retention_limits
FOR EACH ROW EXECUTE FUNCTION public.batchforge_retention_policy_guard();

CREATE FUNCTION public.batchforge_retention_task_insert() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog,public AS $$
DECLARE l public.tenant_retention_limits%ROWTYPE; u public.tenant_retention_usage%ROWTYPE; amount bigint;
BEGIN
  PERFORM public.batchforge_retention_lock(NEW.tenant_id);
  SELECT * INTO STRICT l FROM public.tenant_retention_limits WHERE tenant_id=NEW.tenant_id;
  SELECT * INTO STRICT u FROM public.tenant_retention_usage WHERE tenant_id=NEW.tenant_id;
  IF NEW.max_attempts NOT BETWEEN 1 AND 20 OR NEW.attempts NOT BETWEEN 0 AND NEW.max_attempts THEN
    RAISE EXCEPTION USING ERRCODE='BP413',MESSAGE='attempt_budget_invalid';
  END IF;
  NEW.retention_body_bytes:=public.batchforge_json_bytes(NEW.body);
  NEW.retention_response_limit:=l.max_response_bytes;
  NEW.retention_error_limit:=l.max_error_bytes;
  NEW.retention_response_bytes:=l.max_response_bytes;
  NEW.retention_error_bytes:=(NEW.max_attempts::bigint+1)*l.max_error_bytes;
  NEW.retention_payload_cleared:=false; NEW.retention_errors_sealed:=false;
  IF public.batchforge_json_bytes(NEW.response)>NEW.retention_response_limit
     OR public.batchforge_json_bytes(NEW.error)>NEW.retention_error_limit THEN
    RAISE EXCEPTION USING ERRCODE='BP413',MESSAGE='result_payload_too_large';
  END IF;
  amount:=NEW.retention_body_bytes+NEW.retention_response_bytes+NEW.retention_error_bytes;
  IF u.retained_tasks>=l.max_retained_tasks THEN
    RAISE EXCEPTION USING ERRCODE='BQ429',MESSAGE='retained_task_limit';
  END IF;
  IF amount>l.max_payload_bytes OR u.charged_payload_bytes>l.max_payload_bytes-amount THEN
    RAISE EXCEPTION USING ERRCODE='BQ429',MESSAGE='retained_payload_limit';
  END IF;
  RETURN NEW;
END $$;
CREATE FUNCTION public.batchforge_retention_task_charge() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog,public AS $$
BEGIN
  UPDATE public.tenant_retention_usage SET retained_tasks=retained_tasks+1,
    charged_payload_bytes=charged_payload_bytes+NEW.retention_body_bytes+NEW.retention_response_bytes+NEW.retention_error_bytes
    WHERE tenant_id=NEW.tenant_id;
  RETURN NEW;
END $$;
CREATE TRIGGER retention_task_insert BEFORE INSERT ON public.tasks
FOR EACH ROW EXECUTE FUNCTION public.batchforge_retention_task_insert();
CREATE TRIGGER retention_task_charge AFTER INSERT ON public.tasks
FOR EACH ROW EXECUTE FUNCTION public.batchforge_retention_task_charge();

CREATE FUNCTION public.batchforge_retention_task_update() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog,public AS $$
DECLARE actual_errors bigint; released bigint;
BEGIN
  IF (NEW.id,NEW.tenant_id,NEW.batch_id,NEW.max_attempts,
      NEW.retention_body_bytes,NEW.retention_response_bytes,NEW.retention_error_bytes,
      NEW.retention_response_limit,NEW.retention_error_limit,NEW.retention_payload_cleared,NEW.retention_errors_sealed)
     IS DISTINCT FROM
     (OLD.id,OLD.tenant_id,OLD.batch_id,OLD.max_attempts,
      OLD.retention_body_bytes,OLD.retention_response_bytes,OLD.retention_error_bytes,
      OLD.retention_response_limit,OLD.retention_error_limit,OLD.retention_payload_cleared,OLD.retention_errors_sealed) THEN
    RAISE EXCEPTION USING ERRCODE='BR409',MESSAGE='retention_accounting_immutable';
  END IF;
  IF NEW.attempts NOT BETWEEN 0 AND NEW.max_attempts THEN
    RAISE EXCEPTION USING ERRCODE='BP413',MESSAGE='attempt_budget_invalid';
  END IF;
  IF OLD.status NOT IN ('pending','running') AND NEW.status IS DISTINCT FROM OLD.status THEN
    RAISE EXCEPTION USING ERRCODE='BR409',MESSAGE='terminal_task_immutable';
  END IF;
  IF NEW.error IS DISTINCT FROM OLD.error AND (OLD.retention_errors_sealed
       OR public.batchforge_json_bytes(NEW.error)>OLD.retention_error_limit) THEN
    RAISE EXCEPTION USING ERRCODE='BP413',MESSAGE='task_error_payload_immutable_or_too_large';
  END IF;
  IF NEW.body IS NOT DISTINCT FROM OLD.body AND NEW.response IS NOT DISTINCT FROM OLD.response THEN RETURN NEW; END IF;
  IF OLD.retention_payload_cleared THEN
    RAISE EXCEPTION USING ERRCODE='BR409',MESSAGE='purged_payload_immutable';
  END IF;
  IF OLD.status='completed' AND NEW.status='completed' AND NEW.body='{}'::jsonb AND NEW.response IS NULL
     AND EXISTS(SELECT 1 FROM public.result_archive_runs r WHERE r.tenant_id=OLD.tenant_id AND r.batch_id=OLD.batch_id AND r.state='ready') THEN
    -- v8 purge already holds this tenant umbrella before batch/task. Every
    -- usage writer takes the same tenant first, so this late ledger lock cannot
    -- contend with another tenant-ledger writer. No completion takes this lock.
    PERFORM public.batchforge_retention_lock(OLD.tenant_id);
    SELECT public.batchforge_json_bytes(NEW.error)+coalesce(sum(public.batchforge_json_bytes(a.error)),0)
      INTO actual_errors FROM public.attempts a WHERE a.task_id=OLD.id;
    IF actual_errors>OLD.retention_error_bytes THEN
      RAISE EXCEPTION USING ERRCODE='BR409',MESSAGE='retention_error_budget_underestimated';
    END IF;
    released:=OLD.retention_body_bytes+OLD.retention_response_bytes+OLD.retention_error_bytes-actual_errors;
    UPDATE public.tenant_retention_usage SET charged_payload_bytes=charged_payload_bytes-released WHERE tenant_id=OLD.tenant_id;
    NEW.retention_body_bytes:=0; NEW.retention_response_bytes:=0; NEW.retention_error_bytes:=actual_errors;
    NEW.retention_payload_cleared:=true; NEW.retention_errors_sealed:=true;
    RETURN NEW;
  END IF;
  IF NEW.body IS DISTINCT FROM OLD.body OR OLD.status NOT IN ('pending','running') THEN
    RAISE EXCEPTION USING ERRCODE='BR409',MESSAGE='task_payload_immutable';
  END IF;
  IF public.batchforge_json_bytes(NEW.response)>OLD.retention_response_limit THEN
    RAISE EXCEPTION USING ERRCODE='BP413',MESSAGE='response_payload_too_large';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER retention_task_update BEFORE UPDATE OF id,tenant_id,batch_id,max_attempts,attempts,status,body,response,error,
  retention_body_bytes,retention_response_bytes,retention_error_bytes,retention_response_limit,retention_error_limit,
  retention_payload_cleared,retention_errors_sealed ON public.tasks
FOR EACH ROW EXECUTE FUNCTION public.batchforge_retention_task_update();

CREATE FUNCTION public.batchforge_retention_attempt_guard() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog,public AS $$
DECLARE t public.tasks%ROWTYPE;
BEGIN
  SELECT * INTO STRICT t FROM public.tasks WHERE id=NEW.task_id FOR UPDATE;
  IF TG_OP='UPDATE' AND (NEW.token,NEW.task_id,NEW.tenant_id,NEW.batch_id,NEW.number)
      IS DISTINCT FROM (OLD.token,OLD.task_id,OLD.tenant_id,OLD.batch_id,OLD.number) THEN
    RAISE EXCEPTION USING ERRCODE='BR409',MESSAGE='attempt_identity_immutable';
  END IF;
  IF NEW.tenant_id<>t.tenant_id OR NEW.batch_id<>t.batch_id OR NEW.number NOT BETWEEN 1 AND t.max_attempts THEN
    RAISE EXCEPTION USING ERRCODE='BP413',MESSAGE='attempt_budget_invalid';
  END IF;
  IF (TG_OP='INSERT' OR NEW.error IS DISTINCT FROM OLD.error) AND t.retention_errors_sealed THEN
    RAISE EXCEPTION USING ERRCODE='BR409',MESSAGE='retained_errors_sealed';
  END IF;
  IF public.batchforge_json_bytes(NEW.error)>t.retention_error_limit THEN
    RAISE EXCEPTION USING ERRCODE='BP413',MESSAGE='attempt_error_payload_too_large';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER retention_attempt_guard BEFORE INSERT OR UPDATE OF token,task_id,tenant_id,batch_id,number,error ON public.attempts
FOR EACH ROW EXECUTE FUNCTION public.batchforge_retention_attempt_guard();

-- This authorizes a deliberately operator-owned history cleanup transaction;
-- it does not delete data or repair batch/file/idempotency references for one.
CREATE FUNCTION public.batchforge_retention_authorize_delete(p_tenant text) RETURNS void
LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog,public AS $$
BEGIN
  IF NOT public.batchforge_retention_owner() THEN
    RAISE EXCEPTION USING ERRCODE='BR403',MESSAGE='retention_delete_operator_only';
  END IF;
  PERFORM public.batchforge_retention_lock(p_tenant);
  PERFORM set_config('batchforge.retention_delete_tenant',p_tenant,true);
END $$;
REVOKE ALL ON FUNCTION public.batchforge_retention_authorize_delete(text) FROM PUBLIC;
CREATE FUNCTION public.batchforge_retention_delete_guard() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog,public AS $$
DECLARE t public.tasks%ROWTYPE;
BEGIN
  IF NOT public.batchforge_retention_owner()
     OR current_setting('batchforge.retention_delete_tenant',true) IS DISTINCT FROM OLD.tenant_id THEN
    RAISE EXCEPTION USING ERRCODE='BR403',MESSAGE='retention_delete_requires_operator_transaction';
  END IF;
  IF TG_TABLE_NAME='tasks' THEN t:=OLD;
  ELSE SELECT * INTO STRICT t FROM public.tasks WHERE id=OLD.task_id FOR UPDATE; END IF;
  IF t.status IN ('pending','running') THEN
    RAISE EXCEPTION USING ERRCODE='BR409',MESSAGE='retention_delete_active_task';
  END IF;
  IF TG_TABLE_NAME='tasks' THEN
    UPDATE public.tenant_retention_usage SET retained_tasks=retained_tasks-1,
      charged_payload_bytes=charged_payload_bytes-OLD.retention_body_bytes-OLD.retention_response_bytes-OLD.retention_error_bytes
      WHERE tenant_id=OLD.tenant_id;
  END IF;
  RETURN OLD;
END $$;
CREATE TRIGGER retention_task_delete BEFORE DELETE ON public.tasks
FOR EACH ROW EXECUTE FUNCTION public.batchforge_retention_delete_guard();
CREATE TRIGGER retention_attempt_delete BEFORE DELETE ON public.attempts
FOR EACH ROW EXECUTE FUNCTION public.batchforge_retention_delete_guard();
