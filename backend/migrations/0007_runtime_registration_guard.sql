-- Additive v7. Preserve immutable v6 bytes. A legacy API may still register a
-- worker or dispatch an unbound batch during a rolling release. Those paths
-- must not leave a stale typed profile or falsely certify numerical identity.

-- Repair drift already left by an old registration path before this migration.
-- JSONB numeric equality avoids a text-format comparison of calibrated rates.
UPDATE public.workers SET scheduler_profile_id=NULL,runtime_fingerprint=NULL,
  prefill_tokens_per_second=NULL,decode_tokens_per_second=NULL,hourly_cost_usd=NULL
WHERE scheduler_profile_id IS NOT NULL AND (
  metadata#>>'{scheduling_profile,profile_id}' IS DISTINCT FROM scheduler_profile_id
  OR metadata#>>'{scheduling_profile,runtime_fingerprint}' IS DISTINCT FROM runtime_fingerprint
  OR metadata#>'{scheduling_profile,prefill_tokens_per_second}' IS DISTINCT FROM to_jsonb(prefill_tokens_per_second)
  OR metadata#>'{scheduling_profile,decode_tokens_per_second}' IS DISTINCT FROM to_jsonb(decode_tokens_per_second)
  OR metadata#>'{scheduling_profile,hourly_cost_usd}' IS DISTINCT FROM to_jsonb(hourly_cost_usd));

-- Claims before this guard which did not record task identity remain explicitly
-- unverified, even if another attempt later pinned the batch to a known runtime.
UPDATE public.batches b SET runtime_compatibility_scope='legacy_unverified'
WHERE b.runtime_compatibility_scope='first_dispatch'
  AND EXISTS(SELECT 1 FROM public.tasks t WHERE t.batch_id=b.id
    AND t.attempts>0 AND t.runtime_fingerprint IS NULL);

CREATE FUNCTION public.batchforge_clear_changed_worker_profile() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog,public AS $worker$
BEGIN
  IF NEW.model IS DISTINCT FROM OLD.model
    OR NEW.metadata->'scheduling_profile' IS DISTINCT FROM OLD.metadata->'scheduling_profile' THEN
    -- The current Store repopulates validated fields later in the same
    -- registration transaction. Old binaries stop here and fail closed.
    NEW.scheduler_profile_id := NULL;
    NEW.runtime_fingerprint := NULL;
    NEW.prefill_tokens_per_second := NULL;
    NEW.decode_tokens_per_second := NULL;
    NEW.hourly_cost_usd := NULL;
  END IF;
  RETURN NEW;
END
$worker$;
CREATE TRIGGER batchforge_clear_changed_worker_profile
BEFORE UPDATE OF metadata,model ON public.workers
FOR EACH ROW EXECUTE FUNCTION public.batchforge_clear_changed_worker_profile();

CREATE OR REPLACE FUNCTION public.batchforge_guard_task_runtime() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog,public AS $guard$
DECLARE v_fingerprint text; v_batch public.batches%ROWTYPE;
BEGIN
  IF OLD.runtime_fingerprint IS NOT NULL AND NEW.runtime_fingerprint IS DISTINCT FROM OLD.runtime_fingerprint THEN
    RAISE EXCEPTION USING ERRCODE='BF409', MESSAGE='runtime_fingerprint_immutable';
  END IF;
  IF NEW.status='running' AND (OLD.status IS DISTINCT FROM 'running'
    OR NEW.worker_id IS DISTINCT FROM OLD.worker_id OR NEW.attempt_token IS DISTINCT FROM OLD.attempt_token) THEN
    -- All supported SQL/Python claim paths already hold worker -> queue -> batch
    -- -> task locks. The UPDATE below reuses that batch lock; it acquires no new
    -- worker or queue lock from inside a task trigger.
    SELECT runtime_fingerprint INTO v_fingerprint FROM public.workers WHERE id=NEW.worker_id;
    IF NOT FOUND THEN
      RAISE EXCEPTION USING ERRCODE='BF409', MESSAGE='runtime_worker_missing';
    END IF;
    SELECT * INTO v_batch FROM public.batches WHERE id=NEW.batch_id;
    IF (NEW.runtime_fingerprint IS NOT NULL AND NEW.runtime_fingerprint IS DISTINCT FROM v_fingerprint)
      OR (v_batch.runtime_fingerprint IS NOT NULL AND v_batch.runtime_fingerprint IS DISTINCT FROM v_fingerprint) THEN
      RAISE EXCEPTION USING ERRCODE='BF409', MESSAGE='runtime_fingerprint_mismatch';
    END IF;
    IF v_batch.runtime_fingerprint IS NULL AND (v_fingerprint IS NOT NULL
      OR v_batch.runtime_compatibility_scope<>'legacy_unverified') THEN
      UPDATE public.batches SET
        runtime_fingerprint=v_fingerprint,
        runtime_compatibility_scope=CASE WHEN v_fingerprint IS NULL
          THEN 'legacy_unverified' ELSE runtime_compatibility_scope END
        WHERE id=NEW.batch_id;
    END IF;
    NEW.runtime_fingerprint := coalesce(NEW.runtime_fingerprint,v_fingerprint);
  END IF;
  RETURN NEW;
END
$guard$;
