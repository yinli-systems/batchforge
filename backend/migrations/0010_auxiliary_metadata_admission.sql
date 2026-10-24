-- Immutable v10 candidate. No history, fence, key or object is deleted here.
-- New metadata INSERTs are bounded. Output/error finalization never touches
-- the auxiliary ledger. DELETE callers must prelock the tenant before rows.
-- Operator write-quiescence gate. Never wait while holding a partial set of
-- relation gates: an existing writer may own batches while awaiting files, or
-- hold a tenant tuple before its first metadata INSERT. Retry the whole
-- migration transaction later if any NOWAIT acquisition is refused.
LOCK TABLE public.tenants IN EXCLUSIVE MODE NOWAIT;
LOCK TABLE public.batches,public.tasks,public.attempts,
 files,idempotency,blob_upload_reservations,blob_objects,blob_read_leases,
 result_archive_runs,result_archive_files,result_archive_chunks,result_read_leases
 IN SHARE ROW EXCLUSIVE MODE NOWAIT;

CREATE TABLE public.tenant_auxiliary_limits (
 tenant_id text NOT NULL REFERENCES public.tenants(id),
 resource text NOT NULL CHECK(resource IN ('input_files','upload_reservations','idempotency',
  'blob_generations','archive_runs','archive_chunks','input_read_leases','result_read_leases')),
 max_rows bigint NOT NULL CHECK(max_rows BETWEEN 1 AND 1000000000),
 max_bytes bigint NOT NULL CHECK(max_bytes BETWEEN 0 AND 1099511627776),
 max_row_bytes bigint NOT NULL CHECK(max_row_bytes BETWEEN 1 AND 1073741824),
 PRIMARY KEY(tenant_id,resource)
);
CREATE TABLE public.tenant_auxiliary_usage (
 tenant_id text NOT NULL,
 resource text NOT NULL,
 rows bigint NOT NULL DEFAULT 0 CHECK(rows>=0),
 bytes bigint NOT NULL DEFAULT 0 CHECK(bytes>=0),
 PRIMARY KEY(tenant_id,resource),
 FOREIGN KEY(tenant_id,resource) REFERENCES public.tenant_auxiliary_limits(tenant_id,resource)
);
CREATE FUNCTION public.batchforge_auxiliary_admin_guard() RETURNS trigger
LANGUAGE plpgsql SECURITY INVOKER SET search_path=pg_catalog,public AS $$
BEGIN
 IF current_user<>(SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid=TG_RELID) THEN
  RAISE EXCEPTION 'Auxiliary limits and counters are owner-managed' USING ERRCODE='BR403';
 END IF;
 IF TG_OP='TRUNCATE' THEN RETURN NULL;
 ELSIF TG_OP='DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
END $$;
CREATE TRIGGER auxiliary_owner_only BEFORE INSERT OR UPDATE OR DELETE ON public.tenant_auxiliary_limits
 FOR EACH ROW EXECUTE FUNCTION public.batchforge_auxiliary_admin_guard();
CREATE TRIGGER auxiliary_owner_only BEFORE INSERT OR UPDATE OR DELETE ON public.tenant_auxiliary_usage
 FOR EACH ROW EXECUTE FUNCTION public.batchforge_auxiliary_admin_guard();
CREATE TRIGGER auxiliary_owner_only_truncate BEFORE TRUNCATE ON public.tenant_auxiliary_limits
 FOR EACH STATEMENT EXECUTE FUNCTION public.batchforge_auxiliary_admin_guard();
CREATE TRIGGER auxiliary_owner_only_truncate BEFORE TRUNCATE ON public.tenant_auxiliary_usage
 FOR EACH STATEMENT EXECUTE FUNCTION public.batchforge_auxiliary_admin_guard();
ALTER TABLE public.idempotency ADD COLUMN auxiliary_reserved_bytes bigint NOT NULL DEFAULT 65536;
UPDATE public.idempotency SET auxiliary_reserved_bytes=GREATEST(65536,
 octet_length(COALESCE(response::text,'')));

CREATE FUNCTION public.batchforge_auxiliary_lock(p_tenant text) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
BEGIN
 PERFORM id FROM public.tenants WHERE id=p_tenant FOR NO KEY UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION 'Tenant not found' USING ERRCODE='BF404'; END IF;
 INSERT INTO public.tenant_auxiliary_limits(tenant_id,resource,max_rows,max_bytes,max_row_bytes)
 SELECT p_tenant,v.* FROM (VALUES
  ('input_files',10000::bigint,0::bigint,16777216::bigint),
  ('upload_reservations',32,0,8192),('idempotency',10000,268435456,65536),
  ('blob_generations',20000,0,8192),('archive_runs',10000,0,8192),
  ('archive_chunks',100000,0,8192),('input_read_leases',128,0,4096),
  ('result_read_leases',128,0,4096)) AS v(resource,max_rows,max_bytes,max_row_bytes)
 ON CONFLICT DO NOTHING;
 INSERT INTO public.tenant_auxiliary_usage(tenant_id,resource)
 SELECT tenant_id,resource FROM public.tenant_auxiliary_limits WHERE tenant_id=p_tenant
 ON CONFLICT DO NOTHING;
 -- Fixed order also supports an owner reconciliation transaction.
 PERFORM resource FROM public.tenant_auxiliary_usage WHERE tenant_id=p_tenant ORDER BY resource FOR UPDATE;
END $$;

CREATE FUNCTION public.batchforge_auxiliary_adjust(p_tenant text,p_resource text,p_rows bigint,p_bytes bigint)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
DECLARE u public.tenant_auxiliary_usage; l public.tenant_auxiliary_limits;
BEGIN
 PERFORM public.batchforge_auxiliary_lock(p_tenant);
 SELECT * INTO STRICT u FROM public.tenant_auxiliary_usage WHERE tenant_id=p_tenant AND resource=p_resource;
 SELECT * INTO STRICT l FROM public.tenant_auxiliary_limits WHERE tenant_id=p_tenant AND resource=p_resource;
 IF (p_rows>0 OR p_bytes>0) AND
    (u.rows+p_rows>l.max_rows OR (l.max_bytes>0 AND u.bytes+p_bytes>l.max_bytes)) THEN
  RAISE EXCEPTION 'Auxiliary metadata quota reached: %',p_resource USING ERRCODE='BQ429',DETAIL=p_resource;
 END IF;
 UPDATE public.tenant_auxiliary_usage SET rows=rows+p_rows,bytes=bytes+p_bytes
 WHERE tenant_id=p_tenant AND resource=p_resource;
END $$;

CREATE FUNCTION public.batchforge_auxiliary_guard() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
DECLARE r text; maxrow bigint; j jsonb; oldj jsonb; mutable text[]; field text; ceiling integer;
BEGIN
 IF TG_TABLE_NAME='files' THEN
  IF TG_OP='UPDATE' AND (NEW.tenant_id,NEW.id,NEW.kind,NEW.batch_id) IS DISTINCT FROM
      (OLD.tenant_id,OLD.id,OLD.kind,OLD.batch_id) THEN
   RAISE EXCEPTION 'Immutable file identity' USING ERRCODE='BR409';
  END IF;
  IF NEW.kind<>'input' THEN RETURN NEW; END IF;
  IF TG_OP='UPDATE' THEN
   IF (NEW.filename,NEW.bytes,NEW.created_at,NEW.content) IS DISTINCT FROM
       (OLD.filename,OLD.bytes,OLD.created_at,OLD.content) THEN
    RAISE EXCEPTION 'Immutable input file payload' USING ERRCODE='BR409';
   END IF;
   IF NEW.blob_id IS DISTINCT FROM OLD.blob_id AND octet_length(COALESCE(NEW.blob_id,''))>128 THEN
    RAISE EXCEPTION 'Blob reference exceeds byte ceiling' USING ERRCODE='BP413';
   END IF;
   RETURN NEW;
  END IF;
  -- Never turn a legacy BYTEA into a second, hex-expanded JSON payload.
  j=jsonb_build_object('id',NEW.id,'tenant_id',NEW.tenant_id,'filename',NEW.filename,
    'kind',NEW.kind,'bytes',NEW.bytes,'batch_id',NEW.batch_id,'created_at',NEW.created_at,
    'blob_id',NEW.blob_id,'storage_expires_at',NEW.storage_expires_at,
    'storage_deleted_at',NEW.storage_deleted_at,'admission_tasks',NEW.admission_tasks);
  r='input_files'; mutable=ARRAY['blob_id','storage_expires_at','storage_deleted_at','admission_tasks'];
 ELSE
  j=to_jsonb(NEW);
  IF TG_TABLE_NAME='idempotency' THEN r='idempotency'; mutable=ARRAY['response'];
 ELSIF TG_TABLE_NAME='blob_upload_reservations' THEN r='upload_reservations'; mutable=ARRAY['blob_id','expires_at'];
 ELSIF TG_TABLE_NAME='blob_objects' THEN r='blob_generations'; mutable=ARRAY['state','write_token','write_expires_at','unreferenced_at','gc_after','updated_at'];
 ELSIF TG_TABLE_NAME='result_archive_runs' THEN r='archive_runs'; mutable=ARRAY['state','writer_token','lease_expires_at','reserved_bytes','bytes','manifest_sha256','purge_started_at','purge_cursor','rows_purged','purged_at','gc_after','ready_at'];
 ELSIF TG_TABLE_NAME='result_archive_chunks' THEN r='archive_chunks'; mutable=ARRAY['state','gc_after'];
 ELSIF TG_TABLE_NAME='blob_read_leases' THEN r='input_read_leases'; mutable=ARRAY['expires_at'];
 ELSIF TG_TABLE_NAME='result_read_leases' THEN r='result_read_leases'; mutable=ARRAY['expires_at'];
  ELSE RAISE EXCEPTION 'Unknown auxiliary resource'; END IF;
 END IF;
 IF TG_OP='UPDATE' THEN
  oldj=to_jsonb(OLD);
  IF (j-mutable) IS DISTINCT FROM (oldj-mutable) THEN
   RAISE EXCEPTION 'Immutable auxiliary identity or reservation' USING ERRCODE='BR409';
  END IF;
  -- Renewals/state transitions allocate no credit and acquire no ledger lock.
  IF r='idempotency' THEN
   IF NEW.response IS DISTINCT FROM OLD.response AND
      octet_length(COALESCE(NEW.response::text,''))>OLD.auxiliary_reserved_bytes THEN
    RAISE EXCEPTION 'Idempotency response exceeds reserved bytes' USING ERRCODE='BP413';
   END IF;
  END IF;
  FOREACH field IN ARRAY ARRAY['write_token','writer_token','purge_cursor','manifest_sha256','blob_id'] LOOP
   ceiling=CASE WHEN field='manifest_sha256' THEN 64 ELSE 128 END;
   IF j->field IS DISTINCT FROM oldj->field AND octet_length(COALESCE(j->>field,''))>ceiling THEN
    RAISE EXCEPTION 'Auxiliary mutable field exceeds byte ceiling' USING ERRCODE='BP413';
   END IF;
  END LOOP;
  RETURN NEW;
 END IF;
 -- BEFORE INSERT, before unique-conflict resolution: lock but do not charge.
 -- ON CONFLICT DO NOTHING replay never fires the INSERT accounting trigger.
 PERFORM public.batchforge_auxiliary_lock(NEW.tenant_id);
 SELECT max_row_bytes INTO STRICT maxrow FROM public.tenant_auxiliary_limits
  WHERE tenant_id=NEW.tenant_id AND resource=r;
 IF r='input_files' THEN
  IF COALESCE(octet_length(NEW.content),0)>maxrow OR NEW.bytes>maxrow OR
     octet_length(NEW.filename)>1024 OR octet_length((j-'content')::text)>8192 THEN
   RAISE EXCEPTION 'Input file exceeds metadata or content byte ceiling' USING ERRCODE='BP413';
  END IF;
 ELSIF r='idempotency' THEN
  NEW.auxiliary_reserved_bytes=maxrow;
  IF octet_length((j-ARRAY['response','auxiliary_reserved_bytes'])::text)>2048 OR
     octet_length(COALESCE(NEW.response::text,''))>maxrow THEN
   RAISE EXCEPTION 'Idempotency row exceeds reserved byte ceiling' USING ERRCODE='BP413';
  END IF;
 ELSE
  IF octet_length(j::text)>maxrow THEN
   RAISE EXCEPTION 'Auxiliary row exceeds byte ceiling' USING ERRCODE='BP413';
  END IF;
  IF r='upload_reservations' THEN
   IF NEW.bytes>(SELECT max_row_bytes FROM public.tenant_auxiliary_limits
       WHERE tenant_id=NEW.tenant_id AND resource='input_files') THEN
    RAISE EXCEPTION 'Reserved input exceeds file ceiling' USING ERRCODE='BP413';
   END IF;
  END IF;
 END IF;
 RETURN NEW;
END $$;

CREATE FUNCTION public.batchforge_auxiliary_account() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
DECLARE r text; n bigint; b bigint=0; t text; rowdata jsonb;
BEGIN
 IF TG_TABLE_NAME='files' THEN
  rowdata=CASE WHEN TG_OP='DELETE' THEN jsonb_build_object('tenant_id',OLD.tenant_id,'kind',OLD.kind)
   ELSE jsonb_build_object('tenant_id',NEW.tenant_id,'kind',NEW.kind) END;
 ELSE
  rowdata=CASE WHEN TG_OP='DELETE' THEN to_jsonb(OLD) ELSE to_jsonb(NEW) END;
 END IF;
 t=rowdata->>'tenant_id'; n=CASE WHEN TG_OP='DELETE' THEN -1 ELSE 1 END;
 IF TG_TABLE_NAME='files' THEN
  IF rowdata->>'kind'<>'input' THEN RETURN NULL; END IF; r='input_files';
 ELSIF TG_TABLE_NAME='idempotency' THEN r='idempotency';b=(rowdata->>'auxiliary_reserved_bytes')::bigint*n;
 ELSIF TG_TABLE_NAME='blob_upload_reservations' THEN
  PERFORM public.batchforge_auxiliary_adjust(t,'input_files',n,0);r='upload_reservations';
 ELSIF TG_TABLE_NAME='blob_objects' THEN r='blob_generations';
 ELSIF TG_TABLE_NAME='result_archive_runs' THEN r='archive_runs';
 ELSIF TG_TABLE_NAME='result_archive_chunks' THEN r='archive_chunks';
 ELSIF TG_TABLE_NAME='blob_read_leases' THEN r='input_read_leases';
 ELSIF TG_TABLE_NAME='result_read_leases' THEN r='result_read_leases';
 ELSE RAISE EXCEPTION 'Unknown auxiliary resource'; END IF;
 PERFORM public.batchforge_auxiliary_adjust(t,r,n,b);
 RETURN NULL;
END $$;

-- Derivational bounds: no admission ledger or quota lock on result finalization.
CREATE UNIQUE INDEX files_one_terminal_kind_per_batch ON public.files(tenant_id,batch_id,kind)
 WHERE kind IN ('output','error');
ALTER TABLE public.files ADD CONSTRAINT files_result_batch_identity
 FOREIGN KEY(tenant_id,batch_id) REFERENCES public.batches(tenant_id,id)
 DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE public.files ADD CONSTRAINT files_result_has_batch
 CHECK(kind='input' OR batch_id IS NOT NULL) NOT VALID;
CREATE UNIQUE INDEX archive_files_one_kind ON public.result_archive_files(archive_id,kind);

CREATE FUNCTION public.batchforge_archive_metadata_identity() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,public AS $$
DECLARE expected integer;
BEGIN
 IF TG_TABLE_NAME='result_archive_files' THEN
  IF NOT EXISTS(SELECT 1 FROM public.result_archive_runs r JOIN public.files f
    ON f.tenant_id=r.tenant_id AND f.batch_id=r.batch_id
    WHERE r.id=NEW.archive_id AND r.tenant_id=NEW.tenant_id AND f.id=NEW.file_id AND f.kind=NEW.kind) THEN
   RAISE EXCEPTION 'Archive file batch identity differs' USING ERRCODE='BR409';
  END IF;
  IF TG_OP='UPDATE' AND (NEW.tenant_id,NEW.archive_id,NEW.file_id,NEW.kind,NEW.expected_count)
      IS DISTINCT FROM (OLD.tenant_id,OLD.archive_id,OLD.file_id,OLD.kind,OLD.expected_count) THEN
   RAISE EXCEPTION 'Immutable archive file identity' USING ERRCODE='BR409';
  END IF;
 ELSE
  SELECT expected_count INTO expected FROM public.result_archive_files
   WHERE tenant_id=NEW.tenant_id AND archive_id=NEW.archive_id AND file_id=NEW.file_id;
  IF expected IS NULL OR NEW.ordinal>=expected THEN
   RAISE EXCEPTION 'Archive chunk ordinal exceeds result count' USING ERRCODE='BP413';
  END IF;
 END IF;
 RETURN NEW;
END $$;

-- Bootstrap with writers blocked. Existing over-limit histories remain valid.
DO $$ DECLARE t text; BEGIN
 FOR t IN SELECT id FROM public.tenants ORDER BY id LOOP
  -- Metadata write gates are already held. Do not acquire the hot-path
  -- NO KEY UPDATE tenant tuple here: a writer may hold it while awaiting files.
  -- FK KEY SHARE acquired by these inserts is compatible with that umbrella.
  INSERT INTO public.tenant_auxiliary_limits(tenant_id,resource,max_rows,max_bytes,max_row_bytes)
  SELECT t,v.* FROM (VALUES
   ('input_files',10000::bigint,0::bigint,16777216::bigint),
   ('upload_reservations',32,0,8192),('idempotency',10000,268435456,65536),
   ('blob_generations',20000,0,8192),('archive_runs',10000,0,8192),
   ('archive_chunks',100000,0,8192),('input_read_leases',128,0,4096),
   ('result_read_leases',128,0,4096)) AS v(resource,max_rows,max_bytes,max_row_bytes)
  ON CONFLICT DO NOTHING;
  INSERT INTO public.tenant_auxiliary_usage(tenant_id,resource)
  SELECT tenant_id,resource FROM public.tenant_auxiliary_limits WHERE tenant_id=t
  ON CONFLICT DO NOTHING;
  UPDATE public.tenant_auxiliary_usage u SET rows=v.n,bytes=v.b FROM (
   SELECT 'input_files'::text AS resource,(SELECT count(*) FROM public.files WHERE tenant_id=t AND kind='input')+
    (SELECT count(*) FROM public.blob_upload_reservations WHERE tenant_id=t) AS n,0::bigint AS b
   UNION ALL SELECT 'upload_reservations',count(*),0 FROM public.blob_upload_reservations WHERE tenant_id=t
   UNION ALL SELECT 'idempotency',count(*),COALESCE(sum(auxiliary_reserved_bytes),0)::bigint FROM public.idempotency WHERE tenant_id=t
   UNION ALL SELECT 'blob_generations',count(*),0 FROM public.blob_objects WHERE tenant_id=t
   UNION ALL SELECT 'archive_runs',count(*),0 FROM public.result_archive_runs WHERE tenant_id=t
   UNION ALL SELECT 'archive_chunks',count(*),0 FROM public.result_archive_chunks WHERE tenant_id=t
   UNION ALL SELECT 'input_read_leases',count(*),0 FROM public.blob_read_leases WHERE tenant_id=t
   UNION ALL SELECT 'result_read_leases',count(*),0 FROM public.result_read_leases WHERE tenant_id=t
  ) v WHERE u.tenant_id=t AND u.resource=v.resource;
 END LOOP;
END $$;
DO $$ DECLARE t text; BEGIN
 FOREACH t IN ARRAY ARRAY['files','idempotency','blob_upload_reservations','blob_objects',
   'blob_read_leases','result_archive_runs','result_archive_chunks','result_read_leases'] LOOP
  EXECUTE format('CREATE TRIGGER auxiliary_guard BEFORE INSERT OR UPDATE ON public.%I FOR EACH ROW EXECUTE FUNCTION public.batchforge_auxiliary_guard()',t);
  EXECUTE format('CREATE TRIGGER auxiliary_account AFTER INSERT OR DELETE ON public.%I FOR EACH ROW EXECUTE FUNCTION public.batchforge_auxiliary_account()',t);
 END LOOP;
END $$;
CREATE TRIGGER archive_file_identity BEFORE INSERT OR UPDATE ON public.result_archive_files
 FOR EACH ROW EXECUTE FUNCTION public.batchforge_archive_metadata_identity();
CREATE TRIGGER archive_chunk_identity BEFORE INSERT ON public.result_archive_chunks
 FOR EACH ROW EXECUTE FUNCTION public.batchforge_archive_metadata_identity();
-- Historical violations abort the whole migration for operator repair; do not
-- leave an unvalidated constraint while claiming restore validation passed.
ALTER TABLE public.files VALIDATE CONSTRAINT files_result_has_batch;
REVOKE ALL ON public.tenant_auxiliary_limits,public.tenant_auxiliary_usage FROM PUBLIC;
REVOKE ALL ON FUNCTION public.batchforge_auxiliary_lock(text),
 public.batchforge_auxiliary_adjust(text,text,bigint,bigint),public.batchforge_auxiliary_guard(),
 public.batchforge_auxiliary_account(),public.batchforge_archive_metadata_identity(),
 public.batchforge_auxiliary_admin_guard() FROM PUBLIC;
