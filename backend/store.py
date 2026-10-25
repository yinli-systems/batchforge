"""PostgreSQL state machine. All mutations lock tenant queue, then batch, then task.

This lock order makes completion, cancellation and lease expiry mutually exclusive.
The attempt token is a fencing token, not a request authentication credential.
"""
import hashlib
import json
import logging
import os
import secrets
from datetime import datetime, timezone

import psycopg
from psycopg.types.json import Jsonb

from .validation import body_hash, validate_rows, validate_usage
from .scheduling import estimate_work, profile_from_metadata
from .blob_storage import BlobCatalog, LocalBlobStore, S3BlobStore, BlobExpired
from .result_archival import ResultArchiver
from .retention import guard_retention_errors
from .admission_metrics import admission_stage, observed_connection, observed_transaction


class APIError(Exception):
    def __init__(self, status: int, message: str, code: str = "invalid_request"):
        self.status, self.message, self.code = status, message, code


def identifier(prefix: str) -> str:
    return f"{prefix}_{secrets.token_hex(16)}"


def unix(value):
    return int(value.timestamp()) if value else None


def batch_view(row):
    result = {
        "id": row["id"], "object": "batch", "endpoint": row["endpoint"], "model": row["model"],
        "input_file_id": row["input_file_id"], "completion_window": row["completion_window"],
        "status": row["status"], "metadata": row["metadata"],
        **{key: unix(row[key]) for key in ("created_at", "in_progress_at", "completed_at", "failed_at", "cancelled_at")},
        "output_file_id": row["output_file_id"], "error_file_id": row["error_file_id"],
        "request_counts": {key: row[f"{key}_count"] for key in ("total", "completed", "failed", "cancelled", "running", "pending")},
        "usage": {"prompt_tokens": row["prompt_tokens"], "completion_tokens": row["completion_tokens"]},
    }
    if row.get('history_retirement_started_at') is not None:
        result['history_retention'] = {
            'state': 'retired' if row.get('history_retired_at') is not None else 'retiring',
            'retired_requests': row['history_retired_tasks'],
            'started_at': unix(row['history_retirement_started_at']),
            'retired_at': unix(row.get('history_retired_at')),
        }
    return result


def worker_view(row):
    """Expose liveness and bounded readiness counts, never worker metadata."""
    result = {key: row[key] for key in ("id", "name", "model", "concurrency", "status")}
    result["last_seen_at"] = row["last_seen_at"].isoformat()
    metadata = row.get("metadata")
    metadata = metadata if isinstance(metadata, dict) else {}
    for key in ("engine_count", "ready_engine_count"):
        value = metadata.get(key)
        result[key] = value if type(value) is int and 0 <= value <= 4096 else None
    return result


def file_view(row):
    return {"id": row["id"], "object": "file", "bytes": row["bytes"],
            "created_at": unix(row["created_at"]), "filename": row["filename"],
            "purpose": "batch", "status": "processed"}


async def one(conn, sql, params=()):
    return await (await conn.execute(sql, params)).fetchone()


class Store:
    def __init__(self, pool, settings, *, schema_version=None):
        self.pool, self.settings = pool, settings
        self.schema_version = schema_version or settings.minimum_schema_version()
        self.blobs = None
        self.results = None
        catalogue=getattr(settings,'quality_evidence_catalogue','')
        self.quality_evidence=None
        if catalogue:
            from .evaluations import EvidenceCatalogue
            self.quality_evidence=EvidenceCatalogue(catalogue)
        if settings.blob_storage_backend != 'inline':
            if settings.blob_storage_backend == 'local':
                adapter = LocalBlobStore(settings.blob_storage_root)
            else:
                adapter = S3BlobStore.from_credentials(
                    endpoint_url=os.environ['AWS_ENDPOINT_URL_S3'], region=os.environ['AWS_REGION'],
                    bucket=settings.blob_storage_bucket, access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                    secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
            self.blobs = BlobCatalog(pool, adapter,
                max_stored_input_bytes_per_tenant=settings.max_stored_input_bytes_per_tenant,
                max_queued_tasks_per_tenant=settings.max_queued_tasks_per_tenant,
                upload_reservation_seconds=settings.upload_reservation_seconds,
                input_retention_seconds=settings.input_retention_seconds)
            if settings.result_readers_enabled:
                self.results = ResultArchiver(pool, adapter,
                    max_archived_bytes_per_tenant=settings.max_archived_bytes_per_tenant,
                    purge_rows=settings.result_archive_purge_rows)

    async def tenant(self, key: str):
        digest = hashlib.sha256(key.encode()).hexdigest()
        async with self.pool.connection() as conn:
            row = await one(conn, "SELECT tenant_id FROM api_keys WHERE key_hash=%s AND NOT revoked", (digest,))
        if not row:
            raise APIError(401, "Invalid API key", "invalid_api_key")
        return row["tenant_id"]

    async def upload(self, tenant, filename, content):
        rows = validate_rows(content, self.settings)
        if self.blobs:
            return file_view(await self.blobs.put_input(tenant, filename[:255], content, len(rows)))
        async with self.pool.connection() as conn:
            row = await one(conn, "INSERT INTO files(id,tenant_id,filename,kind,content,bytes) VALUES(%s,%s,%s,'input',%s,%s) RETURNING id,bytes,created_at,filename", (identifier("file"), tenant, filename[:255], content, len(content)))
        return file_view(row)

    async def idempotency(self, conn, tenant, scope, key, body):
        if key is None:
            return None
        if not 1 <= len(key) <= 128:
            raise APIError(400, "Idempotency-Key must contain 1 to 128 characters")
        digest = body_hash(body)
        # New-key accounting and conflict/replay resolution use the same tenant
        # umbrella as task/file admission, including during the v8-compatible roll.
        with admission_stage('idempotency_tenant_lock'):
            await conn.execute('SELECT id FROM tenants WHERE id=%s FOR NO KEY UPDATE', (tenant,))
        if self.schema_version >= 11:
            await conn.execute('SELECT public.batchforge_idempotency_admit(%s,%s,%s)', (tenant, scope, key))
        await conn.execute("INSERT INTO idempotency(tenant_id,scope,key,body_hash) VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING", (tenant, scope, key, digest))
        row = await one(conn, "SELECT body_hash,response FROM idempotency WHERE tenant_id=%s AND scope=%s AND key=%s FOR UPDATE", (tenant, scope, key))
        if row["body_hash"] != digest:
            raise APIError(409, "Idempotency key was used with a different request", "idempotency_conflict")
        return row["response"]

    async def remember(self, conn, tenant, scope, key, response):
        if key:
            await conn.execute("UPDATE idempotency SET response=%s WHERE tenant_id=%s AND scope=%s AND key=%s", (Jsonb(response), tenant, scope, key))

    async def committed_response(self, tenant, scope, key, body):
        """Fast replay before external I/O; the write transaction rechecks it."""
        if key is None:
            return None
        if not 1 <= len(key) <= 128:
            raise APIError(400, 'Idempotency-Key must contain 1 to 128 characters')
        async with observed_connection(self.pool) as conn:
            if self.schema_version >= 11:
                await conn.execute('SELECT public.batchforge_idempotency_check(%s,%s,%s)', (tenant, scope, key))
            row = await one(conn, 'SELECT body_hash,response FROM idempotency WHERE tenant_id=%s AND scope=%s AND key=%s', (tenant, scope, key))
        if row and row['body_hash'] != body_hash(body):
            raise APIError(409, 'Idempotency key was used with a different request', 'idempotency_conflict')
        return row['response'] if row else None

    @guard_retention_errors(APIError)
    async def create_rows(self, conn, tenant, input_file_id, rows, metadata, runtime_source=None):
        model = rows[0]["body"]["model"] if rows else None
        if model not in self.settings.served_models() or any(task["body"]["model"] != model for task in rows):
            raise APIError(400, "Batch must use one configured model")
        # Tenant row serializes admission control, preventing concurrent submissions
        # from overshooting the configured queue bound. NO KEY UPDATE stays
        # compatible with KEY SHARE locks acquired by tenant foreign keys
        # (including the preceding idempotency insert).
        with admission_stage('tenant_lock'):
            await conn.execute("SELECT id FROM tenants WHERE id=%s FOR NO KEY UPDATE", (tenant,))
        if self.schema_version >= 9:
            # Limits/counters are database-owned. An API environment flag cannot
            # disable the installed triggers or widen another replica's policy.
            with admission_stage('retention_lock'):
                await conn.execute('SELECT public.batchforge_retention_lock(%s)', (tenant,))
        with admission_stage('queue_precheck'):
            queued = await one(conn, "SELECT COALESCE(sum(pending_count+running_count),0) AS n FROM batches WHERE tenant_id=%s AND status='in_progress'", (tenant,))
        if queued["n"] + len(rows) > self.settings.max_queued_tasks_per_tenant:
            raise APIError(429, "Tenant queue limit reached", "queue_limit")
        with admission_stage('queue_insert'):
            await conn.execute("INSERT INTO tenant_queues(tenant_id,model) VALUES(%s,%s) ON CONFLICT DO NOTHING", (tenant, model))
        with admission_stage('queue_lock'):
            await conn.execute("SELECT tenant_id FROM tenant_queues WHERE tenant_id=%s AND model=%s FOR UPDATE", (tenant, model))
        with admission_stage('queue_update'):
            await conn.execute("UPDATE tenant_queues SET pending_count=pending_count+%s,next_ready_at=LEAST(next_ready_at,clock_timestamp()) WHERE tenant_id=%s AND model=%s", (len(rows), tenant, model))
        bid = identifier("batch")
        with admission_stage('batch_insert'):
            row = await one(conn, "INSERT INTO batches(id,tenant_id,input_file_id,model,status,metadata,total_count,pending_count) VALUES(%s,%s,%s,%s,'in_progress',%s,%s,%s) RETURNING *", (bid, tenant, input_file_id, model, Jsonb(metadata), len(rows), len(rows)))
            if self.schema_version >= 6 and runtime_source is not None:
                await conn.execute('UPDATE batches SET runtime_fingerprint=%s,runtime_compatibility_scope=%s WHERE id=%s',
                    (runtime_source.get('runtime_fingerprint'), runtime_source.get('runtime_compatibility_scope', 'legacy_unverified'), bid))
        with admission_stage('task_insert'):
            async with conn.cursor() as cursor:
                if self.settings.scheduler_policy == 'legacy':
                    await cursor.executemany("INSERT INTO tasks(id,tenant_id,batch_id,custom_id,model,body,status,max_attempts) VALUES(%s,%s,%s,%s,%s,%s,'pending',%s)", [(identifier("task"), tenant, bid, task["custom_id"], model, Jsonb(task["body"]), self.settings.max_attempts) for task in rows])
                else:
                    values = []
                    for task in rows:
                        work = estimate_work(task['body'])
                        values.append((identifier('task'), tenant, bid, task['custom_id'], model,
                            Jsonb(task['body']), self.settings.max_attempts, work['estimated_input_tokens'],
                            work['estimated_output_tokens'], work['estimate_source']))
                    await cursor.executemany("INSERT INTO tasks(id,tenant_id,batch_id,custom_id,model,body,status,max_attempts,estimated_input_tokens,estimated_output_tokens,estimate_source) VALUES(%s,%s,%s,%s,%s,%s,'pending',%s,%s,%s,%s)", values)
        return batch_view(row)

    async def create(self, tenant, body, key):
        with admission_stage('total'):
            scope = "/v1/batches"
            external_rows = None
            if self.blobs:
                with admission_stage('idempotency_precheck'):
                    previous = await self.committed_response(tenant, scope, key, body)
                if previous is not None:
                    return previous
                # Network object I/O must not hold an idempotency lock or pool slot.
                with admission_stage('input_read_validate'):
                    external_rows = validate_rows(await self.blobs.read_input(tenant, body['input_file_id']), self.settings)
            async with observed_connection(self.pool) as conn, observed_transaction(conn):
                with admission_stage('idempotency'):
                    previous = await self.idempotency(conn, tenant, scope, key, body)
                if previous is not None:
                    return previous
                with admission_stage('input_lookup'):
                    lookup = ("SELECT id FROM files WHERE id=%s AND tenant_id=%s AND kind='input'"
                              if external_rows is not None else
                              "SELECT id,content FROM files WHERE id=%s AND tenant_id=%s AND kind='input'")
                    file = await one(conn, lookup, (body["input_file_id"], tenant))
                if not file:
                    raise APIError(404, "Input file not found", "not_found")
                rows = external_rows
                if rows is None:
                    with admission_stage('inline_validation'):
                        rows = validate_rows(bytes(file['content']), self.settings)
                if self.blobs:
                    with admission_stage('input_admission'):
                        await self.blobs.admit_batch(conn, tenant, file['id'], len(rows))
                result = await self.create_rows(conn, tenant, file["id"], rows, body.get("metadata", {}))
                with admission_stage('idempotency_remember'):
                    await self.remember(conn, tenant, scope, key, result)
                return result

    async def get_batch(self, tenant, bid):
        async with self.pool.connection() as conn:
            row = await one(conn, "SELECT * FROM batches WHERE id=%s AND tenant_id=%s", (bid, tenant))
        if not row:
            raise APIError(404, "Batch not found", "not_found")
        return batch_view(row)

    async def evaluation_views(self, tenant, *, details=False):
        if not self.quality_evidence:return []
        if self.schema_version<6:raise APIError(409,'Quality evidence linkage requires schema 6 or later','feature_unavailable')
        from .evaluations import LEDGER_SQL
        ids=sorted({bid for r in self.quality_evidence.records for bid in r.batch_ids})
        async with self.pool.connection() as conn:
            rows=await (await conn.execute(LEDGER_SQL,(tenant,ids))).fetchall()
        return self.quality_evidence.visible(rows,details=details)

    async def list_evaluations(self, tenant):
        return {'object':'list','data':await self.evaluation_views(tenant)}

    async def get_evaluation(self, tenant, eid):
        views=await self.evaluation_views(tenant,details=True)
        for view in views:
            if view['id']==eid:return view
        raise APIError(404,'Quality evaluation not found','not_found')

    async def capacity(self, tenant):
        from .capacity import CAPACITY_SQL,capacity_view
        if self.schema_version<9:
            raise APIError(409,'Admission headroom requires schema 9 or later','feature_unavailable')
        async with self.pool.connection() as conn:
            row=await one(conn,CAPACITY_SQL,(tenant,))
        if not row:raise APIError(409,'Admission counters are unavailable','feature_unavailable')
        return capacity_view(row,self.settings)

    async def batch_receipt(self, tenant, bid):
        from .receipts import RECEIPT_SQL,format_receipt
        if self.schema_version<8:
            raise APIError(409,'Execution receipts require schema 8 or later','feature_unavailable')
        async with self.pool.connection() as conn:
            row=await one(conn,RECEIPT_SQL,(bid,tenant))
        if not row:raise APIError(404,'Batch not found','not_found')
        return format_receipt(row)

    async def task_page(self, tenant, bid, limit=50, after=None):
        from .receipts import TASK_PAGE_SQL
        if self.schema_version<8:
            raise APIError(409,'Task ledger pages require schema 8 or later','feature_unavailable')
        if type(limit) is not int or not 1<=limit<=200:
            raise APIError(400,'Task page limit must be between 1 and 200')
        async with self.pool.connection() as conn:
            row=await one(conn,TASK_PAGE_SQL,(bid,tenant,after,after,limit+1))
        if not row:raise APIError(404,'Batch not found','not_found')
        if after and not row['cursor_found']:raise APIError(404,'Task cursor not found in this batch','not_found')
        rows=row['data'];data=rows[:limit]
        return {'object':'list','data':data,'has_more':len(rows)>limit,
                'last_id':data[-1]['id'] if data else None,'retired_tasks':row['retired_tasks']}

    async def list_batches(self, tenant, limit, after=None):
        async with self.pool.connection() as conn:
            if after:
                anchor = await one(conn, "SELECT created_at,id FROM batches WHERE id=%s AND tenant_id=%s", (after, tenant))
                if not anchor:
                    raise APIError(404, "Pagination cursor not found", "not_found")
                cursor = await conn.execute("SELECT * FROM batches WHERE tenant_id=%s AND (created_at,id)<(%s,%s) ORDER BY created_at DESC,id DESC LIMIT %s", (tenant, anchor["created_at"], after, limit + 1))
            else:
                cursor = await conn.execute("SELECT * FROM batches WHERE tenant_id=%s ORDER BY created_at DESC,id DESC LIMIT %s", (tenant, limit + 1))
            rows = await cursor.fetchall()
        data = [batch_view(row) for row in rows[:limit]]
        return {"object": "list", "data": data, "has_more": len(rows) > limit, "last_id": data[-1]["id"] if data else None}

    async def lock_queue(self, conn, tenant, model, skip=False):
        suffix = " SKIP LOCKED" if skip else ""
        return await one(conn, "SELECT * FROM tenant_queues WHERE tenant_id=%s AND model=%s FOR UPDATE" + suffix, (tenant, model))

    async def refresh_queue(self, conn, tenant, model, delta):
        await conn.execute("UPDATE tenant_queues SET pending_count=pending_count+%s,next_ready_at=COALESCE((SELECT ready_at FROM tasks WHERE tenant_id=%s AND model=%s AND status='pending' ORDER BY ready_at,id LIMIT 1),clock_timestamp()) WHERE tenant_id=%s AND model=%s", (delta, tenant, model, tenant, model))

    async def finish_batch(self, conn, bid):
        row = await one(conn, "SELECT * FROM batches WHERE id=%s", (bid,))
        if row["pending_count"] or row["running_count"]:
            return
        status = row["status"]
        if status == "in_progress":
            status = "failed" if row["completed_count"] == 0 and row["failed_count"] else "completed"
        output_id, error_id = row["output_file_id"], row["error_file_id"]
        if row["completed_count"] and not output_id:
            output_id = identifier("file")
            await conn.execute("INSERT INTO files(id,tenant_id,filename,kind,batch_id) VALUES(%s,%s,%s,'output',%s)", (output_id, row["tenant_id"], f"{bid}-output.jsonl", bid))
        if (row["failed_count"] or row["cancelled_count"]) and not error_id:
            error_id = identifier("file")
            await conn.execute("INSERT INTO files(id,tenant_id,filename,kind,batch_id) VALUES(%s,%s,%s,'error',%s)", (error_id, row["tenant_id"], f"{bid}-errors.jsonl", bid))
        await conn.execute("UPDATE batches SET status=%s,output_file_id=%s,error_file_id=%s,completed_at=CASE WHEN %s='completed' THEN COALESCE(completed_at,now()) ELSE completed_at END,failed_at=CASE WHEN %s='failed' THEN COALESCE(failed_at,now()) ELSE failed_at END WHERE id=%s", (status, output_id, error_id, status, status, bid))

    async def cancel_locked(self, conn, batch, code="batch_cancelled", message="Batch was cancelled", status="cancelled"):
        bid = batch["id"]
        # Batch lock fences claims and reports before task rows are modified.
        await conn.execute("UPDATE attempts SET status='cancelled',finished_at=clock_timestamp(),error=%s WHERE batch_id=%s AND status='running'", (Jsonb({"code": code, "message": message}), bid))
        await conn.execute("UPDATE tasks SET status='cancelled',error=%s,finished_at=clock_timestamp(),lease_expires_at=NULL WHERE batch_id=%s AND status IN ('pending','running')", (Jsonb({"code": code, "message": message}), bid))
        await conn.execute("UPDATE batches SET status=%s,cancelled_count=cancelled_count+pending_count+running_count,pending_count=0,running_count=0,cancelled_at=CASE WHEN %s='cancelled' THEN clock_timestamp() ELSE cancelled_at END,failed_at=CASE WHEN %s='failed' THEN clock_timestamp() ELSE failed_at END WHERE id=%s", (status, status, status, bid))
        await self.refresh_queue(conn, batch["tenant_id"], batch["model"], -batch["pending_count"])
        await self.finish_batch(conn, bid)

    async def cancel(self, tenant, bid):
        async with self.pool.connection() as conn, conn.transaction():
            # A rolling model change does not alter historical batch identity.
            # Lock its actual queue before the batch, even if this API now serves
            # another model. Using current MODEL_NAME violates the lock order.
            source = await one(conn, "SELECT model FROM batches WHERE id=%s AND tenant_id=%s", (bid, tenant))
            if not source:
                raise APIError(404, "Batch not found", "not_found")
            await self.lock_queue(conn, tenant, source["model"])
            row = await one(conn, "SELECT * FROM batches WHERE id=%s AND tenant_id=%s FOR UPDATE", (bid, tenant))
            if not row:
                raise APIError(404, "Batch not found", "not_found")
            if row["status"] == "in_progress":
                await self.cancel_locked(conn, row)
            return batch_view(await one(conn, "SELECT * FROM batches WHERE id=%s", (bid,)))

    async def retry(self, tenant, bid, key):
        scope = f"/v1/batches/{bid}/retry"
        if self.blobs:
            previous = await self.committed_response(tenant, scope, key, {})
            if previous is not None:
                return previous
            async with self.pool.connection() as conn:
                source = await one(conn, "SELECT * FROM batches WHERE id=%s AND tenant_id=%s", (bid, tenant))
                if not source:
                    raise APIError(404, 'Batch not found', 'not_found')
                if source.get('history_retirement_started_at') is not None:
                    raise APIError(410, 'Task history has expired; archived results remain available', 'history_retired')
                if source['status'] == 'in_progress':
                    raise APIError(409, 'Wait until source batch is terminal before retrying', 'batch_not_terminal')
                if source['model'] not in self.settings.served_models():
                    raise APIError(409, 'Source model is not currently served', 'model_unavailable')
                rows = await (await conn.execute("SELECT custom_id,body FROM tasks WHERE batch_id=%s AND status='failed' ORDER BY id", (bid,))).fetchall()
            if not rows:
                raise APIError(409, 'Source batch has no failed tasks', 'no_failed_tasks')
            content = b''.join((json.dumps({'custom_id': r['custom_id'], 'method': 'POST',
                'url': '/v1/chat/completions', 'body': r['body']}, ensure_ascii=False)+'\n').encode() for r in rows)
            file = await self.blobs.put_input(tenant, f'{bid}-retry.jsonl', content, len(rows))
            submitted = False
            try:
                async with self.pool.connection() as conn, conn.transaction():
                    previous = await self.idempotency(conn, tenant, scope, key, {})
                    if previous is not None:
                        result = previous
                    else:
                        # Owner retirement uses this same tenant umbrella. The
                        # source was read before external upload; recheck after
                        # taking the lock so a concurrent retirement cannot
                        # produce a partial retry from a stale task snapshot.
                        await conn.execute('SELECT id FROM tenants WHERE id=%s FOR NO KEY UPDATE', (tenant,))
                        current = await one(conn, 'SELECT * FROM batches WHERE id=%s AND tenant_id=%s', (bid, tenant))
                        if current is None:
                            raise APIError(404, 'Batch not found', 'not_found')
                        if current.get('history_retirement_started_at') is not None:
                            raise APIError(410, 'Task history has expired; archived results remain available', 'history_retired')
                        await self.blobs.admit_batch(conn, tenant, file['id'], len(rows))
                        result = await self.create_rows(conn, tenant, file['id'], rows, {'retry_of': bid}, runtime_source=source)
                        await self.remember(conn, tenant, scope, key, result)
                submitted = result['input_file_id'] == file['id']
                return result
            finally:
                # Only unsubmitted staging files qualify. An uncertain COMMIT
                # is safe: a committed batch reference prevents cleanup.
                if not submitted:
                    try:
                        await self.blobs.discard_unsubmitted(tenant, file['id'])
                    except Exception as exc:
                        # Bounded retention cleans up later; cleanup must not
                        # hide a committed idempotent response or original error.
                        logging.getLogger('batchforge').warning('Staged input cleanup unavailable: %s', type(exc).__name__)
        async with self.pool.connection() as conn, conn.transaction():
            await conn.execute('SELECT id FROM tenants WHERE id=%s FOR NO KEY UPDATE', (tenant,))
            previous = await self.idempotency(conn, tenant, scope, key, {})
            if previous is not None:
                return previous
            source = await one(conn, "SELECT * FROM batches WHERE id=%s AND tenant_id=%s", (bid, tenant))
            if not source:
                raise APIError(404, "Batch not found", "not_found")
            if source.get('history_retirement_started_at') is not None:
                raise APIError(410, 'Task history has expired; archived results remain available', 'history_retired')
            if source["status"] == "in_progress":
                raise APIError(409, "Wait until source batch is terminal before retrying", "batch_not_terminal")
            if source["model"] not in self.settings.served_models():
                raise APIError(409, "Source model is not currently served; submit a new batch using an available model", "model_unavailable")
            rows = await (await conn.execute("SELECT custom_id,body FROM tasks WHERE batch_id=%s AND status='failed' ORDER BY id", (bid,))).fetchall()
            if not rows:
                raise APIError(409, "Source batch has no failed tasks", "no_failed_tasks")
            content = b"".join((json.dumps({"custom_id": row["custom_id"], "method": "POST", "url": "/v1/chat/completions", "body": row["body"]}, ensure_ascii=False) + "\n").encode() for row in rows)
            fid = identifier("file")
            await conn.execute("INSERT INTO files(id,tenant_id,filename,kind,content,bytes) VALUES(%s,%s,%s,'input',%s,%s)", (fid, tenant, f"{bid}-retry.jsonl", content, len(content)))
            result = await self.create_rows(conn, tenant, fid, rows, {"retry_of": bid}, runtime_source=source)
            await self.remember(conn, tenant, scope, key, result)
            return result

    async def file(self, tenant, fid):
        async with self.pool.connection() as conn:
            row = await one(conn, "SELECT * FROM files WHERE id=%s AND tenant_id=%s", (fid, tenant))
        if not row:
            raise APIError(404, "File not found", "not_found")
        if row.get('storage_deleted_at'):
            raise BlobExpired('Input retention expired')
        return row

    async def file_chunks(self, tenant, file):
        if file["kind"] == "input":
            if self.blobs:
                async with self.blobs.open_input(tenant, file['id']) as chunks:
                    async for chunk in chunks:
                        yield chunk
                return
            data = bytes(file["content"])
            for offset in range(0, len(data), 65536):
                yield data[offset:offset + 65536]
            return
        if self.results:
            async with self.results.open_file(tenant, file['id']) as chunks:
                async for chunk in chunks:
                    yield chunk
            return
        # Keyset pages avoid tying a DB connection to a slow HTTP consumer.
        last = ""
        states = ["completed"] if file["kind"] == "output" else ["failed", "cancelled"]
        while True:
            async with self.pool.connection() as conn:
                rows = await (await conn.execute("SELECT id,custom_id,response,error FROM tasks WHERE batch_id=%s AND tenant_id=%s AND status=ANY(%s) AND id>%s ORDER BY id LIMIT 100", (file["batch_id"], tenant, states, last))).fetchall()
            if not rows:
                break
            for row in rows:
                result = {"id": row["id"], "custom_id": row["custom_id"], "response": {"status_code": 200, "request_id": row["id"], "body": row["response"]} if row["response"] is not None else None, "error": row["error"]}
                yield (json.dumps(result, ensure_ascii=False, separators=(",", ":")) + "\n").encode()
            last = rows[-1]["id"]

    async def register(self, body):
        worker_id = body.get("worker_id") or identifier("worker")
        try:
            profile = profile_from_metadata(body.get('metadata', {}))
        except ValueError as exc:
            raise APIError(400, str(exc), 'invalid_scheduler_profile') from None
        if self.settings.scheduler_policy == 'cost_aware' and profile is None:
            raise APIError(400, 'Cost-aware workers require an operator scheduling profile', 'scheduler_profile_required')
        async with self.pool.connection() as conn:
            async with conn.transaction():
                await conn.execute("INSERT INTO workers(id,name,model,concurrency,metadata) VALUES(%s,%s,%s,%s,%s) ON CONFLICT(id) DO UPDATE SET name=excluded.name,model=excluded.model,concurrency=excluded.concurrency,metadata=excluded.metadata,last_seen_at=clock_timestamp()", (worker_id, body["name"], body["model"], body["concurrency"], Jsonb(body.get("metadata", {}))))
                if self.schema_version >= 6:
                    profile = profile or {}
                    await conn.execute("UPDATE workers SET scheduler_profile_id=%s,runtime_fingerprint=%s,prefill_tokens_per_second=%s,decode_tokens_per_second=%s,hourly_cost_usd=%s WHERE id=%s",
                        tuple(profile.get(key) for key in ('profile_id','runtime_fingerprint','prefill_tokens_per_second','decode_tokens_per_second','hourly_cost_usd')) + (worker_id,))
        return {"worker_id": worker_id, "heartbeat_interval_seconds": max(1, self.settings.lease_seconds // 3), "lease_seconds": self.settings.lease_seconds}

    async def claim(self, worker_id, model, limit):
        if self.settings.scheduler_policy != 'legacy':
            return await self.sql_attempt('claim_scheduled', (worker_id, model, limit,
                self.settings.lease_seconds, self.settings.scheduler_policy,
                self.settings.scheduler_aging_seconds, self.settings.scheduler_worker_freshness_seconds))
        if self.settings.sql_claim_enabled:
            return await self.sql_attempt("claim", (worker_id, model, limit, self.settings.lease_seconds))
        async with self.pool.connection() as conn, conn.transaction():
            worker = await one(conn, "SELECT * FROM workers WHERE id=%s FOR UPDATE", (worker_id,))
            if not worker or worker["model"] != model:
                raise APIError(409, "Register this worker/model before claiming", "worker_not_registered")
            await conn.execute("UPDATE workers SET last_seen_at=clock_timestamp() WHERE id=%s", (worker_id,))
            active = await one(conn, "SELECT count(*) AS n FROM tasks WHERE worker_id=%s AND status='running' AND lease_expires_at>clock_timestamp()", (worker_id,))
            capacity = min(limit, max(0, worker["concurrency"] - active["n"]))
            if not capacity:
                return {"tasks": [], "lease_seconds": self.settings.lease_seconds}
            # Oldest dispatched ready tenant gets the next bounded claim. Durable
            # queue rows prevent one large upload from starving another tenant.
            queues = await (await conn.execute("SELECT * FROM tenant_queues WHERE model=%s AND pending_count>0 AND next_ready_at<=clock_timestamp() ORDER BY last_dispatched_at,tenant_id LIMIT 8 FOR UPDATE SKIP LOCKED", (model,))).fetchall()
            batch, queue = None, None
            for candidate in queues:
                source = await one(conn, "SELECT batch_id FROM tasks WHERE tenant_id=%s AND model=%s AND status='pending' AND ready_at<=clock_timestamp() ORDER BY ready_at,id LIMIT 1", (candidate["tenant_id"], model))
                if not source:
                    continue
                batch = await one(conn, "SELECT * FROM batches WHERE id=%s AND status='in_progress' AND expires_at>clock_timestamp() FOR UPDATE SKIP LOCKED", (source["batch_id"],))
                if batch:
                    queue = candidate
                    break
            if not batch:
                return {"tasks": [], "lease_seconds": self.settings.lease_seconds}
            rows = await (await conn.execute("SELECT * FROM tasks WHERE batch_id=%s AND status='pending' AND ready_at<=clock_timestamp() ORDER BY ready_at,id LIMIT %s FOR UPDATE SKIP LOCKED", (batch["id"], capacity))).fetchall()
            result = []
            for task in rows:
                token = identifier("attempt")
                updated = await one(conn, "UPDATE tasks SET status='running',attempts=attempts+1,attempt_token=%s,worker_id=%s,lease_expires_at=clock_timestamp()+(%s*interval '1 second'),error=NULL WHERE id=%s RETURNING *", (token, worker_id, self.settings.lease_seconds, task["id"]))
                await conn.execute("INSERT INTO attempts(token,task_id,batch_id,tenant_id,worker_id,number,status) VALUES(%s,%s,%s,%s,%s,%s,'running')", (token, task["id"], batch["id"], task["tenant_id"], worker_id, updated["attempts"]))
                result.append({"id": task["id"], "batch_id": batch["id"], "custom_id": task["custom_id"], "body": task["body"], "attempt_token": token, "lease_expires_at": updated["lease_expires_at"].isoformat(), "attempt_number": updated["attempts"]})
            await conn.execute("UPDATE batches SET pending_count=pending_count-%s,running_count=running_count+%s WHERE id=%s", (len(result), len(result), batch["id"]))
            await self.refresh_queue(conn, queue["tenant_id"], model, -len(result))
            await conn.execute("UPDATE tenant_queues SET last_dispatched_at=clock_timestamp() WHERE tenant_id=%s AND model=%s", (queue["tenant_id"], model))
            return {"tasks": result, "lease_seconds": self.settings.lease_seconds}

    async def locked_task(self, conn, tid):
        source = await one(conn, "SELECT batch_id,tenant_id,model FROM tasks WHERE id=%s", (tid,))
        if not source:
            raise APIError(404, "Task not found", "not_found")
        await self.lock_queue(conn, source["tenant_id"], source["model"])
        batch = await one(conn, "SELECT id,status,expires_at FROM batches WHERE id=%s FOR UPDATE", (source["batch_id"],))
        task = await one(conn, "SELECT *,lease_expires_at>clock_timestamp() AS lease_valid FROM tasks WHERE id=%s FOR UPDATE", (tid,))
        clock = await one(conn, "SELECT clock_timestamp() AS current_time")
        task["batch_valid"] = batch["status"] == "in_progress" and batch["expires_at"] > clock["current_time"]
        return task

    def fence(self, task, worker_id, token):
        if not task["batch_valid"] or task["status"] != "running" or task["worker_id"] != worker_id or task["attempt_token"] != token or not task["lease_valid"]:
            raise APIError(409, "Lease expired, cancelled, or superseded; discard this result", "stale_attempt")

    async def heartbeat(self, tid, worker_id, token):
        if self.settings.sql_completion_enabled:
            return await self.sql_attempt("heartbeat", (tid, worker_id, token, self.settings.lease_seconds))
        async with self.pool.connection() as conn, conn.transaction():
            await conn.execute("UPDATE workers SET last_seen_at=clock_timestamp() WHERE id=%s", (worker_id,))
            task = await self.locked_task(conn, tid)
            self.fence(task, worker_id, token)
            row = await one(conn, "UPDATE tasks SET lease_expires_at=clock_timestamp()+(%s*interval '1 second') WHERE id=%s RETURNING lease_expires_at", (self.settings.lease_seconds, tid))
            await conn.execute("UPDATE workers SET last_seen_at=clock_timestamp() WHERE id=%s", (worker_id,))
            return {"ok": True, "lease_expires_at": row["lease_expires_at"].isoformat()}

    @guard_retention_errors(APIError)
    async def complete(self, tid, worker_id, token, response):
        prompt, completion = validate_usage(response)
        if self.settings.sql_completion_enabled:
            return await self.sql_attempt("complete", (tid, worker_id, token, Jsonb(response), prompt, completion))
        async with self.pool.connection() as conn, conn.transaction():
            await conn.execute("UPDATE workers SET last_seen_at=clock_timestamp() WHERE id=%s", (worker_id,))
            task = await self.locked_task(conn, tid)
            if task["status"] == "completed" and task["attempt_token"] == token and task["worker_id"] == worker_id:
                return {"ok": True, "duplicate": True}
            self.fence(task, worker_id, token)
            await conn.execute("UPDATE tasks SET status='completed',response=%s,error=NULL,prompt_tokens=%s,completion_tokens=%s,finished_at=clock_timestamp(),lease_expires_at=NULL WHERE id=%s", (Jsonb(response), prompt, completion, tid))
            await conn.execute("UPDATE attempts SET status='completed',prompt_tokens=%s,completion_tokens=%s,finished_at=clock_timestamp() WHERE token=%s", (prompt, completion, token))
            await conn.execute("UPDATE batches SET running_count=running_count-1,completed_count=completed_count+1,prompt_tokens=prompt_tokens+%s,completion_tokens=completion_tokens+%s WHERE id=%s", (prompt, completion, task["batch_id"]))
            await conn.execute("UPDATE workers SET last_seen_at=clock_timestamp() WHERE id=%s", (worker_id,))
            await self.finish_batch(conn, task["batch_id"])
            return {"ok": True}

    async def sql_attempt(self, operation, values):
        """One implicit statement transaction; SQL owns all locks and fencing.

        Do not wrap this in conn.transaction(): that adds client BEGIN/COMMIT
        round trips. Pool connections use autocommit and a SELECT function call
        is itself a PostgreSQL atomic transaction.
        """
        statements = {
            "heartbeat": "SELECT public.batchforge_heartbeat(%s,%s,%s,%s) AS result",
            "complete": "SELECT public.batchforge_complete(%s,%s,%s,%s,%s,%s) AS result",
            "claim": "SELECT public.batchforge_claim(%s,%s,%s,%s) AS result",
            "claim_scheduled": "SELECT public.batchforge_claim_scheduled(%s,%s,%s,%s,%s,%s,%s) AS result",
            "fail": "SELECT public.batchforge_fail(%s,%s,%s,%s,%s,%s) AS result",
            "recover": "SELECT public.batchforge_recover(%s,%s,%s) AS result",
        }
        try:
            async with self.pool.connection() as conn:
                row = await one(conn, statements[operation], values)
                return row["result"]
        except psycopg.Error as exc:
            if exc.sqlstate == "BF404":
                raise APIError(404, "Task not found", "not_found") from None
            if exc.sqlstate == "BF409":
                raise APIError(409, "Lease expired, cancelled, or superseded; discard this result", "stale_attempt") from None
            if exc.sqlstate == "BF410":
                raise APIError(409, "Register this worker/model before claiming", "worker_not_registered") from None
            if exc.sqlstate == "BF400":
                raise APIError(400, "Invalid worker result or lease configuration") from None
            raise

    async def failure_locked(self, conn, task, error, retryable, expired=False):
        retry = retryable and task["attempts"] < task["max_attempts"]
        target = "pending" if retry else "failed"
        delay = min(300, self.settings.retry_base_seconds * 2 ** max(0, task["attempts"] - 1))
        await conn.execute("UPDATE tasks SET status=%s,error=%s,lease_expires_at=NULL,ready_at=clock_timestamp()+(%s*interval '1 second'),finished_at=CASE WHEN %s THEN NULL ELSE clock_timestamp() END WHERE id=%s", (target, Jsonb(error), delay, retry, task["id"]))
        await conn.execute("UPDATE attempts SET status=%s,error=%s,finished_at=clock_timestamp() WHERE token=%s", (("expired" if expired else "retrying") if retry else "failed", Jsonb(error), task["attempt_token"]))
        await conn.execute("UPDATE batches SET running_count=running_count-1,pending_count=pending_count+%s,failed_count=failed_count+%s WHERE id=%s", (int(retry), int(not retry), task["batch_id"]))
        await self.refresh_queue(conn, task["tenant_id"], task["model"], int(retry))
        await self.finish_batch(conn, task["batch_id"])
        return target

    @guard_retention_errors(APIError)
    async def fail(self, tid, worker_id, token, error, retryable):
        if self.settings.sql_recovery_enabled:
            return await self.sql_attempt('fail', (tid, worker_id, token, Jsonb(error), retryable, self.settings.retry_base_seconds))
        async with self.pool.connection() as conn, conn.transaction():
            await conn.execute("UPDATE workers SET last_seen_at=clock_timestamp() WHERE id=%s", (worker_id,))
            task = await self.locked_task(conn, tid)
            self.fence(task, worker_id, token)
            target = await self.failure_locked(conn, task, error, retryable)
            await conn.execute("UPDATE workers SET last_seen_at=clock_timestamp() WHERE id=%s", (worker_id,))
            return {"ok": True, "status": target}

    async def recover(self):
        """Bounded, indexed sweep; safe in every API replica."""
        if self.settings.sql_recovery_enabled:
            result = await self.sql_attempt('recover', (self.settings.recovery_task_limit,
                self.settings.recovery_batch_limit, self.settings.retry_base_seconds))
            return result['recovered']
        recovered = 0
        async with self.pool.connection() as conn, conn.transaction():
            expired_batches = await (await conn.execute("SELECT id,tenant_id,model FROM batches WHERE status='in_progress' AND expires_at<=clock_timestamp() ORDER BY expires_at LIMIT 8")).fetchall()
            for source in expired_batches:
                if not await self.lock_queue(conn, source["tenant_id"], source["model"], skip=True):
                    continue
                batch = await one(conn, "SELECT * FROM batches WHERE id=%s AND status='in_progress' FOR UPDATE SKIP LOCKED", (source["id"],))
                if batch:
                    await self.cancel_locked(conn, batch, "batch_expired", "24-hour completion window expired", "failed")
            candidates = await (await conn.execute("SELECT batch_id,tenant_id,model FROM tasks WHERE status='running' AND lease_expires_at<=clock_timestamp() ORDER BY lease_expires_at,id LIMIT 100")).fetchall()
            seen = set()
            for source in candidates:
                bid = source["batch_id"]
                if bid in seen:
                    continue
                seen.add(bid)
                if not await self.lock_queue(conn, source["tenant_id"], source["model"], skip=True):
                    continue
                batch = await one(conn, "SELECT id FROM batches WHERE id=%s AND status='in_progress' FOR UPDATE SKIP LOCKED", (bid,))
                if not batch:
                    continue
                tasks = await (await conn.execute("SELECT * FROM tasks WHERE batch_id=%s AND status='running' AND lease_expires_at<=clock_timestamp() ORDER BY lease_expires_at LIMIT 100 FOR UPDATE SKIP LOCKED", (bid,))).fetchall()
                for task in tasks:
                    await self.failure_locked(conn, task, {"code": "lease_expired", "message": "Worker lease expired"}, True, expired=True)
                    recovered += 1
        return recovered

    async def fleet(self, tenant):
        from .fleet import fleet_view
        async with self.pool.connection() as conn:
            rows=await (await conn.execute("""SELECT id,model,
                CASE WHEN octet_length((metadata->'gpu_inventory')::text)<=65536 THEN metadata->'gpu_inventory' END AS inventory,
                CASE WHEN last_seen_at>clock_timestamp()-(%s*interval '1 second') THEN 'online' ELSE 'offline' END AS status
                FROM workers WHERE model=ANY(%s) ORDER BY last_seen_at DESC,id LIMIT 100""",
                (self.settings.lease_seconds*2,list(self.settings.served_models())))).fetchall()
        return fleet_view(rows,datetime.now(timezone.utc))

    async def overview(self, tenant):
        async with self.pool.connection() as conn:
            stats = await one(conn, "SELECT count(*) AS total_batches,COALESCE(sum(total_count),0) AS total_tasks,COALESCE(sum(completed_count),0) AS completed_tasks,COALESCE(sum(failed_count),0) AS failed_tasks,COALESCE(sum(cancelled_count),0) AS cancelled_tasks,COALESCE(sum(pending_count),0) AS queued_tasks,COALESCE(sum(running_count),0) AS running_tasks,COALESCE(sum(prompt_tokens),0) AS prompt_tokens,COALESCE(sum(completion_tokens),0) AS completion_tokens FROM batches WHERE tenant_id=%s", (tenant,))
            if self.schema_version >= 11:
                # Both live and compacted history use complete UTC hour
                # buckets, including the current hour and preceding 23 hours.
                buckets = await (await conn.execute("SELECT timestamp,completed,failed,tokens FROM batchforge_attempt_hourly WHERE tenant_id=%s AND timestamp>=date_trunc('hour',now(),'UTC')-interval '23 hours' ORDER BY timestamp", (tenant,))).fetchall()
            else:
                buckets = await (await conn.execute("SELECT date_trunc('hour',finished_at) AS timestamp,count(*) FILTER(WHERE status='completed') AS completed,count(*) FILTER(WHERE status='failed') AS failed,COALESCE(sum(prompt_tokens+completion_tokens) FILTER(WHERE status='completed'),0) AS tokens FROM attempts WHERE tenant_id=%s AND finished_at>=now()-interval '24 hours' GROUP BY 1 ORDER BY 1", (tenant,))).fetchall()
            workers = await (await conn.execute("SELECT id,name,model,concurrency,jsonb_build_object('engine_count',metadata->'engine_count','ready_engine_count',metadata->'ready_engine_count') AS metadata,last_seen_at,CASE WHEN last_seen_at>clock_timestamp()-(%s*interval '1 second') THEN 'online' ELSE 'offline' END AS status FROM workers ORDER BY last_seen_at DESC LIMIT 100", (self.settings.lease_seconds * 2,))).fetchall()
            batches = await (await conn.execute("SELECT * FROM batches WHERE tenant_id=%s ORDER BY created_at DESC,id DESC LIMIT 10", (tenant,))).fetchall()
        return {"stats": {key: int(value) for key, value in stats.items()}, "throughput": [{**row, "timestamp": row["timestamp"].isoformat(), "tokens": int(row["tokens"])} for row in buckets], "workers": [worker_view(row) for row in workers], "recent_batches": [batch_view(row) for row in batches]}
