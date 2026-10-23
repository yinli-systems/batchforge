"""PostgreSQL state machine. All mutations lock tenant queue, then batch, then task.

This lock order makes completion, cancellation and lease expiry mutually exclusive.
The attempt token is a fencing token, not a request authentication credential.
"""
import hashlib
import json
import secrets
from datetime import datetime, timezone

from psycopg.types.json import Jsonb

from .validation import body_hash, validate_rows, validate_usage


class APIError(Exception):
    def __init__(self, status: int, message: str, code: str = "invalid_request"):
        self.status, self.message, self.code = status, message, code


def identifier(prefix: str) -> str:
    return f"{prefix}_{secrets.token_hex(16)}"


def unix(value):
    return int(value.timestamp()) if value else None


def batch_view(row):
    return {
        "id": row["id"], "object": "batch", "endpoint": row["endpoint"],
        "input_file_id": row["input_file_id"], "completion_window": row["completion_window"],
        "status": row["status"], "metadata": row["metadata"],
        **{key: unix(row[key]) for key in ("created_at", "in_progress_at", "completed_at", "failed_at", "cancelled_at")},
        "output_file_id": row["output_file_id"], "error_file_id": row["error_file_id"],
        "request_counts": {key: row[f"{key}_count"] for key in ("total", "completed", "failed", "cancelled", "running", "pending")},
        "usage": {"prompt_tokens": row["prompt_tokens"], "completion_tokens": row["completion_tokens"]},
    }


def file_view(row):
    return {"id": row["id"], "object": "file", "bytes": row["bytes"],
            "created_at": unix(row["created_at"]), "filename": row["filename"],
            "purpose": "batch", "status": "processed"}


async def one(conn, sql, params=()):
    return await (await conn.execute(sql, params)).fetchone()


class Store:
    def __init__(self, pool, settings):
        self.pool, self.settings = pool, settings

    async def tenant(self, key: str):
        digest = hashlib.sha256(key.encode()).hexdigest()
        async with self.pool.connection() as conn:
            row = await one(conn, "SELECT tenant_id FROM api_keys WHERE key_hash=%s AND NOT revoked", (digest,))
        if not row:
            raise APIError(401, "Invalid API key", "invalid_api_key")
        return row["tenant_id"]

    async def upload(self, tenant, filename, content):
        validate_rows(content, self.settings)
        async with self.pool.connection() as conn:
            row = await one(conn, "INSERT INTO files(id,tenant_id,filename,kind,content,bytes) VALUES(%s,%s,%s,'input',%s,%s) RETURNING *", (identifier("file"), tenant, filename[:255], content, len(content)))
        return file_view(row)

    async def idempotency(self, conn, tenant, scope, key, body):
        if key is None:
            return None
        if not 1 <= len(key) <= 128:
            raise APIError(400, "Idempotency-Key must contain 1 to 128 characters")
        digest = body_hash(body)
        await conn.execute("INSERT INTO idempotency(tenant_id,scope,key,body_hash) VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING", (tenant, scope, key, digest))
        row = await one(conn, "SELECT * FROM idempotency WHERE tenant_id=%s AND scope=%s AND key=%s FOR UPDATE", (tenant, scope, key))
        if row["body_hash"] != digest:
            raise APIError(409, "Idempotency key was used with a different request", "idempotency_conflict")
        return row["response"]

    async def remember(self, conn, tenant, scope, key, response):
        if key:
            await conn.execute("UPDATE idempotency SET response=%s WHERE tenant_id=%s AND scope=%s AND key=%s", (Jsonb(response), tenant, scope, key))

    async def create_rows(self, conn, tenant, input_file_id, rows, metadata):
        # Tenant row serializes admission control, preventing concurrent submissions
        # from overshooting the configured queue bound. NO KEY UPDATE stays
        # compatible with KEY SHARE locks acquired by tenant foreign keys
        # (including the preceding idempotency insert).
        await conn.execute("SELECT id FROM tenants WHERE id=%s FOR NO KEY UPDATE", (tenant,))
        queued = await one(conn, "SELECT COALESCE(sum(pending_count+running_count),0) AS n FROM batches WHERE tenant_id=%s AND status='in_progress'", (tenant,))
        if queued["n"] + len(rows) > self.settings.max_queued_tasks_per_tenant:
            raise APIError(429, "Tenant queue limit reached", "queue_limit")
        await conn.execute("INSERT INTO tenant_queues(tenant_id,model) VALUES(%s,%s) ON CONFLICT DO NOTHING", (tenant, self.settings.model_name))
        await conn.execute("SELECT tenant_id FROM tenant_queues WHERE tenant_id=%s AND model=%s FOR UPDATE", (tenant, self.settings.model_name))
        await conn.execute("UPDATE tenant_queues SET pending_count=pending_count+%s,next_ready_at=LEAST(next_ready_at,clock_timestamp()) WHERE tenant_id=%s AND model=%s", (len(rows), tenant, self.settings.model_name))
        bid = identifier("batch")
        row = await one(conn, "INSERT INTO batches(id,tenant_id,input_file_id,model,status,metadata,total_count,pending_count) VALUES(%s,%s,%s,%s,'in_progress',%s,%s,%s) RETURNING *", (bid, tenant, input_file_id, self.settings.model_name, Jsonb(metadata), len(rows), len(rows)))
        async with conn.cursor() as cursor:
            await cursor.executemany("INSERT INTO tasks(id,tenant_id,batch_id,custom_id,model,body,status,max_attempts) VALUES(%s,%s,%s,%s,%s,%s,'pending',%s)", [(identifier("task"), tenant, bid, task["custom_id"], self.settings.model_name, Jsonb(task["body"]), self.settings.max_attempts) for task in rows])
        return batch_view(row)

    async def create(self, tenant, body, key):
        scope = "/v1/batches"
        async with self.pool.connection() as conn, conn.transaction():
            previous = await self.idempotency(conn, tenant, scope, key, body)
            if previous is not None:
                return previous
            file = await one(conn, "SELECT * FROM files WHERE id=%s AND tenant_id=%s AND kind='input'", (body["input_file_id"], tenant))
            if not file:
                raise APIError(404, "Input file not found", "not_found")
            rows = validate_rows(bytes(file["content"]), self.settings)
            result = await self.create_rows(conn, tenant, file["id"], rows, body.get("metadata", {}))
            await self.remember(conn, tenant, scope, key, result)
            return result

    async def get_batch(self, tenant, bid):
        async with self.pool.connection() as conn:
            row = await one(conn, "SELECT * FROM batches WHERE id=%s AND tenant_id=%s", (bid, tenant))
        if not row:
            raise APIError(404, "Batch not found", "not_found")
        return batch_view(row)

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
            await self.lock_queue(conn, tenant, self.settings.model_name)
            row = await one(conn, "SELECT * FROM batches WHERE id=%s AND tenant_id=%s FOR UPDATE", (bid, tenant))
            if not row:
                raise APIError(404, "Batch not found", "not_found")
            if row["status"] == "in_progress":
                await self.cancel_locked(conn, row)
            return batch_view(await one(conn, "SELECT * FROM batches WHERE id=%s", (bid,)))

    async def retry(self, tenant, bid, key):
        scope = f"/v1/batches/{bid}/retry"
        async with self.pool.connection() as conn, conn.transaction():
            previous = await self.idempotency(conn, tenant, scope, key, {})
            if previous is not None:
                return previous
            source = await one(conn, "SELECT * FROM batches WHERE id=%s AND tenant_id=%s", (bid, tenant))
            if not source:
                raise APIError(404, "Batch not found", "not_found")
            if source["status"] == "in_progress":
                raise APIError(409, "Wait until source batch is terminal before retrying", "batch_not_terminal")
            rows = await (await conn.execute("SELECT custom_id,body FROM tasks WHERE batch_id=%s AND status='failed' ORDER BY id", (bid,))).fetchall()
            if not rows:
                raise APIError(409, "Source batch has no failed tasks", "no_failed_tasks")
            content = b"".join((json.dumps({"custom_id": row["custom_id"], "method": "POST", "url": "/v1/chat/completions", "body": row["body"]}, ensure_ascii=False) + "\n").encode() for row in rows)
            fid = identifier("file")
            await conn.execute("INSERT INTO files(id,tenant_id,filename,kind,content,bytes) VALUES(%s,%s,%s,'input',%s,%s)", (fid, tenant, f"{bid}-retry.jsonl", content, len(content)))
            result = await self.create_rows(conn, tenant, fid, rows, {"retry_of": bid})
            await self.remember(conn, tenant, scope, key, result)
            return result

    async def file(self, tenant, fid):
        async with self.pool.connection() as conn:
            row = await one(conn, "SELECT * FROM files WHERE id=%s AND tenant_id=%s", (fid, tenant))
        if not row:
            raise APIError(404, "File not found", "not_found")
        return row

    async def file_chunks(self, tenant, file):
        if file["kind"] == "input":
            data = bytes(file["content"])
            for offset in range(0, len(data), 65536):
                yield data[offset:offset + 65536]
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
        async with self.pool.connection() as conn:
            await conn.execute("INSERT INTO workers(id,name,model,concurrency,metadata) VALUES(%s,%s,%s,%s,%s) ON CONFLICT(id) DO UPDATE SET name=excluded.name,model=excluded.model,concurrency=excluded.concurrency,metadata=excluded.metadata,last_seen_at=clock_timestamp()", (worker_id, body["name"], body["model"], body["concurrency"], Jsonb(body.get("metadata", {}))))
        return {"worker_id": worker_id, "heartbeat_interval_seconds": max(1, self.settings.lease_seconds // 3), "lease_seconds": self.settings.lease_seconds}

    async def claim(self, worker_id, model, limit):
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
        async with self.pool.connection() as conn, conn.transaction():
            await conn.execute("UPDATE workers SET last_seen_at=clock_timestamp() WHERE id=%s", (worker_id,))
            task = await self.locked_task(conn, tid)
            self.fence(task, worker_id, token)
            row = await one(conn, "UPDATE tasks SET lease_expires_at=clock_timestamp()+(%s*interval '1 second') WHERE id=%s RETURNING lease_expires_at", (self.settings.lease_seconds, tid))
            await conn.execute("UPDATE workers SET last_seen_at=clock_timestamp() WHERE id=%s", (worker_id,))
            return {"ok": True, "lease_expires_at": row["lease_expires_at"].isoformat()}

    async def complete(self, tid, worker_id, token, response):
        prompt, completion = validate_usage(response)
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

    async def fail(self, tid, worker_id, token, error, retryable):
        async with self.pool.connection() as conn, conn.transaction():
            await conn.execute("UPDATE workers SET last_seen_at=clock_timestamp() WHERE id=%s", (worker_id,))
            task = await self.locked_task(conn, tid)
            self.fence(task, worker_id, token)
            target = await self.failure_locked(conn, task, error, retryable)
            await conn.execute("UPDATE workers SET last_seen_at=clock_timestamp() WHERE id=%s", (worker_id,))
            return {"ok": True, "status": target}

    async def recover(self):
        """Bounded, indexed sweep; safe in every API replica."""
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

    async def overview(self, tenant):
        async with self.pool.connection() as conn:
            stats = await one(conn, "SELECT count(*) AS total_batches,COALESCE(sum(total_count),0) AS total_tasks,COALESCE(sum(completed_count),0) AS completed_tasks,COALESCE(sum(failed_count),0) AS failed_tasks,COALESCE(sum(cancelled_count),0) AS cancelled_tasks,COALESCE(sum(pending_count),0) AS queued_tasks,COALESCE(sum(running_count),0) AS running_tasks,COALESCE(sum(prompt_tokens),0) AS prompt_tokens,COALESCE(sum(completion_tokens),0) AS completion_tokens FROM batches WHERE tenant_id=%s", (tenant,))
            buckets = await (await conn.execute("SELECT date_trunc('hour',finished_at) AS timestamp,count(*) FILTER(WHERE status='completed') AS completed,count(*) FILTER(WHERE status='failed') AS failed,COALESCE(sum(prompt_tokens+completion_tokens) FILTER(WHERE status='completed'),0) AS tokens FROM attempts WHERE tenant_id=%s AND finished_at>=now()-interval '24 hours' GROUP BY 1 ORDER BY 1", (tenant,))).fetchall()
            workers = await (await conn.execute("SELECT id,name,model,concurrency,last_seen_at,CASE WHEN last_seen_at>clock_timestamp()-(%s*interval '1 second') THEN 'online' ELSE 'offline' END AS status FROM workers ORDER BY last_seen_at DESC LIMIT 100", (self.settings.lease_seconds * 2,))).fetchall()
            batches = await (await conn.execute("SELECT * FROM batches WHERE tenant_id=%s ORDER BY created_at DESC,id DESC LIMIT 10", (tenant,))).fetchall()
        return {"stats": {key: int(value) for key, value in stats.items()}, "throughput": [{**row, "timestamp": row["timestamp"].isoformat(), "tokens": int(row["tokens"])} for row in buckets], "workers": [{**row, "last_seen_at": row["last_seen_at"].isoformat()} for row in workers], "recent_batches": [batch_view(row) for row in batches]}
