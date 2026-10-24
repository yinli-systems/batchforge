"""Tenant-scoped input artifacts with transactional quotas and fenced GC.

Object writes are outside PostgreSQL transactions. A committed reservation and
generation row precede the write, so crash leftovers are discoverable. Retired
generations retain tombstones: a delayed external delete cannot target a new
upload of the same digest. Legacy inline inputs are read unchanged.

Local storage survives process crashes on that filesystem, NOT Pod/disk loss.
Production Pod-loss durability requires an independently durable object store.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
import base64
import hashlib
import os
from pathlib import Path
import re
import secrets
import stat
import tempfile
from urllib.parse import urlsplit

from .auxiliary_admission import collect_expired_read_leases, release_read_lease


class BlobStorageError(RuntimeError):
    pass


class BlobIntegrityError(BlobStorageError):
    pass


class BlobNotFound(BlobStorageError):
    pass


class BlobExpired(BlobNotFound):
    pass


class BlobQuotaExceeded(BlobStorageError):
    def __init__(self, code="storage_limit"):
        self.code = code
        super().__init__("Tenant input storage limit reached" if code == "storage_limit" else "Tenant queue limit reached")


class BlobBusy(BlobStorageError):
    pass


def _id(prefix):
    return prefix + "_" + secrets.token_hex(16)


@dataclass(frozen=True)
class BlobRef:
    id: str
    tenant_id: str
    sha256: str
    bytes: int
    backend: str
    object_key: str

    @classmethod
    def from_row(cls, row):
        return cls(**{key: row[key] for key in cls.__dataclass_fields__})

    def validate(self):
        namespace = hashlib.sha256(self.tenant_id.encode()).hexdigest()
        expected = f"objects/{namespace}/{self.sha256}/{self.id}"
        if (not re.fullmatch(r"blob_[0-9a-f]{32}", self.id)
                or not re.fullmatch(r"[0-9a-f]{64}", self.sha256)
                or type(self.bytes) is not int or self.bytes < 1
                or self.object_key != expected or self.backend not in {"local", "s3"}):
            raise BlobIntegrityError("Invalid artifact reference")


def _check_content(ref, content):
    ref.validate()
    if not isinstance(content, bytes) or len(content) != ref.bytes or hashlib.sha256(content).hexdigest() != ref.sha256:
        raise BlobIntegrityError("Artifact size or checksum differs")


def _verify_stream(stream, ref):
    digest, size = hashlib.sha256(), 0
    while chunk := stream.read(65536):
        size += len(chunk)
        if size > ref.bytes:
            raise BlobIntegrityError("Artifact exceeds declared size")
        digest.update(chunk)
    if size != ref.bytes or digest.hexdigest() != ref.sha256:
        raise BlobIntegrityError("Artifact size or checksum differs")
    stream.seek(0)


class LocalBlobStore:
    backend = "local"

    def __init__(self, root):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, mode=0o700, exist_ok=True)
        if self.root.stat().st_uid != os.geteuid() or stat.S_IMODE(self.root.stat().st_mode) & 0o077:
            raise BlobStorageError("Artifact directory must be owner-only")

    def _path(self, ref, create=False):
        ref.validate()
        if ref.backend != self.backend:
            raise BlobStorageError("Artifact backend differs")
        parts = ref.object_key.split("/")
        directory = self.root
        for part in parts[:-1]:
            child = directory / part
            if create:
                try:
                    child.mkdir(mode=0o700)
                    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
                    try:
                        os.fsync(fd)
                    finally:
                        os.close(fd)
                except FileExistsError:
                    pass
            if child.is_symlink() or (child.exists() and not child.is_dir()):
                raise BlobIntegrityError("Artifact directory is not a regular directory")
            directory = child
        return directory / parts[-1]

    def put_verified(self, ref, content):
        _check_content(ref, content)
        target = self._path(ref, create=True)
        temporary = target.with_name("." + _id("write") + ".tmp")
        try:
            fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, "wb") as stream:
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
            try:
                os.link(temporary, target, follow_symlinks=False)
            except FileExistsError:
                with self.open_verified(ref):
                    pass
            fd = os.open(target.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        finally:
            temporary.unlink(missing_ok=True)

    @contextmanager
    def open_verified(self, ref):
        target = self._path(ref)
        try:
            fd = os.open(target, os.O_RDONLY | os.O_NOFOLLOW)
        except FileNotFoundError:
            raise BlobNotFound("Artifact data is unavailable") from None
        with os.fdopen(fd, "rb") as stream:
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                raise BlobIntegrityError("Artifact is not a regular file")
            # Verify before yielding any bytes, then rewind the same inode.
            _verify_stream(stream, ref)
            yield stream

    def delete(self, ref):
        target = self._path(ref)
        try:
            target.unlink()
        except FileNotFoundError:
            return
        fd = os.open(target.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


class S3BlobStore:
    """Standard S3 client adapter. SDK credentials never enter file metadata.

    Verified download spools before exposing bytes, bounding RAM while ensuring
    corruption is reported before HTTP response data is yielded. The endpoint
    must itself provide durable storage; S3 compatibility alone is not an SLA.
    """
    backend = "s3"

    def __init__(self, client, bucket, *, prefix="batchforge", spool_bytes=8 * 1024 * 1024):
        if not bucket or not re.fullmatch(r"[A-Za-z0-9._-]{1,255}", bucket):
            raise ValueError("Invalid object bucket")
        if not re.fullmatch(r"[A-Za-z0-9/_-]{0,128}", prefix) or ".." in prefix:
            raise ValueError("Invalid object prefix")
        self.client, self.bucket, self.prefix = client, bucket, prefix.strip("/")
        self.spool_bytes = spool_bytes

    @classmethod
    def from_credentials(cls, *, endpoint_url, region, bucket, access_key_id,
                         secret_access_key, session_token=None, prefix="batchforge"):
        url = urlsplit(endpoint_url)
        if url.scheme != "https" or not url.hostname or url.username or url.password or url.query or url.fragment:
            raise ValueError("Object endpoint must be credential-free HTTPS")
        import boto3
        from botocore.config import Config
        client = boto3.client("s3", endpoint_url=endpoint_url, region_name=region,
            aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key,
            aws_session_token=session_token, config=Config(signature_version="s3v4",
                s3={"addressing_style": "path"}, connect_timeout=5, read_timeout=60,
                retries={"max_attempts": 2, "mode": "standard"},
                request_checksum_calculation="when_required", response_checksum_validation="when_required"))
        return cls(client, bucket, prefix=prefix)

    def _key(self, ref):
        ref.validate()
        if ref.backend != self.backend:
            raise BlobStorageError("Artifact backend differs")
        return f"{self.prefix}/{ref.object_key}" if self.prefix else ref.object_key

    def put_verified(self, ref, content):
        _check_content(ref, content)
        try:
            self.client.put_object(Bucket=self.bucket, Key=self._key(ref), Body=content,
                ContentLength=len(content), ContentType="application/jsonl",
                ContentMD5=base64.b64encode(hashlib.md5(content, usedforsecurity=False).digest()).decode(),
                Metadata={"sha256": ref.sha256})
        except Exception:
            raise BlobStorageError("Object upload failed") from None

    @contextmanager
    def open_verified(self, ref):
        body = None
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=self._key(ref))
            body = response["Body"]
            if response.get("ContentLength") != ref.bytes or response.get("Metadata", {}).get("sha256") != ref.sha256:
                raise BlobIntegrityError("Object metadata differs")
            with tempfile.SpooledTemporaryFile(max_size=self.spool_bytes, mode="w+b") as stream:
                total = 0
                while chunk := body.read(65536):
                    total += len(chunk)
                    if total > ref.bytes:
                        raise BlobIntegrityError("Artifact exceeds declared size")
                    stream.write(chunk)
                stream.seek(0)
                _verify_stream(stream, ref)
                yield stream
        except BlobStorageError:
            raise
        except Exception:
            raise BlobStorageError("Object download failed") from None
        finally:
            if body is not None:
                body.close()

    def delete(self, ref):
        try:
            self.client.delete_object(Bucket=self.bucket, Key=self._key(ref))
        except Exception:
            raise BlobStorageError("Object deletion failed") from None


async def _one(conn, sql, params=()):
    return await (await conn.execute(sql, params)).fetchone()


class BlobCatalog:
    def __init__(self, pool, adapter, *, max_stored_input_bytes_per_tenant,
                 max_queued_tasks_per_tenant, upload_reservation_seconds=300,
                 input_retention_seconds=86400, read_lease_seconds=300):
        for value in (max_stored_input_bytes_per_tenant, max_queued_tasks_per_tenant,
                      upload_reservation_seconds, input_retention_seconds, read_lease_seconds):
            if type(value) is not int or value < 1:
                raise ValueError("Storage bounds must be positive integers")
        self.pool, self.adapter = pool, adapter
        self.max_bytes, self.max_tasks = max_stored_input_bytes_per_tenant, max_queued_tasks_per_tenant
        self.upload_seconds, self.retention_seconds, self.read_seconds = upload_reservation_seconds, input_retention_seconds, read_lease_seconds

    async def _tenant_lock(self, conn, tenant):
        if not await _one(conn, "SELECT id FROM tenants WHERE id=%s FOR NO KEY UPDATE", (tenant,)):
            raise BlobNotFound("Tenant not found")
        await conn.execute("INSERT INTO tenant_blob_usage(tenant_id) VALUES(%s) ON CONFLICT DO NOTHING", (tenant,))
        return await _one(conn, "SELECT * FROM tenant_blob_usage WHERE tenant_id=%s FOR UPDATE", (tenant,))

    async def _reserve(self, tenant, content, tasks):
        if not isinstance(content, bytes) or not content or type(tasks) is not int or tasks < 1:
            raise ValueError("Input content and validated task count are required")
        token, digest = _id("upload"), hashlib.sha256(content).hexdigest()
        async with self.pool.connection() as conn, conn.transaction():
            usage = await self._tenant_lock(conn, tenant)
            if usage["stored_bytes"] + usage["reserved_bytes"] + len(content) > self.max_bytes:
                raise BlobQuotaExceeded()
            queued = await _one(conn, "SELECT COALESCE(sum(pending_count+running_count),0) AS n FROM batches WHERE tenant_id=%s AND status='in_progress'", (tenant,))
            if queued["n"] + usage["held_tasks"] + tasks > self.max_tasks:
                raise BlobQuotaExceeded("queue_limit")
            await conn.execute("UPDATE tenant_blob_usage SET reserved_bytes=reserved_bytes+%s,held_tasks=held_tasks+%s WHERE tenant_id=%s", (len(content), tasks, tenant))
            await conn.execute("INSERT INTO blob_upload_reservations(id,tenant_id,sha256,bytes,task_count,expires_at) VALUES(%s,%s,%s,%s,%s,clock_timestamp()+make_interval(secs=>%s))", (token, tenant, digest, len(content), tasks, self.upload_seconds))
        return token, digest

    async def _acquire(self, tenant, token):
        async with self.pool.connection() as conn, conn.transaction():
            await self._tenant_lock(conn, tenant)
            ticket = await _one(conn, "SELECT * FROM blob_upload_reservations WHERE id=%s AND tenant_id=%s AND expires_at>clock_timestamp() FOR UPDATE", (token, tenant))
            if not ticket:
                raise BlobBusy("Upload reservation expired")
            row = await _one(conn, "SELECT * FROM blob_objects WHERE tenant_id=%s AND sha256=%s AND state IN ('writing','ready') FOR UPDATE", (tenant, ticket["sha256"]))
            if row and row["state"] == "writing" and row["write_token"] != token:
                live = await _one(conn, "SELECT write_expires_at>clock_timestamp() AS live FROM blob_objects WHERE id=%s", (row["id"],))
                if live["live"]:
                    return None
                await conn.execute("UPDATE blob_objects SET state='deleting',gc_after=clock_timestamp(),updated_at=clock_timestamp() WHERE id=%s", (row["id"],))
                row = None
            if not row:
                bid = _id("blob")
                namespace = hashlib.sha256(tenant.encode()).hexdigest()
                key = f"objects/{namespace}/{ticket['sha256']}/{bid}"
                row = await _one(conn, "INSERT INTO blob_objects(id,tenant_id,sha256,backend,object_key,bytes,state,write_token,write_expires_at) VALUES(%s,%s,%s,%s,%s,%s,'writing',%s,clock_timestamp()+make_interval(secs=>%s)) RETURNING *", (bid, tenant, ticket["sha256"], self.adapter.backend, key, ticket["bytes"], token, self.upload_seconds))
            if row["backend"] != self.adapter.backend:
                raise BlobStorageError("Artifact backend configuration differs")
            await conn.execute("UPDATE blob_upload_reservations SET blob_id=%s WHERE id=%s", (row["id"], token))
            return BlobRef.from_row(row)

    async def _renew_upload(self, tenant, token, stopped):
        while True:
            try:
                await asyncio.wait_for(stopped.wait(), timeout=max(.1, self.upload_seconds / 3))
                return
            except asyncio.TimeoutError:
                async with self.pool.connection() as conn, conn.transaction():
                    await self._tenant_lock(conn, tenant)
                    ticket = await _one(conn, "UPDATE blob_upload_reservations SET expires_at=clock_timestamp()+make_interval(secs=>%s) WHERE id=%s AND tenant_id=%s AND expires_at>clock_timestamp() RETURNING blob_id", (self.upload_seconds, token, tenant))
                    if not ticket:
                        raise BlobBusy("Upload reservation lost")
                    await conn.execute("UPDATE blob_objects SET write_expires_at=clock_timestamp()+make_interval(secs=>%s) WHERE id=%s AND write_token=%s AND state='writing'", (self.upload_seconds, ticket["blob_id"], token))

    async def _abort(self, tenant, token):
        async with self.pool.connection() as conn, conn.transaction():
            await self._tenant_lock(conn, tenant)
            row = await _one(conn, "DELETE FROM blob_upload_reservations WHERE id=%s AND tenant_id=%s RETURNING *", (token, tenant))
            if row:
                await conn.execute("UPDATE tenant_blob_usage SET reserved_bytes=reserved_bytes-%s,held_tasks=held_tasks-%s WHERE tenant_id=%s", (row["bytes"], row["task_count"], tenant))
                await conn.execute("UPDATE blob_objects SET state='deleting',gc_after=clock_timestamp(),updated_at=clock_timestamp() WHERE id=%s AND write_token=%s AND state='writing'", (row["blob_id"], token))

    async def put_input(self, tenant, filename, content, task_count):
        """Reserve before external I/O; publish a NULL-content input only on commit."""
        token, _ = await self._reserve(tenant, content, task_count)
        stopped = asyncio.Event()
        renewal = asyncio.create_task(self._renew_upload(tenant, token, stopped))
        try:
            ref = None
            for _ in range(max(1, self.upload_seconds * 10)):
                ref = await self._acquire(tenant, token)
                if ref:
                    break
                await asyncio.sleep(.1)
            if ref is None:
                raise BlobBusy("Identical upload is still writing")
            await asyncio.to_thread(self.adapter.put_verified, ref, content)
            if renewal.done():
                renewal.result()
            async with self.pool.connection() as conn, conn.transaction():
                await self._tenant_lock(conn, tenant)
                ticket = await _one(conn, "SELECT * FROM blob_upload_reservations WHERE id=%s AND tenant_id=%s AND expires_at>clock_timestamp() FOR UPDATE", (token, tenant))
                obj = await _one(conn, "SELECT * FROM blob_objects WHERE id=%s AND tenant_id=%s FOR UPDATE", (ref.id, tenant))
                if not ticket or not obj or obj["state"] not in {"writing", "ready"} or (obj["state"] == "writing" and obj["write_token"] != token):
                    raise BlobBusy("Upload generation or reservation lost")
                await conn.execute("UPDATE blob_objects SET state='ready',write_token=NULL,write_expires_at=NULL,updated_at=clock_timestamp() WHERE id=%s", (ref.id,))
                # Transfer the pre-PUT future-file metadata credit to the actual
                # file in this same tenant-locked transaction; rollback restores
                # the reservation if publication fails.
                await conn.execute("DELETE FROM blob_upload_reservations WHERE id=%s", (token,))
                row = await _one(conn, "INSERT INTO files(id,tenant_id,filename,kind,content,bytes,blob_id,storage_expires_at,admission_tasks) VALUES(%s,%s,%s,'input',NULL,%s,%s,clock_timestamp()+make_interval(secs=>%s),%s) RETURNING *", (_id("file"), tenant, str(filename)[:255], len(content), ref.id, self.retention_seconds, task_count))
                await conn.execute("UPDATE tenant_blob_usage SET reserved_bytes=reserved_bytes-%s,stored_bytes=stored_bytes+%s WHERE tenant_id=%s", (len(content), len(content), tenant))
                return row
        except BaseException:
            # Cancellation cannot cancel an in-flight SDK thread. Generation
            # tombstones ensure its late write stays discoverable for later GC.
            await asyncio.shield(self._abort(tenant, token))
            raise
        finally:
            stopped.set()
            try:
                await renewal
            except Exception:
                pass

    async def admit_batch(self, conn, tenant, file_id, task_count):
        """Call inside the same transaction as batch/task inserts, before queues."""
        if type(task_count) is not int or task_count < 1:
            raise ValueError("Validated positive task count is required")
        usage = await self._tenant_lock(conn, tenant)
        file = await _one(conn, "SELECT * FROM files WHERE id=%s AND tenant_id=%s AND kind='input' FOR UPDATE", (file_id, tenant))
        if not file:
            raise BlobNotFound("Input file not found")
        if file.get("storage_deleted_at"):
            raise BlobExpired("Input retention expired")
        queued = await _one(conn, "SELECT COALESCE(sum(pending_count+running_count),0) AS n FROM batches WHERE tenant_id=%s AND status='in_progress'", (tenant,))
        credit = file.get("admission_tasks", 0)
        if queued["n"] + usage["held_tasks"] - credit + task_count > self.max_tasks:
            raise BlobQuotaExceeded("queue_limit")
        if credit:
            await conn.execute("UPDATE tenant_blob_usage SET held_tasks=held_tasks-%s WHERE tenant_id=%s", (credit, tenant))
            await conn.execute("UPDATE files SET admission_tasks=0 WHERE id=%s", (file_id,))

    async def _lease(self, tenant, file_id):
        async with self.pool.connection() as conn, conn.transaction():
            await self._tenant_lock(conn, tenant)
            file = await _one(conn, "SELECT * FROM files WHERE id=%s AND tenant_id=%s AND kind='input' FOR UPDATE", (file_id, tenant))
            if not file:
                raise BlobNotFound("Input file not found")
            if file.get("storage_deleted_at"):
                raise BlobExpired("Input retention expired")
            if not file.get("blob_id"):
                if file["content"] is None:
                    raise BlobNotFound("Inline input data is unavailable")
                return None, bytes(file["content"])
            obj = await _one(conn, "SELECT * FROM blob_objects WHERE tenant_id=%s AND id=%s AND state='ready' FOR UPDATE", (tenant, file["blob_id"]))
            if not obj:
                raise BlobNotFound("Input artifact unavailable")
            lease = _id("read")
            await conn.execute("INSERT INTO blob_read_leases(id,tenant_id,blob_id,expires_at) VALUES(%s,%s,%s,clock_timestamp()+make_interval(secs=>%s))", (lease, tenant, obj["id"], self.read_seconds))
            return (lease, BlobRef.from_row(obj)), None

    async def _renew_read(self, tenant, blob_id, lease, stopped):
        while True:
            try:
                await asyncio.wait_for(stopped.wait(), timeout=max(.1, self.read_seconds / 3))
                return
            except asyncio.TimeoutError:
                async with self.pool.connection() as conn, conn.transaction():
                    await self._tenant_lock(conn, tenant)
                    obj = await _one(conn, "SELECT id FROM blob_objects WHERE id=%s AND tenant_id=%s AND state='ready' FOR UPDATE", (blob_id, tenant))
                    if not obj:
                        raise BlobBusy("Read generation retired")
                    row = await _one(conn, "UPDATE blob_read_leases SET expires_at=clock_timestamp()+make_interval(secs=>%s) WHERE id=%s AND expires_at>clock_timestamp() RETURNING id", (self.read_seconds, lease))
                    if not row:
                        raise BlobBusy("Read lease expired")

    @asynccontextmanager
    async def open_input(self, tenant, file_id):
        """Read lease covers verification and slow client streaming; no held conn."""
        leased, inline = await self._lease(tenant, file_id)
        if leased is None:
            async def legacy():
                for offset in range(0, len(inline), 65536):
                    yield inline[offset:offset + 65536]
            yield legacy()
            return
        lease, ref = leased
        stopped = asyncio.Event()
        renewal = asyncio.create_task(self._renew_read(tenant, ref.id, lease, stopped))
        context = self.adapter.open_verified(ref)
        stream = None
        opening = asyncio.create_task(asyncio.to_thread(context.__enter__))
        try:
            try:
                stream = await asyncio.shield(opening)
            except asyncio.CancelledError:
                # A cancelled await cannot stop a blocking SDK read. Keep the
                # lease until it finishes, then close the resulting stream.
                try:
                    stream = await asyncio.shield(opening)
                except Exception:
                    pass
                raise
            async def chunks():
                while True:
                    if renewal.done():
                        renewal.result()
                    reading = asyncio.create_task(asyncio.to_thread(stream.read, 65536))
                    try:
                        chunk = await asyncio.shield(reading)
                    except asyncio.CancelledError:
                        # Do not close a file concurrently with its SDK thread.
                        try:
                            await asyncio.shield(reading)
                        except Exception:
                            pass
                        raise
                    if not chunk:
                        return
                    yield chunk
            yield chunks()
        finally:
            if stream is not None:
                await asyncio.to_thread(context.__exit__, None, None, None)
            stopped.set()
            try:
                await renewal
            except Exception:
                pass
            await release_read_lease(self.pool, 'blob_read_leases', tenant, lease)

    async def read_input(self, tenant, file_id):
        async with self.open_input(tenant, file_id) as chunks:
            return b"".join([chunk async for chunk in chunks])

    async def discard_unsubmitted(self, tenant, file_id):
        """Release a losing staged retry without touching submitted/legacy files."""
        async with self.pool.connection() as conn, conn.transaction():
            await self._tenant_lock(conn, tenant)
            file = await _one(conn, "SELECT * FROM files WHERE id=%s AND tenant_id=%s AND blob_id IS NOT NULL AND admission_tasks>0 AND storage_deleted_at IS NULL FOR UPDATE", (file_id, tenant))
            if not file or await _one(conn, "SELECT id FROM batches WHERE input_file_id=%s LIMIT 1", (file_id,)):
                return False
            await conn.execute("UPDATE files SET blob_id=NULL,storage_deleted_at=clock_timestamp(),admission_tasks=0 WHERE id=%s", (file_id,))
            await conn.execute("UPDATE tenant_blob_usage SET stored_bytes=stored_bytes-%s,held_tasks=held_tasks-%s WHERE tenant_id=%s", (file["bytes"], file["admission_tasks"], tenant))
            await conn.execute("UPDATE blob_objects SET unreferenced_at=clock_timestamp() WHERE id=%s", (file["blob_id"],))
            return True

    async def expire_inputs(self, limit=100):
        if not 1 <= limit <= 1000:
            raise ValueError("Bound retention work to 1..1000 files")
        async with self.pool.connection() as conn:
            candidates = await (await conn.execute("SELECT f.id,f.tenant_id FROM files f WHERE f.blob_id IS NOT NULL AND f.storage_deleted_at IS NULL AND f.storage_expires_at<clock_timestamp() AND NOT EXISTS(SELECT 1 FROM batches b WHERE b.input_file_id=f.id AND b.status='in_progress') ORDER BY f.storage_expires_at,f.id LIMIT %s", (limit,))).fetchall()
        count = 0
        for candidate in candidates:
            async with self.pool.connection() as conn, conn.transaction():
                await self._tenant_lock(conn, candidate["tenant_id"])
                file = await _one(conn, "SELECT * FROM files WHERE id=%s AND blob_id IS NOT NULL AND storage_deleted_at IS NULL AND storage_expires_at<clock_timestamp() FOR UPDATE SKIP LOCKED", (candidate["id"],))
                if not file or await _one(conn, "SELECT id FROM batches WHERE input_file_id=%s AND status='in_progress' LIMIT 1", (candidate["id"],)):
                    continue
                await conn.execute("UPDATE files SET blob_id=NULL,storage_deleted_at=clock_timestamp(),admission_tasks=0 WHERE id=%s", (file["id"],))
                await conn.execute("UPDATE tenant_blob_usage SET stored_bytes=stored_bytes-%s,held_tasks=held_tasks-%s WHERE tenant_id=%s", (file["bytes"], file["admission_tasks"], file["tenant_id"]))
                await conn.execute("UPDATE blob_objects SET unreferenced_at=clock_timestamp() WHERE id=%s", (file["blob_id"],))
                count += 1
        return {"expired_files": count}

    async def collect_orphans(self, limit=100, grace_seconds=300):
        if not 1 <= limit <= 1000 or not 0 <= grace_seconds <= 604800:
            raise ValueError("Invalid bounded GC limits")
        # Expired reservation credits are released with the same tenant lock.
        await collect_expired_read_leases(self.pool, 'blob_read_leases', limit)
        async with self.pool.connection() as conn:
            expired = await (await conn.execute("SELECT id,tenant_id FROM blob_upload_reservations WHERE expires_at<=clock_timestamp() ORDER BY expires_at,id LIMIT %s", (limit,))).fetchall()
        for ticket in expired:
            # Recheck inside _abort below is done under the tenant lock.
            async with self.pool.connection() as conn, conn.transaction():
                await self._tenant_lock(conn, ticket["tenant_id"])
                row = await _one(conn, "DELETE FROM blob_upload_reservations WHERE id=%s AND expires_at<=clock_timestamp() RETURNING *", (ticket["id"],))
                if row:
                    await conn.execute("UPDATE tenant_blob_usage SET reserved_bytes=reserved_bytes-%s,held_tasks=held_tasks-%s WHERE tenant_id=%s", (row["bytes"], row["task_count"], row["tenant_id"]))
        async with self.pool.connection() as conn:
            candidates = await (await conn.execute("SELECT o.id,o.tenant_id FROM blob_objects o WHERE o.backend=%s AND o.gc_after<=clock_timestamp() AND o.unreferenced_at<=clock_timestamp()-make_interval(secs=>%s) AND NOT EXISTS(SELECT 1 FROM files f WHERE f.tenant_id=o.tenant_id AND f.blob_id=o.id) AND NOT EXISTS(SELECT 1 FROM blob_upload_reservations u WHERE u.tenant_id=o.tenant_id AND u.blob_id=o.id AND u.expires_at>clock_timestamp()) AND NOT EXISTS(SELECT 1 FROM blob_read_leases r WHERE r.tenant_id=o.tenant_id AND r.blob_id=o.id AND r.expires_at>clock_timestamp()) AND (o.state<>'writing' OR o.write_expires_at<=clock_timestamp()) ORDER BY CASE WHEN o.state='deleted' THEN 1 ELSE 0 END,o.gc_after,o.id LIMIT %s", (self.adapter.backend, grace_seconds, limit))).fetchall()
        deleted, failed = 0, 0
        for candidate in candidates:
            async with self.pool.connection() as conn, conn.transaction():
                await self._tenant_lock(conn, candidate["tenant_id"])
                obj = await _one(conn, "SELECT * FROM blob_objects WHERE id=%s AND gc_after<=clock_timestamp() AND unreferenced_at<=clock_timestamp()-make_interval(secs=>%s) FOR UPDATE SKIP LOCKED", (candidate["id"], grace_seconds))
                if not obj:
                    continue
                refs = await _one(conn, "SELECT 1 FROM files WHERE tenant_id=%s AND blob_id=%s LIMIT 1", (obj["tenant_id"], obj["id"]))
                uploads = await _one(conn, "SELECT 1 FROM blob_upload_reservations WHERE tenant_id=%s AND blob_id=%s AND expires_at>clock_timestamp() LIMIT 1", (obj["tenant_id"], obj["id"]))
                readers = await _one(conn, "SELECT 1 FROM blob_read_leases WHERE tenant_id=%s AND blob_id=%s AND expires_at>clock_timestamp() LIMIT 1", (obj["tenant_id"], obj["id"]))
                if refs or uploads or readers:
                    continue
                if obj["state"] == "writing" and await _one(conn, "SELECT 1 FROM blob_objects WHERE id=%s AND write_expires_at>clock_timestamp()", (obj["id"],)):
                    continue
                await conn.execute("UPDATE blob_objects SET state='deleting',updated_at=clock_timestamp(),gc_after=clock_timestamp()+interval '1 minute' WHERE id=%s", (obj["id"],))
                ref = BlobRef.from_row(obj)
            try:
                await asyncio.to_thread(self.adapter.delete, ref)
            except BlobStorageError:
                failed += 1
                continue
            async with self.pool.connection() as conn:
                await conn.execute("UPDATE blob_objects SET state='deleted',updated_at=clock_timestamp() WHERE id=%s AND state='deleting'", (ref.id,))
            deleted += 1
        return {"deleted_objects": deleted, "delete_errors": failed, "expired_reservations_examined": len(expired)}
