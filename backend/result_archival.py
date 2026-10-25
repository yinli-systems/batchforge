"""Opt-in terminal result archival with reader pins and generation-fenced GC.

Deploy leased output readers on every replica before allowing archive/purge.
No task is deleted. Completed response/body payloads may be cleared only after
verified immutable objects and an atomic manifest, and after DB readers drain.
Failed/cancelled bodies, attempts, usage and task identity remain in PostgreSQL.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import hashlib
import json

from .blob_storage import (BlobRef, BlobStorageError, BlobIntegrityError,
    BlobNotFound, BlobQuotaExceeded, BlobBusy, _id, _one)
from .auxiliary_admission import collect_expired_read_leases, release_read_lease


class ResultQuotaExceeded(BlobQuotaExceeded):
    def __init__(self):
        super().__init__()
        self.code = 'result_storage_limit'
        self.args = ('Tenant result archive limit reached',)


def render_result(row):
    """Same JSONL representation as the original SQL output/error downloader."""
    response = None
    if row['response'] is not None:
        response = {'status_code': 200, 'request_id': row['id'], 'body': row['response']}
    result = {'id': row['id'], 'custom_id': row['custom_id'], 'response': response, 'error': row['error']}
    return (json.dumps(result, ensure_ascii=False, separators=(',', ':')) + '\n').encode()


def _manifest(run, files, chunks):
    output = {'id': run['id'], 'tenant_id': run['tenant_id'], 'batch_id': run['batch_id'], 'files': []}
    for file in sorted(files, key=lambda f: f['file_id']):
        pieces = sorted((c for c in chunks if c['file_id'] == file['file_id']), key=lambda c: c['ordinal'])
        if (not pieces or [c['ordinal'] for c in pieces] != list(range(len(pieces)))
                or any(c['state'] != 'ready' or c['tenant_id'] != run['tenant_id']
                       or c['archive_id'] != run['id'] for c in pieces)
                or sum(c['bytes'] for c in pieces) != file['bytes']
                or file['line_count'] != file['expected_count'] or not file['sha256']):
            raise BlobIntegrityError('Result manifest is incomplete')
        for piece in pieces:
            BlobRef.from_row(piece).validate()
        output['files'].append({key: file[key] for key in ('file_id', 'kind', 'bytes', 'line_count', 'expected_count', 'sha256')} |
            {'chunks': [{key: c[key] for key in ('id', 'ordinal', 'sha256', 'bytes', 'backend', 'object_key')} for c in pieces]})
    if not files or len(chunks) != sum(len(f['chunks']) for f in output['files']):
        raise BlobIntegrityError('Result manifest references unknown files')
    encoded = json.dumps(output, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()
    return hashlib.sha256(encoded).hexdigest()


class ResultArchiver:
    def __init__(self, pool, adapter, *, max_archived_bytes_per_tenant,
                 chunk_bytes=4*1024*1024, lease_seconds=300, page_rows=32, purge_rows=1000):
        if (type(max_archived_bytes_per_tenant) is not int or max_archived_bytes_per_tenant < 1
                or type(chunk_bytes) is not int or not 1 <= chunk_bytes <= 16*1024*1024
                or type(lease_seconds) is not int or lease_seconds < 1
                or type(page_rows) is not int or not 1 <= page_rows <= 256
                or type(purge_rows) is not int or not 1 <= purge_rows <= 10000):
            raise ValueError('Invalid archive bounds')
        self.pool, self.adapter = pool, adapter
        self.max_bytes, self.chunk_bytes = max_archived_bytes_per_tenant, chunk_bytes
        self.lease_seconds, self.page_rows = lease_seconds, page_rows
        self.purge_rows = purge_rows

    async def _lock(self, conn, tenant, batch_id):
        if not await _one(conn, 'SELECT id FROM tenants WHERE id=%s FOR NO KEY UPDATE', (tenant,)):
            raise BlobNotFound('Tenant not found')
        batch = await _one(conn, 'SELECT * FROM batches WHERE id=%s AND tenant_id=%s FOR UPDATE', (batch_id, tenant))
        if not batch:
            raise BlobNotFound('Batch not found')
        return batch

    async def _usage(self, conn, tenant):
        await conn.execute('INSERT INTO tenant_result_usage(tenant_id) VALUES(%s) ON CONFLICT DO NOTHING', (tenant,))
        return await _one(conn, 'SELECT * FROM tenant_result_usage WHERE tenant_id=%s FOR UPDATE', (tenant,))

    async def _writer(self, conn, tenant, batch_id, archive_id, token):
        batch = await self._lock(conn, tenant, batch_id)
        run = await _one(conn, "SELECT * FROM result_archive_runs WHERE id=%s AND tenant_id=%s AND state='building' AND writer_token=%s AND lease_expires_at>clock_timestamp() FOR UPDATE", (archive_id, tenant, token))
        if not run:
            raise BlobBusy('Archive writer lease expired or superseded')
        if batch['status'] == 'in_progress' or batch['pending_count'] or batch['running_count']:
            raise BlobIntegrityError('Archive source is no longer terminal')
        return run, batch

    async def _retire_locked(self, conn, run):
        usage = await self._usage(conn, run['tenant_id'])
        del usage
        if run['state'] == 'building':
            await conn.execute('UPDATE tenant_result_usage SET reserved_bytes=reserved_bytes-%s WHERE tenant_id=%s', (run['reserved_bytes'], run['tenant_id']))
        elif run['state'] == 'ready':
            await conn.execute('UPDATE tenant_result_usage SET stored_bytes=stored_bytes-%s WHERE tenant_id=%s', (run['bytes'], run['tenant_id']))
        await conn.execute("UPDATE result_archive_runs SET state='deleting',reserved_bytes=0,writer_token=NULL,lease_expires_at=NULL,gc_after=clock_timestamp() WHERE id=%s", (run['id'],))
        await conn.execute("UPDATE result_archive_chunks SET state='deleting',gc_after=clock_timestamp() WHERE archive_id=%s AND state IN ('writing','ready')", (run['id'],))

    async def _begin(self, tenant, batch_id):
        async with self.pool.connection() as conn, conn.transaction():
            batch = await self._lock(conn, tenant, batch_id)
            if batch['status'] == 'in_progress' or batch['pending_count'] or batch['running_count']:
                raise BlobBusy('Only terminal batches can be archived')
            current = await _one(conn, "SELECT * FROM result_archive_runs WHERE tenant_id=%s AND batch_id=%s AND state IN ('building','ready') FOR UPDATE", (tenant, batch_id))
            if current and current['state'] == 'ready':
                return current, None
            if current:
                live = await _one(conn, 'SELECT lease_expires_at>clock_timestamp() AS live FROM result_archive_runs WHERE id=%s', (current['id'],))
                if live['live']:
                    raise BlobBusy('Archive is already building')
                await self._retire_locked(conn, current)
            archive_id, token = _id('archive'), _id('writer')
            run = await _one(conn, "INSERT INTO result_archive_runs(id,tenant_id,batch_id,backend,state,writer_token,lease_expires_at) VALUES(%s,%s,%s,%s,'building',%s,clock_timestamp()+make_interval(secs=>%s)) RETURNING *", (archive_id, tenant, batch_id, self.adapter.backend, token, self.lease_seconds))
            for kind, field, count in [('output', 'output_file_id', batch['completed_count']),
                                      ('error', 'error_file_id', batch['failed_count'] + batch['cancelled_count'])]:
                if not count:
                    continue
                file = await _one(conn, 'SELECT * FROM files WHERE id=%s AND tenant_id=%s AND batch_id=%s AND kind=%s', (batch[field], tenant, batch_id, kind))
                if not file:
                    raise BlobIntegrityError('Terminal result file metadata is missing')
                await conn.execute('INSERT INTO result_archive_files(tenant_id,archive_id,file_id,kind,expected_count) VALUES(%s,%s,%s,%s,%s)', (tenant, archive_id, file['id'], kind, count))
            return run, token

    async def _renew_writer(self, tenant, batch_id, archive_id, token, stopped):
        while True:
            try:
                await asyncio.wait_for(stopped.wait(), max(.1, self.lease_seconds / 3))
                return
            except asyncio.TimeoutError:
                async with self.pool.connection() as conn, conn.transaction():
                    await self._writer(conn, tenant, batch_id, archive_id, token)
                    await conn.execute('UPDATE result_archive_runs SET lease_expires_at=clock_timestamp()+make_interval(secs=>%s) WHERE id=%s', (self.lease_seconds, archive_id))

    async def _abort(self, tenant, batch_id, archive_id, token):
        async with self.pool.connection() as conn, conn.transaction():
            await self._lock(conn, tenant, batch_id)
            run = await _one(conn, "SELECT * FROM result_archive_runs WHERE id=%s AND writer_token=%s AND state='building' FOR UPDATE", (archive_id, token))
            if run:
                await self._retire_locked(conn, run)

    async def _source_page(self, tenant, batch_id, kind, last, *, lease=None, writer=None):
        async with self.pool.connection() as conn, conn.transaction():
            if writer:
                await self._writer(conn, tenant, batch_id, *writer)
            else:
                await self._lock(conn, tenant, batch_id)
                valid = await _one(conn, "SELECT id FROM result_read_leases WHERE id=%s AND tenant_id=%s AND batch_id=%s AND mode='db' AND expires_at>clock_timestamp()", (lease, tenant, batch_id))
                if not valid:
                    raise BlobBusy('Result download lease expired')
            states = ['completed'] if kind == 'output' else ['failed', 'cancelled']
            rows = await (await conn.execute('SELECT id,custom_id,response,error FROM tasks WHERE tenant_id=%s AND batch_id=%s AND status=ANY(%s) AND id>%s ORDER BY id LIMIT %s', (tenant, batch_id, states, last, self.page_rows))).fetchall()
            if kind == 'output' and any(row['response'] is None for row in rows):
                raise BlobIntegrityError('Database result payload is unavailable')
            return rows

    async def _chunk(self, run, token, file_id, ordinal, content):
        digest, bid = hashlib.sha256(content).hexdigest(), _id('blob')
        namespace = hashlib.sha256(run['tenant_id'].encode()).hexdigest()
        ref = BlobRef(bid, run['tenant_id'], digest, len(content), self.adapter.backend, f'objects/{namespace}/{digest}/{bid}')
        async with self.pool.connection() as conn, conn.transaction():
            await self._writer(conn, run['tenant_id'], run['batch_id'], run['id'], token)
            usage = await self._usage(conn, run['tenant_id'])
            if usage['stored_bytes'] + usage['reserved_bytes'] + len(content) > self.max_bytes:
                raise ResultQuotaExceeded()
            await conn.execute('UPDATE tenant_result_usage SET reserved_bytes=reserved_bytes+%s WHERE tenant_id=%s', (len(content), run['tenant_id']))
            await conn.execute('UPDATE result_archive_runs SET reserved_bytes=reserved_bytes+%s WHERE id=%s', (len(content), run['id']))
            await conn.execute("INSERT INTO result_archive_chunks(id,tenant_id,archive_id,file_id,ordinal,sha256,bytes,backend,object_key,state) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,'writing')", (bid, run['tenant_id'], run['id'], file_id, ordinal, digest, len(content), ref.backend, ref.object_key))
        await asyncio.to_thread(self.adapter.put_verified, ref, content)
        await asyncio.to_thread(self._verify, ref)
        async with self.pool.connection() as conn, conn.transaction():
            await self._writer(conn, run['tenant_id'], run['batch_id'], run['id'], token)
            await conn.execute("UPDATE result_archive_chunks SET state='ready' WHERE id=%s AND state='writing'", (bid,))

    def _verify(self, ref):
        with self.adapter.open_verified(ref):
            pass

    def _verify_files(self, files, chunks):
        """Check both object identity and the manifest's concatenated JSONL."""
        for file in files:
            digest, size, lines = hashlib.sha256(), 0, 0
            for chunk in (c for c in chunks if c['file_id'] == file['file_id']):
                with self.adapter.open_verified(BlobRef.from_row(chunk)) as reader:
                    while data := reader.read(65536):
                        digest.update(data)
                        size += len(data)
                        lines += data.count(b'\n')
            if size != file['bytes'] or lines != file['line_count'] or digest.hexdigest() != file['sha256']:
                raise BlobIntegrityError('Result file checksum or count differs')

    async def _rows(self, conn, archive_id):
        files = await (await conn.execute('SELECT * FROM result_archive_files WHERE archive_id=%s ORDER BY file_id', (archive_id,))).fetchall()
        chunks = await (await conn.execute('SELECT * FROM result_archive_chunks WHERE archive_id=%s ORDER BY file_id,ordinal', (archive_id,))).fetchall()
        return files, chunks

    async def archive_batch(self, tenant, batch_id, *, purge_payloads=True):
        """Explicit policy action; never invoked by a migration or constructor."""
        if type(purge_payloads) is not bool:
            raise ValueError('purge_payloads must be boolean')
        run, token = await self._begin(tenant, batch_id)
        if token:
            stopped = asyncio.Event()
            renewal = asyncio.create_task(self._renew_writer(tenant, batch_id, run['id'], token, stopped))
            try:
                async with self.pool.connection() as conn:
                    files, _ = await self._rows(conn, run['id'])
                summaries = []
                for file in files:
                    last, count, size, ordinal = '', 0, 0, 0
                    digest, buffer = hashlib.sha256(), bytearray()
                    while True:
                        if renewal.done():
                            renewal.result()
                        rows = await self._source_page(tenant, batch_id, file['kind'], last, writer=(run['id'], token))
                        if not rows:
                            break
                        for row in rows:
                            encoded = render_result(row)
                            digest.update(encoded)
                            size += len(encoded)
                            count += 1
                            view = memoryview(encoded)
                            while view:
                                take = min(self.chunk_bytes - len(buffer), len(view))
                                buffer.extend(view[:take]); view = view[take:]
                                if len(buffer) == self.chunk_bytes:
                                    await self._chunk(run, token, file['file_id'], ordinal, bytes(buffer))
                                    ordinal += 1; buffer.clear()
                        last = rows[-1]['id']
                    if buffer:
                        await self._chunk(run, token, file['file_id'], ordinal, bytes(buffer))
                    if count != file['expected_count']:
                        raise BlobIntegrityError('Terminal result count differs')
                    summaries.append((size, count, digest.hexdigest(), file['file_id']))
                if renewal.done():
                    renewal.result()
                async with self.pool.connection() as conn, conn.transaction():
                    run, _ = await self._writer(conn, tenant, batch_id, run['id'], token)
                    for size, count, digest, file_id in summaries:
                        await conn.execute('UPDATE result_archive_files SET bytes=%s,line_count=%s,sha256=%s WHERE archive_id=%s AND file_id=%s', (size, count, digest, run['id'], file_id))
                    files, chunks = await self._rows(conn, run['id'])
                    manifest = _manifest(run, files, chunks)
                    total = sum(file['bytes'] for file in files)
                    if total != run['reserved_bytes']:
                        raise BlobIntegrityError('Archive byte accounting differs')
                    await self._usage(conn, tenant)
                    await conn.execute('UPDATE tenant_result_usage SET reserved_bytes=reserved_bytes-%s,stored_bytes=stored_bytes+%s WHERE tenant_id=%s', (total, total, tenant))
                    run = await _one(conn, "UPDATE result_archive_runs SET state='ready',reserved_bytes=0,bytes=%s,manifest_sha256=%s,writer_token=NULL,lease_expires_at=NULL,ready_at=clock_timestamp() WHERE id=%s RETURNING *", (total, manifest, run['id']))
                    for file in files:
                        await conn.execute('UPDATE files SET bytes=%s WHERE id=%s AND tenant_id=%s', (file['bytes'], file['file_id'], tenant))
            except BaseException:
                await asyncio.shield(self._abort(tenant, batch_id, run['id'], token))
                raise
            finally:
                stopped.set()
                try:
                    await renewal
                except Exception:
                    pass
        # Archive-only qualification can prove object readback while retaining
        # the database copy until every serving reader is known compatible.
        purged = await self.purge_batch(tenant, batch_id) if purge_payloads else 0
        async with self.pool.connection() as conn:
            files, _ = await self._rows(conn, run['id'])
            progress = await _one(conn, 'SELECT rows_purged FROM result_archive_runs WHERE id=%s', (run['id'],))
        return {'archive_id': run['id'], 'state': 'ready', 'bytes': run['bytes'],
                'files': len(files), 'purged': purged, 'rows_purged': progress['rows_purged']}

    async def _ready(self, conn, tenant, batch_id):
        run = await _one(conn, "SELECT * FROM result_archive_runs WHERE tenant_id=%s AND batch_id=%s AND state='ready'", (tenant, batch_id))
        if not run:
            return None, [], []
        files, chunks = await self._rows(conn, run['id'])
        if (_manifest(run, files, chunks) != run['manifest_sha256']
                or sum(file['bytes'] for file in files) != run['bytes']):
            raise BlobIntegrityError('Result manifest checksum differs')
        return run, files, chunks

    async def purge_batch(self, tenant, batch_id):
        """Purge only completed payloads, after fresh object verification/read pins."""
        async with self.pool.connection() as conn, conn.transaction():
            batch = await self._lock(conn, tenant, batch_id)
            if batch['status'] == 'in_progress' or batch['pending_count'] or batch['running_count']:
                raise BlobIntegrityError('Archive source is no longer terminal')
            run, files, chunks = await self._ready(conn, tenant, batch_id)
            if not run:
                return False
            if run['purged_at']:
                return True
            if await _one(conn, "SELECT id FROM result_read_leases WHERE tenant_id=%s AND batch_id=%s AND mode='db' AND expires_at>clock_timestamp() LIMIT 1", (tenant, batch_id)):
                return False
        # Verify outside the DB transaction before the first purge page. Ready
        # keys/manifests are immutable; repeating a full GET on every keyset page
        # would make archival egress quadratic. Downloads verify every chunk.
        if run['purge_started_at'] is None:
            await asyncio.to_thread(self._verify_files, files, chunks)
        async with self.pool.connection() as conn, conn.transaction():
            batch = await self._lock(conn, tenant, batch_id)
            if batch['status'] == 'in_progress' or batch['pending_count'] or batch['running_count']:
                raise BlobIntegrityError('Archive source is no longer terminal')
            current, _, _ = await self._ready(conn, tenant, batch_id)
            if not current or current['id'] != run['id'] or current['manifest_sha256'] != run['manifest_sha256']:
                raise BlobBusy('Archive generation changed during verification')
            readers = await _one(conn, "SELECT id FROM result_read_leases WHERE tenant_id=%s AND batch_id=%s AND mode='db' AND expires_at>clock_timestamp() LIMIT 1", (tenant, batch_id))
            if readers:
                return False
            selected = await (await conn.execute("SELECT id FROM tasks WHERE batch_id=%s AND tenant_id=%s AND status='completed' AND id>COALESCE(%s,'') AND (response IS NOT NULL OR body<>'{}'::jsonb) ORDER BY id LIMIT %s FOR UPDATE", (batch_id, tenant, current['purge_cursor'], self.purge_rows))).fetchall()
            ids = [row['id'] for row in selected]
            if ids:
                await conn.execute("UPDATE tasks SET response=NULL,body='{}'::jsonb WHERE id=ANY(%s) AND tenant_id=%s AND batch_id=%s", (ids, tenant, batch_id))
            remaining = await _one(conn, "SELECT id FROM tasks WHERE batch_id=%s AND tenant_id=%s AND status='completed' AND (response IS NOT NULL OR body<>'{}'::jsonb) LIMIT 1", (batch_id, tenant))
            if remaining and not ids:
                raise BlobIntegrityError('Result purge cursor cannot make progress')
            await conn.execute('UPDATE result_archive_runs SET purge_started_at=COALESCE(purge_started_at,clock_timestamp()),purge_cursor=COALESCE(%s,purge_cursor),rows_purged=rows_purged+%s,purged_at=CASE WHEN %s THEN COALESCE(purged_at,clock_timestamp()) ELSE purged_at END WHERE id=%s', (ids[-1] if ids else None, len(ids), not remaining, run['id']))
            return not remaining

    async def _pin(self, tenant, file_id):
        async with self.pool.connection() as conn, conn.transaction():
            file = await _one(conn, "SELECT * FROM files WHERE id=%s AND tenant_id=%s AND kind IN ('output','error')", (file_id, tenant))
            if not file:
                raise BlobNotFound('Result file not found')
            batch = await self._lock(conn, tenant, file['batch_id'])
            if batch['status'] == 'in_progress' or batch['pending_count'] or batch['running_count']:
                raise BlobBusy('Result batch is not terminal')
            file['expected_result_count'] = batch['completed_count'] if file['kind'] == 'output' else batch['failed_count'] + batch['cancelled_count']
            run, files, chunks = await self._ready(conn, tenant, file['batch_id'])
            if run and not any(f['file_id'] == file_id for f in files):
                raise BlobIntegrityError('Result archive file is missing')
            if not run and await _one(conn, 'SELECT id FROM result_archive_runs WHERE tenant_id=%s AND batch_id=%s AND (purged_at IS NOT NULL OR purge_started_at IS NOT NULL) LIMIT 1', (tenant, file['batch_id'])):
                raise BlobIntegrityError('Purged result archive is unavailable')
            lease = _id('result_read')
            await conn.execute('INSERT INTO result_read_leases(id,tenant_id,batch_id,file_id,mode,archive_id,expires_at) VALUES(%s,%s,%s,%s,%s,%s,clock_timestamp()+make_interval(secs=>%s))', (lease, tenant, file['batch_id'], file_id, 'archive' if run else 'db', run['id'] if run else None, self.lease_seconds))
            return lease, file, run, files, chunks

    async def _renew_reader(self, tenant, batch_id, lease, stopped):
        while True:
            try:
                await asyncio.wait_for(stopped.wait(), max(.1, self.lease_seconds / 3))
                return
            except asyncio.TimeoutError:
                async with self.pool.connection() as conn, conn.transaction():
                    await self._lock(conn, tenant, batch_id)
                    row = await _one(conn, 'UPDATE result_read_leases SET expires_at=clock_timestamp()+make_interval(secs=>%s) WHERE id=%s AND expires_at>clock_timestamp() RETURNING id', (self.lease_seconds, lease))
                    if not row:
                        raise BlobBusy('Result download lease expired')

    async def _read_lease_valid(self, tenant, batch_id, lease):
        async with self.pool.connection() as conn, conn.transaction():
            await self._lock(conn, tenant, batch_id)
            if not await _one(conn, 'SELECT id FROM result_read_leases WHERE id=%s AND tenant_id=%s AND expires_at>clock_timestamp()', (lease, tenant)):
                raise BlobBusy('Result download lease expired')

    @asynccontextmanager
    async def open_file(self, tenant, file_id):
        """Pin one complete DB/archive generation without a long-lived DB TX."""
        lease, file, run, files, chunks = await self._pin(tenant, file_id)
        stopped = asyncio.Event()
        renewal = asyncio.create_task(self._renew_reader(tenant, file['batch_id'], lease, stopped))
        try:
            async def stream():
                if run:
                    manifest = next(f for f in files if f['file_id'] == file_id)
                    digest, size = hashlib.sha256(), 0
                    for chunk in (c for c in chunks if c['file_id'] == file_id):
                        if renewal.done():renewal.result()
                        await self._read_lease_valid(tenant, file['batch_id'], lease)
                        # Object chunks are individually bounded and fully
                        # verified before exposure. No DB fallback on corruption.
                        def read_chunk():
                            with self.adapter.open_verified(BlobRef.from_row(chunk)) as reader:
                                return reader.read()
                        data = await asyncio.to_thread(read_chunk)
                        if len(data) != chunk['bytes']:
                            raise BlobIntegrityError('Result chunk length differs')
                        digest.update(data); size += len(data)
                        yield data
                    if size != manifest['bytes'] or digest.hexdigest() != manifest['sha256']:
                        raise BlobIntegrityError('Result file checksum differs')
                else:
                    last, count = '', 0
                    while True:
                        if renewal.done():renewal.result()
                        rows = await self._source_page(tenant, file['batch_id'], file['kind'], last, lease=lease)
                        if not rows:
                            if count != file['expected_result_count']:
                                raise BlobIntegrityError('Database result count differs')
                            return
                        for row in rows:
                            count += 1
                            yield render_result(row)
                        last = rows[-1]['id']
            iterator = stream()
            try:
                yield iterator
            finally:
                await iterator.aclose()
        finally:
            stopped.set()
            try:
                await renewal
            except Exception:
                pass
            await release_read_lease(self.pool, 'result_read_leases', tenant, lease)

    async def discard_unpurged(self, tenant, batch_id):
        """Explicit repair: retire a corrupt generation only while DB is intact."""
        async with self.pool.connection() as conn, conn.transaction():
            await self._lock(conn, tenant, batch_id)
            run = await _one(conn, "SELECT * FROM result_archive_runs WHERE tenant_id=%s AND batch_id=%s AND state='ready' FOR UPDATE", (tenant, batch_id))
            if not run:
                return False
            if run['purged_at'] or run['purge_started_at'] or await _one(conn, "SELECT id FROM tasks WHERE tenant_id=%s AND batch_id=%s AND status='completed' AND response IS NULL LIMIT 1", (tenant, batch_id)):
                raise BlobIntegrityError('Purged archives require object-store recovery')
            await self._retire_locked(conn, run)
            return True

    async def collect_orphans(self, limit=100, grace_seconds=300):
        if not 1 <= limit <= 1000 or not 0 <= grace_seconds <= 604800:
            raise ValueError('Invalid bounded archive GC limits')
        await collect_expired_read_leases(self.pool, 'result_read_leases', limit)
        async with self.pool.connection() as conn:
            runs = await (await conn.execute("SELECT id,tenant_id,batch_id FROM result_archive_runs WHERE backend=%s AND state='building' AND lease_expires_at<=clock_timestamp() ORDER BY lease_expires_at,id LIMIT %s", (self.adapter.backend, limit))).fetchall()
        for run in runs:
            async with self.pool.connection() as conn, conn.transaction():
                await self._lock(conn, run['tenant_id'], run['batch_id'])
                current = await _one(conn, "SELECT * FROM result_archive_runs WHERE id=%s AND state='building' AND lease_expires_at<=clock_timestamp() FOR UPDATE", (run['id'],))
                if current:await self._retire_locked(conn, current)
        async with self.pool.connection() as conn:
            candidates = await (await conn.execute("SELECT c.id,c.tenant_id,r.batch_id FROM result_archive_chunks c JOIN result_archive_runs r ON r.id=c.archive_id WHERE c.backend=%s AND r.state IN ('deleting','deleted') AND c.gc_after<=clock_timestamp() AND c.created_at<=clock_timestamp()-make_interval(secs=>%s) AND NOT EXISTS(SELECT 1 FROM result_read_leases l WHERE l.archive_id=r.id AND l.expires_at>clock_timestamp()) ORDER BY CASE WHEN c.state='deleted' THEN 1 ELSE 0 END,c.gc_after,c.id LIMIT %s", (self.adapter.backend, grace_seconds, limit))).fetchall()
        deleted, errors = 0, 0
        for candidate in candidates:
            async with self.pool.connection() as conn, conn.transaction():
                await self._lock(conn, candidate['tenant_id'], candidate['batch_id'])
                chunk = await _one(conn, 'SELECT c.*,r.state AS run_state FROM result_archive_chunks c JOIN result_archive_runs r ON r.id=c.archive_id WHERE c.id=%s AND c.gc_after<=clock_timestamp() FOR UPDATE OF c SKIP LOCKED', (candidate['id'],))
                if not chunk or chunk['run_state'] not in {'deleting','deleted'}:
                    continue
                if await _one(conn, 'SELECT id FROM result_read_leases WHERE archive_id=%s AND expires_at>clock_timestamp() LIMIT 1', (chunk['archive_id'],)):
                    continue
                await conn.execute("UPDATE result_archive_chunks SET state='deleting',gc_after=clock_timestamp()+interval '1 minute' WHERE id=%s", (chunk['id'],))
                ref = BlobRef.from_row(chunk)
            try:
                await asyncio.to_thread(self.adapter.delete, ref)
            except BlobStorageError:
                errors += 1
                continue
            async with self.pool.connection() as conn:
                await conn.execute("UPDATE result_archive_chunks SET state='deleted' WHERE id=%s AND state='deleting'", (ref.id,))
            deleted += 1
        async with self.pool.connection() as conn:
            await conn.execute("UPDATE result_archive_runs r SET state='deleted' WHERE id IN (SELECT candidate.id FROM result_archive_runs candidate WHERE candidate.state='deleting' AND NOT EXISTS(SELECT 1 FROM result_archive_chunks c WHERE c.archive_id=candidate.id AND c.state<>'deleted') ORDER BY candidate.id LIMIT %s)", (limit,))
        return {'deleted_objects': deleted, 'delete_errors': errors, 'expired_runs_examined': len(runs)}
