"""Owner-run, bounded retirement of archived task detail and time-scoped keys.

Preview is the default. This module never runs from an API/worker background
loop. An explicit database policy must select a created-after cohort first.
Result objects and batch aggregates remain available after task retirement.
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime
import json
import os
from pathlib import Path

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from .blob_storage import LocalBlobStore, S3BlobStore, BlobIntegrityError
from .result_archival import ResultArchiver


def _bound(limit):
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise ValueError('Retirement page size must be between 1 and 1000')


async def history_page(pool, tenant, batch, *, limit=1000, apply=False, archiver=None):
    """Preview or retire one page, verifying immutable archive bytes before apply.

    No external I/O occurs while database locks are held. SQL rechecks the
    generation/manifest, reader pins, terminal state, cohort, age and policy
    after verification. A verification failure leaves every task untouched.
    """
    _bound(limit)
    if type(apply) is not bool:
        raise ValueError('apply must be an explicit boolean')
    async with pool.connection() as conn, conn.transaction():
        result = await (await conn.execute(
            'SELECT public.batchforge_retire_history_page(%s,%s,%s,false) AS result',
            (tenant, batch, limit))).fetchone()
    preview = result['result']
    if not apply or not preview['eligible']:
        return preview
    if archiver is None or archiver.pool is not pool:
        raise ValueError('Applying retirement requires an archive verifier on the same database pool')
    async with pool.connection() as conn:
        run, files, chunks = await archiver._ready(conn, tenant, batch)
    if (not run or run['id'] != preview['archive_id']
            or run['manifest_sha256'] != preview['manifest_sha256']):
        raise BlobIntegrityError('Archive generation changed before retirement verification')
    await asyncio.to_thread(archiver._verify_files, files, chunks)
    async with pool.connection() as conn, conn.transaction():
        result = await (await conn.execute(
            'SELECT public.batchforge_retire_history_page(%s,%s,%s,true,%s) AS result',
            (tenant, batch, limit, run['manifest_sha256']))).fetchone()
    return result['result']


async def idempotency_page(pool, tenant, floor, *, limit=1000, apply=False):
    """Advance the replay fence and compact at most limit resolved bf1 keys.

    Legacy keys, pre-v11 rows, and keys created before the policy cohort are
    never removed. SQL rejects a floor crossing any unresolved/live bf1 key.
    Expired keys always fail closed, even when the physical delete is paged.
    """
    _bound(limit)
    if type(floor) is not int or floor < 0 or type(apply) is not bool:
        raise ValueError('Invalid explicit replay floor or apply flag')
    async with pool.connection() as conn, conn.transaction():
        row = await (await conn.execute(
            'SELECT public.batchforge_retire_idempotency_page(%s,%s,%s,%s) AS result',
            (tenant, floor, limit, apply))).fetchone()
    return row['result']


async def configure_policy(pool, tenant, *, enabled=False, created_after=None,
                           min_age_seconds=86400, replay_ttl_seconds=86400):
    """Owner-only explicit policy; never advances or resets the replay floor."""
    if type(enabled) is not bool:
        raise ValueError('enabled must be a boolean')
    if enabled and (not isinstance(created_after, datetime) or created_after.tzinfo is None):
        raise ValueError('Enabling retirement requires an explicit timezone-aware created-after cohort')
    if any(type(v) is not int or not 60 <= v <= 315360000
           for v in (min_age_seconds, replay_ttl_seconds)):
        raise ValueError('Retention and replay TTL must be between 60 seconds and 10 years')
    async with pool.connection() as conn, conn.transaction():
        await conn.execute('SELECT id FROM public.tenants WHERE id=%s FOR NO KEY UPDATE', (tenant,))
        row = await (await conn.execute('''
          INSERT INTO public.tenant_history_retirement_policy
            (tenant_id,enabled,eligible_created_after,min_age_seconds,replay_ttl_seconds)
          VALUES(%s,%s,%s,%s,%s) ON CONFLICT(tenant_id) DO UPDATE SET
            enabled=EXCLUDED.enabled,eligible_created_after=EXCLUDED.eligible_created_after,
            min_age_seconds=EXCLUDED.min_age_seconds,replay_ttl_seconds=EXCLUDED.replay_ttl_seconds
          RETURNING tenant_id,enabled,eligible_created_after,min_age_seconds,replay_ttl_seconds,replay_floor
          ''', (tenant, enabled, created_after, min_age_seconds, replay_ttl_seconds))).fetchone()
    return row


def _parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('preview', 'retire', 'keys-preview', 'keys-retire', 'configure'))
    parser.add_argument('--tenant', required=True)
    parser.add_argument('--batch')
    parser.add_argument('--limit', type=int, default=1000)
    parser.add_argument('--floor', type=int)
    parser.add_argument('--apply', action='store_true', help='Required for every destructive/configuration action')
    parser.add_argument('--enable', action='store_true')
    parser.add_argument('--created-after', type=datetime.fromisoformat)
    parser.add_argument('--min-age-seconds', type=int, default=86400)
    parser.add_argument('--replay-ttl-seconds', type=int, default=86400)
    parser.add_argument('--database-url-env', default='BATCHFORGE_MAINTENANCE_DATABASE_URL')
    parser.add_argument('--local-root', help='Existing local archive directory; not a destination for exported data')
    parser.add_argument('--s3', action='store_true', help='Verify existing S3 objects using restricted environment credentials')
    parser.add_argument('--bucket')
    return parser


async def _run(args):
    if args.action in ('retire', 'keys-retire', 'configure') and not args.apply:
        raise ValueError('Mutation requires both an explicit action and --apply')
    if args.action in ('preview', 'retire') and not args.batch:
        raise ValueError('--batch is required; no unbounded tenant sweep is supported')
    if args.action in ('keys-preview', 'keys-retire') and args.floor is None:
        raise ValueError('--floor is required')
    dsn = os.environ.get(args.database_url_env)
    if not dsn:
        raise ValueError('Maintenance database environment variable is not set')
    async with AsyncConnectionPool(dsn, min_size=1, max_size=2,
            kwargs={'autocommit': True, 'row_factory': dict_row,
                    'options': '-c statement_timeout=10000 -c lock_timeout=1000'}, open=False) as pool:
        if args.action == 'configure':
            return await configure_policy(pool, args.tenant, enabled=args.enable,
                created_after=args.created_after, min_age_seconds=args.min_age_seconds,
                replay_ttl_seconds=args.replay_ttl_seconds)
        if args.action.startswith('keys-'):
            return await idempotency_page(pool, args.tenant, args.floor,
                limit=args.limit, apply=args.action == 'keys-retire')
        verifier = None
        if args.action == 'retire':
            if bool(args.local_root) == bool(args.s3):
                raise ValueError('Select exactly one existing archive verifier: --local-root or --s3')
            if args.local_root and not Path(args.local_root).is_dir():
                raise ValueError('Local archive verification requires an existing directory')
            if args.s3 and not args.bucket:
                raise ValueError('--bucket is required for S3 verification')
            adapter = (LocalBlobStore(args.local_root) if args.local_root else S3BlobStore.from_credentials(
                endpoint_url=os.environ['AWS_ENDPOINT_URL_S3'], region=os.environ['AWS_REGION'],
                bucket=args.bucket, access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY']))
            verifier = ResultArchiver(pool, adapter, max_archived_bytes_per_tenant=1)
        return await history_page(pool, args.tenant, args.batch, limit=args.limit,
            apply=args.action == 'retire', archiver=verifier)


def main():
    args = _parser().parse_args()
    try:
        result = asyncio.run(_run(args))
    except Exception as exc:
        # Never print a DSN or SQL diagnostic containing tenant request data.
        print(json.dumps({'ok': False, 'error_type': type(exc).__name__,
                          'sqlstate': getattr(exc, 'sqlstate', None)}))
        raise SystemExit(1) from None
    print(json.dumps({'ok': True, 'result': result}, default=lambda x: x.isoformat()))


if __name__ == '__main__':
    main()
