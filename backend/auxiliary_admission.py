"""Tenant-first metadata maintenance; compatible before auxiliary schema v10.

No network/client I/O belongs inside these transactions. Expired lease cleanup
pins the tenant before the lease row, matching reader renewal and archive GC.
"""
from __future__ import annotations

from psycopg import sql

LEASE_TABLES = frozenset({'blob_read_leases', 'result_read_leases'})


async def tenant_prelock(conn, tenant):
    return await (await conn.execute(
        'SELECT id FROM tenants WHERE id=%s FOR NO KEY UPDATE', (tenant,))).fetchone()


async def release_read_lease(pool, table, tenant, lease):
    if table not in LEASE_TABLES:
        raise ValueError('Unknown lease table')
    async with pool.connection() as conn, conn.transaction():
        await tenant_prelock(conn, tenant)
        await conn.execute(sql.SQL('DELETE FROM {} WHERE tenant_id=%s AND id=%s').format(
            sql.Identifier(table)), (tenant, lease))


async def collect_expired_read_leases(pool, table, limit):
    if table not in LEASE_TABLES or type(limit) is not int or not 1 <= limit <= 1000:
        raise ValueError('Invalid bounded lease cleanup')
    async with pool.connection() as conn:
        candidates = await (await conn.execute(sql.SQL(
            'SELECT tenant_id,id FROM {} WHERE expires_at<=clock_timestamp() '
            'ORDER BY expires_at,id LIMIT %s').format(sql.Identifier(table)), (limit,))).fetchall()
    removed = 0
    for candidate in candidates:
        async with pool.connection() as conn, conn.transaction():
            await tenant_prelock(conn, candidate['tenant_id'])
            row = await (await conn.execute(sql.SQL(
                'SELECT id FROM {} WHERE tenant_id=%s AND id=%s '
                'AND expires_at<=clock_timestamp() FOR UPDATE SKIP LOCKED').format(
                    sql.Identifier(table)), (candidate['tenant_id'], candidate['id']))).fetchone()
            if row:
                await conn.execute(sql.SQL('DELETE FROM {} WHERE tenant_id=%s AND id=%s').format(
                    sql.Identifier(table)), (candidate['tenant_id'], candidate['id']))
                removed += 1
    return removed
