"""Owner-only additive Agent namespace; never change the BatchForge v11 ledger."""
from __future__ import annotations
import argparse,hashlib,json,os,re
from pathlib import Path
import psycopg
from psycopg import sql

SQL=Path(__file__).with_name('schema.sql').read_text()
CHECKSUM=hashlib.sha256(SQL.encode()).hexdigest()
LOCK=1936150129


def migrate(database_url,*,runtime_role=None):
    if runtime_role is not None and not re.fullmatch('[a-zA-Z_][a-zA-Z0-9_]{0,62}',runtime_role):raise ValueError('Explicit PostgreSQL role name required')
    with psycopg.connect(database_url,connect_timeout=5,options='-c lock_timeout=3000 -c statement_timeout=30000') as conn:
        conn.execute('SELECT pg_advisory_xact_lock(%s)',(LOCK,))
        exists=conn.execute("SELECT to_regclass('bf_agents.migrations')").fetchone()[0]
        applied=False
        if exists:
            rows=conn.execute('SELECT version,checksum FROM bf_agents.migrations ORDER BY version').fetchall()
            if rows!=[(1,CHECKSUM)]:raise RuntimeError('Agent migration checksum/version mismatch')
        else:
            if conn.execute("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname='bf_agents')").fetchone()[0]:raise RuntimeError('Unversioned Agent schema refuses adoption')
            conn.execute(SQL);conn.execute('INSERT INTO bf_agents.migrations(version,checksum) VALUES(1,%s)',(CHECKSUM,));applied=True
        if runtime_role:
            role=sql.Identifier(runtime_role)
            conn.execute(sql.SQL('GRANT USAGE ON SCHEMA bf_agents TO {}').format(role))
            for table in ('runs','memories'):
                conn.execute(sql.SQL('GRANT SELECT,INSERT,UPDATE ON bf_agents.{} TO {}').format(sql.Identifier(table),role))
            conn.execute(sql.SQL('GRANT SELECT,INSERT ON bf_agents.events TO {}').format(role))
            conn.execute(sql.SQL('GRANT SELECT ON bf_agents.limits,bf_agents.usage,bf_agents.migrations TO {}').format(role))
        return applied


async def verify(conn):
    try:rows=await (await conn.execute('SELECT version,checksum FROM bf_agents.migrations ORDER BY version')).fetchall()
    except psycopg.errors.UndefinedTable:raise RuntimeError('Agent owner migration required; serving BatchForge remains unchanged') from None
    if [(r['version'],r['checksum']) for r in rows]!=[(1,CHECKSUM)]:raise RuntimeError('Agent schema checksum mismatch')


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--runtime-role');p.add_argument('--plan',action='store_true');args=p.parse_args()
    if args.plan:
        print(json.dumps({'schema':'bf_agents','version':1,'checksum':CHECKSUM,'serving_schema_version':11,
            'tables':['migrations','limits','usage','runs','events','memories'],
            'runtime_role':args.runtime_role,'public_schema_migrations_modified':False,'database_connections':0}))
        return
    try:applied=migrate(os.environ['DATABASE_URL'],runtime_role=args.runtime_role)
    except BaseException as exc:print(json.dumps({'passed':False,'error_type':type(exc).__name__,'credentials_suppressed':True}));raise SystemExit(1) from None
    print(json.dumps({'passed':True,'schema':'bf_agents','version':1,'applied':applied,'batchforge_schema_modified':False}))


if __name__=='__main__':main()
