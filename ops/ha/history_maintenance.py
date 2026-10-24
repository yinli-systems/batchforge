"""Finite, single-tenant history maintenance. Preview unless --execute is explicit.

Never enables policy, widens quotas, restarts services, or reads GPU state.
Credentials are loaded from a private file into RAM; reports contain no DSN.
"""
from __future__ import annotations

import argparse
import asyncio
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import secrets
import signal
import stat
import time
from urllib.parse import urlsplit, urlunsplit

import psycopg
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from dotenv import dotenv_values

from backend.blob_storage import LocalBlobStore, S3BlobStore
from backend.history_retirement import history_page
from backend.migrate import verify_schema
from backend.result_archival import ResultArchiver

JOURNAL_LIMIT = 32*1024*1024
SOURCE_ROOT = Path(__file__).resolve().parents[2]
BOUND_SOURCES = ('ops/ha/history_maintenance.py', 'backend/history_retirement.py',
                 'backend/result_archival.py', 'backend/blob_storage.py', 'backend/migrate.py')


def source_hashes():
    return {name:hashlib.sha256((SOURCE_ROOT/name).read_bytes()).hexdigest() for name in BOUND_SOURCES}


def canonical_archive_identity(identity):
    backend=identity.get('backend')
    if backend=='none':
        if set(identity)!={'backend'}:raise ValueError('Invalid empty archive identity')
        return {'backend':'none'}
    if backend=='local':
        if set(identity)!={'backend','root'}:raise ValueError('Invalid local archive identity')
        return {'backend':'local','root':str(Path(identity['root']).resolve())}
    if backend!='s3' or set(identity)!={'backend','endpoint','region','bucket','prefix'}:
        raise ValueError('Invalid S3 archive identity')
    url=urlsplit(identity['endpoint'])
    if url.scheme!='https' or not url.hostname or url.username or url.password or url.query or url.fragment:
        raise ValueError('Archive identity requires credential-free HTTPS')
    host=url.hostname.lower()
    if ':' in host:host='['+host+']'
    if url.port and url.port!=443:host+=':'+str(url.port)
    endpoint=urlunsplit(('https',host,url.path.rstrip('/'),'',''))
    if any(not isinstance(identity[k],str) or not identity[k] for k in ('region','bucket')):
        raise ValueError('S3 archive region and bucket required')
    return {'backend':'s3','endpoint':endpoint,'region':identity['region'],
            'bucket':identity['bucket'],'prefix':identity['prefix'].strip('/')}


def adapter_identity(adapter):
    if adapter is None:return {'backend':'none'}
    if adapter.backend=='local':return canonical_archive_identity({'backend':'local','root':adapter.root})
    if adapter.backend=='s3':
        return canonical_archive_identity({'backend':'s3','endpoint':adapter.client.meta.endpoint_url,
            'region':adapter.client.meta.region_name,'bucket':adapter.bucket,'prefix':adapter.prefix})
    raise ValueError('Unsupported archive identity')


def identity_hash(identity):
    return hashlib.sha256(json.dumps(identity,sort_keys=True,separators=(',',':')).encode()).hexdigest()


class AccountingError(RuntimeError): pass
class ArchiveBudgetReached(RuntimeError): pass


def timestamp(): return datetime.now(timezone.utc).isoformat()


def private_file(path, limit=65536):
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW)
    try:
        info=os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid!=os.geteuid() or info.st_mode&0o077 or info.st_nlink!=1:
            raise ValueError('Private owned regular file required')
        raw=os.read(fd,limit+1)
        if len(raw)>limit:raise ValueError('Private input exceeds byte limit')
        return raw
    finally:os.close(fd)


def private_directory(path):
    path=Path(path)
    path.mkdir(mode=0o700,parents=True,exist_ok=True)
    info=path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid!=os.geteuid() or info.st_mode&0o077:
        raise ValueError('Private owned directory required')
    return path


def atomic_report(path,value):
    raw=(json.dumps(value,sort_keys=True,indent=2)+'\n').encode()
    temporary=path.with_name('.'+path.name+'.'+secrets.token_hex(8)+'.partial')
    fd=os.open(temporary,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    try:
        with os.fdopen(fd,'wb') as target:
            target.write(raw);target.flush();os.fsync(target.fileno())
        os.replace(temporary,path)
        directory=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        try:os.fsync(directory)
        finally:os.close(directory)
    finally:temporary.unlink(missing_ok=True)


@contextmanager
def operation_lock(directory):
    fd=os.open(directory/'maintenance.lock',os.O_RDWR|os.O_CREAT|os.O_NOFOLLOW,0o600)
    try:
        info=os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid!=os.geteuid() or info.st_mode&0o077 or info.st_nlink!=1:
            raise ValueError('Unsafe maintenance lock')
        fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
        yield
    finally:os.close(fd)


class Journal:
    def __init__(self,directory):
        self.directory=private_directory(directory)
        self.path=self.directory/'journal.jsonl'
        self.report_path=self.directory/'report.json'

    def append(self,event,**fields):
        raw=(json.dumps({'at':timestamp(),'event':event,**fields},sort_keys=True)+'\n').encode()
        fd=os.open(self.path,os.O_WRONLY|os.O_CREAT|os.O_APPEND|os.O_NOFOLLOW,0o600)
        try:
            info=os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_uid!=os.geteuid() or info.st_mode&0o077 or info.st_nlink!=1:
                raise ValueError('Unsafe journal')
            if info.st_size+len(raw)>JOURNAL_LIMIT:raise RuntimeError('Maintenance journal byte limit reached')
            with os.fdopen(fd,'ab',closefd=False) as stream:
                stream.write(raw);stream.flush();os.fsync(fd)
        finally:os.close(fd)

    def save(self,report):atomic_report(self.report_path,report)

    def load(self):
        if not self.report_path.exists():return None
        return json.loads(private_file(self.report_path,1024*1024))


class MeteredArchive:
    """Bound verification bytes before any object fetch; no I/O under DB locks."""
    def __init__(self,adapter,limit):
        self.adapter=adapter;self.limit=limit;self.bytes=0;self.verified_bytes=0
        self.backend=adapter.backend

    @contextmanager
    def open_verified(self,ref):
        if self.bytes+ref.bytes>self.limit:raise ArchiveBudgetReached('Archive byte budget reached')
        self.bytes+=ref.bytes
        with self.adapter.open_verified(ref) as reader:
            yield reader
        self.verified_bytes+=ref.bytes


def validate_args(args):
    if not args.tenant or len(args.tenant)>128:raise ValueError('Explicit bounded tenant required')
    if not isinstance(args.created_after,datetime) or args.created_after.tzinfo is None:
        raise ValueError('Explicit timezone-aware created-after cohort required')
    bounds=(('batch_limit',1,8),('task_page_limit',1,1000),('key_page_limit',1,1000),
            ('interval',10,3600),('max_cycles',1,10000),('min_age',60,315360000),
            ('archive_byte_limit',1,1024*1024*1024))
    for name,low,high in bounds:
        value=getattr(args,name)
        if type(value) is not int or not low<=value<=high:raise ValueError('Invalid bounded '+name)
    if type(args.execute) is not bool or type(args.retire_keys) is not bool:raise ValueError('Explicit boolean modes required')


def policy_scope(args,policy):
    if not policy or not policy['enabled'] or policy['eligible_created_after'] is None:
        raise ValueError('Database history policy is disabled')
    if args.created_after<policy['eligible_created_after'] or args.min_age<policy['min_age_seconds']:
        raise ValueError('Maintenance scope must be at least as strict as database policy')
    if args.retire_keys and args.created_after!=policy['eligible_created_after']:
        raise ValueError('Time-scoped key cleanup requires the exact database cohort')


async def read_policy(pool,tenant):
    async with pool.connection() as conn:
        return await (await conn.execute('SELECT * FROM tenant_history_retirement_policy WHERE tenant_id=%s',(tenant,))).fetchone()


async def accounting(pool,tenant):
    """One read-only repeatable snapshot; never compare changing live totals."""
    async with pool.connection() as conn,conn.transaction():
        await conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        row=await (await conn.execute('''WITH t AS (
         SELECT count(*) AS n,coalesce(sum(retention_body_bytes+retention_response_bytes+retention_error_bytes),0) AS charged
         FROM tasks WHERE tenant_id=%(tenant)s), actual AS (
         SELECT batch_id,count(*) AS n,count(*) FILTER(WHERE status='pending') AS pending,
         count(*) FILTER(WHERE status='running') AS running,count(*) FILTER(WHERE status='completed') AS completed,
         count(*) FILTER(WHERE status='failed') AS failed,count(*) FILTER(WHERE status='cancelled') AS cancelled,
         sum(prompt_tokens) AS prompt,sum(completion_tokens) AS completion
         FROM tasks WHERE tenant_id=%(tenant)s GROUP BY batch_id), accepted AS (
         SELECT task_id,count(*) AS n,sum(prompt_tokens) AS p,sum(completion_tokens) AS c
         FROM attempts WHERE tenant_id=%(tenant)s AND status='completed' GROUP BY task_id), retired AS (
         SELECT coalesce(sum(history_retired_completed),0) AS n,coalesce(sum(history_retired_prompt_tokens),0) AS p,
         coalesce(sum(history_retired_completion_tokens),0) AS c FROM batches WHERE tenant_id=%(tenant)s), rollup AS (
         SELECT coalesce(sum(attempts),0) AS n,coalesce(sum(prompt_tokens),0) AS p,coalesce(sum(completion_tokens),0) AS c
         FROM retired_attempt_hourly WHERE tenant_id=%(tenant)s AND status='completed')
         SELECT (SELECT n FROM t) AS retained_tasks,(SELECT charged FROM t) AS charged_payload_bytes,
         (SELECT count(*) FROM tenant_retention_usage u CROSS JOIN t WHERE u.tenant_id=%(tenant)s
          AND (u.retained_tasks,u.charged_payload_bytes) IS DISTINCT FROM (t.n,t.charged)) AS retention_mismatches,
         (SELECT count(*) FROM tenants n LEFT JOIN tenant_retention_usage u ON u.tenant_id=n.id
           WHERE n.id=%(tenant)s AND u.tenant_id IS NULL) AS missing_ledgers,
         (SELECT count(*) FROM tasks d LEFT JOIN accepted a ON a.task_id=d.id WHERE d.tenant_id=%(tenant)s AND
          ((d.status='completed' AND (coalesce(a.n,0)<>1 OR d.prompt_tokens IS DISTINCT FROM a.p OR d.completion_tokens IS DISTINCT FROM a.c))
           OR (d.status<>'completed' AND coalesce(a.n,0)<>0))) AS accepted_mismatches,
         (SELECT count(*) FROM batches b LEFT JOIN actual a ON a.batch_id=b.id WHERE b.tenant_id=%(tenant)s AND
          (b.total_count,b.pending_count,b.running_count,b.completed_count,b.failed_count,b.cancelled_count,b.prompt_tokens,b.completion_tokens)
          IS DISTINCT FROM (coalesce(a.n,0)+b.history_retired_tasks,coalesce(a.pending,0),coalesce(a.running,0),
           coalesce(a.completed,0)+b.history_retired_completed,coalesce(a.failed,0)+b.history_retired_failed,
           coalesce(a.cancelled,0)+b.history_retired_cancelled,coalesce(a.prompt,0)+b.history_retired_prompt_tokens,
           coalesce(a.completion,0)+b.history_retired_completion_tokens)) AS batch_mismatches,
         (SELECT count(*) FROM retired r CROSS JOIN rollup h WHERE (r.n,r.p,r.c) IS DISTINCT FROM (h.n,h.p,h.c)) AS retired_usage_mismatches,
         (SELECT count(*) FROM tenant_queues q WHERE q.tenant_id=%(tenant)s AND q.pending_count<>
          (SELECT count(*) FROM tasks d WHERE d.tenant_id=q.tenant_id AND d.model=q.model AND d.status='pending')) AS queue_mismatches
         ''',{'tenant':tenant})).fetchone()
        expected=await (await conn.execute('''SELECT resource,sum(n)::bigint AS n,sum(bytes)::bigint AS bytes FROM (
          SELECT 'input_files' AS resource,count(*) AS n,0::bigint AS bytes FROM files WHERE tenant_id=%(tenant)s AND kind='input'
          UNION ALL SELECT 'input_files',count(*),0 FROM blob_upload_reservations WHERE tenant_id=%(tenant)s
          UNION ALL SELECT 'upload_reservations',count(*),0 FROM blob_upload_reservations WHERE tenant_id=%(tenant)s
          UNION ALL SELECT 'idempotency',count(*),coalesce(sum(auxiliary_reserved_bytes),0) FROM idempotency WHERE tenant_id=%(tenant)s
          UNION ALL SELECT 'blob_generations',count(*),0 FROM blob_objects WHERE tenant_id=%(tenant)s
          UNION ALL SELECT 'archive_runs',count(*),0 FROM result_archive_runs WHERE tenant_id=%(tenant)s
          UNION ALL SELECT 'archive_chunks',count(*),0 FROM result_archive_chunks WHERE tenant_id=%(tenant)s
          UNION ALL SELECT 'input_read_leases',count(*),0 FROM blob_read_leases WHERE tenant_id=%(tenant)s
          UNION ALL SELECT 'result_read_leases',count(*),0 FROM result_read_leases WHERE tenant_id=%(tenant)s
         ) d GROUP BY resource''',{'tenant':tenant})).fetchall()
        usage=await (await conn.execute('SELECT resource,rows AS n,bytes FROM tenant_auxiliary_usage WHERE tenant_id=%s',(tenant,))).fetchall()
        expected={r['resource']:(r['n'],r['bytes']) for r in expected}
        usage={r['resource']:(r['n'],r['bytes']) for r in usage}
        row['auxiliary_mismatches']=sum(usage.get(k,(0,0))!=v for k,v in expected.items())+sum(k not in expected for k in usage)
    row={k:int(v) for k,v in row.items()}
    if any(value for key,value in row.items() if key.endswith('mismatches') or key=='missing_ledgers'):
        raise AccountingError('Tenant accounting invariant failed')
    return row


async def candidates(pool,args,cursor,limit):
    async with pool.connection() as conn:
        return await (await conn.execute('''SELECT b.id,b.created_at FROM batches b
          WHERE b.tenant_id=%s AND b.created_at>=%s AND b.status<>'in_progress'
          AND b.pending_count=0 AND b.running_count=0 AND b.history_retired_at IS NULL
          AND greatest(b.completed_at,b.failed_at,b.cancelled_at)<=clock_timestamp()-make_interval(secs=>%s)
          AND (%s::timestamptz IS NULL OR (b.created_at,b.id)>(%s::timestamptz,%s))
          AND EXISTS(SELECT 1 FROM result_archive_runs r WHERE r.tenant_id=b.tenant_id AND r.batch_id=b.id
            AND r.state='ready' AND r.purged_at IS NOT NULL)
          ORDER BY b.created_at,b.id LIMIT %s''',(args.tenant,args.created_after,args.min_age,
            cursor[0] if cursor else None,cursor[0] if cursor else None,cursor[1] if cursor else None,limit))).fetchall()


class Maintainer:
    def __init__(self,pool,args,journal,adapter=None,stop=None,archive_identity=None):
        validate_args(args)
        self.pool=pool;self.args=args;self.journal=journal;self.adapter=adapter
        self.stop=stop or asyncio.Event()
        self.current_cycle=None;self.current_meter=None
        self.archive_identity=canonical_archive_identity(archive_identity) if archive_identity is not None else adapter_identity(adapter)
        if adapter is not None and self.archive_identity!=adapter_identity(adapter):
            raise ValueError('Archive identity differs from the loaded verifier')
        if args.execute and adapter is None:raise ValueError('Execution requires an archive verifier')
        self.binding={'tenant':args.tenant,'created_after':args.created_after.isoformat(),
            **{k:getattr(args,k) for k in ('execute','retire_keys','batch_limit','task_page_limit','key_page_limit',
                    'min_age','interval','max_cycles','archive_byte_limit')},
            'source_sha256':source_hashes(),'archive_identity_sha256':identity_hash(self.archive_identity)}
        previous=journal.load()
        if previous and previous.get('binding')!=self.binding:raise ValueError('Operation binding differs; use a new reviewed operation directory')
        if previous and previous.get('pending_action'):raise RuntimeError('Prior action lacks confirmation; review its journal before another operation')
        self.report=previous or {'version':1,'phase':'prepared','created_at':timestamp(),'binding':self.binding,
            'cycles_started':0,'cycles_completed':0,'cursor':None,
            'totals':{'retired_tasks':0,'retired_attempts':0,'retired_keys':0,'archive_attempted_bytes':0,'archive_verified_bytes':0}}

    def meter_progress(self):
        if self.current_cycle is None:return
        cycle=self.current_cycle;meter=self.current_meter
        for key,value in (('archive_attempted_bytes',meter.bytes if meter else 0),
                          ('archive_verified_bytes',meter.verified_bytes if meter else 0)):
            self.report['totals'][key]+=value-cycle.get(key,0)
            cycle[key]=value
        self.report['last_cycle_progress']=cycle

    async def cycle(self):
        if source_hashes()!=self.binding['source_sha256']:
            raise ValueError('Reviewed maintenance source changed')
        current=adapter_identity(self.adapter) if self.adapter is not None else self.archive_identity
        if identity_hash(current)!=self.binding['archive_identity_sha256']:
            raise ValueError('Reviewed archive identity changed')
        self.current_cycle=None;self.current_meter=None
        args=self.args;started=time.monotonic()
        policy=await read_policy(self.pool,args.tenant);policy_scope(args,policy)
        before=await accounting(self.pool,args.tenant)
        self.report['cycles_started']+=1
        number=self.report['cycles_started']
        self.report.update(phase='running',last_started_at=timestamp())
        self.journal.append('cycle_started',cycle=number,execute=args.execute)
        self.journal.save(self.report)
        cycle={'number':number,'started_at':timestamp(),'before':before,'candidates':0,'eligible_tasks':0,
            'retired_tasks':0,'retired_attempts':0,'retired_keys':0,'skip_counts':{},'pages':[]}
        meter=MeteredArchive(self.adapter,args.archive_byte_limit) if self.adapter else None
        verifier=ResultArchiver(self.pool,meter,max_archived_bytes_per_tenant=1) if meter else None
        self.current_cycle=cycle;self.current_meter=meter
        cursor=self.report['cursor'];seen=set()
        for _ in range(args.batch_limit):
            if self.stop.is_set():break
            found=await candidates(self.pool,args,cursor,1)
            if not found and cursor:
                cursor=None;found=await candidates(self.pool,args,None,1)
            if self.stop.is_set() or not found or found[0]['id'] in seen:break
            item=found[0];seen.add(item['id']);cycle['candidates']+=1
            self.report['pending_action']={'kind':'history_page','cycle':number,'batch_id':item['id']}
            self.journal.append('page_started',cycle=number,batch_id=item['id'])
            self.journal.save(self.report)
            try:
                result=await history_page(self.pool,args.tenant,item['id'],limit=args.task_page_limit,
                    apply=args.execute,archiver=verifier)
            except ArchiveBudgetReached:
                result={'eligible':False,'reason':'archive_byte_limit'}
            except psycopg.Error as exc:
                if exc.sqlstate in ('55P03','40P01','40001'):
                    result={'eligible':False,'reason':'database_busy'}
                else:raise
            self.meter_progress()
            cycle['pages'].append({'batch_id':item['id'],**result})
            if result.get('eligible'):
                cycle['eligible_tasks']+=result.get('tasks',0)
                if result.get('apply'):
                    cycle['retired_tasks']+=result.get('tasks',0);cycle['retired_attempts']+=result.get('attempts',0)
            else:
                reason=result.get('reason','ineligible');cycle['skip_counts'][reason]=cycle['skip_counts'].get(reason,0)+1
            cursor=[item['created_at'].isoformat(),item['id']]
            self.report['cursor']=cursor
            self.report['pending_action']=None
            if result.get('apply'):
                self.report['totals']['retired_tasks']+=result.get('tasks',0)
                self.report['totals']['retired_attempts']+=result.get('attempts',0)
            self.report['last_cycle_progress']=cycle
            self.journal.append('page_completed',cycle=number,batch_id=item['id'],result=result)
            self.journal.save(self.report)
            if result.get('reason')=='archive_byte_limit':break
        if args.retire_keys and not self.stop.is_set():
            self.report['pending_action']={'kind':'key_page','cycle':number}
            self.journal.append('key_page_started',cycle=number)
            self.journal.save(self.report)
            try:
                async with self.pool.connection() as conn,conn.transaction():
                    locked=await (await conn.execute('SELECT id FROM tenants WHERE id=%s FOR NO KEY UPDATE SKIP LOCKED',(args.tenant,))).fetchone()
                    if not locked:
                        result={'eligible':False,'reason':'tenant_busy_or_missing'}
                    else:
                        # Hold the policy row too: a direct owner policy UPDATE
                        # cannot widen the key cohort between this check and DELETE.
                        policy=await (await conn.execute('SELECT * FROM tenant_history_retirement_policy WHERE tenant_id=%s FOR UPDATE',(args.tenant,))).fetchone()
                        policy_scope(args,policy)
                        if self.stop.is_set():
                            result={'eligible':False,'reason':'stopped'}
                        else:
                            floor=(await (await conn.execute('SELECT floor(extract(epoch FROM clock_timestamp()))::bigint-%s AS value',(policy['replay_ttl_seconds'],))).fetchone())['value']
                            result=(await (await conn.execute('SELECT batchforge_retire_idempotency_page(%s,%s,%s,%s) AS result',
                                (args.tenant,floor,args.key_page_limit,args.execute))).fetchone())['result']
            except psycopg.Error as exc:
                if exc.sqlstate not in ('55P03','40P01','40001'):raise
                result={'eligible':False,'reason':'database_busy'}
            cycle['key_page']=result
            if result.get('apply'):
                cycle['retired_keys']=result.get('keys',0)
                self.report['totals']['retired_keys']+=cycle['retired_keys']
            if not result.get('eligible'):
                reason=result.get('reason','keys_ineligible');cycle['skip_counts'][reason]=cycle['skip_counts'].get(reason,0)+1
            self.report['pending_action']=None
            self.report['last_cycle_progress']=cycle
            self.journal.append('key_page_completed',cycle=number,result=result)
            self.journal.save(self.report)
        after=await accounting(self.pool,args.tenant)
        cycle.update(after=after,finished_at=timestamp(),elapsed_seconds=time.monotonic()-started,
            archive_attempted_bytes=meter.bytes if meter else 0,archive_verified_bytes=meter.verified_bytes if meter else 0)
        self.report['cycles_completed']+=1;self.report['last_cycle']=cycle
        self.meter_progress()
        self.journal.append('cycle_completed',cycle=number,retired_tasks=cycle['retired_tasks'],retired_keys=cycle['retired_keys'])
        self.journal.save(self.report)
        return cycle

    async def run(self):
        try:
            while self.report['cycles_started']<self.args.max_cycles and not self.stop.is_set():
                await self.cycle()
                if self.report['cycles_started']>=self.args.max_cycles or self.stop.is_set():break
                try:await asyncio.wait_for(self.stop.wait(),timeout=self.args.interval)
                except asyncio.TimeoutError:pass
            self.report.update(phase='stopped' if self.stop.is_set() else 'complete',finished_at=timestamp())
            self.journal.append('operation_'+self.report['phase'])
            self.journal.save(self.report)
            return self.report
        except Exception as exc:
            self.meter_progress()
            self.report.update(phase='failed',failed_at=timestamp(),error_type=type(exc).__name__,sqlstate=getattr(exc,'sqlstate',None))
            self.journal.save(self.report)
            raise


def parser():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--env-file',type=Path,required=True);p.add_argument('--operation-dir',type=Path,required=True)
    p.add_argument('--tenant',required=True);p.add_argument('--created-after',type=datetime.fromisoformat,required=True)
    p.add_argument('--min-age',type=int,required=True);p.add_argument('--max-cycles',type=int,required=True)
    p.add_argument('--execute',action='store_true');p.add_argument('--retire-keys',action='store_true')
    p.add_argument('--batch-limit',type=int,default=2);p.add_argument('--task-page-limit',type=int,default=1000)
    p.add_argument('--key-page-limit',type=int,default=1000);p.add_argument('--interval',type=int,default=30)
    p.add_argument('--archive-byte-limit',type=int,default=256*1024*1024)
    backend=p.add_mutually_exclusive_group();backend.add_argument('--local-root',type=Path);backend.add_argument('--s3',action='store_true')
    p.add_argument('--bucket')
    return p


async def execute(args,journal,values):
    dsn=values.get('BATCHFORGE_MAINTENANCE_DATABASE_URL') or values.get('DATABASE_URL')
    if not dsn:raise ValueError('Private owner database connection is missing')
    adapter=None
    archive={'backend':'none'}
    if args.local_root:
        archive=canonical_archive_identity({'backend':'local','root':args.local_root})
    elif args.s3:
        archive=canonical_archive_identity({'backend':'s3','endpoint':values['AWS_ENDPOINT_URL_S3'],
            'region':values['AWS_REGION'],'bucket':args.bucket,'prefix':'batchforge'})
    if args.execute:
        if bool(args.local_root)==bool(args.s3):raise ValueError('Execution requires exactly one existing archive verifier')
        if args.local_root:
            if not args.local_root.is_dir():raise ValueError('Existing local archive directory required')
            adapter=LocalBlobStore(args.local_root)
        else:
            if not args.bucket:raise ValueError('Existing bucket required')
            adapter=S3BlobStore.from_credentials(endpoint_url=values['AWS_ENDPOINT_URL_S3'],region=values['AWS_REGION'],
                bucket=args.bucket,access_key_id=values['AWS_ACCESS_KEY_ID'],secret_access_key=values['AWS_SECRET_ACCESS_KEY'])
    stop=asyncio.Event();loop=asyncio.get_running_loop()
    for signum in (signal.SIGINT,signal.SIGTERM):loop.add_signal_handler(signum,stop.set)
    options={'autocommit':True,'row_factory':dict_row,'connect_timeout':10,
             'options':'-c statement_timeout=10000 -c lock_timeout=1000 -c application_name=batchforge-history-maintenance'}
    guard=None
    try:
        guard=await psycopg.AsyncConnection.connect(dsn,**options)
        owner=await (await guard.execute("SELECT current_user=pg_get_userbyid(relowner) AS owned FROM pg_class WHERE oid='public.tenant_history_retirement_policy'::regclass")).fetchone()
        if not owner or not owner['owned']:raise ValueError('Maintenance requires the schema owner')
        await verify_schema(guard,minimum_version=11)
        locked=await (await guard.execute("SELECT pg_try_advisory_lock(hashtextextended('batchforge-history-maintenance:'||%s,0)) AS locked",(args.tenant,))).fetchone()
        if not locked['locked']:raise RuntimeError('Another tenant maintenance operator is active')
        async with AsyncConnectionPool(dsn,min_size=1,max_size=3,kwargs=options,open=False) as pool:
            worker=Maintainer(pool,args,journal,adapter,stop,archive_identity=archive)
            identity=conninfo_to_dict(dsn)
            safe={k:identity.get(k) for k in ('host','port','dbname','user')}
            fingerprint=hashlib.sha256(json.dumps(safe,sort_keys=True).encode()).hexdigest()
            old=worker.report.get('database_identity_sha256')
            if old and old!=fingerprint:raise ValueError('Operation database identity differs')
            worker.report['database_identity_sha256']=fingerprint
            return await worker.run()
    finally:
        if guard is not None:await guard.close()
        for signum in (signal.SIGINT,signal.SIGTERM):loop.remove_signal_handler(signum)


def main():
    args=parser().parse_args()
    try:
        validate_args(args)
        raw=private_file(args.env_file).decode()
        from io import StringIO
        values={k:v for k,v in dotenv_values(stream=StringIO(raw),interpolate=False).items() if v is not None}
        journal=Journal(args.operation_dir)
        with operation_lock(journal.directory):result=asyncio.run(execute(args,journal,values))
    except Exception as exc:
        print(json.dumps({'ok':False,'error_type':type(exc).__name__,'sqlstate':getattr(exc,'sqlstate',None)}))
        raise SystemExit(1) from None
    print(json.dumps({'ok':True,'phase':result['phase'],'report':str(journal.report_path),'totals':result['totals']}))


if __name__=='__main__':main()
