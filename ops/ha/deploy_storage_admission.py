"""Prepare a drained v8 -> v10 admission rollout; no action without --apply.

Compatibility APIs are rolled before one owner transaction publishes both
migrations, grants and default-tenant policy. A possibly committed transaction
never rolls source back to a registry which only understands v8. Credentials
remain in existing private files/RAM and are never placed in subprocess argv.
"""
from __future__ import annotations

import argparse
from contextlib import ExitStack, contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import io
import json
import os
from pathlib import Path
import re
import stat
import sys
import tarfile
import time

import httpx
import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from ops.ha.adaptive_qwen235b import private_operation_directory
from ops.ha.deploy_adaptive_worker_source import atomic_source, source_state
from ops.ha.finalize_api_policy import values
from ops.ha.set_scheduler_policy import PolicyRoll, atomic_bytes, owned_identity, timestamp
from ops.supervise import ROOT, load_manifest, private_read, private_write

PATHS = frozenset({'backend/app.py','backend/config.py','backend/store.py','backend/migrate.py',
    'backend/retention.py','backend/auxiliary_admission.py','backend/blob_storage.py','backend/result_archival.py',
    'backend/migrations/0009_retention_admission.sql','backend/migrations/0010_auxiliary_metadata_admission.sql',
    'ops/ha/runtime_grants.sql'})
DEPENDENCIES = ('ops/manage.py','ops/supervise.py','ops/ha/set_scheduler_policy.py',
    'ops/ha/deploy_adaptive_worker_source.py','ops/ha/adaptive_qwen235b.py',
    'ops/ha/finalize_api_policy.py','ops/ha/retune_full_gpu.py')
MIGRATION_FILES = ('backend/schema.sql','backend/migrations/0002_sql_completion.sql',
    'backend/migrations/0003_sql_claim.sql','backend/migrations/0004_sql_failure_recovery.sql',
    'backend/migrations/0005_blob_storage.sql','backend/migrations/0006_cost_scheduler.sql',
    'backend/migrations/0007_runtime_registration_guard.sql','backend/migrations/0008_result_archival.sql',
    'backend/migrations/0009_retention_admission.sql','backend/migrations/0010_auxiliary_metadata_admission.sql')
PYTHON_STEMS = {Path(name).stem for name in PATHS if name.endswith('.py')}
BYTECODE = re.compile(r'^(?:'+ '|'.join(sorted(PYTHON_STEMS)) +r')\.[A-Za-z0-9_-]+(?:\.opt-[0-9]+)?\.pyc$')
DEFAULT_POLICY = {'max_retained_tasks':50000,'max_payload_bytes':320*1024**2,
    'max_response_bytes':1024**2,'max_error_bytes':32768}
AUX_ROWS = {'input_files':2000,'upload_reservations':32,'idempotency':4096,
    'blob_generations':2000,'archive_runs':2000,'archive_chunks':20000,
    'input_read_leases':128,'result_read_leases':128}
MINIMUM_100_TASKS = 100*(1024**2+4*32768)  # excludes the new bodies


def digest(raw):return hashlib.sha256(raw).hexdigest()


def owner_dsn(raw):
    value=raw.decode().strip()
    if re.match(r'^(?:export\s+)?DATABASE_URL\s*=',value):dsn=values(raw).get('DATABASE_URL','')
    else:dsn=value  # URL or libpq keyword/value DSN; neither is shell code.
    params=conninfo_to_dict(dsn)
    if (params.get('sslmode')!='verify-full' or params.get('dbname')!='batchforge'
            or not params.get('user') or params['user']=='bf_runtime'
            or not params.get('host') or '-pooler' in params['host']):
        raise ValueError('Existing private direct owner DSN with verify-full required')
    return dsn


def bound_owner_dsn(raw,runtime_dsns):
    """Bind credentials to both API configurations before opening owner SQL."""
    dsn=owner_dsn(raw);owner=conninfo_to_dict(dsn)
    def endpoint(params,pooled):
        host=params.get('host','');port=params.get('port') or '5432'
        if (not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9.-]*',host)
                or params.get('hostaddr') or params.get('service')
                or not re.fullmatch(r'[0-9]{1,5}',port) or not 1<=int(port)<=65535):
            raise ValueError('A single explicit DNS endpoint and port are required')
        if pooled:
            if host.count('-pooler.')>1:raise ValueError('Ambiguous pooled endpoint')
            host=host.replace('-pooler.','.',1)
        if '-pooler' in host:raise ValueError('Ambiguous direct endpoint')
        return host.lower(),int(port),params.get('dbname')
    target=endpoint(owner,False)
    if len(runtime_dsns)!=2:raise ValueError('Both API database configurations must be bound')
    for runtime_dsn in runtime_dsns:
        runtime=conninfo_to_dict(runtime_dsn)
        if runtime.get('user')!='bf_runtime' or owner.get('user')==runtime.get('user') or endpoint(runtime,True)!=target:
            raise ValueError('Owner and API runtime database endpoints or roles differ')
    # Pin the normalized default port instead of allowing an inherited PGPORT
    # to redirect an owner URL whose port was omitted.
    return make_conninfo(dsn,port=str(target[1]))


def archive_sources(path, expected):
    if not re.fullmatch('[0-9a-f]{64}',expected):raise ValueError('Expected bundle SHA256 required')
    raw=private_read(path,4*1024**2)
    if digest(raw)!=expected:raise ValueError('Bundle checksum differs')
    result={}
    with tarfile.open(fileobj=io.BytesIO(raw),mode='r:*') as archive:
        for member in archive:
            if member.name not in PATHS or member.name in result or not member.isreg() or not 0<=member.size<=512*1024:
                raise ValueError('Bundle must contain exactly the reviewed regular storage-admission paths')
            stream=archive.extractfile(member)
            if stream is None:raise ValueError('Missing source stream')
            value=stream.read(512*1024+1)
            if len(value)!=member.size:raise ValueError('Source size differs')
            if member.name.endswith('.py'):compile(value,member.name,'exec',dont_inherit=True)
            result[member.name]=value
    if set(result)!=PATHS:raise ValueError('Incomplete source bundle')
    return result


def flag_bytes(raw, enabled):
    before=values(raw)
    if before.get('SCHEDULER_POLICY')!='cost_aware' or before.get('RESULT_READERS_ENABLED','').lower() not in {'1','true'} or before.get('RESULT_ARCHIVE_ENABLED','false').lower() not in {'0','false'}:
        raise ValueError('Requires cost-aware scheduler, leased readers ON and publisher OFF')
    out=[];found=False
    for line in raw.decode().splitlines(keepends=True):
        key,sep,_=line.strip().removeprefix('export ').strip().partition('=')
        if sep and key=='RETENTION_ADMISSION_ENABLED':
            found=True;out.append('RETENTION_ADMISSION_ENABLED='+str(enabled).lower()+'\n')
        else:out.append(line)
    if not found:
        if out and not out[-1].endswith(('\n','\r')):out[-1]+='\n'
        out.append('RETENTION_ADMISSION_ENABLED='+str(enabled).lower()+'\n')
    result=''.join(out).encode();after=values(result)
    if {k:v for k,v in before.items() if k!='RETENTION_ADMISSION_ENABLED'}!={k:v for k,v in after.items() if k!='RETENTION_ADMISSION_ENABLED'}:
        raise ValueError('Protected environment changed')
    return result


def private_sources(root):
    for parent in ('backend','backend/migrations','ops','ops/ha'):
        info=(root/parent).lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid!=os.geteuid() or info.st_mode&0o022:
            raise ValueError('Source directory is unsafe')


def cache_entries(root):
    directory=root/'backend/__pycache__'
    if not directory.exists() and not directory.is_symlink():return []
    info=directory.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid!=os.geteuid() or info.st_mode&0o022:
        raise ValueError('Backend cache directory is unsafe')
    result=[]
    for path in directory.iterdir():
        if not BYTECODE.fullmatch(path.name):continue
        info=path.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_uid!=os.geteuid() or info.st_mode&0o022:
            raise ValueError('Scoped backend cache is unsafe')
        result.append((path,info.st_dev,info.st_ino))
    return result


def evict_cache(root):
    entries=cache_entries(root)
    for path,device,inode in entries:
        info=path.lstat()
        if (info.st_dev,info.st_ino)!=(device,inode):raise RuntimeError('Backend cache changed during eviction')
        path.unlink()
    return [path.name for path,_,_ in entries]


@contextmanager
def api_locks(root):
    with ExitStack() as stack:
        for name in ('a','b'):
            fd=os.open(root/'.runtime'/('api-'+name+'.lock'),os.O_RDWR|os.O_CREAT|os.O_NOFOLLOW,0o600)
            info=os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_uid!=os.geteuid() or info.st_mode&0o077:
                os.close(fd);raise ValueError('API role lock must be private and owned')
            stream=stack.enter_context(os.fdopen(fd,'w'))
            fcntl.flock(stream,fcntl.LOCK_EX|fcntl.LOCK_NB)
        yield


def known_migrations(root,sources):
    return [(version,sources.get(name,(root/name).read_bytes() if version<=8 else b''))
        for version,name in enumerate(MIGRATION_FILES,1)]


def validate_prefix(conn,migrations,version):
    actual=conn.execute('SELECT version,checksum FROM public.batchforge_schema_migrations ORDER BY version').fetchall()
    if actual!=[(n,digest(statement)) for n,statement in migrations if n<=version]:
        raise RuntimeError('Exact immutable schema prefix differs')


def ledger_invariants(conn):
    mismatch=conn.execute('''SELECT count(*) FROM public.tenant_retention_usage u LEFT JOIN (
        SELECT tenant_id,count(*) n,sum(retention_body_bytes+retention_response_bytes+retention_error_bytes) b
        FROM public.tasks GROUP BY tenant_id) t ON t.tenant_id=u.tenant_id
        WHERE u.retained_tasks<>coalesce(t.n,0) OR u.charged_payload_bytes<>coalesce(t.b,0)''').fetchone()[0]
    missing=conn.execute('''SELECT count(*) FROM public.tenants t LEFT JOIN public.tenant_retention_usage u ON u.tenant_id=t.id
        LEFT JOIN public.tenant_retention_limits l ON l.tenant_id=t.id WHERE u.tenant_id IS NULL OR l.tenant_id IS NULL''').fetchone()[0]
    under=conn.execute('''SELECT count(*) FROM public.tasks t WHERE
        (NOT retention_payload_cleared AND retention_body_bytes<public.batchforge_json_bytes(body))
        OR retention_response_bytes<public.batchforge_json_bytes(response)
        OR retention_error_bytes<public.batchforge_json_bytes(error)+coalesce((SELECT sum(public.batchforge_json_bytes(a.error))
            FROM public.attempts a WHERE a.task_id=t.id),0)''').fetchone()[0]
    if mismatch or missing or under:raise RuntimeError('Retention ledger invariant failed')
    all_expected={}
    for resource,table,predicate in [('input_files','files',"kind='input'"),
            ('upload_reservations','blob_upload_reservations','true'),('idempotency','idempotency','true'),
            ('blob_generations','blob_objects','true'),('archive_runs','result_archive_runs','true'),
            ('archive_chunks','result_archive_chunks','true'),('input_read_leases','blob_read_leases','true'),
            ('result_read_leases','result_read_leases','true')]:
        amount='coalesce(sum(auxiliary_reserved_bytes),0)' if resource=='idempotency' else '0'
        query=sql.SQL('SELECT tenant_id,count(*),{} FROM {} WHERE {} GROUP BY tenant_id').format(
            sql.SQL(amount),sql.Identifier(table),sql.SQL(predicate))
        for tenant,n,b in conn.execute(query):
            all_expected[(tenant,resource)]=(n,b)
            if resource=='upload_reservations':
                old=all_expected.get((tenant,'input_files'),(0,0))
                all_expected[(tenant,'input_files')]=(old[0]+n,0)
    all_actual={(t,r):(n,b) for t,r,n,b in conn.execute('SELECT tenant_id,resource,rows,bytes FROM tenant_auxiliary_usage')}
    if any(all_actual.get(k,(0,0))!=all_expected.get(k,(0,0)) for k in set(all_actual)|set(all_expected)):
        raise RuntimeError('Auxiliary ledger invariant failed')
    expected={row[0]:(row[1],row[2]) for row in conn.execute('''SELECT resource,
        CASE resource WHEN 'input_files' THEN (SELECT count(*) FROM files WHERE tenant_id='default' AND kind='input')+
          (SELECT count(*) FROM blob_upload_reservations WHERE tenant_id='default')
        WHEN 'upload_reservations' THEN (SELECT count(*) FROM blob_upload_reservations WHERE tenant_id='default')
        WHEN 'idempotency' THEN (SELECT count(*) FROM idempotency WHERE tenant_id='default')
        WHEN 'blob_generations' THEN (SELECT count(*) FROM blob_objects WHERE tenant_id='default')
        WHEN 'archive_runs' THEN (SELECT count(*) FROM result_archive_runs WHERE tenant_id='default')
        WHEN 'archive_chunks' THEN (SELECT count(*) FROM result_archive_chunks WHERE tenant_id='default')
        WHEN 'input_read_leases' THEN (SELECT count(*) FROM blob_read_leases WHERE tenant_id='default')
        WHEN 'result_read_leases' THEN (SELECT count(*) FROM result_read_leases WHERE tenant_id='default') END,
        CASE WHEN resource='idempotency' THEN (SELECT coalesce(sum(auxiliary_reserved_bytes),0) FROM idempotency WHERE tenant_id='default') ELSE 0 END
        FROM tenant_auxiliary_limits WHERE tenant_id='default' ''')}
    actual={r:(n,b) for r,n,b in conn.execute("SELECT resource,rows,bytes FROM tenant_auxiliary_usage WHERE tenant_id='default'")}
    if expected!=actual or set(actual)!=set(AUX_ROWS):raise RuntimeError('Auxiliary ledger invariant failed')
    count,charged=conn.execute("SELECT retained_tasks,charged_payload_bytes FROM tenant_retention_usage WHERE tenant_id='default'").fetchone()
    if count+100>DEFAULT_POLICY['max_retained_tasks'] or charged+MINIMUM_100_TASKS>DEFAULT_POLICY['max_payload_bytes']:
        raise RuntimeError('Default policy cannot reserve the next100 minimum task budgets')
    for resource,(n,b) in actual.items():
        if n>AUX_ROWS[resource] or resource=='idempotency' and b>128*1024**2:
            raise RuntimeError('Default auxiliary policy below existing usage')
    return {'retention_ledger_mismatches':mismatch,'retention_undercharged_tasks':under,
        'retention_missing_tenant_ledgers':missing,'auxiliary_ledger_mismatches':0,
        'default_retained_tasks':count,'default_payload_charge_bytes':charged,
        'minimum_100_tasks_excluding_bodies':MINIMUM_100_TASKS,
        'remaining_after_100_minimum_excluding_new_bodies':DEFAULT_POLICY['max_payload_bytes']-charged-MINIMUM_100_TASKS,
        'default_auxiliary_usage':{r:{'rows':n,'bytes':b} for r,(n,b) in actual.items()}}


def migrate_atomic(conn,migrations,grants,before_commit):
    """The caller's private owner connection is closed on any pre-commit error."""
    try:
        conn.execute('BEGIN')
        conn.execute('SET LOCAL lock_timeout=\'10s\'')
        conn.execute('SET LOCAL statement_timeout=\'120s\'')
        conn.execute('SELECT pg_advisory_xact_lock(%s)',(1936150117,))
        validate_prefix(conn,migrations,8)
        # Preacquire the complete v10 fail-fast window BEFORE immutable v9's
        # relation gates. Never wait while holding a subset across a writer's
        # tenant/foreign-key/table lock order.
        conn.execute('LOCK TABLE public.tenants IN EXCLUSIVE MODE NOWAIT')
        conn.execute('''LOCK TABLE public.batches,public.tasks,public.attempts,public.files,public.idempotency,
            public.blob_upload_reservations,public.blob_objects,public.blob_read_leases,
            public.result_archive_runs,public.result_archive_files,public.result_archive_chunks,
            public.result_read_leases IN SHARE ROW EXCLUSIVE MODE NOWAIT''')
        if conn.execute("SELECT count(*) FROM tasks WHERE status IN ('pending','running')").fetchone()[0]:raise RuntimeError('Active work during migration')
        for version,statement in migrations:
            if version>8:
                conn.execute(statement.decode())
                conn.execute('INSERT INTO batchforge_schema_migrations(version,checksum) VALUES(%s,%s)',(version,digest(statement)))
        # Runtime grants have one fixed reviewed database identifier. Retarget
        # only that exact token for isolated integration databases.
        database=conn.execute('SELECT current_database()').fetchone()[0]
        text=grants.decode()
        if text.count('GRANT CONNECT ON DATABASE batchforge TO bf_runtime;')!=1:raise ValueError('Unexpected grant script')
        text=text.replace('GRANT CONNECT ON DATABASE batchforge TO bf_runtime;',
            sql.SQL('GRANT CONNECT ON DATABASE {} TO bf_runtime;').format(sql.Identifier(database)).as_string(conn))
        conn.execute(text)
        conn.execute('''UPDATE tenant_retention_limits SET max_retained_tasks=50000,max_payload_bytes=335544320,
            max_response_bytes=1048576,max_error_bytes=32768 WHERE tenant_id='default' ''')
        for resource,maximum in AUX_ROWS.items():
            conn.execute("UPDATE tenant_auxiliary_limits SET max_rows=%s WHERE tenant_id='default' AND resource=%s",(maximum,resource))
        conn.execute("UPDATE tenant_auxiliary_limits SET max_bytes=134217728,max_row_bytes=65536 WHERE tenant_id='default' AND resource='idempotency'")
        invariants=ledger_invariants(conn)
        validate_prefix(conn,migrations,10)
        if conn.execute("SELECT count(*) FROM tasks WHERE status IN ('pending','running')").fetchone()[0]:raise RuntimeError('Active work before commit')
        before_commit(invariants)  # Once COMMIT is attempted, timeout is ambiguous.
        conn.commit()
        return invariants
    except BaseException:
        conn.rollback()
        raise


def proofs(spec,migrations,now=None):
    now=now or datetime.now(timezone.utc)
    if set(spec)!={'fresh_backup','historical_restore','production'}:raise ValueError('Three exact proof records are required')
    records={};hashes={}
    for name,entry in spec.items():
        if set(entry)!={'path','sha256'}:raise ValueError('Proof path and reviewed SHA256 required')
        raw=private_read(Path(entry['path']),2*1024**2)
        if digest(raw)!=entry['sha256']:raise ValueError('Proof hash differs')
        records[name]=json.loads(raw);hashes[name]=digest(raw)
    fresh,restore,production=(records[k] for k in ('fresh_backup','historical_restore','production'))
    observed=datetime.fromisoformat(fresh.get('observed_at','').replace('Z','+00:00'))
    if (observed.tzinfo is None or not 0<=(now-observed).total_seconds()<=3600 or not fresh.get('compatible')
            or not fresh.get('archive_list_verified') or fresh.get('database_modified') is not False
            or not fresh.get('off_pod_copy_verified') or fresh.get('off_pod_copy_sha256')!=fresh.get('snapshot_sha256')
            or fresh.get('off_pod_copy_mode')!='0o600' or not 0<fresh.get('snapshot_bytes',0)<=512*1024**2):
        raise ValueError('Fresh compatible, verified off-Pod backup required')
    snapshot=Path(fresh['snapshot_path']);raw=private_read(snapshot,512*1024**2)
    if len(raw)!=fresh['snapshot_bytes'] or digest(raw)!=fresh['snapshot_sha256']:raise ValueError('Fresh backup archive differs')
    if (not restore.get('passed') or not restore.get('restore_verified') or restore.get('schema_version')!=8
            or not restore.get('temporary_cluster_stopped') or not restore.get('temporary_cluster_removed')
            or restore.get('migration_checksums')!={str(v):digest(statement) for v,statement in migrations if v<=8}
            or any(restore.get(k)!=0 for k in ('batch_count_state_usage_mismatches','accepted_attempt_count_usage_mismatches',
                'tenant_queue_pending_mismatches','unvalidated_constraints'))):raise ValueError('Historical schema8 restore proof differs')
    if (not production.get('passed') or not production.get('restore_verified_before_purge')
            or production.get('backup_sha256')!=restore.get('snapshot_sha256') or production.get('task_payloads_cleared')!=20
            or not production.get('attempt_counts_and_usage_unchanged') or not production.get('batch_counters_and_identity_unchanged')
            or set(production.get('byte_identical_on_ports',[]))!={8080,8082,8083}
            or not production.get('archive_publication_still_disabled')
            or not re.fullmatch('[0-9a-f]{64}',production.get('output_sha256',''))
            or not 0<production.get('download_bytes',0)<=16*1024**2):raise ValueError('Historical exact20 production proof differs')
    return {'sha256':hashes,'fresh_snapshot_sha256':fresh['snapshot_sha256'],
        'output_file_id':production['output_file_id'],'output_sha256':production['output_sha256'],
        'output_bytes':production['download_bytes']}


class StorageRoll(PolicyRoll):
    def __init__(self, *, run,sources,bundle_sha,proof_spec,owner_env,access_env,previous=None,
            migrate=None,observe=None,schema_check=None,writer=atomic_source,**kwargs):
        super().__init__(policy='cost_aware',**kwargs)
        self.run,self.sources,self.bundle_sha=run,sources,bundle_sha
        self.owner_env,self.access_env=owner_env,access_env
        self.migrations=known_migrations(self.root,sources)
        self.proof_spec=proof_spec;self.proofs=proofs(proof_spec,self.migrations)
        self.migrate=migrate or self._migrate
        self.observe=observe or self._observe
        self.schema_check=schema_check or self._schema_check
        self.writer=writer;self.commit_attempted=False;self.copied=False
        private_sources(self.root);cache_entries(self.root)
        self.snapshot={};self.original_sources={}
        for name in sorted(PATHS):
            state,raw=source_state(self.root/name);self.snapshot[name]=state
            if raw is not None:self.original_sources[name]=raw
        self.off={p:flag_bytes(raw,False) for p,raw in self.original.items()}
        self.on={p:flag_bytes(raw,True) for p,raw in self.original.items()}
        self.watchdog_bytes=private_read(Path(self.watchdog['env_file']))
        self.expected={self.key(s):self.identity(s) for s in [*self.services,self.watchdog]}
        if not all(self.expected.values()):raise ValueError('Every owned service must be live during preparation')
        self.guard_sources={name:digest((self.root/name).read_bytes()) for name in (*MIGRATION_FILES[:8],*DEPENDENCIES)}
        self.guard_sources[str(Path(__file__).resolve())]=digest(Path(__file__).read_bytes())
        self.assert_drained();self.schema_check(8)
        binding={'bundle_sha256':bundle_sha,'source_sha256':{n:digest(raw) for n,raw in sources.items()},
            'original_sources':self.snapshot,'manifest_sha256':digest(self.original_manifest),
            'environment_sha256':{str(p):digest(raw) for p,raw in self.original.items()},
            'protected_environment_sha256':{str(p):checksum for p,checksum in self.protected_env.items()},'watchdog_environment_sha256':digest(self.watchdog_bytes),
            'owned_identities':self.expected,'guard_sources':self.guard_sources,'proof_evidence':self.proofs}
        if previous is not None:
            if previous.get('phase')!='prepared' or previous.get('binding')!=binding:raise RuntimeError('Prepared operation drift; refusing replay')
            for name,raw in self.original_sources.items():
                if private_read(run/'original'/name)!=raw:raise RuntimeError('Private original snapshot changed')
            for index,raw in enumerate(self.original.values()):
                if private_read(run/f'api-{index}.env')!=raw:raise RuntimeError('Private API environment snapshot changed')
            self.report=previous
        else:
            if any(run.iterdir()):raise RuntimeError('Operation directory is not empty')
            for name,raw in self.original_sources.items():
                path=run/'original'/name;path.parent.mkdir(parents=True,exist_ok=True,mode=0o700);atomic_bytes(path,raw)
            for index,(path,raw) in enumerate(self.original.items()):atomic_bytes(run/f'api-{index}.env',raw)
            self.report={'phase':'prepared','prepared_at':timestamp(),'operation':'schema8-to10-storage-admission',
                'prepared_active_tasks':0,'prepared_schema_prefix':8,
                'binding':binding,'events':[],'changed_environment_keys':['RETENTION_ADMISSION_ENABLED'],
                'default_policy':DEFAULT_POLICY,'default_auxiliary_rows':AUX_ROWS,
                'limitations':['Logical JSON accounting is not physical database/TOAST/index/WAL size',
                    'Post-commit repair is forward-only; old schema8 registry is never restored',
                    'Minimum100-task headroom excludes the unknown new input bodies',
                    'Publisher remains OFF; this does not activate automatic history deletion']}
            self.save()

    def save(self):private_write(self.report_path,self.report)

    def guard(self):
        super().guard()
        if private_read(Path(self.watchdog['env_file']))!=self.watchdog_bytes:raise RuntimeError('Watchdog environment changed')
        for service in [*self.services,self.watchdog]:
            if self.identity(service)!=self.expected[self.key(service)]:raise RuntimeError('Owned service identity changed')
        for path,checksum in self.guard_sources.items():
            actual=Path(path) if Path(path).is_absolute() else self.root/path
            if digest(actual.read_bytes())!=checksum:raise RuntimeError('Guarded source changed')
        if proofs(self.proof_spec,self.migrations)!=self.proofs:raise RuntimeError('Proof evidence changed')
        for name in PATHS:
            state,_=source_state(self.root/name)
            if self.copied:
                if not state['exists'] or state['sha256']!=digest(self.sources[name]):raise RuntimeError('Deployed source changed')
            elif state!=self.snapshot[name]:raise RuntimeError('Original source changed')
        for api in self.apis:
            path=Path(api['env_file'])
            if private_read(path) not in (self.original[path],self.off[path],self.on[path]):raise RuntimeError('Foreign API environment change')
        if not all(self.manager.readiness(s) for s in self.protected):raise RuntimeError('Protected service not ready')

    def invoke(self,service,action):
        key=self.key(service);actual=self.identity(service)
        if actual is not None and actual!=self.expected[key]:raise RuntimeError('Refuse foreign owned-record replacement')
        value=super().invoke(service,action)
        self.expected[key]=self.identity(service)
        if action=='stop' and self.expected[key] is not None:raise RuntimeError('Owned service still alive after stop')
        if action=='start' and self.expected[key] is None:raise RuntimeError('Owned service absent after start')
        return value

    def _copy(self,rollback=False):
        private_sources(self.root)
        with api_locks(self.root):
            cache_entries(self.root)
            for name in PATHS:
                state,_=source_state(self.root/name)
                if state['exists'] and state['sha256'] not in {digest(self.sources[name]),self.snapshot[name].get('sha256')}:
                    raise RuntimeError('Refuse foreign source overwrite')
            for name in sorted(PATHS):
                path=self.root/name
                if rollback and name not in self.original_sources:path.unlink(missing_ok=True);continue
                raw=self.original_sources[name] if rollback else self.sources[name]
                mode=self.snapshot[name].get('mode',0o644)
                old=self.snapshot[name].get('mtime_ns',0)
                self.writer(path,raw,mode,old if rollback else max(time.time_ns(),old+2_000_000_000))
                if name.endswith('.py'):compile(path.read_bytes(),name,'exec',dont_inherit=True)
            self.report['bytecode_evicted_on_'+('rollback' if rollback else 'apply')]=evict_cache(self.root)
        self.copied=not rollback

    def _migrate(self):
        runtime_dsns=[values(private_read(Path(api['env_file'])))['DATABASE_URL'] for api in self.apis]
        dsn=bound_owner_dsn(private_read(self.owner_env),runtime_dsns)
        with psycopg.connect(dsn,autocommit=True,connect_timeout=10) as conn:
            if conn.execute('SELECT current_user').fetchone()[0]=='bf_runtime':raise RuntimeError('Runtime is not migration owner')
            return migrate_atomic(conn,self.migrations,self.sources['ops/ha/runtime_grants.sql'],self._mark_commit)

    def _schema_check(self,version):
        runtime=values(private_read(Path(self.apis[0]['env_file'])))
        with psycopg.connect(runtime['DATABASE_URL'],connect_timeout=5,
                options='-c default_transaction_read_only=on -c statement_timeout=5000') as conn:
            validate_prefix(conn,self.migrations,version)
            if conn.execute('SELECT current_user').fetchone()[0]!='bf_runtime':raise RuntimeError('API must use runtime credentials')

    def _mark_commit(self,invariants):
        self.commit_attempted=True
        self.report.update(phase='commit_attempted',commit_attempted_at=timestamp(),precommit_invariants=invariants)
        self.save()

    def _observe(self,service,enabled,schema):
        current=self.identity(service)
        if service['role']=='api':
            environment={}
            for entry in Path(f'/proc/{current["pid"]}/environ').read_bytes().split(b'\0'):
                key,sep,value=entry.partition(b'=')
                if sep:
                    if key.decode() in environment:raise RuntimeError('Duplicate effective environment assignment')
                    environment[key.decode()]=value.decode()
            expected=values((self.on if enabled else self.off)[Path(service['env_file'])])
            if self.identity(service)!=current or any(environment.get(k)!=v for k,v in expected.items()):raise RuntimeError('Effective API environment differs')
        else:
            expected=values((self.on if enabled else self.off)[Path(self.apis[0]['env_file'])])
        with psycopg.connect(expected['DATABASE_URL'],connect_timeout=5,
                options='-c default_transaction_read_only=on -c statement_timeout=5000') as conn:
            validate_prefix(conn,self.migrations,schema)
            if conn.execute('SELECT current_user').fetchone()[0]!='bf_runtime':raise RuntimeError('API must use runtime credentials')
        access=values(private_read(self.access_env))['BATCHFORGE_API_KEY']
        total=0;sha=hashlib.sha256()
        with httpx.Client(trust_env=False,timeout=10,headers={'Authorization':'Bearer '+access}) as client:
            ready=client.get(f'http://127.0.0.1:{service["port"]}/readyz')
            body=ready.json()
            if ready.status_code!=200 or body.get('status')!='ready' or service['role']=='gateway' and body.get('ready_replicas')!=2:
                raise RuntimeError('Control plane not ready')
            with client.stream('GET',f'http://127.0.0.1:{service["port"]}/v1/files/{self.proofs["output_file_id"]}/content') as response:
                response.raise_for_status()
                for chunk in response.iter_bytes():
                    total+=len(chunk)
                    if total>self.proofs['output_bytes']:raise RuntimeError('Historical result exceeds proof bound')
                    sha.update(chunk)
        if (total,sha.hexdigest())!=(self.proofs['output_bytes'],self.proofs['output_sha256']):raise RuntimeError('Historical result differs')
        if self.identity(service)!=current:raise RuntimeError('Service identity changed during result verification')
        return {'schema_prefix':schema,'retention_enabled':enabled,'ready':True,'old20_sha256':sha.hexdigest()}

    def _roll(self,enabled,schema):
        for api in self.apis:
            self.guard();self.assert_drained();self.invoke(api,'stop')
            atomic_bytes(Path(api['env_file']),(self.on if enabled else self.off)[Path(api['env_file'])])
            self.invoke(api,'start');self.await_ready(api)
            evidence=self.observe(api,enabled,schema)
            self.report.setdefault('api_observations',[]).append({'at':timestamp(),'service':self.key(api),**evidence});self.save()
            self.guard()

    def apply(self):
        self.guard();self.assert_drained();self.report.update(phase='applying',started_at=timestamp());self.save()
        maintenance=False;source_attempted=False
        try:
            maintenance=True;self.invoke(self.watchdog,'stop');self.guard();self.assert_drained()
            source_attempted=True;self._copy();self.guard()
            self._roll(False,8)
            self.report['migration_invariants']=self.migrate()
            if not self.commit_attempted:raise RuntimeError('Migration did not bind the commit boundary')
            self.report.update(phase='committed',committed_at=timestamp());self.save()
            self._roll(True,10)
            for gateway in [s for s in self.protected if s['role']=='gateway']:
                self.report['gateway_observation']=self.observe(gateway,True,10)
            self.guard();self.assert_drained()
            self.report.update(phase='complete',completed_at=timestamp(),non_api_identities_unchanged=True)
        except BaseException as error:
            self.report.update(phase='forward_repair_required' if self.commit_attempted else 'rolling_back',failure_type=type(error).__name__);self.save()
            try:
                # Stop only the two owned APIs. Never restart old code after a
                # COMMIT attempt, even when its acknowledgement was lost.
                for api in self.apis:self.invoke(api,'stop')
                if not self.commit_attempted and source_attempted:self._copy(rollback=True)
                for api in self.apis:
                    path=Path(api['env_file'])
                    if private_read(path) not in (self.original[path],self.off[path],self.on[path]):raise RuntimeError('Refuse foreign API environment rollback')
                    atomic_bytes(path,self.off[path] if self.commit_attempted else self.original[path])
                    self.invoke(api,'start');self.await_ready(api)
                self.guard()
                if not self.commit_attempted:self.report['phase']='rolled_back'
            except BaseException as recovery:
                self.report.update(phase='manual_recovery_required',recovery_failure_type=type(recovery).__name__)
                # Unvalidated APIs must not be automatically restarted.
                maintenance=False
            raise
        finally:
            if maintenance:
                try:self.invoke(self.watchdog,'start');self.report['watchdog_restored']=True
                except BaseException as error:
                    self.report.update(phase='manual_recovery_required',watchdog_restored=False,watchdog_failure_type=type(error).__name__)
                    self.save();raise
            self.save()
        return self.report


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive',type=Path,required=True);parser.add_argument('--sha256',required=True)
    parser.add_argument('--proof-spec',type=Path,required=True)
    parser.add_argument('--owner-env',type=Path,default=ROOT.parent/'neon-owner.env')
    parser.add_argument('--access-env',type=Path,required=True)
    parser.add_argument('--operation',default='storage-admission-v10-v1');parser.add_argument('--apply',action='store_true')
    args=parser.parse_args();os.umask(0o077)
    run=private_operation_directory(ROOT,args.operation)
    fd=os.open(ROOT/'.runtime/storage-admission.lock',os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,'w') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        previous=json.loads(private_read(run/'report.json')) if (run/'report.json').exists() else None
        if args.apply and previous is None:raise RuntimeError('Immutable preparation required before apply')
        manifest,services,_=load_manifest(ROOT/'.runtime/services-primary.json')
        watchdog={'role':'supervisor','name':'primary','env_file':str(ROOT.parent/'supervisor.env'),'python':str(ROOT.parent/'venv/bin/python')}
        def identity(service):
            try:return owned_identity(ROOT,service)
            except RuntimeError:return None
        roll=StorageRoll(root=ROOT,run=run,services=services,watchdog=watchdog,manifest_path=manifest,
            report_path=run/'report.json',sources=archive_sources(args.archive,args.sha256),bundle_sha=args.sha256,
            proof_spec=json.loads(private_read(args.proof_spec)),owner_env=args.owner_env,access_env=args.access_env,
            previous=previous,identity=identity)
        if args.apply:roll.apply()
    print(json.dumps({'report':str(run/'report.json'),'phase':roll.report['phase']}))


if __name__=='__main__':
    try:main()
    except Exception as error:
        print(json.dumps({'error_type':type(error).__name__}),file=sys.stderr);raise SystemExit(1) from None
