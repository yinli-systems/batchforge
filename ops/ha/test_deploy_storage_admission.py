"""Source/rollback control flow and real atomic v8 -> v10 database proofs."""
import hashlib
from datetime import datetime,timezone
import fcntl
import io
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import tarfile

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
import pytest

from backend.migrate import MIGRATIONS,migrate_database
from ops.ha import deploy_storage_admission as deploy
from ops.ha.set_scheduler_policy import atomic_bytes


def bundle(tmp_path,entries=None):
    memory=io.BytesIO()
    with tarfile.open(fileobj=memory,mode='w:gz') as archive:
        for name,raw,kind in entries or [(p,b'VALUE=2\n' if p.endswith('.py') else b'-- reviewed\n',tarfile.REGTYPE) for p in deploy.PATHS]:
            info=tarfile.TarInfo(name);info.type=kind;info.size=len(raw)
            archive.addfile(info,io.BytesIO(raw))
    raw=memory.getvalue();path=tmp_path/'source.tar.gz';path.write_bytes(raw);path.chmod(0o600)
    return path,hashlib.sha256(raw).hexdigest()


def test_archive_is_exact_hashed_regular_and_python_compiled(tmp_path):
    path,sha=bundle(tmp_path)
    assert set(deploy.archive_sources(path,sha))==deploy.PATHS
    with pytest.raises(ValueError):deploy.archive_sources(path,'0'*64)
    records=[(p,b'VALUE=2\n',tarfile.REGTYPE) for p in deploy.PATHS]
    for extra in [('backend/._app.py',b'',tarfile.REGTYPE),('../escape',b'',tarfile.REGTYPE),
            ('backend/app.py',b'bad syntax :',tarfile.REGTYPE),('backend/new.py',b'',tarfile.SYMTYPE)]:
        path,sha=bundle(tmp_path,records+[extra])
        with pytest.raises((ValueError,SyntaxError)):deploy.archive_sources(path,sha)


def test_flag_is_only_changed_setting_and_does_not_enable_publisher():
    raw=b"# comment\nDATABASE_URL='literal sensitive $() text'\nSCHEDULER_POLICY=cost_aware\nRESULT_READERS_ENABLED=true\nRESULT_ARCHIVE_ENABLED=false\n"
    result=deploy.flag_bytes(raw,True)
    before,after=deploy.values(raw),deploy.values(result)
    assert {k:v for k,v in after.items() if k!='RETENTION_ADMISSION_ENABLED'}==before
    assert after['RETENTION_ADMISSION_ENABLED']=='true'
    with pytest.raises(ValueError):deploy.flag_bytes(raw+b'RESULT_READERS_ENABLED=false\n',False)


def test_migration_owner_connection_refuses_pooler_runtime_role_and_unverified_tls():
    direct='postgresql://owner:fixture-password@ep-fixture.neon.tech/batchforge?sslmode=verify-full'
    assert deploy.owner_dsn(direct.encode())==direct
    assert deploy.owner_dsn(('DATABASE_URL='+direct+'\n').encode())==direct
    for value in [direct.replace('ep-fixture.','ep-fixture-pooler.'),direct.replace('owner:','bf_runtime:'),
            direct.replace('verify-full','require'),direct.replace('/batchforge?','/another?')]:
        with pytest.raises(ValueError,match='private direct owner DSN'):deploy.owner_dsn(value.encode())


def test_owner_endpoint_binding_accepts_exact_runtime_pooler_and_default_port():
    from psycopg.conninfo import conninfo_to_dict
    owner='postgresql://owner:fixture-password@ep-fixture.c-8.neon.tech/batchforge?sslmode=verify-full'
    runtime=owner.replace('owner:','bf_runtime:')
    pooled=runtime.replace('ep-fixture.','ep-fixture-pooler.').replace('/batchforge',':5432/batchforge')
    result=deploy.bound_owner_dsn(owner.encode(),[runtime,pooled])
    assert conninfo_to_dict(result)['port']=='5432'
    assert conninfo_to_dict(result)['host']=='ep-fixture.c-8.neon.tech'
    keyword=b"host=ep-fixture.c-8.neon.tech port=5432 dbname=batchforge user=owner password='fixture with space' sslmode=verify-full"
    assert deploy.owner_dsn(keyword)==keyword.decode()
    assert conninfo_to_dict(deploy.bound_owner_dsn(keyword,[runtime,pooled]))['user']=='owner'


@pytest.mark.parametrize('change', ['host','port','dbname','role','multihost','multiport','hostaddr','service'])
def test_owner_endpoint_binding_refuses_foreign_database_before_owner_connect(change):
    owner='postgresql://owner:fixture-password@ep-fixture.c-8.neon.tech/batchforge?sslmode=verify-full'
    runtime=owner.replace('owner:','bf_runtime:')
    from psycopg.conninfo import make_conninfo
    wrong={'host':make_conninfo(runtime,host='ep-other.c-8.neon.tech'),
        'port':make_conninfo(runtime,port='6543'),'dbname':make_conninfo(runtime,dbname='other'),
        'role':make_conninfo(runtime,user='owner'),'multihost':make_conninfo(runtime,host='ep-fixture.c-8.neon.tech,ep-other.c-8.neon.tech'),
        'multiport':make_conninfo(runtime,port='5432,5432'),'hostaddr':make_conninfo(runtime,hostaddr='127.0.0.1'),
        'service':make_conninfo(runtime,service='another-target')}[change]
    with pytest.raises(ValueError):deploy.bound_owner_dsn(owner.encode(),[runtime,wrong])


def test_wrong_owner_endpoint_is_refused_before_any_owner_connection(prepared,monkeypatch):
    root,run,ids,manager,make=prepared
    roll=make();owner=root/'.runtime/owner.env'
    atomic_bytes(owner,b'postgresql://owner:fixture@ep-other.neon.tech/batchforge?sslmode=verify-full')
    roll.owner_env=owner
    for api in roll.apis:
        atomic_bytes(Path(api['env_file']),b'DATABASE_URL=postgresql://bf_runtime:fixture@ep-right.neon.tech/batchforge?sslmode=verify-full\n')
    def must_not_connect(*args,**kwargs):raise AssertionError('Owner SQL must not be opened for another endpoint')
    monkeypatch.setattr(deploy.psycopg,'connect',must_not_connect)
    with pytest.raises(ValueError,match='endpoints or roles differ'):roll._migrate()


class Manager:
    def __init__(self,ids,root):self.ids,self.root=ids,root;self.counter=1000;self.calls=[];self.fail=None
    def invoke(self,service,action):
        key=service['role']+'-'+service['name'];self.calls.append((key,action))
        if self.fail==(key,action):self.fail=None;return None,'injected'
        if action=='stop':self.ids[key]=None
        if action=='start':
            self.counter+=1;self.ids[key]={'pid':self.counter,'start_ticks':str(self.counter),'cwd':str(self.root)}
        return {'status':action,'pid':self.ids[key]['pid'] if self.ids[key] else None,'running':self.ids[key] is not None},None
    def readiness(self,service):return True


@pytest.fixture
def prepared(tmp_path,monkeypatch):
    root=tmp_path/'app';runtime=root/'.runtime';runtime.mkdir(parents=True)
    for name in ('backend/migrations','ops/ha'):(root/name).mkdir(parents=True)
    for name in deploy.DEPENDENCIES:(root/name).write_bytes(b'# original helper\n')
    for name in deploy.PATHS|set(deploy.MIGRATION_FILES):
        if name not in {'backend/retention.py','backend/auxiliary_admission.py','backend/migrations/0009_retention_admission.sql','backend/migrations/0010_auxiliary_metadata_admission.sql'}:
            (root/name).write_bytes(b'VALUE=1\n' if name.endswith('.py') else b'-- original\n')
    services=[]
    for role,name,port in [('api','a',8082),('api','b',8083),('gateway','edge',8080),('engine','qwen235b',8335),('worker','qwen235b',9106)]:
        env=runtime/(role+'-'+name+'.env')
        atomic_bytes(env,b'SCHEDULER_POLICY=cost_aware\nRESULT_READERS_ENABLED=true\nRESULT_ARCHIVE_ENABLED=false\n')
        services.append({'role':role,'name':name,'port':port,'env_file':str(env)})
    watchdog={'role':'supervisor','name':'primary','env_file':str(runtime/'watchdog.env')}
    atomic_bytes(Path(watchdog['env_file']),b'SUPERVISOR_MANIFEST=private\n')
    manifest=runtime/'services.json';atomic_bytes(manifest,json.dumps({'services':services}).encode())
    ids={s['role']+'-'+s['name']:{'pid':i+100,'start_ticks':str(i),'cwd':str(root)} for i,s in enumerate([*services,watchdog])}
    manager=Manager(ids,root)
    monkeypatch.setattr(deploy,'proofs',lambda *a,**k:{'output_file_id':'old20','output_sha256':'a'*64,'output_bytes':1})
    source={p:b'VALUE=2\n' if p.endswith('.py') else b'-- candidate\n' for p in deploy.PATHS}
    run=runtime/'storage-op';run.mkdir(mode=0o700)
    def make(**kwargs):
        return deploy.StorageRoll(root=root,services=services,watchdog=watchdog,manifest_path=manifest,
            report_path=run/'report.json',run=run,sources=source,bundle_sha='1'*64,proof_spec={},
            owner_env=runtime/'never-read-owner.env',access_env=runtime/'access.env',
            manager=manager,identity=lambda s:ids[s['role']+'-'+s['name']],assert_drained=lambda:None,
            observe=kwargs.pop('observe',lambda *a:{'ready':True}),schema_check=lambda _:None,**kwargs)
    return root,run,ids,manager,make


def test_prepare_only_has_private_snapshots_and_replay_rejects_identity_drift(prepared):
    root,run,ids,manager,make=prepared
    roll=make();assert manager.calls==[] and roll.report['phase']=='prepared'
    assert (run/'original/backend/app.py').stat().st_mode&0o777==0o600
    assert (root/'backend/app.py').read_bytes()==b'VALUE=1\n'
    before=(run/'report.json').read_bytes();ids['worker-qwen235b']['start_ticks']='foreign'
    with pytest.raises(RuntimeError):make(previous=json.loads(before))
    assert (run/'report.json').read_bytes()==before


def test_success_rolls_compatible_off_then_commits_then_on_without_engine_worker_actions(prepared):
    root,run,ids,manager,make=prepared
    phases=[];roll=make(observe=lambda s,on,version:phases.append((s['role'],s['name'],on,version)) or {'ready':True})
    before={k:v for k,v in ids.items() if k.startswith(('worker-','engine-','gateway-'))}
    def migration():
        assert phases==[('api','a',False,8),('api','b',False,8)]
        roll._mark_commit({'retention_ledger_mismatches':0});return {'retention_ledger_mismatches':0}
    roll.migrate=migration;report=roll.apply()
    assert report['phase']=='complete' and report['watchdog_restored']
    assert [p[2:] for p in phases]==[(False,8),(False,8),(True,10),(True,10),(True,10)]
    assert {k:ids[k] for k in before}==before
    assert not any(key.startswith(('worker-','engine-','gateway-')) for key,_ in manager.calls)


def test_precommit_failure_restores_old_source_and_environment_not_just_flags(prepared):
    root,run,ids,manager,make=prepared
    roll=make();old={p:raw for p,raw in roll.original.items()}
    roll.migrate=lambda:(_ for _ in ()).throw(RuntimeError('precommit quota refused'))
    with pytest.raises(RuntimeError):roll.apply()
    assert roll.report['phase']=='rolled_back' and roll.report['watchdog_restored']
    assert (root/'backend/app.py').read_bytes()==b'VALUE=1\n'
    assert not (root/'backend/retention.py').exists()
    assert all(p.read_bytes()==raw for p,raw in old.items())


@pytest.mark.parametrize('ambiguous_commit',[False,True])
def test_postcommit_or_ambiguous_commit_keeps_new_registry_and_disables_only_flag(prepared,ambiguous_commit):
    root,run,ids,manager,make=prepared
    def observe(s,on,v):
        if on:raise RuntimeError('new API verification failed')
        return {'ready':True}
    roll=make(observe=observe)
    def migration():
        roll._mark_commit({'retention_ledger_mismatches':0})
        if ambiguous_commit:raise OSError('commit acknowledgement lost')
        return {'retention_ledger_mismatches':0}
    roll.migrate=migration
    with pytest.raises((RuntimeError,OSError)):roll.apply()
    assert roll.report['phase']=='forward_repair_required' and roll.report['watchdog_restored']
    assert (root/'backend/migrate.py').read_bytes()==b'VALUE=2\n'
    assert all(deploy.values(p.read_bytes())['RETENTION_ADMISSION_ENABLED']=='false' for p in roll.original)


def test_initial_watchdog_stop_failure_preserves_original_sources_and_restores_watchdog(prepared):
    root,run,ids,manager,make=prepared
    roll=make();manager.fail=('supervisor-primary','stop')
    with pytest.raises(RuntimeError):roll.apply()
    assert roll.report['phase']=='rolled_back' and roll.report['watchdog_restored']
    assert (root/'backend/app.py').read_bytes()==b'VALUE=1\n'


def test_foreign_source_on_failed_copy_requires_manual_repair_and_keeps_watchdog_off(prepared):
    root,run,ids,manager,make=prepared
    def writer(path,*args):
        deploy.atomic_source(path,*args)
        if path.name=='app.py':path.write_bytes(b'FOREIGN=True\n');raise OSError('injected foreign bytes')
    roll=make(writer=writer)
    with pytest.raises(OSError):roll.apply()
    assert roll.report['phase']=='manual_recovery_required' and ids['supervisor-primary'] is None
    assert (root/'backend/app.py').read_bytes()==b'FOREIGN=True\n'


def test_uncached_new_process_sees_new_backend_after_unchecked_hash_pyc_eviction(prepared):
    import py_compile
    root,run,ids,manager,make=prepared
    (root/'backend/__init__.py').write_bytes(b'')
    cached=py_compile.compile(str(root/'backend/config.py'),doraise=True,invalidation_mode=py_compile.PycInvalidationMode.UNCHECKED_HASH)
    roll=make();roll.migrate=lambda:(_ for _ in ()).throw(RuntimeError('rollback after compatibility'))
    with pytest.raises(RuntimeError):roll.apply()
    assert not Path(cached).exists()
    out=subprocess.check_output([sys.executable,'-I','-B','-c',
        'import sys;sys.path.insert(0,sys.argv[1]);from backend.config import VALUE;print(VALUE)',str(root)],text=True)
    assert out.strip()=='1'


def test_copy_holds_both_api_manage_locks_and_never_evicts_unrelated_cache(prepared):
    root,run,ids,manager,make=prepared
    cache=root/'backend/__pycache__';cache.mkdir()
    unrelated=cache/'scheduling.cpython-312.pyc';unrelated.write_bytes(b'unrelated')
    def writer(path,*args):
        for name in ('a','b'):
            with (root/f'.runtime/api-{name}.lock').open('a') as lock:
                with pytest.raises(BlockingIOError):fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        deploy.atomic_source(path,*args)
    roll=make(writer=writer);roll.migrate=lambda:(_ for _ in ()).throw(RuntimeError('rollback'))
    with pytest.raises(RuntimeError):roll.apply()
    assert unrelated.read_bytes()==b'unrelated'


def test_unsafe_backend_cache_refuses_before_any_source_or_service_changes(prepared):
    root,run,ids,manager,make=prepared
    (root/'backend/__pycache__').symlink_to(root/'.runtime',target_is_directory=True)
    with pytest.raises(ValueError,match='cache directory'):make()
    assert manager.calls==[] and (root/'backend/app.py').read_bytes()==b'VALUE=1\n'


def test_proofs_bind_fresh_private_offpod_archive_and_exact_historical_restore(tmp_path):
    archive=tmp_path/'snapshot.dump';atomic_bytes(archive,b'private fixture archive')
    snapshot=hashlib.sha256(archive.read_bytes()).hexdigest()
    records={
        'fresh_backup':{'observed_at':datetime.now(timezone.utc).isoformat(),'compatible':True,
            'archive_list_verified':True,'database_modified':False,'off_pod_copy_verified':True,
            'snapshot_sha256':snapshot,'off_pod_copy_sha256':snapshot,'off_pod_copy_mode':'0o600',
            'snapshot_path':str(archive),'snapshot_bytes':archive.stat().st_size},
        'historical_restore':{'passed':True,'restore_verified':True,'schema_version':8,
            'temporary_cluster_stopped':True,'temporary_cluster_removed':True,'snapshot_sha256':'b'*64,
            'migration_checksums':{str(n):sha for n,_,sha in MIGRATIONS if n<=8},
            **{k:0 for k in ['batch_count_state_usage_mismatches','accepted_attempt_count_usage_mismatches',
                'tenant_queue_pending_mismatches','unvalidated_constraints']}},
        'production':{'passed':True,'restore_verified_before_purge':True,'backup_sha256':'b'*64,
            'task_payloads_cleared':20,'attempt_counts_and_usage_unchanged':True,
            'batch_counters_and_identity_unchanged':True,'byte_identical_on_ports':[8080,8082,8083],
            'archive_publication_still_disabled':True,'output_sha256':'c'*64,'output_file_id':'old20','download_bytes':52200}}
    spec={}
    for name,value in records.items():
        path=tmp_path/(name+'.json');atomic_bytes(path,json.dumps(value).encode())
        spec[name]={'path':str(path),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}
    assert deploy.proofs(spec,actual_migrations())['fresh_snapshot_sha256']==snapshot
    archive.write_bytes(b'changed private archive')
    with pytest.raises(ValueError,match='archive differs'):deploy.proofs(spec,actual_migrations())


def test_private_prepared_environment_snapshot_cannot_be_rewritten(prepared):
    root,run,ids,manager,make=prepared
    roll=make();before=json.loads((run/'report.json').read_bytes())
    atomic_bytes(run/'api-0.env',b'FOREIGN=changed\n')
    with pytest.raises(RuntimeError,match='snapshot changed'):make(previous=before)


@pytest.fixture
def database():
    source=os.environ.get('TEST_DATABASE_URL')
    if not source:pytest.skip('Requires isolated real PostgreSQL')
    name='bf_storage_roll_'+secrets.token_hex(8)
    with psycopg.connect(source,autocommit=True) as conn:
        conn.execute(sql.SQL('CREATE DATABASE {} TEMPLATE template0').format(sql.Identifier(name)))
        if not conn.execute("SELECT 1 FROM pg_roles WHERE rolname='bf_runtime'").fetchone():conn.execute('CREATE ROLE bf_runtime')
    dsn=make_conninfo(source,dbname=name)
    try:
        migrate_database(dsn,'storage-roll-fixture-key',target_version=8)
        yield dsn
    finally:
        with psycopg.connect(source,autocommit=True) as conn:
            conn.execute(sql.SQL('DROP DATABASE {} WITH(FORCE)').format(sql.Identifier(name)))


def actual_migrations():return [(n,statement.encode()) for n,statement,_ in MIGRATIONS]


def test_real_migrations_policy_grants_and_ledger_publish_in_one_transaction(database):
    seen=[]
    with psycopg.connect(database,autocommit=True) as conn:
        result=deploy.migrate_atomic(conn,actual_migrations(),Path('ops/ha/runtime_grants.sql').read_bytes(),lambda evidence:seen.append(evidence))
        deploy.validate_prefix(conn,actual_migrations(),10)
        assert conn.execute("SELECT max_payload_bytes,max_retained_tasks FROM tenant_retention_limits WHERE tenant_id='default'").fetchone()==(335544320,50000)
        assert conn.execute("SELECT has_table_privilege('bf_runtime','tenant_retention_usage','UPDATE'),has_table_privilege('bf_runtime','tenant_retention_limits','UPDATE')").fetchone()==(True,False)
        assert not conn.execute("SELECT has_function_privilege('bf_runtime','batchforge_auxiliary_adjust(text,text,bigint,bigint)','EXECUTE')").fetchone()[0]
    assert len(seen)==1 and result['default_payload_charge_bytes']==0


def test_real_failure_after_ddl_before_commit_rolls_both_schemas_and_grants_back(database):
    def fail(_):raise RuntimeError('precommit invariant injection')
    with psycopg.connect(database,autocommit=True) as conn:
        with pytest.raises(RuntimeError):deploy.migrate_atomic(conn,actual_migrations(),Path('ops/ha/runtime_grants.sql').read_bytes(),fail)
        deploy.validate_prefix(conn,actual_migrations(),8)
        assert conn.execute("SELECT to_regclass('public.tenant_retention_usage'),to_regclass('public.tenant_auxiliary_usage')").fetchone()==(None,None)


def test_real_overlimit_bootstrap_refuses_policy_commit_without_losing_existing_rows(database):
    with psycopg.connect(database,autocommit=True) as conn:
        # Old inline metadata can exceed the new production policy. Bootstrap
        # remains exact but this operator must refuse rather than widen limits.
        conn.execute("INSERT INTO files(id,tenant_id,filename,kind) SELECT 'input-'||n,'default','old','input' FROM generate_series(1,2001)n")
        with pytest.raises(RuntimeError,match='below existing usage'):
            deploy.migrate_atomic(conn,actual_migrations(),Path('ops/ha/runtime_grants.sql').read_bytes(),lambda _:None)
        deploy.validate_prefix(conn,actual_migrations(),8)
        assert conn.execute('SELECT count(*) FROM files').fetchone()[0]==2001


def test_real_complete_gate_is_nowait_before_immutable9_and_leaves_schema8_on_refusal(database):
    with psycopg.connect(database) as holder,psycopg.connect(database,autocommit=True) as migration:
        holder.execute("SELECT id FROM tenants WHERE id='default' FOR NO KEY UPDATE")
        with pytest.raises(psycopg.errors.LockNotAvailable):
            deploy.migrate_atomic(migration,actual_migrations(),Path('ops/ha/runtime_grants.sql').read_bytes(),lambda _:None)
        deploy.validate_prefix(migration,actual_migrations(),8)
        assert migration.execute("SELECT to_regclass('public.tenant_retention_usage')").fetchone()[0] is None
        holder.rollback()
        # A refusal releases all migration gates; the exact same drained schema
        # then succeeds without widening policy or retrying a partial DDL step.
        deploy.migrate_atomic(migration,actual_migrations(),Path('ops/ha/runtime_grants.sql').read_bytes(),lambda _:None)
