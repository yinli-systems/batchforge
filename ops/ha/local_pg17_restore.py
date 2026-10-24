"""Verify one hash-pinned backup in a new unprivileged Unix-only PG17 cluster.

Never accepts a destination DSN or reads production credentials. The server and
matching clients must already be extracted from verified official packages.
Root only stages private files and drops the isolated children to nobody; no
system package, role, service, port listener, or source database is changed.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import pwd
import shutil
import subprocess
import sys
import tempfile
import time


def now():
    return datetime.now(timezone.utc).isoformat()


def file_sha(path):
    with path.open('rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()


def verify_database(connection, expected_migrations):
    from psycopg import sql
    ledger = connection.execute('SELECT version,checksum FROM batchforge_schema_migrations ORDER BY version').fetchall()
    if ledger != [(v, digest) for v, _, digest in expected_migrations]:
        raise RuntimeError('schema_migration_ledger_differs')
    counts = connection.execute("""WITH actual AS (
      SELECT batch_id,count(*) AS total,
       count(*) FILTER(WHERE status='pending') AS pending,
       count(*) FILTER(WHERE status='running') AS running,
       count(*) FILTER(WHERE status='completed') AS completed,
       count(*) FILTER(WHERE status='failed') AS failed,
       count(*) FILTER(WHERE status='cancelled') AS cancelled,
       coalesce(sum(prompt_tokens),0) AS prompt,coalesce(sum(completion_tokens),0) AS completion
      FROM tasks GROUP BY batch_id)
      SELECT count(*) FROM batches b LEFT JOIN actual a ON a.batch_id=b.id
      WHERE (b.total_count,b.pending_count,b.running_count,b.completed_count,b.failed_count,b.cancelled_count,b.prompt_tokens,b.completion_tokens)
       IS DISTINCT FROM (coalesce(a.total,0),coalesce(a.pending,0),coalesce(a.running,0),coalesce(a.completed,0),coalesce(a.failed,0),coalesce(a.cancelled,0),coalesce(a.prompt,0),coalesce(a.completion,0))""").fetchone()[0]
    accepted = connection.execute("""WITH a AS (
       SELECT task_id,count(*) AS n,sum(prompt_tokens) AS p,sum(completion_tokens) AS c
       FROM attempts WHERE status='completed' GROUP BY task_id)
       SELECT count(*) FROM tasks t LEFT JOIN a ON a.task_id=t.id
       WHERE (t.status='completed' AND (coalesce(a.n,0)<>1 OR t.prompt_tokens<>a.p OR t.completion_tokens<>a.c))
          OR (t.status<>'completed' AND coalesce(a.n,0)<>0)""").fetchone()[0]
    queues = connection.execute("""SELECT count(*) FROM tenant_queues q WHERE q.pending_count <>
      (SELECT count(*) FROM tasks t WHERE t.tenant_id=q.tenant_id AND t.model=q.model AND t.status='pending')""").fetchone()[0]
    invalid = connection.execute("SELECT count(*) FROM pg_constraint WHERE connamespace='public'::regnamespace AND NOT convalidated").fetchone()[0]
    if counts or accepted or queues or invalid:
        raise RuntimeError('restored_invariant_mismatch')
    functions = connection.execute("""SELECT p.proname,p.prosecdef,p.proconfig
      FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
      WHERE n.nspname='public' AND p.proname LIKE 'batchforge_%' ORDER BY p.proname""").fetchall()
    required = {'batchforge_claim','batchforge_complete','batchforge_heartbeat','batchforge_fail','batchforge_recover'}
    if not required.issubset({r[0] for r in functions}) or any(r[1] or not any(s.startswith('search_path=') for s in (r[2] or [])) for r in functions):
        raise RuntimeError('restored_function_security_differs')
    tables = connection.execute("SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename").fetchall()
    digests = {}
    for number, (table,) in enumerate(tables):
        digest, count = hashlib.sha256(), 0
        with connection.cursor(name='backup_digest_' + str(number)) as cursor:
            cursor.execute(sql.SQL('SELECT row_to_json(t)::text FROM public.{} t ORDER BY row_to_json(t)::text').format(sql.Identifier(table)))
            for (row,) in cursor:
                encoded = row.encode('utf-8')
                digest.update(len(encoded).to_bytes(8, 'big'));digest.update(encoded);count += 1
        digests[table] = {'rows': count, 'sha256': digest.hexdigest()}
    totals = connection.execute('SELECT coalesce(sum(prompt_tokens),0),coalesce(sum(completion_tokens),0) FROM tasks').fetchone()
    return {'schema_version':ledger[-1][0], 'migration_checksums':dict(ledger),
        'batch_count_state_usage_mismatches':counts,'accepted_attempt_count_usage_mismatches':accepted,
        'tenant_queue_pending_mismatches':queues,'unvalidated_constraints':invalid,
        'security_invoker_functions':[r[0] for r in functions], 'tables':digests,
        'prompt_tokens':int(totals[0]),'completion_tokens':int(totals[1])}


def restore(args):
    import psycopg
    from backend.migrate import MIGRATIONS
    if os.geteuid() != 0 or sys.platform != 'linux':
        raise RuntimeError('linux_root_staging_with_unprivileged_children_required')
    if args.report.exists():
        raise RuntimeError('report_already_exists')
    manifest = json.loads(args.backup_report.read_text())
    archive = Path(manifest['snapshot_path']).resolve(strict=True)
    if (not manifest['archive_list_verified'] or manifest['server_major'] != 17
            or archive.stat().st_size != manifest['snapshot_bytes'] or file_sha(archive) != manifest['snapshot_sha256']):
        raise RuntimeError('backup_manifest_or_checksum_differs')
    for root in (args.server_root, args.client_root):
        if root.is_symlink() or not root.is_dir():raise RuntimeError('toolchain_root_invalid')
    available = shutil.disk_usage('/tmp').free
    if available < archive.stat().st_size * 4 + 512*1024*1024:
        raise RuntimeError('temporary_disk_headroom_too_low')
    account = pwd.getpwnam('nobody')
    report = {'schema_version':1,'started_at':now(),'passed':False,'snapshot_path':str(archive),
        'snapshot_sha256':manifest['snapshot_sha256'],'snapshot_bytes':manifest['snapshot_bytes'],
        'source_sha256':file_sha(Path(__file__)),'destination':'new private unprivileged local PostgreSQL17 cluster',
        'live_database_modified':False,'roles_or_acl_restored':False,
        'limitations':['--no-owner/--no-acl; cluster roles, service credentials and bf_runtime permissions are not restored.',
          'All-table digests describe the restored archive, not equality against a subsequently changing live database.',
          'Functional restore may overlap a real worker-fault drill; timings are not an isolated performance comparison.',
          'External blob objects and provider disaster recovery are outside this database snapshot proof.']}
    started = time.monotonic();stopped = False
    temporary = Path(tempfile.mkdtemp(prefix='bf-restore-pg17-',dir='/tmp'))
    logpath = temporary/'postgres.log'
    child_env = {'PATH':'/usr/bin:/bin','LANG':'C','LC_ALL':'C','HOME':str(temporary),
        'LD_LIBRARY_PATH':str(temporary/'toolchain/usr/lib/x86_64-linux-gnu')}
    def unprivileged():
        os.setgroups([]);os.setgid(account.pw_gid);os.setuid(account.pw_uid)
    def run(command, timeout=120):
        with (temporary/'operator.log').open('ab') as output:
            result=subprocess.run(command,cwd=temporary,env=child_env,preexec_fn=unprivileged,
                stdin=subprocess.DEVNULL,stdout=output,stderr=subprocess.STDOUT,timeout=timeout)
        if result.returncode:raise RuntimeError('isolated_subprocess_failed')
    binary = temporary/'toolchain/usr/lib/postgresql/17/bin'
    data,sockets = temporary/'data',temporary/'socket'
    try:
        shutil.copytree(args.server_root,temporary/'toolchain',symlinks=True)
        shutil.copytree(args.client_root,temporary/'toolchain',dirs_exist_ok=True,symlinks=True)
        shutil.copyfile(archive,temporary/'snapshot.dump');os.chmod(temporary/'snapshot.dump',0o600)
        if file_sha(temporary/'snapshot.dump') != manifest['snapshot_sha256']:
            raise RuntimeError('private_archive_copy_checksum_differs')
        sockets.mkdir(mode=0o700)
        for path in [temporary,*temporary.rglob('*')]:
            if not path.is_symlink():os.chown(path,account.pw_uid,account.pw_gid)
        run([str(binary/'initdb'),'-D',str(data),'-L',str(temporary/'toolchain/usr/share/postgresql/17'),
             '-U','batchforge','--auth-local=trust','--auth-host=reject','--no-locale','--encoding=UTF8'])
        with (data/'postgresql.conf').open('a') as out:
            out.write("\nlisten_addresses=''\nunix_socket_directories='"+str(sockets)+"'\nunix_socket_permissions=0700\nshared_buffers=32MB\nwork_mem=8MB\nmax_connections=8\njit=off\nfsync=on\n")
        run([str(binary/'pg_ctl'),'-D',str(data),'-l',str(logpath),'-w','start'])
        with psycopg.connect(host=str(sockets),dbname='postgres',user='batchforge') as conn:
            version,address,listeners=conn.execute("SELECT current_setting('server_version'),inet_server_addr(),current_setting('listen_addresses')").fetchone()
            if not version.startswith('17.') or address is not None or listeners != '':raise RuntimeError('local_pg17_unix_only_gate_failed')
            report.update(server_version=version,server_os_uid=account.pw_uid,unix_socket_only=True)
        restored = time.monotonic()
        run([str(binary/'pg_restore'),'--no-owner','--no-acl','--no-password','--exit-on-error',
            '--host',str(sockets),'--username','batchforge','--dbname','postgres',str(temporary/'snapshot.dump')],timeout=600)
        report['pure_restore_seconds']=time.monotonic()-restored
        with psycopg.connect(host=str(sockets),dbname='postgres',user='batchforge') as conn:
            report.update(verify_database(conn,MIGRATIONS))
        report['passed']=True
        report['restore_verified']=True
    except Exception as error:
        report['error_type']=type(error).__name__
        report['error']=str(error) if type(error) is RuntimeError else 'isolated_restore_failed'
    finally:
        if (data/'postmaster.pid').exists():
            try:
                run([str(binary/'pg_ctl'),'-D',str(data),'-m','fast','-w','stop'])
                stopped=True
            except Exception:
                report['cleanup_error']='isolated_postgres_stop_failed'
        else:stopped=True
        report['temporary_cluster_stopped']=stopped
        if stopped:
            if not report['passed']:
                failure_dir=args.report.with_suffix('.failure-logs')
                failure_dir.mkdir(mode=0o700)
                for name in ('postgres.log','operator.log'):
                    if (temporary/name).exists():
                        shutil.copyfile(temporary/name,failure_dir/name);os.chmod(failure_dir/name,0o600)
                report['private_failure_logs']=str(failure_dir)
            shutil.rmtree(temporary)
            report['temporary_cluster_removed']=not temporary.exists()
        else:
            report['temporary_cluster_path']=str(temporary)
            report['passed']=False
        report.update(finished_at=now(),elapsed_seconds=time.monotonic()-started)
        args.report.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
        descriptor=os.open(args.report,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
        with os.fdopen(descriptor,'w') as out:json.dump(report,out,indent=2);out.write('\n')
    return report


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--backup-report',type=Path,required=True)
    parser.add_argument('--server-root',type=Path,required=True)
    parser.add_argument('--client-root',type=Path,required=True)
    parser.add_argument('--report',type=Path,required=True)
    args=parser.parse_args()
    try:
        report=restore(args)
        print(json.dumps({key:report.get(key) for key in ('passed','error','error_type','schema_version','temporary_cluster_stopped','temporary_cluster_removed','pure_restore_seconds','elapsed_seconds')}))
        return 0 if report['passed'] else 1
    except Exception as error:
        print(json.dumps({'passed':False,'error_type':type(error).__name__}))
        return 1


if __name__=='__main__':
    raise SystemExit(main())
