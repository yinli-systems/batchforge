"""Stage committed source or validate it on an isolated Unix-only PostgreSQL17.

Neither command reads an application environment file or accepts a database
DSN. `run` is prepare-only unless --execute is explicitly supplied. PostgreSQL
children run as nobody. Test processes use the existing Python environment,
an isolated working directory and an explicit environment without credentials.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import pwd
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import time

MANIFEST = '.validation-manifest.json'
DIRECTORIES = {'backend', 'worker', 'tests', 'ops', 'scripts', 'devtools'}
EXTRA = {'pyproject.toml', 'backend/CONTRACT.md', 'ops/ha/inventory.example.json',
         'ops/pod2/mig_platform/batchforge_mig_platform-0.1.dist-info/METADATA',
         'ops/pod2/mig_platform/batchforge_mig_platform-0.1.dist-info/entry_points.txt'}


def digest(raw): return hashlib.sha256(raw).hexdigest()
def now(): return datetime.now(timezone.utc).isoformat()


def allowed(name):
    path = PurePosixPath(name)
    if path.is_absolute() or '..' in path.parts or any(p.startswith('.') for p in path.parts): return False
    if name in EXTRA: return True
    return (path.parts[0] in DIRECTORIES and path.suffix in {'.py', '.sql'}
            and not name.startswith('ops/reports/') and '__pycache__' not in path.parts)


def private_json(path, value):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, 'w') as stream:
        json.dump(value, stream, indent=2, sort_keys=True); stream.write('\n'); stream.flush(); os.fsync(stream.fileno())


def stage(repository, commit, destination):
    if not re.fullmatch(r'[0-9a-f]{40}', commit): raise ValueError('Exact40-character committed revision required')
    if destination.exists() or destination.is_symlink(): raise ValueError('Snapshot destination must be new')
    tree = subprocess.check_output(['git', 'ls-tree', '-rz', '--full-tree', commit], cwd=repository)
    inventory = []
    for entry in tree.split(b'\0'):
        if not entry: continue
        header, name = entry.split(b'\t', 1); mode, kind, _ = header.decode().split(); name = name.decode()
        if not allowed(name): continue
        if kind != 'blob' or mode not in {'100644', '100755'}: raise ValueError('Source snapshot cannot contain symlinks or submodules')
        inventory.append(name)
    if not {'pyproject.toml', 'backend/schema.sql', 'devtools/run_checks.py', 'worker/runtime.py'}.issubset(inventory):
        raise ValueError('Committed source snapshot is incomplete')
    destination.mkdir(mode=0o700, parents=False)
    files = {}
    for name in sorted(inventory):
        content = subprocess.check_output(['git', 'show', commit + ':' + name], cwd=repository)
        path = destination / name; path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        with path.open('xb') as stream: stream.write(content)
        path.chmod(0o600); files[name] = digest(content)
    value = {'commit': commit, 'files': files,
             'source_tree_sha256': digest(json.dumps(files, sort_keys=True, separators=(',', ':')).encode()),
             'contains_application_configuration': False, 'staged_at': now()}
    private_json(destination / MANIFEST, value)
    return value


def verify_snapshot(source):
    if source.is_symlink() or not source.is_dir() or source.resolve() != source:
        raise ValueError('Source snapshot must be an absolute nonsymlink directory')
    info = source.stat()
    if info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) & 0o077:
        raise ValueError('Source snapshot must be owner-only')
    manifest_path = source / MANIFEST
    if manifest_path.is_symlink(): raise ValueError('Manifest cannot be a symlink')
    manifest = json.loads(manifest_path.read_text())
    if (not re.fullmatch(r'[0-9a-f]{40}', manifest.get('commit', ''))
            or manifest.get('contains_application_configuration') is not False
            or not isinstance(manifest.get('files'), dict)):
        raise ValueError('Invalid source manifest')
    found = {}
    for path in source.rglob('*'):
        if path.is_symlink(): raise ValueError('Snapshot symlinks are forbidden')
        if path.is_dir(): continue
        if not path.is_file(): raise ValueError('Snapshot contains a nonregular file')
        name = path.relative_to(source).as_posix()
        if name == MANIFEST: continue
        if not allowed(name): raise ValueError('Snapshot contains configuration, reports or an unapproved artifact')
        found[name] = digest(path.read_bytes())
    if found != manifest['files'] or digest(json.dumps(found, sort_keys=True, separators=(',', ':')).encode()) != manifest['source_tree_sha256']:
        raise ValueError('Snapshot file inventory or source SHA differs')
    return manifest


def scoped_processes(roots):
    """Observe only PID/start/cwd; never read other processes' args or secrets."""
    result = []
    for entry in Path('/proc').iterdir():
        if not entry.name.isdecimal() or int(entry.name) == os.getpid(): continue
        try:
            cwd = (entry / 'cwd').resolve(strict=True)
            if not any(cwd == root or cwd.is_relative_to(root) for root in roots): continue
            raw = (entry / 'stat').read_text(); fields = raw[raw.rfind(')') + 2:].split()
            if fields[0] != 'Z': result.append({'pid': int(entry.name), 'start_ticks': fields[19], 'cwd': str(cwd)})
        except (FileNotFoundError, ProcessLookupError, PermissionError): continue
    return result


PLUGIN = '''import json,os
def pytest_sessionfinish(session,exitstatus):
    terminal=session.config.pluginmanager.get_plugin('terminalreporter')
    data={k:len(v) for k,v in terminal.stats.items()}
    data.update(exit_status=int(exitstatus),collected=session.testscollected,
        skips=[{'nodeid':r.nodeid,'reason':str(r.longrepr)} for r in terminal.stats.get('skipped',[])])
    with open(os.environ['BF_VALIDATION_COUNTS'],'w') as output:json.dump(data,output)
'''


def run_validation(args):
    source = Path(os.path.abspath(args.source))
    if source.resolve() != source: raise ValueError('Source path cannot contain symlinks')
    manifest = verify_snapshot(source)
    if args.report.exists(): raise ValueError('Existing validation report refuses duplicate execution')
    report = {'phase': 'prepared', 'passed': False, 'prepared_at': now(), 'commit': manifest['commit'],
        'source_tree_sha256': manifest['source_tree_sha256'], 'source_file_count': len(manifest['files']),
        'source_manifest_sha256': digest((source / MANIFEST).read_bytes()),
        'runner_sha256': digest(Path(__file__).read_bytes()), 'production_credentials_inherited': False,
        'production_database_accessed': False, 'source_database_url_supplied': False,
        'python': str(args.python), 'node_console_executed': False,
        'limitations': ['Committed snapshot manifest is supplied by the operator and verified file-by-file; no production configuration is copied.',
            'CPU correctness validation is separate from GPU performance measurements.',
            'No GPU inference, cloud CI runner, dependency installation, system service or paid resource is started.']}
    if not args.execute:
        args.report.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        private_json(args.report, report); return report
    if sys.platform != 'linux' or os.geteuid() != 0: raise RuntimeError('Linux root staging is required')
    for root in (args.server_root, args.client_root):
        if root.is_symlink() or not root.is_dir(): raise ValueError('Verified extracted toolchain roots required')
    if shutil.disk_usage('/tmp').free < 1024**3: raise RuntimeError('Less than1GiB temporary disk headroom')
    account = pwd.getpwnam('nobody')
    temporary = Path(tempfile.mkdtemp(prefix='bf-pg17-validation-', dir='/tmp'))
    binary = temporary / 'toolchain/usr/lib/postgresql/17/bin'
    data, socket = temporary / 'data', temporary / 'socket'
    started = time.monotonic(); stopped = False
    child_env = {'PATH': '/usr/bin:/bin', 'LANG': 'C', 'LC_ALL': 'C', 'HOME': str(temporary),
        'LD_LIBRARY_PATH': str(temporary / 'toolchain/usr/lib/x86_64-linux-gnu')}
    def drop():
        os.setgroups([]); os.setgid(account.pw_gid); os.setuid(account.pw_uid)
    def command(argv, timeout=180):
        with (temporary / 'postgres-operator.log').open('ab') as output:
            result = subprocess.run(argv, cwd=temporary, env=child_env, preexec_fn=drop,
                stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.STDOUT, timeout=timeout)
        if result.returncode: raise RuntimeError('Private PostgreSQL command failed')
    try:
        shutil.copytree(args.server_root, temporary / 'toolchain', symlinks=True)
        shutil.copytree(args.client_root, temporary / 'toolchain', dirs_exist_ok=True, symlinks=True)
        socket.mkdir(mode=0o700)
        for path in [temporary, *temporary.rglob('*')]:
            if not path.is_symlink(): os.chown(path, account.pw_uid, account.pw_gid)
        command([str(binary / 'initdb'), '-D', str(data), '-L', str(temporary / 'toolchain/usr/share/postgresql/17'),
            '-U', 'batchforge', '--auth-local=trust', '--auth-host=reject', '--no-locale', '--encoding=UTF8'])
        with (data / 'postgresql.conf').open('a') as stream:
            stream.write("\nlisten_addresses=''\nunix_socket_directories='" + str(socket) + "'\nunix_socket_permissions=0700\n"
                'shared_buffers=32MB\nwork_mem=4MB\nmax_connections=96\njit=off\nfsync=on\n')
        command([str(binary / 'pg_ctl'), '-D', str(data), '-l', str(temporary / 'postgres.log'), '-w', 'start'])
        import psycopg
        with psycopg.connect(host=str(socket), user='batchforge', dbname='postgres') as connection:
            version, address, listeners = connection.execute("SELECT current_setting('server_version'),inet_server_addr(),current_setting('listen_addresses')").fetchone()
            if not version.startswith('17.11') or address is not None or listeners != '':
                raise RuntimeError('Private PostgreSQL17.11 Unix-only gate failed')
        report.update(phase='running', started_at=now(), server_version=version,
            unix_socket_only=True, server_os_uid=account.pw_uid,
            postgres_binary_sha256=digest((binary / 'postgres').read_bytes()))
        plugin = temporary / 'bf_validation_plugin.py'; plugin.write_text(PLUGIN); plugin.chmod(0o600)
        counts = temporary / 'pytest-counts.json'
        tests_temporary = temporary / 'tests'; tests_temporary.mkdir(mode=0o700)
        uri = 'postgresql://batchforge@/postgres?host=' + str(socket)
        env = {'PATH': str(args.python.parent) + ':/usr/bin:/bin', 'LANG': 'C', 'LC_ALL': 'C',
            'HOME': str(temporary), 'PYTHONPATH': str(source) + ':' + str(temporary),
            'PYTEST_DISABLE_PLUGIN_AUTOLOAD': '1', 'PYTEST_ADDOPTS': '-p pytest_asyncio.plugin -p bf_validation_plugin',
            'BF_VALIDATION_COUNTS': str(counts), 'TEST_DATABASE_URL': uri, 'DATABASE_URL': uri,
            'PYTEST_DEBUG_TEMPROOT': str(tests_temporary), 'TMPDIR': str(tests_temporary),
            'AWS_EC2_METADATA_DISABLED': 'true', 'CUDA_VISIBLE_DEVICES': '', 'NO_PROXY': '127.0.0.1,localhost'}
        # Validate import resolution before any test may create a child API.
        preflight = "from pathlib import Path;import backend,worker,ops.supervise,devtools.run_checks;root=Path.cwd();assert all(Path(m.__file__).resolve().is_relative_to(root) for m in [backend,worker,ops.supervise,devtools.run_checks])"
        with (temporary / 'pytest.log').open('wb') as output:
            subprocess.run([str(args.python), '-c', preflight], cwd=source, env=env, stdout=output, stderr=subprocess.STDOUT, check=True, timeout=30)
            compile_code = "from pathlib import Path;files=list(Path('.').rglob('*.py'));[compile(p.read_bytes(),str(p),'exec') for p in files];print('compiled',len(files))"
            subprocess.run([str(args.python), '-c', compile_code], cwd=source, env=env, stdout=output, stderr=subprocess.STDOUT, check=True, timeout=30)
            result = subprocess.run([str(args.python), 'devtools/run_checks.py'], cwd=source, env=env,
                stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.STDOUT, timeout=900)
        report['pytest'] = json.loads(counts.read_text()) if counts.exists() else {'missing_summary': True}
        report['test_log_sha256'] = digest((temporary / 'pytest.log').read_bytes())
        report['compiled_python_files'] = sum(name.endswith('.py') for name in manifest['files'])
        report['import_origins_verified_inside_snapshot'] = True
        report['source_files_unchanged_after_tests'] = all(digest((source / name).read_bytes()) == value for name, value in manifest['files'].items())
        report['passed'] = (result.returncode == 0 and report['pytest'].get('exit_status') == 0
            and report['pytest'].get('passed', 0) > 0 and report['source_files_unchanged_after_tests'])
    except Exception as error:
        report.update(error_type=type(error).__name__, passed=False)
    finally:
        if (data / 'postmaster.pid').exists():
            try: command([str(binary / 'pg_ctl'), '-D', str(data), '-m', 'fast', '-w', 'stop']); stopped = True
            except Exception: report['cleanup_error'] = 'Private PostgreSQL stop failed'
        else: stopped = True
        report['temporary_cluster_stopped'] = stopped
        report['remaining_scoped_test_processes'] = scoped_processes([source, temporary])
        if report['remaining_scoped_test_processes']: report['passed'] = False
        args.report.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        logs = args.report.with_suffix('.private-logs'); logs.mkdir(mode=0o700)
        for name in ('postgres.log', 'postgres-operator.log', 'pytest.log'):
            if (temporary / name).exists():
                shutil.copyfile(temporary / name, logs / name); (logs / name).chmod(0o600)
        report['private_logs_directory'] = str(logs)
        if stopped and not report['remaining_scoped_test_processes']:
            shutil.rmtree(temporary); report['temporary_cluster_removed'] = not temporary.exists()
        else:
            report.update(temporary_cluster_removed=False, temporary_cluster_path=str(temporary), passed=False)
        report.update(phase='complete' if report['passed'] else 'failed', finished_at=now(), elapsed_seconds=time.monotonic() - started)
        private_json(args.report, report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__); sub = parser.add_subparsers(dest='action', required=True)
    prepare = sub.add_parser('stage'); prepare.add_argument('--repository', type=Path, required=True)
    prepare.add_argument('--commit', required=True); prepare.add_argument('--destination', type=Path, required=True)
    run = sub.add_parser('run'); run.add_argument('--source', type=Path, required=True)
    run.add_argument('--server-root', type=Path, required=True); run.add_argument('--client-root', type=Path, required=True)
    run.add_argument('--python', type=Path, default=Path('/root/batchforge-runtime/venv/bin/python'))
    run.add_argument('--report', type=Path, required=True); run.add_argument('--execute', action='store_true')
    args = parser.parse_args(); os.umask(0o077)
    result = stage(args.repository, args.commit, args.destination) if args.action == 'stage' else run_validation(args)
    print(json.dumps({key: result[key] for key in ['commit', 'phase', 'passed', 'source_tree_sha256', 'pytest',
        'temporary_cluster_stopped', 'temporary_cluster_removed'] if key in result}), flush=True)
    return 0 if args.action == 'stage' or not args.execute or result.get('passed') else 1


if __name__ == '__main__':
    try: raise SystemExit(main())
    except Exception as error:
        print(json.dumps({'passed': False, 'error_type': type(error).__name__}), file=sys.stderr, flush=True)
        raise SystemExit(1) from None
