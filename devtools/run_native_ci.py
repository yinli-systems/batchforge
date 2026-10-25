"""CPU-only checks on an ephemeral, non-root Linux runner and Unix-only PG17.

BF_CI_PG_ROOT is an operator-staged public PostgreSQL toolchain, not a database
URL. No production environment, GPU inference or TCP database listener is used.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

from run_checks import CHECK_PATHS


def main():
    if sys.platform != 'linux' or os.geteuid() == 0:
        raise RuntimeError('Ephemeral non-root Linux runner required')
    source = Path(__file__).resolve().parent.parent
    toolchain = Path(os.environ['BF_CI_PG_ROOT']).resolve(strict=True)
    binary = toolchain / 'usr/lib/postgresql/17/bin'
    env = {'PATH': os.environ['PATH'], 'HOME': os.environ['HOME'],
           'LANG': 'C', 'LC_ALL': 'C', 'CUDA_VISIBLE_DEVICES': '',
           'AWS_EC2_METADATA_DISABLED': 'true',
           'LD_LIBRARY_PATH': str(toolchain / 'usr/lib/x86_64-linux-gnu')}
    version = subprocess.check_output([str(binary / 'postgres'), '--version'], env=env, text=True).strip()
    if not version.startswith('postgres (PostgreSQL) 17.'):
        raise RuntimeError('Real PostgreSQL 17 required')
    node = shutil.which('node')
    if not node or int(subprocess.check_output([node, '--version'], text=True).strip().lstrip('v').split('.')[0]) < 22:
        raise RuntimeError('Node22 or newer required for console tests')
    print(json.dumps({'postgres_version': version, 'node_version': subprocess.check_output([node, '--version'], text=True).strip(),
        'uid': os.geteuid(), 'production_credentials_inherited': False, 'gpu_inference': False}), flush=True)
    with tempfile.TemporaryDirectory(prefix='bf-native-ci-') as temporary:
        root = Path(temporary)
        data, socket = root / 'data', root / 'socket'
        socket.mkdir(mode=0o700)
        subprocess.run([str(binary / 'initdb'), '-D', str(data), '-L', str(toolchain / 'usr/share/postgresql/17'),
            '-U', 'batchforge', '--auth-local=trust', '--auth-host=reject', '--no-locale', '--encoding=UTF8'],
            env=env, check=True, stdout=subprocess.DEVNULL)
        # No TCP sockets; the private socket directory is mode700 and owned by
        # this ephemeral CI account. At most this test cluster is stopped.
        options = f"-h '' -k {socket} -c shared_buffers=32MB -c work_mem=4MB -c max_connections=96"
        control = str(binary / 'pg_ctl')
        subprocess.run([control, '-D', str(data), '-l', str(root / 'postgres.log'), '-o', options, '-w', 'start'],
            env=env, check=True, stdout=subprocess.DEVNULL)
        try:
            uri = 'postgresql://batchforge@/postgres?host=' + str(socket)
            tests_env = dict(env, TEST_DATABASE_URL=uri, DATABASE_URL=uri, PYTHONDONTWRITEBYTECODE='1')
            subprocess.run([sys.executable, '-m', 'pytest', *CHECK_PATHS, '-q'], cwd=source, env=tests_env, check=True, timeout=600)
            subprocess.run([node, '--test', *map(str, sorted((source / 'frontend/tests').glob('*.test.js')))],
                           cwd=source, env=env, check=True, timeout=120)
            subprocess.run([sys.executable, '-m', 'compileall', '-q', 'backend', 'worker', 'ops', 'scripts', 'devtools'],
                           cwd=source, env=env, check=True, timeout=120)
        finally:
            subprocess.run([control, '-D', str(data), '-m', 'fast', '-w', 'stop'], env=env, check=True, stdout=subprocess.DEVNULL)


if __name__ == '__main__':
    main()
