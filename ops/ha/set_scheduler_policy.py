"""Roll only API scheduler policy, preserving inference and storage settings.

Dry-run by default. Execute only in an operator-controlled drained window.
Uses managed PID identities; never enumerates or signals unrelated processes.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import time

from ops import manage
from ops.ha.retune_full_gpu import file_env
from ops.supervise import Manager, ROOT, load_manifest, private_read, private_write


def timestamp():
    return datetime.now(timezone.utc).isoformat()


def policy_bytes(raw: bytes, policy: str) -> bytes:
    if policy not in {'fair', 'cost_aware'}:
        raise ValueError('Unsupported scheduling policy')
    lines, count = [], 0
    for line in raw.decode().splitlines(keepends=True):
        assignment = line.strip().removeprefix('export ').strip()
        key, separator, _ = assignment.partition('=')
        if separator and key.strip() == 'SCHEDULER_POLICY':
            count += 1
            if count > 1:
                raise ValueError('Duplicate SCHEDULER_POLICY assignments')
            lines.append('SCHEDULER_POLICY=' + policy + '\n')
        else:
            lines.append(line)
    if not count:
        if lines and not lines[-1].endswith(('\n', '\r')):
            lines[-1] += '\n'
        lines.append('SCHEDULER_POLICY=' + policy + '\n')
    return ''.join(lines).encode()


def atomic_bytes(path: Path, content: bytes):
    temporary = path.with_name(path.name + '.' + secrets.token_hex(8) + '.tmp')
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(descriptor, 'wb') as stream:
            stream.write(content);stream.flush();os.fsync(stream.fileno())
        temporary.replace(path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:os.fsync(directory)
        finally:os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


def owned_identity(root, service):
    record = manage.owned_record(root / '.runtime' / f"{service['role']}-{service['name']}.json")
    if not record:
        raise RuntimeError('owned_service_not_running')
    return {key: record[key] for key in ('pid', 'start_ticks', 'cwd')}


class PolicyManager(Manager):
    def invoke(self, service, action):
        if action != 'stop':
            return super().invoke(service, action)
        command = [sys.executable, str(self.root/'ops/manage.py'), 'stop', service['role'],
                   '--name', service['name'], '--grace-seconds', '10']
        try:
            result = subprocess.run(command, cwd=self.root, stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=30, check=False)
            if result.returncode or len(result.stdout)>4096:
                return None,'manager_stop_failed'
            value = json.loads(result.stdout)
            if not isinstance(value,dict) or value.get('status') not in {'stopped','not_running'}:
                return None,'manager_stop_protocol_error'
            return value,None
        except (OSError,ValueError,subprocess.TimeoutExpired):
            return None,'manager_stop_unavailable'


class PolicyRoll:
    def __init__(self, *, root, services, watchdog, manifest_path, report_path, policy,
                 manager=None, identity=None, assert_drained=None, verify_policy=None):
        self.root,self.services,self.watchdog = root,services,watchdog
        self.manifest_path,self.report_path,self.policy = manifest_path,report_path,policy
        self.manager=manager or PolicyManager(root=root,timeout=30)
        self.identity=identity or (lambda service:owned_identity(root,service))
        self.assert_drained=assert_drained or self._drained
        self.verify_policy=verify_policy or self._effective_policy
        self.apis=sorted((s for s in services if s['role']=='api'),key=lambda s:s['name'])
        if [(s['name'],s.get('port')) for s in self.apis] != [('a',8082),('b',8083)]:
            raise ValueError('Requires existing API-a/b on8082/8083')
        self.protected=[s for s in services if s['role']!='api']
        self.original_manifest=private_read(manifest_path)
        self.original={Path(s['env_file']):private_read(Path(s['env_file'])) for s in self.apis}
        self.patched={path:policy_bytes(raw,policy) for path,raw in self.original.items()}
        self.protected_env={Path(s['env_file']):hashlib.sha256(private_read(Path(s['env_file']))).hexdigest() for s in self.protected}
        self.before={self.key(s):self.identity(s) for s in self.protected}
        self.report={'phase':'prepared','policy':policy,'prepared_at':timestamp(),
            'changed_environment_keys':['SCHEDULER_POLICY'],'events':[],
            'manifest_sha256':hashlib.sha256(self.original_manifest).hexdigest(),
            'non_api_identities_before':self.before}

    @staticmethod
    def key(service):return service['role']+'-'+service['name']

    def save(self):private_write(self.report_path,self.report)

    def invoke(self,service,action):
        value,error=self.manager.invoke(service,action)
        if error:raise RuntimeError(self.key(service)+'_'+action+'_failed')
        self.report['events'].append({'at':timestamp(),'service':self.key(service),'action':action,
            'status':value.get('status'),'pid':value.get('pid')})
        self.save();return value

    def _drained(self):
        import psycopg
        env=file_env(Path(self.apis[0]['env_file']))
        with psycopg.connect(env['DATABASE_URL'],connect_timeout=5,
                options='-c default_transaction_read_only=on -c statement_timeout=5000') as conn:
            active=conn.execute("SELECT count(*) FROM tasks WHERE status IN ('pending','running')").fetchone()[0]
        if active:raise RuntimeError('refused_active_tasks')

    def _effective_policy(self,service):
        identity=self.identity(service)
        data=Path(f"/proc/{identity['pid']}/environ").read_bytes().split(b'\0')
        # Read only this owned API's policy; never log the process environment.
        found=[part.partition(b'=')[2].decode() for part in data if part.startswith(b'SCHEDULER_POLICY=')]
        if found != [self.policy]:raise RuntimeError('effective_policy_mismatch')

    def guard(self):
        if private_read(self.manifest_path)!=self.original_manifest:
            raise RuntimeError('manifest_changed')
        if {self.key(s):self.identity(s) for s in self.protected}!=self.before:
            raise RuntimeError('non_api_identity_changed')
        if any(hashlib.sha256(private_read(path)).hexdigest()!=digest for path,digest in self.protected_env.items()):
            raise RuntimeError('non_api_environment_changed')

    def await_ready(self,service):
        until=time.monotonic()+45
        while time.monotonic()<until:
            if self.manager.readiness(service):return
            status,error=self.manager.invoke(service,'status')
            if error or not status.get('running'):raise RuntimeError('api_exited_before_ready')
            time.sleep(.5)
        raise RuntimeError('api_readiness_timeout')

    def apply(self):
        self.guard();self.assert_drained()
        self.report['api_identities_before']={self.key(s):self.identity(s) for s in self.apis}
        self.report['phase']='applying';self.save()
        touched=[];maintenance_attempted=False
        try:
            maintenance_attempted=True
            self.invoke(self.watchdog,'stop')
            self.guard();self.assert_drained()
            for api in self.apis:
                path=Path(api['env_file']);current=private_read(path)
                if current not in (self.original[path],self.patched[path]):
                    raise RuntimeError('api_environment_changed_concurrently')
                touched.append(api)
                self.invoke(api,'stop')
                atomic_bytes(path,self.patched[path])
                self.invoke(api,'start');self.await_ready(api);self.verify_policy(api)
                self.guard()
            if any(private_read(path)!=value for path,value in self.patched.items()):
                raise RuntimeError('api_environment_changed_concurrently')
            self.report.update(phase='complete',completed_at=timestamp(),
                non_api_identities_unchanged=True,all_other_environment_bytes_preserved=True,
                api_identities_after={self.key(s):self.identity(s) for s in self.apis})
        except BaseException as error:
            self.report.update(phase='rolling_back',failure_type=type(error).__name__);self.save()
            try:
                for api in reversed(touched):
                    path=Path(api['env_file']);current=private_read(path)
                    if current not in (self.original[path],self.patched[path]):
                        raise RuntimeError('rollback_refuses_foreign_environment_changes')
                    self.invoke(api,'stop');atomic_bytes(path,self.original[path])
                    self.invoke(api,'start');self.await_ready(api)
                self.guard()
                self.report['phase']='rolled_back'
            except BaseException as rollback_error:
                self.report.update(phase='manual_recovery_required',rollback_failure_type=type(rollback_error).__name__)
            raise
        finally:
            if maintenance_attempted:
                try:
                    self.invoke(self.watchdog,'start')
                    self.report['watchdog_restored']=True
                except BaseException as restore_error:
                    self.report.update(phase='manual_recovery_required',watchdog_restored=False,
                        watchdog_failure_type=type(restore_error).__name__)
                    self.save();raise
            self.save()
        if self.report['phase']!='complete':raise RuntimeError('policy_roll_not_complete')
        return self.report


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--policy',choices=['fair','cost_aware'],required=True)
    parser.add_argument('--report',type=Path,required=True)
    parser.add_argument('--manifest',type=Path,default=ROOT/'.runtime/services-primary.json')
    parser.add_argument('--execute',action='store_true')
    args=parser.parse_args();os.umask(0o077)
    report=args.report.resolve()
    if not report.is_relative_to(ROOT/'.runtime') or report.suffix!='.json':
        parser.error('Report must be a JSON file inside app/.runtime')
    report.parent.mkdir(parents=True,exist_ok=True,mode=0o700)
    lock_path=ROOT/'.runtime/scheduler-policy.lock'
    fd=os.open(lock_path,os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,'w') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        if report.exists():
            existing=json.loads(private_read(report))
            if existing.get('phase')!='prepared' or existing.get('policy')!=args.policy:
                raise RuntimeError('existing_report_refuses_duplicate_or_uncertain_roll')
        manifest,services,_=load_manifest(args.manifest)
        watchdog={'role':'supervisor','name':'primary','env_file':str(ROOT.parent/'supervisor.env'),
                  'python':str(ROOT.parent/'venv/bin/python')}
        roll=PolicyRoll(root=ROOT,services=services,watchdog=watchdog,manifest_path=manifest,
            report_path=report,policy=args.policy)
        if args.execute:roll.apply()
        else:roll.save()
    print(json.dumps({'report':str(report),'phase':roll.report['phase'],'policy':args.policy}),flush=True)


if __name__=='__main__':
    try:
        main()
    except Exception as error:
        print(json.dumps({'error_type':type(error).__name__}),file=sys.stderr,flush=True)
        raise SystemExit(1) from None
