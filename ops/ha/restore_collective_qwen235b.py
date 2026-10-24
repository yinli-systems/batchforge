"""Restore custom collective adaptive64 after an explicit NCCL comparison.

Default invocation only prepares immutable restoration evidence. A separate
--apply restores the original numerical profile and exact token budget. The
completed activation report is never modified; failure excludes the worker from
the guarded manifest and reports manual recovery instead of success.
"""
from __future__ import annotations

import argparse
import copy
import fcntl
import json
import os
from pathlib import Path
import re
import time

from backend.scheduling import validate_profile
from ops.ha import collective_qwen235b as collective
from ops.ha.adaptive_qwen235b import identity, private_operation_directory, sha
from ops.ha.finalize_api_policy import values
from ops.ha.set_scheduler_policy import timestamp
from ops.supervise import Manager, ROOT, load_manifest, private_read, private_write


def original_services(root, manifest_path, original):
    """Normalize the live manifest, then recover bound original env paths.

    load_manifest intentionally rejects nested snapshots. The completed activation
    binds the original document; only its two env paths may differ from the live
    candidate, which the constructor subsequently compares byte for byte.
    """
    _, current, _ = load_manifest(manifest_path, root=root)
    document = json.loads(original)
    entries = document.get('services', [])
    old = {collective.CollectiveRoll.key(s): s for s in entries}
    if len(old) != len(entries) or set(old) != {collective.CollectiveRoll.key(s) for s in current}:
        raise RuntimeError('Original and current service identity sets differ')
    return [{**s, 'env_file': old[collective.CollectiveRoll.key(s)]['env_file']} for s in current]


class RestoreCollective(collective.CollectiveRoll):
    def __init__(self, *, root, run, activation_run, activation_sha256, manifest_path, watchdog,
                 support, previous=None, manager=None, identity_reader=None, assert_drained=None,
                 engine_observer=None, worker_observer=None, path_observer=None,
                 clock=time.monotonic, sleep=time.sleep):
        if not re.fullmatch('[0-9a-f]{64}', activation_sha256): raise ValueError('Exact completed-report SHA256 required')
        self.root, self.run, self.restore_run = root, activation_run, run
        self.manifest_path, self.watchdog = manifest_path, watchdog
        self.activation_path = activation_run / 'report.json'
        raw = private_read(self.activation_path)
        if sha(raw) != activation_sha256: raise RuntimeError('Completed activation report SHA256 differs')
        activation = json.loads(raw)
        if activation.get('phase') != 'complete' or activation.get('watchdog_restored') is not True:
            raise RuntimeError('Only a successfully completed and supervised NCCL candidate can be restored')
        binding = activation['prepared_binding']
        if support != binding['support']: raise RuntimeError('Runtime/manager support changed after activation')
        self.support = support
        for name, checksum in binding['snapshot_sha256'].items():
            if name not in {'original-manifest.json', 'original-engine.env', 'original-worker.env', 'engine.env', 'worker.env'}:
                raise RuntimeError('Unexpected activation snapshot')
            if sha(private_read(activation_run / name)) != checksum: raise RuntimeError('Activation snapshot bytes changed')
        if set(binding['snapshot_sha256']) != {'original-manifest.json', 'original-engine.env', 'original-worker.env', 'engine.env', 'worker.env'}:
            raise RuntimeError('Activation snapshot set is incomplete')
        self.original = private_read(activation_run / 'original-manifest.json')
        if sha(self.original) != binding['manifest_sha256']: raise RuntimeError('Original manifest snapshot differs')
        self.document = json.loads(self.original)
        self.services = original_services(root, manifest_path, self.original)
        engines, workers = ([s for s in self.services if s['role'] == role] for role in ('engine', 'worker'))
        if len(engines) != 1 or len(workers) != 1: raise ValueError('Original deployment must have one engine and worker')
        self.engine, self.worker = engines[0], workers[0]
        self.protected = [s for s in self.services if s['role'] not in {'engine', 'worker'}]
        if any(s['role'] not in {'api', 'gateway'} for s in self.protected): raise ValueError('Unexpected protected service')
        self.env_bytes = {Path(s['env_file']): private_read(Path(s['env_file'])) for s in [*self.services, watchdog]}
        if {str(path): sha(data) for path, data in self.env_bytes.items()} != binding['environment_sha256']:
            raise RuntimeError('Original private environment changed after activation')
        self.engine_env, self.worker_env = (values(self.env_bytes[Path(s['env_file'])]) for s in (self.engine, self.worker))
        self._validate_current()  # Validate the immutable original adaptive64 config.
        if self.budget != binding['worker_budget_tokens']: raise RuntimeError('Original token budget differs')
        self.original_profile = validate_profile(activation['original_profile'])
        if self.original_profile != validate_profile(json.loads(self.worker_env['SCHEDULER_PROFILE_JSON'])):
            raise RuntimeError('Original numerical profile differs from its environment')
        self.profile = validate_profile(activation['profile'])
        if self.profile != collective.nccl_profile(self.original_profile)[0]: raise RuntimeError('NCCL profile derivation differs')
        self.candidate_engine = {**self.engine, 'env_file': str(activation_run / 'engine.env')}
        self.candidate_worker = {**self.worker, 'env_file': str(activation_run / 'worker.env')}
        self.candidate_engine_bytes = private_read(activation_run / 'engine.env')
        self.candidate_worker_bytes = private_read(activation_run / 'worker.env')
        self.candidate = copy.deepcopy(self.document)
        for service in self.candidate['services']:
            if self.key(service) == self.key(self.engine): service['env_file'] = self.candidate_engine['env_file']
            if self.key(service) == self.key(self.worker): service['env_file'] = self.candidate_worker['env_file']
        candidate_raw = json.dumps(self.candidate, separators=(',', ':')).encode()
        if private_read(manifest_path) != candidate_raw: raise RuntimeError('Live manifest is not the exact completed NCCL candidate')
        self.original_engine_only = copy.deepcopy(self.document)
        self.original_engine_only['services'] = [s for s in self.original_engine_only['services'] if s['role'] != 'worker']
        self.allowed_manifest_bytes = {self.original, candidate_raw}
        self.manager = manager or Manager(root=root, timeout=300)
        self.identity = identity_reader or (lambda service: identity(root, service))
        self.assert_drained = assert_drained or self._drained
        self.engine_observer = engine_observer or self._observe_engine
        self.profile_observer = worker_observer or self._observe_profile
        self.path_observer = path_observer or (lambda disabled: collective.running_collective(root, self.engine, disabled))
        self.clock, self.sleep = clock, sleep
        expected = copy.deepcopy(binding['owned_identities'])
        for service, field in ((self.engine, 'engine_identity_after'), (self.worker, 'worker_identity_after'),
                               (watchdog, 'watchdog_identity_after')):
            if not activation.get(field): raise RuntimeError('Completed candidate launch identity is absent')
            expected[self.key(service)] = activation[field]
        self.before = {self.key(s): self.identity(s) for s in [*self.services, watchdog]}
        if self.before != expected: raise RuntimeError('Current NCCL PID/start identity differs from the completed activation')
        self.expected = dict(expected)
        restore_binding = {'activation_report_sha256': activation_sha256,
            'controller_sha256': sha(Path(__file__).read_bytes()), 'owned_candidate_identities': self.before,
            'candidate_manifest_sha256': sha(candidate_raw), 'activation_binding': binding}
        if previous is not None:
            if previous.get('phase') != 'prepared' or previous.get('restore_binding') != restore_binding:
                raise RuntimeError('Prepared restoration changed; refusing evidence overwrite or service stop')
            self.report = previous
        else:
            if any(run.iterdir()): raise RuntimeError('Incomplete restoration preparation requires inspection')
            self.report = {'phase': 'prepared', 'operation': 'restore-custom-collective-after-nccl-comparison',
                'prepared_at': timestamp(), 'restore_binding': restore_binding, 'prepared_binding': binding,
                'activation_report': str(self.activation_path), 'worker_budget_tokens': self.budget,
                'profile_to_restore': self.original_profile, 'events': [], 'restored': False,
                'failure_policy': 'Exclude worker from original engine-only manifest and report manual recovery; never silently resume an unvalidated worker'}
        self.guard(initial=True); self.assert_drained()
        self.path_observer(True); self.profile_observer(self.profile)
        if previous is None: self.save()

    def save(self): private_write(self.restore_run / 'report.json', self.report)

    def guard(self, *, initial=False):
        super().guard(initial=initial)
        if sha(private_read(self.activation_path)) != self.report['restore_binding']['activation_report_sha256']:
            raise RuntimeError('Completed activation report changed during restoration')
        if sha(Path(__file__).read_bytes()) != self.report['restore_binding']['controller_sha256']:
            raise RuntimeError('Restoration implementation changed')

    def apply(self):
        self.guard(initial=True); self.assert_drained()
        self.report['candidate_collective_proof'] = self.path_observer(True)
        self.profile_observer(self.profile)
        self.report.update(phase='restoring', started_at=timestamp()); self.save()
        maintenance = False
        try:
            maintenance = True; self.stop(self.watchdog); self.guard()
            self.stop(self.candidate_worker); self.assert_drained(); self.stop(self.candidate_engine); self.guard()
            self._manifest(self.original_engine_only)
            self.report['restored_engine_preflight'] = self.start_engine(self.engine, False)
            self.guard(); self.assert_drained(); self._manifest()
            self.invoke(self.worker, 'start'); self.ready(self.worker, 60)
            self.report['restored_worker_preflight'] = self.profile_observer(self.original_profile)
            self.guard(); self.assert_drained()
            self.report.update(phase='complete', restored=True, completed_at=timestamp(),
                engine_identity_after=self.identity(self.engine), worker_identity_after=self.identity(self.worker),
                protected_services_unchanged=True)
        except BaseException as error:
            self.report.update(phase='manual_recovery_required', restored=False, failure_type=type(error).__name__)
            try:
                self._manifest(self.original_engine_only)
                self.stop(self.watchdog); self.stop(self.worker)
                self.report['worker_excluded_and_owned_worker_stopped'] = True
            except BaseException as exclusion:
                self.report.update(worker_excluded_and_owned_worker_stopped=False, exclusion_failure_type=type(exclusion).__name__)
            raise
        finally:
            if maintenance:
                safe = self.report['phase'] == 'complete' or self.report.get('worker_excluded_and_owned_worker_stopped') is True
                if safe:
                    try:
                        self.invoke(self.watchdog, 'start')
                        self.report.update(watchdog_restored=True, watchdog_identity_after=self.identity(self.watchdog))
                    except BaseException as error:
                        self.report.update(phase='manual_recovery_required', restored=False, watchdog_restored=False,
                                           watchdog_failure_type=type(error).__name__)
                        self.save(); raise
                else: self.report['watchdog_restored'] = False
            self.save()
        return self.report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--activation-operation', default='qwen235b-nccl-adaptive64-v1')
    parser.add_argument('--activation-sha256', required=True)
    parser.add_argument('--operation', default='qwen235b-custom-restore-v1')
    args = parser.parse_args(); os.umask(0o077)
    for name in (args.operation, args.activation_operation):
        if not re.fullmatch('[a-zA-Z0-9_-]{1,80}', name): raise ValueError('Invalid operation name')
    activation = ROOT / '.runtime' / args.activation_operation
    if not activation.is_dir() or activation.is_symlink(): raise ValueError('Activation directory must already exist and be real')
    run = private_operation_directory(ROOT, args.operation)
    fd = os.open(ROOT / '.runtime/qwen235b-maintenance.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        previous = json.loads(private_read(run / 'report.json')) if (run / 'report.json').exists() else None
        if args.apply and previous is None: raise RuntimeError('Explicit immutable restoration preparation required')
        original = private_read(activation / 'original-manifest.json')
        services = original_services(ROOT, ROOT / '.runtime/services-primary.json', original)
        engine = next(s for s in services if s['role'] == 'engine')
        watchdog = {'role': 'supervisor', 'name': 'primary', 'env_file': str(ROOT.parent / 'supervisor.env'),
                    'python': str(ROOT.parent / 'venv/bin/python')}
        roll = RestoreCollective(root=ROOT, run=run, activation_run=activation, activation_sha256=args.activation_sha256,
            manifest_path=ROOT / '.runtime/services-primary.json', watchdog=watchdog,
            support=collective.source_evidence(ROOT, engine), previous=previous)
        if args.apply: roll.apply()
    print(json.dumps({'phase': roll.report['phase'], 'restored': roll.report['restored'], 'report': str(run / 'report.json')}))


if __name__ == '__main__': main()
