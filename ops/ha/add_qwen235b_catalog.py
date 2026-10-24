"""Add pinned Qwen235B to both API catalogs without changing any other setting."""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shlex
import sys

import httpx

from ops.ha.retune_full_gpu import file_env
from ops.ha.set_scheduler_policy import PolicyRoll, atomic_bytes
from ops.ha.switch_qwen235b import MODEL
from ops.supervise import ROOT, load_manifest, private_read


def catalog_bytes(raw):
    lines, index, names = raw.decode().splitlines(keepends=True), None, []
    for i, line in enumerate(lines):
        key, sep, value = line.strip().removeprefix('export ').strip().partition('=')
        if sep and key.strip() == 'MODEL_NAMES_JSON':
            if index is not None:
                raise ValueError('Duplicate MODEL_NAMES_JSON assignments')
            index = i
            names = json.loads(' '.join(shlex.split(value, comments=True)))
    if not isinstance(names, list) or any(not isinstance(n, str) or not n for n in names):
        raise ValueError('Invalid existing model catalog')
    if MODEL in names:
        return raw
    names.append(MODEL)
    replacement = 'MODEL_NAMES_JSON=' + shlex.quote(json.dumps(names, separators=(',', ':'))) + '\n'
    if index is None:
        if lines and not lines[-1].endswith(('\n', '\r')):
            lines[-1] += '\n'
        lines.append(replacement)
    else:
        lines[index] = replacement
    return ''.join(lines).encode()


class CatalogRoll(PolicyRoll):
    def __init__(self, *, access_env, **kwargs):
        super().__init__(policy='fair', verify_policy=self.verify_catalog, **kwargs)
        self.access_env = access_env
        self.patched = {path: catalog_bytes(raw) for path, raw in self.original.items()}
        self.report.pop('policy')
        self.report.update(model_added=MODEL, changed_environment_keys=['MODEL_NAMES_JSON'],
                           default_model_unchanged=True)
        from backend.config import Settings
        for path in self.original:
            env = file_env(path)
            if env.get('RESULT_ARCHIVE_ENABLED', 'false').lower() not in {'0', 'false'}:
                raise RuntimeError('Publisher must remain off during timed model experiments')
            settings = Settings(**{k.lower(): v for k, v in env.items()})
            if MODEL not in settings.served_models() and len(settings.served_models()) >= 16:
                raise ValueError('Catalog is full')

    def verify_catalog(self, service):
        expected = file_env(Path(service['env_file']))
        owned = self.identity(service)
        actual = dict(part.split(b'=', 1) for part in Path(f"/proc/{owned['pid']}/environ").read_bytes().split(b'\0') if b'=' in part)
        if any(actual.get(k.encode()) != v.encode() for k, v in expected.items()):
            raise RuntimeError('API effective environment differs from preserved configuration')
        token = file_env(self.access_env)['BATCHFORGE_API_KEY']
        with httpx.Client(trust_env=False, timeout=10, headers={'Authorization': 'Bearer ' + token}) as client:
            response = client.get(f'http://127.0.0.1:{service["port"]}/v1/models')
            response.raise_for_status()
            names = {v['id'] for v in response.json()['data']}
        if MODEL not in names:
            raise RuntimeError('New model absent from live API catalog')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--access-env', type=Path, default=ROOT.parent / 'access.env')
    args = parser.parse_args()
    os.umask(0o077)
    run = ROOT / '.runtime/qwen235b-catalog-v1'
    run.mkdir(mode=0o700, exist_ok=True)
    report = run / 'report.json'
    descriptor = os.open(ROOT / '.runtime/scheduler-policy.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, 'w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if report.exists() and json.loads(private_read(report)).get('phase') != 'prepared':
            raise RuntimeError('Existing catalog operation requires inspection instead of resubmission')
        manifest, services, _ = load_manifest(ROOT / '.runtime/services-primary.json')
        watchdog = {'role': 'supervisor', 'name': 'primary', 'env_file': str(ROOT.parent / 'supervisor.env'),
                    'python': str(ROOT.parent / 'venv/bin/python')}
        roll = CatalogRoll(root=ROOT, services=services, watchdog=watchdog, manifest_path=manifest,
                           report_path=report, access_env=args.access_env)
        atomic_bytes(run / 'original-manifest.json', private_read(manifest))
        for api in roll.apis:
            path = Path(api['env_file'])
            atomic_bytes(run / ('original-api-' + api['name'] + '.env'), roll.original[path])
        roll.report['controller_sha256'] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        if args.execute:
            roll.apply()
        else:
            roll.save()
        print(json.dumps({'report': str(report), 'phase': roll.report['phase']}), flush=True)


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        print(json.dumps({'error_type': type(error).__name__}), file=sys.stderr, flush=True)
        raise SystemExit(1) from None
