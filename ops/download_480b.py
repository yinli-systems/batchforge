"""Download one public, pinned AWQ checkpoint onto an existing persistent volume.

Downloads data and safetensors only; no repository Python is fetched/executed.
No provider API, resource creation, or GPU operation is performed.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import time
import urllib.request

REPO = 'QuantTrio/Qwen3-Coder-480B-A35B-Instruct-AWQ'
REVISION = '9ce3eaa67fe88609afec235117e97eb03d9b3cda'
DATA_FILES = {'config.json', 'generation_config.json', 'tokenizer.json', 'tokenizer_config.json',
              'vocab.json', 'merges.txt', 'model.safetensors.index.json', 'README.md', 'LICENSE'}


def sha256(path):
    checksum = hashlib.sha256()
    with path.open('rb') as source:
        for block in iter(lambda: source.read(8 * 1024 * 1024), b''):
            checksum.update(block)
    return checksum.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--destination', type=Path, required=True)
    args = parser.parse_args()
    destination = args.destination.resolve()
    if not destination.is_relative_to('/workspace/batchforge/models'):
        raise ValueError('Destination must be on the existing /workspace volume')
    os.umask(0o077)
    destination.mkdir(parents=True, exist_ok=True)
    import fcntl
    with (destination / 'download.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with urllib.request.urlopen(f'https://huggingface.co/api/models/{REPO}/revision/{REVISION}?blobs=true', timeout=30) as response:
            inventory = json.load(response)
        if inventory['sha'] != REVISION or inventory.get('gated') or inventory['cardData']['license'] != 'apache-2.0':
            raise ValueError('Unexpected model revision, gate, or license')
        files = [entry for entry in inventory['siblings'] if entry['rfilename'] in DATA_FILES or
                 (entry['rfilename'].startswith('model-') and entry['rfilename'].endswith('.safetensors'))]
        expected = sum(entry['size'] for entry in files)
        existing = sum((destination / entry['rfilename']).stat().st_size for entry in files
                       if (destination / entry['rfilename']).is_file())
        if shutil.disk_usage(destination).free < expected - existing + 20 * 1024**3:
            raise ValueError('Insufficient disk capacity plus 20 GiB margin')
        receipt = {'repo': REPO, 'revision': REVISION, 'download_bytes': expected,
            'started_at': datetime.now(timezone.utc).isoformat(), 'files': files,
            'license': 'apache-2.0', 'remote_code': False, 'status': 'downloading'}
        (destination / 'download-receipt.json').write_text(json.dumps(receipt, indent=2))
        os.environ['HF_HUB_OFFLINE'] = '0'
        os.environ['HF_HUB_DISABLE_TELEMETRY'] = '1'
        from huggingface_hub import hf_hub_download
        def download(entry):
            name = entry['rfilename']
            if Path(name).name != name:
                raise ValueError('Unexpected nested model path')
            path = Path(hf_hub_download(REPO, name, revision=REVISION, local_dir=destination,
                                       token=False, local_files_only=False))
            if path.stat().st_size != entry['size']:
                raise ValueError('Downloaded size mismatch')
            expected_hash = entry.get('lfs', {}).get('sha256')
            digest = sha256(path)
            if expected_hash and digest != expected_hash:
                raise ValueError('Downloaded SHA-256 mismatch')
            print(json.dumps({'event': 'verified', 'file': name, 'bytes': entry['size'], 'sha256': digest}), flush=True)
            return {'file': name, 'bytes': entry['size'], 'sha256': digest, 'lfs_hash_verified': bool(expected_hash)}
        started = time.monotonic()
        with ThreadPoolExecutor(max_workers=4) as pool:
            verified = list(pool.map(download, files))
        index = json.loads((destination / 'model.safetensors.index.json').read_text())
        shards = set(index['weight_map'].values())
        if not shards.issubset({entry['file'] for entry in verified}):
            raise ValueError('Missing checkpoint shards')
        receipt.update(status='verified', verified=verified, elapsed_seconds=time.monotonic()-started,
                       finished_at=datetime.now(timezone.utc).isoformat())
        temporary = destination / 'download-receipt.json.tmp'
        with temporary.open('w') as output:
            json.dump(receipt, output, indent=2)
            output.flush()
            os.fsync(output.fileno())
        temporary.replace(destination / 'download-receipt.json')
        print(json.dumps({'status': 'verified', 'revision': REVISION, 'bytes': expected}), flush=True)


if __name__ == '__main__':
    main()
