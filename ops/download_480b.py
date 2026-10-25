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
    parser.add_argument('--workers', type=int, choices=range(1, 17), default=8)
    parser.add_argument('--spool', type=Path, required=True,
                        help='Existing container-disk staging directory; never fetch large shards directly into the network mount')
    parser.add_argument('--publish', action='store_true',
                        help='Publish into the existing object volume only after its write health and storage budget are verified')
    parser.add_argument('--reuse-archive', action='store_true',
                        help='Explicitly attempt reads of existing public archive shards; default avoids a degraded mount entirely')
    args = parser.parse_args()
    if args.publish:
        from ops.storage_semantics import require_journal_filesystem
        require_journal_filesystem(args.destination)
    destination = args.destination.resolve()
    if not destination.is_relative_to('/workspace/batchforge/models'):
        raise ValueError('Destination must be on the existing /workspace volume')
    os.umask(0o077)
    if args.publish:
        destination.mkdir(parents=True, exist_ok=True)
    spool = args.spool.resolve()
    if not spool.is_relative_to('/root/batchforge-runtime') or spool == destination:
        raise ValueError('Staging must use the existing container disk')
    spool.mkdir(parents=True, exist_ok=True)
    import fcntl
    with (spool / 'download.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with urllib.request.urlopen(f'https://huggingface.co/api/models/{REPO}/revision/{REVISION}?blobs=true', timeout=30) as response:
            inventory = json.load(response)
        if inventory['sha'] != REVISION or inventory.get('gated') or inventory['cardData']['license'] != 'apache-2.0':
            raise ValueError('Unexpected model revision, gate, or license')
        files = [entry for entry in inventory['siblings'] if entry['rfilename'] in DATA_FILES or
                 (entry['rfilename'].startswith('model-') and entry['rfilename'].endswith('.safetensors'))]
        expected = sum(entry['size'] for entry in files)
        existing = (sum((destination / entry['rfilename']).stat().st_size for entry in files
                        if (destination / entry['rfilename']).is_file()) if args.publish else 0)
        if args.publish and shutil.disk_usage(destination).free < expected - existing + 20 * 1024**3:
            raise ValueError('Insufficient disk capacity plus 20 GiB margin')
        if shutil.disk_usage(spool).free < expected + 20 * 1024**3:
            raise ValueError('Insufficient container staging capacity')
        receipt = {'repo': REPO, 'revision': REVISION, 'download_bytes': expected,
            'started_at': datetime.now(timezone.utc).isoformat(), 'files': files,
            'license': 'apache-2.0', 'remote_code': False, 'status': 'downloading'}
        (spool / 'download-receipt.json').write_text(json.dumps(receipt, indent=2))
        os.environ['HF_HUB_OFFLINE'] = '0'
        os.environ['HF_HUB_DISABLE_TELEMETRY'] = '1'
        from huggingface_hub import hf_hub_download
        def download(entry):
            name = entry['rfilename']
            if Path(name).name != name:
                raise ValueError('Unexpected nested model path')
            existing_path = destination / name
            path = spool / name
            expected_hash = entry.get('lfs', {}).get('sha256')
            # Reuse an already verified public shard without consuming network
            # download twice. Never trust a size-only match for weight shards.
            if args.reuse_archive and not path.exists() and expected_hash:
                try:
                    if existing_path.is_file() and existing_path.stat().st_size == entry['size'] and sha256(existing_path) == expected_hash:
                        shutil.copyfile(existing_path, path)
                except OSError as error:
                    print(json.dumps({'event': 'archive_read_unavailable', 'file': name, 'errno': error.errno}), flush=True)
            if not (expected_hash and path.is_file() and path.stat().st_size == entry['size'] and sha256(path) == expected_hash):
                path = Path(hf_hub_download(REPO, name, revision=REVISION, local_dir=spool,
                                           token=False, local_files_only=False))
            if path.stat().st_size != entry['size']:
                raise ValueError('Downloaded size mismatch')
            digest = sha256(path)
            if expected_hash and digest != expected_hash:
                raise ValueError('Downloaded SHA-256 mismatch')
            print(json.dumps({'event': 'verified', 'file': name, 'bytes': entry['size'], 'sha256': digest}), flush=True)
            return {'file': name, 'bytes': entry['size'], 'sha256': digest, 'lfs_hash_verified': bool(expected_hash)}
        started = time.monotonic()
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            verified = list(pool.map(download, files))
        index = json.loads((spool / 'model.safetensors.index.json').read_text())
        shards = set(index['weight_map'].values())
        if not shards.issubset({entry['file'] for entry in verified}):
            raise ValueError('Missing checkpoint shards')
        if not args.publish:
            receipt.update(status='verified-local', verified=verified, elapsed_seconds=time.monotonic()-started,
                           persistent_copy_verified=False, finished_at=datetime.now(timezone.utc).isoformat())
            (spool / 'download-receipt.json').write_text(json.dumps(receipt, indent=2))
            print(json.dumps({'status': 'verified-local', 'revision': REVISION, 'bytes': expected}), flush=True)
            return
        # The network mount failed small journal flushes while several large
        # shards were open for writing. Publish one verified shard at a time,
        # with no concurrent network-mount weight writers. A receipt is never
        # marked verified before every published byte has been read back.
        for entry in verified:
            source, target = spool / entry['file'], destination / entry['file']
            if target.is_file() and target.stat().st_size == entry['bytes'] and sha256(target) == entry['sha256']:
                continue
            temporary = target.with_name(target.name + '.publishing')
            with source.open('rb') as incoming, temporary.open('wb') as outgoing:
                shutil.copyfileobj(incoming, outgoing, 4 * 1024 * 1024)
                outgoing.flush()
                os.fsync(outgoing.fileno())
            if temporary.stat().st_size != entry['bytes'] or sha256(temporary) != entry['sha256']:
                raise ValueError('Persistent copy verification failed')
            temporary.replace(target)
            print(json.dumps({'event': 'published', 'file': entry['file'], 'bytes': entry['bytes']}), flush=True)
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
