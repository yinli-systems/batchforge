"""Export a bounded, allowlisted evidence snapshot from the existing H100 Pod.

No private environments, workload text, tenant outputs, engine logs or checkpoint
weights are exported. A snapshot may be in progress: its manifest never upgrades
that state to a completed experiment. The destination must be new.
"""
from __future__ import annotations

import argparse
import base64
import io
import json
from pathlib import Path
import re
import subprocess
import tarfile


HOST = '4x7ej43hrqznjn-64410eb2@ssh.runpod.io'


def remote_script(remote_run):
    if not re.fullmatch(r'/root/batchforge-runtime/(?:platform480|promotion480)-[a-zA-Z0-9-]+', remote_run):
        raise ValueError('Only an owned platform480 experiment may be collected')
    return "stty -echo\npython3 - <<'BF_COLLECT_PY'\n" + f"root={remote_run!r}\n" + '''
from pathlib import Path
from datetime import datetime, timezone
import io, tarfile, base64, hashlib, json, re
root=Path(root)
paths=[]
for name in ['source-receipt.json','storage-preflight.json','launcher-v1.json',
             'hardware-topology.json',
             'source-deploy.json','rollout/report.json','rollout/original-manifest.json',
             'legacy-readback.json','primary-smoke.json','post-rollout-health.json',
             'lease-alignment.json','validation-launcher.json','validation.log','validator.py',
             'launcher-v2.json','queue-report.json','queue.py','queue-v2.py',
             'controller-v1.log','queue-v2.log',
             'launcher-v3.json','successor-v3.py','successor-v3.log',
             'storage-audit-correction.json',
             'trial-v1/trial.json','trial-v1/platform/report.json',
             'trial-v1/platform/gpu-engine-samples.jsonl']:
 p=root/name
 if p.is_file() and not p.is_symlink():paths.append(p)
platform=root/'trial-v1/platform'
if platform.is_dir():
 for child in sorted(platform.iterdir()):
  if child.is_dir() and re.fullmatch(r'[0-9]{2}-(capacity|policy|kv)-[AB]',child.name):
   for name in ['benchmark.json','worker-metrics.txt']:
    p=child/name
    if p.is_file() and not p.is_symlink():paths.append(p)
limit=32*1024*1024
manifest={'collected_at':datetime.now(timezone.utc).isoformat(),
          'source_run':str(root),'files':{},'scope':'Allowlisted snapshot; inspect report state for completion'}
buf=io.BytesIO();total=0
with tarfile.open(fileobj=buf,mode='w:gz') as tar:
 for p in paths:
  if p.stat().st_size>limit:raise RuntimeError('evidence_file_exceeds_bound')
  data=p.read_bytes();total+=len(data)
  if total>limit:raise RuntimeError('evidence_snapshot_exceeds_bound')
  source_name=str(p.relative_to(root));name=source_name+'.txt' if p.suffix=='.log' else source_name
  removed=0
  if p.suffix=='.jsonl' and data and not data.endswith(b'\\n'):
   end=data.rfind(b'\\n')+1;removed=len(data)-end;data=data[:end]
  manifest['files'][name]={'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest(),
                         'source_relative_path':source_name,'trailing_partial_jsonl_bytes_removed':removed}
  info=tarfile.TarInfo(name);info.size=len(data);info.mode=0o600
  tar.addfile(info,io.BytesIO(data))
 data=(json.dumps(manifest,indent=2)+'\\n').encode()
 info=tarfile.TarInfo('evidence-manifest.json');info.size=len(data);info.mode=0o600
 tar.addfile(info,io.BytesIO(data))
print('BF_EVIDENCE_BEGIN',flush=True)
print(base64.encodebytes(buf.getvalue()).decode(),flush=True)
print('BF_EVIDENCE_END',flush=True)
BF_COLLECT_PY
exit
'''


def collect(remote_run, destination):
    if destination.exists():
        raise ValueError('Evidence destination already exists; preserve it')
    result=subprocess.run(['ssh','-tt','-i',str(Path.home()/'.ssh/id_ed25519'),
        '-o','BatchMode=yes','-o','ConnectTimeout=10',HOST],
        input=remote_script(remote_run),text=True,stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,timeout=90)
    matches=re.findall(r'\nBF_EVIDENCE_BEGIN\n([A-Za-z0-9+/=\n]+)\nBF_EVIDENCE_END',
                       result.stdout.replace('\r',''))
    if not matches:raise RuntimeError('No complete evidence snapshot received')
    payload=base64.b64decode(matches[-1])
    if len(payload)>32*1024*1024:raise RuntimeError('Compressed evidence exceeds bound')
    destination.mkdir(parents=True,mode=0o700,exist_ok=False)
    with tarfile.open(fileobj=io.BytesIO(payload),mode='r:gz') as tar:
        tar.extractall(destination,filter='data')
    import hashlib
    manifest=json.loads((destination/'evidence-manifest.json').read_text())
    for name,receipt in manifest['files'].items():
        data=(destination/name).read_bytes()
        if len(data)!=receipt['bytes'] or hashlib.sha256(data).hexdigest()!=receipt['sha256']:
            raise RuntimeError('Transferred evidence does not match source receipt')
    print(json.dumps({'destination':str(destination),'files':len(manifest['files']),
                      'compressed_bytes':len(payload),'sha256_verified':True}))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--remote-run',required=True)
    parser.add_argument('--destination',type=Path,required=True)
    args=parser.parse_args();collect(args.remote_run,args.destination)


if __name__=='__main__':main()
