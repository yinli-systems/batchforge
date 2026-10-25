"""Independently verify a receipt checksum and bounded authenticated artifact bytes.

No payload is saved or printed. This is reproducibility checking, not an issuer
signature, a hardware attestation or an answer-quality assessment.
"""
from __future__ import annotations

import argparse
from datetime import datetime,timezone
import hashlib
import json
import os
from pathlib import Path
import re

import httpx

from backend.receipts import verify


def check_artifact(client,artifact,*,max_bytes):
    fid=artifact.get('id');expected=artifact.get('sha256')
    if not isinstance(fid,str) or not re.fullmatch(r'file_[a-f0-9]{32}',fid):
        raise ValueError('Invalid artifact identity')
    if not isinstance(expected,str) or not re.fullmatch('[a-f0-9]{64}',expected):
        return {'file_id':fid,'state':'hash_not_published','passed':False}
    declared=artifact.get('bytes')
    if type(declared) is not int or not 0<=declared<=max_bytes:
        return {'file_id':fid,'state':'declared_size_exceeds_bound','passed':False}
    count=0;digest=hashlib.sha256()
    with client.stream('GET','/v1/files/'+fid+'/content') as response:
        if response.status_code in (404,410):
            return {'file_id':fid,'state':'unavailable','http_status':response.status_code,'passed':False}
        response.raise_for_status()
        for chunk in response.iter_bytes():
            count+=len(chunk)
            if count>max_bytes:raise ValueError('Artifact stream exceeds readback bound')
            digest.update(chunk)
    matched=count==declared and digest.hexdigest()==expected
    return {'file_id':fid,'state':'matched' if matched else 'mismatch','passed':matched,
            'bytes':count,'sha256':digest.hexdigest(),'expected_sha256':expected}


def readback(client,envelope,*,max_bytes=64*1024**2):
    if not verify(envelope):raise ValueError('Receipt checksum is invalid')
    artifacts=envelope['receipt']['artifacts']
    if not isinstance(artifacts,list) or not 1<=len(artifacts)<=3:
        raise ValueError('Require one through three receipt artifacts')
    results=[check_artifact(client,a,max_bytes=max_bytes) for a in artifacts]
    return {'protocol':'batchforge.readback.v1','captured_at':datetime.now(timezone.utc).isoformat(),
        'receipt_sha256':envelope['receipt_sha256'],'batch_id':envelope['receipt']['batch_id'],
        'checksum_verified':True,'artifacts':results,'passed':all(r['passed'] for r in results),
        'scope':'Authenticated bytes read now, compared to the recorded receipt; no payload persisted, no attestation or quality claim'}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--receipt',type=Path,required=True);p.add_argument('--url',required=True)
    p.add_argument('--out',type=Path,required=True)
    args=p.parse_args();key=os.environ.get('BATCHFORGE_API_KEY')
    if not key:p.error('BATCHFORGE_API_KEY environment variable is required')
    if args.out.exists():p.error('Preserve previous readbacks; choose a new output path')
    raw=args.receipt.read_bytes()
    if len(raw)>1024**2:p.error('Receipt exceeds 1 MiB bound')
    with httpx.Client(base_url=args.url,headers={'Authorization':'Bearer '+key},trust_env=False,timeout=30,
                      follow_redirects=False) as client:
        report=readback(client,json.loads(raw))
    fd=os.open(args.out,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(fd,'w') as f:
        json.dump(report,f,indent=2);f.write('\n')
    print(json.dumps({'passed':report['passed'],'artifacts':len(report['artifacts'])}))
    if not report['passed']:raise SystemExit(1)


if __name__=='__main__':main()
