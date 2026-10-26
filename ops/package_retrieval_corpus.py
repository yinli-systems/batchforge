"""Prepare an owner-only retrieval release, without SQL, network or service action."""
from pathlib import Path
import argparse,hashlib,json
from agents.retrieval import Snapshot,Registry,REGISTRY,NAME,NAMESPACE,MAX_BYTES
from backend.evaluations import private_release_bytes
from agents.contracts import canonical
from ops.ha.set_scheduler_policy import atomic_bytes


def package(corpus,destination,*,tenant,namespace):
    if not isinstance(tenant,str) or not NAME.fullmatch(tenant) or not isinstance(namespace,str) or not NAMESPACE.fullmatch(namespace):
        raise ValueError('Explicit tenant and namespace audience required')
    raw=private_release_bytes(Path(corpus),MAX_BYTES);snapshot=Snapshot(raw)
    catalogue={'protocol':REGISTRY,'snapshots':[{'tenant_id':tenant,'namespace':namespace,'file':'corpus.json',
        'sha256':hashlib.sha256(raw).hexdigest(),'snapshot_id':snapshot.id}]}
    destination=Path(destination);destination.mkdir(mode=0o700,exist_ok=False)
    atomic_bytes(destination/'corpus.json',raw);atomic_bytes(destination/'catalogue.json',canonical(catalogue))
    loaded=Registry(destination/'catalogue.json')
    if loaded.visible(tenant)!=[{'namespace':namespace,**snapshot.manifest()}]:raise ValueError('Materialized retrieval identity differs')
    result={'phase':'prepared','passed':True,'snapshot':snapshot.manifest(),'namespace':namespace,
        'new_gpu_tasks':0,'database_modified':False,'catalogue':str(destination/'catalogue.json')}
    atomic_bytes(destination/'receipt.json',canonical(result));return result


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--corpus',required=True,type=Path)
    p.add_argument('--destination',required=True,type=Path);p.add_argument('--tenant',required=True);p.add_argument('--namespace',required=True)
    a=p.parse_args();print(json.dumps(package(a.corpus,a.destination,tenant=a.tenant,namespace=a.namespace)))


if __name__=='__main__':main()
