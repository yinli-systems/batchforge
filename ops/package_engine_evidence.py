"""Validate and fsync an operator-owned engine evidence release; no service action."""
from pathlib import Path
import argparse,hashlib,json
from backend.engine_evaluations import EngineEvidenceCatalogue,evaluation,ARTIFACTS,private_release_bytes,EID,TENANT
from ops.ha.set_scheduler_policy import atomic_bytes


def package(paths,destination,*,tenant,eid,name):
    if set(paths)!=set(ARTIFACTS):raise ValueError('All pinned engine and correction artifacts required')
    if (not isinstance(tenant,str) or not TENANT.fullmatch(tenant) or not isinstance(eid,str) or not EID.fullmatch(eid)
        or not isinstance(name,str) or not 1<=len(name)<=128):raise ValueError('Explicit valid tenant and named evaluation required')
    blobs={key:private_release_bytes(Path(path),16*1024**2 if key=='scores' else 1024**2) for key,path in paths.items()}
    if sum(map(len,blobs.values()))>64*1024**2:raise ValueError('Bounded engine release required')
    entry={'id':eid,'name':name,'tenant_id':tenant}
    for key,raw in blobs.items():entry[key]={'file':key+('.jsonl' if key=='scores' else '.json'),'sha256':hashlib.sha256(raw).hexdigest()}
    # No output is created if the measured/corrected evidence does not agree.
    view=evaluation(entry,blobs)
    destination=Path(destination);destination.mkdir(mode=0o700,exist_ok=False)
    for key,raw in blobs.items():atomic_bytes(destination/entry[key]['file'],raw)
    catalogue=destination/'catalogue.json'
    atomic_bytes(catalogue,(json.dumps({'protocol':'batchforge.engine-quality-catalogue.v1','evaluations':[entry]},indent=2)+'\n').encode())
    published=EngineEvidenceCatalogue(catalogue).visible(tenant)
    if published!=[view]:raise ValueError('Materialized engine release differs')
    return {'phase':'prepared','passed':True,'catalogue':str(catalogue),'evidence_id':eid,'files':len(blobs)+1,
        'catalogue_sha256':hashlib.sha256(catalogue.read_bytes()).hexdigest(),
        'corrected_exact_match_percent':view['metrics']['exact_match_percent'],'f1_percent':view['metrics']['f1_percent'],
        'new_inference_requests':0,'database_changes':False,'primary_services_changed':False}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--benchmark',type=Path,required=True);p.add_argument('--audit',type=Path,required=True)
    p.add_argument('--correction',type=Path,required=True);p.add_argument('--out',type=Path,required=True)
    p.add_argument('--tenant',required=True);p.add_argument('--id',required=True);p.add_argument('--name',required=True)
    a=p.parse_args();b=a.benchmark;c=a.correction
    paths={'report':b/'run/report.json','plan':b/'run/plan.json','scores':b/'run/scores.jsonl','audit':a.audit,
        'campaign':b/'campaign.json','restore':b/'recovery-validation.json','correction':c/'run/report.json','correction_audit':c/'validation.json'}
    print(json.dumps(package(paths,a.out,tenant=a.tenant,eid=a.id,name=a.name)))


if __name__=='__main__':main()
