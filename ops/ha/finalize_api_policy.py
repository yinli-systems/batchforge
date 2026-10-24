"""Prepare an API-only default-model/future-result-policy roll; default dry-run.

Run only after timed work and with an explicit future UTC cutoff. Credentials
are private files/RAM only. This never stops an engine, worker or gateway.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import sys

import httpx

from backend.config import Settings
from backend.migrate import MIGRATIONS
from backend.scheduling import validate_profile
from ops.ha.retune_full_gpu import file_env
from ops.ha.set_scheduler_policy import PolicyRoll, atomic_bytes
from ops.supervise import ROOT, load_manifest, private_read

MODEL = 'Qwen/Qwen3-235B-A22B-Instruct-2507-FP8'
CHANGED_KEYS = {'MODEL_NAME','MODEL_NAMES_JSON','RESULT_READERS_ENABLED','RESULT_ARCHIVE_ENABLED',
                'RESULT_ARCHIVE_AFTER','RESULT_ARCHIVE_MIN_AGE_SECONDS','RESULT_ARCHIVE_PURGE_ROWS'}


def values(raw):
    result = {}
    for line in raw.decode().splitlines():
        text=line.strip().removeprefix('export ').strip()
        if not text or text.startswith('#'):continue
        key,sep,value=text.partition('=')
        if not sep or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',key) or key in result:
            raise ValueError('Invalid or duplicate private environment assignment')
        result[key]=' '.join(shlex.split(value,comments=True))
    return result


def cutoff_utc(value):
    result=datetime.fromisoformat(value.replace('Z','+00:00'))
    if result.utcoffset()!=timedelta(0):raise ValueError('Cutoff must be explicit UTC')
    return result


def configured_models(env):
    return Settings(**{k.lower():v for k,v in env.items()}).served_models()


def policy_bytes(raw, legacy, cutoff):
    old=values(raw)
    if old.get('SCHEDULER_POLICY')!='cost_aware':raise ValueError('Preserve the released cost-aware scheduler')
    if old.get('RESULT_READERS_ENABLED','').lower() not in {'true','1'} or old.get('RESULT_ARCHIVE_ENABLED','false').lower() not in {'false','0'}:
        raise ValueError('Leased readers must already be on and publisher must still be off')
    if old.get('BLOB_STORAGE_BACKEND')!='s3':raise ValueError('Requires the proven production S3 backend')
    names=[name for name in dict.fromkeys(legacy) if name!=MODEL]
    changes={'MODEL_NAME':MODEL,'MODEL_NAMES_JSON':json.dumps(names,separators=(',',':')),
        'RESULT_READERS_ENABLED':'true','RESULT_ARCHIVE_ENABLED':'true',
        'RESULT_ARCHIVE_AFTER':cutoff.isoformat(),'RESULT_ARCHIVE_MIN_AGE_SECONDS':'120',
        'RESULT_ARCHIVE_PURGE_ROWS':'1000'}
    output=[];written=set()
    for line in raw.decode().splitlines(keepends=True):
        key,sep,_=line.strip().removeprefix('export ').strip().partition('=')
        if sep and key in changes:
            output.append(key+'='+shlex.quote(changes[key])+'\n');written.add(key)
        else:output.append(line)
    for key,value in changes.items():
        if key not in written:
            if output and not output[-1].endswith(('\n','\r')):output[-1]+='\n'
            output.append(key+'='+shlex.quote(value)+'\n')
    patched=''.join(output).encode();new=values(patched)
    if {k:v for k,v in old.items() if k not in CHANGED_KEYS}!={k:v for k,v in new.items() if k not in CHANGED_KEYS}:
        raise RuntimeError('Protected API settings changed')
    if not set(legacy).issubset(configured_models(new)):raise ValueError('Legacy catalog would be lost')
    return patched


def validate_proofs(paths, expected_hashes):
    required={'backup','restore','production'}
    latest=MIGRATIONS[-1][0]
    if latest>8:required|={'current_backup','current_restore'}
    required_hashes={'restore','production'}|({'current_restore'} if latest>8 else set())
    if set(paths)!=required or not required_hashes.issubset(expected_hashes):
        raise ValueError('Exact reviewed restore and production hashes are required')
    records={};digests={}
    for name,path in paths.items():
        raw=private_read(path,2*1024*1024);digests[name]=hashlib.sha256(raw).hexdigest()
        if name in expected_hashes and digests[name]!=expected_hashes[name]:raise ValueError('Reviewed proof SHA differs')
        records[name]=json.loads(raw)
    backup,restore,production=(records[k] for k in ('backup','restore','production'))
    snapshot=backup.get('snapshot_sha256','')
    if (not re.fullmatch('[0-9a-f]{64}',snapshot) or not backup.get('archive_list_verified')
            or backup.get('database_modified') is not False
            or not restore.get('passed') or not restore.get('restore_verified')
            or restore.get('schema_version')!=8 or not restore.get('temporary_cluster_stopped')
            or not restore.get('temporary_cluster_removed') or restore.get('snapshot_sha256')!=snapshot
            or restore.get('migration_checksums')!={str(v):digest for v,_,digest in MIGRATIONS if v<=8}
            or any(restore.get(k)!=0 for k in ['batch_count_state_usage_mismatches',
                'accepted_attempt_count_usage_mismatches','tenant_queue_pending_mismatches','unvalidated_constraints'])):
        raise ValueError('Passed schema8 backup/restore evidence is required')
    if (not production.get('passed') or not production.get('restore_verified_before_purge')
            or production.get('backup_sha256')!=snapshot or production.get('task_payloads_cleared')!=20
            or not production.get('attempt_counts_and_usage_unchanged')
            or not production.get('batch_counters_and_identity_unchanged')
            or set(production.get('byte_identical_on_ports',[]))!={8080,8082,8083}
            or not 0<production.get('download_bytes',0)<=16*1024*1024
            or not re.fullmatch('[0-9a-f]{64}',production.get('output_sha256',''))
            or not production.get('output_file_id') or not production.get('archive_publication_still_disabled')):
        raise ValueError('Passed exact20 production archival evidence is required')
    steps=production.get('steps',[])
    if not steps or not steps[-1].get('purged') or steps[-1].get('rows_purged')!=20:
        raise ValueError('Production purge proof is incomplete')
    current_snapshot=None
    if latest>8:
        current_backup,current_restore=(records[k] for k in ('current_backup','current_restore'))
        current_snapshot=current_backup.get('snapshot_sha256','')
        current_invariants=['batch_count_state_usage_mismatches','accepted_attempt_count_usage_mismatches',
            'tenant_queue_pending_mismatches','unvalidated_constraints','retention_ledger_mismatches',
            'retention_undercharged_tasks','retention_missing_tenant_ledgers']
        if latest>=10:current_invariants.append('auxiliary_ledger_mismatches')
        if (not re.fullmatch('[0-9a-f]{64}',current_snapshot)
                or not current_backup.get('archive_list_verified')
                or current_backup.get('database_modified') is not False
                or not current_restore.get('passed') or not current_restore.get('restore_verified')
                or current_restore.get('schema_version')!=latest
                or current_restore.get('snapshot_sha256')!=current_snapshot
                or current_restore.get('migration_checksums')!={str(v):digest for v,_,digest in MIGRATIONS}
                or not current_restore.get('temporary_cluster_stopped')
                or not current_restore.get('temporary_cluster_removed')
                or any(current_restore.get(k)!=0 for k in current_invariants)):
            raise ValueError('Passed current-schema backup/restore evidence is required')
    return {'sha256':digests,'snapshot_sha256':snapshot,'output_file_id':production['output_file_id'],
            'output_sha256':production['output_sha256'],'output_bytes':production['download_bytes'],
            'current_schema_snapshot_sha256':current_snapshot}


class FinalPolicyRoll(PolicyRoll):
    def __init__(self, *, cutoff, proof_paths, proof_hashes, access_env, clock=None,
                 environment_reader=None, catalog_reader=None, archive_reader=None, **kwargs):
        self.cutoff=cutoff_utc(cutoff) if isinstance(cutoff,str) else cutoff
        self.clock=clock or (lambda:datetime.now(timezone.utc))
        if self.cutoff.tzinfo is None or self.cutoff.utcoffset()!=timedelta(0) or self.cutoff<=self.clock():
            raise ValueError('Cutoff must still be in the future')
        self.proof_paths,self.proof_hashes=proof_paths,proof_hashes
        self.proofs=validate_proofs(proof_paths,proof_hashes)
        self.access_env=access_env
        self.environment_reader=environment_reader or self._process_environment
        self.catalog_reader=catalog_reader or self._catalog
        self.archive_reader=archive_reader or self._archive
        super().__init__(policy='cost_aware',verify_policy=self.verify_live,**kwargs)
        self.watchdog_bytes=private_read(Path(self.watchdog['env_file']))
        engines=[s for s in self.protected if s['role']=='engine']
        workers=[s for s in self.protected if s['role']=='worker']
        if len(engines)!=1 or len(workers)!=1:raise ValueError('Final policy requires the sole current Qwen235 pool')
        self.current_engine,self.current_worker=engines[0],workers[0]
        engine_env,worker_env=(file_env(Path(s['env_file'])) for s in [engines[0],workers[0]])
        if (engine_env.get('MODEL_NAME')!=MODEL or worker_env.get('MODEL_NAME')!=MODEL
                or engine_env.get('ENGINE_REVISION')!='e156cb4efae43fbee1a1ab073f946a1377e6b969'
                or engine_env.get('ENGINE_TENSOR_PARALLEL_SIZE')!='4'
                or worker_env.get('INFERENCE_BASE_URLS')!=f"http://127.0.0.1:{engines[0]['port']}"
                or worker_env.get('ENGINE_MAX_MODEL_LEN')!=engine_env.get('ENGINE_MAX_MODEL_LEN')):
            raise ValueError('Current Qwen engine/worker contract differs')
        self.current_profile=validate_profile(json.loads(worker_env['SCHEDULER_PROFILE_JSON']))
        self.current_worker_id=worker_env['WORKER_ID']
        maximum=int(worker_env['WORKER_MAX_INFLIGHT'])
        self.current_capacity=(maximum if worker_env.get('WORKER_ADAPTIVE_KV_ENABLED','false').lower() in {'true','1'}
            else min(maximum,int(worker_env['WORKER_TOKEN_BUDGET'])//int(worker_env['ENGINE_MAX_MODEL_LEN'])))
        self.legacy=list(dict.fromkeys(name for raw in self.original.values() for name in configured_models(values(raw))))
        self.patched={path:policy_bytes(raw,self.legacy,self.cutoff) for path,raw in self.original.items()}
        self.report.pop('policy')
        self.report.update(operation='qwen235-default-and-future-result-archival',
            changed_environment_keys=sorted(CHANGED_KEYS),default_model=MODEL,legacy_models_preserved=self.legacy,
            future_batch_cutoff=self.cutoff.isoformat(),minimum_terminal_age_seconds=120,purge_page_rows=1000,
            proof_evidence=self.proofs,api_environment_sha256_before={str(p):hashlib.sha256(raw).hexdigest() for p,raw in self.original.items()},
            preserved_qwen_profile={'worker_id':self.current_worker_id,'profile_id':self.current_profile['profile_id'],
                'runtime_fingerprint':self.current_profile['runtime_fingerprint'],'capacity':self.current_capacity},
            api_flags_before={str(p):{k:v for k,v in values(raw).items() if k in CHANGED_KEYS|{'SCHEDULER_POLICY','BLOB_STORAGE_BACKEND'}} for p,raw in self.original.items()})

    def _drained(self):
        import psycopg
        env=file_env(Path(self.apis[0]['env_file']))
        with psycopg.connect(env['DATABASE_URL'],connect_timeout=5,
                options='-c default_transaction_read_only=on -c statement_timeout=5000') as conn:
            if conn.execute("SELECT count(*) FROM tasks WHERE status IN ('pending','running')").fetchone()[0]:
                raise RuntimeError('refused_active_tasks')
            row=conn.execute("""SELECT scheduler_profile_id,runtime_fingerprint,concurrency,
                coalesce((metadata->>'ready_engine_count')::integer,0),
                coalesce((metadata->'kv_admission'->>'paused')::boolean,false)
                FROM workers WHERE id=%s AND model=%s AND last_seen_at>clock_timestamp()-interval '60 seconds'""",
                (self.current_worker_id,MODEL)).fetchone()
        if row!=(self.current_profile['profile_id'],self.current_profile['runtime_fingerprint'],self.current_capacity,1,False):
            raise RuntimeError('Current registered Qwen profile or capacity is not ready')

    def guard(self):
        super().guard()
        if private_read(Path(self.watchdog['env_file']))!=self.watchdog_bytes:raise RuntimeError('Watchdog config changed')
        if not all(self.manager.readiness(s) for s in [self.current_engine,self.current_worker]):
            raise RuntimeError('Current preserved Qwen pool is not ready')

    def invoke(self,service,action):
        value=super().invoke(service,action)
        if service['role']=='supervisor' and action=='start':
            status,error=self.manager.invoke(service,'status')
            if error or not status.get('running'):raise RuntimeError('Restored watchdog not running')
            self.report['watchdog_identity_after']=self.identity(service)
        return value

    def _process_environment(self,service):
        identity=self.identity(service)
        result={}
        for part in Path(f"/proc/{identity['pid']}/environ").read_bytes().split(b'\0'):
            key,sep,value=part.partition(b'=')
            if sep:
                name=key.decode()
                if name in result:raise RuntimeError('Duplicate effective API environment key')
                result[name]=value.decode()
        if self.identity(service)!=identity:raise RuntimeError('API identity changed during inspection')
        return result

    def _client(self):
        return httpx.Client(trust_env=False,timeout=10,headers={'Authorization':'Bearer '+file_env(self.access_env)['BATCHFORGE_API_KEY']})

    def _catalog(self,service):
        with self._client() as client:
            response=client.get(f"http://127.0.0.1:{service['port']}/v1/models");response.raise_for_status()
            if len(response.content)>128*1024:raise RuntimeError('Catalog response exceeds bound')
            return {row['id'] for row in response.json()['data']}

    def _archive(self,service):
        digest,size=hashlib.sha256(),0
        with self._client() as client,client.stream('GET',f"http://127.0.0.1:{service['port']}/v1/files/{self.proofs['output_file_id']}/content") as response:
            response.raise_for_status()
            for data in response.iter_bytes():
                size+=len(data)
                if size>self.proofs['output_bytes']:raise RuntimeError('Archived result exceeds reviewed bytes')
                digest.update(data)
        return {'bytes':size,'sha256':digest.hexdigest()}

    def observe(self,service,raw):
        expected=values(raw);actual=self.environment_reader(service)
        if any(actual.get(k)!=v for k,v in expected.items()):raise RuntimeError('Effective API configuration differs')
        if actual.get('RESULT_READERS_ENABLED','').lower() not in {'true','1'} or actual.get('BLOB_STORAGE_BACKEND')!='s3':
            raise RuntimeError('Live leased S3 reader not observed')
        if set(self.catalog_reader(service))!=set(configured_models(expected)):raise RuntimeError('Live catalog differs')
        output=self.archive_reader(service)
        if output!={'bytes':self.proofs['output_bytes'],'sha256':self.proofs['output_sha256']}:
            raise RuntimeError('Live archived20 reader checksum differs')
        return {'api':self.key(service),'identity':self.identity(service),'leased_s3_reader':True,
            'archived20_bytes':output['bytes'],'archived20_sha256':output['sha256']}

    def verify_live(self,service):
        if self.clock()>=self.cutoff:raise RuntimeError('Future cutoff elapsed before rolling completed')
        observation=self.observe(service,self.patched[Path(service['env_file'])])
        self.report.setdefault('live_verifications_after',[]).append(observation)

    def await_ready(self,service):
        super().await_ready(service)
        if self.report.get('phase')=='rolling_back':self.observe(service,self.original[Path(service['env_file'])])

    def apply(self):
        if self.cutoff-self.clock()<timedelta(seconds=120):raise RuntimeError('Cutoff needs at least120seconds rollout lead')
        if validate_proofs(self.proof_paths,self.proof_hashes)!=self.proofs:raise RuntimeError('Prepared proof changed')
        self.guard();self.assert_drained()
        self.report['both_existing_leased_readers_observed']=[self.observe(api,self.original[Path(api['env_file'])]) for api in self.apis]
        return super().apply()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cutoff',required=True,help='Future explicit UTC, at least120s beyond execution start')
    parser.add_argument('--report',type=Path,default=ROOT/'.runtime/final-api-policy-v1/report.json')
    parser.add_argument('--backup-report',type=Path,default=ROOT/'.runtime/archival-v8/pre-purge-backup.json')
    parser.add_argument('--restore-report',type=Path,default=ROOT/'.runtime/archival-v8/pre-purge-restore.json')
    parser.add_argument('--restore-sha256',required=True)
    parser.add_argument('--production-proof',type=Path,required=True)
    parser.add_argument('--production-sha256',required=True)
    parser.add_argument('--current-backup-report',type=Path)
    parser.add_argument('--current-restore-report',type=Path)
    parser.add_argument('--current-restore-sha256')
    parser.add_argument('--access-env',type=Path,default=ROOT.parent/'access.env')
    parser.add_argument('--execute',action='store_true')
    args=parser.parse_args();os.umask(0o077)
    if MIGRATIONS[-1][0]>8 and (not args.current_backup_report or not args.current_restore_report
            or not re.fullmatch('[0-9a-f]{64}',args.current_restore_sha256 or '')):
        parser.error('A new schema requires its own exact reviewed current backup and restore proof')
    proof_paths={'backup':args.backup_report,'restore':args.restore_report,'production':args.production_proof}
    proof_hashes={'restore':args.restore_sha256,'production':args.production_sha256}
    if MIGRATIONS[-1][0]>8:
        proof_paths.update(current_backup=args.current_backup_report,current_restore=args.current_restore_report)
        proof_hashes['current_restore']=args.current_restore_sha256
    report=args.report.resolve();runtime=ROOT/'.runtime'
    if (not report.is_relative_to(runtime) or report.parent==runtime or report.name!='report.json'
            or any(not re.fullmatch('[0-9a-f]{64}',v) for v in [args.restore_sha256,args.production_sha256])):
        parser.error('Requires a private dedicated runtime/report.json and exact reviewed proof hashes')
    report.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
    fd=os.open(runtime/'scheduler-policy.lock',os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,'w') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        old=json.loads(private_read(report)) if report.exists() else None
        if old and (old.get('phase')!='prepared' or old.get('future_batch_cutoff')!=cutoff_utc(args.cutoff).isoformat()):
            raise RuntimeError('Existing operation refuses duplicate or uncertain rollout')
        manifest,services,_=load_manifest(runtime/'services-primary.json')
        watchdog={'role':'supervisor','name':'primary','env_file':str(ROOT.parent/'supervisor.env'),'python':str(ROOT.parent/'venv/bin/python')}
        roll=FinalPolicyRoll(root=ROOT,services=services,watchdog=watchdog,manifest_path=manifest,
            report_path=report,cutoff=args.cutoff,proof_paths=proof_paths,
            proof_hashes=proof_hashes,access_env=args.access_env)
        if old and (old['manifest_sha256']!=roll.report['manifest_sha256'] or old['api_environment_sha256_before']!=roll.report['api_environment_sha256_before']):
            raise RuntimeError('Prepared environment or manifest drifted')
        for api in roll.apis:
            path=report.parent/('original-api-'+api['name']+'.env');raw=roll.original[Path(api['env_file'])]
            if path.exists() and private_read(path)!=raw:raise RuntimeError('Refusing to overwrite original API snapshot')
            if not path.exists():atomic_bytes(path,raw)
        roll.report['controller_sha256']=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        if args.execute:roll.apply()
        else:roll.save()
        print(json.dumps({'phase':roll.report['phase'],'report':str(report),'future_batch_cutoff':roll.cutoff.isoformat()}))


if __name__=='__main__':
    try:main()
    except Exception as error:
        print(json.dumps({'error_type':type(error).__name__}),file=sys.stderr)
        raise SystemExit(1) from None
