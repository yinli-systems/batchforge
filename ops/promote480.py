"""Promote an already qualified 480B runtime using existing app processes only.

Requires a closed repeated KV qualification, a drained operator window and the
compatible API admission-gate source already installed. No provisioning, schema
change, cloud-resource restart, data deletion or credential export is performed.
Configuration backups remain owner-only on the same Pod. Default is preparation;
--apply performs the finite application rollout with bounded rollback.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import sys
import time
import uuid

from ops.download_480b import REPO, REVISION
from ops.gpu_campaign import write_json
from ops.gpu_model_trial import candidate_env_path
from ops.platform480 import admission_budget


def require(value, why):
    if not value:raise RuntimeError(why)


def qualified(report, trial):
    require(report.get('passed') is True and report.get('finished_at')
            and not report.get('cleanup_error_type'), 'closed_successful_canary_required')
    require(trial.get('phase')=='platform_canary_passed' and trial.get('finished_at')
            and trial.get('restore_failures')==[] and not trial.get('candidate_stop_failed'),
            'prior_serving_restoration_required')
    require(report.get('model')==REPO and report.get('revision')==REVISION,'pinned_model_required')
    phases=report['phases']
    require(len(phases)==12 and len(report['plan'])==12 and report.get('workload_repeats')==3,
            'three_full_kv_abba_blocks_required')
    for repeat in range(3):
        block=[p for p in phases if p['repeat']==repeat]
        require([p['treatment'] for p in block]==list('ABBA') and all(p['study']=='kv' and p['policy']=='fair' for p in block),
                'qualified_kv_plan_differs')
        require([p['adaptive'] for p in block]==[False,True,True,False]
                and [p['slots'] for p in block]==[8,32,32,8]
                and all(p['audit']['tasks']==60 and p['audit']['one_accepted_attempt_per_task']
                        and p['audit']['batch_task_attempt_metering_match'] for p in block),
                'qualified_kv_acceptance_differs')
    require(len(report.get('faults',[]))==2 and all(f.get('adaptive') is True
                and f['stale_heartbeat_status']==f['stale_completion_status']==409
                and f['metering']['tasks']==60 for f in report['faults']),
            'adaptive_process_recovery_qualification_required')
    require({f['scope'] for f in report['faults']}=={'worker_process_loss','engine_and_worker_process_loss'},
            'both_fault_scopes_required')
    persistence=report.get('persistence',{})
    require(persistence.get('verified_files_after_api_restart')==28 and persistence.get('s3_input_objects')==14
            and persistence.get('archived_batches')==14 and persistence.get('s3_https_readback_verified') is True,
            'all_14_qualification_batches_require_restart_readback')


def verify_file(model, entry):
    path=model/entry['file']
    require(not path.is_symlink() and path.resolve().is_relative_to(model.resolve()),'unsafe_checkpoint_file')
    require(path.stat().st_size==entry['bytes'],'checkpoint_size_changed')
    digest=hashlib.sha256()
    with path.open('rb') as reader:
        while block:=reader.read(8*1024*1024):digest.update(block)
    require(digest.hexdigest()==entry['sha256'],'checkpoint_hash_changed')
    return entry['bytes']


class Promotion:
    def __init__(self, root, run, qualification):
        self.root,self.run=root.resolve(),run.resolve()
        require(self.root==Path('/root/batchforge-runtime/app') and self.run.is_relative_to(self.root.parent),
                'owned_existing_runtime_required')
        self.run.mkdir(mode=0o700,exist_ok=False)
        sys.path.insert(0,str(self.root))
        from ops.supervise import Manager,load_manifest,private_read,private_write
        from ops.ha.retune_full_gpu import file_env,write_env
        from backend.config import Settings
        import httpx,psycopg
        self.manager=Manager(root=self.root,timeout=120)
        self.write_env,self.private_write=write_env,private_write
        qrun=qualification.resolve()
        require(qrun.is_relative_to(self.root.parent),'local_qualification_required')
        p=qrun/'platform/report.json';q=json.loads(p.read_text())
        qualified(q,json.loads((qrun/'trial.json').read_text()))
        require('batch_submission_enabled' in Settings.model_fields,'deploy_compatible_api_gate_source_first')
        self.manifest,self.services,_=load_manifest(self.root/'.runtime/services-primary.json',root=self.root)
        self.original=json.loads(private_read(self.manifest))
        private_write(self.run/'original-manifest.json',self.original)
        self.apis=[s for s in self.services if s['role']=='api']
        self.old_engine=next(s for s in self.services if s['role']=='engine')
        self.old_worker=next(s for s in self.services if s['role']=='worker')
        require(self.old_engine['name']==self.old_worker['name']=='qwen235b','expected_235b_rollback_pair_required')
        self.api_envs={s['name']:file_env(s['env_file']) for s in self.apis}
        for service in self.apis:
            write_env(self.run/('original-api-'+service['name']+'.env'),self.api_envs[service['name']])
        first=next(iter(self.api_envs.values()))
        self.db=psycopg.connect(first['DATABASE_URL'],autocommit=True,connect_timeout=10,
                               options='-c default_transaction_read_only=on -c statement_timeout=10000')
        self.key=file_env(self.root.parent/'access.env')['BATCHFORGE_API_KEY']
        self.http=httpx.Client(timeout=10,trust_env=False,headers={'Authorization':'Bearer '+self.key})
        self.watchdog={'role':'supervisor','name':'primary','python':str(self.root.parent/'venv/bin/python'),
                       'env_file':str(self.root.parent/'supervisor.env')}
        for name,digest in q['source_sha256'].items():
            if name in {'worker/runtime.py','worker/config.py','worker/kv_admission.py'}:
                require(hashlib.sha256((self.root/name).read_bytes()).hexdigest()==digest,'qualified_worker_source_changed')
        env_path=candidate_env_path(self.root,qrun)
        e=file_env(env_path)
        expected={'MODEL_NAME':REPO,'ENGINE_REVISION':REVISION,'ENGINE_TENSOR_PARALLEL_SIZE':'4',
                  'ENGINE_MAX_MODEL_LEN':'18440','ENGINE_MAX_NUM_SEQS':'32','ENGINE_ENFORCE_EAGER':'1',
                  'ENGINE_ENABLE_PREFIX_CACHING':'0','ENGINE_MAX_NUM_BATCHED_TOKENS':'4096'}
        require(all(e.get(k)==v for k,v in expected.items()),'qualified_engine_configuration_changed')
        self.engine={'role':'engine','name':'qwen480bench','env_file':str(env_path),
                     'python':self.old_engine['python'],'port':8348,'gpu':self.old_engine['gpu']}
        canary_name='p480-'+hashlib.sha256(str(qrun/'platform').encode()).hexdigest()[:10]
        w=file_env(self.root/'.runtime'/(canary_name+'-worker.env'))
        old=file_env(self.old_worker['env_file'])
        self.old_model=old['MODEL_NAME']
        for name in ['CONTROL_PLANE_URL','CONTROL_PLANE_URLS']:
            if name in old:w[name]=old[name]
            else:w.pop(name,None)
        w.update(WORKER_ID='qwen480-prod-'+uuid.uuid4().hex[:12],WORKER_NAME='BatchForge 480B TP4',
                 WORKER_METRICS_PORT=str(self.old_worker['port']))
        self.worker_env=w
        worker_env_path=self.root/'.runtime'/('promoted480-'+hashlib.sha256(str(self.run).encode()).hexdigest()[:16]+'.env')
        require(not worker_env_path.exists(),'promotion_worker_config_already_exists')
        self.worker={'role':'worker','name':'qwen480','env_file':str(worker_env_path),
                     'python':self.old_worker['python'],'port':self.old_worker['port']}
        self.candidate={'services':[self.engine if s['role']=='engine' else self.worker if s['role']=='worker' else s
                                    for s in self.original['services']]}
        write_env(worker_env_path,w)
        candidate_path=self.root/'.runtime'/('promoted480-'+hashlib.sha256(str(self.run).encode()).hexdigest()[:16]+'.json')
        private_write(candidate_path,self.candidate);load_manifest(candidate_path,root=self.root)
        self.report={'phase':'prepared','model':REPO,'revision':REVISION,
            'qualification_sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'events':[],
            'cost':None,'limitations':['One existing Pod; no independent-node HA','No new cloud resources or pricing claim']}
        self.mutated=False;self.gpu_swapped=False;self.save()
        model=Path(e['ENGINE_MODEL_PATH'])
        require(model==self.root.parent/'gpu-takeover-20261025/model-spool','qualified_local_checkpoint_required')
        receipt=json.loads((model/'download-receipt.json').read_text())
        require(receipt['revision']==REVISION and len([v for v in receipt['verified']
                if v['file'].endswith('.safetensors')])==51,'pinned_51_shard_receipt_required')
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=4) as pool:
            size=sum(pool.map(lambda entry:verify_file(model,entry),receipt['verified']))
        self.report.update(checkpoint_reverified_sha256=True,checkpoint_verified_bytes=size);self.save()

    def save(self):write_json(self.run/'report.json',self.report)
    def event(self,name):
        self.report['events'].append({'at':datetime.now(timezone.utc).isoformat(),'event':name});self.save()

    def invoke(self,service,action):
        result,error=self.manager.invoke(service,action)
        require(error is None,'managed_'+action+'_failed')
        return result

    def wait(self,predicate,seconds,reason):
        until=time.monotonic()+seconds
        while not predicate():
            require(time.monotonic()<until,reason);time.sleep(1)

    def ready(self,service,seconds=60):
        def probe():
            state=self.invoke(service,'status')
            require(state['running'],'owned_service_exited_during_start')
            # Status also captures owned engine descendants during model load.
            return True if service.get('port') is None else self.manager.readiness(service)
        self.wait(probe,seconds,'service_readiness_timeout')

    def active(self,model=None):
        query="SELECT count(*) FROM tasks WHERE status IN ('pending','running')"
        return self.db.execute(query+(' AND model=%s' if model else ''),(model,) if model else ()).fetchone()[0]

    def apis_config(self,*,enabled,model=None,restore=False):
        for service in self.apis:
            env=dict(self.api_envs[service['name']])
            if not restore:
                env['BATCH_SUBMISSION_ENABLED']=str(enabled).lower()
                if model:env.update(MODEL_NAME=model,MODEL_NAMES_JSON=json.dumps([model]))
                if model==REPO:env['SCHEDULER_POLICY']='fair'
            self.write_env(Path(service['env_file']),env)
            self.invoke(service,'stop');self.invoke(service,'start');self.ready(service)

    def assert_admission_closed(self):
        for api in self.apis:
            response=self.http.post(f'http://127.0.0.1:{api["port"]}/v1/batches',
                json={'input_file_id':'file_nonexistent_admission_probe'})
            require(response.status_code==503 and response.json().get('error',{}).get('code')=='admission_paused',
                    'all_api_replicas_must_reject_admission_before_gpu_swap')

    def apply(self):
        for service in self.services:
            require(self.invoke(service,'status')['running'],'managed_identity_must_be_reconciled')
        self.mutated=True;self.invoke(self.watchdog,'stop')
        self.apis_config(enabled=False);self.assert_admission_closed()
        self.event('batch_admission_closed_on_all_replicas')
        self.wait(lambda:self.active()==0,1200,'global_queue_drain_timeout')
        self.gpu_swapped=True
        self.invoke(self.old_worker,'stop');self.invoke(self.old_engine,'stop')
        self.apis_config(enabled=False,model=REPO)
        engine_only={'services':[s for s in self.candidate['services'] if s['role']!='worker']}
        self.private_write(self.manifest,engine_only)
        self.wait(lambda:self.manager.start_gate(self.engine)[0],120,'fresh_gpu_memory_gate_timeout')
        self.invoke(self.engine,'start');self.ready(self.engine,1200)
        state=self.invoke(self.engine,'status')
        log=(self.root/'.runtime/engine-qwen480bench.log').read_text(errors='replace')
        marker=f'APIServer pid={state["pid"]}';require(marker in log,'fresh_cache_identity_missing')
        counts=re.findall(r'GPU KV cache size: ([0-9,]+) tokens',log[log.index(marker):])
        require(counts,'fresh_physical_kv_missing')
        physical=min(int(v.replace(',','')) for v in counts)
        budget=admission_budget(physical,32,True)
        self.worker_env.update(WORKER_TOKEN_BUDGET=str(budget),WORKER_ADAPTIVE_KV_ENABLED='true',WORKER_MAX_INFLIGHT='32')
        self.write_env(Path(self.worker['env_file']),self.worker_env)
        self.invoke(self.worker,'start');self.ready(self.worker)
        self.report.update(physical_kv_tokens=physical,admission_budget_tokens=budget)
        self.private_write(self.manifest,self.candidate)
        self.invoke(self.watchdog,'start');self.ready(self.watchdog)
        self.event('qualified_480b_worker_and_watchdog_ready_before_admission')
        self.apis_config(enabled=True,model=REPO)
        for service in self.apis:
            response=self.http.get(f'http://127.0.0.1:{service["port"]}/v1/models');response.raise_for_status()
            require({m['id'] for m in response.json()['data']}=={REPO},'primary_model_catalog_not_480b')
        self.ready(self.worker)
        self.report.update(phase='complete',finished_at=datetime.now(timezone.utc).isoformat())
        self.event('primary_480b_catalog_and_worker_activated')

    def rollback(self):
        # Newly accepted 480B work must drain before its GPU runtime is stopped.
        # If it cannot drain, retain that runtime/manifest and report the block.
        self.apis_config(enabled=False,model=REPO if self.gpu_swapped else None)
        self.assert_admission_closed()
        if self.gpu_swapped:
            self.wait(lambda:self.active(REPO)==0,1200,'new_480b_work_must_drain_before_rollback')
            self.invoke(self.watchdog,'stop')
            self.invoke(self.worker,'stop');self.invoke(self.engine,'stop')
            self.private_write(self.manifest,self.original)
            self.apis_config(enabled=False,model=self.old_model)
            if not self.invoke(self.old_engine,'status')['running']:
                self.wait(lambda:self.manager.start_gate(self.old_engine)[0],120,'rollback_gpu_memory_gate_timeout')
                self.invoke(self.old_engine,'start')
            self.ready(self.old_engine,1200);self.invoke(self.old_worker,'start');self.ready(self.old_worker)
        self.apis_config(enabled=True,restore=True)
        self.invoke(self.watchdog,'start');self.report['phase']='rolled_back';self.event('original_serving_pair_restored')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--qualification',type=Path,required=True);parser.add_argument('--run',type=Path,required=True)
    parser.add_argument('--apply',action='store_true');args=parser.parse_args()
    root=Path('/root/batchforge-runtime/app');os.umask(0o077)
    def interrupted(number,frame):raise RuntimeError('promotion_interrupted')
    signal.signal(signal.SIGTERM,interrupted);signal.signal(signal.SIGINT,interrupted)
    with (root/'.runtime/model-trial.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        roll=Promotion(root,args.run,args.qualification)
        try:
            if args.apply:roll.apply()
        except BaseException as error:
            signal.signal(signal.SIGTERM,signal.SIG_IGN);signal.signal(signal.SIGINT,signal.SIG_IGN)
            roll.report['failure_type']=type(error).__name__
            if type(error) is RuntimeError:roll.report['failure_code']=str(error)
            if roll.mutated:
                try:roll.rollback()
                except BaseException as rollback:
                    roll.report.update(phase='rollback_blocked',rollback_failure_type=type(rollback).__name__)
                    if type(rollback) is RuntimeError:roll.report['rollback_failure_code']=str(rollback)
            roll.save();raise RuntimeError('promotion_failed_inspect_private_report') from None
        finally:roll.db.close();roll.http.close()


if __name__=='__main__':main()
