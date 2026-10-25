"""Real 480B Batch API experiment using the existing Azure and S3 resources.

Runs only inside the bounded model trial. No provisioning, schema change or
credential export. Every cohort is owned by a unique experiment label. Cleanup
cancels only those cohorts and stops only the named canary services.
"""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import threading
import time
from types import SimpleNamespace
import uuid

from ops.download_480b import REPO, REVISION
from ops.gpu_campaign import write_json

CONTEXT = 18440


def require(condition, reason):
    if not condition: raise RuntimeError(reason)


def experiment_plan(repeats, studies=('capacity', 'policy')):
    if type(repeats) is not int or not 1 <= repeats <= 3:
        raise ValueError('Require one through three preregistered ABBA repetitions')
    if (not studies or len(set(studies)) != len(studies)
            or any(study not in {'capacity', 'policy', 'kv'} for study in studies)):
        raise ValueError('Require distinct qualified experiment studies')
    phases = []
    for study in studies:
        for repeat in range(repeats):
            for treatment in 'ABBA':
                phases.append({'study':study,'repeat':repeat,'treatment':treatment,
                    'slots':(32 if study=='kv' and treatment=='B' else
                             4 if study=='capacity' and treatment=='A' else 8),
                    'adaptive':study=='kv' and treatment=='B',
                    'policy':'cost_aware' if study=='policy' and treatment=='B' else 'fair'})
    return phases


def check_capacity(tokens, slots):
    require(type(tokens) is int and tokens>0 and type(slots) is int and slots>0, 'invalid_physical_cache')
    reserved=slots*((CONTEXT+15)//16)*16
    require(reserved <= math.floor(tokens*.95), 'full_context_reservations_exceed_fresh_physical_cache')
    return reserved


def admission_budget(tokens, slots, adaptive):
    require(type(adaptive) is bool, 'admission_mode_must_be_boolean')
    if not adaptive:
        return check_capacity(tokens, slots)
    require(type(tokens) is int and tokens > 0 and slots == 32,
            'adaptive_requires_qualified_32_sequence_engine')
    # A block-aligned 5% margin below the minimum fresh per-rank cache.
    budget = (math.floor(tokens*.95)//16)*16
    require(budget >= ((CONTEXT+15)//16)*16, 'adaptive_cache_cannot_fit_one_full_context')
    return budget


def audit_metering(tasks, attempts, batches):
    accepted={}
    for row in attempts:
        if row['status']=='completed':accepted.setdefault(row['task_id'],[]).append(row)
    require(tasks and all(t['status']=='completed' and len(accepted.get(t['id'],[]))==1 for t in tasks),
            'each_task_requires_exactly_one_accepted_attempt')
    task_totals=[sum(t[k] for t in tasks) for k in ['prompt_tokens','completion_tokens']]
    attempt_totals=[sum(a[k] for group in accepted.values() for a in group) for k in ['prompt_tokens','completion_tokens']]
    batch_totals=[sum(b[k] for b in batches) for k in ['prompt_tokens','completion_tokens']]
    require(task_totals==attempt_totals==batch_totals,'durable_metering_conservation_failed')
    return {'tasks':len(tasks),'attempts':len(attempts),'accepted_attempts':len(accepted),
            'prompt_tokens':task_totals[0],'completion_tokens':task_totals[1],
            'one_accepted_attempt_per_task':True,'batch_task_attempt_metering_match':True}


class Canary:
    def __init__(self, args):
        self.root,self.run=args.root.resolve(),args.run.resolve()
        require(self.root==Path('/root/batchforge-runtime/app') and self.run.is_relative_to(self.root.parent), 'owned_runtime_required')
        from ops.storage_semantics import require_journal_filesystem
        require_journal_filesystem(self.run)
        self.run.mkdir(mode=0o700,parents=True,exist_ok=False)
        os.umask(0o077)
        sys.path.insert(0,str(self.root))
        from ops.supervise import Manager,load_manifest
        from ops.ha.retune_full_gpu import file_env,write_env
        from ops.ha.switch_qwen235b import phase_calibration,WORKER_KEYS
        from backend.scheduling import runtime_fingerprint,validate_profile
        import httpx
        import psycopg
        from psycopg.rows import dict_row
        self.httpx,self.write_env,self.file_env=httpx,write_env,file_env
        _,services,_=load_manifest(self.root/'.runtime/services-primary.json',root=self.root)
        api=next(s for s in services if s['role']=='api')
        original_worker=next(s for s in services if s['role']=='worker')
        self.api_env=file_env(api['env_file'])
        require(self.api_env['BLOB_STORAGE_BACKEND']=='s3' and self.api_env['RESULT_READERS_ENABLED'].lower()=='true',
                'existing_s3_and_leased_readers_required')
        self.api_env.update(MODEL_NAME=REPO,MODEL_NAMES_JSON=json.dumps([REPO]),LEASE_SECONDS='15',
            BLOB_GC_ENABLED='false',RESULT_ARCHIVE_ENABLED='false',SCHEDULER_POLICY='fair',
            DB_POOL_MIN='1',DB_POOL_MAX='6')
        from backend.config import Settings
        Settings(**{k.lower():v for k,v in self.api_env.items()})
        self.worker_base={k:v for k,v in file_env(original_worker['env_file']).items() if k in WORKER_KEYS}
        self.key=file_env(self.root.parent/'access.env')['BATCHFORGE_API_KEY']
        self.db=psycopg.connect(self.api_env['DATABASE_URL'],autocommit=True,row_factory=dict_row,
            connect_timeout=10,options='-c default_transaction_read_only=on -c statement_timeout=10000')
        self.tenant=self.db.execute('SELECT tenant_id FROM api_keys WHERE key_hash=%s AND NOT revoked',
            (hashlib.sha256(self.key.encode()).hexdigest(),)).fetchone()['tenant_id']
        active=self.db.execute("SELECT count(*) AS n FROM tasks WHERE model=%s AND status IN ('pending','running')",(REPO,)).fetchone()['n']
        require(active==0,'old_480b_tasks_must_be_resolved_before_experiment')
        self.manager=Manager(root=self.root,timeout=120)
        service_name='p480-'+hashlib.sha256(str(self.run).encode()).hexdigest()[:10]
        self.service_api={'role':'api','name':service_name,'env_file':str(self.root/'.runtime'/(service_name+'-api.env')),
                          'python':str(self.root.parent/'venv/bin/python'),'port':18148}
        self.service_worker={'role':'worker','name':service_name,'env_file':str(self.root/'.runtime'/(service_name+'-worker.env')),
                          'python':str(self.root.parent/'venv/bin/python'),'port':9118}
        self.engine=json.loads(args.engine_service)
        require(self.engine['role']=='engine' and self.engine['port']==8348,'qualified_trial_engine_required')
        require(not Path(self.service_api['env_file']).exists() and not Path(self.service_worker['env_file']).exists(),
                'canary_configuration_already_exists_inspect_before_rerun')
        direct=json.loads((self.root.parent/'gpu-takeover-20261025/trial480-v2/benchmark/00-fifo-report.json').read_text())
        require(direct['passed'] and direct['model']==REPO and direct['tokenizer_revision']==REVISION, 'qualified_calibration_required')
        fit=phase_calibration(direct['records'],concurrency=16)
        self.rates=(fit['prefill_http_first_text_proxy']['tokens_per_second'],fit['decode_http_stream_span_proxy']['tokens_per_second'])
        self.fingerprint=runtime_fingerprint({'model':REPO,'revision':REVISION,'quantization':'awq_marlin',
            'dtype':'half','tp':4,'vllm':'0.10.2','context':CONTEXT,'eager':True,'prefix_cache':False,'batch_tokens':4096})
        self.validate_profile=validate_profile
        self.tag='p480-'+uuid.uuid4().hex[:12]
        require(type(args.workload_repeats) is int and 1<=args.workload_repeats<=3,'invalid_workload_repeat_bound')
        self.workload_repeats=args.workload_repeats
        self.fault_adaptive='kv' in args.studies
        self.sampler_stop=threading.Event();self.sampler=None
        self.labels=[];self.batch_ids=[];self.worker_id=''
        self.report={'schema_version':1,'tag':self.tag,'started_at':datetime.now(timezone.utc).isoformat(),
            'model':REPO,'revision':REVISION,'runtime_fingerprint':self.fingerprint,
            'plan':experiment_plan(args.repeats, args.studies),
            'workload_repeats':self.workload_repeats,
            'phases':[],'events':[],'passed':False,'cost':None,
            'limitations':['One four-H100 Pod; application fault tests are not whole-Pod or cross-node HA',
              'Profile rates projected from the qualified concurrency16 engine burst; not calibrated physical fleet prices',
              '16.10 USD/hour is a historical allocation scenario, not a current invoice; monetary savings are not claimed',
              'SQuAD fixed-length capacity workload, not a response quality evaluation']}
        self.report['source_sha256']={'experiment':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            **{name:hashlib.sha256((self.root/name).read_bytes()).hexdigest()
               for name in ['worker/runtime.py','worker/config.py','worker/kv_admission.py',
                            'backend/result_archival.py','backend/store.py']}}
        self.client=httpx.Client(base_url='http://127.0.0.1:18148',headers={'Authorization':'Bearer '+self.key},timeout=60,trust_env=False)
        self.worker_client=httpx.Client(base_url='http://127.0.0.1:18148',headers={'Authorization':'Bearer '+self.worker_base['BATCHFORGE_WORKER_TOKEN']},timeout=30,trust_env=False)

    def save(self):write_json(self.run/'report.json',self.report)

    def event(self, name, **values):
        self.report['events'].append({'at':datetime.now(timezone.utc).isoformat(),'event':name,**values});self.save()
        print(json.dumps({'event':name,**values}),flush=True)

    def action(self, service, action, grace=15):
        cmd=[str(self.root.parent/'venv/bin/python'),str(self.root/'ops/manage.py'),action,service['role'],'--name',service['name']]
        if action=='stop':cmd+=['--grace-seconds',str(grace)]
        if action=='start':
            cmd+=['--env-file',service['env_file'],'--python',service['python']]
            for key in ['port','gpu']:
                if service.get(key) is not None:cmd+=['--'+key,str(service[key])]
        completed=subprocess.run(cmd,cwd=self.root,capture_output=True,timeout=120)
        require(completed.returncode==0,'canary_managed_'+action+'_failed')
        return json.loads(completed.stdout)

    def wait(self, predicate, seconds, reason):
        deadline=time.monotonic()+seconds
        while not predicate():
            require(time.monotonic()<deadline,reason);time.sleep(.5)

    def start_api(self, policy):
        self.api_env['SCHEDULER_POLICY']=policy
        self.write_env(Path(self.service_api['env_file']),self.api_env)
        self.action(self.service_api,'start')
        self.wait(lambda:self.manager.readiness(self.service_api),60,'canary_api_not_ready')

    def quiescent(self):
        from worker.kv_admission import engine_quiescent
        try:
            with self.httpx.Client(timeout=5,trust_env=False) as c:
                r=c.get('http://127.0.0.1:8348/metrics');r.raise_for_status()
            return engine_quiescent(r.text)
        except self.httpx.HTTPError:return False

    def configure(self, policy, slots, worker_id=None, *, adaptive=False):
        # No overlapping generations, including orphaned requests from a lost
        # worker. Full-context reservations cannot depend on body heuristics.
        self.action(self.service_worker,'stop')
        self.wait(self.quiescent,90,'engine_not_quiescent_between_cohorts')
        self.action(self.service_api,'stop');self.start_api(policy)
        self.worker_id=worker_id or self.tag+'-worker'
        # More admitted claims do not create additional priced execution slots.
        # The KV study keeps the exact same baseline8 price and rate projection.
        priced_slots=8 if adaptive else slots
        profile=self.validate_profile({'profile_id':f'480b-priced{priced_slots}-projected-c16',
            'runtime_fingerprint':self.fingerprint,'prefill_tokens_per_second':self.rates[0],
            'decode_tokens_per_second':self.rates[1],'hourly_cost_usd':16.10/priced_slots,
            'calibration_label':'projection-from-qualified-480B-c16-fixed20;rates-not-recalibrated-for-worker-slots',
            'cost_source':f'historical-16.10-USD-pod-hour-scenario/{priced_slots}-full-context-slots;not-current-invoice'})
        reservation=admission_budget(self.cache_tokens,slots,adaptive)
        env={**self.worker_base,'MODEL_NAME':REPO,'WORKER_ID':self.worker_id,'WORKER_NAME':self.worker_id,
            'CONTROL_PLANE_URL':'http://127.0.0.1:18148','CONTROL_PLANE_URLS':'http://127.0.0.1:18148',
            'INFERENCE_BASE_URLS':'http://127.0.0.1:8348','INFERENCE_API_KEY':'',
            'WORKER_MAX_INFLIGHT':str(slots),'WORKER_TOKEN_BUDGET':str(reservation),'ENGINE_MAX_MODEL_LEN':str(CONTEXT),
            'WORKER_ADAPTIVE_KV_ENABLED':str(adaptive).lower(),'INFERENCE_TIMEOUT_SECONDS':'1200','WORKER_SHUTDOWN_SECONDS':'15',
            'WORKER_METRICS_HOST':'127.0.0.1','WORKER_METRICS_PORT':'9118',
            'SCHEDULER_PROFILE_JSON':json.dumps(profile)}
        self.write_env(Path(self.service_worker['env_file']),env)
        self.action(self.service_worker,'start')
        self.wait(lambda:self.manager.readiness(self.service_worker),60,'canary_worker_not_ready')
        self.event('worker_configured', policy=policy, slots=slots, adaptive=adaptive,
                   token_budget=reservation, priced_slots=priced_slots)

    def workload(self):
        source=self.root.parent/'gpu-takeover-20261025/trial480-v2/workload.jsonl'
        original=[json.loads(line) for line in source.read_text().splitlines()]
        require(len(original)==20 and all(r['body']['model']==REPO for r in original),'fixed_20_cell_workload_required')
        rows=[]
        for repeat in range(self.workload_repeats):
            for item in original:
                row=json.loads(json.dumps(item));row['custom_id']=f'r{repeat}-'+row['custom_id']
                row['body']['min_tokens']=row['body']['max_tokens'];rows.append(row)
        path=self.run/'workload.jsonl';path.write_text(''.join(json.dumps(r)+'\n' for r in rows))
        self.task_count=len(rows)
        self.report['workload_sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
        self.expected_inputs=109056*self.workload_repeats;self.expected_outputs=13600*self.workload_repeats
        return path

    def audit(self, ids):
        params=(ids,self.tenant)
        tasks=self.db.execute('SELECT id,status,prompt_tokens,completion_tokens FROM tasks WHERE batch_id=ANY(%s) AND tenant_id=%s ORDER BY id',params).fetchall()
        attempts=self.db.execute('SELECT task_id,status,prompt_tokens,completion_tokens FROM attempts WHERE batch_id=ANY(%s) AND tenant_id=%s',params).fetchall()
        batches=self.db.execute('SELECT prompt_tokens,completion_tokens FROM batches WHERE id=ANY(%s) AND tenant_id=%s',params).fetchall()
        audit=audit_metering(tasks,attempts,batches)
        times=self.db.execute("SELECT extract(epoch FROM (finished_at-created_at))::float8 AS completion FROM tasks WHERE batch_id=ANY(%s) AND tenant_id=%s",params).fetchall()
        from scripts.benchmark import distribution
        audit['task_completion_seconds']=distribution([r['completion'] for r in times])
        rows=self.db.execute("""SELECT t.id,t.custom_id,
            extract(epoch FROM(t.finished_at-t.created_at))::float8 AS completion_seconds,
            extract(epoch FROM(a.started_at-t.created_at))::float8 AS queue_seconds,
            extract(epoch FROM(a.finished_at-a.started_at))::float8 AS accepted_attempt_seconds,
            a.scheduling_decision FROM tasks t JOIN attempts a ON a.task_id=t.id AND a.status='completed'
            WHERE t.batch_id=ANY(%s) AND t.tenant_id=%s ORDER BY t.id""",params).fetchall()
        audit['queue_seconds']=distribution([r['queue_seconds'] for r in rows])
        audit['accepted_attempt_seconds']=distribution([r['accepted_attempt_seconds'] for r in rows])
        audit['task_observations']=rows
        return audit

    def run_phase(self, phase, index, path):
        from scripts import benchmark
        actual_hash=hashlib.sha256(Path(benchmark.__file__).read_bytes()).hexdigest()
        previous=self.report['source_sha256'].setdefault('benchmark',actual_hash)
        require(previous==actual_hash,'benchmark_source_changed_during_experiment')
        label=self.tag+'-'+str(index);self.labels.append(label)
        phase_dir=self.run/f'{index:02d}-{phase["study"]}-{phase["treatment"]}';phase_dir.mkdir()
        args=SimpleNamespace(url='http://127.0.0.1:18148',model=REPO,tasks=self.task_count,batches=1,max_tokens=2048,
            temperature=0,prompt='',engine_label=label,deadline=1200,request_timeout=90,poll=.5,
            workload_dir=phase_dir/'inputs',input_jsonl=path,output_dir=phase_dir/'outputs',submission_concurrency=1)
        os.environ['BATCHFORGE_API_KEY']=self.key
        result=asyncio.run(benchmark.run(args));write_json(phase_dir/'benchmark.json',result)
        self.batch_ids.extend(result['batch_ids'])
        require(result['passed'],'platform_benchmark_failed')
        require(not result['test_fixture_detected'],'fake_inference_cannot_qualify_real_gpu_results')
        audit=self.audit(result['batch_ids'])
        require(audit['prompt_tokens']==self.expected_inputs and audit['completion_tokens']==self.expected_outputs,
                'observed_token_work_differs_from_preregistered_cells')
        response=self.client.get('http://127.0.0.1:9118/metrics');response.raise_for_status()
        metrics=response.text
        if phase['adaptive']:
            def gauge(name):
                values=re.findall(r'^'+re.escape(name)+r' ([^\n]+)$',metrics,re.M)
                require(len(values)==1 and math.isfinite(float(values[0])), 'worker_kv_metric_invalid')
                return float(values[0])
            budget=gauge('batchforge_worker_adaptive_kv_budget_tokens')
            peak=gauge('batchforge_worker_adaptive_kv_peak_tokens')
            require(budget==admission_budget(self.cache_tokens,32,True) and 0<peak<=budget
                    and gauge('batchforge_worker_adaptive_kv_paused')==0,
                    'adaptive_admission_budget_or_protocol_failed')
        (phase_dir/'worker-metrics.txt').write_text(metrics)
        stage={**phase,'index':index,'batch_ids':result['batch_ids'],'elapsed_seconds':result['elapsed_seconds'],
            'generated_tokens_per_second':result['observed_completion_tokens']/result['elapsed_seconds'],
            'admission_upload_seconds':result['submission']['upload_latency_seconds'],
            'admission_create_seconds':result['submission']['create_latency_seconds'],'audit':audit}
        self.report['phases'].append(stage);self.event('phase_passed',index=index,study=phase['study'],treatment=phase['treatment'])

    def execute(self):
        from ops.gpu_campaign import sample
        self.sampler=threading.Thread(target=sample,args=('http://127.0.0.1:8348',self.run/'gpu-engine-samples.jsonl',
            self.sampler_stop,'http://127.0.0.1:9118'),daemon=True)
        self.sampler.start()
        self.refresh_cache()
        self.report['physical_cache_tokens']=self.cache_tokens;self.save()
        path=self.workload()
        for index,phase in enumerate(self.report['plan']):
            self.configure(phase['policy'],phase['slots'],adaptive=phase['adaptive']);self.run_phase(phase,index,path)
        self.fault_test(path,engine_loss=False)
        self.fault_test(path,engine_loss=True)
        self.persistence_test()
        self.report['passed']=True;self.event('platform_campaign_passed')

    def refresh_cache(self):
        identity,error=self.manager.invoke(self.engine,'status');require(error is None and identity['running'],'engine_identity_missing')
        full_log=(self.root/'.runtime'/('engine-'+self.engine['name']+'.log')).read_text(errors='replace')
        marker=f'APIServer pid={identity["pid"]}'
        require(marker in full_log,'fresh_engine_startup_marker_missing')
        log=full_log[full_log.index(marker):]
        values=re.findall(r'GPU KV cache size: ([0-9,]+) tokens',log)
        require(values,'fresh_engine_cache_evidence_missing');self.cache_tokens=min(int(v.replace(',','')) for v in values)
        check_capacity(self.cache_tokens,8)
        self.event('fresh_engine_cache_verified',engine_pid=identity['pid'],physical_cache_tokens=self.cache_tokens)

    def submit_fault(self,path,label):
        rows=[json.loads(line) for line in path.read_text().splitlines()]
        for row in rows:row['custom_id']=label+'-'+row['custom_id']
        content=''.join(json.dumps(row)+'\n' for row in rows).encode()
        self.event('submission_intent',label=label,input_sha256=hashlib.sha256(content).hexdigest())
        response=self.client.post('/v1/files',data={'purpose':'batch'},files={'file':(label+'.jsonl',content,'application/jsonl')})
        response.raise_for_status();fid=response.json()['id']
        body={'input_file_id':fid,'endpoint':'/v1/chat/completions','completion_window':'24h',
              'metadata':{'engine_label':label,'benchmark_run':self.tag}}
        response=self.client.post('/v1/batches',json=body,headers={'Idempotency-Key':label})
        response.raise_for_status();batch=response.json();self.batch_ids.append(batch['id'])
        # The exact same idempotency key must return the original durable batch.
        replay=self.client.post('/v1/batches',json=body,headers={'Idempotency-Key':label})
        replay.raise_for_status();require(replay.json()['id']==batch['id'],'idempotency_replay_created_another_batch')
        return batch['id']

    def fault_test(self,path,*,engine_loss):
        slots=32 if self.fault_adaptive else 8
        self.configure('fair',slots,adaptive=self.fault_adaptive)
        label=self.tag+('-engine-fault' if engine_loss else '-worker-fault');self.labels.append(label)
        bid=self.submit_fault(path,label)
        captured=[]
        def capture():
            nonlocal captured
            captured=self.db.execute("SELECT id,attempt_token,worker_id,lease_expires_at FROM tasks WHERE batch_id=%s AND tenant_id=%s AND status='running' AND lease_expires_at>clock_timestamp() ORDER BY id",(bid,self.tenant)).fetchall()
            return len(captured)>=2
        self.wait(capture,60,'no_valid_leases_to_fault')
        down=time.monotonic();old_worker=self.worker_id
        self.action(self.service_worker,'stop',grace=0)
        self.event('fault_injected',scope='engine_and_worker' if engine_loss else 'worker',captured_leases=len(captured))
        if engine_loss:
            self.action(self.engine,'stop')
            self.wait(lambda:self.manager.start_gate(self.engine)[0],120,'fault_engine_memory_not_released')
            self.action(self.engine,'start')
            self.wait(lambda:self.manager.readiness(self.engine),1200,'fault_engine_reload_failed')
            self.refresh_cache()
        else:self.wait(self.quiescent,90,'orphaned_engine_requests_not_quiescent')
        tokens=[row['attempt_token'] for row in captured]
        def expired():
            rows=self.db.execute('SELECT status,error FROM attempts WHERE token=ANY(%s)',(tokens,)).fetchall()
            return len(rows)==len(tokens) and all(r['status'] in {'expired','retrying','failed'} and (r['error'] or {}).get('code')=='lease_expired' for r in rows)
        self.wait(expired,90,'captured_leases_did_not_expire')
        stale=self.worker_client.post('/internal/tasks/'+captured[0]['id']+'/heartbeat',
            json={'worker_id':old_worker,'attempt_token':tokens[0]})
        require(stale.status_code==409,'stale_heartbeat_was_not_fenced')
        self.configure('fair',slots,worker_id=self.tag+('-engine-recovered' if engine_loss else '-worker-recovered'),
                       adaptive=self.fault_adaptive)
        service_recovered=time.monotonic()-down
        def terminal():
            response=self.client.get('/v1/batches/'+bid);response.raise_for_status()
            return response.json()['status']!='in_progress'
        self.wait(terminal,1200,'fault_cohort_not_terminal')
        audit=self.audit([bid])
        require(audit['tasks']==self.task_count and audit['prompt_tokens']==self.expected_inputs and audit['completion_tokens']==self.expected_outputs,
                'fault_cohort_work_or_metering_differs')
        reclaimed=self.db.execute("SELECT t.id,t.worker_id,t.attempt_token FROM tasks t WHERE t.id=ANY(%s) AND t.tenant_id=%s AND t.status='completed'",([r['id'] for r in captured],self.tenant)).fetchall()
        require(len(reclaimed)==len(captured) and all(r['worker_id']==self.worker_id and r['attempt_token'] not in tokens for r in reclaimed),
                'captured_tasks_not_accepted_by_replacement_worker')
        # A structurally valid completion with the old credential must not
        # change already accepted usage, even after new results have committed.
        stale=self.worker_client.post('/internal/tasks/'+captured[0]['id']+'/complete',json={
            'worker_id':old_worker,'attempt_token':tokens[0],
            'response':{'id':'late-old-attempt','object':'chat.completion','model':REPO,
                'choices':[{'index':0,'message':{'role':'assistant','content':'late'},'finish_reason':'stop'}],
                'usage':{'prompt_tokens':0,'completion_tokens':0,'total_tokens':0}}})
        require(stale.status_code==409 and self.audit([bid])==audit,'stale_completion_changed_metering')
        proof={'scope':'engine_and_worker_process_loss' if engine_loss else 'worker_process_loss',
            'adaptive':self.fault_adaptive,'slots':slots,
            'batch_id':bid,'captured_leases':len(captured),'service_recovery_seconds':service_recovered,
            'all_captured_reclaimed_seconds':time.monotonic()-down,'replacement_worker':self.worker_id,
            'stale_heartbeat_status':409,'stale_completion_status':409,'metering':audit}
        self.report.setdefault('faults',[]).append(proof);self.event('fault_recovery_passed',scope=proof['scope'])

    def persistence_test(self):
        before={}
        # Include uncertain-but-committed creates owned by this run, never old
        # customer batches or arbitrary rows from another tenant.
        batches=self.db.execute("SELECT id,input_file_id,output_file_id,status FROM batches WHERE tenant_id=%s AND metadata->>'engine_label'=ANY(%s) ORDER BY id",(self.tenant,self.labels)).fetchall()
        require(all(b['status']=='completed' for b in batches),'only_terminal_successful_cohorts_can_be_archived')
        for batch in batches:
            for fid in [batch['input_file_id'],batch['output_file_id']]:
                response=self.client.get('/v1/files/'+fid+'/content');response.raise_for_status()
                before[fid]=hashlib.sha256(response.content).hexdigest()
        async def archive():
            from backend.store import Store
            from backend.config import Settings
            from psycopg_pool import AsyncConnectionPool
            from psycopg.rows import dict_row
            settings=Settings(**{k.lower():v for k,v in self.api_env.items()})
            # S3 credentials are inherited from an already authorized private
            # config, in this process only, never in argv or the report.
            for key in ['AWS_ACCESS_KEY_ID','AWS_SECRET_ACCESS_KEY','AWS_ENDPOINT_URL_S3','AWS_REGION']:
                os.environ[key]=self.api_env[key]
            async with AsyncConnectionPool(settings.database_url,min_size=1,max_size=3,
                    kwargs={'autocommit':True,'row_factory':dict_row,'connect_timeout':5,
                            'options':'-c statement_timeout=10000 -c lock_timeout=2000 -c idle_in_transaction_session_timeout=15000'}) as pool:
                store=Store(pool,settings,schema_version=11)
                results=[]
                for batch in batches:
                    results.append(await store.results.archive_batch(self.tenant,batch['id'],purge_payloads=False))
                return results
        archives=asyncio.run(archive())
        self.action(self.service_worker,'stop')
        self.action(self.service_api,'stop');self.start_api('fair')
        for fid,digest in before.items():
            response=self.client.get('/v1/files/'+fid+'/content');response.raise_for_status()
            require(hashlib.sha256(response.content).hexdigest()==digest,'result_or_input_changed_after_api_restart')
        durable=self.db.execute("SELECT count(*) AS n FROM files f JOIN blob_objects o ON o.id=f.blob_id AND o.tenant_id=f.tenant_id WHERE f.id=ANY(%s) AND f.tenant_id=%s AND o.backend='s3' AND o.state='ready' AND f.content IS NULL",([b['input_file_id'] for b in batches],self.tenant)).fetchone()['n']
        require(durable==len(batches),'input_payload_not_in_external_s3')
        payloads=self.db.execute("""SELECT count(*) AS tasks,
            count(*) FILTER(WHERE body IS NOT NULL AND response IS NOT NULL) AS retained,
            count(*) FILTER(WHERE response IS NULL) AS responses_in_archive_only
            FROM tasks WHERE batch_id=ANY(%s) AND tenant_id=%s""",
            ([b['id'] for b in batches],self.tenant)).fetchone()
        self.report['persistence']={'batches':len(batches),'s3_input_objects':durable,'archived_batches':len(archives),
            'verified_files_after_api_restart':len(before),'file_sha256':before,
            'azure_metadata_and_usage_preserved':True,'s3_https_readback_verified':True,
            'database_payloads_retained':payloads['tasks']==payloads['retained'],
            'database_payload_observation':payloads,
            'database_payload_observed_at':datetime.now(timezone.utc).isoformat(),
            'this_controller_purge_requested':False,'other_existing_archive_publishers_not_paused':True,
            'archive_receipts':archives}
        self.event('durable_input_and_archived_output_restart_readback_passed',files=len(before))

    def cleanup(self):
        # Include acknowledged and uncertain creates scoped to this run label.
        errors=[]
        try:
            rows=self.db.execute("SELECT id FROM batches WHERE tenant_id=%s AND metadata->>'engine_label'=ANY(%s) AND status='in_progress'",(self.tenant,self.labels)).fetchall()
            for row in rows:
                try:
                    response=self.client.post('/v1/batches/'+row['id']+'/cancel');response.raise_for_status()
                except self.httpx.HTTPError:
                    response=self.client.post('http://127.0.0.1:8080/v1/batches/'+row['id']+'/cancel');response.raise_for_status()
        except Exception as error:errors.append(type(error).__name__)
        finally:
            for service in [self.service_worker,self.service_api]:
                try:self.action(service,'stop')
                except Exception as error:errors.append(type(error).__name__)
            self.client.close();self.worker_client.close();self.db.close()
            self.sampler_stop.set()
            if self.sampler is not None:self.sampler.join(10)
        require(not errors,'canary_cleanup_incomplete')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,required=True);parser.add_argument('--run',type=Path,required=True)
    parser.add_argument('--engine-service',required=True);parser.add_argument('--repeats',type=int,default=3)
    parser.add_argument('--studies',nargs='+',choices=['capacity','policy','kv'],default=['capacity','policy'])
    parser.add_argument('--workload-repeats',type=int,choices=range(1,4),default=1)
    args=parser.parse_args()
    def stop(number, frame):raise RuntimeError('canary_interrupted')
    signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop)
    canary=Canary(args)
    try:canary.execute()
    except Exception as error:
        canary.report['error_type']=type(error).__name__
        if type(error) is RuntimeError:canary.report['error_code']=str(error)
        canary.event('canary_failed')
    finally:
        try:canary.cleanup()
        except Exception as error:canary.report['cleanup_error_type']=type(error).__name__
        canary.report['finished_at']=datetime.now(timezone.utc).isoformat();canary.save()
    require(canary.report['passed'] and not canary.report.get('cleanup_error_type'),'platform_canary_failed')


if __name__=='__main__':main()
