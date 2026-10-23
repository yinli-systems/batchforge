"""Released final bounded TP4 seq8 retune after all seq4 policy matrices complete."""
import asyncio
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

import httpx

APP=Path('/root/batchforge-runtime/app')
sys.path[:0]=[str(APP),str(APP/'.runtime')]
from retune_primary16 import manager,write_env,ready
from ops.manage import owned_record
from ops.supervise import private_write
from primary_json_eos_probe import now

WORK=APP/'.runtime/primary-gptoss120-seq8-tune'
MANIFEST=APP/'.runtime/services-primary.json'
REPORT=APP/'.runtime/primary-gptoss120-seq8-report.json'
ENGINE_PY='/root/batchforge-runtime/engine-venv/bin/python'

def persist(report):
    private_write(REPORT,report)

def install_and_adopt(report):
    stage=APP/'.runtime/staged-operators-v4'
    names=('ops/engine_members.py','ops/host_memory.py','ops/llama_backend.py','ops/manage.py','ops/supervise.py')
    report['operator_archive_sha256']='a2dec12c6d1a3ecebccaa7602213f37829fd7e906211aa996f4db499cf98d86e'
    expected={'ops/manage.py':'cd15563e36f8a0a269378aff74e1b7bc42e575cb7a9062a73d28519bca3bcd85',
              'ops/supervise.py':'c349a9760573fcc6ced56566d59edfb73d9c3801720d81cd46e67fcccae1c386',
              'ops/engine_members.py':'e4d0b2f2c130dce6fa16a4925d3557ca16475793a5b4f156f62a3d8f2df471ff',
              'ops/host_memory.py':'e4887b81fd3c95e7f94bd118d0917775d5e8839c7c59e07b7dd8d98552150902',
              'ops/llama_backend.py':'d27fce11beb565b287d19bce61aa9c3edbf5cb802fd3630fa4788e7aeb31adb4'}
    assert hashlib.sha256((APP/'.runtime/batchforge-managed-engines-v4.tar').read_bytes()).hexdigest()==report['operator_archive_sha256']
    assert {n:hashlib.sha256((stage/n).read_bytes()).hexdigest() for n in names}==expected
    report['operator_files']={}
    for name in names:
        source=stage/name;target=APP/name
        previous=WORK/'operators-original'/name
        previous.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
        if target.exists():
            previous.write_bytes(target.read_bytes());previous.chmod(0o600)
        data=source.read_bytes()
        temporary=target.with_suffix('.new')
        temporary.write_bytes(data);temporary.chmod(0o600);temporary.replace(target)
        report['operator_files'][name]=hashlib.sha256(data).hexdigest()
    # No engine restart: adopt only the live, verified original leader and
    # kernel-descendant identities, while the watchdog is paused.
    from ops import engine_members
    pidfile=APP/'.runtime/engine-gptoss120.json'
    record=owned_record(pidfile)
    assert record['pid']==json.loads((APP/'.runtime/primary-gptoss120-seq4-report.json').read_text())['engine_start']['pid']
    current=engine_members.identity(record['pid'])
    assert engine_members.matches_leader(current,record)
    assert current['pgid']==current['session']==record['pid']
    snapshot=pidfile.with_suffix('.members.json')
    assert engine_members.capture(snapshot,record)
    captured=engine_members.read(snapshot)
    required={144661,144662,144663,144664}
    assert required <= {m['pid'] for m in captured['members']}
    assert all(engine_members.identity(m['pid'])==m for m in captured['members'])
    private_write(WORK/'engine-record-before-adoption.json',record)
    record['track_members']=True
    engine_members.save(pidfile,record)
    report['live_adoption']={'leader':captured['leader'],'members':captured['members'],
                             'four_original_rank_pids_captured':True,'engine_restarted':False}

def memory_gate(fraction=.26, margin_bytes=1024**3):
    code='import torch,json; print(json.dumps([dict(rank=i,free_bytes=f,total_bytes=t) for i in range(torch.cuda.device_count()) for f,t in [torch.cuda.mem_get_info(i)]]))'
    p=subprocess.run([ENGINE_PY,'-c',code],cwd=APP,
        env={**os.environ,'CUDA_VISIBLE_DEVICES':'0,1,2,3'},capture_output=True,text=True,timeout=30,check=True)
    rows=json.loads(p.stdout)
    assert len(rows)==4 and {r['rank'] for r in rows}=={0,1,2,3}
    for row in rows:
        row['required_free_bytes']=row['total_bytes']*fraction+margin_bytes
        row['predicted_headroom_bytes']=row['free_bytes']-row['total_bytes']*fraction
        row['passed']=row['free_bytes']>=row['required_free_bytes']
    return rows

async def engine_ready(expected_pid, seconds=360):
    began=time.monotonic();last=-20
    async with httpx.AsyncClient(trust_env=False,timeout=2) as client:
        while time.monotonic()-began<seconds:
            record=owned_record(APP/'.runtime/engine-gptoss120.json')
            assert record and record['pid']==expected_pid
            try:
                if (await client.get('http://127.0.0.1:8300/health')).status_code==200:return
            except httpx.HTTPError:pass
            elapsed=time.monotonic()-began
            if elapsed-last>=15:
                print(json.dumps({'phase':'engine_wait','pid':expected_pid,'elapsed_seconds':round(elapsed,1)}),flush=True)
                last=elapsed
            await asyncio.sleep(1)
    raise RuntimeError('engine_readiness_deadline')

async def stop_engine_bounded():
    deadline=time.monotonic()+120;attempts=[]
    while True:
        try:
            result=manager('stop','engine','gptoss120')
            return {'result':result,'prior_errors':attempts}
        except (subprocess.SubprocessError,RuntimeError) as exc:
            attempts.append({'at':now(),'error_type':type(exc).__name__})
            if time.monotonic()>=deadline:raise
            await asyncio.sleep(1)

async def smi_query(*arguments):
    process=await asyncio.create_subprocess_exec('nvidia-smi',*arguments,
        stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.DEVNULL)
    try:
        raw,_=await asyncio.wait_for(process.communicate(),10)
        if process.returncode:raise RuntimeError('owned_nvml_monitor_failed')
        return raw
    except BaseException:
        if process.returncode is None:
            try:process.terminate()
            except ProcessLookupError:pass
            try:await asyncio.wait_for(process.wait(),3)
            except TimeoutError:
                try:process.kill()
                except ProcessLookupError:pass
                await asyncio.wait_for(process.wait(),3)
        raise

async def natural_eight():
    root=APP/'.runtime/token-matrix-gptoss120'
    row=next(json.loads(line) for line in (root/'workload.jsonl').read_text().splitlines()
             if json.loads(line)['custom_id']=='mix-0-16384-2048')
    body=row['body'];assert body['max_tokens']==2048
    peak=0;finished=False;samples=[];responses=[];memory=[];physical=[];calls=[];safety_violation=False;monitor_errors=[]
    manager('status','engine','gptoss120')
    members=json.loads((APP/'.runtime/engine-gptoss120.members.json').read_text())
    owned={m['pid'] for m in members['members']}
    async with httpx.AsyncClient(trust_env=False,timeout=600,limits=httpx.Limits(max_connections=16)) as c:
        async def observe():
            nonlocal peak,safety_violation
            ticks=0
            while not finished:
                try:
                    m=await c.get('http://127.0.0.1:8300/metrics',timeout=3)
                    values=re.findall(r'^vllm:num_requests_running\{[^\n]*\}\s+([0-9.eE+-]+)',m.text,re.M)
                    running=sum(float(v) for v in values);peak=max(peak,running)
                    samples.append({'at':now(),'running':running})
                    if ticks%50==0:
                        raw=await smi_query('--query-compute-apps=pid,used_gpu_memory,gpu_uuid','--format=csv,noheader,nounits')
                        for line in raw.decode().splitlines():
                            fields=[v.strip() for v in line.split(',')]
                            if len(fields)==3 and fields[0].isdigit() and int(fields[0]) in owned:
                                memory.append({'at':now(),'pid':int(fields[0]),'gpu_uuid':fields[2],
                                    'used_gpu_memory_mib':int(fields[1]) if fields[1].isdigit() else None})
                        raw=await smi_query('-i','0,1,2,3','--query-gpu=index,memory.free,memory.total','--format=csv,noheader,nounits')
                        current=[]
                        for line in raw.decode().splitlines():
                            fields=[v.strip() for v in line.split(',')]
                            if len(fields)==3 and all(v.isdigit() for v in fields):
                                current.append({'at':now(),'gpu':int(fields[0]),'free_mib':int(fields[1]),'total_mib':int(fields[2])})
                        physical.extend(current)
                        if {v['gpu'] for v in current}!={0,1,2,3} or any(v['free_mib']<1024 for v in current):
                            raise RuntimeError('sampled_shared_gpu_headroom_below_gate')
                    ticks+=1
                except (httpx.HTTPError,ValueError):pass
                except (RuntimeError,TimeoutError,OSError) as exc:
                    safety_violation=True;monitor_errors.append({'at':now(),'error_type':type(exc).__name__})
                    for call in calls:
                        if not call.done():call.cancel()
                    return
                await asyncio.sleep(.1)
        async def invoke(index):
            started=time.monotonic()
            r=await c.post('http://127.0.0.1:8300/v1/chat/completions',json=body)
            data=r.json() if r.status_code==200 else {}
            responses.append({'index':index,'status':r.status_code,'elapsed_seconds':time.monotonic()-started,'response':data})
        calls=[asyncio.create_task(invoke(i)) for i in range(8)]
        sampler=asyncio.create_task(observe())
        began=time.monotonic()
        try:
            results=await asyncio.gather(*calls,return_exceptions=True)
        finally:
            finished=True;await sampler
            private_write(WORK/'eight-natural-private.json',{'body':body,'responses':responses,'samples':samples,'owned_process_gpu_memory':memory,'physical_free_memory':physical,'safety_violation':safety_violation,'monitor_errors':monitor_errors})
    return {'requests':8,'same_exact_public_body_repeated':True,
        'body_sha256':hashlib.sha256(json.dumps(body,sort_keys=True,separators=(',',':')).encode()).hexdigest(),
        'forced_minimum_tokens':False,'ignore_eos':False,'elapsed_seconds':time.monotonic()-began,
        'peak_engine_running_requests':peak,'owned_process_gpu_memory_samples':memory,
        'physical_free_memory_samples':physical,
        'physical_free_scope':'Shared device free including other workloads; used only as a safety gate',
        'physical_sampling_limit':'Sampled free memory is not a continuous memory reservation or guarantee',
        'safety_violation':safety_violation,'monitor_errors':monitor_errors,
        'all_sampled_device_free_ge1GiB':not safety_violation and {p['gpu'] for p in physical}=={0,1,2,3} and all(p['free_mib']>=1024 for p in physical),
        'transport_error_types':[type(r).__name__ for r in results if isinstance(r,BaseException)],
        'all_eight_exact_full_context_outputs':len(responses)==8 and all(r['status']==200 and
            r['response'].get('usage',{}).get('prompt_tokens')==16384 and
            r['response'].get('usage',{}).get('completion_tokens')==2048 for r in responses),
        'early_natural_stop_is_not_itself_capacity_failure':True,
        'records':[{'index':r['index'],'http_status':r['status'],'elapsed_seconds':r['elapsed_seconds'],
            'usage':r['response'].get('usage'),'finish_reason':r['response'].get('choices',[{}])[0].get('finish_reason'),
            'final_chars':len(r['response'].get('choices',[{}])[0].get('message',{}).get('content') or ''),
            'reasoning_chars':len(r['response'].get('choices',[{}])[0].get('message',{}).get('reasoning_content') or ''),
            'final_text_sha256':hashlib.sha256((r['response'].get('choices',[{}])[0].get('message',{}).get('content') or '').encode()).hexdigest()}
           for r in sorted(responses,key=lambda r:r['index'])]}

async def main():
    baseline=json.loads((APP/'.runtime/token-matrix-gptoss120-seq4/controller-report.json').read_text())
    assert baseline['passed'] and len(baseline['policies'])==3
    WORK.mkdir(mode=0o700,exist_ok=False)
    original=MANIFEST.read_bytes();document=json.loads(original)
    (WORK/'original-manifest.json').write_bytes(original);(WORK/'original-manifest.json').chmod(0o600)
    oldengine=APP/'.env.engine-gptoss120-tp4-seq4';oldworker=APP/'.env.worker-gptoss120-tp4-seq4'
    for name,path in [('engine-seq4.env',oldengine),('worker-seq4.env',oldworker)]:
        (WORK/name).write_bytes(path.read_bytes());(WORK/name).chmod(0o600)
    report={'schema_version':1,'started_at':now(),'passed':False,'model':'openai/gpt-oss-120b',
            'revision':'b5c939de8f754692c1647ca79fbf85e8c1e70f8a','baseline_matrix_finished_at':baseline['finished_at'],
            'candidate':{'fraction':.26,'max_num_seqs':8,'worker_slots':8,'worker_token_budget':147520,
                         'context':18440,'batch_tokens':1024,'tensor_parallel_size':4},
            'changes_only':['max_num_seqs4→8','worker_slots4→8','worker_budget73760→147520'],
            'scope':'App-owned TP4 engine/worker only; no other workload or package changes',
            'phase':'begin','original_api_pids':{n:owned_record(APP/f'.runtime/api-{n}.json')['pid'] for n in ('a','b')}}
    paused=False;engine_stop_attempted=False;candidate_env=APP/'.env.engine-gptoss120-tp4-seq8'
    worker_env=APP/'.env.worker-gptoss120-tp4-seq8'
    persist(report)
    try:
        report['watchdog_stop']=manager('stop','supervisor','primary');paused=True
        install_and_adopt(report);report['phase']='operators_adopted';persist(report)
        report['worker_stop']=manager('stop','worker','gptoss120')
        engine_stop_attempted=True
        report['engine_stop']=await stop_engine_bounded()
        report['free_gates']=memory_gate();persist(report)
        assert all(r['passed'] for r in report['free_gates'])
        write_env(candidate_env,oldengine.read_text(),{'ENGINE_GPU_MEMORY_UTILIZATION':'.26','ENGINE_MAX_NUM_SEQS':8})
        write_env(worker_env,oldworker.read_text(),{'WORKER_MAX_INFLIGHT':8,'WORKER_TOKEN_BUDGET':147520})
        log=APP/'.runtime/engine-gptoss120.log';offset=log.stat().st_size;report['log_offset']=offset
        report['engine_start']=manager('start','engine','gptoss120',env=candidate_env,port=8300,gpu='0,1,2,3')
        report['phase']='candidate_booting';persist(report)
        print(json.dumps({'phase':report['phase'],'pid':report['engine_start']['pid']}),flush=True)
        await engine_ready(report['engine_start']['pid'])
        with log.open('rb') as f:f.seek(offset);text=f.read().decode(errors='replace')
        report['capacity_log_lines']=[s for s in text.splitlines() if any(k in s for k in ('Available KV cache memory:','GPU KV cache size:','Maximum concurrency for','Model loading took'))]
        capacities=[float(v) for v in re.findall(r'Maximum concurrency for 18,440 tokens per request:\s*([0-9.]+)x',text)]
        report['minimum_logged_full_context_concurrency']=min(capacities) if capacities else None
        assert capacities and min(capacities)>=8
        report['phase']='natural_eight_probe';persist(report)
        report['natural_eight']=await natural_eight()
        assert report['natural_eight']['all_eight_exact_full_context_outputs'] and report['natural_eight']['peak_engine_running_requests']>=8 and report['natural_eight']['all_sampled_device_free_ge1GiB']
        await ready(8300,'/health',10)
        report['worker_start']=manager('start','worker','gptoss120',env=worker_env)
        await ready(9105,'/readyz',45)
        for service in document['services']:
            if service['role']=='engine' and service['name']=='gptoss120':service['env_file']=str(candidate_env)
            if service['role']=='worker' and service['name']=='gptoss120':service['env_file']=str(worker_env)
        private_write(MANIFEST,document)
        report['passed']=True;report['phase']='candidate_accepted'
    except Exception as exc:
        report['error_type']=type(exc).__name__;report['phase']='rollback'
        if engine_stop_attempted:
            try:
                manager('stop','worker','gptoss120');report['rollback_stop']=await stop_engine_bounded()
                private_write(MANIFEST,json.loads(original))
                report['rollback_free_gates']=memory_gate(.26,1024**3)
                assert all(r['passed'] for r in report['rollback_free_gates'])
                report['rollback_engine_start']=manager('start','engine','gptoss120',env=oldengine,port=8300,gpu='0,1,2,3')
                await engine_ready(report['rollback_engine_start']['pid'])
                report['rollback_worker_start']=manager('start','worker','gptoss120',env=oldworker)
                await ready(9105,'/readyz',45);report['rollback_ready']=True
            except Exception as rollback:
                report['rollback_error_type']=type(rollback).__name__
        else:
            await ready(8300,'/health',10)
            manager('start','worker','gptoss120',env=oldworker)
            await ready(9105,'/readyz',45);report['original_pair_ready']=True
    finally:
        if paused:
            report['watchdog_start']=manager('start','supervisor','primary',env=Path('/root/batchforge-runtime/supervisor.env'))
        report['api_pids_unchanged']=all(owned_record(APP/f'.runtime/api-{n}.json')['pid']==pid for n,pid in report['original_api_pids'].items())
        report['finished_at']=now();persist(report)
        print(json.dumps(report),flush=True)

if __name__=='__main__':asyncio.run(main())
