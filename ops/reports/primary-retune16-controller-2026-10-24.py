"""One-off, app-owned primary GPU retune; rollback every failed target to eight."""
import asyncio
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tarfile
import time

import httpx

ROOT = Path('/root/batchforge-runtime/app')
sys.path.insert(0, str(ROOT))
from ops.manage import owned_record
from ops.supervise import private_write, load_manifest

PY = '/root/batchforge-runtime/venv/bin/python'
ENGINE_PY = '/root/batchforge-runtime/engine-venv/bin/python'
ARCHIVE_SHA = 'a2a3ac2f6f5be7593f13a52cbf9cc3167031c9751c8f9df2e9c1ae47323c428f'
MODEL = 'Qwen/Qwen2.5-7B-Instruct'
WORK = ROOT / '.runtime/primary-retune16-20261024'
REPORT = ROOT / '.runtime/primary-retune16-report.json'
MANIFEST = ROOT / '.runtime/services-primary.json'
report = {'started_at': datetime.now(timezone.utc).isoformat(), 'scope': 'primary four app-owned H100 engine/worker pairs only',
          'worker_archive_sha256': ARCHIVE_SHA, 'model': MODEL, 'revision': 'a09a35458c702b33eeacc393d103063234e8bc28',
          'unchanged': {'dtype':'bfloat16','max_model_len':4096,'gpu_memory_utilization':0.24,'max_num_batched_tokens':4096,'prefix_caching':False,'tensor_parallel_size':1,'eager':True},
          'requested_sequences':16, 'requested_worker_token_budget':65536, 'targets':[]}

def persist():
    private_write(REPORT, report)

def manager(action, role, name, *, env=None, port=None, gpu=None):
    cmd=[PY, str(ROOT/'ops/manage.py'), action, role, '--name', name]
    if action == 'stop': cmd += ['--grace-seconds','20']
    if action == 'start':
        cmd += ['--env-file',str(env), '--python',ENGINE_PY if role=='engine' else PY]
        if port: cmd += ['--port',str(port)]
        if gpu is not None: cmd += ['--gpu',str(gpu)]
    p=subprocess.run(cmd,cwd=ROOT,text=True,capture_output=True,timeout=40,check=True)
    return json.loads(p.stdout)

def write_env(path, original, updates):
    lines=original.splitlines()
    seen=set()
    for i,line in enumerate(lines):
        key=line.partition('=')[0].strip().removeprefix('export ')
        if key in updates:
            lines[i]=key+'='+str(updates[key]); seen.add(key)
    lines += [key+'='+str(value) for key,value in updates.items() if key not in seen]
    path.write_text('\n'.join(lines)+'\n'); os.chmod(path,0o600)

async def ready(port, path, seconds=180):
    async with httpx.AsyncClient(trust_env=False, timeout=2) as c:
        end=time.monotonic()+seconds
        while time.monotonic()<end:
            try:
                if (await c.get(f'http://127.0.0.1:{port}{path}')).status_code==200:return
            except httpx.HTTPError:pass
            await asyncio.sleep(0.5)
    raise RuntimeError('readiness_timeout')

async def probe(port):
    """Sixteen real simultaneous HTTP requests, sampled vLLM running gauge."""
    requests=[]; peak=0; done=False; start=time.monotonic()
    async with httpx.AsyncClient(trust_env=False,timeout=90,limits=httpx.Limits(max_connections=24)) as c:
        async def sample():
            nonlocal peak
            while not done:
                try:
                    r=await c.get(f'http://127.0.0.1:{port}/metrics',timeout=2)
                    values=re.findall(r'^vllm:num_requests_running(?:\{[^\n]*\})?\s+([0-9.eE+-]+)',r.text,re.M)
                    peak=max(peak,sum(float(v) for v in values))
                except (httpx.HTTPError,ValueError):pass
                await asyncio.sleep(.02)
        async def generate(index):
            body={'model':MODEL,'messages':[{'role':'user','content':f'Concurrency diagnostic request {index}. Write a detailed numbered explanation of how a durable distributed job queue uses leases, fencing tokens, retries and idempotent accepted usage. Discuss at least fifteen separate design considerations and explain each in multiple sentences.'}], 'max_tokens':128,'temperature':0,'seed':20261024+index}
            r=await c.post(f'http://127.0.0.1:{port}/v1/chat/completions',json=body)
            data=r.json() if r.status_code==200 else {}
            u=data.get('usage',{})
            item={'index':index,'request_sha256':hashlib.sha256(json.dumps(body,sort_keys=True,separators=(',',':')).encode()).hexdigest(),'status':r.status_code,'prompt_tokens':u.get('prompt_tokens'),'completion_tokens':u.get('completion_tokens'),'finish_reason':data.get('choices',[{}])[0].get('finish_reason')}
            requests.append(item)
            if r.status_code!=200 or not isinstance(u.get('completion_tokens'),int) or u['completion_tokens']<=0:raise RuntimeError('generation_probe_failed')
        sampler=asyncio.create_task(sample())
        try:await asyncio.gather(*(generate(i) for i in range(16)))
        finally:
            done=True; await sampler
    result={'requests':sorted(requests,key=lambda v:v['index']), 'elapsed_seconds':time.monotonic()-start,'peak_engine_running_requests':peak,'max_tokens':128,'temperature':0,'forced_minimum_tokens':False,'ignore_eos':False,'engine_only':True,'not_platform_capacity_benchmark':True}
    if len(requests)!=16 or peak<16:raise RuntimeError('generation_probe_did_not_observe_16_running:'+json.dumps(result))
    return result

def free_gate(gpu):
    cmd=[ENGINE_PY,'-c',"import json,torch;f,t=torch.cuda.mem_get_info(0);print(json.dumps({'free_bytes':f,'total_bytes':t}))"]
    env={**os.environ,'CUDA_VISIBLE_DEVICES':str(gpu)}
    end=time.monotonic()+30
    while True:
        p=subprocess.run(cmd,cwd=ROOT,env=env,text=True,capture_output=True,timeout=20,check=True)
        v=json.loads(p.stdout); v['required_free_bytes']=v['total_bytes']*.24+512*1024**2
        v['passed']=v['free_bytes']>=v['required_free_bytes']
        if v['passed'] or time.monotonic()>=end:return v
        time.sleep(1)

async def main():
    WORK.mkdir(mode=0o700,parents=True,exist_ok=False)
    old_engine=(ROOT/'.env.engine7b').read_text()
    backup_engine=WORK/'engine8.env'; write_env(backup_engine,old_engine,{})
    document=json.loads(MANIFEST.read_text())
    private_write(WORK/'original-services.json',document)
    original_workers={}
    for gpu in range(4):
        source=ROOT/f'.env.worker7b-gpu{gpu}'
        original_workers[gpu]=source.read_text()
        write_env(WORK/f'worker-gpu{gpu}-8.env',original_workers[gpu],{})
        for service in document['services']:
            if service['role']=='engine' and service['name']==f'gpu{gpu}':service['env_file']=str(backup_engine)
    report['original_api_pids']={name:owned_record(ROOT/f'.runtime/api-{name}.json')['pid'] for name in ('a','b')}
    report['original_engine_pids']=[owned_record(ROOT/f'.runtime/engine-gpu{g}.json')['pid'] for g in range(4)]
    persist()
    watchdog_stopped=False
    try:
        report['watchdog_stop']=manager('stop','supervisor','primary');watchdog_stopped=True
        private_write(MANIFEST,document)
        for gpu in range(4):manager('stop','worker',f'gpu{gpu}')
        archive=ROOT/f'.runtime/worker-{ARCHIVE_SHA}.tar'
        if hashlib.sha256(archive.read_bytes()).hexdigest()!=ARCHIVE_SHA:raise RuntimeError('archive_hash_mismatch')
        with tarfile.open(archive) as tf:
            members=tf.getmembers()
            if any(not m.isfile() or not m.name.startswith('worker/') or '..' in Path(m.name).parts for m in members):raise RuntimeError('unsafe_worker_archive')
            tf.extractall(ROOT,members=members,filter='data')
        for gpu in range(4):
            target={'gpu':gpu,'started_at':datetime.now(timezone.utc).isoformat(),'accepted_sequences':8}
            report['targets'].append(target);persist()
            logfile=ROOT/f'.runtime/engine-gpu{gpu}.log'
            newenv=ROOT/f'.env.engine7b-gpu{gpu}-16'
            try:
                manager('stop','engine',f'gpu{gpu}')
                target['free_gate']=free_gate(gpu)
                if not target['free_gate']['passed']:raise RuntimeError('free_memory_gate_failed')
                write_env(newenv,old_engine,{'ENGINE_MAX_NUM_SEQS':16})
                offset=logfile.stat().st_size
                target['engine_start']=manager('start','engine',f'gpu{gpu}',env=newenv,port=8100+gpu,gpu=gpu)
                await ready(8100+gpu,'/health')
                with logfile.open('rb') as f:f.seek(offset); text=f.read().decode(errors='replace')
                found=re.findall(r'GPU KV cache size:\s*([\d,]+)\s*tokens',text)
                target['kv_cache_capacity_tokens']=int(found[-1].replace(',','')) if found else None
                target['kv_capacity_log_lines']=[line for line in text.splitlines() if 'GPU KV cache size:' in line]
                if not found or target['kv_cache_capacity_tokens']<65536:raise RuntimeError('insufficient_proven_kv_capacity')
                target['generation_probe']=await probe(8100+gpu)
                target['accepted_sequences']=16
                write_env(ROOT/f'.env.worker7b-gpu{gpu}',original_workers[gpu],{'WORKER_MAX_INFLIGHT':16,'WORKER_TOKEN_BUDGET':65536})
                for service in document['services']:
                    if service['role']=='engine' and service['name']==f'gpu{gpu}':service['env_file']=str(newenv)
                private_write(MANIFEST,document)
                target['worker_start']=manager('start','worker',f'gpu{gpu}',env=ROOT/f'.env.worker7b-gpu{gpu}')
                await ready(9101+gpu,'/readyz',30)
                target['worker_ready']=True
            except Exception as e:
                target['upgrade_error']=str(e)[:12000]
                manager('stop','worker',f'gpu{gpu}')
                manager('stop','engine',f'gpu{gpu}')
                target['rollback_free_gate']=free_gate(gpu)
                if not target['rollback_free_gate']['passed']:raise RuntimeError('rollback_waiting_for_free_memory')
                target['rollback_engine_start']=manager('start','engine',f'gpu{gpu}',env=backup_engine,port=8100+gpu,gpu=gpu)
                await ready(8100+gpu,'/health')
                target['accepted_sequences']=8
                write_env(ROOT/f'.env.worker7b-gpu{gpu}',original_workers[gpu],{})
                for service in document['services']:
                    if service['role']=='engine' and service['name']==f'gpu{gpu}':service['env_file']=str(backup_engine)
                private_write(MANIFEST,document)
                target['rollback_worker_start']=manager('start','worker',f'gpu{gpu}',env=ROOT/f'.env.worker7b-gpu{gpu}')
                await ready(9101+gpu,'/readyz',30);target['rollback_ready']=True
            target['finished_at']=datetime.now(timezone.utc).isoformat();persist()
            print(json.dumps({'gpu':gpu,'sequences':target['accepted_sequences'],'kv_tokens':target.get('kv_cache_capacity_tokens'),'ready':target.get('worker_ready',target.get('rollback_ready'))}),flush=True)
        report['passed_all16']=all(t['accepted_sequences']==16 and t.get('worker_ready') for t in report['targets'])
    except Exception as e:
        report['fatal_error']=str(e)[:2000]
        raise
    finally:
        if watchdog_stopped:
            load_manifest(MANIFEST)
            report['watchdog_start']=manager('start','supervisor','primary',env=Path('/root/batchforge-runtime/supervisor.env'))
        report['final_api_pids']={name:owned_record(ROOT/f'.runtime/api-{name}.json')['pid'] for name in ('a','b')}
        report['api_pids_unchanged']=report['final_api_pids']==report['original_api_pids']
        report['finished_at']=datetime.now(timezone.utc).isoformat();persist()

if __name__=='__main__':asyncio.run(main())
