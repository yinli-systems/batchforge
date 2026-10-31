"""Finite 480B trial on the existing TP4 allocation, then restore the serving pair.

Runs on the Pod independently of SSH. API/catalog/database configuration is not
changed. Exact managed identities, a drained queue and a verified checkpoint are
required before swapping engines. A finally block restores the original engine,
worker and watchdog after success or failure. This is not whole-Pod HA.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

from ops.download_480b import REPO, REVISION
from ops.gpu_campaign import write_json


def require(condition, reason):
    if not condition:
        raise RuntimeError(reason)


def restore_services(manager, engine, worker, supervisor, wait, record):
    """Do not start a worker against an unready engine; always restore supervision."""
    failures = []
    try:
        wait(lambda: manager.start_gate(engine)[0], 120, 'original_gpu_memory_gate_timeout')
        manager.invoke(engine, 'start')
        wait(lambda: manager.ready(engine), 900, 'original_engine_readiness_timeout')
        manager.invoke(worker, 'start')
        wait(lambda: manager.ready(worker), 120, 'original_worker_readiness_timeout')
        record('serving_pair_restored')
    except Exception as error:
        failures.append(type(error).__name__)
        record('serving_pair_restore_failed')
    finally:
        try:
            manager.invoke(supervisor, 'start')
            record('watchdog_restored')
        except Exception as error:
            failures.append(type(error).__name__)
            record('watchdog_restore_failed')
    return failures


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('/root/batchforge-runtime/app'))
    parser.add_argument('--run', type=Path, required=True)
    parser.add_argument('--model', type=Path, required=True)
    parser.add_argument('--benchmark-source', type=Path, required=True)
    args = parser.parse_args()
    root, run = args.root.resolve(), args.run.resolve()
    require(root == Path('/root/batchforge-runtime/app'), 'only_existing_batchforge_runtime_allowed')
    require(run.is_relative_to(root.parent) and not run.exists(), 'new_local_trial_directory_required')
    os.umask(0o077)
    run.mkdir(mode=0o700, parents=True)
    # Import only the installed owned-process management code. The candidate
    # engine does not inherit tenant API keys or database credentials.
    sys.path.insert(0, str(root))
    from ops.supervise import Manager, load_manifest, private_read
    from ops.ha.retune_full_gpu import file_env, write_env
    import psycopg

    class Managed:
        def __init__(self): self.delegate = Manager(root=root, timeout=90)
        def invoke(self, service, action):
            command = [str(root.parent/'venv/bin/python'), str(root/'ops/manage.py'), action,
                       service['role'], '--name', service['name']]
            if action == 'stop': command += ['--grace-seconds', '15']
            if action == 'start':
                command += ['--env-file', service['env_file'], '--python', service['python']]
                for key in ('port', 'gpu'):
                    if service.get(key) is not None: command += ['--'+key, str(service[key])]
            result = subprocess.run(command, cwd=root, capture_output=True, timeout=90)
            require(result.returncode == 0, 'managed_'+action+'_failed')
            value = json.loads(result.stdout)
            return value
        def ready(self, service):
            # Capture live descendant identities while initialization is in
            # progress, so cleanup never guesses ownership after leader loss.
            self.delegate.invoke(service, 'status')
            return self.delegate.readiness(service)
        def start_gate(self, service): return self.delegate.start_gate(service)

    with (root/'.runtime/model-trial.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        manifest, services, _ = load_manifest(root/'.runtime/services-primary.json', root=root)
        original_manifest = private_read(manifest)
        old_engine = next(row for row in services if (row['role'],row['name']) == ('engine','qwen235b'))
        old_worker = next(row for row in services if (row['role'],row['name']) == ('worker','qwen235b'))
        supervisor = {'role':'supervisor','name':'primary','python':str(root.parent/'venv/bin/python'),
                      'env_file':str(root.parent/'supervisor.env')}
        original_env = file_env(old_engine['env_file'])
        require(original_env['MODEL_NAME'] == 'Qwen/Qwen3-235B-A22B-Instruct-2507-FP8', 'unexpected_original_model')
        receipt = json.loads((args.model/'download-receipt.json').read_text())
        require(receipt.get('status') in {'verified-local','verified'} and receipt.get('revision') == REVISION,
                'verified_pinned_checkpoint_required')
        verified = receipt['verified']
        require(len([x for x in verified if x['file'].endswith('.safetensors')]) == 51, 'checkpoint_shard_count')
        for entry in verified:
            require((args.model/entry['file']).stat().st_size == entry['bytes'], 'checkpoint_changed_after_verification')
        api = next(row for row in services if row['role'] == 'api')
        with psycopg.connect(file_env(api['env_file'])['DATABASE_URL'], connect_timeout=10,
                             options='-c default_transaction_read_only=on -c statement_timeout=10000') as database:
            active = database.execute("SELECT count(*) FROM tasks WHERE model=%s AND status IN ('pending','running')",
                                      (original_env['MODEL_NAME'],)).fetchone()[0]
        require(active == 0, 'serving_queue_must_be_drained')
        env_path = root/'.runtime/480b-trial-engine.env'
        require(not env_path.exists(), 'candidate_configuration_already_exists')
        write_env(env_path, {'MODEL_NAME':REPO, 'ENGINE_MODEL_PATH':str(args.model), 'ENGINE_REVISION':REVISION,
            'ENGINE_BACKEND':'vllm', 'ENGINE_DTYPE':'half', 'ENGINE_TENSOR_PARALLEL_SIZE':'4',
            'ENGINE_MAX_MODEL_LEN':'18440', 'ENGINE_MAX_NUM_SEQS':'32',
            'ENGINE_GPU_MEMORY_UTILIZATION':'.90', 'ENGINE_GPU_MEMORY_MARGIN_MIB':'2048',
            'ENGINE_MAX_NUM_BATCHED_TOKENS':'4096', 'ENGINE_ENFORCE_EAGER':'1',
            'ENGINE_ENABLE_CHUNKED_PREFILL':'1','ENGINE_ENABLE_PREFIX_CACHING':'0',
            'ENGINE_GENERATION_CONFIG':'vllm', 'OMP_NUM_THREADS':'8', 'TOKENIZERS_PARALLELISM':'false',
            'HF_HUB_OFFLINE':'1', 'TRANSFORMERS_OFFLINE':'1'})
        candidate = {'role':'engine','name':'qwen480bench','env_file':str(env_path),
                     'python':old_engine['python'],'port':8348,'gpu':old_engine['gpu']}
        report = {'started_at':datetime.now(timezone.utc).isoformat(), 'phase':'prepared',
            'model':REPO,'revision':REVISION, 'total_parameters':480154875392,'active_parameters_approx':35000000000,
            'quantization':'AWQ INT4; kernel must be confirmed in actual startup log',
            'manifest_sha256':hashlib.sha256(original_manifest).hexdigest(), 'events':[],
            'limitations':['One existing four-H100 Pod, no independent-node HA',
                           'Local staging is not Pod-loss durable; evidence must be exported off Pod',
                           'Finite engine benchmark; no tenant routing or production capacity claim']}
        def event(name):
            report['events'].append({'at':datetime.now(timezone.utc).isoformat(),'event':name})
            write_json(run/'trial.json', report)
            print(name, flush=True)
        def wait(predicate, seconds, reason):
            deadline = time.monotonic()+seconds
            while not predicate():
                require(time.monotonic()<deadline, reason)
                # While the shared watchdog is paused, keep its control-plane
                # process-liveness responsibility. Database readiness failures
                # never trigger a restart of a live API.
                for service in services:
                    if service['role'] not in {'api','gateway'}:
                        continue
                    state, error = manager.delegate.invoke(service,'status')
                    require(error is None, 'control_plane_identity_probe_failed')
                    if not state['running']:
                        count = report.setdefault('control_plane_restarts', 0)
                        require(count < 3, 'control_plane_restart_limit')
                        report['control_plane_restarts'] = count + 1
                        manager.invoke(service,'start')
                        event('control_plane_process_recovered')
                time.sleep(2)
        manager = Managed()
        altered = False
        campaign_lock = None
        try:
            # The original campaign lock is held for the whole swap, not just
            # checked once. It coordinates explicitly participating operators.
            name=hashlib.sha256(b'http://127.0.0.1:8300').hexdigest()[:16]
            campaign_lock = open('/tmp/batchforge-gpu-campaign-'+name+'.lock','a')
            fcntl.flock(campaign_lock,fcntl.LOCK_EX | fcntl.LOCK_NB)
            if campaign_lock:
                altered = True
                manager.invoke(supervisor,'stop'); event('watchdog_paused')
                manager.invoke(old_worker,'stop'); event('original_worker_drained')
                manager.invoke(old_engine,'stop'); event('original_engine_stopped')
                wait(lambda:manager.start_gate(candidate)[0],120,'candidate_gpu_memory_gate_timeout')
                manager.invoke(candidate,'start'); event('480b_engine_loading')
                wait(lambda:manager.ready(candidate),1200,'candidate_engine_readiness_timeout')
                event('480b_engine_ready')
                command=[str(root.parent/'engine-venv/bin/python'),'-m','scripts.token_workload',
                    '--dataset',str(root/'.runtime/public-data/dev-v1.1.json'),'--tokenizer',str(args.model),
                    '--revision',REVISION,'--model',REPO,'--repeats','1','--context-limit','18440',
                    '--output',str(run/'workload.jsonl'),'--manifest',str(run/'manifest.json')]
                subprocess.run(command,cwd=root,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,check=True,timeout=180)
                command=[str(root.parent/'engine-venv/bin/python'),'-m','ops.gpu_campaign',
                    '--url','http://127.0.0.1:8348','--workload',str(run/'workload.jsonl'),
                    '--manifest',str(run/'manifest.json'),'--out',str(run/'benchmark'),
                    '--engine-label','480B-A35B-AWQ-TP4-eager','--policies','fifo','--concurrency','16']
                subprocess.run(command,cwd=args.benchmark_source,check=True,timeout=1800)
                report['phase']='benchmark_passed'; event('480b_benchmark_passed')
        except Exception as error:
            report['phase']='failed'; report['error_type']=type(error).__name__; event('trial_failed')
        finally:
            if altered:
                try:
                    manager.invoke(candidate,'stop'); event('candidate_stopped')
                except Exception:
                    report['candidate_stop_failed']=True
                report['restore_failures']=restore_services(manager,old_engine,old_worker,supervisor,wait,event)
                require(private_read(manifest)==original_manifest,'serving_manifest_changed_during_trial')
            report['finished_at']=datetime.now(timezone.utc).isoformat()
            write_json(run/'trial.json',report)
            if campaign_lock is not None:
                campaign_lock.close()
        require(report['phase']=='benchmark_passed' and not report.get('restore_failures')
                and not report.get('candidate_stop_failed'), 'trial_or_recovery_failed_inspect_private_logs')


if __name__=='__main__': main()
