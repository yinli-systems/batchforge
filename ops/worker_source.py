"""Prepare/probe an immutable worker source without changing a live service.

PYTHONPATH alone loses to the managed worker's cwd when Python runs ``-m``.
Python 3.11+ safe-path mode removes that implicit cwd entry. The probe imports
definitions and validates Settings; it does not run Worker, register, claim,
open a listener, sample NVIDIA or execute inference.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess

from worker.inventory import selection

PROBED_SOURCES=('worker/__init__.py','worker/__main__.py','worker/config.py','worker/runtime.py',
                'worker/metrics.py','worker/kv_admission.py','worker/inventory.py',
                'backend/scheduling.py','backend/fleet.py')


def source_environment(original,source,*,node_id,gpu_indices):
    source=Path(source)
    if not source.is_absolute() or os.pathsep in str(source) or source==Path('/workspace') or Path('/workspace') in source.parents:
        raise ValueError('One absolute approved source directory is required')
    if not isinstance(node_id,str) or not 1<=len(node_id)<=256:
        raise ValueError('Explicit bounded node identity required')
    if not gpu_indices or any(type(i) is not int for i in gpu_indices):raise ValueError('Explicit device selection required')
    if not original.get('WORKER_ID') or not original.get('MODEL_NAME'):
        raise ValueError('Source-only rollout must preserve explicit worker and model identities')
    mask=','.join(str(i) for i in gpu_indices);selection(mask)
    return {**original,'PYTHONSAFEPATH':'1','PYTHONPATH':str(source),
            'WORKER_INVENTORY_NODE_ID':node_id,'WORKER_INVENTORY_GPU_INDICES':mask}


PROBE=r'''
import hashlib,importlib,json,sys
from pathlib import Path
source=Path(sys.argv[1]).resolve();stage='safe_path'
result={'protocol':'batchforge.worker-source-probe.v1','passed':False}
try:
 if not sys.flags.safe_path:raise RuntimeError('safe_path_required')
 stage='module_origins';files={}
 for name in ('worker','worker.config','worker.runtime','worker.metrics','worker.kv_admission','worker.inventory','backend.scheduling','backend.fleet'):
  module=importlib.import_module(name);path=Path(module.__file__).resolve()
  if not path.is_relative_to(source):raise RuntimeError('module_origin_differs')
  files[str(path.relative_to(source))]=hashlib.sha256(path.read_bytes()).hexdigest()
 # Read the entrypoint bytes; importing worker.__main__ would start the worker.
 entrypoint=(source/'worker/__main__.py').resolve()
 if not entrypoint.is_relative_to(source):raise RuntimeError('entrypoint_origin_differs')
 files['worker/__main__.py']=hashlib.sha256(entrypoint.read_bytes()).hexdigest()
 stage='worker_settings'
 from worker.config import Settings
 config=Settings.from_env()
 result.update(passed=True,safe_path=True,source_files=files,model=config.model,worker_id=config.worker_id,
  slots=config.slots,token_budget=config.token_budget,max_model_len=config.max_model_len,
  adaptive_kv_enabled=config.adaptive_kv_enabled,inventory_gpu_indices=list(config.inventory_gpu_indices),
  inventory_node_sha256=hashlib.sha256(config.inventory_node_id.encode()).hexdigest(),
  runtime_fingerprint=(config.scheduling_profile or {}).get('runtime_fingerprint'),
  registration_performed=False,inference_performed=False,services_changed=False)
except BaseException as exc:
 result.update(stage=stage,error_type=type(exc).__name__)
print(json.dumps(result,separators=(',',':')))
sys.exit(0 if result['passed'] else 1)
'''


def probe_source(python,source,environment,cwd,expected_sources):
    """Returns only fixed metadata, with module bytes pinned to a source receipt."""
    expected={name:expected_sources[name] for name in PROBED_SOURCES}
    try:
        completed=subprocess.run([str(python),'-c',PROBE,str(source)],cwd=cwd,env=environment,
            stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,timeout=15,check=False)
        if len(completed.stdout)>8192:raise ValueError('probe_output_bound')
        result=json.loads(completed.stdout)
    except (OSError,ValueError,subprocess.SubprocessError):
        raise RuntimeError('Worker source probe did not return bounded metadata') from None
    if (not isinstance(result,dict) or completed.returncode or result.get('protocol')!='batchforge.worker-source-probe.v1'
        or result.get('passed') is not True or result.get('safe_path') is not True
        or result.get('source_files')!=expected or result.get('model')!=environment.get('MODEL_NAME')
        or result.get('worker_id')!=environment.get('WORKER_ID')
        or result.get('inventory_gpu_indices')!=list(selection(environment['WORKER_INVENTORY_GPU_INDICES']))
        or result.get('inventory_node_sha256')!=hashlib.sha256(environment['WORKER_INVENTORY_NODE_ID'].encode()).hexdigest()
        or any(result.get(k) is not False for k in ('registration_performed','inference_performed','services_changed'))):
        raise RuntimeError('Worker source origin, configuration or receipt check failed')
    allowed={'protocol','passed','safe_path','source_files','model','worker_id','slots','token_budget','max_model_len',
        'adaptive_kv_enabled','inventory_gpu_indices','inventory_node_sha256','runtime_fingerprint',
        'registration_performed','inference_performed','services_changed'}
    return {key:value for key,value in result.items() if key in allowed}
