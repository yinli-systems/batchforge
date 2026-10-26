
from pathlib import Path
import ast,json,hashlib,traceback
from scripts.engine_quality import prepare,response_record
from scripts.engine_quality_correction import correction
from scripts import quality_eval,contract_scoring
root=Path('/root/batchforge-runtime/soak480-engine-quality-full-20261026-v1')
base=Path('/root/batchforge-runtime/evidence-api-engine-contract-correction-20261026-v1')
base.mkdir(mode=0o700,exist_ok=False);(base/'run').mkdir(mode=0o700)
def sha(raw):return hashlib.sha256(raw).hexdigest()
def write(path,value):
 from ops.ha.set_scheduler_policy import atomic_bytes
 atomic_bytes(path,(json.dumps(value,indent=2,allow_nan=False)+'\n').encode())
r={'protocol':'batchforge.engine-contract-correction-audit.v1','passed':False,'phase':'auditing',
 'new_inference_requests':0,'original_artifacts_modified':False,'scope':'Read-only replay of unchanged public benchmark raw bytes under invalid-contract-zero policy; no model rerun'}
try:
 report_raw=(root/'run/report.json').read_bytes();scores_raw=(root/'run/scores.jsonl').read_bytes()
 report=json.loads(report_raw);scores={}
 for line in scores_raw.splitlines():
  row=json.loads(line)
  if row['event']=='inference.terminal':scores[row['record']['id']]=row['record']
 policy=correction(report_raw,scores_raw)
 corpus=prepare(Path('/root/batchforge-runtime/app/.runtime/public-data/dev-v1.1.json').read_bytes());assert len(corpus.cases)==10570
 old=Path('/root/batchforge-runtime/evidence-api-agent-runtime-20261026-v14/source/scripts/quality_eval.py').read_bytes()
 assert sha(old)==report['scorer_sha256']
 def definitions(raw):
  tree=ast.parse(raw);return {node.name:ast.dump(node,include_attributes=False) for node in tree.body if isinstance(node,(ast.FunctionDef,ast.AsyncFunctionDef)) and node.name in ('normalize','score')}
 legacy=definitions(old);current=definitions(Path(quality_eval.__file__).read_bytes());assert legacy==current and len(legacy)==2
 total=0;changed=[]
 for case in corpus.cases:
  original=scores[case.id];p=root/'private-responses'/(sha(case.id.encode())+'.json');assert not p.is_symlink() and p.stat().st_size<=2*1024**2
  raw=p.read_bytes();total+=len(raw);assert total<=64*1024**2 and sha(raw)==original['response_bytes_sha256']
  replay=response_record(case,json.loads(raw),prompt_count=original['prompt_count'])
  expected=contract_scoring.enforce(original['answer_contract_valid'],{key:original[key] for key in ('exact_match','f1')})
  for key,value in replay.items():assert value==(expected[key] if key in expected else original[key])
  if any(replay[key]!=original[key] for key in expected):changed.append(case.id)
 assert sorted(changed)==sorted(v['id'] for v in policy['changed_cases'])
 write(base/'run/report.json',policy)
 assert sha((root/'run/report.json').read_bytes())==sha(report_raw) and sha((root/'run/scores.jsonl').read_bytes())==sha(scores_raw)
 source=Path(__import__('scripts.engine_quality_correction',fromlist=['x']).__file__).resolve().parents[1]
 receipt=json.loads((source.parent/'source-receipt.json').read_text())
 r.update(phase='complete',passed=True,original_report_sha256=sha(report_raw),original_scores_sha256=sha(scores_raw),
  correction_sha256=sha((base/'run/report.json').read_bytes()),policy_source_sha256=policy['policy_source_sha256'],
  private_response_count=10570,private_response_bytes=total,all_raw_responses_replayed=True,
  original_reference_metric_functions_unchanged=True,reference_functions_ast_sha256=sha(json.dumps(legacy,sort_keys=True).encode()),
  correction_builder_source_sha256=sha(Path(__import__('scripts.engine_quality_correction',fromlist=['x']).__file__).read_bytes()),
  correction_operator_commit=receipt['git_commit'],changed_records=len(changed),changed_case_ids=changed,
  corrected_exact_results=policy['corrected_exact_results'],corrected_exact_match_percent=policy['corrected_exact_match_percent'],
  corrected_f1_percent=policy['corrected_f1_percent'],corrected_quality_passed=policy['corrected_quality_passed'])
except BaseException as e:r.update(phase='failed',error_type=type(e).__name__,frames=[{'file':Path(t.filename).name,'line':t.lineno,'function':t.name} for t in traceback.extract_tb(e.__traceback__)])
write(base/'validation.json',r);print('BF_CORRECTION_RESULT',json.dumps(r),flush=True)
