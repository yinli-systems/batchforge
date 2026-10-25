from pathlib import Path
from datetime import datetime,timezone
import json,hashlib,os,psycopg,pwd,subprocess,signal,time,fcntl
from psycopg.rows import dict_row
from agents.contracts import digest
from agents.store import event_document
from ops.worker_latch_guard import Pins
b=Path('/root/batchforge-runtime');base=b/'evidence-api-agent-tool-order-audit-20261025-v2';run=b/'soak480-agent-runtime-schema-20261025-v2'
report={'passed':False,'phase':'prepared','production_database_connected':False,'gpu_tasks_submitted':0,'primary_services_changed':False,'validator_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest()};child=None
try:
 lock=(b/'app/.runtime/product-campaign.lock').open('a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
 source_report=run/'report.json';cohort=json.loads(source_report.read_bytes());assert cohort['passed'] and cohort['isolated_pg_stopped']
 pins=Pins(b/'app');pins.check(full=True);assert pins.identities==cohort['primary_identities_after']
 stage=Path('/tmp/bf-soak480-agent-runtime-schema-20261025-v2');assert not (stage/'data/postmaster.pid').exists()
 tools=Path('/tmp/bf-ci-20261025-v14/toolchain');account=pwd.getpwnam('nobody')
 def drop():os.setgroups([]);os.setgid(account.pw_gid);os.setuid(account.pw_uid)
 env={'PATH':'/usr/bin:/bin','HOME':str(stage),'LANG':'C','LC_ALL':'C','LD_LIBRARY_PATH':str(tools/'usr/lib/x86_64-linux-gnu')}
 command=[str(tools/'usr/lib/postgresql/17/bin/postgres'),'-D',str(stage/'data'),'-h','','-p','55484','-k',str(stage/'socket'),'-c','default_transaction_read_only=on']
 with (base/'postgres-private.log').open('xb') as output:child=subprocess.Popen(command,env=env,cwd=stage,preexec_fn=drop,stdout=output,stderr=subprocess.STDOUT,start_new_session=True)
 fd=os.pidfd_open(child.pid);report['native_pg_pid']=child.pid
 cfg=json.loads((run/'.runtime/config.json').read_bytes());deadline=time.monotonic()+20
 while True:
  if child.poll() is not None:raise RuntimeError('Read-only audit cluster stopped')
  try:db=psycopg.connect(cfg['state_dsn'],row_factory=dict_row,options='-c default_transaction_read_only=on -c statement_timeout=5000');break
  except psycopg.OperationalError:
   if time.monotonic()>deadline:raise
   time.sleep(.2)
 with db:
  assert db.execute('SHOW transaction_read_only').fetchone()['transaction_read_only']=='on'
  rows=db.execute('SELECT id,tenant_id,status,policy,state,result,revision,event_hash,model_calls,tool_calls,generated_tokens FROM bf_agents.runs ORDER BY id').fetchall()
  checked=[]
  for row in rows:
   required=row['policy']['tool_sequence'];actual=[o['tool'] for o in row['state']['observations']]
   assert row['status']=='completed' and required and actual[:len(required)]==required
   events=db.execute('SELECT revision,kind,payload,previous_hash,event_hash FROM bf_agents.events WHERE run_id=%s ORDER BY revision',(row['id'],)).fetchall()
   previous='0'*64
   for i,e in enumerate(events):
    assert e['revision']==i and e['previous_hash']==previous and digest(event_document(row['tenant_id'],row['id'],i,e['kind'],e['payload'],previous))==e['event_hash'];previous=e['event_hash']
   assert len(events)==row['revision']+1 and previous==row['event_hash']
   assert events[-1]['kind']=='run.completed' and events[-1]['payload']['state_sha256']==digest(row['state']) and events[-1]['payload']['result_sha256']==digest(row['result'])
   assert any(v['run_id']==row['id'] and v['head_sha256']==previous for v in cohort['agent_event_heads'])
   checked.append({'run_id':row['id'],'required':required,'actual':actual,'model_calls':row['model_calls'],'tool_calls':row['tool_calls'],'generated_tokens':row['generated_tokens'],'state_sha256':digest(row['state']),'event_head_sha256':previous})
  assert len(checked)==20
 report.update(passed=True,phase='complete',source_commit=cohort['source_commit'],source_report_sha256=hashlib.sha256(source_report.read_bytes()).hexdigest(),checks=checked,scope='Independent native PostgreSQL restart in read-only mode; actual committed tool order and full event/state/result hash chain survived; no Azure Agent migration or Pod-loss durability claim')
 pins.check(full=True);report['primary_identities']=pins.identities
except BaseException as exc:report.update(passed=False,phase='failed',error_type=type(exc).__name__,credentials_suppressed=True)
finally:
 if child is not None and child.poll() is None:
  signal.pidfd_send_signal(fd,signal.SIGTERM)
  try:child.wait(timeout=15)
  except subprocess.TimeoutExpired:
   signal.pidfd_send_signal(fd,signal.SIGINT)
   try:child.wait(timeout=15)
   except subprocess.TimeoutExpired:report.update(passed=False,phase='failed',cleanup_incomplete=True)
 report['native_pg_stopped']=child is None or child.poll() is not None
 report['captured_at']=datetime.now(timezone.utc).isoformat();(base/'validation.json').write_text(json.dumps(report,indent=2)+'\n')
 print('BF_ORDER_V2',report['passed'],report['native_pg_stopped'])
