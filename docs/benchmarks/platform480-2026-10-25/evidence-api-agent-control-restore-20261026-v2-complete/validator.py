"""Bounded complete physical restore from the actual Mac archive; native only."""
from pathlib import Path
from datetime import datetime,timezone
import os,signal,subprocess,json,hashlib,tarfile,io,fcntl,time,asyncio
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from agents.contracts import canonical,digest
from agents.store import AgentStore,AgentError
from agents.migrate import verify as verify_agents
from backend.migrate import verify_schema
from ops.worker_latch_guard import Pins
from ops.manage import process_identity
from ops.qualify_agent_runtime import write
BASE=Path(__file__).parent;ROOT=Path('/root/batchforge-runtime/app');STAGE=Path('/tmp/bf-agent-control-restore-20261026-v2')
EXPECTED={'archive_sha256': '02c7c80fd9560d77020ba0cd902d994e1ca6156d2e07ae9adbc6c4ac1a704fd1', 'compressed_bytes': 6061650, 'records': [{'run_id': 'agrun_3d0944df6f373aca5e50eb6f8ae99905', 'status': 'completed', 'answer_disposition': 'completed', 'revision': 43, 'event_head_sha256': '7350208bfac1ea39ff1eb49516573555e28086244520f0724574d76d779fc19a', 'state_sha256': '9e9314604f1372c12767e7e6f927bcfbc34f7f368c3a39263d14213d65a9c0f4', 'result_sha256': '7296bf400eb91dd6380fb68a6d9372fd8877f25ad99a1ca896c80a983a2a0fbb', 'receipts': [{'batch_id': 'batch_eccf04ece6fe26fb594a212714ef0032', 'current_receipt_sha256': 'a117b8dd4482518eae61ff31d1f5571a9b2d68a288190030bd64607af5bc1e51'}, {'batch_id': 'batch_e39bd517ec13cbf17af477697e5f749b', 'current_receipt_sha256': 'e5e7b364b7a950b14c64decb5c4a50d342e95b9c30e45363b3b003294026448f'}]}, {'run_id': 'agrun_7c6769c246f273c36f0168c975ebede1', 'status': 'completed', 'answer_disposition': 'completed', 'revision': 49, 'event_head_sha256': 'e5d0ed5e77d68f8a38512b130061a953b99a1f30dec4812220b823e4de676c96', 'state_sha256': '97ffc42b9a95500f87721fdd5fea75f5fc361f47d339b4ed66ee32826c0df711', 'result_sha256': '0032cb696f771dd4c8f870f3effc5d12d501de93113190ae1404c3c3588ff3e1', 'receipts': [{'batch_id': 'batch_0fa25d5047433d9dd224f06b55946089', 'current_receipt_sha256': 'cb01ff66837966866798c1b6bb4fb65a5cbf9092973d30900199e3ef80c8807b'}, {'batch_id': 'batch_33650a3f719736403b5b3bfb7bc84878', 'current_receipt_sha256': 'e4aa441a3ee9715ffe9e7a67d64526f796ab2ecb39bf28599149836c59d4a82d'}]}, {'run_id': 'agrun_254902356e5af7996511c8391df4de2f', 'status': 'completed', 'answer_disposition': 'completed', 'revision': 48, 'event_head_sha256': '0bdf841feee6abe6b82075a0b67c990fbc91694f3cece2af5cbf0020a30ae6b6', 'state_sha256': '6b11c3b5389fe89aef0aa83d8c809c9d39a1bb9419c5a14ce134b9fa5f119046', 'result_sha256': 'ea3f2caa4414a48cc7c6323783a21c63da945fd57a50447c5f00dbdb32a31f43', 'receipts': [{'batch_id': 'batch_61dad4da436a7f2030a3c9b8c27befe4', 'current_receipt_sha256': '3a4ca9afdef55c9dbfc5f159e736086b8329158f1adf20486567fd2aad41d265'}, {'batch_id': 'batch_9a97781a5f71025c9c15c00d8d9bfd34', 'current_receipt_sha256': '35bdba74db680399c9b3db813f8468dbb4388c6841577d44281544e95efc7892'}]}, {'run_id': 'agrun_f9a0b1b21b36c98b0ed3b5cedcef82b9', 'status': 'completed', 'answer_disposition': 'completed', 'revision': 48, 'event_head_sha256': '4a6354a23dcbc42a70eecb88374a90f087125f00ad5436165f9c92235ce040b9', 'state_sha256': '3cd019050cc2eef6afa9b3889ca88091756d0f41d64df795b4517eb14f6b6fc7', 'result_sha256': '967910915e3589ea826df020034662705b109eec7f1246bf5879a7c2af2d15db', 'receipts': [{'batch_id': 'batch_ece7af9acaf34dbbf82ca91ece7e33e5', 'current_receipt_sha256': 'f8235a91ed4eaa6ffac3642bc9726dc3559de53f7d948078ed2befc557a769cc'}, {'batch_id': 'batch_b291092d33872a18d623217779bff5dc', 'current_receipt_sha256': '36b9240dae1b7dbd04b63a84dfc123fd3a1a1a7e8cb9e8b4423fc705c0de1ffd'}]}, {'run_id': 'agrun_79b1a3ce08dab3df1a0ac0750d2cb283', 'status': 'completed', 'answer_disposition': 'completed', 'revision': 48, 'event_head_sha256': 'b084109b5869bcc67e4797a83c4e84fd8ae598b349db3a90c212e70c38d695c4', 'state_sha256': 'b5ae713c63d5d91214822e8c73b90d9f9d175563d20fd0921147585c88f71db0', 'result_sha256': '0fe9c048c0512b3508fbd18f5659403d6b917bf9060abcc841eef7653adab0a7', 'receipts': [{'batch_id': 'batch_96df6a27966843572442922b98562781', 'current_receipt_sha256': '0a12ab7acef1d560d19b4ad9351cac7d2f8fee4937c293dc0d072ff82fd0f7a9'}, {'batch_id': 'batch_0ceb5814d9f8ccf4578bfa2c6d8ea567', 'current_receipt_sha256': 'ddb74faf33ce672943db687fd814d524a629f8cf1b1f192a11eb2d8e8f73d4d8'}]}, {'run_id': 'agrun_d0441a6f8e46e00a4b389a8a93de7440', 'status': 'completed', 'answer_disposition': 'completed', 'revision': 47, 'event_head_sha256': 'f87ea1fbb394439f166eb7de1c8e6bb309cf4ccd6229ffc249237bc527f1059a', 'state_sha256': '353e299414d5a926bfc139d1b5b778b5dd3d34f388853022b2830ba63cbd83b3', 'result_sha256': '211eff390dc0d0910dbe241ff99115cd61995d0bbe6e01f659436c3ce444bbdd', 'receipts': [{'batch_id': 'batch_c088924ec0c9fdf1c4dc3627855e7370', 'current_receipt_sha256': 'aa8b0806646e9dbb03e860c07baa1692b0c93e11dc82b36c2b82c5d2f6bc7e87'}, {'batch_id': 'batch_d45bd4b341e546defcdf1e1cd6b868c7', 'current_receipt_sha256': '14aff3f371df67bfeb7d3cfea03d37816f8da0c5f093243a3148da05115df5c9'}]}, {'run_id': 'agrun_467def0ba259f018fa2fb83360094525', 'status': 'completed', 'answer_disposition': 'completed', 'revision': 46, 'event_head_sha256': 'aae63fa7ff835879c7701945cd432656a1a7cf2376dd0b217a371a1b65e86d7a', 'state_sha256': '359a8c3fc028bb5edb2a9866d57c25ced55fa9266edf3f3dc6ca0eaabf9060c4', 'result_sha256': 'af6b4560755e8929770204fb644a8454fcb809fd3a7b6a388602908202267100', 'receipts': [{'batch_id': 'batch_83f9ca5dc6b980166dd2fddc1f3628a2', 'current_receipt_sha256': '51f952ea974f662384e10fd1140d50b58d920f4f199f9e7d79c32dede087d56b'}, {'batch_id': 'batch_0c7a7b2faab8c79aa4dd836162941c29', 'current_receipt_sha256': 'a869ecc0621e1e6b695d63a6d09ba8a1a86edab02d11b96806a01df52e25d72a'}]}, {'run_id': 'agrun_409fbeb9450027d73180c9dbfcda5ee7', 'status': 'completed', 'answer_disposition': 'completed', 'revision': 48, 'event_head_sha256': 'cbf9519ef9843e80287e5f2ca6d1ea28b9b709af891cee04e4e865e9e497c047', 'state_sha256': 'f95e30e1de401b68adf45b3c28f903a444533939db90c91eeac9e6482ef79608', 'result_sha256': 'baa2343f6a546be9f1b1f2ec991d6caec7e9c155dd2501028453a998a0c9e7e1', 'receipts': [{'batch_id': 'batch_a4eebd36bd892c48b204e888baa5f007', 'current_receipt_sha256': 'b99222632925c2c6c3993c1ce98b81be1296463b64a530dde9328f87d3958ffb'}, {'batch_id': 'batch_a0bc3843ae3aa78ab59c55fd877981f5', 'current_receipt_sha256': '4bd297850399b34a1b43e7b02dda6da73f65c55a4c0ed071218d160b6b90a524'}]}], 'task_results': [{'run_id': 'agrun_254902356e5af7996511c8391df4de2f', 'case_id': '5726d7faf1498d1400e8ecdc', 'passed': True, 'completed': True, 'answer_correct': True, 'required_tools': True, 'citation_verified': True, 'exact_match': 1, 'f1': 1.0, 'retrieval_replay_verified': True, 'abstained': False, 'answer_delivered': True, 'answer_sha256': '97c6f38af0dfbe051c4e20c2c7b1ce4fb065feaa8bd95b9d21783a6a823d6bec', 'quoted_span_verified': True, 'status': 'completed', 'model_calls_reserved': 2, 'tool_calls': 1, 'failure_code': None, 'completion_seconds': 43.562022, 'output_contract_rejections': 0, 'accepted_generated_tokens': 199}, {'run_id': 'agrun_3d0944df6f373aca5e50eb6f8ae99905', 'case_id': '5728d5793acd2414000dffb5', 'passed': True, 'completed': True, 'answer_correct': True, 'required_tools': True, 'citation_verified': True, 'exact_match': 1, 'f1': 1.0, 'retrieval_replay_verified': True, 'abstained': False, 'answer_delivered': True, 'answer_sha256': '7bc511f3ddda35324345fd5cd05830c3d795d10618ba1d8478766f563a2ddc80', 'quoted_span_verified': True, 'status': 'completed', 'model_calls_reserved': 2, 'tool_calls': 1, 'failure_code': None, 'completion_seconds': 43.877277, 'output_contract_rejections': 0, 'accepted_generated_tokens': 186}, {'run_id': 'agrun_409fbeb9450027d73180c9dbfcda5ee7', 'case_id': '573005b9947a6a140053cf6c', 'passed': True, 'completed': True, 'answer_correct': True, 'required_tools': True, 'citation_verified': True, 'exact_match': 1, 'f1': 1.0, 'retrieval_replay_verified': True, 'abstained': False, 'answer_delivered': True, 'answer_sha256': '671ed1e21466a9cf663dc2556fe46fcffa1f57b44e29fb14ae5edbc7e3627ee2', 'quoted_span_verified': True, 'status': 'completed', 'model_calls_reserved': 2, 'tool_calls': 1, 'failure_code': None, 'completion_seconds': 47.928757, 'output_contract_rejections': 0, 'accepted_generated_tokens': 193}, {'run_id': 'agrun_467def0ba259f018fa2fb83360094525', 'case_id': '5727aec03acd2414000de994', 'passed': True, 'completed': True, 'answer_correct': True, 'required_tools': True, 'citation_verified': True, 'exact_match': 1, 'f1': 1.0, 'retrieval_replay_verified': True, 'abstained': False, 'answer_delivered': True, 'answer_sha256': 'e979074ebd5633a7263aa48c4b7dea055a769115fbc0a7de459f8a74cd2efd2b', 'quoted_span_verified': True, 'status': 'completed', 'model_calls_reserved': 2, 'tool_calls': 1, 'failure_code': None, 'completion_seconds': 43.982975, 'output_contract_rejections': 0, 'accepted_generated_tokens': 179}, {'run_id': 'agrun_79b1a3ce08dab3df1a0ac0750d2cb283', 'case_id': '56d70e570d65d2140019833e', 'passed': True, 'completed': True, 'answer_correct': True, 'required_tools': True, 'citation_verified': True, 'exact_match': 1, 'f1': 1.0, 'retrieval_replay_verified': True, 'abstained': False, 'answer_delivered': True, 'answer_sha256': '373ba82067e099495ea36f10dbf8061b58231e9e665cf605bcd2968e34e28833', 'quoted_span_verified': True, 'status': 'completed', 'model_calls_reserved': 2, 'tool_calls': 1, 'failure_code': None, 'completion_seconds': 45.963389, 'output_contract_rejections': 0, 'accepted_generated_tokens': 188}, {'run_id': 'agrun_7c6769c246f273c36f0168c975ebede1', 'case_id': '56f884cba6d7ea1400e17707', 'passed': False, 'completed': True, 'answer_correct': False, 'required_tools': True, 'citation_verified': True, 'exact_match': 0, 'f1': 0.6666666666666666, 'retrieval_replay_verified': True, 'abstained': False, 'answer_delivered': True, 'answer_sha256': 'b2e72d215aaa73e9336da5107e33e49f9da4ec3c7f375ed8cff5fe3955d91b27', 'quoted_span_verified': True, 'status': 'completed', 'model_calls_reserved': 2, 'tool_calls': 1, 'failure_code': None, 'completion_seconds': 45.36059, 'output_contract_rejections': 0, 'accepted_generated_tokens': 194}, {'run_id': 'agrun_d0441a6f8e46e00a4b389a8a93de7440', 'case_id': '572f6a0ba23a5019007fc5ec', 'passed': True, 'completed': True, 'answer_correct': True, 'required_tools': True, 'citation_verified': True, 'exact_match': 1, 'f1': 1.0, 'retrieval_replay_verified': True, 'abstained': False, 'answer_delivered': True, 'answer_sha256': '69cbc06e0c5b945e5b8145eb4f886d7cfab89ea6947e812c5529135582c36186', 'quoted_span_verified': True, 'status': 'completed', 'model_calls_reserved': 2, 'tool_calls': 1, 'failure_code': None, 'completion_seconds': 45.355426, 'output_contract_rejections': 0, 'accepted_generated_tokens': 184}, {'run_id': 'agrun_f9a0b1b21b36c98b0ed3b5cedcef82b9', 'case_id': '572751b4708984140094dc1c', 'passed': False, 'completed': True, 'answer_correct': False, 'required_tools': True, 'citation_verified': True, 'exact_match': 0, 'f1': 0.15384615384615385, 'retrieval_replay_verified': True, 'abstained': False, 'answer_delivered': True, 'answer_sha256': '9cf5e1863bf8b931e83b50d4b2e6ce5893eacf50abaf57292c1a29c3289ee749', 'quoted_span_verified': True, 'status': 'completed', 'model_calls_reserved': 2, 'tool_calls': 1, 'failure_code': None, 'completion_seconds': 43.518186, 'output_contract_rejections': 0, 'accepted_generated_tokens': 198}], 'events': 385, 'usage': {'prompt_tokens': 25583, 'completion_tokens': 1521}}
state={'passed':False,'phase':'prepared','new_gpu_tasks':0,'external_database_connections':0,'source_commit':'dc139375570339b4b109ed801887ca74756133be',
 'archive_sha256':EXPECTED['archive_sha256'],'scope':'Restore of stopped terminal qualification cluster from actual off-Pod Mac archive, not Azure restore, interrupted workflow resume or whole-Pod HA'}
child=None;childfd=None;began=time.monotonic()
def drop():os.setgroups([]);os.setgid(65534);os.setuid(65534)
def interrupted(*_):raise InterruptedError('Bounded restore interrupted')
signal.signal(signal.SIGTERM,interrupted);signal.signal(signal.SIGINT,interrupted)
def phase(name):state['phase']=name;write(BASE/'validation.json',state)
async def verify_state(dsn):
 pool=AsyncConnectionPool(dsn,min_size=1,max_size=2,open=False,kwargs={'autocommit':True,'row_factory':dict_row,'options':'-c default_transaction_read_only=on -c statement_timeout=5000 -c lock_timeout=1000'})
 await pool.open(wait=True)
 try:
  store=AgentStore(pool)
  async with pool.connection() as db:
   assert await verify_schema(db)==11;await verify_agents(db)
   assert (await (await db.execute('SHOW transaction_read_only')).fetchone())['transaction_read_only']=='on'
   assert (await (await db.execute('SHOW listen_addresses')).fetchone())['listen_addresses']==''
   rows=await (await db.execute('SELECT tenant_id,id FROM bf_agents.runs ORDER BY id')).fetchall()
   assert len(rows)==8 and len({r['tenant_id'] for r in rows})==1
   assert (await (await db.execute('SELECT count(*) n FROM public.tasks')).fetchone())['n']==0
   usage=await (await db.execute('SELECT active_runs,retained_runs FROM bf_agents.usage WHERE tenant_id=%s',(rows[0]['tenant_id'],))).fetchone()
   assert usage=={'active_runs':0,'retained_runs':8}
  definitions={r['run_id']:r for r in EXPECTED['records']};assert set(definitions)=={r['id'] for r in rows};records=[];usage={'prompt_tokens':0,'completion_tokens':0}
  cases={r['id']:r for r in __import__('ops.retrieval_squad',fromlist=['inputs']).inputs('retrieval-span-v1')[1]}
  case_map={r['run_id']:r for r in EXPECTED['task_results']}
  from ops.retrieval_squad import grade
  for r in rows:
   run=await store.get(r['tenant_id'],r['id']);events=await store.events(r['tenant_id'],r['id']);old=definitions[r['id']]
   assert run['status']==old['status']=='completed' and run['revision']==old['revision']==events['revision'] and run['event_hash']==old['event_head_sha256']==events['head_sha256']
   assert digest(run['state'])==old['state_sha256'] and digest(run['result'])==old['result_sha256']
   graded=grade(cases[case_map[r['id']]['case_id']],run)
   for key in ('passed','exact_match','f1','citation_verified','quoted_span_verified','answer_sha256'):assert graded[key]==case_map[r['id']][key]
   assert run['model_calls']==2 and run['tool_calls']==1
   assert len(run['state']['receipts'])==len(old['receipts'])==2
   for receipt in run['state']['receipts']:
    for k in usage:usage[k]+=receipt['usage'][k]
   assert run['generated_tokens']==sum(x['usage']['completion_tokens'] for x in run['state']['receipts'])
   try:await store.get('ungranted-restorer-audience',r['id'])
   except AgentError as exc:assert exc.status==404
   else:raise AssertionError('Tenant isolation failed')
   records.append({'run_id':r['id'],'revision':run['revision'],'event_head_sha256':run['event_hash'],'events':len(events['events']),
    'state_sha256':digest(run['state']),'result_sha256':digest(run['result']),'source_quote_verified':True})
  assert sum(r['events'] for r in records)==EXPECTED['events']==385 and usage==EXPECTED['usage']
  return {'runs':records,'event_count':385,'child_receipts':16,'accepted_usage':usage,'tenant_scope_verified':True,'all_source_proofs_and_quality_replayed':True,
    'schema_versions':{'public':11,'bf_agents':1},'native_control_counters':{'active':0,'retained':8},'database_read_only':True,'tcp_listener_enabled':False}
 finally:await pool.close()
try:
 lock=(ROOT/'.runtime/product-campaign.lock').open('a');fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
 pins=Pins(ROOT);pins.check(full=True);state['primary_before']=dict(pins.identities)
 raw=(BASE/'private-archive.tar.gz').read_bytes();assert hashlib.sha256(raw).hexdigest()==EXPECTED['archive_sha256'] and len(raw)==EXPECTED['compressed_bytes']
 assert not STAGE.exists();STAGE.mkdir(mode=0o700);os.chown(STAGE,65534,65534)
 with tarfile.open(fileobj=io.BytesIO(raw),mode='r:gz') as tar:
  manifest=json.load(tar.extractfile('private-manifest.json'));assert manifest['protocol']=='batchforge.stopped-pg-private-export.v2'
  allowed={'private-manifest.json'}|{'data/'+n for n in manifest['files']}|{'data' if n=='.' else 'data/'+n for n in manifest['directories']}
  assert len(tar.getmembers())==len(allowed) and {m.name for m in tar.getmembers()}==allowed
  for m in tar.getmembers():
   assert '..' not in Path(m.name).parts and not m.name.startswith('/') and (m.isfile() or m.isdir())
   assert m.isfile()==(m.name=='private-manifest.json' or m.name.removeprefix('data/') in manifest['files'])
  tar.extractall(STAGE,filter='data')
 for n in manifest['directories']:
  p=STAGE/'data' if n=='.' else STAGE/'data'/n;assert p.is_dir();p.chmod(0o700);os.chown(p,65534,65534)
 for n,info in manifest['files'].items():
  p=STAGE/'data'/n;assert p.is_file() and len(p.read_bytes())==info['bytes'] and hashlib.sha256(p.read_bytes()).hexdigest()==info['sha256'];p.chmod(0o600);os.chown(p,65534,65534)
 data=STAGE/'data';assert not (data/'postmaster.pid').exists() and (data/'PG_VERSION').read_text().strip()=='17'
 socket=STAGE/'socket';socket.mkdir(mode=0o700);os.chown(socket,65534,65534)
 tool=Path('/tmp/bf-ci-20261025-v14/toolchain');binary=tool/'usr/lib/postgresql/17/bin'
 env={'PATH':'/usr/bin:/bin','HOME':str(STAGE),'LANG':'C','LC_ALL':'C','LD_LIBRARY_PATH':str(tool/'usr/lib/x86_64-linux-gnu')}
 control=subprocess.run([str(binary/'pg_controldata'),str(data)],env=env,preexec_fn=drop,stdout=subprocess.PIPE,stderr=subprocess.PIPE,check=True,timeout=10).stdout.decode()
 assert any('Database cluster state:' in line and line.strip().endswith('shut down') for line in control.splitlines())
 state['all_archive_file_and_directory_digests_verified']=True;state['files']=len(manifest['files']);state['directories']=len(manifest['directories']);state['cold_cluster_state']='shut down';phase('native_starting')
 command=['/usr/bin/timeout','--signal=INT','--kill-after=10','120',str(binary/'postgres'),'-D',str(data),'-h','','-p','55487','-k',str(socket),
  '-c','shared_buffers=32MB','-c','max_connections=8','-c','default_transaction_read_only=on']
 with (BASE/'postgres-private.log').open('xb') as log:child=subprocess.Popen(command,env=env,cwd=STAGE,preexec_fn=drop,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
 childfd=os.pidfd_open(child.pid);state['timeout_identity']=process_identity(child.pid);state['independent_timeout_seconds']=120;phase('native_verifying')
 dsn='postgresql://agent_canary@/postgres?host='+str(socket)+'&port=55487';until=time.monotonic()+25
 while True:
  assert child.poll() is None
  try:
   with psycopg.connect(dsn,connect_timeout=2) as db:state['postgres_version']=db.execute('SELECT version()').fetchone()[0]
   break
  except psycopg.OperationalError:
   if time.monotonic()>until:raise
   time.sleep(.2)
 state['restored']=asyncio.run(verify_state(dsn));pins.check(full=True);assert pins.identities==state['primary_before'];state['primary_after']=dict(pins.identities)
 state['application_restore_verified']=True;phase('application_verified')
except BaseException as exc:state['error_type']=type(exc).__name__;state['phase_before_failure']=state['phase'];phase('failed')
finally:
 if child is not None and child.poll() is None:
  signal.pidfd_send_signal(childfd,signal.SIGINT)
  try:child.wait(timeout=25)
  except subprocess.TimeoutExpired:signal.pidfd_send_signal(childfd,signal.SIGKILL);child.wait(timeout=10)
 if childfd is not None:os.close(childfd)
 state['restored_pg_stopped']=child is None or child.poll() is not None
 state['postmaster_pid_absent']=not (STAGE/'data/postmaster.pid').exists();state['elapsed_seconds']=time.monotonic()-began
 state['passed']=bool(state.get('application_restore_verified') and state['restored_pg_stopped'] and state['postmaster_pid_absent'])
 if state['passed']:state['phase']='complete'
 write(BASE/'validation.json',state)
 print('BF_AGENT_RESTORE_RESULT',json.dumps({'passed':state['passed'],'phase':state['phase'],'error_type':state.get('error_type'),'events':state.get('restored',{}).get('event_count')}))
