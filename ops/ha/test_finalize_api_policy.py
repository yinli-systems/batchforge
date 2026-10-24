from datetime import datetime,timedelta,timezone
import hashlib
import json
from pathlib import Path

import pytest

from backend.migrate import MIGRATIONS
from ops.ha.finalize_api_policy import (CHANGED_KEYS,MODEL,FinalPolicyRoll,configured_models,
    cutoff_utc,policy_bytes,validate_proofs,values)
from ops.ha.set_scheduler_policy import atomic_bytes
from ops.supervise import private_write

NOW=datetime(2026,10,24,6,tzinfo=timezone.utc)
CUTOFF=NOW+timedelta(minutes=5)


def original(default='old-default',extra='old-extra'):
    return (f"# private configuration\nMODEL_NAME={default}\nMODEL_NAMES_JSON='[\"{extra}\",\"{MODEL}\"]'\n"
        "SCHEDULER_POLICY=cost_aware\nBLOB_STORAGE_BACKEND=s3\nRESULT_READERS_ENABLED=true\n"
        "RESULT_ARCHIVE_ENABLED=false\nDATABASE_URL='private-url-value'\nSECRET='literal $() and spaces'\n").encode()


def proofs(tmp_path):
    snapshot='a'*64
    records={'backup':{'snapshot_sha256':snapshot,'archive_list_verified':True,'database_modified':False},
        'restore':{'passed':True,'restore_verified':True,'schema_version':8,'snapshot_sha256':snapshot,
            'temporary_cluster_stopped':True,'temporary_cluster_removed':True,
            'migration_checksums':{str(v):sha for v,_,sha in MIGRATIONS if v<=8},
            **{k:0 for k in ['batch_count_state_usage_mismatches','accepted_attempt_count_usage_mismatches',
                'tenant_queue_pending_mismatches','unvalidated_constraints']}},
        'production':{'passed':True,'restore_verified_before_purge':True,'backup_sha256':snapshot,
            'task_payloads_cleared':20,'attempt_counts_and_usage_unchanged':True,
            'batch_counters_and_identity_unchanged':True,'byte_identical_on_ports':[8080,8082,8083],
            'download_bytes':52200,'output_sha256':'b'*64,'output_file_id':'file-old20',
            'archive_publication_still_disabled':True,'steps':[{'purged':True,'rows_purged':20}]}}
    if MIGRATIONS[-1][0]>8:
        records['current_backup']={'snapshot_sha256':'c'*64,'archive_list_verified':True,'database_modified':False}
        records['current_restore']=dict(records['restore'],schema_version=MIGRATIONS[-1][0],
            snapshot_sha256='c'*64,migration_checksums={str(v):sha for v,_,sha in MIGRATIONS},
            retention_ledger_mismatches=0,retention_undercharged_tasks=0,
            retention_missing_tenant_ledgers=0,auxiliary_ledger_mismatches=0)
    paths={name:tmp_path/(name+'.json') for name in records}
    for name,path in paths.items():private_write(path,records[name])
    names=['restore','production']+(['current_restore'] if MIGRATIONS[-1][0]>8 else [])
    hashes={name:hashlib.sha256(paths[name].read_bytes()).hexdigest() for name in names}
    return paths,hashes


def test_only_allowed_environment_fields_change_preserving_all_legacy_ids():
    raw=original();changed=policy_bytes(raw,['old-default','old-extra',MODEL,'other-api-legacy'],CUTOFF)
    before,after=values(raw),values(changed)
    assert {k:v for k,v in before.items() if k not in CHANGED_KEYS}=={k:v for k,v in after.items() if k not in CHANGED_KEYS}
    assert before['MODEL_NAME']=='old-default' and after['MODEL_NAME']==MODEL
    assert set(configured_models(after))=={'old-default','old-extra','other-api-legacy',MODEL}
    assert after['RESULT_ARCHIVE_ENABLED']=='true' and after['RESULT_ARCHIVE_PURGE_ROWS']=='1000'
    assert after['RESULT_ARCHIVE_AFTER']==CUTOFF.isoformat() and b"SECRET='literal $() and spaces'\n" in changed


@pytest.mark.parametrize('raw',[original()+b'RESULT_ARCHIVE_ENABLED=false\n',original().replace(b'cost_aware',b'fair'),
    original().replace(b'RESULT_READERS_ENABLED=true',b'RESULT_READERS_ENABLED=false')])
def test_ambiguous_or_unreleased_environment_is_refused(raw):
    with pytest.raises(ValueError):policy_bytes(raw,['old-default'],CUTOFF)


def test_cutoff_is_explicit_utc_and_catalog_never_silently_drops_full_list():
    with pytest.raises(ValueError):cutoff_utc('2026-10-24T07:00:00')
    with pytest.raises(ValueError):cutoff_utc('2026-10-24T07:00:00+01:00')
    assert cutoff_utc('2026-10-24T07:00:00Z').tzinfo is not None
    with pytest.raises(ValueError):policy_bytes(original(),['legacy-'+str(i) for i in range(16)],CUTOFF)


def test_hash_pinned_proofs_crosscheck_snapshot_and_completed_exact20_purge(tmp_path):
    paths,hashes=proofs(tmp_path)
    assert validate_proofs(paths,hashes)['snapshot_sha256']=='a'*64
    with pytest.raises(ValueError):validate_proofs(paths,{'restore':hashes['restore']})
    records=json.loads(paths['production'].read_text());records['backup_sha256']='c'*64;private_write(paths['production'],records)
    with pytest.raises(ValueError,match='SHA differs'):validate_proofs(paths,hashes)
    hashes['production']=hashlib.sha256(paths['production'].read_bytes()).hexdigest()
    with pytest.raises(ValueError,match='production archival'):validate_proofs(paths,hashes)


def test_new_schema_requires_an_independently_verified_current_restore(tmp_path):
    if MIGRATIONS[-1][0]<=8:pytest.skip('Requires an additive schema after the archival proof')
    paths,hashes=proofs(tmp_path)
    assert validate_proofs(paths,hashes)['current_schema_snapshot_sha256']=='c'*64
    old_only={k:v for k,v in paths.items() if not k.startswith('current_')}
    with pytest.raises(ValueError,match='reviewed'):validate_proofs(old_only,hashes)
    current=json.loads(paths['current_restore'].read_text())
    current['migration_checksums'].pop(str(MIGRATIONS[-1][0]))
    private_write(paths['current_restore'],current)
    hashes['current_restore']=hashlib.sha256(paths['current_restore'].read_bytes()).hexdigest()
    with pytest.raises(ValueError,match='current-schema'):validate_proofs(paths,hashes)


class Manager:
    def __init__(self,fail=None):self.calls=[];self.fail=fail;self.nextpid=100
    def invoke(self,service,action):
        self.calls.append((service['role'],service['name'],action))
        if self.fail==(service['name'],action):self.fail=None;return None,'injected_failure'
        self.nextpid+=action=='start'
        return {'status':'stopped' if action=='stop' else 'started','running':True,'pid':self.nextpid},None
    def readiness(self,service):return True


def prepared(tmp_path,fail=None,environment_reader=None,archive_reader=None):
    runtime=tmp_path/'.runtime';runtime.mkdir();paths,hashes=proofs(runtime)
    services=[]
    for role,name,port in [('api','a',8082),('api','b',8083),('engine','qwen235b',8335),('worker','qwen235b',9106),('gateway','edge',8080)]:
        env=runtime/(role+'-'+name+'.env');atomic_bytes(env,original('old-'+name,'legacy-'+name))
        if role=='engine':atomic_bytes(env,(f'MODEL_NAME={MODEL}\nENGINE_REVISION=e156cb4efae43fbee1a1ab073f946a1377e6b969\nENGINE_TENSOR_PARALLEL_SIZE=4\nENGINE_MAX_MODEL_LEN=18440\n').encode())
        if role=='worker':
            profile={'profile_id':'qwen235-tested','runtime_fingerprint':'sha256:'+'d'*64,
                'prefill_tokens_per_second':100,'decode_tokens_per_second':10,'hourly_cost_usd':1,
                'calibration_label':'actual-tested','cost_source':'operator-allocation'}
            atomic_bytes(env,(f'MODEL_NAME={MODEL}\nINFERENCE_BASE_URLS=http://127.0.0.1:8335\nENGINE_MAX_MODEL_LEN=18440\nWORKER_MAX_INFLIGHT=16\nWORKER_TOKEN_BUDGET=295040\nWORKER_ID=primary-qwen235b\nSCHEDULER_PROFILE_JSON=\''+json.dumps(profile)+"'\n").encode())
        services.append({'role':role,'name':name,'port':port,'env_file':str(env)})
    watch_env=runtime/'watchdog.env';atomic_bytes(watch_env,b'SUPERVISOR_MANIFEST=private\n')
    manifest=runtime/'services.json';private_write(manifest,{'services':services})
    manager=Manager(fail)
    watcher={'role':'supervisor','name':'primary','env_file':str(watch_env)}
    roll=FinalPolicyRoll(root=tmp_path,services=services,watchdog=watcher,manifest_path=manifest,
        report_path=runtime/'report.json',cutoff=CUTOFF,clock=lambda:NOW,proof_paths=paths,proof_hashes=hashes,
        access_env=runtime/'not-needed.env',manager=manager,assert_drained=lambda:None,
        identity=lambda row:{'pid':123,'start_ticks':'456','cwd':str(tmp_path)},
        environment_reader=environment_reader or (lambda row:values(Path(row['env_file']).read_bytes())),
        catalog_reader=lambda row:configured_models(values(Path(row['env_file']).read_bytes())),
        archive_reader=archive_reader or (lambda row:{'bytes':52200,'sha256':'b'*64}))
    return roll,manager


def test_success_observes_both_old_readers_then_rolls_apis_only_and_restores_watchdog(tmp_path):
    roll,manager=prepared(tmp_path);report=roll.apply()
    assert report['phase']=='complete' and report['watchdog_restored']
    assert len(report['both_existing_leased_readers_observed'])==len(report['live_verifications_after'])==2
    assert all(role in {'api','supervisor'} for role,_,_ in manager.calls)
    assert manager.calls[-2:]==[('supervisor','primary','start'),('supervisor','primary','status')]
    assert all(values(path.read_bytes())['MODEL_NAME']==MODEL for path in roll.original)
    assert all(hashlib.sha256(path.read_bytes()).hexdigest()==sha for path,sha in roll.protected_env.items())
    assert 'private-url-value' not in json.dumps(report) and 'literal $()' not in json.dumps(report)


@pytest.mark.parametrize('failure',[('primary','stop'),('b','start')])
def test_partial_roll_failure_restores_both_exact_original_files_and_watchdog(tmp_path,failure):
    roll,manager=prepared(tmp_path,fail=failure)
    with pytest.raises(RuntimeError):roll.apply()
    assert roll.report['phase']=='rolled_back' and roll.report['watchdog_restored']
    assert all(path.read_bytes()==raw for path,raw in roll.original.items())
    assert manager.calls[-2:]==[('supervisor','primary','start'),('supervisor','primary','status')]


def test_stale_live_reader_or_bad_archive_refuses_before_any_stop(tmp_path):
    def bad(row):return {**values(Path(row['env_file']).read_bytes()),'RESULT_READERS_ENABLED':'false'}
    roll,manager=prepared(tmp_path,environment_reader=bad)
    with pytest.raises(RuntimeError):roll.apply()
    assert not manager.calls


def test_near_or_past_cutoff_and_busy_queue_refuse_before_any_stop(tmp_path):
    roll,manager=prepared(tmp_path)
    roll.clock=lambda:CUTOFF-timedelta(seconds=119)
    with pytest.raises(RuntimeError,match='120seconds'):roll.apply()
    assert not manager.calls
    roll.clock=lambda:NOW
    def busy():raise RuntimeError('active')
    roll.assert_drained=busy
    with pytest.raises(RuntimeError,match='active'):roll.apply()
    assert not manager.calls


def test_foreign_api_environment_change_is_never_overwritten_in_rollback(tmp_path):
    roll,manager=prepared(tmp_path)
    def foreign(service):
        if service['name']=='b':
            atomic_bytes(Path(service['env_file']),Path(service['env_file']).read_bytes()+b'FOREIGN=operator\n')
            raise RuntimeError('foreign mutation')
        return {'bytes':52200,'sha256':'b'*64}
    # Only inject after the first API has rolled; preflight must still pass.
    original=roll.verify_policy
    def verify(service):
        if service['name']=='b':roll.archive_reader=foreign
        original(service)
    roll.verify_policy=verify
    with pytest.raises(RuntimeError):roll.apply()
    assert roll.report['phase']=='manual_recovery_required' and roll.report['watchdog_restored']
    assert b'FOREIGN=operator' in Path(roll.apis[1]['env_file']).read_bytes()
