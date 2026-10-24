"""One negative HTTP admission probe within an exclusive operator workload window.

This never uploads and refuses POST when the snapshot shows adequate budget.
The server-side admission trigger remains authoritative. No lock spans HTTP. A future
archive grace horizon and the operator's no-manual-purge/no-other-submit window
are required to avoid turning a negative probe into accidental inference.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import time

import httpx
import psycopg
from psycopg.rows import dict_row


TIMEOUT = 15
MARGIN = 10
# This is a controlled private-API proof, not a distributed transaction. Leave
# substantial room for the external input GET and per-phase HTTP timeouts; the
# production policy uses120s after terminal, and manual purge is excluded.
MIN_ARCHIVE_HORIZON = 90


def save(path, value):
    temporary = path.with_suffix('.tmp')
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w') as stream:
        json.dump(value, stream, indent=2, default=str)
        stream.write('\n')
    temporary.replace(path)


def snapshot(dsn, run_id, key, cutoff, grace, max_attempts):
    """Read one repeatable SQL snapshot; return no prompt, response or credential."""
    with psycopg.connect(dsn, row_factory=dict_row, connect_timeout=5,
            options='-c default_transaction_read_only=on -c statement_timeout=5000') as conn:
        conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
        batches = conn.execute("""SELECT id,tenant_id,input_file_id,total_count,created_at
            FROM batches WHERE metadata->>'benchmark_run'=%s ORDER BY created_at,id""", (run_id,)).fetchall()
        if len(batches) != 5 or sum(b['total_count'] for b in batches) != 100:
            raise ValueError('Expected exactly five durable20-task batches')
        tenants = {b['tenant_id'] for b in batches}
        if len(tenants) != 1 or any(b['total_count'] != 20 for b in batches):
            raise ValueError('Expected a single tenant and20 tasks per input')
        tenant = next(iter(tenants)); bid = batches[0]['id']; fid = batches[0]['input_file_id']
        policy = conn.execute('SELECT * FROM tenant_retention_limits WHERE tenant_id=%s', (tenant,)).fetchone()
        usage = conn.execute('SELECT * FROM tenant_retention_usage WHERE tenant_id=%s', (tenant,)).fetchone()
        if not policy or not usage: raise ValueError('Schema9 retention ledger required')
        derived = conn.execute("""SELECT count(*) AS n,COALESCE(sum(retention_body_bytes+
            retention_response_bytes+retention_error_bytes),0) AS bytes FROM tasks WHERE tenant_id=%s""", (tenant,)).fetchone()
        selected = conn.execute("""SELECT count(*) AS n,COALESCE(sum(retention_body_bytes),0) AS body_bytes,
            bool_and(NOT retention_payload_cleared AND max_attempts=%s) AS intact
            FROM tasks WHERE tenant_id=%s AND batch_id=%s""", (max_attempts,tenant,bid)).fetchone()
        cohort = conn.execute("""SELECT count(*) AS n,md5(string_agg(id,',' ORDER BY id)) AS identity_hash,
            count(*) FILTER(WHERE status='completed') AS completed,
            count(*) FILTER(WHERE status IN ('failed','cancelled')) AS failed_cancelled
            FROM tasks WHERE tenant_id=%s AND batch_id=ANY(%s)""", (tenant,[b['id'] for b in batches])).fetchone()
        now = conn.execute('SELECT clock_timestamp() AS now').fetchone()['now']
        # A still-running batch cannot become eligible before now+grace. A
        # terminal batch uses the exact same terminal timestamp as archive_loop.
        archive = conn.execute("""SELECT min(CASE WHEN b.status='in_progress' THEN %s::timestamptz
              ELSE COALESCE(b.completed_at,b.failed_at,b.cancelled_at,b.created_at) END
              +make_interval(secs=>%s)) AS earliest
            FROM batches b WHERE b.tenant_id=%s AND b.created_at>=%s
              AND NOT EXISTS(SELECT 1 FROM result_archive_runs r WHERE r.tenant_id=b.tenant_id
                  AND r.batch_id=b.id AND r.purged_at IS NOT NULL)""", (now,grace,tenant,cutoff)).fetchone()
        active_archive = conn.execute("""SELECT count(*) AS n FROM result_archive_runs
            WHERE tenant_id=%s AND state IN ('building','ready') AND purged_at IS NULL""", (tenant,)).fetchone()['n']
        file = conn.execute("""SELECT kind,admission_tasks,storage_deleted_at,storage_expires_at,
            blob_id IS NOT NULL AS has_blob FROM files WHERE tenant_id=%s AND id=%s""", (tenant,fid)).fetchone()
        aux = conn.execute("""SELECT l.resource,l.max_rows,l.max_bytes,l.max_row_bytes,u.rows,u.bytes
            FROM tenant_auxiliary_limits l JOIN tenant_auxiliary_usage u USING(tenant_id,resource)
            WHERE l.tenant_id=%s AND l.resource IN ('idempotency','input_read_leases')""", (tenant,)).fetchall()
        residue = conn.execute("""SELECT
            (SELECT count(*) FROM idempotency WHERE tenant_id=%s AND scope='/v1/batches' AND key=%s) AS keys,
            (SELECT count(*) FROM batches WHERE tenant_id=%s AND metadata->>'backpressure_probe'=%s) AS batches,
            (SELECT count(*) FROM tasks t JOIN batches b ON b.id=t.batch_id
                WHERE b.tenant_id=%s AND b.metadata->>'backpressure_probe'=%s) AS tasks,
            (SELECT count(*) FROM attempts a JOIN batches b ON b.id=a.batch_id
                WHERE b.tenant_id=%s AND b.metadata->>'backpressure_probe'=%s) AS attempts""", (tenant,key,tenant,key,tenant,key,tenant,key)).fetchone()
        totals = conn.execute("""SELECT (SELECT count(*) FROM batches WHERE tenant_id=%s) AS batches,
            (SELECT count(*) FROM tasks WHERE tenant_id=%s) AS tasks""", (tenant,tenant)).fetchone()
        duplicates = conn.execute("""SELECT count(*) AS n FROM (SELECT a.task_id FROM attempts a
            WHERE a.batch_id=ANY(%s) AND a.status='completed' GROUP BY a.task_id HAVING count(*)>1) x""", ([b['id'] for b in batches],)).fetchone()['n']
        required = selected['body_bytes'] + 20 * (policy['max_response_bytes'] + (max_attempts+1)*policy['max_error_bytes'])
        return dict(at=now,tenant_id=tenant,input_file_id=fid,cohort=cohort,
            cohort_age_seconds=(now-min(b['created_at'] for b in batches)).total_seconds(),
            all_cohort_after_cutoff=all(b['created_at']>=cutoff for b in batches),
            selected=selected,required_payload_bytes=required,
            remaining_payload_bytes=policy['max_payload_bytes']-usage['charged_payload_bytes'],
            remaining_task_rows=policy['max_retained_tasks']-usage['retained_tasks'],
            ledger=usage,derived=derived,earliest_autoarchive_at=archive['earliest'],
            archive_horizon_seconds=(archive['earliest']-now).total_seconds() if archive['earliest'] else None,
            active_unpurged_archives=active_archive,file=file,auxiliary=aux,residue=residue,
            tenant_totals=totals,duplicate_accepted_tasks=duplicates)


def refusal_reason(before):
    """Fail closed before HTTP; preflight refusal is a skipped proof, not429."""
    if before['ledger']['retained_tasks'] != before['derived']['n'] or before['ledger']['charged_payload_bytes'] != before['derived']['bytes']:
        return 'ledger_not_derived'
    if any(before['residue'].values()): return 'existing_probe_no_replay'
    if before['cohort']['n'] != 100 or before['cohort']['failed_cancelled'] or before['duplicate_accepted_tasks']:
        return 'cohort_integrity'
    if not 0 <= before['cohort_age_seconds'] <= 60 or not before['all_cohort_after_cutoff']:
        return 'outside_fresh_cohort_window'
    if before['selected']['n'] != 20 or before['selected']['intact'] is not True:
        return 'selected_input_not_intact'
    if before['required_payload_bytes'] <= before['remaining_payload_bytes']:
        return 'headroom_adequate_no_post'
    if before['remaining_task_rows'] < 20: return 'task_limit_would_confound_payload_limit'
    if before['active_unpurged_archives'] or (before['archive_horizon_seconds'] or 0) <= MIN_ARCHIVE_HORIZON:
        return 'purge_could_release_budget'
    f = before['file']
    if not f or f['kind'] != 'input' or f['admission_tasks'] != 0 or f['storage_deleted_at'] is not None or not f['has_blob']:
        return 'existing_external_input_required'
    if f['storage_expires_at'] is None or (f['storage_expires_at']-before['at']).total_seconds() <= TIMEOUT+MARGIN:
        return 'input_expiry_window'
    resources = {r['resource']:r for r in before['auxiliary']}
    if set(resources) != {'idempotency','input_read_leases'}: return 'auxiliary_policy_missing'
    for name, r in resources.items():
        if r['rows'] >= r['max_rows']: return 'auxiliary_limit_would_confound'
        if name == 'idempotency' and r['max_bytes'] > 0 and r['bytes']+r['max_row_bytes'] > r['max_bytes']:
            return 'auxiliary_limit_would_confound'
    return None


def run_once(dsn, api_key, run_id, cutoff, grace, max_attempts, report, *, snapshot_fn=snapshot, post=None):
    """Exactly one POST or an explicit skip. Never retries a transport failure."""
    if not re.fullmatch(r'bench-[0-9a-f]{12}',run_id): raise ValueError('Exact benchmark run required')
    if cutoff.tzinfo is None or not 1 <= max_attempts <= 20 or grace < TIMEOUT+MARGIN:
        raise ValueError('Explicit future-cutoff policy and safe grace required')
    report=Path(report); report.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
    # Marker persists even if the caller dies after POST but before report save.
    marker=report.with_suffix('.started'); fd=os.open(marker,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600);os.close(fd)
    if report.exists(): raise ValueError('Existing report; never repeat this probe')
    key='retention-proof-'+run_id
    doc={'run_id':run_id,'idempotency_key':key,'phase':'preflight','started_at':datetime.now(timezone.utc),
         'scope':'functional negative admission probe; one POST; requires exclusive no-other-submit/no-manual-purge window; no DB lock across network',
         'limitations':['The insufficient-budget proof is one SQL snapshot, not an atomic HTTP dry run.',
             'HTTPX timeout is per phase, not an end-to-end bound; client timeout does not cancel server-side processing.',
             'The90-second archive horizon is a conservative risk gate, not a guarantee against an arbitrarily delayed S3 read or future purge.',
             'Unexpected success or transport/verification uncertainty fails this proof and is never retried.']}
    save(report,doc)
    before=snapshot_fn(dsn,run_id,key,cutoff,grace,max_attempts);doc['before']=before
    reason=refusal_reason(before)
    if reason:
        doc.update(phase='skipped_no_post',reason=reason);save(report,doc);return doc
    body={'input_file_id':before['input_file_id'],'endpoint':'/v1/chat/completions','completion_window':'24h',
          'metadata':{'backpressure_probe':key,'benchmark_run':run_id+'-negative'}}
    doc.update(phase='post_intent',request_body_sha256=hashlib.sha256(json.dumps(body,sort_keys=True).encode()).hexdigest());save(report,doc)
    # The snapshot has committed and its connection closed before this POST.
    started=time.monotonic()
    try:
        if post is None:
            with httpx.Client(timeout=TIMEOUT,trust_env=False,follow_redirects=False) as client:
                response=client.post('http://127.0.0.1:8080/v1/batches',json=body,
                    headers={'Authorization':'Bearer '+api_key,'Idempotency-Key':key})
        else: response=post(body,key)
        doc['http_status']=response.status_code
        try: code=response.json().get('error',{}).get('code')
        except (ValueError,AttributeError): code=None
        doc['error_code']=code
    except Exception as error:
        doc.update(phase='uncertain_no_retry',transport_error_type=type(error).__name__)
    doc['http_elapsed_seconds']=time.monotonic()-started
    if doc['phase']!='uncertain_no_retry':
        doc['phase']='http429_verification_pending' if doc.get('http_status')==429 and doc.get('error_code')=='retained_payload_limit' else 'unexpected_http_no_retry'
    # Preserve a known200/429 or transport error before any verification query.
    # A DB outage must never erase evidence of an already observed acceptance.
    doc['http_outcome_phase']=doc['phase'];save(report,doc)
    try:after=snapshot_fn(dsn,run_id,key,cutoff,grace,max_attempts)
    except Exception as error:
        doc.update(phase='verification_failed_no_retry',verification_error_type=type(error).__name__)
        save(report,doc);return doc
    doc['after']=after
    checks={'http429_payload_limit':doc.get('http_status')==429 and doc.get('error_code')=='retained_payload_limit',
        'no_probe_residue':not any(after['residue'].values()),
        'no_new_batch_or_task':after['tenant_totals']==before['tenant_totals'],
        'cohort_identity_unchanged':after['cohort']['identity_hash']==before['cohort']['identity_hash'] and after['cohort']['n']==100,
        'accepted_progress_conserved':after['cohort']['completed']>=before['cohort']['completed'] and not after['cohort']['failed_cancelled'] and after['duplicate_accepted_tasks']==0,
        'ledger_matches_derived':after['ledger']['retained_tasks']==after['derived']['n'] and after['ledger']['charged_payload_bytes']==after['derived']['bytes'],
        'no_budget_released_during_probe':after['ledger']==before['ledger']}
    doc['checks']=checks
    if doc['phase']!='uncertain_no_retry':doc['phase']='passed' if all(checks.values()) else 'failed_no_retry'
    doc['finished_at']=datetime.now(timezone.utc);save(report,doc);return doc
