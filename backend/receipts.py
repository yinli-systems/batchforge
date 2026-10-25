"""Tenant-scoped, one-MVCC-snapshot execution ledger receipts.

Checksums make exports reproducible. They are not signatures, GPU attestation,
an object-store readback or a response-quality verdict. No payload bytes, lease
tokens or secrets are returned; inline input bytes may be hashed inside Postgres.
Retired history is explicitly distinguished from inconsistency.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json

PROTOCOL='batchforge.receipt.v1'
DOMAIN=(PROTOCOL+'\0').encode()

RECEIPT_SQL="""
WITH b AS (SELECT * FROM batches WHERE id=%s AND tenant_id=%s),
t AS (SELECT t.id,t.status,t.attempts,t.prompt_tokens,t.completion_tokens FROM tasks t
      JOIN b ON t.batch_id=b.id AND t.tenant_id=b.tenant_id),
a AS (SELECT a.task_id,a.status,a.prompt_tokens,a.completion_tokens,
             t.status AS task_status,t.prompt_tokens AS task_prompt_tokens,t.completion_tokens AS task_completion_tokens,
             a.scheduling_decision->>'runtime_fingerprint' AS runtime_fingerprint
      FROM attempts a JOIN t ON a.task_id=t.id JOIN b ON a.batch_id=b.id AND a.tenant_id=b.tenant_id),
ts AS (SELECT count(*) AS total,
      coalesce(sum(attempts),0)::bigint AS reported_attempts,
      count(*) FILTER(WHERE status='pending') AS pending,
      count(*) FILTER(WHERE status='running') AS running,
      count(*) FILTER(WHERE status='completed') AS completed,
      count(*) FILTER(WHERE status='failed') AS failed,
      count(*) FILTER(WHERE status='cancelled') AS cancelled,
      coalesce(sum(prompt_tokens),0)::bigint AS prompt_tokens,
      coalesce(sum(completion_tokens),0)::bigint AS completion_tokens FROM t),
ats AS (SELECT count(*) AS total,
      count(*) FILTER(WHERE status='completed') AS accepted,
      count(DISTINCT task_id) FILTER(WHERE status='completed') AS accepted_tasks,
      count(*) FILTER(WHERE status='completed' AND (task_status<>'completed' OR prompt_tokens<>task_prompt_tokens
                           OR completion_tokens<>task_completion_tokens)) AS row_usage_mismatches,
      coalesce(sum(prompt_tokens) FILTER(WHERE status='completed'),0)::bigint AS prompt_tokens,
      coalesce(sum(completion_tokens) FILTER(WHERE status='completed'),0)::bigint AS completion_tokens FROM a),
ac AS (SELECT task_id,count(*) AS records FROM a GROUP BY task_id),
history_check AS (SELECT count(*) FILTER(WHERE coalesce(ac.records,0)<t.attempts) AS tasks_missing_records,
                        count(*) FILTER(WHERE coalesce(ac.records,0)>t.attempts) AS tasks_with_excess_records
                 FROM t LEFT JOIN ac ON ac.task_id=t.id),
rg AS (SELECT runtime_fingerprint,count(*) AS accepted_attempts FROM a
      WHERE status='completed' GROUP BY runtime_fingerprint),
rt AS (SELECT * FROM rg ORDER BY runtime_fingerprint NULLS LAST LIMIT 32),
art AS (SELECT f.id,f.kind,f.bytes,f.storage_expires_at,f.storage_deleted_at,
      coalesce(af.sha256,bo.sha256,CASE WHEN f.kind='input' AND f.content IS NOT NULL
        THEN encode(sha256(f.content),'hex') END) AS sha256,
      CASE WHEN af.sha256 IS NOT NULL THEN 'published_archive_manifest'
           WHEN bo.sha256 IS NOT NULL THEN 'verified_upload_metadata'
           WHEN f.kind='input' AND f.content IS NOT NULL THEN 'database_bytes' ELSE 'not_published' END AS hash_source,
      ar.manifest_sha256 AS archive_manifest_sha256
      FROM files f JOIN b ON f.tenant_id=b.tenant_id AND f.id IN(b.input_file_id,b.output_file_id,b.error_file_id)
      LEFT JOIN blob_objects bo ON bo.id=f.blob_id AND bo.tenant_id=f.tenant_id
      LEFT JOIN result_archive_runs ar ON ar.batch_id=b.id AND ar.tenant_id=b.tenant_id AND ar.state='ready'
      LEFT JOIN result_archive_files af ON af.archive_id=ar.id AND af.file_id=f.id AND af.tenant_id=f.tenant_id)
SELECT to_jsonb(b) AS batch,to_jsonb(ts) AS tasks,to_jsonb(ats) AS attempts,
       (SELECT to_jsonb(history_check) FROM history_check) AS attempt_record_checks,
       coalesce((SELECT jsonb_agg(to_jsonb(rt)) FROM rt),'[]') AS runtimes,
       (SELECT count(*) FROM rg) AS runtime_groups,
       coalesce((SELECT jsonb_agg(to_jsonb(art) ORDER BY kind,id) FROM art),'[]') AS artifacts
FROM b CROSS JOIN ts CROSS JOIN ats
"""


def canonical(receipt):
    return json.dumps(receipt,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()


def verify(envelope):
    try:
        return (envelope.get('protocol')==PROTOCOL and
                hashlib.sha256(DOMAIN+canonical(envelope['receipt'])).hexdigest()==envelope.get('receipt_sha256'))
    except (KeyError,TypeError,ValueError,AttributeError):return False


def format_receipt(row):
    b,t,a=row['batch'],row['tasks'],row['attempts']
    retired={k:int(b.get('history_retired_'+k,0)) for k in ['tasks','completed','failed','cancelled','prompt_tokens','completion_tokens']}
    counts={k:int(b[k+'_count']) for k in ['total','pending','running','completed','failed','cancelled']}
    reconciled={k:int(t[k])+retired.get(k,0) for k in counts}
    reconciled['total']=int(t['total'])+retired['tasks']
    counts_match=counts==reconciled
    usage={k:int(b[k]) for k in ['prompt_tokens','completion_tokens']}
    task_usage={k:int(t[k])+retired[k] for k in usage}
    task_metering_match=usage==task_usage
    duplicate_acceptance=int(a['accepted'])!=int(a['accepted_tasks'])
    live_history_complete=int(a['accepted_tasks'])==int(t['completed'])
    records=row.get('attempt_record_checks',{})
    missing_records=int(records.get('tasks_missing_records',0))
    excess_records=int(records.get('tasks_with_excess_records',0))
    attempt_history_checked='attempt_record_checks' in row
    attempt_counts_match=attempt_history_checked and missing_records==0 and excess_records==0
    attempt_metering_match=all(int(a[k])==int(t[k]) for k in usage)
    inconsistent=(not counts_match or not task_metering_match or duplicate_acceptance or excess_records>0 or int(a['row_usage_mismatches'])>0 or
                  (live_history_complete and not attempt_metering_match))
    state=('inconsistent' if inconsistent else 'incomplete_history' if retired['tasks'] or not live_history_complete or not attempt_counts_match
           else 'in_progress' if b['status']=='in_progress' else 'consistent_ledger')
    receipt={'batch_id':b['id'],'model':b['model'],'status':b['status'],'created_at':b['created_at'],
        'counts':counts,'accepted_usage':usage,'ledger_state':state,
        'checks':{'batch_and_task_counts_match':counts_match,'batch_and_task_usage_match':task_metering_match,
                  'no_duplicate_accepted_attempts_observed':not duplicate_acceptance,
                  'live_completed_tasks_have_accepted_attempts':live_history_complete,
                  'live_task_attempt_counts_match':attempt_counts_match,
                  'live_task_and_attempt_rows_match':int(a['row_usage_mismatches'])==0,
                  'live_task_and_accepted_attempt_usage_match':attempt_metering_match},
        'history':{'live_tasks':int(t['total']),'retired_tasks':retired['tasks'],
                   'observed_attempts':int(a['total']),'observed_accepted_attempts':int(a['accepted']),
                   'reported_attempts':int(t['reported_attempts']) if 'reported_attempts' in t else None,
                   'tasks_missing_attempt_records':missing_records if attempt_history_checked else None,
                   'tasks_with_excess_attempt_records':excess_records if attempt_history_checked else None,
                   'full_attempt_history_available':not retired['tasks'] and live_history_complete and attempt_counts_match},
        'runtime_fingerprint':b.get('runtime_fingerprint'),
        'runtime_compatibility_scope':b.get('runtime_compatibility_scope','legacy_unverified'),
        'reported_runtimes':row['runtimes'],'runtime_list_truncated':int(row['runtime_groups'])>32,
        'artifacts':row['artifacts'],'quality':{'state':'not_evaluated_by_this_receipt'},
        'verification_scope':['Single PostgreSQL statement snapshot, including retained-history counters',
            'Runtime fingerprints are worker-reported, not independent hardware attestation',
            'Artifact hashes are recorded metadata; verify downloaded bytes separately',
            'SHA-256 checksum is not an issuer signature; no monetary cost is inferred']}
    return {'protocol':PROTOCOL,'receipt':receipt,
            'receipt_sha256':hashlib.sha256(DOMAIN+canonical(receipt)).hexdigest(),
            'captured_at':datetime.now(timezone.utc).isoformat()}


TASK_PAGE_SQL="""
WITH b AS (SELECT * FROM batches WHERE id=%s AND tenant_id=%s),
anchor AS (SELECT t.id FROM tasks t JOIN b ON t.batch_id=b.id AND t.tenant_id=b.tenant_id WHERE t.id=%s),
page AS (SELECT t.id,t.custom_id,t.model,t.status,t.attempts,t.max_attempts,t.prompt_tokens,t.completion_tokens,
        t.created_at,t.finished_at,t.worker_id,t.runtime_fingerprint,
        extract(epoch FROM(t.finished_at-t.created_at))::float8 AS completion_seconds
        FROM tasks t JOIN b ON t.batch_id=b.id AND t.tenant_id=b.tenant_id
        WHERE (%s::text IS NULL OR t.id>(SELECT id FROM anchor)) ORDER BY t.id LIMIT %s)
SELECT b.id AS batch_id,coalesce((to_jsonb(b)->>'history_retired_tasks')::bigint,0) AS retired_tasks,
       EXISTS(SELECT 1 FROM anchor) AS cursor_found,
       coalesce((SELECT jsonb_agg(to_jsonb(page) ORDER BY id) FROM page),'[]') AS data FROM b
"""
