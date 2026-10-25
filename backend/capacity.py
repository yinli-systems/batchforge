"""Owner-scoped logical admission headroom, never physical storage or GPU capacity."""

CAPACITY_SQL="""
SELECT l.max_retained_tasks,l.max_payload_bytes,l.max_response_bytes,l.max_error_bytes,
       u.retained_tasks,u.charged_payload_bytes,
       b.stored_bytes AS input_stored_bytes,b.reserved_bytes AS input_reserved_bytes
FROM tenant_retention_limits l JOIN tenant_retention_usage u USING(tenant_id)
JOIN tenant_blob_usage b USING(tenant_id) WHERE l.tenant_id=%s
"""


def capacity_view(row, settings):
    def quota(limit, used, reserved=0):
        return {'limit':int(limit),'used':int(used),'reserved':int(reserved),
                'available':max(0,int(limit)-int(used)-int(reserved)),
                'over_limit':int(used)+int(reserved)>int(limit)}
    return {'object':'workspace.capacity',
        'retained_tasks':quota(row['max_retained_tasks'],row['retained_tasks']),
        'task_payload_bytes':quota(row['max_payload_bytes'],row['charged_payload_bytes']),
        'input_bytes':quota(settings.max_stored_input_bytes_per_tenant,row['input_stored_bytes'],row['input_reserved_bytes']),
        'new_task_reservation':{
            'response_bytes':row['max_response_bytes'],
            'error_bytes_per_attempt':row['max_error_bytes'],
            'max_attempts':settings.max_attempts,
            'fixed_bytes_per_task':row['max_response_bytes']+(settings.max_attempts+1)*row['max_error_bytes'],
            'additional_body_bytes':'PostgreSQL canonical JSON bytes of each task body'},
        'snapshot_scope':'Logical tenant admission counters in one statement; not disk, WAL, object-store or GPU capacity',
        'advisory':True,
        'admission_note':'Concurrent submissions can consume headroom. The transaction at submission is authoritative.'}
