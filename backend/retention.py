"""Translate database-owned retention admission failures without leaking SQL.

Limits live in tenant_retention_limits, never in per-API environment settings.
The feature flag is a minimum-schema gate, not a bypass for installed triggers.
"""
from functools import wraps

import psycopg


def retention_error(exc):
    code = exc.sqlstate
    if code == 'BQ429':
        if (exc.diag.message_primary or '').startswith('Auxiliary metadata quota reached:'):
            return (429, 'Storage metadata retention limit reached; an operator must retire history',
                'metadata_retention_limit')
        task_limit = exc.diag.message_primary == 'retained_task_limit'
        return (429, 'Retained task history limit reached; ask an operator to retire history' if task_limit
            else 'Retained payload budget reached; wait for verified archival or operator cleanup',
            'retained_task_limit' if task_limit else 'retained_payload_limit')
    if code == 'BP413':
        return 413, 'Payload exceeds its durable storage reservation bound', 'retention_payload_too_large'
    if code == 'BR409':
        return 409, 'Immutable stored identity or quota accounting', 'retention_conflict'
    if code == 'BR403':
        return 403, 'Retention policy and deletion require an operator', 'retention_operator_required'
    return None


def guard_retention_errors(error_type):
    def decorate(function):
        @wraps(function)
        async def guarded(*args, **kwargs):
            try:
                return await function(*args, **kwargs)
            except psycopg.Error as exc:
                translated = retention_error(exc)
                if translated is not None:
                    raise error_type(*translated) from None
                raise
        return guarded
    return decorate
