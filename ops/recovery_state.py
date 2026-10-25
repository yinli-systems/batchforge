"""Hash-only handoff from a finite observer to the locked native recovery CLI."""
from __future__ import annotations
import hashlib,json


def state_document(manifest,configs,receipt,manager_sources,identities):
    digest=lambda raw:hashlib.sha256(raw).hexdigest()
    return {'protocol':'batchforge.recovery-state.v1','manifest_sha256':digest(manifest),
        'configuration_sha256':{str(p):digest(raw) for p,raw in configs.items()},
        'source_receipt_sha256':digest(receipt),
        'manager_source_sha256':{str(p):sha for p,sha in manager_sources.items()},
        'identities':identities}


def fingerprint(state):
    return hashlib.sha256(json.dumps(state,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()


def require_expected(state,expected):
    actual=fingerprint(state)
    if expected is not None and expected!=actual:
        raise ValueError('Locked recovery state differs from the observer pinned state')
    return actual
