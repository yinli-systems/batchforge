"""Answer-format validity precedes reference normalization; no answer repair."""
import math

POLICY='invalid-contract-zero-v1'


def enforce(valid,grade):
    if (type(valid) is not bool or not isinstance(grade,dict) or type(grade.get('exact_match')) is not int
        or grade['exact_match'] not in (0,1) or type(grade.get('f1')) not in (int,float)
        or not math.isfinite(grade['f1']) or not 0<=grade['f1']<=1):raise ValueError('Valid bounded reference grade required')
    # Normalizers can turn both a missing prediction and an article-only
    # reference into the empty string. Invalid protocol output still gets0.
    return dict(grade) if valid else {'exact_match':0,'f1':0.0}
