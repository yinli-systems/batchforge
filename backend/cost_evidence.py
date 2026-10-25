"""Validate an operator-rate allocation against its pinned quality report.

No invoice verification or provider pricing lookup is performed. Only measured
whole-Pod wall time is allocated; overlapping intervals are not additive.
"""
from decimal import Decimal,InvalidOperation
import re


def allocation_view(raw,report,report_sha256):
    if (not isinstance(raw,dict) or raw.get('source_report_sha256')!=report_sha256
        or raw.get('currency')!='USD' or raw.get('measured_invoice_usd') is not None
        or raw.get('original_report_modified') is not False):
        raise ValueError('Pinned report and explicit non-invoice allocation required')
    def decimal(value):
        if type(value) not in (str,int,float):raise ValueError('Finite allocation number required')
        try:d=Decimal(str(value))
        except InvalidOperation:raise ValueError('Invalid allocation number') from None
        if not d.is_finite() or d<0:raise ValueError('Finite nonnegative allocation required')
        return d
    def equal(value,expected):
        if abs(decimal(value)-expected)>Decimal('0.000000000001'):raise ValueError('Allocation arithmetic differs')
    rate=decimal(raw.get('hourly_rate'));seconds=decimal(report['elapsed_seconds'])
    if not 0<rate<=100000 or seconds<=0:raise ValueError('Bounded positive rate/time required')
    equal(raw.get('measured_wall_seconds'),seconds)
    amount=rate*seconds/3600;tasks=report['tasks']
    equal(raw.get('pod_window_allocation_usd'),amount)
    equal(raw.get('allocation_usd_per_delivered_result'),amount/tasks)
    if type(raw.get('delivered_results')) is not int or raw['delivered_results']!=tasks:raise ValueError('Delivered allocation count differs')
    exact=round(decimal(report['exact_match_percent'])*tasks/100) if report['deadline_met'] else 0
    if type(raw.get('on_time_reference_exact_results')) is not int or raw['on_time_reference_exact_results']!=exact:
        raise ValueError('Reference-exact allocation count differs')
    if exact:equal(raw.get('allocation_usd_per_on_time_reference_exact_result'),amount/exact)
    elif raw.get('allocation_usd_per_on_time_reference_exact_result') is not None:raise ValueError('Zero exact results have no unit cost')
    if (raw.get('quality_scored') is not True or raw.get('quality_cohort_gate_passed') is not report['passed']
        or type(raw.get('gate_passing_cohorts')) is not int or raw['gate_passing_cohorts']!=int(report['passed'])):
        raise ValueError('Quality-cohort allocation state differs')
    if report['passed']:equal(raw.get('allocation_usd_per_gate_passing_cohort'),amount)
    elif raw.get('allocation_usd_per_gate_passing_cohort') is not None:raise ValueError('Failed cohort has no passing-cohort cost')
    for k in ('source_scores_sha256','allocator_sha256'):
        if not isinstance(raw.get(k),str) or not re.fullmatch('[a-f0-9]{64}',raw[k]):raise ValueError('Allocation source hashes required')
    return {'kind':'operator_rate_time_allocation','currency':'USD','hourly_rate_usd':str(rate),
        'whole_pod_window_seconds':str(seconds),'whole_pod_window_usd':str(amount),
        'usd_per_delivered_result':str(amount/tasks),'on_time_reference_exact_results':exact,
        'usd_per_on_time_reference_exact_result':str(amount/exact) if exact else None,
        'measured_invoice_usd':None,
        'scope':'Operator-confirmed rate applied to measured whole-Pod time; not an invoice, marginal cost or additive across overlapping workloads; excludes unverified tax/storage/network charges'}
