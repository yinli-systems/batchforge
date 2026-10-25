"""Add a separate, source-pinned Pod-window cost allocation; never rewrite evidence."""
from __future__ import annotations
import argparse
from datetime import datetime,timezone
from decimal import Decimal,InvalidOperation
import hashlib,json
from pathlib import Path


def allocate(report,rate,*,scores=None):
    rate=Decimal(str(rate))
    elapsed=Decimal(str(report.get('elapsed_seconds',report.get('elapsed_seconds_including_disconnect_and_validation'))))
    if not rate.is_finite() or not elapsed.is_finite() or rate<=0 or elapsed<=0:
        raise ValueError('Positive finite confirmed rate and measured wall interval required')
    amount=rate*elapsed/Decimal(3600)
    quality=report.get('protocol')=='squad-1.1-em-f1-v1'
    delivered=report.get('delivery_passed') is True if quality else report.get('passed') is True
    count=report.get('tasks') if quality else report.get('completed_tasks')
    if type(count) is not int or count<0:raise ValueError('Measured task count required')
    result={'currency':'USD','hourly_rate':str(rate),'measured_wall_seconds':str(elapsed),
        'pod_window_allocation_usd':str(amount),'delivered_results':count if delivered else None,
        'allocation_usd_per_delivered_result':str(amount/count) if delivered and count else None,
        'quality_scored':quality,'quality_cohort_gate_passed':report.get('passed') if quality else None,
        'on_time_reference_exact_results':None,'allocation_usd_per_on_time_reference_exact_result':None,
        'gate_passing_cohorts':int(report.get('passed') is True) if quality else None,
        'allocation_usd_per_gate_passing_cohort':str(amount) if quality and report.get('passed') is True else None,
        'measured_invoice_usd':None,
        'basis':'Operator-confirmed hourly rate assumed constant over the measured whole-Pod window; not an invoice, marginal cost or additive allocation across overlapping workloads',
        'excluded_or_unverified':['Provider billing rounding, discounts, taxes, separate storage/network charges and time outside the stated measurement interval']}
    if quality and scores is not None:
        if report.get('evaluation_valid') is not True or len(scores)!=count or len({r['id'] for r in scores})!=count:
            raise ValueError('Complete unique validated reference scores required')
        if any(type(r.get('exact_match')) is not int or r['exact_match'] not in (0,1) for r in scores):
            raise ValueError('Reference exact counts must be integral')
        exact=sum(r['exact_match'] for r in scores)
        if abs(Decimal(str(report['exact_match_percent']))-Decimal(exact)*100/count)>Decimal('0.0000001'):
            raise ValueError('Scores differ from the pinned quality report')
        on_time=exact if report.get('deadline_met') is True and delivered else 0
        result.update(on_time_reference_exact_results=on_time,
            allocation_usd_per_on_time_reference_exact_result=str(amount/on_time) if on_time else None)
    return result


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--report',type=Path,required=True)
    p.add_argument('--scores',type=Path);p.add_argument('--hourly-rate',required=True);p.add_argument('--out',type=Path,required=True)
    p.add_argument('--rate-source',required=True);args=p.parse_args()
    raw=args.report.read_bytes();scores_raw=args.scores.read_bytes() if args.scores else None
    result=allocate(json.loads(raw),args.hourly_rate,scores=[json.loads(x) for x in scores_raw.splitlines()] if scores_raw else None)
    result.update(captured_at=datetime.now(timezone.utc).isoformat(),rate_source=args.rate_source,
        source_report_sha256=hashlib.sha256(raw).hexdigest(),source_scores_sha256=hashlib.sha256(scores_raw).hexdigest() if scores_raw else None,
        allocator_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),original_report_modified=False)
    with args.out.open('x') as f:json.dump(result,f,indent=2,allow_nan=False);f.write('\n')
    print(json.dumps({k:result[k] for k in ['pod_window_allocation_usd','allocation_usd_per_delivered_result',
        'on_time_reference_exact_results','allocation_usd_per_on_time_reference_exact_result','gate_passing_cohorts']}))


if __name__=='__main__':main()
