"""Describe real ABBA cohorts without treating task samples as independent runs."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scripts.benchmark import distribution


def summarize(report):
    phases=report['phases']
    groups={}
    for phase in phases:
        key=(phase['study'],phase['treatment'])
        groups.setdefault(key,[]).append(phase)
    result={'campaign_passed':report.get('passed') is True and not report.get('cleanup_error_type')
                               and bool(report.get('finished_at')),
            'completed_phases':len(phases),'planned_phases':len(report['plan']),
            'model':report['model'],'revision':report['revision'],'cost':None,
            'groups':[],'abba_blocks':[],
            'limitations':['One existing four-H100 Pod; no heterogeneous hardware or cross-node HA claim',
                'Task quantiles are descriptive mixed-workload quantiles; tasks within a burst are correlated',
                'Throughput is sum of accepted output tokens divided by sum of measured platform phase walls',
                'Allocation wall hours cover phase measurement windows, excluding model loading, inter-phase setup and faults',
                'No confidence interval, independent run significance, monetary savings or daily SLA is claimed']}
    for (study,treatment),rows in sorted(groups.items()):
        wall=sum(p['elapsed_seconds'] for p in rows)
        outputs=sum(p['audit']['completion_tokens'] for p in rows)
        observations=[t for p in rows for t in p['audit']['task_observations']]
        completion=[t.get('completion_seconds',t['queue_seconds']+t['accepted_attempt_seconds'])
                    for t in observations]
        result['groups'].append({'study':study,'treatment':treatment,'phases':len(rows),
            'tasks':sum(p['audit']['tasks'] for p in rows),'slots':sorted({p['slots'] for p in rows}),
            'policy':sorted({p['policy'] for p in rows}),
            'adaptive':sorted({p.get('adaptive',False) for p in rows}),
            'phase_wall_seconds':wall,'output_tokens':outputs,
            'output_tokens_per_second':outputs/wall,
            'allocation_wall_hours_per_million_output_tokens':wall/3600*1_000_000/outputs,
            'tasks_per_second':sum(p['audit']['tasks'] for p in rows)/wall,
            'task_completion_seconds':distribution(completion),
            'queue_seconds':distribution([t['queue_seconds'] for t in observations]),
            'accepted_attempt_seconds':distribution([t['accepted_attempt_seconds'] for t in observations]),
            'completion_population':'DB task created through accepted completion; legacy packs sum queue and accepted attempt'})
    for study in sorted({p['study'] for p in phases}):
        for repeat in sorted({p['repeat'] for p in phases if p['study']==study}):
            block=[p for p in phases if p['study']==study and p['repeat']==repeat]
            if [p['treatment'] for p in block]!=list('ABBA'):continue
            def rate(treatment):
                rows=[p for p in block if p['treatment']==treatment]
                return sum(p['audit']['completion_tokens'] for p in rows)/sum(p['elapsed_seconds'] for p in rows)
            a,b=rate('A'),rate('B')
            result['abba_blocks'].append({'study':study,'repeat':repeat,
                'a_output_tokens_per_second':a,'b_output_tokens_per_second':b,'b_over_a_throughput':b/a})
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('report',type=Path);parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    result=summarize(json.loads(args.report.read_text()))
    with args.output.open('x') as stream:json.dump(result,stream,indent=2);stream.write('\n')


if __name__=='__main__':main()
