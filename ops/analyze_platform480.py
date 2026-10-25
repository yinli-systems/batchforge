"""Describe real ABBA cohorts without treating task samples as independent runs."""
from __future__ import annotations

import argparse
import csv
from datetime import datetime
import json
import math
from pathlib import Path
import re

from scripts.benchmark import distribution


def workload_strata(observations):
    groups={};unknown=0
    for row in observations:
        cell=re.search(r'-mix-\d+-(128|512|2048|8192|16384)-(32|128|512|2048)$',row.get('custom_id',''))
        if not cell:unknown+=1;continue
        prompt,output=map(int,cell.groups())
        label=('prefill_heavy' if prompt>=8192 and output<=128 else
               'decode_heavy' if prompt<=512 and output>=512 else 'other')
        groups.setdefault(label,[]).append(row)
    return {'population':'Requested token cells within the same mixed-arrival bursts; not isolated workload-class trials',
        'unclassified_observations':unknown,'classes':[{ 'class':label,'tasks':len(rows),
            'task_completion_seconds':distribution([r.get('completion_seconds',r['queue_seconds']+r['accepted_attempt_seconds']) for r in rows]),
            'queue_seconds':distribution([r['queue_seconds'] for r in rows]),
            'accepted_attempt_seconds':distribution([r['accepted_attempt_seconds'] for r in rows])}
            for label,rows in sorted(groups.items())]}


def telemetry_by_phase(root, report):
    """NVML samples inside measured request windows; never fill gaps with zero."""
    path=root/'gpu-engine-samples.jsonl'
    if not path.exists():return []
    samples=[json.loads(line) for line in path.read_text().splitlines()]
    result=[]
    for phase in report['phases']:
        label=f'{phase["index"]:02d}-{phase["study"]}-{phase["treatment"]}'
        bench=json.loads((root/label/'benchmark.json').read_text())
        start=datetime.fromisoformat(bench['measurement_started_at'])
        end=datetime.fromisoformat(bench['terminal_observation_ended_at'])
        selected=[s for s in samples if start<=datetime.fromisoformat(s['at'])<=end]
        devices={};invalid=0;running=[];kv=[];paused=[];preemptions=[]
        for row in selected:
            for line in row.get('gpu_csv',[]):
                try:
                    fields=next(csv.reader([line],skipinitialspace=True))
                    index=int(fields[0]);memory,util,power=map(float,[fields[4],fields[5],fields[6]])
                    if not all(math.isfinite(v) for v in [memory,util,power]) or not 0<=util<=100:
                        raise ValueError('invalid NVML sample')
                    devices.setdefault(index,[]).append((memory,util,power))
                except (ValueError,IndexError):invalid+=1
            for field,prefix,output in [('engine_metrics','vllm:num_requests_running',running),
                    ('engine_metrics','vllm:num_preemptions_total',preemptions),
                    ('worker_metrics','batchforge_worker_reserved_context_tokens',kv),
                    ('worker_metrics','batchforge_worker_adaptive_kv_paused',paused)]:
                for line in row.get(field,[]):
                    parts=line.split()
                    if len(parts)==2 and parts[0].split('{')[0]==prefix:
                        try:value=float(parts[1])
                        except ValueError:continue
                        if math.isfinite(value):output.append(value)
        intervals=[b['monotonic']-a['monotonic'] for a,b in zip(selected,selected[1:])]
        result.append({'index':phase['index'],'study':phase['study'],'treatment':phase['treatment'],
            'samples':len(selected),'invalid_gpu_rows':invalid,
            'sampling_gaps_over_5_seconds':sum(v>5 for v in intervals),
            'max_sampling_interval_seconds':max(intervals,default=None),
            'max_engine_running_requests':max(running,default=None),
            'engine_preemptions_first_counter':preemptions[0] if preemptions else None,
            'engine_preemptions_last_counter':preemptions[-1] if preemptions else None,
            'peak_worker_reserved_tokens':max(kv,default=None),'peak_worker_admission_paused':max(paused,default=None),
            'gpu':[{'index':index,'samples':len(rows),'mean_utilization_percent':sum(r[1] for r in rows)/len(rows),
                    'max_utilization_percent':max(r[1] for r in rows),'peak_memory_mib':max(r[0] for r in rows),
                    'mean_power_watts':sum(r[2] for r in rows)/len(rows)} for index,rows in sorted(devices.items())],
            'scope':'Arithmetic sample means; no gap imputation, SM occupancy, wall-time energy or billing estimate'})
    return result


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
            'workload_strata':workload_strata(observations),
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
    report=json.loads(args.report.read_text());result=summarize(report)
    result['telemetry_by_phase']=telemetry_by_phase(args.report.parent,report)
    with args.output.open('x') as stream:json.dump(result,stream,indent=2);stream.write('\n')


if __name__=='__main__':main()
