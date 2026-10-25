"""Operator-policy projection into vLLM's standard JSON-schema response format.

The engine constraint is a reliability aid. Runtime authorization/strict parsing
remain mandatory even when a provider ignores or mishandles this schema.
No expected answer, secret, alternate tenant or caller-provided schema enters it.
"""
from __future__ import annotations
import copy
from .contracts import FinalDecision,canonical
from .tools import SPECS


def response_format(policy,*,observed_citations,remaining_tools,executed_tools=()):
    if policy.planner_protocol=='json-v1':return {'type':'json_object'}
    if type(remaining_tools) is not int or remaining_tools<0:raise ValueError('Explicit remaining tool budget required')
    citations=sorted(set(observed_citations))
    if len(citations)>64 or any(not isinstance(c,str) or len(c)>512 for c in citations):raise ValueError('Bounded observed citations required')
    variants=[]
    pending=policy.pending_tool(executed_tools)
    if remaining_tools:
        for spec in SPECS:
            if spec['name'] not in policy.allowed_tools:continue
            if pending is not None and spec['name']!=pending:continue
            variants.append({'type':'object','additionalProperties':False,'required':['type','name','arguments'],
                'properties':{'type':{'type':'string','const':'tool'},'name':{'type':'string','const':spec['name']},'arguments':copy.deepcopy(spec['arguments'])}})
    if pending is None and (citations or not policy.require_citations):
        final=copy.deepcopy(FinalDecision.model_json_schema())
        # Force a consistent complete final envelope, including [] for a purely
        # arithmetic answer. json_object alone never enforces these field types.
        final['required']=['type','answer','citations']
        if policy.answer_format=='identifier':
            final['properties']['answer'].update(pattern=r'^[A-Za-z0-9][A-Za-z0-9_.:/@+\-]{0,127}$',maxLength=128)
        elif policy.answer_format=='number':
            final['properties']['answer'].update(pattern=r'^[+-]?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]{1,3})?$',maxLength=128)
        final['properties']['citations']={'type':'array','items':{'type':'string'},'maxItems':32}
        if citations:
            final['properties']['citations']['items']['enum']=citations
            if policy.require_citations:final['properties']['citations']['minItems']=1
        else:final['properties']['citations']['maxItems']=0
        variants.append(final)
    if not variants:raise ValueError('No legal decision remains within evidence/tool budget')
    schema={'anyOf':variants}
    if len(canonical(schema))>32768:raise ValueError('Planner grammar exceeds bound')
    return {'type':'json_schema','json_schema':{'name':'batchforge_agent_decision_v1','schema':schema}}
