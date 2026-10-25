"""Bounded, opt-in JSON-schema decoding subset; no refs or remote resolution."""
from __future__ import annotations
import json,math,re

PATTERNS=frozenset({
    r'^[a-zA-Z0-9_.:-]+$',
    r'^[A-Za-z0-9][A-Za-z0-9_.:/@+\-]{0,127}$',
    r'^[+-]?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]{1,3})?$'})
KEYWORDS=frozenset({'type','properties','required','additionalProperties','items','anyOf','enum','const',
    'minLength','maxLength','minItems','maxItems','minimum','maximum','pattern','title','description','default'})


def validate_response_format(value,*,allow_json_schema=False):
    if value in ({'type':'text'},{'type':'json_object'}):return
    if not allow_json_schema:raise ValueError('response_format supports only type=text or type=json_object on this deployment')
    if not isinstance(value,dict) or set(value)!={'type','json_schema'} or value['type']!='json_schema':raise ValueError('Invalid response_format envelope')
    descriptor=value['json_schema']
    if (not isinstance(descriptor,dict) or not {'name','schema'}<=set(descriptor) or set(descriptor)-{'name','schema','strict','description'}
        or not isinstance(descriptor['name'],str) or not re.fullmatch('[A-Za-z0-9_-]{1,64}',descriptor['name'])):
        raise ValueError('Invalid bounded json_schema descriptor')
    if 'strict' in descriptor and type(descriptor['strict']) is not bool:raise ValueError('strict must be boolean')
    if 'description' in descriptor and (not isinstance(descriptor['description'],str) or len(descriptor['description'])>512):raise ValueError('Schema description exceeds bound')
    try:size=len(json.dumps(value,ensure_ascii=False,allow_nan=False).encode())
    except (TypeError,ValueError,UnicodeError,RecursionError):raise ValueError('Invalid finite schema JSON') from None
    if size>32768:raise ValueError('Response schema exceeds byte bound')
    count=0
    def scalar(v):
        return (v is None or type(v) is bool or (type(v) is str and len(v)<=512)
                or (type(v) in (int,float) and abs(v)<=1e18 and math.isfinite(v)))
    def walk(node,depth):
        nonlocal count
        count+=1
        if depth>12 or count>256 or not isinstance(node,dict) or not node or set(node)-KEYWORDS:
            raise ValueError('Unsupported or excessive response schema')
        kind=node.get('type')
        if 'type' in node and kind not in ('object','array','string','integer','number','boolean','null'):raise ValueError('Unsupported schema type')
        if kind is None and not {'anyOf','enum','const'}&set(node):raise ValueError('Explicit schema type or alternatives required')
        if kind=='object':
            props=node.get('properties',{})
            if (not isinstance(props,dict) or len(props)>32 or node.get('additionalProperties') is not False
                or any(not isinstance(k,str) or not 1<=len(k)<=128 for k in props)):
                raise ValueError('Object schemas require bounded named properties and additionalProperties=false')
            required=node.get('required',[])
            if not isinstance(required,list) or any(not isinstance(k,str) or k not in props for k in required) or len(set(required))!=len(required):
                raise ValueError('Required fields must reference unique declared properties')
            for child in props.values():walk(child,depth+1)
        elif set(node)&{'properties','required','additionalProperties'}:raise ValueError('Object keywords require an object type')
        if kind=='array':
            if type(node.get('maxItems')) is not int or not 0<=node['maxItems']<=128 or 'items' not in node:raise ValueError('Arrays need bounded items')
            walk(node['items'],depth+1)
        elif set(node)&{'items','minItems','maxItems'}:raise ValueError('Array keywords require an array type')
        for key,bound in [('minLength',16384),('maxLength',16384),('minItems',128),('maxItems',128)]:
            if key in node and (type(node[key]) is not int or not 0<=node[key]<=bound):raise ValueError('Schema size constraint exceeds bound')
        for lower,upper in [('minLength','maxLength'),('minItems','maxItems')]:
            if lower in node and upper in node and node[lower]>node[upper]:raise ValueError('Contradictory schema bounds')
        if set(node)&{'minLength','maxLength','pattern'} and kind!='string':raise ValueError('String keywords require a string type')
        for key in ('minimum','maximum'):
            if key in node and (type(node[key]) not in (int,float) or abs(node[key])>1e18 or not math.isfinite(node[key])):raise ValueError('Schema numeric bound invalid')
        if set(node)&{'minimum','maximum'} and kind not in ('integer','number'):raise ValueError('Numeric bounds require a numeric type')
        if 'minimum' in node and 'maximum' in node and node['minimum']>node['maximum']:raise ValueError('Contradictory numeric bounds')
        if 'pattern' in node and (not isinstance(node['pattern'],str) or node['pattern'] not in PATTERNS):raise ValueError('Pattern is outside the qualified schema subset')
        if 'anyOf' in node:
            children=node['anyOf']
            if not isinstance(children,list) or not 1<=len(children)<=8:raise ValueError('Schema alternatives exceed bound')
            for child in children:walk(child,depth+1)
        if 'enum' in node:
            if not isinstance(node['enum'],list) or not 1<=len(node['enum'])<=64 or not all(scalar(v) for v in node['enum']):raise ValueError('Enum exceeds bounded scalar subset')
        for key in ('const','default'):
            if key in node and not scalar(node[key]):raise ValueError('Schema scalar exceeds bound')
        for key in ('title','description'):
            if key in node and (not isinstance(node[key],str) or len(node[key])>512):raise ValueError('Schema metadata exceeds bound')
    walk(descriptor['schema'],0)
