"""Capability-checked data tools; no shell, arbitrary Python or network access.

These trusted implementations are not a process/container sandbox. Arbitrary
code execution remains unavailable until a separate isolation backend qualifies.
"""
from __future__ import annotations
import ast,math,operator,time
from dataclasses import dataclass
from typing import Protocol
from pydantic import Field
from .contracts import Strict,ToolDecision,Policy,canonical,digest


class Memory(Protocol):
    async def get(self,tenant_id:str,namespace:str,key:str):...
    async def search(self,tenant_id:str,namespace:str,query:str,limit:int):...


class Calculate(Strict):
    expression:str=Field(min_length=1,max_length=512)


class MemoryGet(Strict):
    key:str=Field(min_length=1,max_length=128,pattern=r'^[a-zA-Z0-9_.:-]+$')


class MemorySearch(Strict):
    query:str=Field(min_length=1,max_length=1024)
    limit:int=Field(default=5,ge=1,le=20)


SPECS=[{'name':name,'arguments':schema.model_json_schema()} for name,schema in
       [('math.calculate',Calculate),('memory.get',MemoryGet),('memory.search',MemorySearch)]]


def calculate(expression):
    if not isinstance(expression,str) or len(expression)>512:raise ValueError('Expression exceeds bound')
    try:tree=ast.parse(expression,mode='eval')
    except (SyntaxError,RecursionError):raise ValueError('Invalid arithmetic expression') from None
    if sum(1 for _ in ast.walk(tree))>128:raise ValueError('Expression complexity exceeds bound')
    ops={ast.Add:operator.add,ast.Sub:operator.sub,ast.Mult:operator.mul,
         ast.Div:operator.truediv,ast.FloorDiv:operator.floordiv,ast.Mod:operator.mod}
    def bounded(value):
        if type(value) not in (int,float) or not math.isfinite(value) or abs(value)>10**18:
            raise ValueError('Arithmetic result exceeds numeric bound')
        return value
    def visit(node,depth=0):
        if depth>16:raise ValueError('Expression nesting exceeds bound')
        if isinstance(node,ast.Constant):return bounded(node.value)
        if isinstance(node,ast.UnaryOp) and type(node.op) in (ast.UAdd,ast.USub):
            return bounded((1 if isinstance(node.op,ast.UAdd) else -1)*visit(node.operand,depth+1))
        if isinstance(node,ast.BinOp):
            left=visit(node.left,depth+1);right=visit(node.right,depth+1)
            if isinstance(node.op,ast.Pow):
                if type(right) is not int or not 0<=right<=12:raise ValueError('Exponent exceeds bound')
                return bounded(left**right)
            if type(node.op) in ops:return bounded(ops[type(node.op)](left,right))
        raise ValueError('Only bounded arithmetic literals and operators are supported')
    try:return visit(tree.body)
    except (ArithmeticError,TypeError):raise ValueError('Invalid arithmetic operation') from None


@dataclass(frozen=True)
class Context:
    tenant_id:str
    policy:Policy
    memory:Memory|None=None

    def __post_init__(self):
        if not isinstance(self.tenant_id,str) or not 1<=len(self.tenant_id)<=128:raise ValueError('Explicit tenant required')


async def execute(decision,context):
    if not isinstance(decision,ToolDecision):raise ValueError('A typed tool decision is required')
    context.policy.authorize(decision)
    started=time.monotonic()
    if decision.name=='math.calculate':
        args=Calculate.model_validate(decision.arguments)
        value={'value':calculate(args.expression)}
    elif decision.name=='memory.get':
        args=MemoryGet.model_validate(decision.arguments)
        if context.memory is None:raise RuntimeError('Persistent memory adapter unavailable')
        value=await context.memory.get(context.tenant_id,context.policy.memory_namespace,args.key)
    elif decision.name=='memory.search':
        args=MemorySearch.model_validate(decision.arguments)
        if context.memory is None:raise RuntimeError('Persistent memory adapter unavailable')
        value=await context.memory.search(context.tenant_id,context.policy.memory_namespace,args.query,args.limit)
    else:raise PermissionError('Tool implementation is unavailable')
    encoded=canonical(value)
    if len(encoded)>16384:raise ValueError('Tool result exceeds observation bound')
    return {'tool':decision.name,'arguments_sha256':digest(decision.arguments),'result':value,
            'result_sha256':digest(value),'result_bytes':len(encoded),'elapsed_seconds':time.monotonic()-started}
