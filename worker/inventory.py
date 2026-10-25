"""Opt-in, read-only NVIDIA inventory. No CUDA context or benchmark is created."""
import csv
from datetime import datetime,timezone
import hashlib
import re
import subprocess

from backend.fleet import PROTOCOL,inventory_view


def selection(value):
    if not re.fullmatch(r'[0-9]{1,3}(?:,[0-9]{1,3}){0,63}',value):raise ValueError('Explicit GPU indices required')
    indices=tuple(int(i) for i in value.split(','))
    if len(set(indices))!=len(indices) or max(indices)>127:raise ValueError('Invalid GPU indices')
    return indices


def collect(node_id,indices):
    def command(args):
        result=subprocess.run(args,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,timeout=3,check=True)
        if len(result.stdout)>262144:raise ValueError('Inventory command output exceeds bound')
        return result.stdout.decode('utf-8',errors='strict')
    query=command(['nvidia-smi','-i',','.join(str(i) for i in indices),
        '--query-gpu=index,uuid,name,memory.total,memory.used,utilization.gpu,power.draw,temperature.gpu','--format=csv,noheader,nounits'])
    devices=[]
    def numeric(value):
        try:return float(value)
        except ValueError:return None
    for row in csv.reader(query.splitlines()):
        if len(row)!=8:raise ValueError('Unexpected NVIDIA inventory columns')
        index,uuid,name,total,used,util,power,temp=[s.strip() for s in row]
        if not re.fullmatch(r'(?:GPU|MIG)-[A-Za-z0-9/-]{1,100}',uuid):raise ValueError('Invalid GPU identifier')
        devices.append({'index':int(index),'id':hashlib.sha256(uuid.encode()).hexdigest(),'name':name,
            'memory_total_mib':numeric(total),'memory_used_mib':numeric(used),'utilization_percent':numeric(util),
            'power_watts':numeric(power),'temperature_celsius':numeric(temp)})
    if {d['index'] for d in devices}!=set(indices):raise ValueError('NVIDIA device selection differs')
    links=[]
    try:
        topology=re.sub(r'\x1b\[[0-9;]*m','',command(['nvidia-smi','topo','-m']))
        lines=[line.split() for line in topology.splitlines() if line.strip()]
        header=next(line for line in lines if len(line)>1 and re.fullmatch(r'GPU\d+',line[0]) and re.fullmatch(r'GPU\d+',line[1]))
        columns=[int(label[3:]) for label in header if re.fullmatch(r'GPU\d+',label)]
        matrix={}
        for line in lines:
            if not re.fullmatch(r'GPU\d+',line[0]):continue
            a=int(line[0][3:])
            if a not in columns or len(line)<=columns.index(a)+1 or line[columns.index(a)+1]!='X':continue
            matrix[a]=dict(zip(columns,line[1:]))
        for a in indices:
            for b in indices:
                relation=matrix.get(a,{}).get(b,'')
                if a<b and relation==matrix.get(b,{}).get(a) and re.fullmatch(r'NV[1-9][0-9]?|SYS|NODE|PHB|PXB|PIX',relation):
                    links.append({'a':a,'b':b,'relation':relation})
    except (ValueError,StopIteration,subprocess.SubprocessError):
        links=[]
    return inventory_view({'protocol':PROTOCOL,'node_id':hashlib.sha256(node_id.encode()).hexdigest(),
        'observed_at':datetime.now(timezone.utc).isoformat(),'devices':devices,'links':links})
