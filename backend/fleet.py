"""Bounded projection of worker-reported physical inventory, never allocation."""
from datetime import datetime,timezone
import math
import re

PROTOCOL='batchforge.gpu-inventory.v1'


def number(value,low,high,*,integer=False):
    if type(value) not in (int,float) or not math.isfinite(value) or not low<=value<=high:return None
    if integer and int(value)!=value:return None
    return int(value) if integer else value


def inventory_view(raw):
    if not isinstance(raw,dict) or raw.get('protocol')!=PROTOCOL:raise ValueError('Invalid inventory protocol')
    if not isinstance(raw.get('node_id'),str) or not re.fullmatch(r'[0-9a-f]{64}',raw['node_id']):raise ValueError('Invalid node identity')
    at=datetime.fromisoformat(raw['observed_at'])
    if at.tzinfo is None:raise ValueError('Inventory timestamp needs timezone')
    devices=raw['devices']
    if not isinstance(devices,list) or not 1<=len(devices)<=64:raise ValueError('Inventory device bound')
    safe=[];ids=set();indices=set()
    for d in devices:
        identity=d['id'];index=number(d.get('index'),0,127,integer=True)
        name=d.get('name');total=number(d.get('memory_total_mib'),1,2097152,integer=True)
        if (not isinstance(identity,str) or not re.fullmatch(r'[0-9a-f]{64}',identity) or identity in ids
            or index is None or index in indices or not isinstance(name,str)
            or not re.fullmatch(r'[A-Za-z0-9 ._()/+-]{1,96}',name) or total is None):raise ValueError('Invalid GPU identity')
        ids.add(identity);indices.add(index)
        safe.append({'id':identity,'index':index,'name':name,'memory_total_mib':total,
            'memory_used_mib':number(d.get('memory_used_mib'),0,total,integer=True),
            'utilization_percent':number(d.get('utilization_percent'),0,100),
            'power_watts':number(d.get('power_watts'),0,5000),'temperature_celsius':number(d.get('temperature_celsius'),-20,150)})
    links=raw.get('links',[])
    if not isinstance(links,list) or len(links)>2016:raise ValueError('Inventory topology bound')
    safe_links=[];pairs=set()
    for link in links:
        a,b=link['a'],link['b'];relation=link['relation']
        if type(a) is not int or type(b) is not int or a not in indices or b not in indices or a>=b or (a,b) in pairs or not re.fullmatch(r'NV[1-9][0-9]?|SYS|NODE|PHB|PXB|PIX',relation):raise ValueError('Invalid topology edge')
        pairs.add((a,b));safe_links.append({'a':a,'b':b,'relation':relation})
    return {'protocol':PROTOCOL,'node_id':raw['node_id'],'observed_at':at.astimezone(timezone.utc).isoformat(),
        'devices':safe,'links':safe_links,'topology_complete':len(safe_links)==len(safe)*(len(safe)-1)//2}


def fleet_view(rows,now):
    nodes={};rejected=0;global_devices={}
    for row in rows:
        if row.get('inventory') is None:continue
        try:inventory=inventory_view(row['inventory'])
        except (ValueError,TypeError,KeyError,OverflowError):rejected+=1;continue
        age=(now-datetime.fromisoformat(inventory['observed_at'])).total_seconds()
        fresh=(-5<=age<=60 and row['status']=='online')
        nid=inventory['node_id'];node=nodes.setdefault(nid,{'id':nid,'reports':[],'device_ids':set(),'fresh_device_ids':set(),'conflicts':[]})
        node['reports'].append({'worker_id':row['id'],'model':row['model'],'worker_status':row['status'],
            'inventory_state':'fresh' if fresh else 'stale','age_seconds':age,'inventory':inventory})
        for device in inventory['devices']:
            did=device['id'];node['device_ids'].add(did)
            if fresh:node['fresh_device_ids'].add(did)
            signature=(device['name'],device['memory_total_mib'])
            previous=global_devices.get(did)
            if previous and (previous[0]!=nid or previous[1]!=signature):
                node['conflicts'].append('device_identity_conflict')
                nodes[previous[0]]['conflicts'].append('device_identity_conflict')
            global_devices[did]=(nid,signature)
    result=[]
    for node in nodes.values():
        node['observed_device_count']=len(node.pop('device_ids'))
        node['fresh_device_count']=len(node.pop('fresh_device_ids'))
        node['conflicts']=sorted(set(node['conflicts']));node['placement_capacity']=None
        result.append(node)
    return {'object':'list','data':result,'rejected_reports':rejected,'observed_at':now.isoformat(),
        'scope':'Shared workers serving configured models; device-wide operator-selected observations, not exclusive allocation or hardware attestation',
        'limitations':['Freshness requires a worker heartbeat and a report no older than 60 seconds; future skew over five seconds is stale',
            'Observed devices and link labels are not free GPU capacity, measured interconnect bandwidth, or proof of inference placement']}
