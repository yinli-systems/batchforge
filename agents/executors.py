"""Read an operator-owned executor registry without exposing its credentials."""
from pathlib import Path
import os,stat
from .contracts import protocol_json


def credentials(path):
    path=Path(path)
    try:fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW)
    except OSError:raise ValueError('Regular protected registry required') from None
    with os.fdopen(fd,'rb') as stream:
        st=os.fstat(stream.fileno())
        if not stat.S_ISREG(st.st_mode) or st.st_uid!=os.geteuid() or stat.S_IMODE(st.st_mode)&0o077 or st.st_size>65536:
            raise ValueError('Executor registry must be owner-only and bounded')
        raw=stream.read(65537)
    if len(raw)>65536:raise ValueError('Executor registry exceeds bound')
    value=protocol_json(raw)
    if not isinstance(value,dict) or not 1<=len(value)<=64 or any(not isinstance(k,str) or not 1<=len(k)<=128 or not isinstance(v,str) or not 16<=len(v)<=256 for k,v in value.items()):
        raise ValueError('Explicit tenant-key registry required')
    return value
