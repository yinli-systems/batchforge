"""Bound CPU-expert admission by host availability and visible cgroup limits."""
from pathlib import Path


def available_bytes(proc=Path('/proc'), cgroup=Path('/sys/fs/cgroup')):
    fields = dict(line.split(':', 1) for line in (proc / 'meminfo').read_text().splitlines())
    amount, unit = fields['MemAvailable'].split()
    if unit != 'kB' or not amount.isdecimal():
        raise ValueError('Invalid host memory observation')
    available = int(amount) * 1024
    # cgroup namespaces can expose the current container at mount root. When
    # the full membership path is visible, include all its visible ancestors.
    membership = [line[3:] for line in (proc / 'self/cgroup').read_text().splitlines() if line.startswith('0::')]
    if len(membership) != 1 or not membership[0].startswith('/'):
        raise ValueError('CPU-expert admission requires visible cgroup v2 memory accounting')
    root = cgroup.resolve()
    candidate = (root / membership[0].lstrip('/')).resolve()
    current = candidate if candidate.is_relative_to(root) and (candidate / 'memory.max').exists() else root
    observed = 0
    while True:
        limit_file, used_file = current / 'memory.max', current / 'memory.current'
        if limit_file.exists():
            limit, used = limit_file.read_text().strip(), used_file.read_text().strip()
            if not used.isdecimal() or (limit != 'max' and not limit.isdecimal()):
                raise ValueError('Invalid cgroup memory observation')
            if limit != 'max':
                available = min(available, max(0, int(limit) - int(used)))
            observed += 1
        if current == root:
            break
        current = current.parent
    if not observed:
        raise ValueError('Missing cgroup memory accounting')
    return available


def admission(model_path, *, reserve_bytes=80 * 1024**3):
    model = Path(model_path)
    if not model.is_file() or model.stat().st_size <= 0:
        raise ValueError('Missing CPU-expert model file')
    return available_bytes() >= model.stat().st_size + reserve_bytes
