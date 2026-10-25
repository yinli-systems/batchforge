"""Detect object filesystem mounts before relying on POSIX journal semantics."""
from pathlib import Path
import re


def filesystem_type(path, mountinfo=None):
    path = Path(path).resolve()
    if mountinfo is None:
        source = Path('/proc/self/mountinfo')
        if not source.exists():
            return None
        mountinfo = source.read_text()
    best = None
    for line in mountinfo.splitlines():
        left, separator, right = line.partition(' - ')
        fields = left.split()
        if not separator or len(fields) < 5 or not right.split():
            continue
        mount = Path(re.sub(r'\\([0-7]{3})', lambda m: chr(int(m[1], 8)), fields[4]))
        if path.is_relative_to(mount) and (best is None or len(str(mount)) > best[0]):
            best = len(str(mount)), right.split()[0]
    return best[1] if best else None


def require_journal_filesystem(path):
    kind = filesystem_type(path)
    if kind and (kind.startswith('fuse.') or kind in {'fuse', 'fuseblk'}):
        raise ValueError(f'{kind} cannot host this POSIX journal: use a local disk and export closed evidence off host')
