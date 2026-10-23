from pathlib import Path

import pytest

from ops.host_memory import available_bytes


def layout(tmp_path, membership='/'):
    proc, group = tmp_path / 'proc', tmp_path / 'cgroup'
    (proc / 'self').mkdir(parents=True)
    group.mkdir()
    (proc / 'meminfo').write_text('MemAvailable: 1000 kB\nMemTotal: 9000 kB\n')
    (proc / 'self/cgroup').write_text('0::' + membership + '\n')
    return proc, group


def memory(path, limit, used):
    path.mkdir(exist_ok=True)
    (path / 'memory.max').write_text(str(limit))
    (path / 'memory.current').write_text(str(used))


def test_container_headroom_and_parent_limit_override_large_host(tmp_path):
    proc, group = layout(tmp_path, '/child')
    memory(group, 800, 750)
    memory(group / 'child', 500, 100)
    assert available_bytes(proc, group) == 50
    memory(group, 'max', 750)
    assert available_bytes(proc, group) == 400
    memory(group / 'child', 'max', 100)
    assert available_bytes(proc, group) == 1024000


def test_namespaced_mount_root_and_over_limit_container(tmp_path):
    proc, group = layout(tmp_path, '/unexposed/container')
    memory(group, 100, 110)
    assert available_bytes(proc, group) == 0


@pytest.mark.parametrize('limit,used', [('oops', '1'), ('max', '-1')])
def test_invalid_observations_never_admit(tmp_path, limit, used):
    proc, group = layout(tmp_path)
    memory(group, limit, used)
    with pytest.raises(ValueError):
        available_bytes(proc, group)


def test_missing_container_accounting_fails_closed(tmp_path):
    proc, group = layout(tmp_path)
    with pytest.raises(ValueError):
        available_bytes(proc, group)
