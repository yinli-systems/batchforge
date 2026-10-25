from pathlib import Path

import pytest

from ops import storage_semantics
from ops.gpu_campaign import execute
from types import SimpleNamespace


MOUNTS = ('1 0 0:1 / / rw - overlay overlay rw\n'
          '2 1 0:2 / /workspace rw - fuse.geesefs object-volume rw\n'
          '3 1 0:3 / /workspace/local\\040disk rw - ext4 /dev/test rw\n')


def test_most_specific_mount_wins_and_escaped_spaces_are_decoded():
    assert storage_semantics.filesystem_type('/workspace/job', MOUNTS) == 'fuse.geesefs'
    assert storage_semantics.filesystem_type('/workspace/local disk/job', MOUNTS) == 'ext4'
    assert storage_semantics.filesystem_type('/root/job', MOUNTS) == 'overlay'


def test_object_filesystem_is_refused_before_creating_a_campaign(tmp_path, monkeypatch):
    monkeypatch.setattr(storage_semantics, 'filesystem_type', lambda path: 'fuse.geesefs')
    out = tmp_path / 'not-created'
    with pytest.raises(ValueError, match='cannot host this POSIX journal'):
        execute(SimpleNamespace(url='http://127.0.0.1:8300', out=out))
    assert not out.exists()
