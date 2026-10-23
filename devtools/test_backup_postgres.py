"""Credential handling and archive publication regressions; no live database."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import subprocess
from unittest.mock import MagicMock, patch

import pytest
from psycopg.conninfo import make_conninfo

from devtools import backup_postgres as backup


@pytest.fixture
def binaries(tmp_path):
    directory = tmp_path / "bin"
    directory.mkdir()
    for name in ("pg_dump", "pg_restore"):
        path = directory / name
        path.write_text("#!/bin/sh\nexit 0\n")
        path.chmod(0o700)
    return directory


@pytest.fixture
def database_url():
    return make_conninfo(host="fixture.invalid", dbname="fixture", user="fixture",
                         password="colon:slash\\", sslmode="verify-full")


def test_bin_directory_selection_and_executable_validation(binaries, monkeypatch):
    assert backup.select_bin_dir(binaries, {"PG_BIN_DIR": "/missing"}) == binaries
    assert backup.select_bin_dir(None, {"PG_BIN_DIR": str(binaries)}) == binaries

    def version(argv, **kwargs):
        assert argv == [str(binaries / "pg_dump"), "--version"]
        assert not {"DATABASE_URL", "PGPASSWORD", "PGPASSFILE"} & kwargs["env"].keys()
        return subprocess.CompletedProcess(argv, 0, "pg_dump (PostgreSQL) 17.11", "")

    monkeypatch.setattr(backup.subprocess, "run", version)
    assert backup.tool_version(binaries / "pg_dump", {
        "DATABASE_URL": "fixture-secret", "PGPASSWORD": "fixture-secret", "PGPASSFILE": "/fixture",
    }) == (17, "pg_dump (PostgreSQL) 17.11")
    (binaries / "pg_restore").chmod(0o600)
    with pytest.raises(backup.BackupError, match="Missing executable pg_restore"):
        backup.select_bin_dir(binaries, {})


def test_older_client_refused_before_archive_creation(binaries, tmp_path):
    connection = MagicMock()
    connection.execute.return_value.fetchone.return_value = ("17.11", 170011)
    connection.pgconn.ssl_in_use = True
    with patch("psycopg.connect") as connect, patch.object(
        backup, "tool_version", return_value=(16, "pg_dump (PostgreSQL) 16.6")
    ):
        connect.return_value.__enter__.return_value = connection
        with pytest.raises(backup.BackupError, match="pg_dump16 cannot back up PostgreSQL17"):
            backup.preflight("postgresql://fixture@localhost/test", binaries, {})
    assert not list(tmp_path.glob("*.dump"))


def test_pgpass_private_escaped_removed_and_tls_preserved(database_url, tmp_path):
    with backup.connection_environment(database_url, {
        "DATABASE_URL": database_url, "PGPASSWORD": "stale", "PGSSLMODE": "disable",
        "PGPASSFILE": "/unrelated/password-file", "PGSERVICE": "unrelated-service",
    }, tmp_path) as env:
        password_file = Path(env["PGPASSFILE"])
        assert password_file.stat().st_mode & 0o777 == 0o600
        assert password_file.read_text() == "*:*:*:*:colon\\:slash\\\\\n"
        assert env["PGSSLMODE"] == "verify-full"
        assert "DATABASE_URL" not in env and "PGPASSWORD" not in env
        assert "PGSERVICE" not in env
    assert not password_file.exists()
    assert not list(tmp_path.iterdir())
    with pytest.raises(backup.BackupError, match="Multiline passwords"):
        backup.pgpass_escape("fixture\npassword")


def test_archive_published_only_after_list_check_with_private_hashed_metadata(
    binaries, database_url, tmp_path, monkeypatch
):
    payload = b"MOCK-ARCHIVE; not a PostgreSQL dump"
    output = tmp_path / "snapshots"
    output.mkdir(mode=0o777)
    observed_password_files = []

    def run(argv, **kwargs):
        env = kwargs["env"]
        assert database_url not in argv
        assert "DATABASE_URL" not in env and "PGPASSWORD" not in env
        observed_password_files.append(Path(env["PGPASSFILE"]))
        assert observed_password_files[-1].stat().st_mode & 0o777 == 0o600
        assert not list(output.glob("*.dump"))
        if Path(argv[0]).name == "pg_dump":
            assert "--no-password" in argv
            assert "--lock-wait-timeout=10s" in argv
            Path(argv[argv.index("--file") + 1]).write_bytes(payload)
        return subprocess.CompletedProcess(argv, 0, "; header\n1; TABLE fixture\n", "")

    monkeypatch.setattr(backup.subprocess, "run", run)
    destination, report = backup.create_backup(database_url, binaries, output, {}, {"compatible": True})
    assert output.stat().st_mode & 0o777 == 0o700
    assert destination.stat().st_mode & 0o777 == 0o600
    assert destination.with_suffix(".json").stat().st_mode & 0o777 == 0o600
    assert report["snapshot_sha256"] == hashlib.sha256(payload).hexdigest()
    assert json.loads(destination.with_suffix(".json").read_text()) == report
    assert report["archive_list_verified"] is True
    assert report["restore_verified"] is False
    assert report["archive_toc_entries"] == 1
    assert all(not path.exists() for path in observed_password_files)
    assert sorted(path.suffix for path in output.iterdir()) == [".dump", ".json"]


def test_failed_dump_or_archive_listing_removes_partial_and_password_files(
    binaries, database_url, tmp_path, monkeypatch
):
    for failing_program in ("pg_dump", "pg_restore"):
        output = tmp_path / failing_program

        def run(argv, **kwargs):
            if Path(argv[0]).name == failing_program:
                raise subprocess.CalledProcessError(1, argv)
            Path(argv[argv.index("--file") + 1]).write_bytes(b"mock partial archive")
            return subprocess.CompletedProcess(argv, 0, "", "")

        monkeypatch.setattr(backup.subprocess, "run", run)
        with pytest.raises(subprocess.CalledProcessError):
            backup.create_backup(database_url, binaries, output, {}, {"compatible": True})
        assert not list(output.iterdir())
