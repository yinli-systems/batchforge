"""Real-database restore qualification, including deliberately corrupt ledgers."""
import os
import secrets

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
import pytest

from backend.migrate import MIGRATIONS, migrate_database
from ops.ha.local_pg17_restore import admission_invariants, verify_database

SOURCE = os.environ.get('TEST_DATABASE_URL')
pytestmark = pytest.mark.skipif(not SOURCE, reason='Requires isolated real PostgreSQL')


@pytest.fixture
def database():
    name = 'bf_restore_checks_' + secrets.token_hex(8)
    with psycopg.connect(SOURCE, autocommit=True) as connection:
        connection.execute(sql.SQL('CREATE DATABASE {} TEMPLATE template0').format(sql.Identifier(name)))
    dsn = make_conninfo(SOURCE, dbname=name)
    try:
        migrate_database(dsn, 'restore-fixture-private-key')
        yield dsn
    finally:
        with psycopg.connect(SOURCE, autocommit=True) as connection:
            connection.execute(sql.SQL('DROP DATABASE {} WITH(FORCE)').format(sql.Identifier(name)))


def test_full_schema_and_private_definer_inventory_are_qualified(database):
    with psycopg.connect(database) as connection:
        report = verify_database(connection, MIGRATIONS)
    assert report['schema_version'] == MIGRATIONS[-1][0]
    assert report['retention_ledger_mismatches'] == 0
    assert report['auxiliary_ledger_mismatches'] == 0
    assert len(report['private_source_auxiliary_definer_functions']) == 5


@pytest.mark.parametrize('kind', ['retention', 'auxiliary'])
def test_restore_refuses_a_counter_that_differs_from_restored_rows(database, kind):
    with psycopg.connect(database) as connection:
        if kind == 'retention':
            connection.execute('ALTER TABLE tenant_retention_usage DISABLE TRIGGER retention_usage_guard')
            connection.execute('UPDATE tenant_retention_usage SET retained_tasks=1')
            connection.execute('ALTER TABLE tenant_retention_usage ENABLE TRIGGER retention_usage_guard')
        else:
            # The owner may initialize policy; this creates no application row.
            connection.execute("SELECT batchforge_auxiliary_lock('default')")
            connection.execute("UPDATE tenant_auxiliary_usage SET rows=1 WHERE resource='input_files'")
        with pytest.raises(RuntimeError, match='admission_ledger_mismatch'):
            admission_invariants(connection, MIGRATIONS[-1][0])


def test_restore_refuses_an_unreviewed_security_definer(database):
    with psycopg.connect(database) as connection:
        connection.execute('''CREATE FUNCTION batchforge_unreviewed() RETURNS integer
            LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog,public AS $$SELECT 1$$''')
        with pytest.raises(RuntimeError, match='function_security_differs'):
            verify_database(connection, MIGRATIONS)
