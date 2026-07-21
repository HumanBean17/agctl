"""Tests for the MySQL DB driver skeleton (PyMySQL-backed, Task 6).

These tests follow the FakeCursor/FakeConn seam pattern of
``test_postgresql_driver.py``: no real MySQL server is required. Real-MySQL
integration is covered by Task 9. The 12 scenarios below mirror the brief.

PyMySQL is lazy-imported inside ``MySQLDriver.connect()``, so most tests inject
a fake connection (``connectable=FakeConn(cur)``). The connect()-path tests
monkeypatch a stub ``pymysql`` module so the recorded kwargs can be inspected.
"""

from __future__ import annotations

import sys
import types

import pytest

from agctl.clients.db_client import DbClient
from agctl.clients.db_driver_protocol import (
    DBDriver,
    ColumnInfo,
    ForeignKey,
    SchemaItem,
    SchemaMatch,
    UniqueConstraint,
    WriteResult,
)
from agctl.clients.db_drivers.mysql import MySQLDriver, _parse_mysql_enum
from agctl.errors import ConfigError, ConnectionFailure


# --- Test seams (mirror test_postgresql_driver.py) -----------------------


def _col(name):
    return types.SimpleNamespace(name=name)


class FakeCursor:
    def __init__(self, description, rows, rowcount=1):
        self.description = description  # sequence of objects with .name
        self._rows = rows
        self.rowcount = rowcount
        self.last_sql = None
        self.last_params = None

    def execute(self, sql, params):
        self.last_sql = sql
        self.last_params = params

    def fetchall(self):
        return self._rows

    def close(self):  # pragma: no cover - trivial
        pass


class FakeConn:
    def __init__(self, cursor, commit_raises=False):
        self._cursor = cursor
        self.closed = False
        self.commit_called = False
        self.rollback_called = False
        self._commit_raises = commit_raises

    def cursor(self):
        return self._cursor

    def close(self):
        self.closed = True

    def commit(self):
        self.commit_called = True
        if self._commit_raises:
            raise RuntimeError("network failure")

    def rollback(self):
        self.rollback_called = True


class _StubPyMySQLError(Exception):
    """Stand-in for ``pymysql.Error`` used by fakes that need a real exc type."""


class _RecordingPyMySQL:
    """Stand-in for the ``pymysql`` module: records connect() kwargs.

    Exposes an ``Error`` attribute so ``except pymysql.Error`` in the driver's
    connect() path matches the stub raised by ``_RaisingPyMySQL`` below.
    """

    def __init__(self):
        self.calls = []
        self.Error = _StubPyMySQLError

    def connect(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return object()


class _RaisingPyMySQL:
    """Stand-in for ``pymysql`` module whose ``connect`` raises ``Error``.

    ``Error`` is a real exception type so ``pymysql.Error`` in the driver's
    connect() except-clause catches it.
    """

    def __init__(self, message):
        self.message = message
        self.Error = _StubPyMySQLError

    def connect(self, *args, **kwargs):
        raise self.Error(self.message)


def _install_fake_pymysql(monkeypatch, fake):
    """Install ``fake`` as ``sys.modules['pymysql']`` for the duration of the test."""
    monkeypatch.setitem(sys.modules, "pymysql", fake)


@pytest.fixture
def fake_pymysql(monkeypatch):
    """Install a stub ``pymysql`` module so execute()/execute_write() can import it.

    Tests that exercise execute()/execute_write() but do NOT need to inspect
    connect() kwargs use this fixture. The stub's ``Error`` attribute is a real
    exception type (``_StubPyMySQLError``) so ``except pymysql.Error`` in the
    driver matches it when a fake cursor raises.
    """
    fake = _RecordingPyMySQL()
    _install_fake_pymysql(monkeypatch, fake)
    return fake


# --- Scenario 1: param translation (:name -> %(name)s) -------------------


def test_execute_translates_named_params_to_pymysql_style(fake_pymysql):
    """convert_sql_params rewrites :orderId -> %(orderId)s before execute."""
    cur = FakeCursor(description=[_col("id")], rows=[("o9",)])
    driver = MySQLDriver(connectable=FakeConn(cur))

    driver.execute("SELECT id FROM t WHERE id = :orderId", {"orderId": "o9"})

    assert cur.last_sql == "SELECT id FROM t WHERE id = %(orderId)s"
    assert cur.last_params == {"orderId": "o9"}


# --- Scenario 2: execute returns dict rows --------------------------------


def test_execute_returns_dict_rows_keyed_by_column_name(fake_pymysql):
    """execute builds dict rows from cur.description + cur.fetchall()."""
    cols = [_col("id"), _col("status")]
    rows = [("o9", "CONFIRMED")]
    cur = FakeCursor(description=cols, rows=rows)
    driver = MySQLDriver(connectable=FakeConn(cur))

    result = driver.execute("SELECT id, status FROM t WHERE id = :id", {"id": "o9"})

    assert result == [{"id": "o9", "status": "CONFIRMED"}]


# --- Scenario 3: execute_write returns WriteResult ------------------------


def test_execute_write_returns_write_result_rows_affected_no_returning(fake_pymysql):
    """execute_write materializes rowcount and always returns returning=[].

    MySQL has no RETURNING clause, so cur.description is None for non-SELECT
    statements and ``returning`` is always empty.
    """
    cur = FakeCursor(description=None, rows=[], rowcount=2)
    driver = MySQLDriver(connectable=FakeConn(cur))

    result = driver.execute_write("UPDATE t SET status = :s", {"s": "shipped"})

    assert isinstance(result, WriteResult)
    assert result.rows_affected == 2
    assert result.returning == []


# --- Scenario 4: execute_write commits ------------------------------------


def test_execute_write_commits_after_success(fake_pymysql):
    """execute_write issues commit() after a successful execute."""
    cur = FakeCursor(description=None, rows=[], rowcount=1)
    conn = FakeConn(cur)
    driver = MySQLDriver(connectable=conn)

    driver.execute_write("INSERT INTO t VALUES (1)", {})

    assert conn.commit_called is True
    assert conn.rollback_called is False


# --- Scenario 5: execute_write rolls back on pymysql.Error ----------------


def test_execute_write_rolls_back_on_pymysql_error_and_raises_connection_failure(
    fake_pymysql,
):
    """execute_write raises ConnectionFailure and rolls back on pymysql.Error.

    The fake cursor raises ``fake_pymysql.Error`` (a stand-in for
    ``pymysql.Error``) inside execute; the driver must catch -> rollback ->
    re-raise as ConnectionFailure (commit must NOT have been called).
    """

    class ErrorCursor:
        def __init__(self):
            self.description = None
            self.rowcount = -1

        def execute(self, sql, params):
            raise fake_pymysql.Error("connection lost")

        def close(self):
            pass

    cur = ErrorCursor()
    conn = FakeConn(cur)
    driver = MySQLDriver(connectable=conn)

    with pytest.raises(ConnectionFailure) as exc_info:
        driver.execute_write("UPDATE t SET status = 'shipped'", {})

    assert "connection lost" in str(exc_info.value)
    assert conn.rollback_called is True
    assert conn.commit_called is False


# --- Scenario 6: connect kwargs construction (discrete fields) -----------


def test_connect_discrete_fields_become_pymysql_connect_kwargs(monkeypatch):
    """Discrete fields build pymysql.connect kwargs + autocommit=False.

    database= comes from config['dbname'] (PyMySQL kwarg name).
    """
    fake = _RecordingPyMySQL()
    _install_fake_pymysql(monkeypatch, fake)

    driver = MySQLDriver()
    driver.connect(
        {
            "type": "mysql",
            "host": "h",
            "port": 3307,
            "dbname": "testdb",
            "user": "u",
            "password": "p",
        }
    )

    assert len(fake.calls) == 1
    args, kwargs = fake.calls[0]
    assert args == ()
    assert kwargs == {
        "host": "h",
        "port": 3307,
        "database": "testdb",
        "user": "u",
        "password": "p",
        "autocommit": False,
    }


# --- Scenario 7: connect URL parsing --------------------------------------


def test_connect_parses_url_into_discrete_kwargs(monkeypatch):
    """A mysql:// url is parsed into host/port/user/password/database kwargs.

    Query-string extras (e.g. ``?charset=utf8mb4``) are NOT auto-extracted
    into kwargs; that is documented as out of scope for the v1 driver.
    """
    fake = _RecordingPyMySQL()
    _install_fake_pymysql(monkeypatch, fake)

    driver = MySQLDriver()
    driver.connect(
        {
            "type": "mysql",
            "url": "mysql://u:pass@h:3306/dbname?charset=utf8mb4",
        }
    )

    assert len(fake.calls) == 1
    args, kwargs = fake.calls[0]
    assert args == ()
    assert kwargs == {
        "host": "h",
        "port": 3306,
        "user": "u",
        "password": "pass",
        "database": "dbname",
        "autocommit": False,
    }


# --- Scenario 8: connect options merge ------------------------------------


def test_connect_options_merge_into_pymysql_kwargs(monkeypatch):
    """options dict is merged into pymysql.connect kwargs."""
    fake = _RecordingPyMySQL()
    _install_fake_pymysql(monkeypatch, fake)

    driver = MySQLDriver()
    driver.connect(
        {
            "type": "mysql",
            "host": "h",
            "options": {"charset": "utf8mb4", "connect_timeout": 5},
        }
    )

    assert len(fake.calls) == 1
    args, kwargs = fake.calls[0]
    assert kwargs["charset"] == "utf8mb4"
    assert kwargs["connect_timeout"] == 5
    assert kwargs["host"] == "h"
    assert kwargs["autocommit"] is False


# --- Scenario 9: discrete fields override URL -----------------------------


def test_connect_discrete_fields_override_url_values(monkeypatch):
    """Discrete fields win when both url and discrete are present."""
    fake = _RecordingPyMySQL()
    _install_fake_pymysql(monkeypatch, fake)

    driver = MySQLDriver()
    driver.connect(
        {
            "type": "mysql",
            "url": "mysql://u:p@h:3306/db",
            "port": 3307,
        }
    )

    assert len(fake.calls) == 1
    args, kwargs = fake.calls[0]
    # Discrete port wins over URL port=3306.
    assert kwargs["port"] == 3307
    # URL values still feed the rest.
    assert kwargs["host"] == "h"
    assert kwargs["user"] == "u"
    assert kwargs["password"] == "p"
    assert kwargs["database"] == "db"


# --- Scenario 10: connect lazy-import error -------------------------------


def test_connect_raises_config_error_when_pymysql_missing(monkeypatch):
    """Missing pymysql extra surfaces as ConfigError with the pip install hint.

    The lazy-import path in ``BaseDBDriver._lazy_import_or_raise`` is exercised
    here: when ``pymysql`` is unimportable, the driver must raise
    ``ConfigError`` mentioning ``pip install 'agctl[mysql]'``.
    """
    # Force ImportError on `import pymysql` by popping the module from
    # sys.modules and ensuring no real one resolves. (Test env may have
    # PyMySQL installed, so the import_module lookup is patched directly.)
    monkeypatch.setitem(sys.modules, "pymysql", None)

    driver = MySQLDriver()
    with pytest.raises(ConfigError) as exc_info:
        driver.connect({"type": "mysql", "host": "h"})

    assert "pip install 'agctl[mysql]'" in str(exc_info.value)


# --- Scenario 11: connect failure redacts config --------------------------


def test_connect_failure_redacts_password_in_detail(monkeypatch):
    """A pymysql.Error surfaces as ConnectionFailure with redacted config.

    detail['driver'] == 'mysql' and detail['config']['password'] == '***'.
    """
    fake = _RaisingPyMySQL("access denied")
    _install_fake_pymysql(monkeypatch, fake)

    driver = MySQLDriver()
    with pytest.raises(ConnectionFailure) as exc_info:
        driver.connect(
            {
                "type": "mysql",
                "host": "h",
                "user": "u",
                "password": "secret",
            }
        )

    detail = exc_info.value.detail
    assert detail["driver"] == "mysql"
    assert detail["config"]["password"] == "***"


# --- Scenario 12: DBDriver protocol conformance ---------------------------


def test_mysql_driver_satisfies_dbdriver_protocol():
    """MySQLDriver is a runtime-conformant DBDriver (connect/execute/close)."""
    assert isinstance(MySQLDriver(), DBDriver)


# --- close() ownership (mirror existing postgres tests) -------------------


def test_close_does_not_close_injected_connection():
    """close() leaves an injected connection open."""
    cur = FakeCursor(description=[_col("id")], rows=[("o9",)])
    conn = FakeConn(cur)
    driver = MySQLDriver(connectable=conn)

    driver.close()

    assert conn.closed is False


def test_close_closes_owned_connection():
    """close() closes a connection the driver owns."""
    cur = FakeCursor(description=[_col("id")], rows=[("o9",)])
    conn = FakeConn(cur)
    driver = MySQLDriver()  # no connectable -> owned
    driver._conn = conn
    driver._owned = True

    driver.close()

    assert conn.closed is True


def test_close_is_safe_when_no_connection():
    """close() must not raise when no connection was ever set."""
    MySQLDriver().close()


# ===========================================================================
# describe_schema (Level-1 + Level-2) and _parse_mysql_enum (Task 7)
#
# Catalog rows are staged on a shared multi-stage FakeCursor (mirroring the
# PostgreSQL test seam). Each catalog SELECT is keyed by a SQL substring
# unique to that query, so the generic dispatch (first staged substring
# contained in the executed SQL) routes each query to its canned rowset:
#   L1 relations -> 'information_schema.tables'
#   L1 counts    -> 'COUNT(*)'           (Level-1 count query is the only one)
#   L2 relations -> 'information_schema.tables'
#   L2 columns   -> 'column_comment'     (Level-2 columns query is the only one)
#   L2 PK/unique -> 'table_constraints'
#   L2 FK        -> 'referential_constraints'
# ===========================================================================


class CatalogFakeCursor:
    """Cursor that dispatches staged result sets by SQL substring.

    Mirrors the PostgreSQL test seam. ``execute(sql, params)`` inspects
    ``sql`` and stages the canned ``(description, rows)`` registered for the
    first matching substring; ``description`` / ``fetchall()`` return the
    currently staged values.
    """

    def __init__(self):
        self._staged: list = []  # list of (sql_substring, description, rows)
        self.description = None
        self._rows = []
        self.last_sql = None
        self.last_params = None
        # Observability: every execute call recorded as (sql, params). The
        # Level-2 branch issues several catalog SELECTs, so last_* only
        # reflects the final one; tests that need to assert on an earlier
        # query's SQL or bind params filter this list.
        self.history: list = []

    def stage(self, sql_substring, description, rows):
        self._staged.append((sql_substring, description, rows))

    def execute(self, sql, params):
        self.last_sql = sql
        self.last_params = params
        self.history.append((sql, params))
        for substring, description, rows in self._staged:
            if substring in sql:
                self.description = description
                self._rows = rows
                return
        # No staged match -> empty result set.
        self.description = None
        self._rows = []

    def fetchall(self):
        return self._rows

    def close(self):  # pragma: no cover - trivial
        pass


class CatalogFakeConn:
    """Minimal connection returning one shared :class:`CatalogFakeCursor`.

    ``describe_schema`` issues read-only catalog SELECTs and never commits,
    so this double intentionally has no ``commit`` / ``rollback`` -- only
    ``cursor`` (always the same staged cursor) and ``close`` for ownership
    regression checks.
    """

    def __init__(self, cursor):
        self._cursor = cursor
        self.closed = False

    def cursor(self):
        return self._cursor

    def close(self):
        self.closed = True


def _client_for(driver) -> DbClient:
    """Wrap a driver in a DbClient for boundary-level dict-shape assertions.

    The driver returns DTOs (``SchemaItem``, ``SchemaMatch``); :class:`DbClient`
    serializes those to plain dicts at the boundary via ``dataclasses.asdict``.
    """
    return DbClient({"type": "mysql"}, driver=driver)


# --- canned row builders --------------------------------------------------

_L1_REL_DESC = [_col("table_schema"), _col("table_name"), _col("table_type")]
_L1_COUNT_DESC = [
    _col("table_schema"),
    _col("table_name"),
    _col("column_count"),
]
_L2_REL_DESC = [_col("table_schema"), _col("table_name"), _col("table_type")]
_L2_COL_DESC = [
    _col("column_name"),
    _col("data_type"),
    _col("is_nullable"),
    _col("column_default"),
    _col("extra"),
    _col("column_type"),
    _col("column_comment"),
]
_L2_CON_DESC = [
    _col("constraint_name"),
    _col("constraint_type"),
    _col("column_name"),
    _col("ordinal_position"),
]
_L2_FK_DESC = [
    _col("constraint_name"),
    _col("column_name"),
    _col("ordinal_position"),
    _col("ref_schema"),
    _col("ref_table"),
    _col("ref_column"),
    _col("ref_ordinal"),
]


def _l1_relation(schema, name, table_type):
    """One canned information_schema.tables row (Level-1 relations)."""
    return (schema, name, table_type)


def _l1_count(schema, name, count):
    """One canned information_schema.columns row (Level-1 column-count)."""
    return (schema, name, count)


def _l2_relation(schema, name, table_type):
    """One canned information_schema.tables row (Level-2 relations)."""
    return (schema, name, table_type)


def _l2_column(
    name,
    data_type,
    is_nullable,
    column_default=None,
    extra="",
    column_type=None,
    comment=None,
):
    """One canned information_schema.columns row (Level-2 column detail)."""
    return (name, data_type, is_nullable, column_default, extra, column_type, comment)


def _l2_constraint(name, ctype, column, ordinal):
    """One canned key_column_usage/table_constraints row (PK or UNIQUE)."""
    return (name, ctype, column, ordinal)


def _l2_fk(name, column, ordinal, ref_schema, ref_table, ref_column, ref_ordinal):
    """One canned referential_constraints row (one column of one FK)."""
    return (name, column, ordinal, ref_schema, ref_table, ref_column, ref_ordinal)


def _level1_catalog_conn(relations, counts):
    """Build a (CatalogFakeConn, CatalogFakeCursor) staging Level-1 rowsets."""
    cur = CatalogFakeCursor()
    cur.stage("information_schema.tables", _L1_REL_DESC, list(relations))
    cur.stage("COUNT(*)", _L1_COUNT_DESC, list(counts))
    return CatalogFakeConn(cur), cur


def _level2_catalog_conn(
    *,
    relations,
    columns=(),
    constraints=(),
    fks=(),
):
    """Build a (CatalogFakeConn, CatalogFakeCursor) staging Level-2 rowsets.

    Rowsets not passed default to empty; the fake cursor returns an empty
    result for any query whose substring was not staged.
    """
    cur = CatalogFakeCursor()
    cur.stage("information_schema.tables", _L2_REL_DESC, list(relations))
    cur.stage("column_comment", _L2_COL_DESC, list(columns))
    cur.stage("table_constraints", _L2_CON_DESC, list(constraints))
    cur.stage("referential_constraints", _L2_FK_DESC, list(fks))
    return CatalogFakeConn(cur), cur


# ===========================================================================
# Scenario 1: Level-1 happy path (table=None) -- list of SchemaItem
# ===========================================================================


def test_describe_schema_level1_lists_relations_sorted_excluding_system_schemas(
    fake_pymysql,
):
    """Level-1 returns SchemaItem list sorted by (schema, name); system excluded.

    Canned relations include a ``mysql.user`` system-schema row that MUST be
    filtered out by the driver. Items are sorted ascending by (schema, name)
    so 'active_users' < 'orders' < 'users'. Asserts at the DbClient boundary
    (dict shape consumers see); the driver returns SchemaItem DTOs internally.
    """
    conn, _ = _level1_catalog_conn(
        relations=[
            _l1_relation("public", "users", "BASE TABLE"),
            _l1_relation("public", "active_users", "VIEW"),
            _l1_relation("mysql", "user", "BASE TABLE"),  # system; excluded
            _l1_relation("public", "orders", "BASE TABLE"),
        ],
        counts=[
            _l1_count("public", "users", 2),
            _l1_count("public", "active_users", 1),
            _l1_count("public", "orders", 3),
        ],
    )
    client = _client_for(MySQLDriver(connectable=conn))

    result = client.describe_schema(table=None, schema=None)

    assert result == {
        "items": [
            {
                "schema": "public",
                "name": "active_users",
                "kind": "view",
                "column_count": 1,
            },
            {"schema": "public", "name": "orders", "kind": "table", "column_count": 3},
            {"schema": "public", "name": "users", "kind": "table", "column_count": 2},
        ],
        "matches": [],
    }


# ===========================================================================
# Scenario 2: Level-1 schema filter
# ===========================================================================


def test_describe_schema_level1_schema_filter_restricts_and_system_yields_empty(
    fake_pymysql,
):
    """schema='public' returns the same 3 items; schema='mysql' returns empty.

    A user schema filter restricts to that schema; a system schema filter
    yields an empty items list (the driver filters system schemas before
    applying the user filter, or equivalently treats them as no-match).
    """
    relations = [
        _l1_relation("public", "users", "BASE TABLE"),
        _l1_relation("public", "active_users", "VIEW"),
        _l1_relation("mysql", "user", "BASE TABLE"),
        _l1_relation("public", "orders", "BASE TABLE"),
    ]
    counts = [
        _l1_count("public", "users", 2),
        _l1_count("public", "active_users", 1),
        _l1_count("public", "orders", 3),
    ]

    # schema='public' -- the same 3 items as Scenario 1.
    conn, _ = _level1_catalog_conn(relations, counts)
    client = _client_for(MySQLDriver(connectable=conn))
    result = client.describe_schema(table=None, schema="public")
    assert [it["name"] for it in result["items"]] == [
        "active_users",
        "orders",
        "users",
    ]

    # schema='mysql' -- a system schema yields empty items.
    conn2, _ = _level1_catalog_conn(relations, counts)
    client2 = _client_for(MySQLDriver(connectable=conn2))
    result2 = client2.describe_schema(table=None, schema="mysql")
    assert result2 == {"items": [], "matches": []}


# ===========================================================================
# Scenario 3: Level-2 happy path -- full SchemaMatch with column, PK, FK, unique
# ===========================================================================


def test_describe_schema_level2_full_match_with_pk_fk_and_unique(fake_pymysql):
    """Level-2 returns a SchemaMatch with columns, PK, FK, unique constraint.

    Asserts at the DbClient boundary (dict shape consumers see); the driver
    returns SchemaMatch/ColumnInfo/ForeignKey/UniqueConstraint DTOs internally.
    The id column carries ``extra=''`` (no auto_increment -- covered in
    Scenario 5); the status column carries an enum ``column_type`` parsed via
    ``_parse_mysql_enum``. PK/UNIQUE columns come from the constraints query
    (key_column_usage + table_constraints); FK columns and ref columns come
    from the FK query (referential_constraints + key_column_usage) joined on
    ordinal_position for positional pairing.
    """
    conn, _ = _level2_catalog_conn(
        relations=[_l2_relation("public", "orders", "BASE TABLE")],
        columns=[
            _l2_column(
                "id",
                "int",
                "NO",
                column_default=None,
                extra="",
                column_type="int",
                comment="primary key",
            ),
            _l2_column(
                "status",
                "enum",
                "YES",
                column_default=None,
                extra="",
                column_type="enum('new','old')",
                comment=None,
            ),
        ],
        constraints=[
            _l2_constraint("PRIMARY", "PRIMARY KEY", "id", 1),
            _l2_constraint("uniq_status", "UNIQUE", "status", 1),
        ],
        fks=[
            _l2_fk(
                "fk_orders_cust",
                "customer_id",
                1,
                "public",
                "users",
                "id",
                1,
            ),
        ],
    )
    client = _client_for(MySQLDriver(connectable=conn))

    result = client.describe_schema(table="orders", schema=None)

    assert result == {
        "items": [],
        "matches": [
            {
                "schema": "public",
                "table": "orders",
                "kind": "table",
                "columns": [
                    {
                        "name": "id",
                        "data_type": "int",
                        "nullable": False,
                        "default": None,
                        "generated": None,
                        "enum_values": None,
                        "comment": "primary key",
                    },
                    {
                        "name": "status",
                        "data_type": "enum",
                        "nullable": True,
                        "default": None,
                        "generated": None,
                        "enum_values": ["new", "old"],
                        "comment": None,
                    },
                ],
                "primary_key": ["id"],
                "foreign_keys": [
                    {
                        "name": "fk_orders_cust",
                        "columns": ["customer_id"],
                        "references_schema": "public",
                        "references_table": "users",
                        "references_columns": ["id"],
                    }
                ],
                "unique_constraints": [{"name": "uniq_status", "columns": ["status"]}],
                "comment": None,
            }
        ],
    }


# ===========================================================================
# Scenario 4: _parse_mysql_enum literal parsing
# ===========================================================================


def test_parse_mysql_enum_handles_quoted_literals_embedded_commas_and_non_enum():
    """Enum parser handles common cases, embedded commas, and non-enum inputs.

    - ``enum('new','old','paid')`` -> ``["new", "old", "paid"]``
    - ``enum('a,b','c')`` -> ``["a,b", "c"]`` (embedded comma respected)
    - ``int`` -> ``None`` (doesn't start with ``enum(``)
    Never raises on anomalous input -- returns None instead.
    """
    assert _parse_mysql_enum("enum('new','old','paid')") == ["new", "old", "paid"]
    assert _parse_mysql_enum("enum('a,b','c')") == ["a,b", "c"]
    assert _parse_mysql_enum("int") is None
    # Defensive cases: non-string, mismatched prefix/suffix, empty body, and
    # an escaped single quote (MySQL doubles the quote inside literals).
    assert _parse_mysql_enum(None) is None
    assert _parse_mysql_enum("enum('a','b')") == ["a", "b"]  # tight, no spaces
    assert _parse_mysql_enum("enum( 'x' , 'y' )") == ["x", "y"]  # whitespace OK
    assert _parse_mysql_enum("enum('it''s')") == ["it's"]  # '' escape


# ===========================================================================
# Scenario 5: Auto_increment mapping (extra="auto_increment" -> generated)
# ===========================================================================


def test_describe_schema_level2_auto_increment_maps_to_by_default_identity(
    fake_pymysql,
):
    """MySQL ``extra='auto_increment'`` maps to ``generated='by_default_identity'``.

    MySQL's AUTO_INCREMENT is semantically closest to PostgreSQL's BY DEFAULT
    identity (the user can supply a value OR let it auto-generate). The
    redaction rule from PostgreSQL carries over: when generated is non-None,
    default is None regardless of what column_default reports.
    """
    conn, _ = _level2_catalog_conn(
        relations=[_l2_relation("public", "orders", "BASE TABLE")],
        columns=[
            _l2_column(
                "id",
                "int",
                "NO",
                column_default=None,
                extra="auto_increment",
                column_type="int",
                comment=None,
            ),
        ],
    )
    client = _client_for(MySQLDriver(connectable=conn))

    result = client.describe_schema(table="orders", schema=None)

    col = result["matches"][0]["columns"][0]
    assert col["name"] == "id"
    assert col["generated"] == "by_default_identity"
    assert col["default"] is None


# ===========================================================================
# Scenario 5b: Composite FK positional pairing (brief contract requirement)
# ===========================================================================


def test_describe_schema_level2_composite_fk_positional_pairing(fake_pymysql):
    """Composite FK columns and references_columns preserve ordinal pairing.

    The brief contract: "FK rows preserve positional ``columns`` ->
    ``references_columns`` pairing via
    ``information_schema.key_column_usage.ordinal_position``." The FK query
    joins local and referenced ``key_column_usage`` rows on
    ``ordinal_position``; this test verifies a two-column FK surfaces as ONE
    ForeignKey DTO with ``columns=["a","b"]`` and
    ``references_columns=["x","y"]`` (not two single-column DTOs).
    """
    conn, _ = _level2_catalog_conn(
        relations=[_l2_relation("public", "orders", "BASE TABLE")],
        columns=[
            _l2_column("a", "int", "NO"),
            _l2_column("b", "int", "NO"),
        ],
        fks=[
            _l2_fk("orders_comp_fkey", "a", 1, "public", "other", "x", 1),
            _l2_fk("orders_comp_fkey", "b", 2, "public", "other", "y", 2),
        ],
    )
    client = _client_for(MySQLDriver(connectable=conn))

    result = client.describe_schema(table="orders", schema=None)

    fks = result["matches"][0]["foreign_keys"]
    assert len(fks) == 1
    fk = fks[0]
    assert fk["name"] == "orders_comp_fkey"
    assert fk["columns"] == ["a", "b"]
    assert fk["references_schema"] == "public"
    assert fk["references_table"] == "other"
    assert fk["references_columns"] == ["x", "y"]


def test_describe_schema_level2_unknown_table_returns_empty_matches(fake_pymysql):
    """A table that doesn't exist returns empty items and matches."""
    conn, _ = _level2_catalog_conn(
        relations=[],  # no relation named 'nonexistent'
        columns=[],
    )
    client = _client_for(MySQLDriver(connectable=conn))

    result = client.describe_schema(table="nonexistent", schema=None)

    assert result == {"items": [], "matches": []}


# ===========================================================================
# Scenario 7: Level-2 schema-filtered -- no match in that schema
# ===========================================================================


def test_describe_schema_level2_schema_filter_no_match_returns_empty(fake_pymysql):
    """schema='other' with no relation in that schema returns empty matches."""
    # Relation exists in 'public' but caller asks for 'other'; the driver
    # filters the relations rowset by schema and finds no match.
    conn, _ = _level2_catalog_conn(
        relations=[_l2_relation("public", "orders", "BASE TABLE")],
        columns=[_l2_column("id", "int", "NO")],
    )
    client = _client_for(MySQLDriver(connectable=conn))

    result = client.describe_schema(table="orders", schema="other")

    assert result == {"items": [], "matches": []}


# ===========================================================================
# Scenario 8: pymysql.Error surfaces as ConnectionFailure
# ===========================================================================


def test_describe_schema_pymysql_error_surfaces_as_connection_failure(fake_pymysql):
    """A pymysql.Error during the catalog SELECT surfaces as ConnectionFailure.

    The fake cursor raises ``fake_pymysql.Error`` (a stand-in for
    ``pymysql.Error``) inside execute; the driver must catch and re-raise as
    ConnectionFailure.
    """

    class ErrorCursor:
        def __init__(self):
            self.description = None

        def execute(self, sql, params):
            raise fake_pymysql.Error("connection lost")

        def close(self):
            pass

    driver = MySQLDriver(connectable=FakeConn(ErrorCursor()))

    with pytest.raises(ConnectionFailure) as exc_info:
        driver.describe_schema(table=None, schema=None)

    assert "connection lost" in str(exc_info.value)


# --- describe_schema DTO-type contracts (mirror SQLite discipline) --------


def test_describe_schema_level1_returns_schema_item_dto_instances(fake_pymysql):
    """Level-1 items are SchemaItem DTOs (DbClient asdict's them at boundary)."""
    conn, _ = _level1_catalog_conn(
        relations=[_l1_relation("public", "orders", "BASE TABLE")],
        counts=[_l1_count("public", "orders", 3)],
    )
    driver = MySQLDriver(connectable=conn)

    result = driver.describe_schema(table=None, schema=None)

    assert result["matches"] == []
    assert all(isinstance(it, SchemaItem) for it in result["items"])


def test_describe_schema_level2_returns_schema_match_dto_instances(fake_pymysql):
    """Level-2 matches are SchemaMatch DTOs with nested DTOs."""
    conn, _ = _level2_catalog_conn(
        relations=[_l2_relation("public", "orders", "BASE TABLE")],
        columns=[_l2_column("id", "int", "NO")],
    )
    driver = MySQLDriver(connectable=conn)

    result = driver.describe_schema(table="orders", schema=None)

    assert result["items"] == []
    match = result["matches"][0]
    assert isinstance(match, SchemaMatch)
    assert all(isinstance(c, ColumnInfo) for c in match.columns)
    assert all(isinstance(fk, ForeignKey) for fk in match.foreign_keys)
    assert all(isinstance(uc, UniqueConstraint) for uc in match.unique_constraints)


def test_describe_schema_does_not_close_injected_connection(fake_pymysql):
    """An injected connectable is not closed after describe_schema."""
    conn, _ = _level1_catalog_conn(relations=[], counts=[])
    driver = MySQLDriver(connectable=conn)

    driver.describe_schema(table=None, schema=None)

    assert conn.closed is False


def test_describe_schema_level2_passes_table_and_schema_as_positional_params(
    fake_pymysql,
):
    """Catalog queries use PyMySQL ``%s`` placeholders and positional params.

    The brief specifies that catalog queries use ``%s`` placeholders
    (PyMySQL's native paramstyle) -- NOT ``convert_sql_params``, which is
    only for user SQL. Verify the bind values reach the relations query as
    a tuple/list (positional), not a dict.
    """
    conn, cur = _level2_catalog_conn(
        relations=[_l2_relation("public", "orders", "BASE TABLE")],
        columns=[_l2_column("id", "int", "NO")],
    )
    driver = MySQLDriver(connectable=conn)

    driver.describe_schema(table="orders", schema="public")

    # The Level-2 relations query is the one filtering by table_name; find
    # it in the call history (Level-2 issues several SELECTs).
    rel_calls = [h for h in cur.history if "information_schema.tables" in h[0]]
    assert rel_calls, "relations query was not issued"
    rel_sql, rel_params = rel_calls[0]
    # PyMySQL positional paramstyle: params is a tuple/list, not a dict.
    assert not isinstance(rel_params, dict)
    # Both bind values are present in order. The exact parameter order is
    # the implementer's choice, but both ``orders`` and ``public`` must be
    # bound (not interpolated into the SQL).
    assert "orders" in list(rel_params)
    assert "public" in list(rel_params)
    # No literal identifier interpolation: the SQL has ``%s`` placeholders.
    assert "%s" in rel_sql
