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

from agctl.clients.db_driver_protocol import DBDriver, WriteResult
from agctl.clients.db_drivers.mysql import MySQLDriver
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

    driver.execute(
        "SELECT id FROM t WHERE id = :orderId", {"orderId": "o9"}
    )

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


def test_execute_write_rolls_back_on_pymysql_error_and_raises_connection_failure(fake_pymysql):
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
