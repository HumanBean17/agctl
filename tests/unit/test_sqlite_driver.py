"""Tests for the SQLite DBDriver skeleton (connect/execute/execute_write/close).

Test discipline (deliberate deviation from the PostgreSQL pattern): these
tests use REAL in-memory ``sqlite3`` connections (no FakeCursor/FakeConn).
Stdlib ``sqlite3`` is trivially instantiable, and exercising the real cursor /
description / rowcount paths surfaces more realistic behavior than fakes would
(e.g. RETURNING support, in_transaction, rollback semantics). See the task
brief's Test Discipline note.
"""

from __future__ import annotations

import sqlite3

import pytest

from agctl.clients.db_driver_protocol import DBDriver, WriteResult
from agctl.clients.db_drivers.sqlite import SQLiteDriver
from agctl.errors import ConnectionFailure


# ---------------------------------------------------------------------------
# Scenario 1: end-to-end CREATE / INSERT / SELECT with injected connection
# ---------------------------------------------------------------------------


def test_execute_create_insert_select_round_trip():
    """CREATE TABLE, INSERT, SELECT round-trip preserves integer and dict shape."""
    driver = SQLiteDriver(connectable=sqlite3.connect(":memory:"))
    try:
        driver.execute("CREATE TABLE t (id INTEGER)", {})
        driver.execute("INSERT INTO t VALUES (1)", {})
        rows = driver.execute("SELECT id FROM t", {})
    finally:
        driver.close()

    assert rows == [{"id": 1}]


# ---------------------------------------------------------------------------
# Scenario 2: named :param placeholder is passed through UNCHANGED
# ---------------------------------------------------------------------------


def test_execute_named_placeholder_passes_through_unchanged():
    """SQLite natively accepts :name params; no convert_sql_params rewrite."""
    conn = sqlite3.connect(":memory:")
    driver = SQLiteDriver(connectable=conn)
    try:
        driver.execute(
            "CREATE TABLE orders (id TEXT, status TEXT, total_cents INTEGER)",
            {},
        )
        driver.execute(
            "INSERT INTO orders VALUES (:id, :status, :total_cents)",
            {"id": "o9", "status": "CONFIRMED", "total_cents": 1500},
        )
        rows = driver.execute(
            "SELECT * FROM orders WHERE id = :orderId",
            {"orderId": "o9"},
        )
    finally:
        driver.close()

    assert rows == [
        {"id": "o9", "status": "CONFIRMED", "total_cents": 1500}
    ]


# ---------------------------------------------------------------------------
# Scenario 3: execute_write returns WriteResult(rows_affected=1, returning=[])
# ---------------------------------------------------------------------------


def test_execute_write_returns_rows_affected_without_returning():
    """A plain INSERT returns rows_affected=1 and an empty returning list."""
    driver = SQLiteDriver(connectable=sqlite3.connect(":memory:"))
    try:
        driver.execute("CREATE TABLE t (id INTEGER)", {})
        result = driver.execute_write(
            "INSERT INTO t VALUES (:v)",
            {"v": 42},
        )
    finally:
        driver.close()

    assert isinstance(result, WriteResult)
    assert result.rows_affected == 1
    assert result.returning == []


# ---------------------------------------------------------------------------
# Scenario 4: execute_write with RETURNING (SQLite >= 3.35)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    sqlite3.sqlite_version_info < (3, 35, 0),
    reason="RETURNING clause requires SQLite >= 3.35",
)
def test_execute_write_with_returning_materializes_rows():
    """INSERT ... RETURNING * surfaces in WriteResult.returning."""
    driver = SQLiteDriver(connectable=sqlite3.connect(":memory:"))
    try:
        driver.execute("CREATE TABLE t (id INTEGER)", {})
        result = driver.execute_write(
            "INSERT INTO t VALUES (1) RETURNING *",
            {},
        )
    finally:
        driver.close()

    assert isinstance(result, WriteResult)
    assert result.rows_affected == 1
    assert result.returning == [{"id": 1}]


# ---------------------------------------------------------------------------
# Scenario 5: execute_write rolls back on error, leaving clean state
# ---------------------------------------------------------------------------


def test_execute_write_rolls_back_on_error():
    """A NOT NULL violation raises ConnectionFailure and rolls the tx back.

    After the failed write, the table must contain zero rows (rollback worked,
    no partial insert). The connection's ``in_transaction`` flag is consulted
    BEFORE the failing write (via an explicit ``BEGIN``) to prove the rollback
    path operates on an actually-open transaction, and again AFTER to confirm
    the rollback closed it.
    """
    conn = sqlite3.connect(":memory:")
    driver = SQLiteDriver(connectable=conn)
    try:
        driver.execute("CREATE TABLE t (id INTEGER NOT NULL)", {})

        # CREATE TABLE auto-commits in sqlite3's default isolation_level mode;
        # explicitly open a tx so in_transaction is observable before the
        # failing write (proving the rollback path operates on a real tx).
        conn.execute("BEGIN")
        assert conn.in_transaction, "tx must be open before the failing write"

        with pytest.raises(ConnectionFailure):
            driver.execute_write(
                "INSERT INTO t (id) VALUES (NULL)",
                {},
            )

        # Rollback closed the transaction and restored a clean state.
        assert not conn.in_transaction, "tx must be closed by the rollback"
        rows = driver.execute("SELECT COUNT(*) AS c FROM t", {})
        assert rows == [{"c": 0}]
    finally:
        driver.close()


# ---------------------------------------------------------------------------
# Scenario 6: connect({":memory:"}) on a fresh driver; subsequent execute works
# ---------------------------------------------------------------------------


def test_connect_in_memory_then_execute():
    """A fresh SQLiteDriver() (no connectable) connects to :memory: and serves reads."""
    driver = SQLiteDriver()
    try:
        driver.connect({"url": ":memory:"})
        rows = driver.execute("SELECT 1 AS x", {})
    finally:
        driver.close()

    assert rows == [{"x": 1}]


# ---------------------------------------------------------------------------
# Scenario 7: connect with URI-mode URL
# ---------------------------------------------------------------------------


def test_connect_uri_mode_shared_cache():
    """A `file:` URL enables uri mode; shared-cache in-memory DB serves reads."""
    driver = SQLiteDriver()
    try:
        driver.connect({"url": "file::memory:?cache=shared"})
        rows = driver.execute("SELECT 1 AS x", {})
    finally:
        driver.close()

    assert rows == [{"x": 1}]


# ---------------------------------------------------------------------------
# Scenario 8: connect() failure -> ConnectionFailure(driver="sqlite", config=...)
# ---------------------------------------------------------------------------


def test_connect_failure_raises_connection_failure_with_driver_label():
    """A path SQLite cannot open raises ConnectionFailure; path is NOT redacted.

    SQLite config has no secret keys, so the redactor leaves the url verbatim
    in the detail (load-bearing: the agent needs the bad path to self-correct).
    """
    driver = SQLiteDriver()
    with pytest.raises(ConnectionFailure) as exc_info:
        driver.connect({"url": "/nonexistent/path/db.sqlite"})

    detail = exc_info.value.detail
    assert detail["driver"] == "sqlite"
    # Path is NOT redacted — no secret keys in SQLite config.
    assert detail["config"]["url"] == "/nonexistent/path/db.sqlite"


# ---------------------------------------------------------------------------
# Scenario 9: close() ownership — owned closes, injected does not
# ---------------------------------------------------------------------------


def test_close_owned_connection_closes_it():
    """A driver that built its own connection closes it on close()."""
    driver = SQLiteDriver()
    driver.connect({"url": ":memory:"})

    driver.close()

    # The closed connection must reject further queries.
    with pytest.raises(Exception):
        driver.execute("SELECT 1 AS x", {})


def test_close_injected_connection_does_not_close_it():
    """A driver given an injected connection leaves it open on close()."""
    conn = sqlite3.connect(":memory:")
    driver = SQLiteDriver(connectable=conn)

    driver.close()

    # The original connection is still usable.
    cur = conn.cursor()
    cur.execute("SELECT 1 AS x")
    assert cur.fetchall() == [(1,)]
    conn.close()


# ---------------------------------------------------------------------------
# Scenario 10: SQLiteDriver satisfies the DBDriver Protocol (runtime check)
# ---------------------------------------------------------------------------


def test_sqlite_driver_satisfies_dbdriver_protocol():
    """SQLiteDriver is a structural match for the runtime-checkable DBDriver."""
    assert isinstance(SQLiteDriver(), DBDriver)
