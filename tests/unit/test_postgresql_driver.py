"""Tests for the PostgreSQL DB driver and DBDriver protocol (DESIGN §9.1, §7)."""

import datetime
import decimal
import types

import pytest

from agctl.assertions import coerce_db_value
from agctl.clients.db_driver_protocol import DBDriver
from agctl.clients.db_drivers.postgresql import PostgreSQLDriver
from agctl.errors import ConfigError, ConnectionFailure


# --- Test seams -----------------------------------------------------------

class FakeCursor:
    def __init__(self, description, rows, rowcount=1):
        self.description = description  # sequence of objects with .name
        self._rows = rows
        self.rowcount = rowcount  # for write operations
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


def _col(name):
    return types.SimpleNamespace(name=name)


# --- SQL translation ------------------------------------------------------

def test_execute_translates_named_params_to_psycopg_style():
    cur = FakeCursor(description=[_col("id")], rows=[("o9",)])
    driver = PostgreSQLDriver(connectable=FakeConn(cur))

    driver.execute("SELECT id FROM orders WHERE id = :orderId", {"orderId": "o9"})

    assert cur.last_sql == "SELECT id FROM orders WHERE id = %(orderId)s"
    assert cur.last_params == {"orderId": "o9"}


def test_execute_returns_no_rows_for_empty_result():
    cur = FakeCursor(description=[_col("id")], rows=[])
    driver = PostgreSQLDriver(connectable=FakeConn(cur))

    result = driver.execute("SELECT id FROM orders WHERE id = :orderId", {"orderId": "missing"})

    assert result == []


# --- Result coercion ------------------------------------------------------

def test_execute_returns_dict_rows_keyed_by_column_name_with_coerced_values():
    cols = [_col("id"), _col("total"), _col("created")]
    row = ("o9", decimal.Decimal("4999"), datetime.datetime(2026, 6, 29, 14, 22, 0))
    cur = FakeCursor(description=cols, rows=[row])
    driver = PostgreSQLDriver(connectable=FakeConn(cur))

    result = driver.execute("SELECT * FROM orders", {})

    assert result == [
        {
            "id": "o9",
            "total": 4999,
            "created": "2026-06-29T14:22:00",
        }
    ]


def test_execute_preserves_row_order_for_multiple_rows():
    cols = [_col("id"), _col("total")]
    rows = [
        ("o9", decimal.Decimal("4999")),
        ("o10", decimal.Decimal("12.50")),
        ("o11", decimal.Decimal("0")),
    ]
    cur = FakeCursor(description=cols, rows=rows)
    driver = PostgreSQLDriver(connectable=FakeConn(cur))

    result = driver.execute("SELECT id, total FROM orders", {})

    assert result == [
        {"id": "o9", "total": 4999},
        {"id": "o10", "total": 12.50},
        {"id": "o11", "total": 0},
    ]


def test_coerce_db_value_decimal_integral_is_int():
    assert coerce_db_value(decimal.Decimal("4999")) == 4999
    assert isinstance(coerce_db_value(decimal.Decimal("4999")), int)


def test_coerce_db_value_decimal_fractional_is_float():
    assert coerce_db_value(decimal.Decimal("12.50")) == 12.50
    assert isinstance(coerce_db_value(decimal.Decimal("12.50")), float)


# --- close() ownership semantics -----------------------------------------

def test_close_does_not_close_injected_connection():
    cur = FakeCursor(description=[_col("id")], rows=[("o9",)])
    conn = FakeConn(cur)
    driver = PostgreSQLDriver(connectable=conn)

    driver.close()

    assert conn.closed is False


def test_close_closes_owned_connection():
    cur = FakeCursor(description=[_col("id")], rows=[("o9",)])
    conn = FakeConn(cur)
    driver = PostgreSQLDriver()  # no connectable -> owned
    driver._conn = conn
    driver._owned = True

    driver.close()

    assert conn.closed is True


def test_close_is_safe_when_no_connection():
    driver = PostgreSQLDriver()  # no connection ever set
    driver.close()  # must not raise


# --- connect() / protocol -------------------------------------------------

def test_connect_is_noop_when_connection_already_injected():
    cur = FakeCursor(description=[_col("id")], rows=[("o9",)])
    conn = FakeConn(cur)
    driver = PostgreSQLDriver(connectable=conn)

    # Even with a config, the injected connection must be kept.
    driver.connect({"host": "x"})

    assert driver._conn is conn


def test_connect_raises_connection_failure_on_operational_error(monkeypatch):
    import psycopg
    import psycopg.errors

    def _boom(*args, **kwargs):
        raise psycopg.errors.OperationalError("nope")

    monkeypatch.setattr(psycopg, "connect", _boom)

    driver = PostgreSQLDriver()
    with pytest.raises(ConnectionFailure):
        driver.connect({"host": "x"})


def test_protocol_is_satisfied_by_postgresql_driver():
    # runtime_checkable structural check: PostgreSQLDriver has all 3 methods.
    driver = PostgreSQLDriver()
    assert hasattr(driver, "connect")
    assert hasattr(driver, "execute")
    assert hasattr(driver, "close")
    # DBDriver protocol is importable and usable.
    assert DBDriver is not None


# --- execute_write() tests ---------------------------------------------------


def test_execute_write_insert_with_returning_returns_rows_affected_and_returning_rows():
    """Test 1: INSERT with RETURNING (rowcount==1, description present)."""
    cols = [_col("id"), _col("status")]
    rows = [(1, "pending")]
    cur = FakeCursor(description=cols, rows=rows, rowcount=1)
    conn = FakeConn(cur)
    driver = PostgreSQLDriver(connectable=conn)

    result = driver.execute_write(
        "INSERT INTO orders (total) VALUES (100) RETURNING id, status", {}
    )

    assert result == {"rows_affected": 1, "returning": [{"id": 1, "status": "pending"}]}
    assert conn.commit_called is True
    assert conn.rollback_called is False
    assert cur.last_sql == "INSERT INTO orders (total) VALUES (100) RETURNING id, status"


def test_execute_write_plain_write_no_returning():
    """Test 2: Plain write no RETURNING (rowcount==3, description is None)."""
    cur = FakeCursor(description=None, rows=[], rowcount=3)
    conn = FakeConn(cur)
    driver = PostgreSQLDriver(connectable=conn)

    result = driver.execute_write("UPDATE orders SET status = :status", {"status": "shipped"})

    assert result == {"rows_affected": 3, "returning": []}
    assert conn.commit_called is True
    assert conn.rollback_called is False
    assert cur.last_sql == "UPDATE orders SET status = %(status)s"
    assert cur.last_params == {"status": "shipped"}


def test_execute_write_no_count_statement_ddl():
    """Test 3: No-count statement / DDL (rowcount==-1, description is None)."""
    cur = FakeCursor(description=None, rows=[], rowcount=-1)
    conn = FakeConn(cur)
    driver = PostgreSQLDriver(connectable=conn)

    result = driver.execute_write("CREATE TABLE foo (id INT)", {})

    assert result == {"rows_affected": None, "returning": []}
    assert conn.commit_called is True
    assert conn.rollback_called is False


def test_execute_write_zero_affected_with_returning():
    """Test 4: 0-affected (rowcount==0, description present)."""
    cols = [_col("id")]
    rows = []
    cur = FakeCursor(description=cols, rows=rows, rowcount=0)
    conn = FakeConn(cur)
    driver = PostgreSQLDriver(connectable=conn)

    result = driver.execute_write(
        "UPDATE orders SET status = 'shipped' WHERE id = :id RETURNING id", {"id": 999}
    )

    assert result == {"rows_affected": 0, "returning": []}
    assert conn.commit_called is True
    assert conn.rollback_called is False


def test_execute_write_rollback_on_coercion_error():
    """Test 5: Coercion-error ordering guarantee (non-psycopg exception)."""
    cols = [_col("id")]

    class BrokenCursor:
        def __init__(self):
            self.description = cols
            self.rowcount = 1
            self.last_sql = None
            self.last_params = None

        def execute(self, sql, params):
            self.last_sql = sql
            self.last_params = params

        def fetchall(self):
            raise RuntimeError("Coercion/materialization failure")

        def close(self):
            pass

    cur = BrokenCursor()
    conn = FakeConn(cur)
    driver = PostgreSQLDriver(connectable=conn)

    with pytest.raises(ConnectionFailure) as exc_info:
        driver.execute_write("INSERT INTO orders VALUES (1) RETURNING id", {})

    assert "Coercion/materialization failure" in str(exc_info.value)
    assert conn.rollback_called is True
    assert conn.commit_called is False


def test_execute_write_rollback_on_execute_error():
    """Test 6: Execute-error path (psycopg.Error)."""
    import psycopg

    class ErrorCursor:
        def __init__(self):
            self.description = None
            self.rowcount = -1

        def execute(self, sql, params):
            raise psycopg.Error("Connection lost")

        def close(self):
            pass

    cur = ErrorCursor()
    conn = FakeConn(cur)
    driver = PostgreSQLDriver(connectable=conn)

    with pytest.raises(ConnectionFailure) as exc_info:
        driver.execute_write("UPDATE orders SET status = 'shipped'", {})

    assert "Connection lost" in str(exc_info.value)
    assert conn.rollback_called is True
    assert conn.commit_called is False


def test_execute_write_rollback_on_commit_failure():
    """Test 7: Commit failure after successful execute and materialization."""
    cols = [_col("id"), _col("status")]
    rows = [(1, "pending")]
    cur = FakeCursor(description=cols, rows=rows, rowcount=1)
    conn = FakeConn(cur, commit_raises=True)
    driver = PostgreSQLDriver(connectable=conn)

    with pytest.raises(ConnectionFailure) as exc_info:
        driver.execute_write(
            "INSERT INTO orders (total) VALUES (100) RETURNING id, status", {}
        )

    assert "network failure" in str(exc_info.value)
    assert conn.rollback_called is True
    assert conn.commit_called is True
    assert isinstance(exc_info.value, ConnectionFailure)
    assert not isinstance(exc_info.value.__cause__, ConnectionFailure)


def test_execute_write_does_not_close_injected_connection():
    """Test 8: Injected connectable still not closed after a write."""
    cur = FakeCursor(description=None, rows=[], rowcount=1)
    conn = FakeConn(cur)
    driver = PostgreSQLDriver(connectable=conn)

    driver.execute_write("UPDATE orders SET status = 'shipped'", {})

    assert conn.closed is False


# --- describe_schema() Level 1 test seams ---------------------------------


class CatalogFakeCursor:
    """Cursor that dispatches staged result sets by SQL substring.

    ``execute(sql, params)`` inspects ``sql`` and stages the canned
    ``(description, rows)`` registered for the first matching substring;
    ``description`` / ``fetchall()`` return the currently staged values.

    This double is reused and extended in Task 3 for the Level-2 catalog
    SELECTs (column / constraint queries) -- stage any number of queries,
    each keyed by a distinguishing SQL substring.
    """

    def __init__(self):
        self._staged = []  # list of (sql_substring, description, rows)
        self.description = None
        self._rows = []
        self.last_sql = None
        self.last_params = None

    def stage(self, sql_substring, description, rows):
        self._staged.append((sql_substring, description, rows))

    def execute(self, sql, params):
        self.last_sql = sql
        self.last_params = params
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

    ``describe_schema`` issues read-only catalog SELECTs and never commits, so
    this double intentionally has no ``commit`` / ``rollback`` -- only
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


# Level-1 canned pg_class rowset shape. Alias names are the implementer's
# choice (the contract asserts on the normalized output, not the SQL); they
# line up positionally with the driver's SELECT column order.
_RELATION_DESC = [
    _col("schema_name"),
    _col("relation_name"),
    _col("relkind"),
    _col("relispartition"),
    _col("column_count"),
]


def _relation(schema, name, relkind, relispartition, column_count):
    """Build one canned pg_class row tuple in the Level-1 column order."""
    return (schema, name, relkind, relispartition, column_count)


def _catalog_conn_with_relations(*relations):
    """Build a (CatalogFakeConn, CatalogFakeCursor) serving a Level-1 rowset.

    The relation rowset is keyed on the ``pg_class`` SQL substring -- the
    driver's Level-1 catalog SELECT always reads ``FROM pg_class``.
    """
    cur = CatalogFakeCursor()
    cur.stage("pg_class", _RELATION_DESC, list(relations))
    return CatalogFakeConn(cur), cur


# --- describe_schema() Level 1 tests --------------------------------------


def test_describe_schema_level1_mixed_tables_and_view():
    """Scenario 1: two tables + one view, normalized and sorted by (schema, name)."""
    conn, _ = _catalog_conn_with_relations(
        _relation("public", "orders", "r", False, 6),
        _relation("public", "order_items", "r", False, 4),
        _relation("public", "order_view", "v", False, 3),
    )
    driver = PostgreSQLDriver(connectable=conn)

    result = driver.describe_schema(table=None, schema=None)

    # Ascending string sort: '_' (0x5F) precedes lowercase letters, so
    # "order_items" < "order_view" < "orders".
    assert result == {
        "items": [
            {"schema": "public", "name": "order_items", "kind": "table", "column_count": 4},
            {"schema": "public", "name": "order_view", "kind": "view", "column_count": 3},
            {"schema": "public", "name": "orders", "kind": "table", "column_count": 6},
        ]
    }


def test_describe_schema_level1_excludes_system_schemas():
    """Scenario 2 (D6): pg_* and information_schema relations are excluded."""
    conn, _ = _catalog_conn_with_relations(
        _relation("public", "orders", "r", False, 6),
        _relation("pg_catalog", "pg_class", "r", False, 12),
        _relation("information_schema", "columns", "v", False, 9),
    )
    driver = PostgreSQLDriver(connectable=conn)

    result = driver.describe_schema(table=None, schema=None)

    assert result == {
        "items": [
            {"schema": "public", "name": "orders", "kind": "table", "column_count": 6},
        ]
    }


def test_describe_schema_level1_excludes_partition_leaves():
    """Scenario 3 (D6): partition leaf (relispartition=true) excluded; parent kept."""
    conn, _ = _catalog_conn_with_relations(
        _relation("public", "orders", "p", False, 6),  # partitioned parent
        _relation("public", "orders_p2026", "r", True, 6),  # partition leaf
    )
    driver = PostgreSQLDriver(connectable=conn)

    result = driver.describe_schema(table=None, schema=None)

    assert result == {
        "items": [
            {"schema": "public", "name": "orders", "kind": "table", "column_count": 6},
        ]
    }


def test_describe_schema_level1_excludes_matview_and_sequence():
    """Scenario 4: relkind 'm' (matview) and 'S' (sequence) are excluded from v1."""
    conn, _ = _catalog_conn_with_relations(
        _relation("public", "orders", "r", False, 6),
        _relation("public", "orders_summary", "m", False, 3),
        _relation("public", "orders_id_seq", "S", False, 1),
    )
    driver = PostgreSQLDriver(connectable=conn)

    result = driver.describe_schema(table=None, schema=None)

    assert result == {
        "items": [
            {"schema": "public", "name": "orders", "kind": "table", "column_count": 6},
        ]
    }


def test_describe_schema_level1_schema_filter_passed_as_bind_param():
    """Scenario 5: --schema restricts to one namespace and reaches SQL as a bind param."""
    conn, cur = _catalog_conn_with_relations(
        _relation("public", "orders", "r", False, 6),
        _relation("analytics", "events", "r", False, 5),
        _relation("analytics", "events_view", "v", False, 4),
    )
    driver = PostgreSQLDriver(connectable=conn)

    result = driver.describe_schema(table=None, schema="analytics")

    assert result == {
        "items": [
            {"schema": "analytics", "name": "events", "kind": "table", "column_count": 5},
            {"schema": "analytics", "name": "events_view", "kind": "view", "column_count": 4},
        ]
    }
    # The schema value reaches the query as a bind parameter (never interpolated).
    assert cur.last_params == {"schema": "analytics"}
    assert "%(schema)s" in cur.last_sql
    assert "analytics" not in cur.last_sql


def test_describe_schema_level1_unknown_schema_returns_empty_not_error():
    """Scenario 6: an empty/unknown --schema yields items == [], not an error."""
    conn, _ = _catalog_conn_with_relations(
        _relation("public", "orders", "r", False, 6),
    )
    driver = PostgreSQLDriver(connectable=conn)

    result = driver.describe_schema(table=None, schema="nope")

    assert result == {"items": []}


def test_describe_schema_level1_does_not_close_injected_connection():
    """Scenario 7: an injected connectable is not closed after describe_schema."""
    conn, _ = _catalog_conn_with_relations(
        _relation("public", "orders", "r", False, 6),
    )
    driver = PostgreSQLDriver(connectable=conn)

    driver.describe_schema(table=None, schema=None)

    assert conn.closed is False
