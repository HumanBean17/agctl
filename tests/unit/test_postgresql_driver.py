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
    def __init__(self, description, rows):
        self.description = description  # sequence of objects with .name
        self._rows = rows
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
    def __init__(self, cursor):
        self._cursor = cursor
        self.closed = False

    def cursor(self):
        return self._cursor

    def close(self):
        self.closed = True


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
