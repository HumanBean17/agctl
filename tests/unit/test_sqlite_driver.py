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

from agctl.clients.db_driver_protocol import (
    ColumnInfo,
    DBDriver,
    ForeignKey,
    SchemaItem,
    SchemaMatch,
    UniqueConstraint,
    WriteResult,
)
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


# ===========================================================================
# describe_schema (Level-1 + Level-2)
#
# The SQLite-specific contract (DESIGN §9.1 / Task 5 brief):
#   - PRAGMAs do NOT accept bind parameters in sqlite3, so identifiers are
#     interpolated via f-string AFTER regex validation (^[A-Za-z_][A-Za-z0-9_]*$).
#     Validation is the ONLY injection-defense mechanism: rejection, not escape.
#   - schema is always "main" for v1 (attached DBs are out of scope); a
#     non-"main" schema argument is silently ignored.
#   - FKs are anonymous in SQLite -> ForeignKey.name is None.
#   - enum_values always None (SQLite has no enum type).
#   - comment always None (SQLite has no native comment metadata).
# ===========================================================================


def _seed_orders_users_unique(driver):
    """Seed the brief's canonical Level-2 fixture.

    CREATE TABLE orders(id INTEGER PRIMARY KEY,
                        customer_id INTEGER NOT NULL DEFAULT 5,
                        status TEXT,
                        FOREIGN KEY(customer_id) REFERENCES users(id))
    CREATE TABLE users(id INTEGER PRIMARY KEY)
    CREATE UNIQUE INDEX idx_orders_status ON orders(status)
    """
    driver.execute("CREATE TABLE users(id INTEGER PRIMARY KEY)", {})
    driver.execute(
        "CREATE TABLE orders("
        "id INTEGER PRIMARY KEY, "
        "customer_id INTEGER NOT NULL DEFAULT 5, "
        "status TEXT, "
        "FOREIGN KEY(customer_id) REFERENCES users(id))",
        {},
    )
    driver.execute(
        "CREATE UNIQUE INDEX idx_orders_status ON orders(status)", {}
    )


# ---------------------------------------------------------------------------
# Scenario 11: Level-1 happy path (table=None) — list of SchemaItem
# ---------------------------------------------------------------------------


def test_describe_schema_level_1_lists_tables_and_views_sorted_by_name():
    """Level-1 returns SchemaItem list sorted by name; sqlite_* tables excluded.

    Seeds users (2 cols) + orders (3 cols) + active_users (view). Asserts the
    items list is sorted ascending by name, that internal sqlite_* tables
    (auto-created for the FK constraint) do not appear, and that kinds map
    correctly (table vs view).
    """
    driver = SQLiteDriver(connectable=sqlite3.connect(":memory:"))
    try:
        driver.execute(
            "CREATE TABLE users(id INTEGER, name TEXT)", {}
        )
        driver.execute(
            "CREATE TABLE orders(id INTEGER, customer_id INTEGER, status TEXT)",
            {},
        )
        driver.execute(
            "CREATE VIEW active_users AS SELECT id, name FROM users WHERE name IS NOT NULL",
            {},
        )
        result = driver.describe_schema(table=None, schema=None)
    finally:
        driver.close()

    assert result["matches"] == []
    items = result["items"]
    # Sorted by name ascending: active_users, orders, users.
    assert [it.name for it in items] == ["active_users", "orders", "users"]
    assert [it.schema for it in items] == ["main", "main", "main"]
    assert [it.kind for it in items] == ["view", "table", "table"]
    assert [it.column_count for it in items] == [2, 3, 2]
    # All entries are SchemaItem instances (not dicts): DbClient asdict's them.
    assert all(isinstance(it, SchemaItem) for it in items)
    # No internal sqlite_* auto-index tables leak through.
    assert all(not it.name.startswith("sqlite_") for it in items)


# ---------------------------------------------------------------------------
# Scenario 12: Level-1 empty DB — no user relations
# ---------------------------------------------------------------------------


def test_describe_schema_level_1_empty_db_returns_empty_items():
    """A fresh in-memory DB (no user tables) returns empty items and matches."""
    driver = SQLiteDriver(connectable=sqlite3.connect(":memory:"))
    try:
        result = driver.describe_schema(table=None, schema=None)
    finally:
        driver.close()

    assert result == {"items": [], "matches": []}


# ---------------------------------------------------------------------------
# Scenario 13: Level-2 happy path — full SchemaMatch with FK + unique index
# ---------------------------------------------------------------------------


def test_describe_schema_level_2_returns_full_match_with_fk_and_unique():
    """Level-2 returns a SchemaMatch with columns, PK, FK (anonymous), unique idx.

    Uses the canonical orders/users fixture: orders has id (PK), customer_id
    (NOT NULL DEFAULT 5, FK -> users.id), status (TEXT, covered by a unique
    index named idx_orders_status). Verifies the brief's exact DTO shapes:
      - columns: nullable inverts notnull, default is the raw dflt_value string,
        enum_values/comment/generated all None for plain columns.
      - primary_key: ["id"] from table_info's pk flag.
      - foreign_keys: anonymous (name=None) with positional columns preserved.
      - unique_constraints: one entry from index_list filtered to origin=='u'.
    """
    driver = SQLiteDriver(connectable=sqlite3.connect(":memory:"))
    try:
        _seed_orders_users_unique(driver)
        result = driver.describe_schema(table="orders", schema=None)
    finally:
        driver.close()

    assert result["items"] == []
    matches = result["matches"]
    assert len(matches) == 1
    match = matches[0]
    assert isinstance(match, SchemaMatch)

    # Top-level fields.
    assert match.schema == "main"
    assert match.table == "orders"
    assert match.kind == "table"
    assert match.comment is None

    # Columns (ordered to match the catalog).
    assert match.columns == [
        ColumnInfo(
            name="id",
            data_type="INTEGER",
            nullable=True,  # INTEGER PRIMARY KEY -> notnull=0 in table_info.
            default=None,
            generated=None,
            enum_values=None,
            comment=None,
        ),
        ColumnInfo(
            name="customer_id",
            data_type="INTEGER",
            nullable=False,
            default="5",
            generated=None,
            enum_values=None,
            comment=None,
        ),
        ColumnInfo(
            name="status",
            data_type="TEXT",
            nullable=True,
            default=None,
            generated=None,
            enum_values=None,
            comment=None,
        ),
    ]
    assert all(isinstance(c, ColumnInfo) for c in match.columns)

    # PK.
    assert match.primary_key == ["id"]

    # FK (anonymous: name=None).
    assert match.foreign_keys == [
        ForeignKey(
            name=None,
            columns=["customer_id"],
            references_schema=None,
            references_table="users",
            references_columns=["id"],
        )
    ]
    assert all(isinstance(fk, ForeignKey) for fk in match.foreign_keys)

    # Unique constraint (from the unique index).
    assert match.unique_constraints == [
        UniqueConstraint(name="idx_orders_status", columns=["status"])
    ]
    assert all(
        isinstance(uc, UniqueConstraint) for uc in match.unique_constraints
    )


# ---------------------------------------------------------------------------
# Scenario 14: Level-2 not found — empty matches
# ---------------------------------------------------------------------------


def test_describe_schema_level_2_unknown_table_returns_empty_matches():
    """A table that doesn't exist returns empty items and matches."""
    driver = SQLiteDriver(connectable=sqlite3.connect(":memory:"))
    try:
        driver.execute("CREATE TABLE t(id INTEGER)", {})
        result = driver.describe_schema(table="nonexistent", schema=None)
    finally:
        driver.close()

    assert result == {"items": [], "matches": []}


# ---------------------------------------------------------------------------
# Scenario 15: Level-2 view — kind="view", empty constraints
# ---------------------------------------------------------------------------


def test_describe_schema_level_2_view_has_empty_pk_fk_unique():
    """A view returns kind='view' with empty PK / FK / unique-constraint lists."""
    driver = SQLiteDriver(connectable=sqlite3.connect(":memory:"))
    try:
        driver.execute("CREATE TABLE users(id INTEGER, name TEXT)", {})
        driver.execute(
            "CREATE VIEW my_view AS SELECT id FROM users WHERE id > 0", {}
        )
        result = driver.describe_schema(table="my_view", schema=None)
    finally:
        driver.close()

    matches = result["matches"]
    assert len(matches) == 1
    match = matches[0]
    assert match.kind == "view"
    assert match.table == "my_view"
    assert match.primary_key == []
    assert match.foreign_keys == []
    assert match.unique_constraints == []
    assert match.comment is None
    # View still has a column list.
    assert [c.name for c in match.columns] == ["id"]


# ---------------------------------------------------------------------------
# Scenario 16: Identifier injection rejection — SQL in the table arg
# ---------------------------------------------------------------------------


def test_describe_schema_identifier_injection_raises_connection_failure():
    """A table name containing SQL injection payload is rejected by the validator.

    The regex ^[A-Za-z_][A-Za-z0-9_]*$ refuses anything with punctuation,
    whitespace, or semicolons, so the payload never reaches a PRAGMA. The
    surfaced error is ConnectionFailure with the offending identifier in detail.
    """
    driver = SQLiteDriver(connectable=sqlite3.connect(":memory:"))
    try:
        with pytest.raises(ConnectionFailure) as exc_info:
            driver.describe_schema(
                table="t; DROP TABLE users; --", schema=None
            )
    finally:
        driver.close()

    # Detail echoes the rejected identifier so downstream can self-correct.
    assert exc_info.value.detail["identifier"] == "t; DROP TABLE users; --"


# ---------------------------------------------------------------------------
# Scenario 17: Identifier with quote rejection
# ---------------------------------------------------------------------------


def test_describe_schema_identifier_with_quote_raises_connection_failure():
    """A table name containing a double-quote is rejected by the validator."""
    driver = SQLiteDriver(connectable=sqlite3.connect(":memory:"))
    try:
        with pytest.raises(ConnectionFailure) as exc_info:
            driver.describe_schema(table='t"', schema=None)
    finally:
        driver.close()

    assert exc_info.value.detail["identifier"] == 't"'


# ---------------------------------------------------------------------------
# Scenario 18: enum_values always None (SQLite has no enum type)
# ---------------------------------------------------------------------------


def test_describe_schema_columns_enum_values_always_none():
    """No SQLite column carries enum_values; all are None regardless of type."""
    driver = SQLiteDriver(connectable=sqlite3.connect(":memory:"))
    try:
        driver.execute(
            "CREATE TABLE t(a TEXT, b INTEGER, c BLOB, d REAL)", {}
        )
        result = driver.describe_schema(table="t", schema=None)
    finally:
        driver.close()

    match = result["matches"][0]
    assert all(c.enum_values is None for c in match.columns)


# ---------------------------------------------------------------------------
# Scenario 19: comment always None (SQLite has no native comment metadata)
# ---------------------------------------------------------------------------


def test_describe_schema_comment_always_none():
    """No SQLite column or relation carries a comment; all are None."""
    driver = SQLiteDriver(connectable=sqlite3.connect(":memory:"))
    try:
        _seed_orders_users_unique(driver)
        # Level-2: columns + relation-level comment all None.
        result = driver.describe_schema(table="orders", schema=None)
    finally:
        driver.close()

    match = result["matches"][0]
    assert match.comment is None
    assert all(c.comment is None for c in match.columns)
