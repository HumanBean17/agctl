"""Live integration test: ``db query`` against a real Postgres.

Requires env:
- ``AGCTL_TEST_PG_DSN`` — psycopg DSN for the live Postgres (also drives the
  ``main-db`` connection host/db/user/password via the matching env vars).

Skips (via the ``require_postgres`` fixture) when Postgres is unavailable.
"""

import os

from click.testing import CliRunner

from agctl.cli import cli

FIXTURE = (
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    + "/fixtures/agctl.yaml"
)


def _env():
    return {
        "ORDER_SERVICE_URL": "http://localhost:8081",
        "PAYMENT_SERVICE_URL": "http://localhost:8082",
        "PAYMENT_SERVICE_TOKEN": "tok",
        "KAFKA_BROKER": "localhost",
        "DB_HOST": os.environ.get("AGCTL_TEST_PG_HOST", "localhost"),
        "DB_PORT": os.environ.get("DB_PORT", "5432"),
        "DB_NAME": os.environ.get("AGCTL_TEST_PG_DB", "n"),
        "DB_USER": os.environ.get("AGCTL_TEST_PG_USER", "u"),
        "DB_PASSWORD": os.environ.get("AGCTL_TEST_PG_PASSWORD", "p"),
        "ANALYTICS_DB_HOST": "localhost",
        "ANALYTICS_DB_USER": "au",
        "ANALYTICS_DB_PASSWORD": "ap",
    }


def test_db_query_select_one(require_postgres):
    """``db query --sql 'SELECT 1 AS one'`` returns a single row over Postgres."""
    result = CliRunner().invoke(
        cli,
        [
            "--config",
            FIXTURE,
            "db",
            "query",
            "--sql",
            "SELECT 1 AS one",
            "--connection",
            "main-db",
        ],
        env=_env(),
    )
    assert result.exit_code == 0, result.output
    import json

    envelope = json.loads(result.output)
    assert envelope["ok"] is True
    assert envelope["result"]["row_count"] == 1
    assert envelope["result"]["rows"] == [{"one": 1}]


def test_db_execute_then_query_visible(require_postgres):
    """Integration round-trip: committed write from ``db execute`` is visible to a
    subsequent ``db query`` in a separate invocation, plus rollback-on-error.
    """
    import json

    test_id = "seed-test-1"

    # Step 1: Create throwaway table (DDL commits)
    result = CliRunner().invoke(
        cli,
        [
            "--config",
            FIXTURE,
            "db",
            "execute",
            "--connection",
            "main-db-writable",
            "--sql",
            "CREATE TABLE IF NOT EXISTS agctl_seed (id text PRIMARY KEY, status text)",
            "--write",
        ],
        env=_env(),
    )
    assert result.exit_code == 0, result.output
    envelope = json.loads(result.output)
    assert envelope["ok"] is True
    ra = envelope["result"]["rows_affected"]
    assert ra is None or ra >= 0  # DDL reports None (cursor.rowcount == -1) or 0

    # Step 2: Seed via template (idempotent ON CONFLICT DO NOTHING)
    result = CliRunner().invoke(
        cli,
        [
            "--config",
            FIXTURE,
            "db",
            "execute",
            "--template",
            "seed-order",
            "--param",
            f"orderId={test_id}",
            "--param",
            "status=PENDING",
            "--write",
        ],
        env=_env(),
    )
    assert result.exit_code == 0, result.output
    envelope = json.loads(result.output)
    assert envelope["ok"] is True
    assert envelope["result"]["rows_affected"] in (0, 1)  # 0 on conflict, 1 on insert
    # returning contains the row when inserted, [] when it already existed
    if envelope["result"]["rows_affected"] == 1:
        assert len(envelope["result"]["returning"]) == 1
        assert envelope["result"]["returning"][0]["id"] == test_id
        assert envelope["result"]["returning"][0]["status"] == "PENDING"
    else:
        assert envelope["result"]["returning"] == []

    # Step 3: Visibility - fresh invocation sees the committed row
    result = CliRunner().invoke(
        cli,
        [
            "--config",
            FIXTURE,
            "db",
            "query",
            "--connection",
            "main-db-writable",
            "--sql",
            "SELECT status FROM agctl_seed WHERE id = :i",
            "--param",
            f"i={test_id}",
        ],
        env=_env(),
    )
    assert result.exit_code == 0, result.output
    envelope = json.loads(result.output)
    assert envelope["ok"] is True
    assert envelope["result"]["row_count"] == 1
    assert envelope["result"]["rows"] == [{"status": "PENDING"}]

    # Step 4: Rollback-on-error - rejected statement rolls back the transaction
    result = CliRunner().invoke(
        cli,
        [
            "--config",
            FIXTURE,
            "db",
            "execute",
            "--connection",
            "main-db-writable",
            "--sql",
            "INSERT INTO no_such_table (id) VALUES (:i)",
            "--param",
            f"i={test_id}",
            "--write",
        ],
        env=_env(),
    )
    assert result.exit_code == 2, result.output  # ConnectionFailure
    envelope = json.loads(result.output)
    assert envelope["ok"] is False
    assert "error" in envelope

    # Verify no stray data was committed - agctl_seed still has only the test_id row
    result = CliRunner().invoke(
        cli,
        [
            "--config",
            FIXTURE,
            "db",
            "query",
            "--connection",
            "main-db-writable",
            "--sql",
            "SELECT id, status FROM agctl_seed",
        ],
        env=_env(),
    )
    assert result.exit_code == 0, result.output
    envelope = json.loads(result.output)
    assert envelope["ok"] is True
    assert envelope["result"]["row_count"] == 1
    assert envelope["result"]["rows"] == [{"id": test_id, "status": "PENDING"}]


def _exec_ddl(sql: str) -> None:
    """Run one DDL statement via ``db execute --connection main-db-writable --write``.

    Each statement is its own invocation so a failure is unambiguous. DDL commits.
    """
    result = CliRunner().invoke(
        cli,
        [
            "--config",
            FIXTURE,
            "db",
            "execute",
            "--connection",
            "main-db-writable",
            "--sql",
            sql,
            "--write",
        ],
        env=_env(),
    )
    assert result.exit_code == 0, result.output


def test_db_schema_live_introspection(require_postgres):
    """Build a rich schema via DDL, then assert ``db schema`` (both levels)
    returns the real introspected shape from a live Postgres 16.

    Uses a dedicated throwaway schema (``agctl_schema_test``) so Level-1
    assertions are deterministic and isolated from other tests' objects
    (``agctl_seed``, etc.). ``require_postgres`` skips the whole test when
    Postgres is unreachable.
    """
    import json

    # --- Setup: throwaway schema + objects (one DDL statement per invocation) ---
    _exec_ddl("DROP SCHEMA IF EXISTS agctl_schema_test CASCADE")
    _exec_ddl("CREATE SCHEMA agctl_schema_test")
    _exec_ddl(
        "CREATE TYPE agctl_schema_test.order_state "
        "AS ENUM ('PENDING','PAID','CANCELLED')"
    )
    _exec_ddl(
        "CREATE TABLE agctl_schema_test.customers "
        "(id uuid PRIMARY KEY, email text NOT NULL, created_at timestamptz)"
    )
    _exec_ddl(
        "CREATE TABLE agctl_schema_test.orders ("
        "id uuid PRIMARY KEY, "
        "customer_id uuid NOT NULL REFERENCES agctl_schema_test.customers(id), "
        "status agctl_schema_test.order_state NOT NULL DEFAULT 'PENDING', "
        "payload jsonb, "
        "audit_id integer GENERATED ALWAYS AS IDENTITY, "
        "row_total integer GENERATED ALWAYS AS (0) STORED, "
        "UNIQUE (status, customer_id)"
        ")"
    )
    _exec_ddl(
        "CREATE VIEW agctl_schema_test.order_view "
        "AS SELECT id, status FROM agctl_schema_test.orders"
    )
    _exec_ddl(
        "COMMENT ON COLUMN agctl_schema_test.orders.status IS 'lifecycle state'"
    )
    _exec_ddl("COMMENT ON TABLE agctl_schema_test.orders IS 'Customer orders'")

    # --- Level 1: list relations in the throwaway schema ---
    result = CliRunner().invoke(
        cli,
        [
            "--config",
            FIXTURE,
            "db",
            "schema",
            "--connection",
            "main-db-writable",
            "--schema",
            "agctl_schema_test",
        ],
        env=_env(),
    )
    assert result.exit_code == 0, result.output
    envelope = json.loads(result.output)
    assert envelope["ok"] is True
    assert envelope["command"] == "db.schema.tables"
    res = envelope["result"]
    assert res["schema_filter"] == "agctl_schema_test"
    assert res["count"] == 3

    # Exactly three relations: customers (table), orders (table), order_view (view).
    # Enum TYPE is not a relation and must not appear. Assert the set is exactly
    # those three; membership-style to tolerate ordering variation.
    expected = {
        ("agctl_schema_test", "customers", "table"),
        ("agctl_schema_test", "orders", "table"),
        ("agctl_schema_test", "order_view", "view"),
    }
    actual = {
        (it["schema"], it["name"], it["kind"]) for it in res["items"]
    }
    assert actual == expected

    # --- Level 2: introspect the orders table ---
    result = CliRunner().invoke(
        cli,
        [
            "--config",
            FIXTURE,
            "db",
            "schema",
            "--connection",
            "main-db-writable",
            "--schema",
            "agctl_schema_test",
            "--table",
            "orders",
        ],
        env=_env(),
    )
    assert result.exit_code == 0, result.output
    envelope = json.loads(result.output)
    assert envelope["ok"] is True
    assert envelope["command"] == "db.schema.table"
    res = envelope["result"]
    assert res["table"] == "orders"
    assert res["kind"] == "table"
    assert res["comment"] == "Customer orders"

    # Primary key.
    assert res["primary_key"] == ["id"]

    # Foreign keys: one entry, customer_id -> customers(id).
    fks = res["foreign_keys"]
    assert len(fks) == 1
    fk = fks[0]
    assert fk["columns"] == ["customer_id"]
    assert fk["references_schema"] == "agctl_schema_test"
    assert fk["references_table"] == "customers"
    assert fk["references_columns"] == ["id"]

    # Unique constraints: one entry with the two columns in declared order.
    uqs = res["unique_constraints"]
    assert len(uqs) == 1
    assert uqs[0]["columns"] == ["status", "customer_id"]

    # Column-level richness. Index columns by name so ordering is non-load-bearing.
    cols = {c["name"]: c for c in res["columns"]}

    status = cols["status"]
    assert status["data_type"].endswith("order_state")
    assert status["enum_values"] == ["PENDING", "PAID", "CANCELLED"]
    assert status["nullable"] is False
    assert status["default"] is not None and "'PENDING'" in status["default"]
    assert status["comment"] == "lifecycle state"

    audit_id = cols["audit_id"]
    assert audit_id["generated"] == "always_identity"
    assert audit_id["default"] is None

    row_total = cols["row_total"]
    assert row_total["generated"] == "stored"
    assert row_total["default"] is None

    payload = cols["payload"]
    assert payload["data_type"] == "jsonb"
    assert payload["enum_values"] is None


def test_db_schema_standalone_unique_index_surfaces(require_postgres):
    """A standalone ``CREATE UNIQUE INDEX`` (no ``pg_constraint`` backing) must
    appear in ``unique_constraints``, a ``pg_constraint``-backed ``UNIQUE`` must
    NOT be double-counted, and a plain non-unique index must NOT leak in.

    This verifies the SQL-level ``NOT EXISTS`` / ``NOT indisprimary`` /
    ``indpred IS NULL`` predicates in the ``pg_index`` discovery query — the
    FakeCursor unit tests cannot evaluate SQL, so the filtering is only
    meaningfully provable here against a real Postgres. ``require_postgres``
    skips when Postgres is unreachable.
    """
    import json

    # Throwaway schema so this is isolated from other tests' objects.
    _exec_ddl("DROP SCHEMA IF EXISTS agctl_uniq_test CASCADE")
    _exec_ddl("CREATE SCHEMA agctl_uniq_test")
    _exec_ddl(
        "CREATE TABLE agctl_uniq_test.t ("
        "id integer PRIMARY KEY, "
        "email text NOT NULL, "
        "external_ref text, "
        "code text, "
        "tag text, "
        "UNIQUE (email)"  # pg_constraint-backed unique (contype='u')
        ")"
    )
    # Standalone unique index: NO pg_constraint entry -> only the pg_index
    # query surfaces it. This is the case the fix adds support for.
    _exec_ddl(
        "CREATE UNIQUE INDEX t_external_ref_uidx "
        "ON agctl_uniq_test.t (external_ref)"
    )
    # Expression unique index: indkey placeholder is 0, so it has no column
    # list to map. Must be SKIPPED (not surfaced as a misleading empty-cols
    # entry). The driver filters this at the row-mapping layer.
    _exec_ddl(
        "CREATE UNIQUE INDEX t_code_lower_uidx "
        "ON agctl_uniq_test.t (lower(code))"
    )
    # Partial unique index: excluded by the ``indpred IS NULL`` predicate.
    _exec_ddl(
        "CREATE UNIQUE INDEX t_tag_partial_uidx "
        "ON agctl_uniq_test.t (tag) WHERE tag IS NOT NULL"
    )
    # Plain (non-unique) index: must NOT appear in unique_constraints.
    _exec_ddl("CREATE INDEX t_id_nonunique ON agctl_uniq_test.t (id)")

    result = CliRunner().invoke(
        cli,
        [
            "--config",
            FIXTURE,
            "db",
            "schema",
            "--connection",
            "main-db-writable",
            "--schema",
            "agctl_uniq_test",
            "--table",
            "t",
        ],
        env=_env(),
    )
    assert result.exit_code == 0, result.output
    envelope = json.loads(result.output)
    assert envelope["ok"] is True
    uqs = envelope["result"]["unique_constraints"]

    pairs = {(u["name"], tuple(u["columns"])) for u in uqs}
    # pg_constraint-backed UNIQUE(email) captured EXACTLY ONCE (not double-counted
    # by the pg_index query whose NOT EXISTS excludes constraint-backed indexes).
    assert sum(1 for u in uqs if tuple(u["columns"]) == ("email",)) == 1
    # Standalone unique index surfaced by the pg_index query.
    assert ("t_external_ref_uidx", ("external_ref",)) in pairs
    # Primary key not duplicated into unique_constraints.
    assert all("id" != tuple(u["columns"]) for u in uqs)
    # Plain non-unique index must not leak in.
    assert all(u["name"] != "t_id_nonunique" for u in uqs)
    # Expression unique index (indkey=0) skipped — no misleading empty-cols entry.
    assert all(u["name"] != "t_code_lower_uidx" for u in uqs)
    assert all(u["columns"] != [] for u in uqs)
    # Partial unique index excluded by the indpred IS NULL predicate.
    assert all(u["name"] != "t_tag_partial_uidx" for u in uqs)
    # No duplicate entries overall.
    assert len(uqs) == len({u["name"] for u in uqs})

