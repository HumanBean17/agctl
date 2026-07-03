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
    assert envelope["result"]["rows_affected"] >= 0  # DDL may report 0 or unknown

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
