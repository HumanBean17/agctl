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
