"""Self-skipping fixtures for live integration tests.

These fixtures detect whether a real backing service (HTTP / Postgres / Kafka)
is reachable and ``pytest.skip()`` cleanly when it is not. They NEVER fail when
the service is absent — that is the whole point: integration tests run in CI
against real services but skip in the local dev environment.

Required environment variables (all optional; unset => skip):

- ``AGCTL_TEST_HTTP_URL`` — base URL of the live HTTP service under test
  (default ``http://localhost:8081``).
- ``AGCTL_TEST_PG_DSN`` — psycopg DSN for the live Postgres under test.
- ``AGCTL_TEST_KAFKA_BROKER`` — ``host:port`` of the live Kafka broker.

Each fixture yields the resolved connection handle (or URL string) so the test
can use it, or skips before yielding.
"""

from __future__ import annotations

import os

import pytest

DEFAULT_HTTP_URL = "http://localhost:8081"


# --- HTTP -------------------------------------------------------------------


@pytest.fixture
def require_http_service():
    """Skip if the live HTTP service is unreachable.

    Yields the base URL to test against. Detection: a 1s ``httpx.get`` against
    ``$AGCTL_TEST_HTTP_URL`` (default ``http://localhost:8081``). Skips if
    httpx is missing or the request fails for any reason.
    """
    base_url = os.environ.get("AGCTL_TEST_HTTP_URL", DEFAULT_HTTP_URL)
    try:
        import httpx
    except ImportError:
        pytest.skip("httpx not installed; skipping live HTTP integration test")

    try:
        httpx.get(base_url, timeout=1.0)
    except Exception as exc:  # noqa: BLE001 - any failure means no service
        pytest.skip(f"HTTP service at {base_url} unavailable: {exc}")
    return base_url


# --- Postgres ---------------------------------------------------------------


@pytest.fixture
def require_postgres():
    """Skip if a live Postgres is unreachable.

    Yields an open psycopg connection. Requires ``$AGCTL_TEST_PG_DSN`` (unset
    => skip). Skips if psycopg is missing or the connection fails. The
    connection is closed on teardown.
    """
    dsn = os.environ.get("AGCTL_TEST_PG_DSN")
    if not dsn:
        pytest.skip("AGCTL_TEST_PG_DSN not set; skipping live Postgres test")

    try:
        import psycopg
    except ImportError:
        pytest.skip("psycopg not installed; skipping live Postgres test")

    conn = None
    try:
        conn = psycopg.connect(dsn, connect_timeout=2)
    except Exception as exc:  # noqa: BLE001 - any failure means no service
        pytest.skip(f"Postgres at {dsn} unavailable: {exc}")

    try:
        yield conn
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001 - best-effort cleanup
            pass


# --- Kafka ------------------------------------------------------------------


@pytest.fixture
def require_kafka():
    """Skip if a live Kafka broker is unreachable.

    Yields the broker address string (``$AGCTL_TEST_KAFKA_BROKER``). Requires
    confluent_kafka; skips if the import fails or the broker cannot be
    reached.
    """
    broker = os.environ.get("AGCTL_TEST_KAFKA_BROKER")
    if not broker:
        pytest.skip("AGCTL_TEST_KAFKA_BROKER not set; skipping live Kafka test")

    try:
        from confluent_kafka import admin
    except ImportError:
        pytest.skip("confluent_kafka not installed; skipping live Kafka test")

    try:
        # A metadata request is the cheapest reachability probe.
        client = admin.AdminClient({"bootstrap.servers": broker})
        # .list_topics() blocks for the request timeout; force a short one.
        client.list_topics(timeout=2)
    except Exception as exc:  # noqa: BLE001 - any failure means no service
        pytest.skip(f"Kafka broker at {broker} unavailable: {exc}")
    return broker
