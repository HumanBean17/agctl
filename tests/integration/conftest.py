"""Self-skipping fixtures for live integration tests.

These fixtures detect whether a real backing service (HTTP / Postgres / Kafka)
is reachable and ``pytest.skip()`` cleanly when it is not. They NEVER fail when
the service is absent — that is the whole point: integration tests run in CI
against real services but skip in the local dev environment.

There are two ways to provide a live service:

1. **Manual / CI** — point the tests at an already-running service by setting the
   env vars below. Useful with ``docker compose up`` or a deployed SUT.

2. **Local, via Docker (opt-in)** — set ``AGCTL_TEST_LIVE=1`` and the
   :func:`_live_services` session fixture spins up throwaway containers (Postgres,
   Kafka) via `testcontainers` plus a local HTTP mock, wiring the discovered
   host:ports into the same env vars. When the flag is unset, nothing starts and
   every test skips — so a plain ``pytest`` run stays fast and never pulls images.

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
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

DEFAULT_HTTP_URL = "http://localhost:8081"

# Opt-in switch: when "1", the session fixture tries to start throwaway services
# via Docker. Unset/other => the original skip-on-absent behavior is preserved.
LIVE = os.environ.get("AGCTL_TEST_LIVE") == "1"

# Per-service failure reasons recorded by _live_services when a container/mock
# fails to start. The require_* fixtures surface these so a live run that could
# not, e.g., reach Docker reports that clearly instead of "env var not set".
_LIVE_SKIP_REASONS: dict[str, str] = {}


def _live_skip_reason(service: str) -> str | None:
    """If running live and ``service`` failed to start, return why; else None."""
    return _LIVE_SKIP_REASONS.get(service) if LIVE else None


# --- Live-service lifecycle (Docker via testcontainers) ---------------------


class _LiveStack:
    """Holds the handles of whatever the session fixture started, for teardown."""

    def __init__(self) -> None:
        self.postgres = None
        self.kafka = None
        self.http_server: ThreadingHTTPServer | None = None
        self.http_thread: threading.Thread | None = None

    def stop_all(self) -> None:
        for attr in ("postgres", "kafka"):
            container = getattr(self, attr)
            if container is not None:
                try:
                    container.stop()
                except Exception:  # noqa: BLE001 - best-effort teardown
                    pass
        if self.http_server is not None:
            try:
                self.http_server.shutdown()
                self.http_server.server_close()
            except Exception:  # noqa: BLE001 - best-effort teardown
                pass
        if self.http_thread is not None:
            try:
                self.http_thread.join(timeout=5)
            except Exception:  # noqa: BLE001 - best-effort teardown
                pass


class _OkHandler(BaseHTTPRequestHandler):
    """Trivial 200-OK handler for the HTTP integration test (/actuator/health)."""

    def do_GET(self):  # noqa: N802 - http.server protocol
        body = b'{"status":"UP"}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):  # noqa: D401, ARG002 - silence access log
        pass


def _start_postgres(stack: _LiveStack) -> None:
    try:
        from testcontainers.postgres import PostgresContainer

        pg = PostgresContainer(
            "postgres:16-alpine",
            username="test",
            password="test",
            dbname="test",
        )
        pg.start()
    except Exception as exc:  # noqa: BLE001 - any failure => skip that service
        _LIVE_SKIP_REASONS["postgres"] = f"{type(exc).__name__}: {exc}"
        return

    stack.postgres = pg
    port = str(pg.get_exposed_port(5432))
    os.environ["AGCTL_TEST_PG_HOST"] = "localhost"
    os.environ["AGCTL_TEST_PG_DB"] = "test"
    os.environ["AGCTL_TEST_PG_USER"] = "test"
    os.environ["AGCTL_TEST_PG_PASSWORD"] = "test"
    os.environ["AGCTL_TEST_PG_DSN"] = (
        f"host=localhost port={port} dbname=test user=test password=test"
    )
    os.environ["DB_PORT"] = port


def _start_kafka(stack: _LiveStack) -> None:
    try:
        from testcontainers.kafka import KafkaContainer

        kc = KafkaContainer().with_kraft()
        kc.start()
    except Exception as exc:  # noqa: BLE001 - any failure => skip that service
        _LIVE_SKIP_REASONS["kafka"] = f"{type(exc).__name__}: {exc}"
        return

    stack.kafka = kc
    internal_port = getattr(kc, "port", 9093)
    port = str(kc.get_exposed_port(internal_port))
    os.environ["AGCTL_TEST_KAFKA_BROKER"] = f"localhost:{port}"


def _start_http(stack: _LiveStack) -> None:
    try:
        server = ThreadingHTTPServer(("127.0.0.1", 0), _OkHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
    except Exception as exc:  # noqa: BLE001 - any failure => skip that service
        _LIVE_SKIP_REASONS["http"] = f"{type(exc).__name__}: {exc}"
        return

    stack.http_server = server
    stack.http_thread = thread
    port = server.server_address[1]
    os.environ["AGCTL_TEST_HTTP_URL"] = f"http://127.0.0.1:{port}"


@pytest.fixture(scope="session", autouse=True)
def _live_services():
    """When ``AGCTL_TEST_LIVE=1``, start throwaway Postgres/Kafka/HTTP for the
    session and wire their addresses into the env vars the require_* fixtures
    read. Each service starts independently; a failure records a reason and the
    matching tests skip. When the flag is unset this is a no-op.
    """
    if not LIVE:
        yield
        return

    stack = _LiveStack()
    _start_postgres(stack)
    _start_kafka(stack)
    _start_http(stack)
    try:
        yield
    finally:
        stack.stop_all()


# --- HTTP -------------------------------------------------------------------


@pytest.fixture
def require_http_service():
    """Skip if the live HTTP service is unreachable.

    Yields the base URL to test against. Detection: a 1s ``httpx.get`` against
    ``$AGCTL_TEST_HTTP_URL`` (default ``http://localhost:8081``). Skips if
    httpx is missing or the request fails for any reason.
    """
    reason = _live_skip_reason("http")
    if reason:
        pytest.skip(f"live HTTP service unavailable: {reason}")

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
    reason = _live_skip_reason("postgres")
    if reason:
        pytest.skip(f"live Postgres unavailable: {reason}")

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
    reason = _live_skip_reason("kafka")
    if reason:
        pytest.skip(f"live Kafka broker unavailable: {reason}")

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


@pytest.fixture
def require_kafka_second_broker():
    """Skip if a SECOND live Kafka broker is unreachable.

    Yields the second broker address (``$AGCTL_TEST_KAFKA_BROKER_2``). Used by
    the multi-cluster integration test, which needs two independent brokers so
    it can prove ``--cluster`` routes produce/assert to the named cluster.
    Unset (the common single-broker dev/CI case) => skip, never fail.
    """
    broker = os.environ.get("AGCTL_TEST_KAFKA_BROKER_2")
    if not broker:
        pytest.skip(
            "AGCTL_TEST_KAFKA_BROKER_2 not set; skipping multi-cluster Kafka test"
        )

    try:
        from confluent_kafka import admin
    except ImportError:
        pytest.skip("confluent_kafka not installed; skipping live Kafka test")

    try:
        client = admin.AdminClient({"bootstrap.servers": broker})
        client.list_topics(timeout=2)
    except Exception as exc:  # noqa: BLE001 - any failure means no service
        pytest.skip(f"Second Kafka broker at {broker} unavailable: {exc}")
    return broker
