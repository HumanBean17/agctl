"""Live integration test: ``http request`` against a real HTTP service.

Requires env:
- ``AGCTL_TEST_HTTP_URL`` — base URL of the live service (default
  ``http://localhost:8081``). Also used as ``ORDER_SERVICE_URL`` for config
  resolution.

Skips (via the ``require_http_service`` fixture) when no service is reachable.
Run in CI with the SUT deployed, e.g.::

    AGCTL_TEST_HTTP_URL=http://sut:8081 ORDER_SERVICE_URL=http://sut:8081 \\
        PAYMENT_SERVICE_URL=http://sut:8082 PAYMENT_SERVICE_TOKEN=tok \\
        KAFKA_BROKER=sut DB_HOST=sut DB_NAME=n DB_USER=u DB_PASSWORD=p \\
        ANALYTICS_DB_HOST=sut ANALYTICS_DB_USER=au ANALYTICS_DB_PASSWORD=ap \\
        python3 -m pytest tests/integration -q
"""

import os

import pytest
from click.testing import CliRunner

from agctl.cli import cli

FIXTURE = (
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    + "/fixtures/agctl.yaml"
)


def _env(base_url):
    env = {
        "ORDER_SERVICE_URL": base_url,
        "PAYMENT_SERVICE_URL": os.environ.get(
            "AGCTL_TEST_HTTP_URL", "http://localhost:8082"
        ),
        "PAYMENT_SERVICE_TOKEN": "integration",
        "KAFKA_BROKER": "localhost",
        "DB_HOST": "localhost",
        "DB_NAME": "n",
        "DB_USER": "u",
        "DB_PASSWORD": "p",
        "ANALYTICS_DB_HOST": "localhost",
        "ANALYTICS_DB_USER": "au",
        "ANALYTICS_DB_PASSWORD": "ap",
    }
    return env


def test_http_request_ok(require_http_service):
    """A free-form http request against the live service yields ok:true."""
    base_url = require_http_service
    env = _env(base_url)
    result = CliRunner().invoke(
        cli,
        [
            "--config",
            FIXTURE,
            "http",
            "request",
            "--service",
            "order-service",
            "--method",
            "GET",
            "--path",
            "/actuator/health",
        ],
        env=env,
    )
    assert result.exit_code == 0, result.output
    import json

    envelope = json.loads(result.output)
    assert envelope["ok"] is True
    assert envelope["result"]["status_code"] == 200
