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


def test_http_request_jq_path_assertion_pass(require_http_service):
    """Wrong-branch detection: matching --jq-path/--equals branch -> exit 0.

    Hits the live service health endpoint (``{"status":"UP"}``, 200) and asserts
    ``.status == "UP"`` via ``--jq-path``/``--equals``. Per DESIGN §8.1, a
    passing assertion exits 0. Self-skips when no live service is reachable.
    """
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
            "--status",
            "200",
            "--jq-path",
            ".status",
            "--equals",
            '"UP"',
        ],
        env=env,
    )
    assert result.exit_code == 0, result.output
    import json

    envelope = json.loads(result.output)
    assert envelope["ok"] is True
    assert envelope["result"]["status_code"] == 200


def test_http_request_jq_path_assertion_wrong_branch(require_http_service):
    """Wrong-branch detection: mismatched --equals -> AssertionError exit 1.

    Same endpoint (``{"status":"UP"}``) but ``--equals '"QUEUED"'`` must fail:
    the response took the "UP" branch, not "QUEUED". Per DESIGN §8.1/§14, a
    failed assertion raises ``AssertionError`` (exit 1) with the failing
    ``jq-path`` mode surfaced in ``error.detail.failures``. Self-skips when no
    live service is reachable.
    """
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
            "--status",
            "200",
            "--jq-path",
            ".status",
            "--equals",
            '"QUEUED"',
        ],
        env=env,
    )
    assert result.exit_code == 1, result.output
    import json

    envelope = json.loads(result.output)
    assert envelope["ok"] is False
    assert envelope["error"]["type"] == "AssertionError"
    failures = envelope["error"]["detail"]["failures"]
    # The jq-path mode must be the one reporting the mismatch (actual "UP" vs
    # expected "QUEUED"); --status 200 matches the live mock, so only jq-path
    # fails -- isolating the wrong-branch signal.
    assert any(f.get("mode") == "jq-path" for f in failures), failures
