"""Unit tests for `check ready` (DESIGN §3.4, D7)."""

import json
from pathlib import Path

import httpx
import pytest
from click.testing import CliRunner

from agctl.cli import cli
from agctl.commands import http_commands
from agctl.commands.check_commands import _check_ready_with_config
from agctl.config.models import Config, Defaults, ServiceConfig

FIXTURE = Path(__file__).parent.parent / "fixtures" / "agctl.yaml"

ENV = {
    "ORDER_SERVICE_URL": "http://localhost:8081",
    "PAYMENT_SERVICE_URL": "http://localhost:8082",
    "PAYMENT_SERVICE_TOKEN": "tok",
    "KAFKA_BROKER": "localhost",
    "DB_HOST": "h",
    "DB_NAME": "n",
    "DB_USER": "u",
    "DB_PASSWORD": "secret",
    "ANALYTICS_DB_HOST": "ah",
    "ANALYTICS_DB_USER": "au",
    "ANALYTICS_DB_PASSWORD": "ap",
}


def _run(args, env=ENV):
    return CliRunner().invoke(cli, args, env=env)


# --------------------------------------------------------------------------- #
# D7 default-to-all: a service whose health returns 200 is ready.
# --------------------------------------------------------------------------- #


def _routing_transport(captured=None):
    """MockTransport that routes by URL:
    - order-service health -> 200
    - payment-service health -> 500
    - anything else -> 200
    """

    def handler(request: httpx.Request) -> httpx.Response:
        if captured is not None:
            captured.setdefault("requests", []).append(request)
        url = str(request.url)
        if "8081" in url:  # order-service
            return httpx.Response(200, json={"status": "UP"})
        if "8082" in url:  # payment-service
            return httpx.Response(500, json={"status": "DOWN"})
        return httpx.Response(200, json={"ok": True})

    return httpx.MockTransport(handler)


def test_check_ready_single_service_ready():
    captured = {}
    transport = _routing_transport(captured)
    http_commands.set_default_transport(transport)
    try:
        result = _run(
            ["--config", str(FIXTURE), "check", "ready", "--service", "order-service"]
        )
        payload = json.loads(result.output)
    finally:
        http_commands.set_default_transport(None)

    assert result.exit_code == 0
    assert payload["command"] == "check.ready"
    assert payload["ok"] is True

    services = payload["result"]["services"]
    assert list(services.keys()) == ["order-service"]
    order = services["order-service"]
    assert order["ready"] is True
    assert order["status_code"] == 200
    assert order["url"].endswith("/actuator/health")
    assert isinstance(order["response_time_ms"], int)
    assert "error" not in order
    assert payload["result"]["all_ready"] is True


def test_check_ready_no_flags_checks_all_d7():
    """D7: no flags == check ALL configured services."""
    captured = {}
    transport = _routing_transport(captured)
    http_commands.set_default_transport(transport)
    try:
        result = _run(["--config", str(FIXTURE), "check", "ready"])
        payload = json.loads(result.output)
    finally:
        http_commands.set_default_transport(None)

    assert result.exit_code == 0
    assert payload["ok"] is True

    services = payload["result"]["services"]
    assert set(services.keys()) == {"order-service", "payment-service"}
    # order is 200 -> ready; payment is 500 -> not ready -> all_ready False.
    assert services["order-service"]["ready"] is True
    assert services["payment-service"]["ready"] is False
    assert services["payment-service"]["status_code"] == 500
    assert payload["result"]["all_ready"] is False


def test_check_ready_all_flag_same_as_no_flags():
    """--all is equivalent to no flags (both services checked)."""
    transport = _routing_transport()
    http_commands.set_default_transport(transport)
    try:
        result = _run(["--config", str(FIXTURE), "check", "ready", "--all"])
        payload = json.loads(result.output)
    finally:
        http_commands.set_default_transport(None)

    assert result.exit_code == 0
    services = payload["result"]["services"]
    assert set(services.keys()) == {"order-service", "payment-service"}


def test_check_ready_500_is_ok_true_overall():
    """A 500 on one service makes it not-ready but the command is still ok."""
    transport = _routing_transport()
    http_commands.set_default_transport(transport)
    try:
        result = _run(
            ["--config", str(FIXTURE), "check", "ready", "--service", "payment-service"]
        )
        payload = json.loads(result.output)
    finally:
        http_commands.set_default_transport(None)

    assert result.exit_code == 0
    assert payload["ok"] is True
    svc = payload["result"]["services"]["payment-service"]
    assert svc["ready"] is False
    assert svc["status_code"] == 500
    assert payload["result"]["all_ready"] is False


def test_check_ready_connect_error_marks_not_ready():
    """A ConnectError on a service -> ready False, status_code None, error present,
    command still ok True."""

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    http_commands.set_default_transport(httpx.MockTransport(handler))
    try:
        result = _run(
            ["--config", str(FIXTURE), "check", "ready", "--service", "order-service"]
        )
        payload = json.loads(result.output)
    finally:
        http_commands.set_default_transport(None)

    assert result.exit_code == 0
    assert payload["ok"] is True
    svc = payload["result"]["services"]["order-service"]
    assert svc["ready"] is False
    assert svc["status_code"] is None
    assert svc["response_time_ms"] is None
    assert "error" in svc and svc["error"]
    assert payload["result"]["all_ready"] is False


def test_check_ready_unknown_service_config_error():
    transport = _routing_transport()
    http_commands.set_default_transport(transport)
    try:
        result = _run(
            ["--config", str(FIXTURE), "check", "ready", "--service", "ghost"]
        )
        payload = json.loads(result.output)
    finally:
        http_commands.set_default_transport(None)

    assert result.exit_code == 2
    assert payload["ok"] is False
    assert payload["error"]["type"] == "ConfigError"
    assert payload["error"]["detail"]["service"] == "ghost"


def test_check_ready_service_and_all_conflict():
    transport = _routing_transport()
    http_commands.set_default_transport(transport)
    try:
        result = _run(
            [
                "--config",
                str(FIXTURE),
                "check",
                "ready",
                "--service",
                "order-service",
                "--all",
            ]
        )
        payload = json.loads(result.output)
    finally:
        http_commands.set_default_transport(None)

    assert result.exit_code == 2
    assert payload["ok"] is False
    assert payload["error"]["type"] == "ConfigError"


# --------------------------------------------------------------------------- #
# health_path None -> uses GET <base_url>/
# --------------------------------------------------------------------------- #


def test_check_ready_health_path_none_uses_root():
    """A service without health_path is probed at GET <base_url>/."""
    cfg = Config(
        version="1",
        services={
            "bare": ServiceConfig(base_url="http://localhost:9999"),
        },
        defaults=Defaults(timeout_seconds=10),
    )

    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        return httpx.Response(200, json={"ok": True})

    http_commands.set_default_transport(httpx.MockTransport(handler))
    try:
        result = _check_ready_with_config(
            cfg, service=None, all_services=False, timeout=None
        )
    finally:
        http_commands.set_default_transport(None)

    assert captured["url"] == "http://localhost:9999/"
    svc = result["services"]["bare"]
    assert svc["ready"] is True
    assert svc["url"] == "http://localhost:9999/"
    assert result["all_ready"] is True
