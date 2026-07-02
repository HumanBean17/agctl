"""Unit tests for `http call` and `http request` commands (DESIGN §3.1, D5)."""

import json
from pathlib import Path

import httpx
import pytest
from click.testing import CliRunner

from agctl.cli import cli
from agctl.commands import http_commands

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


@pytest.fixture
def captured():
    """A closure dict that a MockTransport handler can stash requests into."""
    box = {"requests": []}
    return box


@pytest.fixture
def mock_transport(captured):
    def handler(request: httpx.Request) -> httpx.Response:
        captured["requests"].append(request)
        return httpx.Response(200, json={"ok": True})

    transport = httpx.MockTransport(handler)
    http_commands.set_default_transport(transport)
    yield transport
    http_commands.set_default_transport(None)


def _run(args, env=ENV):
    return CliRunner().invoke(cli, args, env=env)


# --------------------------------------------------------------------------- #
# http call
# --------------------------------------------------------------------------- #


def test_http_call_get_order_fills_path_param(mock_transport, captured):
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "http",
            "call",
            "get-order",
            "--param",
            "order_id=o9",
        ]
    )
    payload = json.loads(result.output)

    assert result.exit_code == 0
    assert payload["command"] == "http.call"
    assert payload["ok"] is True
    assert payload["result"]["status_code"] == 200
    assert payload["result"]["url"].endswith("/api/v1/orders/o9")
    assert payload["result"]["method"] == "GET"


def test_http_call_d5_body_merge(mock_transport, captured):
    """--body merges over the filled template body (D5 recursive merge)."""
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "http",
            "call",
            "create-order",
            "--param",
            "customer_id=cust-42",
            "--param",
            "sku=WIDGET-001",
            "--body",
            '{"priority":"high"}',
        ]
    )
    payload = json.loads(result.output)

    assert result.exit_code == 0
    assert payload["ok"] is True

    sent = captured["requests"][0]
    sent_body = json.loads(sent.content)

    # Filled from template body.
    assert sent_body["customer_id"] == "cust-42"
    assert sent_body["items"][0]["sku"] == "WIDGET-001"
    # Merged from --body.
    assert sent_body["priority"] == "high"


def test_http_call_missing_template(mock_transport, captured):
    result = _run(
        ["--config", str(FIXTURE), "http", "call", "no-such-template"]
    )
    payload = json.loads(result.output)

    assert result.exit_code == 2
    assert payload["ok"] is False
    assert payload["error"]["type"] == "TemplateNotFound"


def test_http_ping_malformed_body_is_internal_error():
    """Malformed --body JSON in `http ping` yields a structured InternalError
    envelope + exit 2, not a raw traceback (parity with @envelope commands)."""
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "http",
            "ping",
            "--service",
            "order-service",
            "--path",
            "/x",
            "--interval",
            "1",
            "--body",
            "{not-json",
        ]
    )
    payload = json.loads(result.output)

    assert result.exit_code == 2
    assert payload["ok"] is False
    assert payload["command"] == "http.ping"
    assert payload["error"]["type"] == "InternalError"


def test_http_call_header_merge_caller_wins(mock_transport, captured):
    """Template headers are sent; --header overrides the same header."""
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "http",
            "call",
            "create-order",
            "--param",
            "customer_id=c1",
            "--param",
            "sku=s1",
            "--header",
            "X-Request-Source=overridden",
        ]
    )
    payload = json.loads(result.output)
    assert result.exit_code == 0
    assert payload["ok"] is True

    sent_headers = captured["requests"][0].headers
    # Caller override wins.
    assert sent_headers["x-request-source"] == "overridden"
    # Template header intact.
    assert sent_headers["content-type"] == "application/json"


def test_http_call_500_is_ok_true(mock_transport, captured):
    """A 5xx response is a successful request, not an assertion failure."""

    def handler(request: httpx.Request) -> httpx.Response:
        captured["requests"].append(request)
        return httpx.Response(500, json={"err": "boom"})

    http_commands.set_default_transport(httpx.MockTransport(handler))
    try:
        result = _run(
            [
                "--config",
                str(FIXTURE),
                "http",
                "call",
                "create-order",
                "--param",
                "customer_id=c1",
                "--param",
                "sku=s1",
            ]
        )
        payload = json.loads(result.output)

        assert result.exit_code == 0
        assert payload["ok"] is True
        assert payload["result"]["status_code"] == 500
    finally:
        http_commands.set_default_transport(None)


# --------------------------------------------------------------------------- #
# http request
# --------------------------------------------------------------------------- #


def test_http_request_free_form(mock_transport, captured):
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "http",
            "request",
            "--service",
            "order-service",
            "--method",
            "GET",
            "--path",
            "/x",
        ]
    )
    payload = json.loads(result.output)

    assert result.exit_code == 0
    assert payload["command"] == "http.request"
    assert payload["ok"] is True
    assert payload["result"]["status_code"] == 200
    assert payload["result"]["url"].endswith("/x")


def test_http_request_unknown_service(mock_transport, captured):
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "http",
            "request",
            "--service",
            "ghost",
            "--method",
            "GET",
            "--path",
            "/x",
        ]
    )
    payload = json.loads(result.output)

    assert result.exit_code == 2
    assert payload["ok"] is False
    assert payload["error"]["type"] == "ConfigError"
