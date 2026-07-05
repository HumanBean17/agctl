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


# --------------------------------------------------------------------------- #
# response assertion flags (--status / --contains / --match / --jq-path / --equals)
# Task 9: validate BEFORE request, evaluate AFTER, AssertionFailure on miss.
# --------------------------------------------------------------------------- #


def _transport_returning(status_code, body):
    """Build a fresh MockTransport that returns a fixed (status, body) response."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code, json=body)

    return httpx.MockTransport(handler)


def test_http_call_zero_assertion_flags_is_regression(mock_transport, captured):
    """(a) Zero assertion flags on a 200 -> ok:true, exit 0, full result dict."""
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
    assert payload["ok"] is True
    assert payload["command"] == "http.call"
    result_dict = payload["result"]
    # Full §4.2 result schema present.
    assert result_dict["status_code"] == 200
    assert set(result_dict) >= {
        "status_code",
        "response_time_ms",
        "headers",
        "body",
        "url",
        "method",
    }


def test_http_request_zero_assertion_flags_is_regression(mock_transport, captured):
    """(a) Zero assertion flags on http request -> ok:true, exit 0, full result."""
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
    assert payload["ok"] is True
    assert payload["command"] == "http.request"
    assert payload["result"]["status_code"] == 200


def test_http_request_status_assertion_pass(mock_transport, captured):
    """(b) --status 201 on a 201 response -> ok:true, exit 0."""
    http_commands.set_default_transport(_transport_returning(201, {"id": "x"}))
    try:
        result = _run(
            [
                "--config",
                str(FIXTURE),
                "http",
                "request",
                "--service",
                "order-service",
                "--method",
                "POST",
                "--path",
                "/x",
                "--status",
                "201",
            ]
        )
        payload = json.loads(result.output)

        assert result.exit_code == 0
        assert payload["ok"] is True
        assert payload["result"]["status_code"] == 201
    finally:
        http_commands.set_default_transport(None)


def test_http_request_status_assertion_fail(mock_transport, captured):
    """(c) --status 200 on a 201 -> ok:false, AssertionError, pinned detail, exit 1."""
    http_commands.set_default_transport(_transport_returning(201, {"id": "x"}))
    try:
        result = _run(
            [
                "--config",
                str(FIXTURE),
                "http",
                "request",
                "--service",
                "order-service",
                "--method",
                "POST",
                "--path",
                "/x",
                "--status",
                "200",
            ]
        )
        payload = json.loads(result.output)

        assert result.exit_code == 1
        assert payload["ok"] is False
        assert payload["result"] is None
        assert payload["error"]["type"] == "AssertionError"
        assert payload["error"]["detail"]["failures"] == [
            {"mode": "status", "expected": 200, "actual": 201}
        ]
        assert payload["error"]["detail"]["response"]["status_code"] == 201
    finally:
        http_commands.set_default_transport(None)


def test_http_call_status_assertion_fail(mock_transport, captured):
    """(c) http call wiring: --status 200 on a 201 -> AssertionError, exit 1."""
    http_commands.set_default_transport(_transport_returning(201, {"id": "x"}))
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
                "--status",
                "200",
            ]
        )
        payload = json.loads(result.output)

        assert result.exit_code == 1
        assert payload["ok"] is False
        assert payload["error"]["type"] == "AssertionError"
        assert payload["error"]["detail"]["failures"] == [
            {"mode": "status", "expected": 200, "actual": 201}
        ]
    finally:
        http_commands.set_default_transport(None)


def test_http_request_match_assertion_pass(mock_transport, captured):
    """(d) --match '.body.status=="PENDING"' on {"status":"PENDING"} -> ok:true."""
    http_commands.set_default_transport(
        _transport_returning(200, {"status": "PENDING"})
    )
    try:
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
                "--match",
                '.body.status=="PENDING"',
            ]
        )
        payload = json.loads(result.output)

        assert result.exit_code == 0
        assert payload["ok"] is True
    finally:
        http_commands.set_default_transport(None)


def test_http_request_match_assertion_fail(mock_transport, captured):
    """(d) --match '.body.status=="PENDING"' on {"status":"PAID"} -> ok:false, exit 1."""
    http_commands.set_default_transport(_transport_returning(200, {"status": "PAID"}))
    try:
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
                "--match",
                '.body.status=="PENDING"',
            ]
        )
        payload = json.loads(result.output)

        assert result.exit_code == 1
        assert payload["ok"] is False
        assert payload["error"]["type"] == "AssertionError"
        assert payload["error"]["detail"]["failures"] == [
            {
                "mode": "match",
                "expr": '.body.status=="PENDING"',
                "result": False,
                "root": "response envelope",
                "body": {"status": "PAID"},
            }
        ]
    finally:
        http_commands.set_default_transport(None)


def test_http_request_match_on_status_code(mock_transport, captured):
    """--match '.status_code == 201' reaches the response envelope (not the body):
    against a 201 with empty body -> ok:true. Body-rooting would resolve
    .status_code against {} -> null -> predicate false -> assertion failure."""
    http_commands.set_default_transport(_transport_returning(201, {}))
    try:
        result = _run(
            [
                "--config",
                str(FIXTURE),
                "http",
                "request",
                "--service",
                "order-service",
                "--method",
                "POST",
                "--path",
                "/x",
                "--match",
                ".status_code == 201",
            ]
        )
        payload = json.loads(result.output)

        assert result.exit_code == 0
        assert payload["ok"] is True
    finally:
        http_commands.set_default_transport(None)


def test_http_request_contains_assertion_pass(mock_transport, captured):
    """(d') --contains '{"status":"PENDING"}' on a superset body -> ok:true."""
    http_commands.set_default_transport(
        _transport_returning(200, {"status": "PENDING", "id": "x"})
    )
    try:
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
                "--contains",
                '{"status":"PENDING"}',
            ]
        )
        payload = json.loads(result.output)

        assert result.exit_code == 0
        assert payload["ok"] is True
    finally:
        http_commands.set_default_transport(None)


def test_http_request_contains_assertion_fail(mock_transport, captured):
    """(d') --contains '{"status":"PENDING"}' on {"status":"PAID"} -> ok:false, exit 1."""
    http_commands.set_default_transport(_transport_returning(200, {"status": "PAID"}))
    try:
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
                "--contains",
                '{"status":"PENDING"}',
            ]
        )
        payload = json.loads(result.output)

        assert result.exit_code == 1
        assert payload["ok"] is False
        assert payload["error"]["type"] == "AssertionError"
        assert payload["error"]["detail"]["failures"] == [
            {
                "mode": "contains",
                "needle": {"status": "PENDING"},
                "matched": False,
                "root": "response body",
                "body": {"status": "PAID"},
            }
        ]
    finally:
        http_commands.set_default_transport(None)


def test_http_call_jq_path_without_equals_is_config_error_and_skips_transport(
    mock_transport, captured
):
    """(e) http call: --jq-path without --equals -> ConfigError exit 2 AND the
    mock transport was NOT called (validate raised pre-request)."""
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "http",
            "call",
            "get-order",
            "--param",
            "order_id=o9",
            "--jq-path",
            ".status",
        ]
    )
    payload = json.loads(result.output)

    assert result.exit_code == 2
    assert payload["ok"] is False
    assert payload["error"]["type"] == "ConfigError"
    # Load-bearing: validate raised BEFORE client.request -> transport untouched.
    assert len(captured["requests"]) == 0


def test_http_request_jq_path_without_equals_is_config_error_and_skips_transport(
    mock_transport, captured
):
    """(e) http request: --jq-path without --equals -> ConfigError exit 2 AND the
    mock transport was NOT called (validate raised pre-request)."""
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
            "--jq-path",
            ".status",
        ]
    )
    payload = json.loads(result.output)

    assert result.exit_code == 2
    assert payload["ok"] is False
    assert payload["error"]["type"] == "ConfigError"
    assert len(captured["requests"]) == 0


def test_http_request_match_with_jq_missing_is_config_error(
    mock_transport, captured, monkeypatch
):
    """(f) --match with jq missing (sys.modules['jq']=None) -> ConfigError exit 2."""
    import sys

    monkeypatch.setitem(sys.modules, "jq", None)  # block the lazy `import jq`

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
            "--match",
            '.status=="PENDING"',
        ]
    )
    payload = json.loads(result.output)

    assert result.exit_code == 2
    assert payload["ok"] is False
    assert payload["error"]["type"] == "ConfigError"
    assert "agctl[jq]" in payload["error"]["message"]


def test_http_match_help_states_response_envelope_root():
    """Regression (battle-test incident): the agent trusted --help's old
    'response body' wording and wrote body-rooted .data.operator. The --match
    help MUST now name the response envelope so the root is unambiguous."""
    for cmd in (http_commands.http_call, http_commands.http_request):
        match_opt = next(p for p in cmd.params if p.name == "match")
        assert "response envelope" in match_opt.help
        assert "response body" not in match_opt.help


# --------------------------------------------------------------------------- #
# http request --url mode (free-form URL, no configured service required)
# --------------------------------------------------------------------------- #


def test_http_request_url_mode_hits_full_url(mock_transport, captured):
    """--url sends to the full URL; result.url reflects it; no service needed."""
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "http",
            "request",
            "--url",
            "https://example.invalid:8443/api/v1/orders/ord-1",
            "--method",
            "GET",
        ]
    )
    payload = json.loads(result.output)

    assert result.exit_code == 0
    assert payload["command"] == "http.request"
    assert payload["ok"] is True
    sent_url = str(captured["requests"][0].url)
    assert sent_url == "https://example.invalid:8443/api/v1/orders/ord-1"
    assert payload["result"]["url"] == sent_url
    assert payload["result"]["method"] == "GET"


def test_http_request_url_method_defaults_get(mock_transport, captured):
    """--url with no --method defaults to GET (parity with http ping free-form)."""
    result = _run(
        ["--config", str(FIXTURE), "http", "request", "--url", "https://host/health"]
    )
    payload = json.loads(result.output)

    assert result.exit_code == 0
    assert payload["result"]["method"] == "GET"
    assert captured["requests"][0].method == "GET"


def test_http_request_url_preserves_query_string(mock_transport, captured):
    """--url query string is forwarded intact."""
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "http",
            "request",
            "--url",
            "https://host/api?x=1&y=2",
        ]
    )
    payload = json.loads(result.output)

    assert result.exit_code == 0
    assert str(captured["requests"][0].url) == "https://host/api?x=1&y=2"


def test_http_request_url_malformed_no_scheme(mock_transport, captured):
    """A schemeless --url -> ConfigError exit 2; no request is sent."""
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "http",
            "request",
            "--url",
            "host/path",
        ]
    )
    payload = json.loads(result.output)

    assert result.exit_code == 2
    assert payload["error"]["type"] == "ConfigError"
    assert len(captured["requests"]) == 0


def test_http_request_url_rejects_non_http_scheme(mock_transport, captured):
    """A non-http(s) scheme (e.g. ftp://) -> ConfigError at resolve time, not a
    confusing httpx-layer error later. No request is sent."""
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "http",
            "request",
            "--url",
            "ftp://host/file",
        ]
    )
    payload = json.loads(result.output)

    assert result.exit_code == 2
    assert payload["error"]["type"] == "ConfigError"
    assert len(captured["requests"]) == 0


def test_http_request_url_mutually_exclusive_with_service(mock_transport, captured):
    """--url + --service -> ConfigError exit 2; no request is sent."""
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "http",
            "request",
            "--url",
            "https://host/x",
            "--service",
            "order-service",
        ]
    )
    payload = json.loads(result.output)

    assert result.exit_code == 2
    assert payload["error"]["type"] == "ConfigError"
    assert len(captured["requests"]) == 0


def test_http_request_url_mutually_exclusive_with_path(mock_transport, captured):
    """--url + --path -> ConfigError exit 2."""
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "http",
            "request",
            "--url",
            "https://host/x",
            "--path",
            "/y",
        ]
    )
    payload = json.loads(result.output)

    assert result.exit_code == 2
    assert payload["error"]["type"] == "ConfigError"


def test_http_request_neither_url_nor_service(mock_transport, captured):
    """No --url and no --service -> ConfigError (exactly one mode is required)."""
    result = _run(
        ["--config", str(FIXTURE), "http", "request", "--method", "GET"]
    )
    payload = json.loads(result.output)

    assert result.exit_code == 2
    assert payload["error"]["type"] == "ConfigError"


def test_http_request_service_mode_method_defaults_get(mock_transport, captured):
    """Service mode also picks up the --method GET default (behavior change)."""
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "http",
            "request",
            "--service",
            "order-service",
            "--path",
            "/x",
        ]
    )
    payload = json.loads(result.output)

    assert result.exit_code == 0
    assert payload["result"]["method"] == "GET"


def test_http_request_url_mode_status_assertion(mock_transport, captured):
    """Response assertion flags pass through unchanged in URL mode."""
    http_commands.set_default_transport(_transport_returning(201, {"id": "x"}))
    try:
        result = _run(
            [
                "--config",
                str(FIXTURE),
                "http",
                "request",
                "--url",
                "https://host/x",
                "--method",
                "POST",
                "--status",
                "201",
            ]
        )
        payload = json.loads(result.output)

        assert result.exit_code == 0
        assert payload["result"]["status_code"] == 201
    finally:
        http_commands.set_default_transport(None)
