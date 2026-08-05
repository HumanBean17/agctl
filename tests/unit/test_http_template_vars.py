"""Unit tests for http call/request/ping inline template-variable generators (Task 7).

Threads the per-invocation ``memo`` + ``template_vars_enabled`` flag through the
http commands so ``{{uuid}}``/``{{ts}}``/``{{rand}}`` tokens in ``--body``,
``--header`` values, the path/``--url``, and the resolved template body resolve to
generated values, with the SAME token resolving to the SAME value within one
invocation (shared memo). ``--no-template-vars`` leaves tokens literal; an unknown
generator surfaces as ``ConfigError`` (exit 2) before any request is sent.

The recording seam is the existing ``httpx.MockTransport`` DI (set via
``http_commands.set_default_transport``): its handler stashes each
``httpx.Request`` so tests assert on the post-substitution ``method``/``url``/
``headers``/``content`` without a real network call.
"""

from __future__ import annotations

import json
import re
import uuid as uuidlib
from pathlib import Path

import httpx
import pytest
from click.testing import CliRunner

from agctl.cli import cli
from agctl.commands import http_commands
from agctl.commands.http_commands import ping_loop

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
    """A closure dict that a MockTransport handler stashes requests into."""
    return {"requests": []}


@pytest.fixture
def mock_transport(captured):
    """Install a recording MockTransport; restore the default transport on teardown."""

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
# http request: inline generators + shared memo
# --------------------------------------------------------------------------- #


def test_request_body_shares_uuid_across_fields(mock_transport, captured):
    """``--body '{"id":"{{uuid}}","ref":"{{uuid}}"}'`` resolves BOTH positions to
    the SAME uuid (shared per-invocation memo), and the value is a valid UUID."""
    result = _run(
        [
            "--config", str(FIXTURE),
            "http", "request",
            "--service", "order-service",
            "--path", "/o",
            "--method", "POST",
            "--body", '{"id":"{{uuid}}","ref":"{{uuid}}"}',
        ]
    )
    assert result.exit_code == 0, result.output

    sent = captured["requests"][0]
    body = json.loads(sent.content)
    # Same memo -> identical value for the repeated {{uuid}} token.
    assert body["id"] == body["ref"]
    # Both are valid RFC-4122 UUIDs.
    uuidlib.UUID(body["id"])
    uuidlib.UUID(body["ref"])


def test_request_path_substitutes_timestamp(mock_transport, captured):
    """``--path "/o/{{ts}}"`` carries a 10-digit Unix-seconds timestamp."""
    result = _run(
        [
            "--config", str(FIXTURE),
            "http", "request",
            "--service", "order-service",
            "--path", "/o/{{ts}}",
            "--method", "GET",
        ]
    )
    assert result.exit_code == 0, result.output

    sent = captured["requests"][0]
    assert re.fullmatch(r"/o/\d{10}", sent.url.path)


def test_request_url_substitutes_timestamp(mock_transport, captured):
    """``--url`` mode: a ``{{ts}}`` in the full URL is substituted before split."""
    result = _run(
        [
            "--config", str(FIXTURE),
            "http", "request",
            "--url", "http://localhost:9090/o/{{ts}}",
            "--method", "GET",
        ]
    )
    assert result.exit_code == 0, result.output

    sent = captured["requests"][0]
    assert re.fullmatch(r"/o/\d{10}", sent.url.path)


def test_request_header_value_substituted_and_shared_with_body(
    mock_transport, captured
):
    """``--header X-Trace={{uuid}}`` and ``--body '{"id":"{{uuid}}"}'`` resolve to
    the SAME value (shared memo across headers + body within one request)."""
    result = _run(
        [
            "--config", str(FIXTURE),
            "http", "request",
            "--service", "order-service",
            "--path", "/o",
            "--method", "POST",
            "--body", '{"id":"{{uuid}}"}',
            "--header", "X-Trace={{uuid}}",
        ]
    )
    assert result.exit_code == 0, result.output

    sent = captured["requests"][0]
    body = json.loads(sent.content)
    # httpx lowercases header keys.
    assert sent.headers["x-trace"] == body["id"]
    uuidlib.UUID(body["id"])


def test_request_no_template_vars_leaves_body_literal(mock_transport, captured):
    """``--no-template-vars`` disables substitution end-to-end: the sent body
    carries the LITERAL token text, and no error is raised."""
    result = _run(
        [
            "--config", str(FIXTURE),
            "--no-template-vars",
            "http", "request",
            "--service", "order-service",
            "--path", "/o",
            "--method", "POST",
            "--body", '{"id":"{{uuid}}"}',
        ]
    )
    assert result.exit_code == 0, result.output

    sent = captured["requests"][0]
    body = json.loads(sent.content)
    assert body["id"] == "{{uuid}}"


def test_request_unknown_generator_is_config_error_before_request(
    mock_transport, captured
):
    """``--body '{{nope}}'`` (unknown generator) surfaces as ``ConfigError``
    (exit 2) BEFORE the request is sent — the mock transport records nothing."""
    result = _run(
        [
            "--config", str(FIXTURE),
            "http", "request",
            "--service", "order-service",
            "--path", "/o",
            "--method", "POST",
            "--body", "{{nope}}",
        ]
    )
    payload = json.loads(result.output)

    assert result.exit_code == 2
    assert payload["error"]["type"] == "ConfigError"
    # No request reached the transport.
    assert captured["requests"] == []


# --------------------------------------------------------------------------- #
# http call: template body + header share one memo
# --------------------------------------------------------------------------- #


def test_call_template_body_and_header_share_uuid(mock_transport, captured, tmp_path):
    """A template whose body and a header both reference ``{{uuid}}`` resolve to
    the SAME value (shared memo across the resolved template body + headers)."""
    cfg = tmp_path / "agctl.yaml"
    cfg.write_text(
        'version: "3"\n'
        "services:\n"
        "  svc:\n"
        "    base_url: http://localhost:9090\n"
        "    timeout_seconds: 10\n"
        "templates:\n"
        "  gen:\n"
        "    method: POST\n"
        "    service: svc\n"
        '    path: "/api/x"\n'
        "    headers:\n"
        '      Content-Type: "application/json"\n'
        '      X-Trace: "{{uuid}}"\n'
        "    body:\n"
        '      trace: "{{uuid}}"\n'
    )
    result = _run(["--config", str(cfg), "http", "call", "gen"])
    assert result.exit_code == 0, result.output

    sent = captured["requests"][0]
    body = json.loads(sent.content)
    trace_header = sent.headers["x-trace"]
    # Same memo -> body.trace == X-Trace header.
    assert body["trace"] == trace_header
    uuidlib.UUID(body["trace"])


# --------------------------------------------------------------------------- #
# http ping: per-invocation memo (resolved once, sent on every ping)
# --------------------------------------------------------------------------- #


def test_ping_substitutes_body_and_shares_memo(mock_transport, captured, monkeypatch):
    """``http ping`` resolves ``--body`` generators ONCE (per-invocation memo);
    the SAME resolved body is sent on every ping. Bounded via a max_pings=2 loop."""

    def fake_ping_loop(send_one, *, emit_line, **kwargs):
        ping_lines, total_ms = ping_loop(
            send_one,
            interval=0,
            max_pings=2,
            sleep_fn=lambda s: None,
            emit_line=emit_line,
        )
        failed = sum(1 for p in ping_lines if not p.get("ok"))
        return len(ping_lines), failed, total_ms

    monkeypatch.setattr(http_commands, "_run_pings", fake_ping_loop)

    result = _run(
        [
            "--config", str(FIXTURE),
            "http", "ping",
            "--service", "order-service",
            "--path", "/o",
            "--method", "POST",
            "--body", '{"id":"{{uuid}}","ref":"{{uuid}}"}',
            "--interval", "1",
            "--duration", "5",
        ]
    )
    assert result.exit_code == 0, result.output

    # Two pings, both carrying the SAME resolved body (memo is per-invocation).
    assert len(captured["requests"]) == 2
    b1 = json.loads(captured["requests"][0].content)
    b2 = json.loads(captured["requests"][1].content)
    # Within one ping the repeated token resolves to one value.
    assert b1["id"] == b1["ref"]
    uuidlib.UUID(b1["id"])
    # The body is resolved once and reused across pings.
    assert b1["id"] == b2["id"]


def test_ping_unknown_generator_is_config_error_before_request(
    mock_transport, captured
):
    """``http ping --body '{{nope}}'`` surfaces as ``ConfigError`` (exit 2) during
    startup resolution, before any ping line is streamed or request sent."""
    result = _run(
        [
            "--config", str(FIXTURE),
            "http", "ping",
            "--service", "order-service",
            "--path", "/o",
            "--interval", "1",
            "--body", "{{nope}}",
        ]
    )
    # First line is the startup error envelope (no ping lines streamed).
    payload = json.loads(result.output.splitlines()[0])
    assert result.exit_code == 2
    assert payload["command"] == "http.ping"
    assert payload["error"]["type"] == "ConfigError"
    # No request reached the transport.
    assert captured["requests"] == []
