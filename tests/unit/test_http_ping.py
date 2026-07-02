"""Unit tests for `http ping` streaming NDJSON (DESIGN §3.1, M5 exception)."""

import json
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

NOOP_SLEEP = lambda s: None  # noqa: E731


# --------------------------------------------------------------------------- #
# ping_loop unit tests
# --------------------------------------------------------------------------- #


def test_ping_loop_three_ok():
    collected = []

    def send_one(i):
        return {
            "ping": i,
            "ok": True,
            "status_code": 200,
            "duration_ms": 10,
            "timestamp": "2026-06-29T14:22:05Z",
        }

    lines, total_ms = ping_loop(
        send_one,
        interval=0,
        max_pings=3,
        sleep_fn=NOOP_SLEEP,
        emit_line=collected.append,
    )

    assert len(collected) == 3
    assert [d["ping"] for d in collected] == [1, 2, 3]
    assert all(d["ok"] is True for d in collected)
    assert total_ms >= 0
    assert isinstance(total_ms, int)


def test_ping_loop_one_failing():
    collected = []

    def send_one(i):
        if i == 2:
            return {
                "ping": i,
                "ok": False,
                "status_code": 401,
                "duration_ms": 5,
                "timestamp": "2026-06-29T14:22:05Z",
                "error": "Unexpected status 401",
            }
        return {
            "ping": i,
            "ok": True,
            "status_code": 200,
            "duration_ms": 8,
            "timestamp": "2026-06-29T14:22:05Z",
        }

    lines, total_ms = ping_loop(
        send_one,
        interval=0,
        max_pings=3,
        sleep_fn=NOOP_SLEEP,
        emit_line=collected.append,
    )

    assert collected[1]["ok"] is False
    assert collected[1]["error"] == "Unexpected status 401"
    assert collected[1]["status_code"] == 401
    failed = [d for d in collected if not d["ok"]]
    assert len(failed) == 1


def test_ping_loop_connection_failure_status_null():
    collected = []

    def send_one(i):
        return {
            "ping": i,
            "ok": False,
            "status_code": None,
            "duration_ms": 0,
            "timestamp": "2026-06-29T14:22:05Z",
            "error": "connection refused",
        }

    ping_loop(
        send_one,
        interval=0,
        max_pings=2,
        sleep_fn=NOOP_SLEEP,
        emit_line=collected.append,
    )

    assert all(d["status_code"] is None for d in collected)
    assert all(d["ok"] is False for d in collected)


def test_ping_loop_stop_event_halts():
    import threading

    collected = []
    stop_event = threading.Event()

    def send_one(i):
        if i >= 2:
            stop_event.set()
        return {"ping": i, "ok": True, "status_code": 200, "duration_ms": 1}

    ping_loop(
        send_one,
        interval=0,
        max_pings=100,
        stop_event=stop_event,
        sleep_fn=NOOP_SLEEP,
        emit_line=collected.append,
    )

    assert len(collected) <= 3


def test_ping_loop_duration_bounds():
    collected = []

    def send_one(i):
        return {"ping": i, "ok": True, "status_code": 200, "duration_ms": 1}

    # duration=0 with a fast send_one: at least one ping, but it should stop
    # quickly (not run forever since max_pings is unset).
    lines, _ = ping_loop(
        send_one,
        interval=0,
        duration=0,
        sleep_fn=NOOP_SLEEP,
        emit_line=collected.append,
    )

    # With duration 0, the loop should terminate after a small number of pings.
    assert len(collected) >= 1


# --------------------------------------------------------------------------- #
# Click command via CliRunner
# --------------------------------------------------------------------------- #


def _run(args, env=ENV):
    return CliRunner().invoke(cli, args, env=env)


def test_http_ping_duration_and_until_stopped_mutually_exclusive(monkeypatch):
    """Both --duration and --until-stopped -> ConfigError -> exit 2."""

    # Avoid real HTTP / real loops; the command must reject before any of that.
    def fake_run_pings(*args, **kwargs):
        raise AssertionError("should not reach the loop")

    monkeypatch.setattr(http_commands, "_run_pings", fake_run_pings)

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
            "--duration",
            "5",
            "--until-stopped",
        ]
    )
    assert result.exit_code == 2


def test_http_ping_streams_lines_all_ok(monkeypatch):
    """Bounded run via patched _run_pings: 2 ping lines + 1 summary, exit 0."""
    canned_pings = [
        {
            "ping": 1,
            "ok": True,
            "status_code": 200,
            "duration_ms": 12,
            "timestamp": "2026-06-29T14:22:05Z",
        },
        {
            "ping": 2,
            "ok": True,
            "status_code": 200,
            "duration_ms": 9,
            "timestamp": "2026-06-29T14:22:06Z",
        },
    ]

    def fake_run_pings(send_one, *, emit_line, **kwargs):
        for p in canned_pings:
            emit_line(p)
        total_ms = 100
        failed = sum(1 for p in canned_pings if not p["ok"])
        return len(canned_pings), failed, total_ms

    monkeypatch.setattr(http_commands, "_run_pings", fake_run_pings)

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
            "--duration",
            "5",
        ]
    )

    lines = [ln for ln in result.output.splitlines() if ln.strip()]
    assert len(lines) == 3
    ping_lines = [json.loads(ln) for ln in lines[:-1]]
    summary = json.loads(lines[-1])
    assert all(pl["ok"] is True for pl in ping_lines)
    assert summary["summary"] is True
    assert summary["total_pings"] == 2
    assert summary["failed_pings"] == 0
    assert result.exit_code == 0


def test_http_ping_streams_lines_with_failure_exit1(monkeypatch):
    """A failing ping -> failed_pings>=1 -> exit 1."""
    canned_pings = [
        {
            "ping": 1,
            "ok": True,
            "status_code": 200,
            "duration_ms": 12,
            "timestamp": "2026-06-29T14:22:05Z",
        },
        {
            "ping": 2,
            "ok": False,
            "status_code": 401,
            "duration_ms": 9,
            "timestamp": "2026-06-29T14:22:06Z",
            "error": "Unexpected status 401",
        },
    ]

    def fake_run_pings(send_one, *, emit_line, **kwargs):
        for p in canned_pings:
            emit_line(p)
        total_ms = 100
        failed = sum(1 for p in canned_pings if not p["ok"])
        return len(canned_pings), failed, total_ms

    monkeypatch.setattr(http_commands, "_run_pings", fake_run_pings)

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
            "--duration",
            "5",
        ]
    )

    lines = [ln for ln in result.output.splitlines() if ln.strip()]
    ping_lines = [json.loads(ln) for ln in lines[:-1]]
    summary = json.loads(lines[-1])
    assert ping_lines[1]["ok"] is False
    assert ping_lines[1]["error"] == "Unexpected status 401"
    assert summary["failed_pings"] == 1
    assert result.exit_code == 1


def test_http_ping_end_to_end_with_mock_transport(monkeypatch):
    """Full path: real send_one closure + MockTransport, bounded via max_pings.

    We monkeypatch _run_pings to force a bounded max_pings=2 and a no-op sleep,
    so the loop runs exactly twice against the mock and stops deterministically.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"ok": True})

    http_commands.set_default_transport(httpx.MockTransport(handler))
    try:

        def fake_ping_loop(send_one, *, emit_line, **kwargs):
            # Force a bounded run using the real send_one closure.
            ping_lines, total_ms = ping_loop(
                send_one,
                interval=0,
                max_pings=2,
                sleep_fn=NOOP_SLEEP,
                emit_line=emit_line,
            )
            failed = sum(1 for p in ping_lines if not p.get("ok"))
            return len(ping_lines), failed, total_ms

        monkeypatch.setattr(http_commands, "_run_pings", fake_ping_loop)

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
                "--duration",
                "5",
            ]
        )

        lines = [ln for ln in result.output.splitlines() if ln.strip()]
        assert len(lines) == 3  # 2 pings + summary
        ping_lines = [json.loads(ln) for ln in lines[:-1]]
        summary = json.loads(lines[-1])
        assert [pl["ping"] for pl in ping_lines] == [1, 2]
        assert all(pl["ok"] is True for pl in ping_lines)
        assert all(pl["status_code"] == 200 for pl in ping_lines)
        assert summary["total_pings"] == 2
        assert summary["failed_pings"] == 0
        assert result.exit_code == 0
    finally:
        http_commands.set_default_transport(None)


def test_http_ping_non_2xx_carries_error(monkeypatch):
    """A 401 mock response yields ok=False + 'Unexpected status 401' line."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"err": "no"})

    http_commands.set_default_transport(httpx.MockTransport(handler))
    try:

        def fake_ping_loop(send_one, *, emit_line, **kwargs):
            ping_lines, total_ms = ping_loop(
                send_one,
                interval=0,
                max_pings=1,
                sleep_fn=NOOP_SLEEP,
                emit_line=emit_line,
            )
            failed = sum(1 for p in ping_lines if not p.get("ok"))
            return len(ping_lines), failed, total_ms

        monkeypatch.setattr(http_commands, "_run_pings", fake_ping_loop)

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
                "--duration",
                "5",
            ]
        )

        lines = [ln for ln in result.output.splitlines() if ln.strip()]
        ping_line = json.loads(lines[0])
        summary = json.loads(lines[-1])
        assert ping_line["ok"] is False
        assert ping_line["status_code"] == 401
        assert ping_line["error"] == "Unexpected status 401"
        assert summary["failed_pings"] == 1
        assert result.exit_code == 1
    finally:
        http_commands.set_default_transport(None)


# --------------------------------------------------------------------------- #
# http ping — startup error envelope (Fix B)
# --------------------------------------------------------------------------- #


def test_http_ping_bad_template_emits_structured_error(monkeypatch):
    """A bad template name fails before any ping with a structured envelope:
    TemplateNotFound -> exit 2. No MockTransport needed (it errors at resolve)."""

    def fake_run_pings(*args, **kwargs):
        raise AssertionError("should not reach the loop")

    monkeypatch.setattr(http_commands, "_run_pings", fake_run_pings)

    result = _run(
        [
            "--config",
            str(FIXTURE),
            "http",
            "ping",
            "no-such-template",
            "--interval",
            "1",
            "--until-stopped",
        ]
    )
    assert result.exit_code == 2
    data = json.loads(result.output)
    assert data["ok"] is False
    assert data["error"]["type"] == "TemplateNotFound"


def test_http_ping_duration_and_until_stopped_emits_config_error(monkeypatch):
    """--duration + --until-stopped together -> ConfigError -> exit 2, with a
    structured envelope (no uncaught traceback)."""

    def fake_run_pings(*args, **kwargs):
        raise AssertionError("should not reach the loop")

    monkeypatch.setattr(http_commands, "_run_pings", fake_run_pings)

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
            "--duration",
            "1",
            "--until-stopped",
        ]
    )
    assert result.exit_code == 2
    data = json.loads(result.output)
    assert data["ok"] is False
    assert data["error"]["type"] == "ConfigError"
