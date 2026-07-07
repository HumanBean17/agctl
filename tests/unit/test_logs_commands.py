"""Unit tests for `logs query/assert` commands (DESIGN §6.2, §6.3).

The _FakeLogsClient test double mirrors the LogClient contract. Tests monkeypatch
`logs_commands.new_logs_client` to return a fake client, so no real backend is
required.
"""

import dataclasses
import datetime
from pathlib import Path
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pytest
from click.testing import CliRunner

from agctl.cli import cli
from agctl.clients.log_backend_protocol import AwaitResult, CanonicalEntry, LogFilter, ScanResult
from agctl.commands import logs_commands

FIXTURE = Path(__file__).parent.parent / "fixtures" / "agctl.yaml"

ENV = {
    "ORDER_SERVICE_URL": "http://localhost:8081",
    "PAYMENT_SERVICE_URL": "http://localhost:8082",
    "PAYMENT_SERVICE_TOKEN": "tok",
    "KAFKA_BROKER": "localhost",
    "SCHEMA_REGISTRY_URL": "",
    "DB_HOST": "h",
    "DB_NAME": "n",
    "DB_USER": "u",
    "DB_PASSWORD": "p",
    "ANALYTICS_DB_HOST": "ah",
    "ANALYTICS_DB_USER": "au",
    "ANALYTICS_DB_PASSWORD": "ap",
}


# --------------------------------------------------------------------------- #
# _FakeLogsClient test double
# --------------------------------------------------------------------------- #


class _FakeLogsClient:
    """Fake LogClient with canned scan/await_one results."""

    def __init__(
        self,
        scan: ScanResult | None = None,
        await_one: AwaitResult | None = None,
    ):
        self._scan_result = scan
        self._await_one_result = await_one
        self.scan_calls = []
        self.await_one_calls = []

    def scan(
        self,
        filt: LogFilter,
        *,
        since: datetime.datetime | None,
        until: datetime.datetime | None,
        limit: int,
        tail_lines: int,
    ) -> ScanResult:
        self.scan_calls.append(
            {
                "filter": filt,
                "since": since,
                "until": until,
                "limit": limit,
                "tail_lines": tail_lines,
            }
        )
        return self._scan_result

    def await_one(
        self,
        filt: LogFilter,
        *,
        since: datetime.datetime | None,
        timeout_s: float,
        poll_interval_ms: int,
    ) -> AwaitResult:
        self.await_one_calls.append(
            {
                "filter": filt,
                "since": since,
                "timeout_s": timeout_s,
                "poll_interval_ms": poll_interval_ms,
            }
        )
        return self._await_one_result

    def sample_schema(self, *, sample_lines: int = 100):
        pass

    def follow(self, filt, *, stop_event, poll_interval_ms: int):
        pass

    def validate_config(self):
        pass


@pytest.fixture
def install_fake(monkeypatch):
    """Install a _FakeLogsClient that captures scan/await_one calls."""

    captured = {}

    def _install(scan: ScanResult | None = None, await_one: AwaitResult | None = None):
        fake = _FakeLogsClient(scan=scan, await_one=await_one)
        captured["fake"] = fake

        def factory(src):
            return fake

        monkeypatch.setattr(logs_commands, "new_logs_client", factory)
        return fake

    return _install


def _run(args, env=ENV):
    return CliRunner().invoke(cli, args, env=env)


def _payload(result):
    import json

    return json.loads(result.output)


# --------------------------------------------------------------------------- #
# logs query
# --------------------------------------------------------------------------- #


def test_logs_query_returns_entries(install_fake):
    """Basic query returns entries in the expected envelope shape."""
    entry = CanonicalEntry(
        timestamp="2026-07-08T12:00:00Z",
        level="INFO",
        logger="order-service",
        message="Order created",
    )
    scan_res = ScanResult(
        entries=[entry],
        matched=1,
        scanned=1,
        truncated=False,
    )
    install_fake(scan=scan_res)

    result = _run(
        [
            "--config",
            str(FIXTURE),
            "logs",
            "query",
            "--source",
            "order-service",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 0
    assert payload["command"] == "logs.query"
    assert payload["ok"] is True
    assert payload["result"]["matched"] == 1
    assert payload["result"]["scanned"] == 1
    assert payload["result"]["truncated"] is False
    assert len(payload["result"]["entries"]) == 1
    assert payload["result"]["entries"][0]["level"] == "INFO"


def test_logs_query_level_filter_passed(install_fake):
    """--level filter is passed to scan (case-insensitive -> UPPER)."""
    scan_res = ScanResult(entries=[], matched=0, scanned=0, truncated=False)
    fake = install_fake(scan=scan_res)

    result = _run(
        [
            "--config",
            str(FIXTURE),
            "logs",
            "query",
            "--source",
            "order-service",
            "--level",
            "error",  # lowercase -> should become ERROR
        ]
    )

    assert result.exit_code == 0
    assert len(fake.scan_calls) == 1
    assert fake.scan_calls[0]["filter"].level == "ERROR"


def test_logs_query_truncated_flag(install_fake):
    """When the scan result is truncated, the envelope reports it."""
    e1 = CanonicalEntry(timestamp="T", level="INFO", logger="x", message="m1")
    e2 = CanonicalEntry(timestamp="T", level="INFO", logger="x", message="m2")
    scan_res = ScanResult(entries=[e1, e2], matched=5, scanned=10, truncated=True)
    install_fake(scan=scan_res)

    result = _run(
        [
            "--config",
            str(FIXTURE),
            "logs",
            "query",
            "--source",
            "order-service",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 0
    assert payload["result"]["matched"] == 5
    assert payload["result"]["scanned"] == 10
    assert payload["result"]["truncated"] is True
    assert len(payload["result"]["entries"]) == 2


# --------------------------------------------------------------------------- #
# logs assert
# --------------------------------------------------------------------------- #


def test_logs_assert_match_success(install_fake):
    """Matching entry found -> returns matching_entry."""
    entry = CanonicalEntry(
        timestamp="2026-07-08T12:00:00Z",
        level="ERROR",
        logger="payment-service",
        message="Payment failed",
    )
    await_res = AwaitResult(entry=entry, scanned=5, elapsed_ms=100)
    install_fake(await_one=await_res)

    result = _run(
        [
            "--config",
            str(FIXTURE),
            "logs",
            "assert",
            "--source",
            "payment-service",
            "--since",
            "5m",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 0
    assert payload["command"] == "logs.assert"
    assert payload["ok"] is True
    assert payload["result"]["matched"] is True
    assert payload["result"]["matching_entry"]["level"] == "ERROR"
    assert payload["result"]["entries_scanned"] == 5
    assert payload["result"]["elapsed_ms"] == 100


def test_logs_assert_no_match_is_assertion_error(install_fake):
    """No matching entry -> AssertionError with filter detail."""
    await_res = AwaitResult(entry=None, scanned=10, elapsed_ms=2000)
    install_fake(await_one=await_res)

    result = _run(
        [
            "--config",
            str(FIXTURE),
            "logs",
            "assert",
            "--source",
            "order-service",
            "--since",
            "1h",
            "--timeout",
            "0.5",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 1
    assert payload["ok"] is False
    assert payload["error"]["type"] == "AssertionError"
    assert "No matching log entry found within 0.5s" in payload["error"]["message"]
    assert payload["error"]["detail"]["source"] == "order-service"
    assert payload["error"]["detail"]["entries_scanned"] == 10
    assert payload["error"]["detail"]["elapsed_ms"] == 2000
    assert payload["error"]["detail"]["not"] is False
    # filter echoed in detail
    assert "filter" in payload["error"]["detail"]


def test_logs_assert_not_success_when_no_match(install_fake):
    """--not inverts: no match when --not is set -> success."""
    await_res = AwaitResult(entry=None, scanned=0, elapsed_ms=50)
    install_fake(await_one=await_res)

    result = _run(
        [
            "--config",
            str(FIXTURE),
            "logs",
            "assert",
            "--source",
            "order-service",
            "--since",
            "1m",
            "--not",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 0
    assert payload["ok"] is True
    assert payload["result"]["matched"] is True


def test_logs_assert_not_failure_when_match_found(install_fake):
    """--not inverts: a match when --not is set -> AssertionError."""
    entry = CanonicalEntry(timestamp="T", level="ERROR", logger="x", message="bad")
    await_res = AwaitResult(entry=entry, scanned=3, elapsed_ms=20)
    install_fake(await_one=await_res)

    result = _run(
        [
            "--config",
            str(FIXTURE),
            "logs",
            "assert",
            "--source",
            "order-service",
            "--since",
            "30s",
            "--not",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 1
    assert payload["error"]["type"] == "AssertionError"
    assert "Matching log entry found" in payload["error"]["message"]
    assert payload["error"]["detail"]["not"] is True
    assert payload["error"]["detail"]["matching_entry"]["level"] == "ERROR"


def test_logs_assert_since_required():
    """--since is required for logs assert (missing -> ConfigError)."""
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "logs",
            "assert",
            "--source",
            "order-service",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 2
    assert payload["error"]["type"] == "ConfigError"
    assert "--since" in payload["error"]["message"]


def test_logs_assert_timeout_optional_default_oneshot(install_fake):
    """Omitting --timeout defaults to 0.0 (one-shot)."""
    entry = CanonicalEntry(timestamp="T", level="INFO", logger="x", message="ok")
    await_res = AwaitResult(entry=entry, scanned=0, elapsed_ms=0)
    fake = install_fake(await_one=await_res)

    result = _run(
        [
            "--config",
            str(FIXTURE),
            "logs",
            "assert",
            "--source",
            "order-service",
            "--since",
            "1m",
        ]
    )

    assert result.exit_code == 0
    assert len(fake.await_one_calls) == 1
    assert fake.await_one_calls[0]["timeout_s"] == 0.0


def test_logs_assert_timeout_explicit_value(install_fake):
    """--timeout 5 sets timeout_s=5.0."""
    entry = CanonicalEntry(timestamp="T", level="INFO", logger="x", message="ok")
    await_res = AwaitResult(entry=entry, scanned=0, elapsed_ms=0)
    fake = install_fake(await_one=await_res)

    result = _run(
        [
            "--config",
            str(FIXTURE),
            "logs",
            "assert",
            "--source",
            "order-service",
            "--since",
            "1m",
            "--timeout",
            "5",
        ]
    )

    assert result.exit_code == 0
    assert len(fake.await_one_calls) == 1
    assert fake.await_one_calls[0]["timeout_s"] == 5.0


def test_logs_match_placeholder_fill_and_compile(install_fake):
    """--match with placeholders fills params and compiles jq."""
    await_res = AwaitResult(
        entry=CanonicalEntry(timestamp="T", level="INFO", logger="x", message="ok"),
        scanned=1,
        elapsed_ms=10,
    )
    fake = install_fake(await_one=await_res)

    result = _run(
        [
            "--config",
            str(FIXTURE),
            "logs",
            "assert",
            "--source",
            "order-service",
            "--since",
            "1m",
            "--match",
            '.fields.orderId == "{orderId}"',
            "--param",
            "orderId=ord-1",
        ]
    )

    assert result.exit_code == 0
    assert len(fake.await_one_calls) == 1
    # Placeholder should be filled
    assert fake.await_one_calls[0]["filter"].match_jq == '.fields.orderId == "ord-1"'


def test_logs_match_compile_loud_fail(install_fake):
    """Malformed --match surfaces as ConfigError (exit 2)."""
    await_res = AwaitResult(entry=None, scanned=0, elapsed_ms=0)
    install_fake(await_one=await_res)

    result = _run(
        [
            "--config",
            str(FIXTURE),
            "logs",
            "assert",
            "--source",
            "order-service",
            "--since",
            "1m",
            "--match",
            ".fields.orderId ==",  # truncated expression
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 2
    assert payload["error"]["type"] == "ConfigError"
    assert "jq" in payload["error"]["message"].lower()


def test_logs_unknown_source():
    """Unknown --source -> ConfigError."""
    result = _run(
        [
            "--config",
            str(FIXTURE),
            "logs",
            "query",
            "--source",
            "does-not-exist",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 2
    assert payload["error"]["type"] == "ConfigError"
    assert "does-not-exist" in payload["error"]["message"]


def test_logs_match_missing_jq_extra(monkeypatch):
    """Missing jq library -> ConfigError hinting agctl[logs] (not db/kafka)."""
    # Simulate missing jq by making _jq() raise ConfigError with the db/kafka hint
    import agctl.assertions as assertions_mod

    original_jq = assertions_mod._jq

    def _fake_jq():
        from agctl.errors import ConfigError

        raise ConfigError(
            "jq is required for match/path assertions: pip install 'agctl[db]' or 'agctl[kafka]'",
            {},
        )

    monkeypatch.setattr(assertions_mod, "_jq", _fake_jq)

    result = _run(
        [
            "--config",
            str(FIXTURE),
            "logs",
            "assert",
            "--source",
            "order-service",
            "--since",
            "1m",
            "--match",
            ".x == 1",
        ]
    )
    payload = _payload(result)

    assert result.exit_code == 2
    assert payload["error"]["type"] == "ConfigError"
    # The hint should point at agctl[logs]
    assert "agctl[logs]" in payload["error"]["message"]
    # Should NOT contain the original db/kafka hint
    assert "[db]" not in payload["error"]["message"]
    assert "[kafka]" not in payload["error"]["message"]
