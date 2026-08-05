"""Unit tests for `logs query/assert/tail` + `kafka listen assert/results/messages`
inline template-variable generators (Task 9).

Threads the per-invocation ``memo`` + ``template_vars_enabled`` flag through the
logs commands (so ``{{uuid}}``/``{{ts}}``/``{{rand}}`` tokens in ``--match``
resolve to generated values via the existing ``fill_placeholders`` call in
``_build_log_filter``) and through the kafka-listen evaluation path
(``resolve_spec_modes``/``evaluate_expectations`` fill the pattern + explicit
``--match``; ``_kafka_listen_messages_core`` fills its ``--match``). The SAME
token resolves to the SAME value within one invocation (shared memo).
``--no-template-vars`` leaves tokens literal; an unknown generator surfaces as
``ConfigError`` (exit 2) before any backend call.

DI seams:
- logs: ``monkeypatch logs_commands.new_logs_client`` with a fake that records
  the ``LogFilter.match_jq`` handed to ``scan``/``await_one``.
- kafka-listen: driven directly at the ``_core`` boundary (no Kafka client); a
  canned ``<topic>.ndjson`` capture file is the only setup. Generators are
  monkeypatched to deterministic values so the predicate can be wired to match
  a known canned message.
"""

from __future__ import annotations

import datetime
import json
import os
import re
from pathlib import Path

import pytest
from click.testing import CliRunner

from agctl.cli import cli
from agctl.clients.log_backend_protocol import (
    AwaitResult,
    CanonicalEntry,
    LogFilter,
    ScanResult,
)
from agctl.commands import logs_commands
from agctl.commands.kafka_listen_commands import (
    _kafka_listen_assert_core,
    _kafka_listen_messages_core,
    _kafka_listen_results_core,
)
from agctl.errors import ConfigError
from agctl import template_vars as template_vars_mod

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


# ---------------------------------------------------------------------------
# logs: fake LogClient + install fixture (mirror test_logs_commands.py)
# ---------------------------------------------------------------------------


class _FakeLogsClient:
    """Fake LogClient recording the filter handed to scan/await_one."""

    def __init__(self, scan=None, await_one=None):
        self._scan = scan
        self._await_one = await_one
        self.scan_calls: list[dict] = []
        self.await_one_calls: list[dict] = []

    def scan(self, filt, *, since, until, limit, tail_lines):
        self.scan_calls.append(
            {"filter": filt, "since": since, "until": until, "limit": limit}
        )
        return self._scan

    def await_one(self, filt, *, since, timeout_s, poll_interval_ms, tail_lines):
        self.await_one_calls.append(
            {"filter": filt, "since": since, "timeout_s": timeout_s}
        )
        return self._await_one

    def sample_schema(self, *, sample_lines: int = 100):
        pass

    def validate_config(self):
        pass


@pytest.fixture
def install_fake_logs(monkeypatch):
    """Install a :class:`_FakeLogsClient` capturing scan/await_one calls."""

    captured: dict = {}

    def _install(scan=None, await_one=None):
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
    return json.loads(result.output)


# ---------------------------------------------------------------------------
# logs query: --match generator substitution
# ---------------------------------------------------------------------------


def test_logs_query_substitutes_ts_in_match_via_filter(install_fake_logs):
    """The match expression HANDED TO THE BACKEND contains a 10-digit timestamp
    (the actual substitution assertion, separate from the wrapper above)."""
    fake = install_fake_logs(scan=ScanResult(entries=[], matched=0, scanned=0, truncated=False))
    result = _run(
        [
            "--config", str(FIXTURE),
            "logs", "query",
            "--source", "order-service",
            "--match", '.ts == "{{ts}}"',
        ]
    )
    assert result.exit_code == 0, result.output
    match_jq = fake.scan_calls[0]["filter"].match_jq
    assert re.fullmatch(r'\.ts == "\d{10}"', match_jq), match_jq


def test_logs_query_no_template_vars_leaves_token_literal(install_fake_logs):
    """``--no-template-vars`` leaves ``{{ts}}`` literal end-to-end."""
    fake = install_fake_logs(scan=ScanResult(entries=[], matched=0, scanned=0, truncated=False))
    result = _run(
        [
            "--config", str(FIXTURE),
            "--no-template-vars",
            "logs", "query",
            "--source", "order-service",
            "--match", '.ts == "{{ts}}"',
        ]
    )
    assert result.exit_code == 0, result.output
    match_jq = fake.scan_calls[0]["filter"].match_jq
    assert match_jq == '.ts == "{{ts}}"'


def test_logs_query_unknown_generator_is_config_error(install_fake_logs):
    """An unknown generator in ``--match`` surfaces as ``ConfigError`` (exit 2)
    before any backend scan call."""
    fake = install_fake_logs(scan=ScanResult(entries=[], matched=0, scanned=0, truncated=False))
    result = _run(
        [
            "--config", str(FIXTURE),
            "logs", "query",
            "--source", "order-service",
            "--match", "{{nope}}",
        ]
    )
    payload = _payload(result)
    assert result.exit_code == 2
    assert payload["error"]["type"] == "ConfigError"
    # Backend never reached.
    assert fake.scan_calls == []


# ---------------------------------------------------------------------------
# logs assert: --match generator substitution
# ---------------------------------------------------------------------------


def test_logs_assert_substitutes_ts_in_match(install_fake_logs):
    """``logs assert --match '.ts == "{{ts}}"'`` reaches the backend's
    ``await_one`` with a 10-digit timestamp in ``match_jq``."""
    fake = install_fake_logs(
        await_one=AwaitResult(entry=None, scanned=0, elapsed_ms=0)
    )
    result = _run(
        [
            "--config", str(FIXTURE),
            "logs", "assert",
            "--source", "order-service",
            "--since", "1m",
            "--match", '.ts == "{{ts}}"',
        ]
    )
    assert result.exit_code == 0 or result.exit_code == 1, result.output
    match_jq = fake.await_one_calls[0]["filter"].match_jq
    assert re.fullmatch(r'\.ts == "\d{10}"', match_jq), match_jq


def test_logs_assert_unknown_generator_is_config_error(install_fake_logs):
    """``logs assert --match '{{nope}}'`` surfaces as ``ConfigError`` (exit 2)
    before the backend is reached."""
    fake = install_fake_logs(
        await_one=AwaitResult(entry=None, scanned=0, elapsed_ms=0)
    )
    result = _run(
        [
            "--config", str(FIXTURE),
            "logs", "assert",
            "--source", "order-service",
            "--since", "1m",
            "--match", "{{nope}}",
        ]
    )
    payload = _payload(result)
    assert result.exit_code == 2
    assert payload["error"]["type"] == "ConfigError"
    assert fake.await_one_calls == []


# ---------------------------------------------------------------------------
# kafka listen: helper fixtures + tests
# ---------------------------------------------------------------------------

# kafka-listen tests plant a live-pid pidfile resolved via os.kill, which is
# unstable on Windows shutdown (same skip as test_kafka_listen_assert_msgs).
pytestmark_listen = pytest.mark.skipif(
    os.name == "nt",
    reason="kafka-listen tests plant a live-pid pidfile resolved via os.kill",
)


def _now_iso_z() -> str:
    return (
        datetime.datetime.now(datetime.timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def _envelope(value: dict, *, topic: str, offset: int = 0) -> dict:
    return {
        "topic": topic,
        "key": None,
        "value": value,
        "partition": 0,
        "offset": offset,
        "timestamp": None,
        "headers": {},
        "captured_at": "2026-07-15T00:00:00Z",
    }


# Canned envelope with id="a" — the tests force {{uuid}} -> ["a"] via monkeypatch.
ENV_A = _envelope({"eventType": "ORDER_CREATED", "id": "a"}, topic="orders.created", offset=0)
ENV_B = _envelope({"eventType": "ORDER_CREATED", "id": "b"}, topic="orders.created", offset=1)
MATCH_BY_ID = '.value.id == "{{uuid}}"'


def _plant_listener(tmp_path: Path, *, run_id: str = "aa112233") -> Path:
    """Plant a state dir with one live-pid pidfile + run dir + orders.created.ndjson.

    Returns the state_dir path.
    """
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    rdir = state_dir / f"listen-{run_id}"
    rdir.mkdir(parents=True)
    (rdir / "events.log").write_text("")
    (rdir / "orders.created.ndjson").write_text(
        "\n".join(json.dumps(env) for env in (ENV_A, ENV_B)) + "\n"
    )
    (state_dir / f"listen-{run_id}.pid").write_text(
        json.dumps(
            {
                "pid": os.getpid(),
                "run_id": run_id,
                "topics": ["orders.created"],
                "group": f"agctl-listen-{run_id}",
                "cluster": "default",
                "started_at": _now_iso_z(),
                "state_dir": str(state_dir),
                "log_path": str(rdir / "events.log"),
            }
        )
    )
    return state_dir


def _write_listen_config(tmp_path: Path) -> Path:
    cfg = tmp_path / "agctl.yaml"
    cfg.write_text(
        "\n".join(
            [
                'version: "3"',
                "kafka:",
                "  clusters:",
                "    default:",
                "      brokers: [broker-a:9092]",
                "  default_cluster: default",
                "",
            ]
        )
    )
    return cfg


# ---------------------------------------------------------------------------
# kafka listen messages: --match generator substitution
# ---------------------------------------------------------------------------


@pytestmark_listen
def test_listen_messages_substitutes_generator_in_match(tmp_path, monkeypatch):
    """``kafka listen messages --match '.value.id == "{{uuid}}"'`` resolves the
    token, and (with the uuid generator forced to return ``"a"``) matches ENV_A."""
    state_dir = _plant_listener(tmp_path)
    monkeypatch.setitem(template_vars_mod.BUILTIN_GENERATORS, "uuid", lambda opts: ["a"])

    result = _kafka_listen_messages_core(
        topic="orders.created",
        match=MATCH_BY_ID,
        param=(),
        limit=50,
        run_id=None,
        pid=None,
        state_dir=str(state_dir),
        template_vars_enabled=True,
    )
    assert result["matched"] == 1
    assert result["messages"][0]["value"]["id"] == "a"


@pytestmark_listen
def test_listen_messages_no_template_vars_leaves_token_literal(tmp_path):
    """``template_vars_enabled=False`` leaves ``{{uuid}}`` literal — jq compiles
    (``"{{uuid}}"`` is a valid string literal), no message matches → matched:0."""
    state_dir = _plant_listener(tmp_path)
    result = _kafka_listen_messages_core(
        topic="orders.created",
        match=MATCH_BY_ID,
        param=(),
        limit=50,
        run_id=None,
        pid=None,
        state_dir=str(state_dir),
        template_vars_enabled=False,
    )
    assert result["matched"] == 0


@pytestmark_listen
def test_listen_messages_unknown_generator_is_config_error(tmp_path):
    """An unknown generator in ``--match`` surfaces as ``ConfigError`` before
    the capture file is scanned."""
    state_dir = _plant_listener(tmp_path)
    with pytest.raises(ConfigError):
        _kafka_listen_messages_core(
            topic="orders.created",
            match='.value.id == "{{nope}}"',
            param=(),
            limit=50,
            run_id=None,
            pid=None,
            state_dir=str(state_dir),
            template_vars_enabled=True,
        )


# ---------------------------------------------------------------------------
# kafka listen assert + results: --match generator substitution at eval time
# ---------------------------------------------------------------------------


@pytestmark_listen
def test_listen_results_substitutes_generator_in_match(tmp_path, monkeypatch):
    """``kafka listen assert --match '.value.id == "{{uuid}}"'`` followed by
    ``kafka listen results`` substitutes the token at evaluation time; with the
    uuid generator forced to ``"a"``, the expectation passes (1/1)."""
    state_dir = _plant_listener(tmp_path)
    cfg_path = _write_listen_config(tmp_path)
    monkeypatch.setitem(template_vars_mod.BUILTIN_GENERATORS, "uuid", lambda opts: ["a"])

    _kafka_listen_assert_core(
        topic="orders.created",
        contains=None,
        match=MATCH_BY_ID,
        pattern=None,
        path=None,
        param=(),
        expect_count=1,
        id=None,
        run_id=None,
        pid=None,
        state_dir=str(state_dir),
    )
    result = _kafka_listen_results_core(
        run_id=None,
        pid=None,
        state_dir=str(state_dir),
        config_path=str(cfg_path),
        overlay_paths=None,
        env_file=None,
        template_vars_enabled=True,
    )
    assert result["passed"] == 1
    assert result["failed"] == 0


@pytestmark_listen
def test_listen_results_unknown_generator_is_config_error(tmp_path):
    """An unknown generator in the attached ``--match`` surfaces as
    ``ConfigError`` at results-evaluation time."""
    state_dir = _plant_listener(tmp_path)
    cfg_path = _write_listen_config(tmp_path)

    _kafka_listen_assert_core(
        topic="orders.created",
        contains=None,
        match='.value.id == "{{nope}}"',
        pattern=None,
        path=None,
        param=(),
        expect_count=1,
        id=None,
        run_id=None,
        pid=None,
        state_dir=str(state_dir),
    )
    with pytest.raises(ConfigError):
        _kafka_listen_results_core(
            run_id=None,
            pid=None,
            state_dir=str(state_dir),
            config_path=str(cfg_path),
            overlay_paths=None,
            env_file=None,
            template_vars_enabled=True,
        )


@pytestmark_listen
def test_listen_results_no_template_vars_leaves_token_literal(tmp_path):
    """``template_vars_enabled=False`` leaves ``{{uuid}}`` literal — jq compiles
    (string literal), no canned message matches → expectation fails (0 < 1)."""
    state_dir = _plant_listener(tmp_path)
    cfg_path = _write_listen_config(tmp_path)

    _kafka_listen_assert_core(
        topic="orders.created",
        contains=None,
        match=MATCH_BY_ID,
        pattern=None,
        path=None,
        param=(),
        expect_count=1,
        id=None,
        run_id=None,
        pid=None,
        state_dir=str(state_dir),
    )
    from agctl.errors import AssertionFailure

    with pytest.raises(AssertionFailure):
        _kafka_listen_results_core(
            run_id=None,
            pid=None,
            state_dir=str(state_dir),
            config_path=str(cfg_path),
            overlay_paths=None,
            env_file=None,
            template_vars_enabled=False,
        )
