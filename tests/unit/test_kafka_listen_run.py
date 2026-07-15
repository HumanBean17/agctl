"""Tests for ``agctl kafka listen run`` foreground streaming command (Task 7).

Three slices:

1. **``resolve_subscriptions`` pure helper** — pattern contributes its topic, and
   (when ``--capture-match`` is unset) its ``match``, plus its ``cluster`` as the
   binding; bare ``--topic`` entries are appended; an explicit ``--capture-match``
   wins over the pattern's match; an unknown pattern raises ``TemplateNotFound``.
2. **CliRunner happy path** — a fake ``ListenEngine`` (injected via the
   ``new_listen_engine`` seam) streams canned ``started`` + ``summary`` NDJSON via
   its ``emit_fn``; stdout has one of each and the process exits 0.
3. **Startup error + mutual exclusion** — a fake engine whose ``start()`` raises
   ``ConnectionFailure`` produces a SINGLE ``{"ok":False,...}`` envelope, exit 2,
   and no event lines; ``--duration`` + ``--until-stopped`` together yield a
   ConfigError envelope, exit 2.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from agctl.cli import cli
from agctl.commands.kafka_listen_commands import (
    kafka_listen_group,
    kafka_listen_run,
    new_listen_engine,
    resolve_subscriptions,
)
from agctl.config.models import Config, KafkaConfig, KafkaPattern
from agctl.errors import ConfigError, TemplateNotFound


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------


def _make_config() -> Config:
    """Config with one pattern ``order-created`` bound to the default cluster."""
    return Config(
        version="3",
        kafka=KafkaConfig(
            clusters={"default": {"brokers": ["broker-a:9092"]}},
            default_cluster="default",
            patterns={
                "order-created": KafkaPattern(
                    description="order created",
                    topic="orders.created",
                    match='.value.eventType == "ORDER_CREATED"',
                    cluster="default",
                ),
            },
        ),
    )


def _write_config(tmp_path: Path) -> Path:
    """Write a one-cluster v3 config with the ``order-created`` pattern."""
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
                "  patterns:",
                "    order-created:",
                "      description: order created",
                "      topic: orders.created",
                '      match: \'.value.eventType == "ORDER_CREATED"\'',
                "      cluster: default",
                "",
            ]
        )
    )
    return cfg


# ---------------------------------------------------------------------------
# resolve_subscriptions (pure unit test)
# ---------------------------------------------------------------------------


class TestResolveSubscriptions:
    def test_pattern_contributes_topic_match_cluster(self):
        """Pattern + bare --topic, no explicit --capture-match:
        pattern's topic/match/cluster all flow through; order preserved."""
        cfg = _make_config()
        topics, match, cluster = resolve_subscriptions(
            cfg,
            topics=["payments.events"],
            patterns=["order-created"],
            capture_match=None,
        )
        assert topics == ["payments.events", "orders.created"]
        assert match == '.value.eventType == "ORDER_CREATED"'
        assert cluster == "default"

    def test_pattern_topic_first_when_only_pattern(self):
        """Only --pattern (no --topic): topics is just the pattern's topic."""
        cfg = _make_config()
        topics, match, cluster = resolve_subscriptions(
            cfg, topics=[], patterns=["order-created"], capture_match=None
        )
        assert topics == ["orders.created"]
        assert match == '.value.eventType == "ORDER_CREATED"'
        assert cluster == "default"

    def test_explicit_capture_match_wins(self):
        """An explicit --capture-match is used verbatim; the pattern's match is ignored."""
        cfg = _make_config()
        topics, match, cluster = resolve_subscriptions(
            cfg,
            topics=["payments.events"],
            patterns=["order-created"],
            capture_match='.value.eventType == "PAID"',
        )
        assert topics == ["payments.events", "orders.created"]
        assert match == '.value.eventType == "PAID"'
        # The pattern's cluster binding still flows through.
        assert cluster == "default"

    def test_unknown_pattern_raises_template_not_found(self):
        """An unknown pattern name raises TemplateNotFound pointing at kafka.patterns.<name>."""
        cfg = _make_config()
        with pytest.raises(TemplateNotFound) as exc_info:
            resolve_subscriptions(cfg, topics=[], patterns=["nope"], capture_match=None)
        assert "nope" in exc_info.value.message
        assert exc_info.value.detail == {"path": "kafka.patterns.nope"}

    def test_dedup_preserves_order(self):
        """A pattern whose topic duplicates a bare --topic collapses to one entry
        in the order of first appearance."""
        cfg = _make_config()
        topics, _match, _cluster = resolve_subscriptions(
            cfg,
            topics=["orders.created"],
            patterns=["order-created"],
            capture_match=None,
        )
        assert topics == ["orders.created"]


# ---------------------------------------------------------------------------
# Fake ListenEngine for the CliRunner slices
# ---------------------------------------------------------------------------


class _FakeListenEngine:
    """Fake ListenEngine that streams a canned ``started`` then ``summary`` line.

    ``emit_fn`` is the same callable the real engine receives
    (``emit_ndjson_line`` by default); we drive it directly so the test asserts
    real stdout, not a captured list. ``start()`` may be replaced per-test (e.g.
    to raise ``ConnectionFailure``).
    """

    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs
        # The real ListenEngine defaults emit_fn to emit_ndjson_line; the command
        # does NOT pass emit_fn, so fall back to the real default here so the
        # fake shares stdout with the CliRunner.
        from agctl.output import emit_ndjson_line

        self.emit_fn = kwargs.get("emit_fn") or emit_ndjson_line
        self.started_called = False
        self.run_called = False
        self.shutdown_called = False

    def start(self) -> None:
        self.started_called = True
        self.emit_fn(
            {
                "event": "started",
                "run_id": self.kwargs["run_id"],
                "topics": list(self.kwargs["topics"]),
                "group": self.kwargs["group"],
                "cluster": self.kwargs["cluster"],
                "started_at": "2026-07-15T00:00:00Z",
            }
        )

    def run(self) -> int:
        self.run_called = True
        return 0

    def shutdown(self) -> None:
        self.shutdown_called = True
        self.emit_fn(
            {
                "event": "summary",
                "topics": [{"topic": t, "captured": 0, "overflowed": False} for t in self.kwargs["topics"]],
                "errors": 0,
                "duration_ms": 10,
            }
        )


# ---------------------------------------------------------------------------
# CliRunner slices
# ---------------------------------------------------------------------------


class TestKafkaListenRunCli:
    def test_run_streams_started_and_summary(self, tmp_path):
        """Happy path: fake engine emits one started + one summary line; exit 0."""
        cfg = _write_config(tmp_path)

        fake_instances = []

        def _factory(**kwargs):
            inst = _FakeListenEngine(**kwargs)
            fake_instances.append(inst)
            return inst

        with patch(
            "agctl.commands.kafka_listen_commands.new_listen_engine",
            side_effect=_factory,
        ) as mock_factory:
            result = CliRunner().invoke(
                cli,
                [
                    "--config", str(cfg),
                    "kafka", "listen", "run",
                    "--topic", "orders.created",
                    "--duration", "0.01",
                ],
                catch_exceptions=False,
            )

        assert result.exit_code == 0, result.output

        # The factory was called once with the resolved topic/cluster/group/run_dir.
        mock_factory.assert_called_once()
        kwargs = mock_factory.call_args.kwargs
        assert kwargs["topics"] == ["orders.created"]
        assert kwargs["cluster"] == "default"
        assert kwargs["capture_match"] is None
        assert kwargs["duration"] == 0.01
        assert kwargs["group"] == f"agctl-listen-{kwargs['run_id']}"
        # run_dir ends with listen-<run_id>/.
        assert kwargs["run_dir"].name == f"listen-{kwargs['run_id']}"

        # stdout has exactly one started + one summary line (in order), no envelope.
        lines = [json.loads(ln) for ln in result.output.splitlines() if ln.strip()]
        events = [ln.get("event") for ln in lines]
        assert "started" in events
        assert "summary" in events
        assert events.index("started") < events.index("summary")
        # No ok/envelope line on the happy path (streaming command).
        assert all("ok" not in ln for ln in lines)

        # Full lifecycle was driven.
        assert fake_instances[0].started_called is True
        assert fake_instances[0].run_called is True
        assert fake_instances[0].shutdown_called is True

    def test_run_pattern_resolves_topic_match_cluster(self, tmp_path):
        """``--pattern order-created`` contributes topic + capture_match + cluster."""
        cfg = _write_config(tmp_path)

        captured = {}

        def _factory(**kwargs):
            captured.update(kwargs)
            return _FakeListenEngine(**kwargs)

        with patch(
            "agctl.commands.kafka_listen_commands.new_listen_engine",
            side_effect=_factory,
        ):
            result = CliRunner().invoke(
                cli,
                [
                    "--config", str(cfg),
                    "kafka", "listen", "run",
                    "--pattern", "order-created",
                    "--duration", "0.01",
                ],
                catch_exceptions=False,
            )

        assert result.exit_code == 0, result.output
        # Pattern's topic + match + cluster all flowed through to the engine.
        assert captured["topics"] == ["orders.created"]
        assert captured["capture_match"] == '.value.eventType == "ORDER_CREATED"'
        assert captured["cluster"] == "default"

    def test_engine_start_raises_connection_failure(self, tmp_path):
        """A startup ConnectionFailure → single ok:False envelope, exit 2, no event lines."""

        class _FailingStart(_FakeListenEngine):
            def start(self) -> None:  # noqa: D401 - intentional failure
                from agctl.errors import ConnectionFailure

                raise ConnectionFailure("broker unreachable", {"broker": "broker-a:9092"})

        cfg = _write_config(tmp_path)

        with patch(
            "agctl.commands.kafka_listen_commands.new_listen_engine",
            side_effect=lambda **kw: _FailingStart(**kw),
        ):
            result = CliRunner().invoke(
                cli,
                [
                    "--config", str(cfg),
                    "kafka", "listen", "run",
                    "--topic", "orders.created",
                    "--duration", "0.01",
                ],
            )

        assert result.exit_code == 2
        lines = [json.loads(ln) for ln in result.output.splitlines() if ln.strip()]
        # Exactly one line: the structured startup-error envelope.
        assert len(lines) == 1
        envelope = lines[0]
        assert envelope["ok"] is False
        assert envelope["command"] == "kafka.listen.run"
        assert envelope["error"]["type"] == "ConnectionError"
        assert envelope["error"]["message"] == "broker unreachable"

    def test_duration_and_until_stopped_mutually_exclusive(self, tmp_path):
        """--duration + --until-stopped → ConfigError envelope, exit 2, no engine built."""
        cfg = _write_config(tmp_path)

        with patch(
            "agctl.commands.kafka_listen_commands.new_listen_engine"
        ) as mock_factory:
            result = CliRunner().invoke(
                cli,
                [
                    "--config", str(cfg),
                    "kafka", "listen", "run",
                    "--topic", "orders.created",
                    "--duration", "5",
                    "--until-stopped",
                ],
            )

        assert result.exit_code == 2
        # No engine was constructed (guard short-circuits before build).
        mock_factory.assert_not_called()
        lines = [json.loads(ln) for ln in result.output.splitlines() if ln.strip()]
        assert len(lines) == 1
        envelope = lines[0]
        assert envelope["ok"] is False
        assert envelope["command"] == "kafka.listen.run"
        assert envelope["error"]["type"] == "ConfigError"
        assert "mutually exclusive" in envelope["error"]["message"]

    def test_requires_at_least_one_topic_or_pattern(self, tmp_path):
        """No --topic and no --pattern → ConfigError, exit 2."""
        cfg = _write_config(tmp_path)

        with patch(
            "agctl.commands.kafka_listen_commands.new_listen_engine"
        ) as mock_factory:
            result = CliRunner().invoke(
                cli,
                [
                    "--config", str(cfg),
                    "kafka", "listen", "run",
                    "--duration", "5",
                ],
            )

        assert result.exit_code == 2
        mock_factory.assert_not_called()
        lines = [json.loads(ln) for ln in result.output.splitlines() if ln.strip()]
        assert len(lines) == 1
        envelope = lines[0]
        assert envelope["ok"] is False
        assert envelope["command"] == "kafka.listen.run"
        assert envelope["error"]["type"] == "ConfigError"
        assert "at least one" in envelope["error"]["message"]

    def test_run_malformed_capture_match_raises_configerror(self, tmp_path):
        """A malformed --capture-match jq expression → single ConfigError envelope,
        exit 2, no event lines, engine never built (loud-on-typo parity with the
        other jq modes, which are compile-validated in capture_file.build_predicate)."""
        cfg = _write_config(tmp_path)

        with patch(
            "agctl.commands.kafka_listen_commands.new_listen_engine"
        ) as mock_factory:
            result = CliRunner().invoke(
                cli,
                [
                    "--config", str(cfg),
                    "kafka", "listen", "run",
                    "--topic", "orders.created",
                    "--capture-match", "value.eventType ==",
                    "--duration", "0.01",
                ],
            )

        assert result.exit_code == 2
        # No engine constructed (compile check short-circuits before build/start).
        mock_factory.assert_not_called()
        lines = [json.loads(ln) for ln in result.output.splitlines() if ln.strip()]
        # Exactly one line: the structured startup-error envelope, no event lines.
        assert len(lines) == 1
        envelope = lines[0]
        assert envelope["ok"] is False
        assert envelope["command"] == "kafka.listen.run"
        assert envelope["error"]["type"] == "ConfigError"
        assert "invalid jq expression" in envelope["error"]["message"]


# ---------------------------------------------------------------------------
# CLI group registration
# ---------------------------------------------------------------------------


class TestListenGroupRegistered:
    def test_listen_subcommand_registered_under_kafka(self):
        """``agctl kafka listen --help`` shows the ``run`` subcommand."""
        result = CliRunner().invoke(cli, ["kafka", "listen", "--help"])
        assert result.exit_code == 0
        assert "run" in result.output
        assert "Kafka long-lived capture listener" in result.output

    def test_listen_run_help_lists_flags(self):
        """``agctl kafka listen run --help`` lists the streaming-command flags."""
        result = CliRunner().invoke(cli, ["kafka", "listen", "run", "--help"])
        assert result.exit_code == 0
        for flag in (
            "--topic",
            "--pattern",
            "--cluster",
            "--capture-match",
            "--max-bytes-per-topic",
            "--duration",
            "--until-stopped",
            "--run-id",
            "--state-dir",
        ):
            assert flag in result.output

    def test_group_object_exposed(self):
        """The module exposes ``kafka_listen_group`` and ``kafka_listen_run``."""
        assert kafka_listen_group.name == "listen"
        assert kafka_listen_run.name == "run"
        # new_listen_engine is a callable seam (default → ListenEngine).
        assert callable(new_listen_engine)
