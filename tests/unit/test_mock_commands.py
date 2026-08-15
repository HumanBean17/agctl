"""Tests for agctl mock run command (Task 8)."""

import json
import os
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest
from click.testing import CliRunner

from agctl.cli import cli
from agctl.commands.mock_commands import (
    _mock_start_core,
    _mock_status_core,
    _mock_stop_core,
    _require_posix_daemon,
    _resolve_engines,
    mock_run,
    new_mock_engine,
)
from agctl.errors import ConfigError


@pytest.fixture
def temp_config(tmp_path):
    """Create a temporary agctl.yaml config."""
    config_path = tmp_path / "agctl.yaml"
    return config_path


@pytest.fixture
def fake_engine():
    """A fake MockEngine that records calls and returns a canned exit code."""
    engine = MagicMock()
    engine.start = MagicMock()
    engine.run = MagicMock(return_value=0)
    engine.shutdown = MagicMock()
    return engine


class TestMockRunCommand:
    """Test the mock run command with various scenarios."""

    def test_only_http_with_stubs(self, temp_config, fake_engine):
        """--only http with 2-stub mocks.http -> run_http=True, run_kafka=False, kafka_clients=None."""
        config_content = """
version: "3"
mocks:
  http:
    listen: "0.0.0.0:18080"
    stubs:
      stub1:
        method: GET
        path: /test1
        response:
          status: 200
          body: '{"result": "ok"}'
      stub2:
        method: POST
        path: /test2
        response:
          status: 201
          body: '{"created": true}'
"""
        temp_config.write_text(config_content)

        with patch("agctl.commands.mock_commands.new_mock_engine", return_value=fake_engine) as mock_factory:
            result = CliRunner().invoke(
                cli,
                ["--config", str(temp_config), "mock", "run", "--only", "http"],
                catch_exceptions=False,
            )

            # Verify exit code from fake engine
            assert result.exit_code == 0

            # Verify new_mock_engine was called with correct parameters
            mock_factory.assert_called_once()
            call_kwargs = mock_factory.call_args.kwargs
            assert call_kwargs["run_http"] is True
            assert call_kwargs["run_kafka"] is False
            assert call_kwargs["kafka_clients"] is None

            # Verify engine lifecycle
            fake_engine.start.assert_called_once()
            fake_engine.run.assert_called_once()
            fake_engine.shutdown.assert_called_once()

    def test_only_kafka_with_reactors(self, temp_config, fake_engine):
        """--only kafka with reactors resolved to the default v3 cluster
        (kafka.clusters.default.brokers) -> run_kafka=True, kafka_clients maps
        the reactor name to a KafkaClient built for that resolved cluster."""
        # Skip if confluent_kafka is not installed
        pytest.importorskip("confluent_kafka")

        config_content = """
version: "3"
kafka:
  clusters:
    default:
      brokers:
        - "localhost:9092"
mocks:
  kafka:
    reactors:
      reactor1:
        topic: test-topic
        consumer_group: test-group
        reaction:
          topic: output-topic
          value: {"result": "reacted"}
"""
        temp_config.write_text(config_content)

        with patch("agctl.commands.mock_commands.new_mock_engine", return_value=fake_engine) as mock_factory:
            result = CliRunner().invoke(
                cli,
                ["--config", str(temp_config), "mock", "run", "--only", "kafka"],
                catch_exceptions=False,
            )

            assert result.exit_code == 0

            # Verify new_mock_engine parameters
            mock_factory.assert_called_once()
            call_kwargs = mock_factory.call_args.kwargs
            assert call_kwargs["run_http"] is False
            assert call_kwargs["run_kafka"] is True

            # Verify kafka_clients is a dict mapping reactor name -> KafkaClient
            from agctl.clients.kafka_client import KafkaClient
            kafka_clients = call_kwargs["kafka_clients"]
            assert isinstance(kafka_clients, dict)
            assert "reactor1" in kafka_clients
            assert isinstance(kafka_clients["reactor1"], KafkaClient)

    def test_mock_run_builds_per_cluster_clients(self, temp_config, fake_engine):
        """mock run with two reactors binding two distinct clusters -> new_kafka_client
        called once per distinct cluster, and kafka_clients maps each reactor name
        to the client built for its resolved cluster."""
        config_content = """
version: "3"
kafka:
  clusters:
    main:
      brokers:
        - "main:9092"
    analytics:
      brokers:
        - "analytics:9092"
  default_cluster: main
mocks:
  kafka:
    reactors:
      rA:
        topic: topicA
        cluster: main
        reaction:
          topic: out
          value: {}
      rB:
        topic: topicB
        cluster: analytics
        reaction:
          topic: out
          value: {}
"""
        temp_config.write_text(config_content)

        recorded_clusters = []

        def fake_client_factory(cluster, group_id=None):
            recorded_clusters.append(cluster)
            return MagicMock()  # fake client; engine is also faked

        with patch(
            "agctl.commands.mock_commands.new_kafka_client",
            side_effect=fake_client_factory,
        ), patch(
            "agctl.commands.mock_commands.new_mock_engine",
            return_value=fake_engine,
        ) as mock_factory:
            result = CliRunner().invoke(
                cli,
                ["--config", str(temp_config), "mock", "run", "--only", "kafka"],
                catch_exceptions=False,
            )

            assert result.exit_code == 0

            # One new_kafka_client call per distinct cluster (main, analytics).
            assert len(recorded_clusters) == 2
            brokers_seen = {tuple(c.brokers) for c in recorded_clusters}
            assert brokers_seen == {("main:9092",), ("analytics:9092",)}

            # kafka_clients maps each reactor name to a client.
            call_kwargs = mock_factory.call_args.kwargs
            kafka_clients = call_kwargs["kafka_clients"]
            assert set(kafka_clients.keys()) == {"rA", "rB"}
            # Reactors sharing a cluster would reuse the same instance; here each
            # has its own cluster, so the two client objects differ.
            assert kafka_clients["rA"] is not kafka_clients["rB"]

    def test_mock_run_reuses_client_for_shared_cluster(self, temp_config, fake_engine):
        """Two reactors bound to the SAME cluster -> new_kafka_client called
        exactly once, and both reactors share the same client instance.

        Pins the DRY/reuse path that ``test_mock_run_builds_per_cluster_clients``
        (2 reactors / 2 clusters) does not cover.
        """
        config_content = """
version: "3"
kafka:
  clusters:
    shared:
      brokers:
        - "shared:9092"
  default_cluster: shared
mocks:
  kafka:
    reactors:
      rA:
        topic: topicA
        cluster: shared
        reaction:
          topic: out
          value: {}
      rB:
        topic: topicB
        cluster: shared
        reaction:
          topic: out
          value: {}
"""
        temp_config.write_text(config_content)

        call_count = {"n": 0}

        def fake_client_factory(cluster, group_id=None):
            call_count["n"] += 1
            return MagicMock()  # fake client; engine is also faked

        with patch(
            "agctl.commands.mock_commands.new_kafka_client",
            side_effect=fake_client_factory,
        ), patch(
            "agctl.commands.mock_commands.new_mock_engine",
            return_value=fake_engine,
        ) as mock_factory:
            result = CliRunner().invoke(
                cli,
                ["--config", str(temp_config), "mock", "run", "--only", "kafka"],
                catch_exceptions=False,
            )

            assert result.exit_code == 0

            # The factory is called exactly once for the single shared cluster.
            assert call_count["n"] == 1

            # Both reactors map to the SAME client instance.
            call_kwargs = mock_factory.call_args.kwargs
            kafka_clients = call_kwargs["kafka_clients"]
            assert set(kafka_clients.keys()) == {"rA", "rB"}
            assert kafka_clients["rA"] is kafka_clients["rB"]

    def test_only_http_without_mocks_http(self, temp_config):
        """--only http with no mocks.http -> exit 2, ConfigError envelope."""
        config_content = """
version: "3"
mocks:
  kafka:
    reactors:
      reactor1:
        topic: test-topic
        reaction:
          topic: output-topic
          value: {}
"""
        temp_config.write_text(config_content)

        result = CliRunner().invoke(
            cli,
            ["--config", str(temp_config), "mock", "run", "--only", "http"],
        )

        assert result.exit_code == 2

        # Verify structured error envelope
        output_lines = [line for line in result.output.split("\n") if line.strip()]
        assert len(output_lines) == 1
        envelope = json.loads(output_lines[0])
        assert envelope["ok"] is False
        assert envelope["command"] == "mock.run"
        assert envelope["error"]["type"] == "ConfigError"
        assert "no mocks.http configured" in envelope["error"]["message"]

    def test_no_mocks_section_no_only(self, temp_config, fake_engine):
        """No mocks section, no --only -> exit 0, no-op engine (run_http=False, run_kafka=False)."""
        config_content = """
version: "3"
kafka:
  clusters:
    default:
      brokers:
        - "localhost:9092"
  default_cluster: default
"""
        temp_config.write_text(config_content)

        with patch("agctl.commands.mock_commands.new_mock_engine", return_value=fake_engine) as mock_factory:
            result = CliRunner().invoke(
                cli,
                ["--config", str(temp_config), "mock", "run"],
                catch_exceptions=False,
            )

            assert result.exit_code == 0

            # Verify no-op engine parameters
            mock_factory.assert_called_once()
            call_kwargs = mock_factory.call_args.kwargs
            assert call_kwargs["run_http"] is False
            assert call_kwargs["run_kafka"] is False

            # Verify engine lifecycle
            fake_engine.start.assert_called_once()
            fake_engine.run.assert_called_once()
            fake_engine.shutdown.assert_called_once()

    def test_kafka_reactors_without_brokers(self, temp_config):
        """mocks.kafka reactors whose resolved cluster has empty brokers ->
        exit 2 with a clear ConfigError naming the reactor + cluster.

        The per-reactor brokers guard in ``mock_run`` (spec §11) fires BEFORE
        any confluent_kafka import/probe, so this guarantee holds regardless of
        whether the extra is installed.
        """
        config_content = """
version: "3"
kafka:
  clusters:
    default:
      brokers: []
  default_cluster: default
mocks:
  kafka:
    reactors:
      reactor1:
        topic: test-topic
        reaction:
          topic: output-topic
          value: {}
"""
        temp_config.write_text(config_content)

        result = CliRunner().invoke(
            cli,
            ["--config", str(temp_config), "mock", "run"],
        )

        assert result.exit_code == 2

        # Verify structured error envelope
        output_lines = [line for line in result.output.split("\n") if line.strip()]
        assert len(output_lines) == 1
        envelope = json.loads(output_lines[0])
        assert envelope["ok"] is False
        assert envelope["command"] == "mock.run"

        # Clear ConfigError naming the offending reactor + cluster (spec §11).
        error = envelope["error"]
        assert error["type"] == "ConfigError"
        assert "brokers" in error["message"]
        detail = error["detail"]
        assert detail["reactor"] == "reactor1"
        assert detail["cluster"] == "default"

    def test_duration_and_until_stopped_mutually_exclusive(self, temp_config):
        """--duration and --until-stopped together -> exit 2, ConfigError envelope."""
        config_content = """
version: "3"
"""
        temp_config.write_text(config_content)

        result = CliRunner().invoke(
            cli,
            ["--config", str(temp_config), "mock", "run", "--duration", "10", "--until-stopped"],
        )

        assert result.exit_code == 2

        # Verify structured error envelope
        output_lines = [line for line in result.output.split("\n") if line.strip()]
        assert len(output_lines) == 1
        envelope = json.loads(output_lines[0])
        assert envelope["ok"] is False
        assert envelope["command"] == "mock.run"
        assert envelope["error"]["type"] == "ConfigError"
        assert "mutually exclusive" in envelope["error"]["message"]

    def test_engine_start_raises_connection_failure(self, temp_config):
        """Engine start() raises ConnectionFailure -> mock.run envelope with ConnectionError, exit 2."""
        config_content = """
version: "3"
mocks:
  http:
    listen: "0.0.0.0:18080"
    stubs:
      stub1:
        method: GET
        path: /test
        response:
          status: 200
          body: '{}'
"""
        temp_config.write_text(config_content)

        from agctl.errors import ConnectionFailure

        def failing_start():
            raise ConnectionFailure("broker unreachable", {})

        fake_engine = MagicMock()
        fake_engine.start = failing_start

        with patch("agctl.commands.mock_commands.new_mock_engine", return_value=fake_engine):
            result = CliRunner().invoke(
                cli,
                ["--config", str(temp_config), "mock", "run"],
            )

            assert result.exit_code == 2

            # Verify structured error envelope
            output_lines = [line for line in result.output.split("\n") if line.strip()]
            assert len(output_lines) == 1
            envelope = json.loads(output_lines[0])
            assert envelope["ok"] is False
            assert envelope["command"] == "mock.run"
            assert envelope["error"]["type"] == "ConnectionError"

    def test_http_listen_override(self, temp_config, fake_engine):
        """--http-listen 127.0.0.1:9999 overrides config listen -> engine called with http_listen."""
        config_content = """
version: "3"
mocks:
  http:
    listen: "0.0.0.0:18080"
    stubs:
      stub1:
        method: GET
        path: /test
        response:
          status: 200
          body: '{}'
"""
        temp_config.write_text(config_content)

        with patch("agctl.commands.mock_commands.new_mock_engine", return_value=fake_engine) as mock_factory:
            result = CliRunner().invoke(
                cli,
                ["--config", str(temp_config), "mock", "run", "--http-listen", "127.0.0.1:9999"],
                catch_exceptions=False,
            )

            assert result.exit_code == 0

            # Verify http_listen override
            mock_factory.assert_called_once()
            call_kwargs = mock_factory.call_args.kwargs
            assert call_kwargs["http_listen"] == "127.0.0.1:9999"

    def test_http_listen_bad_value(self, temp_config):
        """--http-listen 'bad:no-port' -> exit 2 (parse_listen fails)."""
        config_content = """
version: "3"
mocks:
  http:
    listen: "0.0.0.0:18080"
    stubs:
      stub1:
        method: GET
        path: /test
        response:
          status: 200
          body: '{}'
"""
        temp_config.write_text(config_content)

        result = CliRunner().invoke(
            cli,
            ["--config", str(temp_config), "mock", "run", "--http-listen", "bad:no-port"],
        )

        assert result.exit_code == 2

        # Verify structured error envelope
        output_lines = [line for line in result.output.split("\n") if line.strip()]
        assert len(output_lines) == 1
        envelope = json.loads(output_lines[0])
        assert envelope["ok"] is False
        assert envelope["command"] == "mock.run"
        assert envelope["error"]["type"] == "ConfigError"

    def test_started_line_emitted(self, temp_config, fake_engine):
        """Config with mocks.http, --only http -> stdout's first line is the engine's started."""
        config_content = """
version: "3"
mocks:
  http:
    listen: "0.0.0.0:18080"
    stubs:
      stub1:
        method: GET
        path: /test
        response:
          status: 200
          body: '{}'
"""
        temp_config.write_text(config_content)

        # Make the fake engine emit a started line
        def fake_run():
            # Emit started line (simulating what the real engine does)
            import sys
            started = {"event": "started", "http": {"listen": "0.0.0.0:18080", "stubs": 1}}
            sys.stdout.write(json.dumps(started) + "\n")
            sys.stdout.flush()
            return 0

        fake_engine.run = fake_run

        with patch("agctl.commands.mock_commands.new_mock_engine", return_value=fake_engine):
            result = CliRunner().invoke(
                cli,
                ["--config", str(temp_config), "mock", "run", "--only", "http"],
                catch_exceptions=False,
            )

            assert result.exit_code == 0

            # Verify first line is the started event
            lines = [line for line in result.output.split("\n") if line.strip()]
            assert len(lines) >= 1
            first_line = json.loads(lines[0])
            assert first_line["event"] == "started"
            assert "http" in first_line

    def test_fail_fast_flag_passed(self, temp_config, fake_engine):
        """--fail-fast flag is passed to the engine."""
        config_content = """
version: "3"
mocks:
  http:
    listen: "0.0.0.0:18080"
    stubs:
      stub1:
        method: GET
        path: /test
        response:
          status: 200
          body: '{}'
"""
        temp_config.write_text(config_content)

        with patch("agctl.commands.mock_commands.new_mock_engine", return_value=fake_engine) as mock_factory:
            result = CliRunner().invoke(
                cli,
                ["--config", str(temp_config), "mock", "run", "--fail-fast"],
                catch_exceptions=False,
            )

            assert result.exit_code == 0

            # Verify fail_fast is passed
            mock_factory.assert_called_once()
            call_kwargs = mock_factory.call_args.kwargs
            assert call_kwargs["fail_fast"] is True

    def test_duration_flag_passed(self, temp_config, fake_engine):
        """--duration flag is passed to the engine."""
        config_content = """
version: "3"
mocks:
  http:
    listen: "0.0.0.0:18080"
    stubs:
      stub1:
        method: GET
        path: /test
        response:
          status: 200
          body: '{}'
"""
        temp_config.write_text(config_content)

        with patch("agctl.commands.mock_commands.new_mock_engine", return_value=fake_engine) as mock_factory:
            result = CliRunner().invoke(
                cli,
                ["--config", str(temp_config), "mock", "run", "--duration", "42.5"],
                catch_exceptions=False,
            )

            assert result.exit_code == 0

            # Verify duration is passed
            mock_factory.assert_called_once()
            call_kwargs = mock_factory.call_args.kwargs
            assert call_kwargs["duration"] == 42.5

    def test_exit_code_from_engine(self, temp_config):
        """Engine returns non-zero exit code -> process exits with that code."""
        config_content = """
version: "3"
mocks:
  http:
    listen: "0.0.0.0:18080"
    stubs:
      stub1:
        method: GET
        path: /test
        response:
          status: 200
          body: '{}'
"""
        temp_config.write_text(config_content)

        fake_engine = MagicMock()
        fake_engine.run = MagicMock(return_value=1)

        with patch("agctl.commands.mock_commands.new_mock_engine", return_value=fake_engine):
            result = CliRunner().invoke(
                cli,
                ["--config", str(temp_config), "mock", "run"],
                catch_exceptions=False,
            )

            assert result.exit_code == 1


# ---------------------------------------------------------------------------
# Task 6 (cross-transport effects): resolver building in mock_run
# ---------------------------------------------------------------------------


def _single_envelope(result) -> dict:
    """Parse the single NDJSON envelope line out of a CliRunner result."""
    lines = [line for line in result.output.split("\n") if line.strip()]
    assert len(lines) == 1, f"expected exactly one output line, got: {lines}"
    return json.loads(lines[0])


class TestMockRunEffectResolvers:
    """Task 6: mock_run builds kafka/http effect resolvers from config."""

    HTTP_STUB_WITH_KAFKA_EFFECT = """
version: "3"
kafka:
  clusters:
    main:
      brokers:
        - "main:9092"
    effects:
      brokers:
        - "effects:9092"
  default_cluster: main
mocks:
  http:
    listen: "0.0.0.0:18080"
    stubs:
      stub1:
        method: GET
        path: /test
        response:
          status: 200
          body: '{}'
        effects:
          - type: kafka
            topic: eff-topic
            cluster: effects
            value: {"ping": 1}
"""

    def test_kafka_effect_resolver_passed_and_client_built(
        self, temp_config, fake_engine, monkeypatch
    ):
        """HTTP stub with a kafka effect on a cluster no reactor uses ->
        new_kafka_client called for that cluster and kafka_resolver passed."""
        temp_config.write_text(self.HTTP_STUB_WITH_KAFKA_EFFECT)

        recorded = []
        built = MagicMock()
        built.codec = None

        def fake_factory(cluster, group_id=None, *, codec=None):
            recorded.append(cluster)
            built.codec = codec
            return built

        monkeypatch.setattr(
            "agctl.commands.mock_commands.new_kafka_client", fake_factory
        )
        # No topic format config anywhere: the real _resolve_codec resolves
        # JSON and returns codec=None without touching Schema Registry.

        with patch(
            "agctl.commands.mock_commands.new_mock_engine",
            return_value=fake_engine,
        ) as mock_factory:
            result = CliRunner().invoke(
                cli,
                ["--config", str(temp_config), "mock", "run", "--only", "http"],
                catch_exceptions=False,
            )

            assert result.exit_code == 0

            call_kwargs = mock_factory.call_args.kwargs
            kafka_resolver = call_kwargs["kafka_resolver"]
            assert callable(kafka_resolver)
            # http_resolver is None: no http effects in this config
            assert call_kwargs["http_resolver"] is None

            # The client is built lazily on first resolver invocation, for the
            # EFFECT's cluster ("effects") — a cluster no reactor references.
            from agctl.config.models import KafkaEffect

            effect = KafkaEffect(
                type="kafka", topic="eff-topic", cluster="effects", value={"ping": 1}
            )
            client, codec = kafka_resolver(effect)
            assert client is built
            assert codec is None  # pure-JSON topic -> codec None
            assert built.codec is None  # client is codec-less (_raw produce)
            assert [c.brokers for c in recorded] == [["effects:9092"]]

    def test_kafka_resolver_caches_and_skips_reprobe(
        self, temp_config, fake_engine, monkeypatch
    ):
        """Calling kafka_resolver twice for the same (cluster, topic, fmt)
        returns the cached (client, codec) and does not re-probe."""
        temp_config.write_text(self.HTTP_STUB_WITH_KAFKA_EFFECT)

        client_calls = {"n": 0}
        codec_calls = []

        def fake_factory(cluster, group_id=None, *, codec=None):
            client_calls["n"] += 1
            return MagicMock()

        def fake_resolve_codec(
            cfg, topic, cluster_name, cli_vf, cli_kf, *, probe=True
        ):
            codec_calls.append(
                {"topic": topic, "cluster": cluster_name, "probe": probe}
            )
            return None, None, None  # JSON codec

        monkeypatch.setattr(
            "agctl.commands.mock_commands.new_kafka_client", fake_factory
        )
        monkeypatch.setattr(
            "agctl.commands.mock_commands._resolve_codec", fake_resolve_codec
        )

        with patch(
            "agctl.commands.mock_commands.new_mock_engine",
            return_value=fake_engine,
        ) as mock_factory:
            result = CliRunner().invoke(
                cli,
                ["--config", str(temp_config), "mock", "run", "--only", "http"],
                catch_exceptions=False,
            )

            assert result.exit_code == 0
            kafka_resolver = mock_factory.call_args.kwargs["kafka_resolver"]

            from agctl.config.models import KafkaEffect

            effect = KafkaEffect(
                type="kafka", topic="eff-topic", cluster="effects", value={"ping": 1}
            )
            first_client, first_codec = kafka_resolver(effect)
            assert client_calls["n"] == 1
            # First resolution probes (the cluster has not been probed yet).
            assert codec_calls == [
                {"topic": "eff-topic", "cluster": "effects", "probe": True}
            ]

            # Second call: cached (client, codec), no new client, no re-resolve.
            second_client, second_codec = kafka_resolver(effect)
            assert second_client is first_client
            assert second_codec is first_codec
            assert client_calls["n"] == 1
            assert len(codec_calls) == 1

    def test_kafka_effect_unresolvable_cluster_fails_startup(
        self, temp_config, fake_engine
    ):
        """A kafka effect naming an unknown cluster -> ConfigError (exit 2)
        BEFORE the engine is constructed."""
        config_content = """
version: "3"
kafka:
  clusters:
    main:
      brokers:
        - "main:9092"
    other:
      brokers:
        - "other:9092"
  default_cluster: main
mocks:
  http:
    listen: "0.0.0.0:18080"
    stubs:
      stub1:
        method: GET
        path: /test
        response:
          status: 200
          body: '{}'
        effects:
          - type: kafka
            topic: eff-topic
            cluster: missing
            value: {"ping": 1}
"""
        temp_config.write_text(config_content)

        with patch(
            "agctl.commands.mock_commands.new_mock_engine",
            return_value=fake_engine,
        ) as mock_factory:
            result = CliRunner().invoke(
                cli,
                ["--config", str(temp_config), "mock", "run", "--only", "http"],
            )

            assert result.exit_code == 2
            envelope = _single_envelope(result)
            assert envelope["error"]["type"] == "ConfigError"
            assert "missing" in envelope["error"]["message"]
            # The engine is never constructed (startup fails before it).
            mock_factory.assert_not_called()

    def test_http_effect_resolver_service_mode(self, temp_config, fake_engine):
        """An http effect with service="order-service" (declared) ->
        http_resolver passed; returns an HttpClient bound to the service's
        base_url and the effect's path."""
        config_content = """
version: "3"
services:
  order-service:
    base_url: http://orders.internal:8080
    timeout_seconds: 7
mocks:
  http:
    listen: "0.0.0.0:18080"
    stubs:
      stub1:
        method: GET
        path: /test
        response:
          status: 200
          body: '{}'
        effects:
          - type: http
            service: order-service
            path: /internal/notify
            method: POST
            body: {"ok": true}
"""
        temp_config.write_text(config_content)

        with patch(
            "agctl.commands.mock_commands.new_mock_engine",
            return_value=fake_engine,
        ) as mock_factory:
            result = CliRunner().invoke(
                cli,
                ["--config", str(temp_config), "mock", "run", "--only", "http"],
                catch_exceptions=False,
            )

            assert result.exit_code == 0
            call_kwargs = mock_factory.call_args.kwargs
            http_resolver = call_kwargs["http_resolver"]
            assert callable(http_resolver)
            # kafka_resolver is None: no kafka effects in this config
            assert call_kwargs["kafka_resolver"] is None

            import httpx

            from agctl.config.models import HttpEffect

            effect = HttpEffect(
                type="http", service="order-service", path="/internal/notify"
            )
            client, path = http_resolver(effect)
            # Assert on the observable HttpClient contract (base_url/timeout on
            # the wrapped httpx client + a working .request) rather than
            # isinstance: test_http_client.py reloads the module, which
            # replaces the class object and breaks class-identity checks.
            assert client._client.base_url == "http://orders.internal:8080"
            # Service timeout_seconds rides along (effect.timeout unset).
            assert client._client.timeout == httpx.Timeout(7)
            assert callable(client.request)
            assert path == "/internal/notify"

    def test_http_effect_resolver_caches_client_per_base_url(
        self, temp_config, fake_engine
    ):
        """Two http effects on the same base_url (same service) reuse ONE
        HttpClient instance (connection pooling)."""
        config_content = """
version: "3"
services:
  order-service:
    base_url: http://orders.internal:8080
mocks:
  http:
    listen: "0.0.0.0:18080"
    stubs:
      stub1:
        method: GET
        path: /test
        response:
          status: 200
          body: '{}'
        effects:
          - type: http
            service: order-service
            path: /a
          - type: http
            service: order-service
            path: /b
"""
        temp_config.write_text(config_content)

        with patch(
            "agctl.commands.mock_commands.new_mock_engine",
            return_value=fake_engine,
        ) as mock_factory:
            result = CliRunner().invoke(
                cli,
                ["--config", str(temp_config), "mock", "run", "--only", "http"],
                catch_exceptions=False,
            )

            assert result.exit_code == 0
            http_resolver = mock_factory.call_args.kwargs["http_resolver"]

            from agctl.config.models import HttpEffect

            c1, p1 = http_resolver(
                HttpEffect(type="http", service="order-service", path="/a")
            )
            c2, p2 = http_resolver(
                HttpEffect(type="http", service="order-service", path="/b")
            )
            assert c1 is c2
            assert (p1, p2) == ("/a", "/b")

    def test_http_effect_resolver_url_mode(self, temp_config, fake_engine):
        """An http effect with a literal url -> resolver splits base_url/path
        from the url (no configured service needed)."""
        config_content = """
version: "3"
mocks:
  http:
    listen: "0.0.0.0:18080"
    stubs:
      stub1:
        method: GET
        path: /test
        response:
          status: 200
          body: '{}'
        effects:
          - type: http
            url: http://legacy.internal:9999/legacy/path?x=1
            method: GET
"""
        temp_config.write_text(config_content)

        with patch(
            "agctl.commands.mock_commands.new_mock_engine",
            return_value=fake_engine,
        ) as mock_factory:
            result = CliRunner().invoke(
                cli,
                ["--config", str(temp_config), "mock", "run", "--only", "http"],
                catch_exceptions=False,
            )

            assert result.exit_code == 0
            http_resolver = mock_factory.call_args.kwargs["http_resolver"]

            from agctl.config.models import HttpEffect

            effect = HttpEffect(
                type="http", url="http://legacy.internal:9999/legacy/path?x=1"
            )
            client, path = http_resolver(effect)
            assert client._client.base_url == "http://legacy.internal:9999"
            assert path == "/legacy/path?x=1"

    def test_http_effect_unknown_service_fails_before_engine(
        self, temp_config, fake_engine
    ):
        """An http effect referencing an undeclared service -> ConfigError
        (exit 2) with the dotted effects path, BEFORE the engine is built."""
        config_content = """
version: "3"
services:
  other-service:
    base_url: http://other.internal:8080
mocks:
  http:
    listen: "0.0.0.0:18080"
    stubs:
      stub1:
        method: GET
        path: /test
        response:
          status: 200
          body: '{}'
        effects:
          - type: http
            service: order-service
            path: /internal/notify
"""
        temp_config.write_text(config_content)

        with patch(
            "agctl.commands.mock_commands.new_mock_engine",
            return_value=fake_engine,
        ) as mock_factory:
            result = CliRunner().invoke(
                cli,
                ["--config", str(temp_config), "mock", "run", "--only", "http"],
            )

            assert result.exit_code == 2
            envelope = _single_envelope(result)
            error = envelope["error"]
            assert error["type"] == "ConfigError"
            assert "order-service" in error["message"]
            assert (
                error["detail"]["path"]
                == "mocks.http.stubs.stub1.effects[0].service"
            )
            mock_factory.assert_not_called()

    def test_reactor_http_effect_unknown_service_fails_before_engine(
        self, temp_config, fake_engine, monkeypatch
    ):
        """Same unknown-service guard for a kafka reactor's http effect, with
        the reactors-scoped dotted path."""
        config_content = """
version: "3"
kafka:
  clusters:
    main:
      brokers:
        - "main:9092"
  default_cluster: main
mocks:
  kafka:
    reactors:
      r1:
        topic: in-topic
        effects:
          - type: http
            service: nope-service
            path: /notify
"""
        temp_config.write_text(config_content)

        monkeypatch.setattr(
            "agctl.commands.mock_commands.new_kafka_client",
            lambda cluster, group_id=None, *, codec=None: MagicMock(),
        )

        with patch(
            "agctl.commands.mock_commands.new_mock_engine",
            return_value=fake_engine,
        ) as mock_factory:
            result = CliRunner().invoke(
                cli,
                ["--config", str(temp_config), "mock", "run"],
            )

            assert result.exit_code == 2
            envelope = _single_envelope(result)
            error = envelope["error"]
            assert error["type"] == "ConfigError"
            assert (
                error["detail"]["path"]
                == "mocks.kafka.reactors.r1.effects[0].service"
            )
            mock_factory.assert_not_called()

    def test_no_effects_passes_no_resolvers(self, temp_config, fake_engine, monkeypatch):
        """A config with no effects anywhere -> both resolvers are None."""
        config_content = """
version: "3"
services:
  order-service:
    base_url: http://orders.internal:8080
kafka:
  clusters:
    main:
      brokers:
        - "main:9092"
  default_cluster: main
mocks:
  http:
    listen: "0.0.0.0:18080"
    stubs:
      stub1:
        method: GET
        path: /test
        response:
          status: 200
          body: '{}'
  kafka:
    reactors:
      r1:
        topic: in-topic
        reaction:
          topic: out-topic
          value: {}
"""
        temp_config.write_text(config_content)

        monkeypatch.setattr(
            "agctl.commands.mock_commands.new_kafka_client",
            lambda cluster, group_id=None, *, codec=None: MagicMock(),
        )

        with patch(
            "agctl.commands.mock_commands.new_mock_engine",
            return_value=fake_engine,
        ) as mock_factory:
            result = CliRunner().invoke(
                cli,
                ["--config", str(temp_config), "mock", "run"],
                catch_exceptions=False,
            )

            assert result.exit_code == 0
            call_kwargs = mock_factory.call_args.kwargs
            assert call_kwargs["kafka_resolver"] is None
            assert call_kwargs["http_resolver"] is None

    def test_reactor_kafka_effect_extends_reactor_cluster_clients(
        self, temp_config, fake_engine, monkeypatch
    ):
        """A kafka effect (reactor-scoped) on a cluster NO reactor binds ->
        the effect's cluster gets its own client, and kafka_resolver is passed."""
        config_content = """
version: "3"
kafka:
  clusters:
    main:
      brokers:
        - "main:9092"
    effects:
      brokers:
        - "effects:9092"
  default_cluster: main
mocks:
  kafka:
    reactors:
      r1:
        topic: in-topic
        effects:
          - type: kafka
            topic: eff-topic
            cluster: effects
            value: {"x": 1}
"""
        temp_config.write_text(config_content)

        recorded = []

        def fake_factory(cluster, group_id=None, *, codec=None):
            recorded.append(tuple(cluster.brokers))
            return MagicMock()

        monkeypatch.setattr(
            "agctl.commands.mock_commands.new_kafka_client", fake_factory
        )

        with patch(
            "agctl.commands.mock_commands.new_mock_engine",
            return_value=fake_engine,
        ) as mock_factory:
            result = CliRunner().invoke(
                cli,
                ["--config", str(temp_config), "mock", "run", "--only", "kafka"],
                catch_exceptions=False,
            )

            assert result.exit_code == 0
            call_kwargs = mock_factory.call_args.kwargs
            kafka_resolver = call_kwargs["kafka_resolver"]
            assert callable(kafka_resolver)
            assert call_kwargs["http_resolver"] is None

            from agctl.config.models import KafkaEffect

            effect = KafkaEffect(
                type="kafka", topic="eff-topic", cluster="effects", value={"x": 1}
            )
            client, codec = kafka_resolver(effect)
            assert codec is None
            # Reactor cluster client (main) + effect cluster client (effects).
            assert ("main:9092",) in recorded
            assert ("effects:9092",) in recorded

    # -- http effect timeout fallback (resolve_timeout parity: ... or 10s) --

    def test_http_effect_url_mode_timeout_falls_back_to_10(
        self, temp_config, fake_engine
    ):
        """url-mode effect with no timeout anywhere (effect.timeout,
        defaults.timeout_seconds all unset — the common case) -> the client
        gets a 10s hard timeout, NOT httpx's timeout=None (timeouts off)."""
        config_content = """
version: "3"
mocks:
  http:
    listen: "0.0.0.0:18080"
    stubs:
      stub1:
        method: GET
        path: /test
        response:
          status: 200
          body: '{}'
        effects:
          - type: http
            url: http://legacy.internal:9999/legacy/path
            method: GET
"""
        temp_config.write_text(config_content)

        with patch(
            "agctl.commands.mock_commands.new_mock_engine",
            return_value=fake_engine,
        ) as mock_factory:
            result = CliRunner().invoke(
                cli,
                ["--config", str(temp_config), "mock", "run", "--only", "http"],
                catch_exceptions=False,
            )

            assert result.exit_code == 0
            http_resolver = mock_factory.call_args.kwargs["http_resolver"]

            import httpx

            from agctl.config.models import HttpEffect

            effect = HttpEffect(
                type="http", url="http://legacy.internal:9999/legacy/path"
            )
            client, _path = http_resolver(effect)
            assert client._client.timeout == httpx.Timeout(10)

    def test_http_effect_service_mode_timeout_falls_back_to_10(
        self, temp_config, fake_engine
    ):
        """service-mode effect with no timeout anywhere (service has no
        timeout_seconds, defaults.timeout_seconds unset) -> 10s."""
        config_content = """
version: "3"
services:
  order-service:
    base_url: http://orders.internal:8080
mocks:
  http:
    listen: "0.0.0.0:18080"
    stubs:
      stub1:
        method: GET
        path: /test
        response:
          status: 200
          body: '{}'
        effects:
          - type: http
            service: order-service
            path: /notify
"""
        temp_config.write_text(config_content)

        with patch(
            "agctl.commands.mock_commands.new_mock_engine",
            return_value=fake_engine,
        ) as mock_factory:
            result = CliRunner().invoke(
                cli,
                ["--config", str(temp_config), "mock", "run", "--only", "http"],
                catch_exceptions=False,
            )

            assert result.exit_code == 0
            http_resolver = mock_factory.call_args.kwargs["http_resolver"]

            import httpx

            from agctl.config.models import HttpEffect

            client, _path = http_resolver(
                HttpEffect(type="http", service="order-service", path="/notify")
            )
            assert client._client.timeout == httpx.Timeout(10)

    def test_http_effect_explicit_timeout_wins(self, temp_config, fake_engine):
        """effect.timeout beats both defaults.timeout_seconds and the 10s
        fallback (both modes; distinct base_urls so per-host caching doesn't
        mask the difference)."""
        config_content = """
version: "3"
defaults:
  timeout_seconds: 9
mocks:
  http:
    listen: "0.0.0.0:18080"
    stubs:
      stub1:
        method: GET
        path: /test
        response:
          status: 200
          body: '{}'
        effects:
          - type: http
            url: http://a.internal:1/x
"""
        temp_config.write_text(config_content)

        with patch(
            "agctl.commands.mock_commands.new_mock_engine",
            return_value=fake_engine,
        ) as mock_factory:
            result = CliRunner().invoke(
                cli,
                ["--config", str(temp_config), "mock", "run", "--only", "http"],
                catch_exceptions=False,
            )

            assert result.exit_code == 0
            http_resolver = mock_factory.call_args.kwargs["http_resolver"]

            import httpx

            from agctl.config.models import HttpEffect

            # url mode: explicit timeout beats defaults (9) and the 10s fallback
            client, _path = http_resolver(
                HttpEffect(type="http", url="http://a.internal:1/x", timeout=3.5)
            )
            assert client._client.timeout == httpx.Timeout(3.5)

    def test_http_effect_defaults_timeout_used_when_no_effect_timeout(
        self, temp_config, fake_engine
    ):
        """url mode, effect.timeout unset but defaults.timeout_seconds set ->
        defaults wins over the 10s fallback."""
        config_content = """
version: "3"
defaults:
  timeout_seconds: 9
mocks:
  http:
    listen: "0.0.0.0:18080"
    stubs:
      stub1:
        method: GET
        path: /test
        response:
          status: 200
          body: '{}'
        effects:
          - type: http
            url: http://legacy.internal:9999/legacy/path
"""
        temp_config.write_text(config_content)

        with patch(
            "agctl.commands.mock_commands.new_mock_engine",
            return_value=fake_engine,
        ) as mock_factory:
            result = CliRunner().invoke(
                cli,
                ["--config", str(temp_config), "mock", "run", "--only", "http"],
                catch_exceptions=False,
            )

            assert result.exit_code == 0
            http_resolver = mock_factory.call_args.kwargs["http_resolver"]

            import httpx

            from agctl.config.models import HttpEffect

            client, _path = http_resolver(
                HttpEffect(type="http", url="http://legacy.internal:9999/x")
            )
            assert client._client.timeout == httpx.Timeout(9)

    def test_http_effect_service_timeout_beats_defaults(
        self, temp_config, fake_engine
    ):
        """service.timeout_seconds (7) beats defaults.timeout_seconds (9);
        the 10s fallback only fires when both are unset."""
        config_content = """
version: "3"
defaults:
  timeout_seconds: 9
services:
  order-service:
    base_url: http://orders.internal:8080
    timeout_seconds: 7
mocks:
  http:
    listen: "0.0.0.0:18080"
    stubs:
      stub1:
        method: GET
        path: /test
        response:
          status: 200
          body: '{}'
        effects:
          - type: http
            service: order-service
            path: /notify
"""
        temp_config.write_text(config_content)

        with patch(
            "agctl.commands.mock_commands.new_mock_engine",
            return_value=fake_engine,
        ) as mock_factory:
            result = CliRunner().invoke(
                cli,
                ["--config", str(temp_config), "mock", "run", "--only", "http"],
                catch_exceptions=False,
            )

            assert result.exit_code == 0
            http_resolver = mock_factory.call_args.kwargs["http_resolver"]

            import httpx

            from agctl.config.models import HttpEffect

            client, _path = http_resolver(
                HttpEffect(type="http", service="order-service", path="/notify")
            )
            assert client._client.timeout == httpx.Timeout(7)


# Task 7: mock start daemon argv forwards --overlay
@pytest.mark.skipif(
    os.name == "nt",
    reason="managed daemon is POSIX-only; gated by _require_posix_daemon on Windows",
)
def test_mock_start_includes_overlay_in_daemon_argv(tmp_path, monkeypatch):
    """mock start includes --overlay in the daemon argv so the spawned daemon loads the overlay."""
    base = tmp_path / "agctl.yaml"
    base.write_text("""version: "3"
services:
  orders:
    base_url: http://localhost:8081
mocks:
  http:
    listen: "0.0.0.0:18080"
    stubs:
      stub1:
        method: GET
        path: /test
        response:
          status: 200
          body: '{}'
""")
    ov = tmp_path / "overlay.yaml"
    ov.write_text("""mocks:
  http:
    listen: "0.0.0.0:18080"
    stubs:
      stub2:
        method: POST
        path: /test2
        response:
          status: 201
          body: '{"created": true}'
""")

    # Track the daemon argv that would be passed to spawn_daemon
    captured_argv = []

    def fake_spawn_daemon(argv, log_path, env=None):
        captured_argv.append(argv)
        return 12345  # Fake PID

    monkeypatch.setattr("agctl.commands.mock_commands.spawn_daemon", fake_spawn_daemon)

    # Also patch is_alive to make the daemon appear running
    def fake_is_alive(pid):
        return True

    monkeypatch.setattr("agctl.commands.mock_commands.is_alive", fake_is_alive)

    # Patch parse_log to return a started event
    from unittest.mock import MagicMock
    fake_parsed = MagicMock()
    fake_parsed.started = {"http": {"listen": "0.0.0.0:18080", "stubs": 1}}
    fake_parsed.startup_error = None
    monkeypatch.setattr("agctl.commands.mock_commands.parse_log", lambda log_path: fake_parsed)

    # Also need to patch read_pidfile to return None (no existing daemon)
    monkeypatch.setattr("agctl.commands.mock_commands.read_pidfile", lambda pidfile: None)

    result = CliRunner().invoke(
        cli,
        ["--overlay", str(ov), "--config", str(base), "mock", "start"],
    )

    # Verify spawn_daemon was called
    assert len(captured_argv) == 1

    # Verify the daemon argv structure
    daemon_argv = captured_argv[0]

    # Global flags (--config, --overlay) must appear BEFORE the "mock" subcommand
    # because they are root-level Click options, not mock-run options
    mock_idx = daemon_argv.index("mock")

    # Verify --overlay is in the argv and appears BEFORE the "mock" subcommand
    assert "--overlay" in daemon_argv
    overlay_idx = daemon_argv.index("--overlay")
    assert overlay_idx < mock_idx, "--overlay must appear before the 'mock' subcommand"
    # Next item should be the absolute path to the overlay
    assert daemon_argv[overlay_idx + 1] == str(Path(ov).absolute())


# Regression: mock start daemon argv forwards --env-file (the daemon re-loads
# config from scratch, so it must receive the flag or it silently falls back to
# the default .env sibling — the parent's readiness load used the right file,
# the server that serves traffic would not).
@pytest.mark.skipif(
    os.name == "nt",
    reason="managed daemon is POSIX-only; gated by _require_posix_daemon on Windows",
)
def test_mock_start_includes_env_file_in_daemon_argv(tmp_path, monkeypatch):
    """mock start forwards --env-file to the daemon argv so the spawned daemon
    loads the same .env as the parent (otherwise it silently uses the default)."""
    base = tmp_path / "agctl.yaml"
    base.write_text("""version: "3"
services:
  orders:
    base_url: http://localhost:8081
mocks:
  http:
    listen: "0.0.0.0:18080"
    stubs:
      stub1:
        method: GET
        path: /test
        response:
          status: 200
          body: '{}'
""")
    envf = tmp_path / "secrets.env"
    envf.write_text("BASE=http://from-envfile\n")

    captured_argv = []

    def fake_spawn_daemon(argv, log_path, env=None):
        captured_argv.append(argv)
        return 12345  # Fake PID

    monkeypatch.setattr("agctl.commands.mock_commands.spawn_daemon", fake_spawn_daemon)
    monkeypatch.setattr("agctl.commands.mock_commands.is_alive", lambda pid: True)

    from unittest.mock import MagicMock

    fake_parsed = MagicMock()
    fake_parsed.started = {"http": {"listen": "0.0.0.0:18080", "stubs": 1}}
    fake_parsed.startup_error = None
    monkeypatch.setattr("agctl.commands.mock_commands.parse_log", lambda log_path: fake_parsed)
    monkeypatch.setattr("agctl.commands.mock_commands.read_pidfile", lambda pidfile: None)

    result = CliRunner().invoke(
        cli,
        ["--env-file", str(envf), "--config", str(base), "mock", "start"],
    )

    assert len(captured_argv) == 1, result.output
    daemon_argv = captured_argv[0]

    # Global flags (--env-file) must appear BEFORE the "mock" subcommand.
    mock_idx = daemon_argv.index("mock")
    assert "--env-file" in daemon_argv
    env_file_idx = daemon_argv.index("--env-file")
    assert env_file_idx < mock_idx, "--env-file must appear before the 'mock' subcommand"
    assert daemon_argv[env_file_idx + 1] == str(Path(envf).absolute())

    # Verify --config also appears BEFORE the "mock" subcommand (if provided)
    if "--config" in daemon_argv:
        config_idx = daemon_argv.index("--config")
        assert config_idx < mock_idx, "--config must appear before the 'mock' subcommand"

    # Fix 3: Re-invoke cli with the captured daemon_argv to verify it's parseable and loads config
    # We need to stub the engine to prevent mock run from hanging (it streams NDJSON)
    def fake_new_mock_engine(**kwargs):
        engine = MagicMock()
        engine.start = MagicMock()
        engine.run = MagicMock(return_value=0)
        engine.shutdown = MagicMock()
        return engine

    monkeypatch.setattr("agctl.commands.mock_commands.new_mock_engine", fake_new_mock_engine)

    # Re-invoke cli with the exact daemon_argv that was captured
    # This verifies the built argv is parseable AND loads config in the daemon child
    reinvoke_result = CliRunner().invoke(cli, daemon_argv)

    # Assert the re-invoke succeeds (exit_code == 0)
    assert reinvoke_result.exit_code == 0, f"Re-invoke failed with exit code {reinvoke_result.exit_code}, output: {reinvoke_result.output}"


class TestDaemonPlatformGate:
    """The managed mock daemon (mock start/stop/status) is gated to POSIX.

    On ``os.name == "nt"`` the three daemon ``_core`` entry points raise
    ``ConfigError`` (exit 2) before touching any pidfile/process. The platform is
    forced by replacing the module-level ``os`` binding in ``agctl.daemon`` (the
    home of ``require_posix_daemon`` since the D8 primitive extraction), so the
    seam never mutates the global ``os`` module and the suite is deterministic on
    the (posix) dev host.
    """

    # Verbatim from the plan's Global Constraints.
    _EXPECTED_MESSAGE = (
        "the managed mock daemon (mock start/stop/status) is supported on "
        "Linux, macOS, and WSL; on native Windows use 'agctl mock run' or "
        "run inside WSL"
    )
    _EXPECTED_HINT = (
        "use 'agctl mock run' (foreground) or run agctl inside WSL for the "
        "managed daemon"
    )

    def test_require_posix_daemon_raises_on_windows(self, monkeypatch):
        monkeypatch.setattr(
            "agctl.daemon.os",
            types.SimpleNamespace(name="nt"),
        )
        with pytest.raises(ConfigError) as exc_info:
            _require_posix_daemon()
        assert exc_info.value.message == self._EXPECTED_MESSAGE
        assert exc_info.value.detail["hint"] == self._EXPECTED_HINT
        assert exc_info.value.detail["platform"] == sys.platform

    def test_require_posix_daemon_noop_on_posix(self, monkeypatch):
        monkeypatch.setattr(
            "agctl.daemon.os",
            types.SimpleNamespace(name="posix"),
        )
        assert _require_posix_daemon() is None

    def test_mock_start_core_gated_on_windows(self, monkeypatch):
        monkeypatch.setattr(
            "agctl.daemon.os",
            types.SimpleNamespace(name="nt"),
        )
        with pytest.raises(ConfigError) as exc_info:
            _mock_start_core(None, None, None, False, None, "./.agctl")
        assert exc_info.value.message == self._EXPECTED_MESSAGE

    def test_mock_stop_and_status_core_gated_on_windows(self, monkeypatch):
        monkeypatch.setattr(
            "agctl.daemon.os",
            types.SimpleNamespace(name="nt"),
        )
        with pytest.raises(ConfigError) as exc_info:
            _mock_stop_core(None, None, False, 10.0, "./.agctl")
        assert exc_info.value.message == self._EXPECTED_MESSAGE
        with pytest.raises(ConfigError) as exc_info:
            _mock_status_core(None, "./.agctl")
        assert exc_info.value.message == self._EXPECTED_MESSAGE


# ----------------------------------------------------------------------------
# Task 10: --grpc-listen / --only grpc / 3-tuple engines / mock start grpc block
# ----------------------------------------------------------------------------


# A minimal gRPC stub config (no descriptors needed for run_grpc=True wiring —
# the engine factory is monkeypatched in run tests; lifecycle tests fabricate
# the started line directly).
GRPC_ONLY_CONFIG = """
version: "3"
mocks:
  grpc:
    listen: "0.0.0.0:50051"
    stubs:
      stub1:
        service: helloworld.Greeter
        method: SayHello
        response:
          message: {"message": "hello"}
"""

HTTP_AND_GRPC_CONFIG = """
version: "3"
mocks:
  http:
    listen: "0.0.0.0:18080"
    stubs:
      stub1:
        method: GET
        path: /test
        response:
          status: 200
          body: '{}'
  grpc:
    listen: "0.0.0.0:50051"
    stubs:
      grpcstub1:
        service: helloworld.Greeter
        method: SayHello
        response:
          message: {"message": "hello"}
"""


class TestResolveEnginesGrpc:
    """`_resolve_engines` returns a 3-tuple (run_http, run_kafka, run_grpc)."""

    def test_only_grpc_with_stubs(self, temp_config):
        """--only grpc with mocks.grpc.stubs -> (False, False, True)."""
        from agctl.config.loader import load_config
        cfg_text = GRPC_ONLY_CONFIG
        temp_config.write_text(cfg_text)
        cfg = load_config(temp_config)
        run_http, run_kafka, run_grpc = _resolve_engines("grpc", cfg.mocks)
        assert (run_http, run_kafka, run_grpc) == (False, False, True)

    def test_only_grpc_no_mocks_grpc(self, temp_config):
        """--only grpc with no mocks.grpc -> ConfigError."""
        temp_config.write_text("""
version: "3"
mocks:
  http:
    listen: "0.0.0.0:18080"
    stubs:
      stub1:
        method: GET
        path: /test
        response:
          status: 200
          body: '{}'
""")
        from agctl.config.loader import load_config
        cfg = load_config(temp_config)
        with pytest.raises(ConfigError) as exc_info:
            _resolve_engines("grpc", cfg.mocks)
        assert "no mocks.grpc.stubs configured" in exc_info.value.message

    def test_only_grpc_empty_stubs(self, temp_config):
        """--only grpc with mocks.grpc.stubs={} (empty) -> ConfigError."""
        temp_config.write_text("""
version: "3"
mocks:
  grpc:
    listen: "0.0.0.0:50051"
    stubs: {}
""")
        from agctl.config.loader import load_config
        cfg = load_config(temp_config)
        with pytest.raises(ConfigError) as exc_info:
            _resolve_engines("grpc", cfg.mocks)
        assert "no mocks.grpc.stubs configured" in exc_info.value.message

    def test_default_resolves_grpc_with_http(self, temp_config):
        """No --only with mocks.http + mocks.grpc -> (True, False, True)."""
        temp_config.write_text(HTTP_AND_GRPC_CONFIG)
        from agctl.config.loader import load_config
        cfg = load_config(temp_config)
        result = _resolve_engines(None, cfg.mocks)
        assert result == (True, False, True)

    def test_default_resolves_grpc_only(self, temp_config):
        """No --only with only mocks.grpc -> (False, False, True)."""
        temp_config.write_text(GRPC_ONLY_CONFIG)
        from agctl.config.loader import load_config
        cfg = load_config(temp_config)
        result = _resolve_engines(None, cfg.mocks)
        assert result == (False, False, True)

    def test_only_http_returns_false_grpc(self, temp_config):
        """--only http -> (True, False, False) — existing branch gains trailing False."""
        temp_config.write_text(HTTP_AND_GRPC_CONFIG)
        from agctl.config.loader import load_config
        cfg = load_config(temp_config)
        result = _resolve_engines("http", cfg.mocks)
        assert result == (True, False, False)

    def test_only_kafka_returns_false_grpc(self, temp_config):
        """--only kafka -> (False, True, False) — existing branch gains trailing False."""
        temp_config.write_text("""
version: "3"
kafka:
  clusters:
    default:
      brokers:
        - "localhost:9092"
mocks:
  kafka:
    reactors:
      r1:
        topic: t
        reaction:
          topic: out
          value: {}
  grpc:
    listen: "0.0.0.0:50051"
    stubs:
      stub1:
        service: helloworld.Greeter
        method: SayHello
        response:
          message: {"message": "hello"}
""")
        from agctl.config.loader import load_config
        cfg = load_config(temp_config)
        result = _resolve_engines("kafka", cfg.mocks)
        assert result == (False, True, False)


class TestMockRunGrpcFlags:
    """`mock run` --grpc-listen / --only grpc wiring."""

    def test_only_grpc_builds_engine_with_run_grpc(self, temp_config, fake_engine):
        """mock run --only grpc with stubs -> new_mock_engine called with run_grpc=True."""
        temp_config.write_text(GRPC_ONLY_CONFIG)
        with patch(
            "agctl.commands.mock_commands.new_mock_engine",
            return_value=fake_engine,
        ) as mock_factory:
            result = CliRunner().invoke(
                cli,
                ["--config", str(temp_config), "mock", "run", "--only", "grpc"],
                catch_exceptions=False,
            )
            assert result.exit_code == 0
            mock_factory.assert_called_once()
            call_kwargs = mock_factory.call_args.kwargs
            assert call_kwargs["run_http"] is False
            assert call_kwargs["run_kafka"] is False
            assert call_kwargs["run_grpc"] is True

    def test_only_grpc_no_mocks_grpc_exits_2(self, temp_config):
        """--only grpc with no mocks.grpc -> exit 2 ConfigError envelope."""
        temp_config.write_text("""
version: "3"
mocks:
  http:
    listen: "0.0.0.0:18080"
    stubs:
      stub1:
        method: GET
        path: /test
        response:
          status: 200
          body: '{}'
""")
        result = CliRunner().invoke(
            cli,
            ["--config", str(temp_config), "mock", "run", "--only", "grpc"],
        )
        assert result.exit_code == 2
        output_lines = [line for line in result.output.split("\n") if line.strip()]
        assert len(output_lines) == 1
        envelope = json.loads(output_lines[0])
        assert envelope["ok"] is False
        assert envelope["command"] == "mock.run"
        assert envelope["error"]["type"] == "ConfigError"
        assert "no mocks.grpc.stubs configured" in envelope["error"]["message"]

    def test_grpc_listen_forwarded_to_engine(self, temp_config, fake_engine):
        """--grpc-listen 127.0.0.1:50051 is forwarded into the engine call."""
        temp_config.write_text(GRPC_ONLY_CONFIG)
        with patch(
            "agctl.commands.mock_commands.new_mock_engine",
            return_value=fake_engine,
        ) as mock_factory:
            result = CliRunner().invoke(
                cli,
                [
                    "--config", str(temp_config),
                    "mock", "run",
                    "--only", "grpc",
                    "--grpc-listen", "127.0.0.1:50051",
                ],
                catch_exceptions=False,
            )
            assert result.exit_code == 0
            call_kwargs = mock_factory.call_args.kwargs
            assert call_kwargs["grpc_listen"] == "127.0.0.1:50051"
            assert call_kwargs["run_grpc"] is True

    def test_grpc_listen_bad_value(self, temp_config):
        """--grpc-listen 'bad:no-port' -> exit 2 ConfigError (parse_listen fails)."""
        temp_config.write_text(GRPC_ONLY_CONFIG)
        result = CliRunner().invoke(
            cli,
            [
                "--config", str(temp_config),
                "mock", "run",
                "--only", "grpc",
                "--grpc-listen", "bad:no-port",
            ],
        )
        assert result.exit_code == 2
        output_lines = [line for line in result.output.split("\n") if line.strip()]
        assert len(output_lines) == 1
        envelope = json.loads(output_lines[0])
        assert envelope["ok"] is False
        assert envelope["error"]["type"] == "ConfigError"
        assert "Invalid --grpc-listen" in envelope["error"]["message"]

    def test_only_rejects_unknown_choice(self, temp_config):
        """--only bogus -> Click usage error (exit 2), not an agctl envelope."""
        temp_config.write_text(GRPC_ONLY_CONFIG)
        result = CliRunner().invoke(
            cli,
            ["--config", str(temp_config), "mock", "run", "--only", "bogus"],
        )
        assert result.exit_code == 2
        # Click's choice validation surfaces a usage error before our envelope
        # handler runs — verify it's a Click-reported invalid choice, not a
        # JSON envelope.
        assert result.output.lstrip().startswith("Usage:") or "Invalid value" in result.output

    def test_http_and_grpc_starts_both_engines(self, temp_config, fake_engine):
        """A config with HTTP+gRPC and no --only → both run_http and run_grpc True."""
        temp_config.write_text(HTTP_AND_GRPC_CONFIG)
        with patch(
            "agctl.commands.mock_commands.new_mock_engine",
            return_value=fake_engine,
        ) as mock_factory:
            result = CliRunner().invoke(
                cli,
                ["--config", str(temp_config), "mock", "run"],
                catch_exceptions=False,
            )
            assert result.exit_code == 0
            call_kwargs = mock_factory.call_args.kwargs
            assert call_kwargs["run_http"] is True
            assert call_kwargs["run_kafka"] is False
            assert call_kwargs["run_grpc"] is True

    def test_top_level_descriptors_threaded(self, temp_config, fake_engine):
        """cfg.grpc.descriptors (top-level) is forwarded as top_level_descriptors."""
        temp_config.write_text("""
version: "3"
grpc:
  descriptors:
    - proto: "helloworld.proto"
      include_paths: ["./protos"]
mocks:
  grpc:
    listen: "0.0.0.0:50051"
    stubs:
      stub1:
        service: helloworld.Greeter
        method: SayHello
        response:
          message: {"message": "hi"}
""")
        with patch(
            "agctl.commands.mock_commands.new_mock_engine",
            return_value=fake_engine,
        ) as mock_factory:
            result = CliRunner().invoke(
                cli,
                ["--config", str(temp_config), "mock", "run", "--only", "grpc"],
                catch_exceptions=False,
            )
            assert result.exit_code == 0
            call_kwargs = mock_factory.call_args.kwargs
            # Forwarded as the top-level descriptors list (length matches config).
            assert call_kwargs.get("top_level_descriptors") is not None
            assert len(call_kwargs["top_level_descriptors"]) == 1

    def test_top_level_descriptors_none_when_unset(self, temp_config, fake_engine):
        """top_level_descriptors defaults to None when cfg.grpc.descriptors is empty."""
        temp_config.write_text(GRPC_ONLY_CONFIG)
        with patch(
            "agctl.commands.mock_commands.new_mock_engine",
            return_value=fake_engine,
        ) as mock_factory:
            result = CliRunner().invoke(
                cli,
                ["--config", str(temp_config), "mock", "run", "--only", "grpc"],
                catch_exceptions=False,
            )
            assert result.exit_code == 0
            call_kwargs = mock_factory.call_args.kwargs
            assert call_kwargs.get("top_level_descriptors") is None


@pytest.mark.skipif(
    os.name == "nt",
    reason="managed daemon is POSIX-only; gated by _require_posix_daemon on Windows",
)
class TestMockStartGrpc:
    """`mock start` --grpc-listen / grpc result block / pidfile keying."""

    def _patch_start_seam(self, monkeypatch, started_line: dict) -> list:
        """Patch the spawn_daemon + log parse seams; return captured argv list."""
        captured_argv: list[list[str]] = []

        def fake_spawn_daemon(argv, log_path, env=None):
            captured_argv.append(argv)
            log_file = Path(log_path)
            log_file.parent.mkdir(parents=True, exist_ok=True)
            log_file.write_text(json.dumps(started_line) + "\n")
            return 12345

        monkeypatch.setattr(
            "agctl.commands.mock_commands.spawn_daemon", fake_spawn_daemon
        )
        monkeypatch.setattr(
            "agctl.commands.mock_commands.is_alive", lambda pid: True
        )
        fake_parsed = MagicMock()
        fake_parsed.started = started_line
        fake_parsed.startup_error = None
        monkeypatch.setattr(
            "agctl.commands.mock_commands.parse_log", lambda log_path: fake_parsed
        )
        monkeypatch.setattr(
            "agctl.commands.mock_commands.read_pidfile", lambda pidfile: None
        )
        return captured_argv

    def test_start_grpc_only_result_block(self, temp_config, monkeypatch, tmp_path):
        """mock start --only grpc → result envelope includes a `grpc` block."""
        temp_config.write_text(GRPC_ONLY_CONFIG)
        started_line = {
            "event": "started",
            "http": None,
            "kafka": None,
            "grpc": {
                "listen": "127.0.0.1:50051",
                "stubs": 1,
                "services": ["helloworld.Greeter"],
                "reflection": True,
                "health": True,
            },
        }
        captured_argv = self._patch_start_seam(monkeypatch, started_line)

        state_dir = tmp_path / "state"
        result = CliRunner().invoke(
            cli,
            [
                "--config", str(temp_config),
                "mock", "start",
                "--only", "grpc",
                "--grpc-listen", "127.0.0.1:50051",
                "--state-dir", str(state_dir),
            ],
        )
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["ok"] is True
        # HTTP keys remain (None / no stubs) when only grpc is running.
        assert payload["result"]["listen"] is None
        assert payload["result"]["stubs"] is None
        # gRPC block is present and matches the started line.
        grpc = payload["result"]["grpc"]
        assert grpc["listen"] == "127.0.0.1:50051"
        assert grpc["stubs"] == 1
        assert grpc["services"] == ["helloworld.Greeter"]
        assert grpc["reflection"] is True
        assert grpc["health"] is True

        # argv forwarded --only grpc and --grpc-listen.
        argv = captured_argv[0]
        assert "--only" in argv
        only_idx = argv.index("--only")
        assert argv[only_idx + 1] == "grpc"
        assert "--grpc-listen" in argv
        gl_idx = argv.index("--grpc-listen")
        assert argv[gl_idx + 1] == "127.0.0.1:50051"
        # HTTP listen should NOT appear in a grpc-only argv.
        assert "--http-listen" not in argv

    def test_start_grpc_only_pidfile_keying(self, temp_config, monkeypatch, tmp_path):
        """grpc-only daemon's pidfile + log are keyed mock-grpc-<port>.* (Task 9)."""
        temp_config.write_text(GRPC_ONLY_CONFIG)
        started_line = {
            "event": "started",
            "http": None,
            "kafka": None,
            "grpc": {
                "listen": "127.0.0.1:50051",
                "stubs": 1,
                "services": ["helloworld.Greeter"],
                "reflection": True,
                "health": True,
            },
        }
        self._patch_start_seam(monkeypatch, started_line)

        state_dir = tmp_path / "state"
        result = CliRunner().invoke(
            cli,
            [
                "--config", str(temp_config),
                "mock", "start",
                "--only", "grpc",
                "--grpc-listen", "127.0.0.1:50051",
                "--state-dir", str(state_dir),
            ],
        )
        assert result.exit_code == 0, result.output
        # Pidfile + log live under mock-grpc-50051.* (no collision with HTTP).
        assert (state_dir / "mock-grpc-50051.pid").exists()
        assert (state_dir / "mock-grpc-50051.log").exists()
        # Legacy HTTP-keyed names must NOT be present.
        assert not (state_dir / "mock-50051.pid").exists()

    def test_start_pidfile_persists_listen_fields(
        self, temp_config, monkeypatch, tmp_path
    ):
        """pidfile JSON persists http_listen + grpc_listen so stop/status can target either."""
        temp_config.write_text(HTTP_AND_GRPC_CONFIG)
        started_line = {
            "event": "started",
            "http": {"listen": "127.0.0.1:18080", "stubs": 1},
            "kafka": None,
            "grpc": {
                "listen": "127.0.0.1:50051",
                "stubs": 1,
                "services": ["helloworld.Greeter"],
                "reflection": True,
                "health": True,
            },
        }
        self._patch_start_seam(monkeypatch, started_line)

        state_dir = tmp_path / "state"
        result = CliRunner().invoke(
            cli,
            [
                "--config", str(temp_config),
                "mock", "start",
                "--http-listen", "127.0.0.1:18080",
                "--grpc-listen", "127.0.0.1:50051",
                "--state-dir", str(state_dir),
            ],
        )
        assert result.exit_code == 0, result.output
        pidfile = state_dir / "mock-18080.pid"
        assert pidfile.exists()
        data = json.loads(pidfile.read_text())
        assert data["http_listen"] == "127.0.0.1:18080"
        assert data["grpc_listen"] == "127.0.0.1:50051"

    def test_start_http_and_grpc_argv_carries_both(
        self, temp_config, monkeypatch, tmp_path
    ):
        """HTTP+gRPC daemon argv carries both --http-listen and --grpc-listen."""
        temp_config.write_text(HTTP_AND_GRPC_CONFIG)
        started_line = {
            "event": "started",
            "http": {"listen": "127.0.0.1:18080", "stubs": 1},
            "kafka": None,
            "grpc": {
                "listen": "127.0.0.1:50051",
                "stubs": 1,
                "services": ["helloworld.Greeter"],
                "reflection": True,
                "health": True,
            },
        }
        captured_argv = self._patch_start_seam(monkeypatch, started_line)

        state_dir = tmp_path / "state"
        result = CliRunner().invoke(
            cli,
            [
                "--config", str(temp_config),
                "mock", "start",
                "--http-listen", "127.0.0.1:18080",
                "--grpc-listen", "127.0.0.1:50051",
                "--state-dir", str(state_dir),
            ],
        )
        assert result.exit_code == 0, result.output
        argv = captured_argv[0]
        assert "--http-listen" in argv
        assert argv[argv.index("--http-listen") + 1] == "127.0.0.1:18080"
        assert "--grpc-listen" in argv
        assert argv[argv.index("--grpc-listen") + 1] == "127.0.0.1:50051"

    def test_start_http_only_no_grpc_block(self, temp_config, monkeypatch, tmp_path):
        """HTTP-only start: result has no `grpc` key (omit when not run_grpc)."""
        temp_config.write_text("""
version: "3"
mocks:
  http:
    listen: "127.0.0.1:18080"
    stubs:
      stub1:
        method: GET
        path: /test
        response:
          status: 200
          body: '{}'
""")
        started_line = {
            "event": "started",
            "http": {"listen": "127.0.0.1:18080", "stubs": 1},
            "kafka": None,
            "grpc": None,
        }
        self._patch_start_seam(monkeypatch, started_line)
        state_dir = tmp_path / "state"
        result = CliRunner().invoke(
            cli,
            [
                "--config", str(temp_config),
                "mock", "start",
                "--only", "http",
                "--state-dir", str(state_dir),
            ],
        )
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert "grpc" not in payload["result"]
        assert payload["result"]["listen"] == "127.0.0.1:18080"

    def test_start_grpc_only_startup_error_detail_listen_is_grpc(
        self, temp_config, monkeypatch, tmp_path
    ):
        """(Fix 5) A grpc-only startup error's ``detail.listen`` points at the
        gRPC address, NOT the HTTP default (0.0.0.0:18080).

        Regression: the previous unconditional ``detail["listen"] = http_listen``
        set a grpc-only startup error's structured ``detail.listen`` to the HTTP
        default address even though the message text named the gRPC port.
        """
        temp_config.write_text(GRPC_ONLY_CONFIG)

        # Patch the spawn + readiness seams; reach the startup_error branch.
        monkeypatch.setattr(
            "agctl.commands.mock_commands.spawn_daemon",
            lambda argv, log_path, env=None: 12345,
        )
        monkeypatch.setattr(
            "agctl.commands.mock_commands.is_alive", lambda pid: True
        )
        # _terminate/remove_pidfile run in the startup_error cleanup path; stub
        # them so the test never signals a real pid or touches the filesystem.
        monkeypatch.setattr(
            "agctl.commands.mock_commands._terminate",
            lambda pid, grace: "SIGTERM",
        )
        monkeypatch.setattr(
            "agctl.commands.mock_commands.remove_pidfile", lambda pidfile: None
        )
        fake_parsed = MagicMock()
        fake_parsed.started = None
        fake_parsed.startup_error = {
            "error": {
                "message": "grpc listen address 127.0.0.1:50051 already in use",
                "detail": {"listen": "127.0.0.1:50051"},
            }
        }
        monkeypatch.setattr(
            "agctl.commands.mock_commands.parse_log", lambda log_path: fake_parsed
        )
        monkeypatch.setattr(
            "agctl.commands.mock_commands.read_pidfile", lambda pidfile: None
        )

        state_dir = tmp_path / "state"
        result = CliRunner().invoke(
            cli,
            [
                "--config", str(temp_config),
                "mock", "start",
                "--only", "grpc",
                "--grpc-listen", "127.0.0.1:50051",
                "--state-dir", str(state_dir),
            ],
        )
        # Startup error -> exit 2 (ConfigError envelope).
        assert result.exit_code == 2, result.output
        payload = json.loads(result.output)
        assert payload["ok"] is False
        # detail.listen MUST be the gRPC address (not the HTTP default).
        assert payload["error"]["detail"]["listen"] == "127.0.0.1:50051"
        assert payload["error"]["detail"]["listen"] != "0.0.0.0:18080"
