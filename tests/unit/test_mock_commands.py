"""Tests for agctl mock run command (Task 8)."""

import json
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest
from click.testing import CliRunner

from agctl.cli import cli
from agctl.commands.mock_commands import mock_run, new_mock_engine


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
        """--only kafka with reactors + kafka.brokers -> run_kafka=True, kafka_client is KafkaClient."""
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
  brokers:
    - "localhost:9092"
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
        """mocks.kafka reactors whose resolved cluster has empty brokers -> exit 2.

        The runtime brokers guard was dropped in Task 3 (per-reactor resolution
        replaced it); empty-brokers validation now lives in the validator's
        per-reactor check. At ``mock run`` time the empty broker list surfaces as
        a probe failure (ConnectionError when confluent_kafka is installed, or a
        ConfigError for the missing extra when it is not). Either way the run
        fails fast with exit 2 and a single error envelope — the contract this
        test pins.
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


# Task 7: mock start daemon argv forwards --overlay
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
