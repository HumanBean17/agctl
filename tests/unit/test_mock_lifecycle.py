"""Unit tests for `mock start` lifecycle command (Task 4)."""

import json
import os
from pathlib import Path

import pytest
from click.testing import CliRunner

from agctl.cli import cli

# Test fixture config (minimal, version 2.0)
MINIMAL_CONFIG = """version: "2.0"
mocks:
  http:
    listen: "127.0.0.1:18080"
    stubs:
      test-stub:
        method: GET
        path: /test
        response:
          status: 200
          body: '{"ok": true}'
"""

# Config without http section
NO_HTTP_CONFIG = """version: "2.0"
mocks:
  kafka:
    reactors:
      test-reactor:
        topic: test-topic
        consumer_group: test-group
        reaction:
          topic: test-topic
          value: "test"
"""


def _run(args, env=None):
    """Helper to run CLI.

    Returns (result, payload). Config file should be created beforehand by caller.
    """
    if env is None:
        env = {}
    result = CliRunner().invoke(cli, args, env=env)

    # Parse output if present and is JSON
    payload = None
    if result.output and result.output.strip():
        try:
            payload = json.loads(result.output)
        except json.JSONDecodeError:
            pass
    return result, payload


def _create_config(config_content=MINIMAL_CONFIG):
    """Create a temporary config file and return its path.

    Caller is responsible for cleanup with os.unlink().
    """
    import tempfile

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(config_content)
        return f.name


def test_mock_start_success_http():
    """Scenario 1: start success (HTTP) - daemon starts, readiness gate passes."""
    import tempfile
    from unittest.mock import patch

    cfg_path = _create_config()

    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)

            # Fake spawn_daemon that writes a started line and returns fake pid
            fake_spawn_calls = []

            def fake_spawn_daemon(argv, log_path, env=None):
                fake_spawn_calls.append({"argv": argv, "log_path": log_path, "env": env})
                # Write the started line to the log
                log_file = Path(log_path)
                log_file.parent.mkdir(parents=True, exist_ok=True)
                log_file.write_text('{"event":"started","http":{"listen":"127.0.0.1:18080","stubs":1},"kafka":null}\n')
                return 12345

            with patch("agctl.commands.mock_commands.spawn_daemon", side_effect=fake_spawn_daemon):
                result, payload = _run(
                    ["--config", cfg_path, "mock", "start", "--only", "http", "--http-listen", "127.0.0.1:18080", "--state-dir", str(tmp_path)],
                )

            # Assertions from brief
            assert result.exit_code == 0, f"stdout: {result.output}"
            assert payload is not None
            assert payload["ok"] is True
            assert payload["command"] == "mock.start"
            assert payload["result"]["pid"] == 12345
            assert payload["result"]["listen"] == "127.0.0.1:18080"
            assert payload["result"]["stubs"] == 1
            assert payload["result"]["log_path"].endswith("mock-18080.log")

            # Check pidfile exists
            pidfile_path = tmp_path / "mock-18080.pid"
            assert pidfile_path.exists()
            pidfile_data = json.loads(pidfile_path.read_text())
            assert pidfile_data["pid"] == 12345
    finally:
        # Cleanup config file
        try:
            os.unlink(cfg_path)
        except:
            pass


def test_mock_start_already_running():
    """Scenario 2: start already-running - pre-write pidfile with live pid, expect ConfigError."""
    import tempfile

    cfg_path = _create_config()

    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)

            # Pre-write pidfile with current process (live)
            pidfile_path = tmp_path / "mock-18080.pid"
            pidfile_path.write_text(json.dumps({
                "pid": os.getpid(),
                "listen": "127.0.0.1:18080",
                "port": 18080,
                "log_path": str(tmp_path / "mock-18080.log"),
                "config_path": None,
                "started_at": "2024-01-01T00:00:00Z",
                "run_id": "test-run"
            }))

            result, payload = _run(
                ["--config", cfg_path, "mock", "start", "--only", "http", "--http-listen", "127.0.0.1:18080", "--state-dir", str(tmp_path)],
            )

            assert result.exit_code == 2
            assert payload["ok"] is False
            assert payload["error"]["type"] == "ConfigError"
            assert "already running" in payload["error"]["message"].lower()
    finally:
        # Cleanup config file
        try:
            os.unlink(cfg_path)
        except:
            pass


def test_mock_start_startup_error():
    """Scenario 3: start startup-error - daemon writes error envelope, cleanup happens."""
    import tempfile
    from unittest.mock import patch

    cfg_path = _create_config()

    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)

            # Fake spawn that writes a startup error
            def fake_spawn_daemon(argv, log_path, env=None):
                log_file = Path(log_path)
                log_file.parent.mkdir(parents=True, exist_ok=True)
                # Write error envelope (no started line)
                log_file.write_text('{"ok":false,"command":"mock.run","error":{"type":"ConfigError","message":"bind failed","detail":{}}}\n')
                return 12345

            with patch("agctl.commands.mock_commands.spawn_daemon", side_effect=fake_spawn_daemon):
                result, payload = _run(
                    ["--config", cfg_path, "mock", "start", "--only", "http", "--http-listen", "127.0.0.1:18080", "--state-dir", str(tmp_path)],
                )

            assert result.exit_code == 2
            assert payload["ok"] is False
            assert payload["error"]["type"] == "ConfigError"
            assert "bind failed" in payload["error"]["message"]

            # Verify pidfile was removed (cleanup)
            pidfile_path = tmp_path / "mock-18080.pid"
            assert not pidfile_path.exists()
    finally:
        # Cleanup config file
        try:
            os.unlink(cfg_path)
        except:
            pass


def test_mock_start_readiness_timeout():
    """Scenario 4: start readiness timeout - daemon never writes started, timeout + cleanup."""
    import tempfile
    from unittest.mock import patch

    cfg_path = _create_config()

    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)

            # Fake spawn that returns but writes nothing
            def fake_spawn_daemon(argv, log_path, env=None):
                log_file = Path(log_path)
                log_file.parent.mkdir(parents=True, exist_ok=True)
                # Write nothing - daemon never becomes ready
                return 12345

            # Patch budget to 0.2s for fast test
            with patch("agctl.commands.mock_commands._START_BUDGET_SECONDS", 0.2):
                with patch("agctl.commands.mock_commands.spawn_daemon", side_effect=fake_spawn_daemon):
                    result, payload = _run(
                        ["--config", cfg_path, "mock", "start", "--only", "http", "--http-listen", "127.0.0.1:18080", "--state-dir", str(tmp_path)],
                    )

            assert result.exit_code == 2
            assert payload["ok"] is False
            assert payload["error"]["type"] == "ConfigError"
            assert "did not become ready" in payload["error"]["message"]

            # Verify pidfile was removed (cleanup)
            pidfile_path = tmp_path / "mock-18080.pid"
            assert not pidfile_path.exists()
    finally:
        # Cleanup config file
        try:
            os.unlink(cfg_path)
        except:
            pass


def test_mock_start_only_http_without_mocks_http():
    """Scenario 5: start --only http without mocks.http - ConfigError from _resolve_engines."""
    cfg_path = _create_config(NO_HTTP_CONFIG)

    try:
        result, payload = _run(
            ["--config", cfg_path, "mock", "start", "--only", "http"],
        )

        assert result.exit_code == 2
        assert payload["ok"] is False
        assert payload["error"]["type"] == "ConfigError"
        assert "no mocks.http" in payload["error"]["message"].lower()
    finally:
        # Cleanup config file
        try:
            os.unlink(cfg_path)
        except:
            pass


def test_mock_start_forwards_flags():
    """Scenario 6: start forwards flags - verify argv passed to spawn_daemon."""
    import tempfile
    from unittest.mock import patch

    cfg_path = _create_config()

    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)

            # Record argv received
            recorded_argv = []

            def fake_spawn_daemon(argv, log_path, env=None):
                recorded_argv.append(argv)
                log_file = Path(log_path)
                log_file.parent.mkdir(parents=True, exist_ok=True)
                log_file.write_text('{"event":"started","http":{"listen":"127.0.0.1:18080","stubs":1},"kafka":null}\n')
                return 12345

            with patch("agctl.commands.mock_commands.spawn_daemon", side_effect=fake_spawn_daemon):
                result, payload = _run(
                    ["--config", cfg_path, "mock", "start", "--only", "http", "--http-listen", "127.0.0.1:18080", "--fail-fast", "--duration", "5", "--state-dir", str(tmp_path)],
                )

            assert result.exit_code == 0
            assert len(recorded_argv) == 1
            argv = recorded_argv[0]

            # Verify all expected flags are present
            assert "mock" in argv
            assert "run" in argv
            assert "--only" in argv
            assert "http" in argv
            assert "--http-listen" in argv
            assert "127.0.0.1:18080" in argv
            assert "--fail-fast" in argv
            assert "--duration" in argv
            # Duration may be "5" or "5.0" - just check it's present
            duration_idx = argv.index("--duration")
            assert argv[duration_idx + 1] in ("5", "5.0")
    finally:
        # Cleanup config file
        try:
            os.unlink(cfg_path)
        except:
            pass
