"""Unit tests for `mock start` lifecycle command (Task 4)."""

import json
import os
import sys
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


# Pytest fixture to track and cleanup sleeper subprocesses
@pytest.fixture
def sleeper_pids():
    """Track sleeper subprocess PIDs and ensure cleanup on test exit."""
    pids = []

    yield pids  # Test code can append to this list

    # Cleanup: terminate all tracked sleepers
    for pid in pids:
        try:
            os.kill(pid, 15)  # SIGTERM
        except (ProcessLookupError, OSError):
            pass  # Already dead
        try:
            os.waitpid(pid, os.WNOHANG)
        except (ChildProcessError, OSError):
            pass  # Not our child or already reaped


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


# ----------------------------------------------------------------------------
# Task 5: `mock stop` tests (7 scenarios)
# ----------------------------------------------------------------------------

import subprocess
import signal


def test_mock_stop_clean(sleeper_pids):
    """Scenario 1: stop clean - daemon exits cleanly, summary parsed, no fatal failures."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)

        # Spawn a real sleeper process that handles signals gracefully
        sleeper_code = """
import signal
import sys
import time

# Custom signal handler that sets a flag
should_exit = False
def handler(signum, frame):
    global should_exit
    should_exit = True

signal.signal(signal.SIGTERM, handler)

# Sleep in very short increments to allow signal handling
end_time = time.time() + 30
while time.time() < end_time and not should_exit:
    time.sleep(0.001)
"""
        sleeper = subprocess.Popen([sys.executable, "-c", sleeper_code])
        sleeper_pids.append(sleeper.pid)

        # Pre-write the log with started + clean summary
        log_path = tmp_path / "mock-18080.log"
        log_path.write_text('{"event":"started","http":{"listen":"127.0.0.1:18080","stubs":1},"kafka":null}\n')
        with open(log_path, 'a') as f:
            f.write('{"event":"summary","http_hits":10,"http_unmatched":0,"kafka_reactions":0,"kafka_errors":0}\n')

        # Write pidfile
        pidfile_path = tmp_path / "mock-18080.pid"
        pidfile_path.write_text(json.dumps({
            "pid": sleeper.pid,
            "listen": "127.0.0.1:18080",
            "port": 18080,
            "log_path": str(log_path),
            "config_path": None,
            "started_at": "2024-01-01T00:00:00Z",
            "run_id": "test-run"
        }))

        # Invoke stop (no selector - exactly one running)
        result, payload = _run(["mock", "stop", "--timeout", "60", "--state-dir", str(tmp_path)])

        # Assert exit_code == 0, envelope ok:true
        assert result.exit_code == 0, f"stdout: {result.output}"
        assert payload is not None
        assert payload["ok"] is True
        assert payload["command"] == "mock.stop"
        assert payload["result"]["stopped"] is True
        assert payload["result"]["signal"] == "SIGTERM"
        assert payload["result"]["summary"]["http_hits"] == 10
        assert payload["result"]["failures"] == []

        # Verify sleeper process has terminated
        sleeper.wait(timeout=2)
        assert sleeper.poll() is not None

        # Verify pidfile was removed
        assert not pidfile_path.exists()


def test_mock_stop_with_fatal_failures(sleeper_pids):
    """Scenario 2: stop with fatal failures - log has http.unmatched + kafka.error, exit 1."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)

        # Spawn a real sleeper process that handles signals gracefully
        sleeper_code = """
import signal
import sys
import time

# Custom signal handler that sets a flag
should_exit = False
def handler(signum, frame):
    global should_exit
    should_exit = True

signal.signal(signal.SIGTERM, handler)

# Sleep in very short increments to allow signal handling
end_time = time.time() + 30
while time.time() < end_time and not should_exit:
    time.sleep(0.001)
"""
        sleeper = subprocess.Popen([sys.executable, "-c", sleeper_code])
        sleeper_pids.append(sleeper.pid)

        # Pre-write the log with started + summary + fatal failures
        log_path = tmp_path / "mock-18080.log"
        log_path.write_text('{"event":"started","http":{"listen":"127.0.0.1:18080","stubs":1},"kafka":null}\n')
        with open(log_path, 'a') as f:
            f.write('{"event":"http.unmatched","method":"GET","path":"/not-found","ts":"2024-01-01T00:00:00Z"}\n')
            f.write('{"event":"kafka.error","reactor":"test-reactor","message":"delivery failed","ts":"2024-01-01T00:00:00Z"}\n')
            f.write('{"event":"summary","http_hits":0,"http_unmatched":1,"kafka_reactions":0,"kafka_errors":1}\n')

        # Write pidfile
        pidfile_path = tmp_path / "mock-18080.pid"
        pidfile_path.write_text(json.dumps({
            "pid": sleeper.pid,
            "listen": "127.0.0.1:18080",
            "port": 18080,
            "log_path": str(log_path),
            "config_path": None,
            "started_at": "2024-01-01T00:00:00Z",
            "run_id": "test-run"
        }))

        # Invoke stop
        result, payload = _run(["mock", "stop", "--timeout", "60", "--state-dir", str(tmp_path)])

        # Assert exit_code == 1, envelope ok:false, error.type == AssertionError
        assert result.exit_code == 1, f"stdout: {result.output}"
        assert payload is not None
        assert payload["ok"] is False
        assert payload["result"] is None
        assert payload["error"]["type"] == "AssertionError"
        assert "fatal failure" in payload["error"]["message"].lower()
        assert payload["error"]["detail"]["stopped"] is True
        assert len(payload["error"]["detail"]["failures"]) == 2
        assert payload["error"]["detail"]["summary"]["http_unmatched"] == 1

        # Verify sleeper process has terminated
        sleeper.wait(timeout=2)
        assert sleeper.poll() is not None


def test_mock_stop_capture_missing_non_fatal(sleeper_pids):
    """Scenario 3: stop with capture.missing - non-fatal, exit 0, failure in list for visibility."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)

        # Spawn a real sleeper process that handles signals gracefully
        sleeper_code = """
import signal
import sys
import time

# Custom signal handler that sets a flag
should_exit = False
def handler(signum, frame):
    global should_exit
    should_exit = True

signal.signal(signal.SIGTERM, handler)

# Sleep in very short increments to allow signal handling
end_time = time.time() + 30
while time.time() < end_time and not should_exit:
    time.sleep(0.001)
"""
        sleeper = subprocess.Popen([sys.executable, "-c", sleeper_code])
        sleeper_pids.append(sleeper.pid)

        # Pre-write the log with started + summary + capture.missing (non-fatal)
        log_path = tmp_path / "mock-18080.log"
        log_path.write_text('{"event":"started","http":{"listen":"127.0.0.1:18080","stubs":1},"kafka":null}\n')
        with open(log_path, 'a') as f:
            f.write('{"event":"capture.missing","expectation_id":"test-exp","ts":"2024-01-01T00:00:00Z"}\n')
            f.write('{"event":"summary","http_hits":10,"http_unmatched":0,"kafka_reactions":0,"kafka_errors":0}\n')

        # Write pidfile
        pidfile_path = tmp_path / "mock-18080.pid"
        pidfile_path.write_text(json.dumps({
            "pid": sleeper.pid,
            "listen": "127.0.0.1:18080",
            "port": 18080,
            "log_path": str(log_path),
            "config_path": None,
            "started_at": "2024-01-01T00:00:00Z",
            "run_id": "test-run"
        }))

        # Invoke stop
        result, payload = _run(["mock", "stop", "--timeout", "60", "--state-dir", str(tmp_path)])

        # Assert exit_code == 0, envelope ok:true, failure in list
        assert result.exit_code == 0, f"stdout: {result.output}"
        assert payload is not None
        assert payload["ok"] is True
        assert payload["result"]["stopped"] is True
        assert payload["result"]["signal"] == "SIGTERM"
        assert len(payload["result"]["failures"]) == 1
        assert payload["result"]["failures"][0]["event"] == "capture.missing"

        # Verify sleeper process has terminated
        sleeper.wait(timeout=2)
        assert sleeper.poll() is not None
        assert not pidfile_path.exists()


def test_mock_stop_not_running():
    """Scenario 4: stop not-running - empty state dir, no selector, return stopped:false."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)

        # Empty state dir (no pidfiles)
        result, payload = _run(["mock", "stop", "--state-dir", str(tmp_path)])

        # Assert exit_code == 0, stopped:false
        assert result.exit_code == 0, f"stdout: {result.output}"
        assert payload is not None
        assert payload["ok"] is True
        assert payload["result"]["stopped"] is False


def test_mock_stop_ambiguous(sleeper_pids):
    """Scenario 5: stop ambiguous - two running mocks, no selector, ConfigError."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)

        # Spawn two sleepers that handle signals gracefully
        sleeper_code = """
import signal
import sys
import time

# Custom signal handler that sets a flag
should_exit = False
def handler(signum, frame):
    global should_exit
    should_exit = True

signal.signal(signal.SIGTERM, handler)

# Sleep in very short increments to allow signal handling
end_time = time.time() + 30
while time.time() < end_time and not should_exit:
    time.sleep(0.001)
"""
        sleeper1 = subprocess.Popen([sys.executable, "-c", sleeper_code])
        sleeper2 = subprocess.Popen([sys.executable, "-c", sleeper_code])
        sleeper_pids.extend([sleeper1.pid, sleeper2.pid])

        # Write two pidfiles with clean logs
        log_path1 = tmp_path / "mock-18080.log"
        log_path1.write_text('{"event":"started","http":{"listen":"127.0.0.1:18080","stubs":1},"kafka":null}\n')
        with open(log_path1, 'a') as f:
            f.write('{"event":"summary","http_hits":10,"http_unmatched":0,"kafka_reactions":0,"kafka_errors":0}\n')

        pidfile1 = tmp_path / "mock-18080.pid"
        pidfile1.write_text(json.dumps({
            "pid": sleeper1.pid,
            "listen": "127.0.0.1:18080",
            "port": 18080,
            "log_path": str(log_path1),
            "config_path": None,
            "started_at": "2024-01-01T00:00:00Z",
            "run_id": "test-run-1"
        }))

        log_path2 = tmp_path / "mock-18081.log"
        log_path2.write_text('{"event":"started","http":{"listen":"127.0.0.1:18081","stubs":1},"kafka":null}\n')
        with open(log_path2, 'a') as f:
            f.write('{"event":"summary","http_hits":10,"http_unmatched":0,"kafka_reactions":0,"kafka_errors":0}\n')

        pidfile2 = tmp_path / "mock-18081.pid"
        pidfile2.write_text(json.dumps({
            "pid": sleeper2.pid,
            "listen": "127.0.0.1:18081",
            "port": 18081,
            "log_path": str(log_path2),
            "config_path": None,
            "started_at": "2024-01-01T00:00:00Z",
            "run_id": "test-run-2"
        }))

        # Invoke stop with no selector (ambiguous)
        result, payload = _run(["mock", "stop", "--state-dir", str(tmp_path)])

        # Assert exit_code == 2, ConfigError, mentions multiple
        assert result.exit_code == 2, f"stdout: {result.output}"
        assert payload is not None
        assert payload["ok"] is False
        assert payload["error"]["type"] == "ConfigError"
        assert "multiple" in payload["error"]["message"].lower()
        assert set(payload["error"]["detail"]["candidates"]) == {"127.0.0.1:18080", "127.0.0.1:18081"}

        # Cleanup sleepers
        sleeper1.terminate()
        sleeper2.terminate()
        sleeper1.wait(timeout=2)
        sleeper2.wait(timeout=2)


def test_mock_stop_all(sleeper_pids):
    """Scenario 6: stop --all - two running mocks, both stopped, list of 2 entries."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)

        # Spawn two sleepers that handle signals gracefully
        sleeper_code = """
import signal
import sys
import time

# Custom signal handler that sets a flag
should_exit = False
def handler(signum, frame):
    global should_exit
    should_exit = True

signal.signal(signal.SIGTERM, handler)

# Sleep in very short increments to allow signal handling
end_time = time.time() + 30
while time.time() < end_time and not should_exit:
    time.sleep(0.001)
"""
        sleeper1 = subprocess.Popen([sys.executable, "-c", sleeper_code])
        sleeper2 = subprocess.Popen([sys.executable, "-c", sleeper_code])
        sleeper_pids.extend([sleeper1.pid, sleeper2.pid])

        # Write two pidfiles with clean logs
        log_path1 = tmp_path / "mock-18080.log"
        log_path1.write_text('{"event":"started","http":{"listen":"127.0.0.1:18080","stubs":1},"kafka":null}\n')
        with open(log_path1, 'a') as f:
            f.write('{"event":"summary","http_hits":10,"http_unmatched":0,"kafka_reactions":0,"kafka_errors":0}\n')

        pidfile1 = tmp_path / "mock-18080.pid"
        pidfile1.write_text(json.dumps({
            "pid": sleeper1.pid,
            "listen": "127.0.0.1:18080",
            "port": 18080,
            "log_path": str(log_path1),
            "config_path": None,
            "started_at": "2024-01-01T00:00:00Z",
            "run_id": "test-run-1"
        }))

        log_path2 = tmp_path / "mock-18081.log"
        log_path2.write_text('{"event":"started","http":{"listen":"127.0.0.1:18081","stubs":1},"kafka":null}\n')
        with open(log_path2, 'a') as f:
            f.write('{"event":"summary","http_hits":10,"http_unmatched":0,"kafka_reactions":0,"kafka_errors":0}\n')

        pidfile2 = tmp_path / "mock-18081.pid"
        pidfile2.write_text(json.dumps({
            "pid": sleeper2.pid,
            "listen": "127.0.0.1:18081",
            "port": 18081,
            "log_path": str(log_path2),
            "config_path": None,
            "started_at": "2024-01-01T00:00:00Z",
            "run_id": "test-run-2"
        }))

        # Invoke stop with --all
        result, payload = _run(["mock", "stop", "--all", "--state-dir", str(tmp_path)])

        # Assert exit_code == 0, stopped is list of 2
        assert result.exit_code == 0, f"stdout: {result.output}"
        assert payload is not None
        assert payload["ok"] is True
        assert isinstance(payload["result"]["stopped"], list)
        assert len(payload["result"]["stopped"]) == 2
        assert all(entry["stopped"] is True for entry in payload["result"]["stopped"])
        assert all(entry["signal"] == "SIGTERM" for entry in payload["result"]["stopped"])

        # Verify both sleepers terminated
        sleeper1.wait(timeout=2)
        sleeper2.wait(timeout=2)
        assert sleeper1.poll() is not None
        assert sleeper2.poll() is not None

        # Verify both pidfiles removed
        assert not pidfile1.exists()
        assert not pidfile2.exists()


def test_mock_stop_sigkill_fallback(sleeper_pids):
    """Scenario 7: stop SIGKILL fallback - daemon ignores SIGTERM, timeout → SIGKILL, warning set."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)

        # Spawn a sleeper that ignores SIGTERM
        sleeper_code = """
import signal
import time
import sys

# Set up signal handler BEFORE any other work
signal.signal(signal.SIGTERM, signal.SIG_IGN)

# Write to stdout to signal readiness
sys.stdout.write('READY\\n')
sys.stdout.flush()

# Then sleep
time.sleep(30)
"""
        sleeper = subprocess.Popen([sys.executable, "-c", sleeper_code], stdout=subprocess.PIPE)
        sleeper_pids.append(sleeper.pid)

        # Wait for sleeper to be ready
        line = sleeper.stdout.readline()
        assert line == b'READY\n', f"Sleeper not ready: {line!r}"

        # Pre-write the log with only started (no summary - daemon never shuts down cleanly)
        log_path = tmp_path / "mock-18080.log"
        log_path.write_text('{"event":"started","http":{"listen":"127.0.0.1:18080","stubs":1},"kafka":null}\n')

        # Write pidfile
        pidfile_path = tmp_path / "mock-18080.pid"
        pidfile_path.write_text(json.dumps({
            "pid": sleeper.pid,
            "listen": "127.0.0.1:18080",
            "port": 18080,
            "log_path": str(log_path),
            "config_path": None,
            "started_at": "2024-01-01T00:00:00Z",
            "run_id": "test-run"
        }))

        # Invoke stop with --timeout 1
        result, payload = _run(["mock", "stop", "--timeout", "1", "--state-dir", str(tmp_path)])

        # Assert exit_code == 0, signal == SIGKILL, warning set
        assert result.exit_code == 0, f"stdout: {result.output}"
        assert payload is not None
        assert payload["ok"] is True
        assert payload["result"]["stopped"] is True
        assert payload["result"]["signal"] == "SIGKILL"
        assert "warning" in payload["result"]
        assert "SIGKILL" in payload["result"]["warning"]
        assert "1s" in payload["result"]["warning"]  # timeout value mentioned

        # Verify sleeper process has terminated (SIGKILL got through)
        sleeper.wait(timeout=2)
        assert sleeper.poll() is not None

        # Verify pidfile was removed
        assert not pidfile_path.exists()


# ----------------------------------------------------------------------------
# Task 6: `mock status` tests (3 scenarios)
# ----------------------------------------------------------------------------


def test_mock_status_running():
    """Scenario 1: status running - live mock with started + http.hit + http.unmatched in log."""
    import tempfile
    from datetime import datetime, timezone

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)

        # Use os.getpid() since status never kills (cleaner test)
        my_pid = os.getpid()

        # Pre-write log with started + http.hit + http.unmatched
        log_path = tmp_path / "mock-18080.log"
        started_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
        log_path.write_text(f'{{"event":"started","http":{{"listen":"127.0.0.1:18080","stubs":1}},"kafka":null}}\n')
        with open(log_path, 'a') as f:
            f.write('{"event":"http.hit","method":"GET","path":"/test","ts":"2024-01-01T00:00:00Z"}\n')
            f.write('{"event":"http.unmatched","method":"POST","path":"/not-found","ts":"2024-01-01T00:00:00Z"}\n')

        # Write pidfile
        pidfile_path = tmp_path / "mock-18080.pid"
        pidfile_path.write_text(json.dumps({
            "pid": my_pid,
            "listen": "127.0.0.1:18080",
            "port": 18080,
            "log_path": str(log_path),
            "config_path": None,
            "started_at": started_at,
            "run_id": "test-run"
        }))

        # Invoke status (no selector - exactly one running)
        result, payload = _run(["mock", "status", "--state-dir", str(tmp_path)])

        # Assert exit_code == 0, envelope ok:true, running:true
        assert result.exit_code == 0, f"stdout: {result.output}"
        assert payload is not None
        assert payload["ok"] is True
        assert payload["command"] == "mock.status"
        assert payload["result"]["running"] is True
        assert payload["result"]["pid"] == my_pid
        assert payload["result"]["listen"] == "127.0.0.1:18080"
        assert isinstance(payload["result"]["uptime_ms"], int) and payload["result"]["uptime_ms"] >= 0
        assert payload["result"]["summary_so_far"]["http_hits"] == 1
        assert payload["result"]["summary_so_far"]["http_unmatched"] == 1
        assert len(payload["result"]["failures_so_far"]) == 1
        assert payload["result"]["failures_so_far"][0]["event"] == "http.unmatched"

        # Assert process still alive and pidfile still exists (status never kills)
        import errno
        try:
            os.kill(my_pid, 0)
        except OSError as e:
            if e.errno == errno.ESRCH:
                assert False, "Process should still be alive"
        assert pidfile_path.exists()


def test_mock_status_not_running():
    """Scenario 2: status not-running - empty state dir, return running:false."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)

        # Empty state dir (no pidfiles)
        result, payload = _run(["mock", "status", "--state-dir", str(tmp_path)])

        # Assert exit_code == 0, running:false
        assert result.exit_code == 0, f"stdout: {result.output}"
        assert payload is not None
        assert payload["ok"] is True
        assert payload["result"]["running"] is False


def test_mock_status_listen_selects():
    """Scenario 3: status --listen selects - two running, selector picks one."""
    import tempfile
    from datetime import datetime, timezone

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)

        # Use os.getpid() for both (status never kills)
        my_pid = os.getpid()

        # Pre-write two pidfiles with logs
        started_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

        # First mock
        log_path1 = tmp_path / "mock-18080.log"
        log_path1.write_text(f'{{"event":"started","http":{{"listen":"127.0.0.1:18080","stubs":1}},"kafka":null}}\n')
        with open(log_path1, 'a') as f:
            f.write('{"event":"http.hit","method":"GET","path":"/test","ts":"2024-01-01T00:00:00Z"}\n')

        pidfile1 = tmp_path / "mock-18080.pid"
        pidfile1.write_text(json.dumps({
            "pid": my_pid,
            "listen": "127.0.0.1:18080",
            "port": 18080,
            "log_path": str(log_path1),
            "config_path": None,
            "started_at": started_at,
            "run_id": "test-run-1"
        }))

        # Second mock
        log_path2 = tmp_path / "mock-18081.log"
        log_path2.write_text(f'{{"event":"started","http":{{"listen":"127.0.0.1:18081","stubs":1}},"kafka":null}}\n')
        with open(log_path2, 'a') as f:
            f.write('{"event":"http.hit","method":"GET","path":"/test","ts":"2024-01-01T00:00:00Z"}\n')

        pidfile2 = tmp_path / "mock-18081.pid"
        pidfile2.write_text(json.dumps({
            "pid": my_pid,
            "listen": "127.0.0.1:18081",
            "port": 18081,
            "log_path": str(log_path2),
            "config_path": None,
            "started_at": started_at,
            "run_id": "test-run-2"
        }))

        # Invoke status with --listen selector
        result, payload = _run(["mock", "status", "--listen", "127.0.0.1:18080", "--state-dir", str(tmp_path)])

        # Assert exit_code == 0, listen matches selector
        assert result.exit_code == 0, f"stdout: {result.output}"
        assert payload is not None
        assert payload["ok"] is True
        assert payload["result"]["running"] is True
        assert payload["result"]["listen"] == "127.0.0.1:18080"

        # Both pidfiles still exist (status never removes)
        assert pidfile1.exists()
        assert pidfile2.exists()
