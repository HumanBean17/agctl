"""Integration round-trip test for mock daemon lifecycle (Task 7).

This test exercises the full mock daemon lifecycle: start → hit → status → stop,
using real subprocesses, real HTTP serving, and real file I/O (no monkeypatching).

Test scenarios (parameterized):
- Variant A (clean): hit a stubbed /ping endpoint, assert clean exit.
- Variant B (with-failure): hit /ping, then /no-such-path to trigger http.unmatched,
  assert AssertionFailure envelope and exit code 1.

Critical hygiene: the test MUST guarantee `mock stop` runs even if an assertion
fails, using try/finally. An orphaned daemon would block the port and break
subsequent test runs.
"""

from __future__ import annotations

import json
import os
import socket
from pathlib import Path
from urllib import error as urllib_error
from urllib import request

import pytest
from click.testing import CliRunner

from agctl.cli import cli


def _allocate_free_port() -> int:
    """Allocate a free port by binding to 127.0.0.1:0 and reading the port."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]
    finally:
        sock.close()


def _write_test_config(config_path: Path, port: int) -> None:
    """Write a minimal agctl.yaml config with one HTTP stub."""
    config_path.write_text(
        f"""version: "2.0"
mocks:
  http:
    listen: "127.0.0.1:{port}"
    stubs:
      ping:
        method: GET
        path: /ping
        response:
          status: 200
          body: '{{"ok": true}}'
"""
    )


def _assert_port_closed(port: int) -> None:
    """Assert that a port is no longer accepting connections."""
    with pytest.raises((ConnectionRefusedError, OSError)):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.settimeout(0.5)
            sock.connect(("127.0.0.1", port))
        finally:
            sock.close()


@pytest.mark.parametrize("with_failure", [False, True])
def test_mock_daemon_round_trip(tmp_path: Path, with_failure: bool) -> None:
    """Test full mock daemon lifecycle: start → hit → status → stop.

    Variant A (with_failure=False):
    - Start daemon with a /ping stub
    - Hit /ping, assert 200 and body contains "ok"
    - status shows running=true, http_hits >= 1, no failures
    - stop returns exit_code=0, ok=true, failures=[]
    - Port is closed after stop

    Variant B (with_failure=True):
    - Start daemon with a /ping stub
    - Hit /ping, assert 200
    - Hit /no-such-path, assert 404 (generates http.unmatched event)
    - status shows running=true, http_hits >= 1, failures_so_far contains http.unmatched
    - stop returns exit_code=1, ok=false, error.type=AssertionError
    - Port is closed after stop
    """
    # Step 1: Allocate a free port and write config
    port = _allocate_free_port()
    config_path = tmp_path / "agctl.yaml"
    state_dir = tmp_path / ".agctl"
    state_dir.mkdir(exist_ok=True)

    _write_test_config(config_path, port)

    # Step 2: Start the daemon
    result = CliRunner().invoke(
        cli,
        [
            "mock",
            "start",
            "--config",
            str(config_path),
            "--only",
            "http",
            "--state-dir",
            str(state_dir),
        ],
    )

    try:
        # Parse the envelope from start's stdout
        start_envelope = json.loads(result.output)
        assert result.exit_code == 0, f"start failed: {result.output}"
        assert start_envelope["ok"] is True
        assert start_envelope["result"]["pid"] is not None
        assert isinstance(start_envelope["result"]["pid"], int)
        assert start_envelope["result"]["listen"] == f"127.0.0.1:{port}"
        assert start_envelope["result"]["stubs"] == 1

        pid = start_envelope["result"]["pid"]

        # Assert pidfile exists
        pidfile = state_dir / f"mock-{port}.pid"
        assert pidfile.exists(), "pidfile should exist after start"
        # Step 3: Hit the /ping endpoint (both variants)
        ping_url = f"http://127.0.0.1:{port}/ping"
        with request.urlopen(ping_url, timeout=5) as response:
            assert response.status == 200, f"ping failed: {response.read().decode()}"
            body = response.read().decode()
            assert "ok" in body, f"ping body missing 'ok': {body}"

        # Step 4: Variant B only - hit unmatched path to trigger failure
        if with_failure:
            unmatched_url = f"http://127.0.0.1:{port}/no-such-path"
            try:
                request.urlopen(unmatched_url, timeout=5)
                # If we get here, something is wrong - 404 should raise
                assert False, "expected HTTPError for 404 response"
            except urllib_error.HTTPError as e:
                # 404 is expected - this generates the http.unmatched event
                assert e.code == 404, f"expected 404, got {e.code}"

        # Step 5: Check status
        status_result = CliRunner().invoke(
            cli,
            ["mock", "status", "--state-dir", str(state_dir)],
        )

        status_envelope = json.loads(status_result.output)
        assert status_result.exit_code == 0, f"status failed: {status_result.output}"
        assert status_envelope["ok"] is True
        assert status_envelope["result"]["running"] is True
        assert status_envelope["result"]["pid"] == pid
        assert status_envelope["result"]["listen"] == f"127.0.0.1:{port}"
        assert status_envelope["result"]["summary_so_far"]["http_hits"] >= 1

        # Variant B: failures_so_far should contain http.unmatched
        if with_failure:
            failures = status_envelope["result"]["failures_so_far"]
            assert any(f.get("event") == "http.unmatched" for f in failures), \
                f"expected http.unmatched in failures_so_far: {failures}"

    finally:
        # CRITICAL: Always run stop, even if assertions fail
        stop_result = CliRunner().invoke(
            cli,
            ["mock", "stop", "--state-dir", str(state_dir)],
        )

        stop_envelope = json.loads(stop_result.output)

        # Variant A: clean exit (exit_code=0, ok=true, stopped=true, failures=[])
        if not with_failure:
            assert stop_result.exit_code == 0, f"stop failed: {stop_result.output}"
            assert stop_envelope["ok"] is True
            assert stop_envelope["result"]["stopped"] is True
            assert stop_envelope["result"]["failures"] == []
        # Variant B: assertion failure (exit_code=1, ok=false, error.type=AssertionError)
        else:
            assert stop_result.exit_code == 1, f"stop should fail: {stop_result.output}"
            assert stop_envelope["ok"] is False
            assert stop_envelope["error"]["type"] == "AssertionError"
            # error.detail should contain the failures list
            error_detail = stop_envelope["error"]["detail"]
            assert "failures" in error_detail
            failures = error_detail["failures"]
            assert any(f.get("event") == "http.unmatched" for f in failures), \
                f"expected http.unmatched in failures: {failures}"

    # Step 7: Assert pidfile was removed
    assert not pidfile.exists(), "pidfile should be removed after stop"

    # Step 8: Assert daemon process is gone
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)  # Should raise ProcessLookupError

    # Step 9: Assert port is closed (daemon actually exited)
    _assert_port_closed(port)
