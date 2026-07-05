"""Tests for agctl/mock/daemon.py — pidfile, liveness, and target resolution."""

import json
import os
from pathlib import Path

import pytest

from agctl.mock.daemon import (
    is_alive,
    list_running_mocks,
    log_path,
    pidfile_path,
    read_pidfile,
    remove_pidfile,
    resolve_target,
    write_pidfile,
    RunningMock,
)


class TestIsAlive:
    """Tests for is_alive(pid)."""

    def test_is_alive_on_live_pid(self):
        """is_alive(os.getpid()) returns True."""
        assert is_alive(os.getpid()) is True

    def test_is_alive_on_dead_pid(self):
        """is_alive(999_999) returns False."""
        assert is_alive(999_999) is False


class TestPathNaming:
    """Tests for pidfile_path and log_path naming rules."""

    def test_pidfile_path_with_port(self, tmp_path):
        """pidfile_path(d, 18080) ends with mock-18080.pid."""
        result = pidfile_path(tmp_path, 18080)
        assert result.name == "mock-18080.pid"
        assert result.parent == tmp_path

    def test_pidfile_path_kafka_only(self, tmp_path):
        """pidfile_path(d, None) ends with mock-kafka.pid."""
        result = pidfile_path(tmp_path, None)
        assert result.name == "mock-kafka.pid"
        assert result.parent == tmp_path

    def test_log_path_with_port(self, tmp_path):
        """log_path(d, 18080) ends with mock-18080.log."""
        result = log_path(tmp_path, 18080)
        assert result.name == "mock-18080.log"
        assert result.parent == tmp_path

    def test_log_path_kafka_only(self, tmp_path):
        """log_path(d, None) ends with mock-kafka.log."""
        result = log_path(tmp_path, None)
        assert result.name == "mock-kafka.log"
        assert result.parent == tmp_path


class TestPidfileRoundTrip:
    """Tests for pidfile read/write/remove operations."""

    def test_write_and_read_pidfile(self, tmp_path):
        """write_pidfile then read_pidfile returns equal dict."""
        pidfile = tmp_path / "test.pid"
        data = {
            "pid": 12345,
            "listen": "127.0.0.1:18080",
            "port": 18080,
            "log_path": "/path/to/log.log",
            "config_path": "/path/to/config.yaml",
            "started_at": "2026-07-05T12:00:00Z",
            "run_id": "test-run-123",
        }
        write_pidfile(pidfile, data)
        result = read_pidfile(pidfile)
        assert result == data

    def test_read_pidfile_non_existent_returns_none(self, tmp_path):
        """read_pidfile on non-existent path returns None."""
        result = read_pidfile(tmp_path / "does-not-exist.pid")
        assert result is None

    def test_read_pidfile_unparseable_returns_none(self, tmp_path):
        """read_pidfile on unparseable file returns None."""
        bad_file = tmp_path / "bad.pid"
        bad_file.write_text("not valid json{{{")
        result = read_pidfile(bad_file)
        assert result is None

    def test_remove_pidfile_deletes_file(self, tmp_path):
        """remove_pidfile deletes the file."""
        pidfile = tmp_path / "to-remove.pid"
        pidfile.write_text("{}")
        remove_pidfile(pidfile)
        assert not pidfile.exists()

    def test_remove_pidfile_missing_file_no_error(self, tmp_path):
        """remove_pidfile does not raise on missing file."""
        remove_pidfile(tmp_path / "does-not-exist.pid")
        # No exception means success


class TestListRunningMocks:
    """Tests for list_running_mocks with live and stale cleanup."""

    def test_list_running_mocks_live_and_stale(self, tmp_path):
        """Returns only live mocks; removes stale pidfiles."""
        # Create two live mocks (using current process pid)
        live_data_1 = {
            "pid": os.getpid(),
            "listen": "127.0.0.1:18080",
            "port": 18080,
            "log_path": "/path/to/log1.log",
            "config_path": "/path/to/config1.yaml",
            "started_at": "2026-07-05T12:00:00Z",
            "run_id": "run-1",
        }
        live_data_2 = {
            "pid": os.getpid(),
            "listen": "127.0.0.1:18081",
            "port": 18081,
            "log_path": "/path/to/log2.log",
            "config_path": "/path/to/config2.yaml",
            "started_at": "2026-07-05T12:01:00Z",
            "run_id": "run-2",
        }
        # Create one stale mock (dead pid)
        stale_data = {
            "pid": 999_999,
            "listen": "127.0.0.1:18082",
            "port": 18082,
            "log_path": "/path/to/log3.log",
            "config_path": "/path/to/config3.yaml",
            "started_at": "2026-07-05T12:02:00Z",
            "run_id": "run-3",
        }

        write_pidfile(pidfile_path(tmp_path, 18080), live_data_1)
        write_pidfile(pidfile_path(tmp_path, 18081), live_data_2)
        stale_pidfile = pidfile_path(tmp_path, 18082)
        write_pidfile(stale_pidfile, stale_data)

        result = list_running_mocks(tmp_path)

        # Should return only the 2 live mocks
        assert len(result) == 2
        assert all(r.pid == os.getpid() for r in result)
        assert {r.port for r in result} == {18080, 18081}

        # Stale pidfile should have been removed
        assert not stale_pidfile.exists()

    def test_list_running_mocks_empty_dir_returns_empty(self, tmp_path):
        """Empty state_dir returns empty list."""
        result = list_running_mocks(tmp_path)
        assert result == []

    def test_list_running_mocks_missing_dir_creates_and_returns_empty(self, tmp_path):
        """Missing state_dir is created and returns empty list."""
        missing_dir = tmp_path / "does-not-exist"
        result = list_running_mocks(missing_dir)
        assert result == []
        assert missing_dir.exists()

    def test_list_running_mocks_skips_unparseable_pidfiles(self, tmp_path):
        """Skips pidfiles that cannot be parsed."""
        # Write a valid pidfile
        live_data = {
            "pid": os.getpid(),
            "listen": "127.0.0.1:18080",
            "port": 18080,
            "log_path": "/path/to/log.log",
            "config_path": "/path/to/config.yaml",
            "started_at": "2026-07-05T12:00:00Z",
            "run_id": "run-1",
        }
        write_pidfile(pidfile_path(tmp_path, 18080), live_data)

        # Write an unparseable pidfile
        bad_pidfile = pidfile_path(tmp_path, 18081)
        bad_pidfile.write_text("invalid json")

        result = list_running_mocks(tmp_path)

        # Should return only the valid one
        assert len(result) == 1
        assert result[0].port == 18080


class TestResolveTarget:
    """Tests for resolve_target with the full matrix."""

    def _write_mock_pidfile(self, state_dir: Path, port: int, pid: int):
        """Helper to write a mock pidfile."""
        data = {
            "pid": pid,
            "listen": f"127.0.0.1:{port}",
            "port": port,
            "log_path": f"/path/to/log-{port}.log",
            "config_path": f"/path/to/config-{port}.yaml",
            "started_at": "2026-07-05T12:00:00Z",
            "run_id": f"run-{port}",
        }
        write_pidfile(pidfile_path(state_dir, port), data)

    def test_resolve_target_all_returns_all(self, tmp_path):
        """all_=True returns all running mocks."""
        self._write_mock_pidfile(tmp_path, 18080, os.getpid())
        self._write_mock_pidfile(tmp_path, 18081, os.getpid())

        result = resolve_target(tmp_path, listen=None, pid=None, all_=True)

        assert len(result) == 2
        assert {r.port for r in result} == {18080, 18081}

    def test_resolve_target_no_args_two_running_raises_error(self, tmp_path):
        """No args with 2 running mocks raises ConfigError."""
        self._write_mock_pidfile(tmp_path, 18080, os.getpid())
        self._write_mock_pidfile(tmp_path, 18081, os.getpid())

        from agctl.errors import ConfigError

        with pytest.raises(ConfigError) as exc_info:
            resolve_target(tmp_path, listen=None, pid=None, all_=False)

        assert "multiple mocks running" in str(exc_info.value)
        assert "candidates" in exc_info.value.detail

    def test_resolve_target_no_args_one_running_returns_one(self, tmp_path):
        """No args with 1 running mock returns that one."""
        self._write_mock_pidfile(tmp_path, 18080, os.getpid())

        result = resolve_target(tmp_path, listen=None, pid=None, all_=False)

        assert len(result) == 1
        assert result[0].port == 18080

    def test_resolve_target_no_args_zero_running_returns_empty(self, tmp_path):
        """No args with 0 running mocks returns empty list."""
        result = resolve_target(tmp_path, listen=None, pid=None, all_=False)
        assert result == []

    def test_resolve_target_listen_matching(self, tmp_path):
        """listen= matching one returns that one."""
        self._write_mock_pidfile(tmp_path, 18080, os.getpid())
        self._write_mock_pidfile(tmp_path, 18081, os.getpid())

        result = resolve_target(tmp_path, listen="127.0.0.1:18080", pid=None, all_=False)

        assert len(result) == 1
        assert result[0].port == 18080

    def test_resolve_target_listen_non_matching_raises_error(self, tmp_path):
        """listen= non-matching raises ConfigError."""
        self._write_mock_pidfile(tmp_path, 18080, os.getpid())

        from agctl.errors import ConfigError

        with pytest.raises(ConfigError) as exc_info:
            resolve_target(tmp_path, listen="127.0.0.1:99999", pid=None, all_=False)

        assert "no running mock on" in str(exc_info.value)
        assert exc_info.value.detail["listen"] == "127.0.0.1:99999"

    def test_resolve_target_pid_matching(self, tmp_path):
        """pid= matching returns that one."""
        self._write_mock_pidfile(tmp_path, 18080, os.getpid())

        result = resolve_target(tmp_path, listen=None, pid=os.getpid(), all_=False)

        assert len(result) == 1
        assert result[0].pid == os.getpid()

    def test_resolve_target_pid_non_matching_raises_error(self, tmp_path):
        """pid= non-matching raises ConfigError."""
        self._write_mock_pidfile(tmp_path, 18080, os.getpid())

        from agctl.errors import ConfigError

        with pytest.raises(ConfigError) as exc_info:
            resolve_target(tmp_path, listen=None, pid=999_999, all_=False)

        assert "no running mock with pid" in str(exc_info.value)
        assert exc_info.value.detail["pid"] == 999_999

    def test_resolve_target_all_with_two_running(self, tmp_path):
        """all_=True with 2 running returns both."""
        self._write_mock_pidfile(tmp_path, 18080, os.getpid())
        self._write_mock_pidfile(tmp_path, 18081, os.getpid())

        result = resolve_target(tmp_path, listen=None, pid=None, all_=True)

        assert len(result) == 2
        assert {r.port for r in result} == {18080, 18081}


class TestTaxonomyConstants:
    """Tests for failure-event taxonomy constants."""

    def test_fatal_failure_events_has_exact_four_names(self):
        """FATAL_FAILURE_EVENTS contains exactly the four fatal names."""
        from agctl.mock.daemon import FATAL_FAILURE_EVENTS

        assert FATAL_FAILURE_EVENTS == {
            "http.unmatched",
            "http.body_parse_skipped",
            "kafka.skipped",
            "kafka.error",
        }

    def test_all_failure_events_includes_capture_missing(self):
        """ALL_FAILURE_EVENTS equals the four plus capture.missing."""
        from agctl.mock.daemon import ALL_FAILURE_EVENTS, FATAL_FAILURE_EVENTS

        expected = FATAL_FAILURE_EVENTS | {"capture.missing"}
        assert ALL_FAILURE_EVENTS == expected


class TestParseLog:
    """Tests for parse_log NDJSON parser."""

    def test_parse_log_happy_path(self, tmp_path):
        """parse_log reads started, events, summary correctly."""
        log_file = tmp_path / "test.log"

        # Write NDJSON log lines
        lines = [
            '{"event":"started","http":{"listen":"0.0.0.0:18080","stubs":2},"kafka":null}',
            '{"event":"http.hit","method":"GET","path":"/"}',
            '{"event":"http.unmatched","method":"POST","path":"/unknown"}',
            '{"event":"kafka.error","message":"delivery failed"}',
            '{"event":"capture.missing","type":"http","key":"req"}',
            '{"event":"summary","http_hits":1,"http_unmatched":1,"http_body_parse_skipped":0,"kafka_reactions":0,"kafka_skipped":0,"kafka_errors":1,"duration_ms":500}',
        ]
        log_file.write_text("\n".join(lines))

        from agctl.mock.daemon import parse_log

        parsed = parse_log(log_file)

        # Check started
        assert parsed.started == {
            "event": "started",
            "http": {"listen": "0.0.0.0:18080", "stubs": 2},
            "kafka": None,
        }

        # Check summary
        assert parsed.summary == {
            "event": "summary",
            "http_hits": 1,
            "http_unmatched": 1,
            "http_body_parse_skipped": 0,
            "kafka_reactions": 0,
            "kafka_skipped": 0,
            "kafka_errors": 1,
            "duration_ms": 500,
        }

        # Check summary_so_far
        assert parsed.summary_so_far == {
            "http_hits": 1,
            "http_unmatched": 1,
            "http_body_parse_skipped": 0,
            "kafka_reactions": 0,
            "kafka_skipped": 0,
            "kafka_errors": 1,
        }

        # Check failures - should have 3 entries (unmatched, kafka.error, capture.missing)
        assert len(parsed.failures) == 3
        assert parsed.failures[0]["event"] == "http.unmatched"
        assert parsed.failures[1]["event"] == "kafka.error"
        assert parsed.failures[2]["event"] == "capture.missing"

    def test_parse_log_startup_error_path(self, tmp_path):
        """parse_log handles startup-error envelope (no event key)."""
        log_file = tmp_path / "startup-error.log"

        # Write a startup-error envelope (no "event" key)
        line = '{"ok":false,"command":"mock.run","error":{"type":"ConfigError","message":"bad"}}'
        log_file.write_text(line)

        from agctl.mock.daemon import parse_log

        parsed = parse_log(log_file)

        # Should have startup_error set
        assert parsed.startup_error == {
            "ok": False,
            "command": "mock.run",
            "error": {"type": "ConfigError", "message": "bad"},
        }

        # Nothing else should be set
        assert parsed.started is None
        assert parsed.summary is None
        assert parsed.failures == []

    def test_parse_log_missing_file_returns_empty_parsed_log(self, tmp_path):
        """parse_log on non-existent path returns empty ParsedLog."""
        from agctl.mock.daemon import parse_log

        parsed = parse_log(tmp_path / "does-not-exist.log")

        assert parsed.started is None
        assert parsed.startup_error is None
        assert parsed.summary is None
        assert parsed.summary_so_far == {
            "http_hits": 0,
            "http_unmatched": 0,
            "http_body_parse_skipped": 0,
            "kafka_reactions": 0,
            "kafka_skipped": 0,
            "kafka_errors": 0,
        }
        assert parsed.failures == []

    def test_parse_log_unreadable_file_returns_empty_parsed_log(self, tmp_path):
        """parse_log on unreadable file (directory) returns empty ParsedLog."""
        from agctl.mock.daemon import parse_log

        # Pass a directory path to trigger OSError (IsADirectoryError)
        parsed = parse_log(tmp_path)

        assert parsed.started is None
        assert parsed.startup_error is None
        assert parsed.summary is None
        assert parsed.summary_so_far == {
            "http_hits": 0,
            "http_unmatched": 0,
            "http_body_parse_skipped": 0,
            "kafka_reactions": 0,
            "kafka_skipped": 0,
            "kafka_errors": 0,
        }
        assert parsed.failures == []


class TestHasFatalFailure:
    """Tests for has_fatal_failure."""

    def test_has_fatal_failure_with_only_capture_missing_returns_false(self):
        """Only capture.missing (non-fatal) returns False."""
        from agctl.mock.daemon import ParsedLog, has_fatal_failure

        parsed = ParsedLog(
            started=None,
            startup_error=None,
            summary=None,
            summary_so_far={},
            failures=[{"event": "capture.missing", "type": "http"}],
        )

        assert has_fatal_failure(parsed) is False

    def test_has_fatal_failure_with_http_unmatched_returns_true(self):
        """Adding http.unmatched (fatal) returns True."""
        from agctl.mock.daemon import ParsedLog, has_fatal_failure

        parsed = ParsedLog(
            started=None,
            startup_error=None,
            summary=None,
            summary_so_far={},
            failures=[
                {"event": "capture.missing", "type": "http"},
                {"event": "http.unmatched", "method": "POST"},
            ],
        )

        assert has_fatal_failure(parsed) is True
