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
