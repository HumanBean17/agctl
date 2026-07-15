"""Tests for agctl/daemon.py — generic process/pidfile lifecycle primitives.

These primitives are shared by `mock` and (in later tasks) `listen`. They were
moved verbatim from `agctl/commands/mock_commands.py` (`spawn_daemon`,
`_terminate`, `_require_posix_daemon`) and `agctl/mock/daemon.py` (`is_alive`,
`read_pidfile`, `write_pidfile`, `remove_pidfile`). These tests pin the new home
and the unchanged behavior.
"""

import os
from pathlib import Path

import pytest

from agctl import daemon


# Generic primitives are POSIX-only (they wrap os.kill/os.waitpid semantics).
pytestmark = pytest.mark.skipif(
    os.name == "nt",
    reason="agctl/daemon.py primitives are POSIX-only",
)


class TestModuleSurface:
    """The new module is the source of the generic primitives."""

    def test_module_exposes_primitives(self):
        """`from agctl import daemon` exposes all seven moved primitives."""
        for name in (
            "spawn_daemon",
            "terminate",
            "require_posix_daemon",
            "is_alive",
            "read_pidfile",
            "write_pidfile",
            "remove_pidfile",
        ):
            assert hasattr(daemon, name), f"daemon module missing {name}"


class TestPidfileRoundTrip:
    """write_pidfile/read_pidfile round-trip a dict in tmp_path."""

    def test_round_trip(self, tmp_path: Path):
        """write_pidfile then read_pidfile returns the same dict."""
        pidfile = tmp_path / "daemon.pid"
        data = {"pid": 12345, "listen": "127.0.0.1:18080", "port": 18080}
        assert not pidfile.exists()

        daemon.write_pidfile(pidfile, data)
        assert pidfile.exists()

        result = daemon.read_pidfile(pidfile)
        assert result == data

    def test_read_pidfile_missing_returns_none(self, tmp_path: Path):
        """read_pidfile on a missing file returns None (never raises)."""
        assert daemon.read_pidfile(tmp_path / "nope.pid") is None


class TestRemovePidfile:
    """remove_pidfile on a missing file is a no-op."""

    def test_remove_missing_is_noop(self, tmp_path: Path):
        """remove_pidfile on a nonexistent path does not raise."""
        missing = tmp_path / "absent.pid"
        assert not missing.exists()
        # Must not raise FileNotFoundError.
        daemon.remove_pidfile(missing)

    def test_remove_existing(self, tmp_path: Path):
        """remove_pidfile deletes an existing pidfile."""
        pidfile = tmp_path / "daemon.pid"
        daemon.write_pidfile(pidfile, {"pid": 1})
        assert pidfile.exists()

        daemon.remove_pidfile(pidfile)
        assert not pidfile.exists()


class TestIsAlive:
    """is_alive on a live pid."""

    def test_is_alive_on_self(self):
        """is_alive(os.getpid()) is True — we are alive."""
        assert daemon.is_alive(os.getpid()) is True

    def test_is_alive_on_dead_pid(self):
        """is_alive on an unlikely pid is False."""
        assert daemon.is_alive(999_999) is False


class TestRequirePosixDaemon:
    """require_posix_daemon() no-op on posix via the module-os seam."""

    def test_noop_on_posix(self, monkeypatch):
        """On os.name == 'posix' require_posix_daemon returns None."""
        # Detection reads `os.name` via the daemon module's `os` binding, so
        # patching `agctl.daemon.os.name` forces the branch without mutating the
        # global `os` module.
        monkeypatch.setattr(daemon.os, "name", "posix")
        assert daemon.require_posix_daemon() is None
