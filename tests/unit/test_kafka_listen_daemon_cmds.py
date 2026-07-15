"""Unit tests for `kafka listen start`/`status`/`stop` managed-daemon commands (Task 8).

Mirrors tests/unit/test_mock_lifecycle.py style: the ``_core`` functions are
driven directly (the brief specifies asserting on ``_kafka_listen_start_core``
return values). ``spawn_daemon`` is monkeypatched so no real daemon is spawned;
a canned ``events.log`` with a ``started`` line is pre-written by the fake.

POSIX-only: the managed daemon surface is gated by ``require_posix_daemon``.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from agctl.commands.kafka_listen_commands import (
    _kafka_listen_start_core,
    _kafka_listen_status_core,
    _kafka_listen_stop_core,
)
from agctl.errors import AssertionFailure, ConfigError

pytestmark = pytest.mark.skipif(
    os.name == "nt",
    reason="managed listen daemon is POSIX-only; use 'kafka listen run' or WSL on Windows",
)


# ---------------------------------------------------------------------------
# Config + path helpers
# ---------------------------------------------------------------------------


def _write_config(tmp_path: Path) -> Path:
    """Write a one-cluster v3 config (no patterns needed for topic-only starts)."""
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


def _now_iso_z() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _started_line(run_id: str, topics: list[str], cluster: str = "default") -> str:
    return (
        json.dumps(
            {
                "event": "started",
                "run_id": run_id,
                "topics": topics,
                "group": f"agctl-listen-{run_id}",
                "cluster": cluster,
                "started_at": _now_iso_z(),
            }
        )
        + "\n"
    )


# ---------------------------------------------------------------------------
# Pytest fixture: track + cleanup sleeper subprocesses (for stop tests)
# ---------------------------------------------------------------------------


@pytest.fixture
def sleeper_pids():
    """Track sleeper subprocess PIDs and ensure cleanup on test exit."""
    pids: list[int] = []

    yield pids

    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
        except (ProcessLookupError, OSError):
            pass
        for _ in range(20):
            try:
                os.waitpid(pid, os.WNOHANG)
            except (ChildProcessError, OSError):
                pass
            try:
                os.kill(pid, 0)
            except (ProcessLookupError, OSError):
                break
            time.sleep(0.05)
        else:
            try:
                os.kill(pid, signal.SIGKILL)
            except (ProcessLookupError, OSError):
                pass
            try:
                os.waitpid(pid, os.WNOHANG)
            except (ChildProcessError, OSError):
                pass


def _spawn_sleeper() -> int:
    """Spawn a real subprocess that handles SIGTERM gracefully; return its pid."""
    code = (
        "import signal, time\n"
        "stop=False\n"
        "def h(s,f):\n"
        "    global stop\n"
        "    stop=True\n"
        "signal.signal(signal.SIGTERM,h)\n"
        "end=time.time()+30\n"
        "while time.time()<end and not stop:\n"
        "    time.sleep(0.001)\n"
    )
    proc = subprocess.Popen([sys.executable, "-c", code])
    return proc.pid


# ---------------------------------------------------------------------------
# start
# ---------------------------------------------------------------------------


class TestStart:
    def test_start_success_writes_pidfile_meta_returns_dict(self, tmp_path):
        """spawn_daemon writes started line + returns fake pid; pidfile + meta
        written; return dict carries run_id/topics/cluster/started_at."""
        cfg_path = _write_config(tmp_path)
        state_dir = tmp_path / "state"

        recorded_argv: list[list[str]] = []

        def fake_spawn(argv, log_path, env=None):
            recorded_argv.append(list(argv))
            log_file = Path(log_path)
            log_file.parent.mkdir(parents=True, exist_ok=True)
            log_file.write_text(
                _started_line("deadbeef", ["orders.created"])
            )
            return 12345

        with patch(
            "agctl.commands.kafka_listen_commands.spawn_daemon",
            side_effect=fake_spawn,
        ):
            result = _kafka_listen_start_core(
                config_path=str(cfg_path),
                topics=["orders.created"],
                patterns=[],
                cluster=None,
                capture_match=None,
                max_bytes=268435456,
                state_dir=str(state_dir),
                overlay_paths=None,
                env_file=None,
            )

        # Return dict shape.
        assert result["pid"] == 12345
        assert isinstance(result["run_id"], str) and len(result["run_id"]) == 8
        assert result["topics"] == ["orders.created"]
        assert result["cluster"] == "default"
        assert result["group"] == f"agctl-listen-{result['run_id']}"
        assert result["started_at"]
        assert result["state_dir"] == str(state_dir)

        # Pidfile written with full metadata.
        pidfile = state_dir / f"listen-{result['run_id']}.pid"
        assert pidfile.exists()
        piddata = json.loads(pidfile.read_text())
        assert piddata["pid"] == 12345
        assert piddata["run_id"] == result["run_id"]
        assert piddata["topics"] == ["orders.created"]
        assert piddata["cluster"] == "default"
        assert piddata["group"] == result["group"]

        # meta.json written.
        rdir = state_dir / f"listen-{result['run_id']}"
        meta = json.loads((rdir / "meta.json").read_text())
        assert meta["topics"] == ["orders.created"]
        assert meta["cluster"] == "default"
        assert meta["group"] == result["group"]
        assert meta["capture_match"] is None
        assert meta["max_bytes_per_topic"] == 268435456

        # Spawn argv: global --config first, then subcommand, then run-id/state/topic.
        argv = recorded_argv[0]
        assert "--config" in argv
        cfg_idx = argv.index("--config")
        assert Path(argv[cfg_idx + 1]).resolve() == cfg_path.resolve()
        assert "kafka" in argv and "listen" in argv and "run" in argv
        assert "--run-id" in argv and result["run_id"] in argv
        assert "--state-dir" in argv
        assert "--topic" in argv and "orders.created" in argv
        assert "--cluster" in argv and "default" in argv
        assert "--max-bytes-per-topic" in argv

    def test_start_already_running_raises_configerror(self, tmp_path):
        """A live pidfile for the (generated) run_id would not collide in practice,
        but a pre-existing live pidfile at the same path blocks start. We verify the
        already-running guard by pre-writing a pidfile whose pid is alive."""
        cfg_path = _write_config(tmp_path)
        state_dir = tmp_path / "state"
        state_dir.mkdir()

        # Force a known run_id by pre-writing a pidfile the start core will see.
        # new_run_id is random, so instead plant a pidfile for os.getpid() and
        # patch new_run_id to a fixed value matching the planted pidfile.
        fixed_rid = "cafebabe"
        pidfile = state_dir / f"listen-{fixed_rid}.pid"
        pidfile.write_text(
            json.dumps(
                {
                    "pid": os.getpid(),
                    "run_id": fixed_rid,
                    "topics": ["orders.created"],
                    "group": f"agctl-listen-{fixed_rid}",
                    "cluster": "default",
                    "started_at": _now_iso_z(),
                    "state_dir": str(state_dir),
                    "log_path": str(state_dir / f"listen-{fixed_rid}" / "events.log"),
                }
            )
        )

        with patch(
            "agctl.commands.kafka_listen_commands.new_run_id", return_value=fixed_rid
        ):
            with pytest.raises(ConfigError) as ei:
                _kafka_listen_start_core(
                    config_path=str(cfg_path),
                    topics=["orders.created"],
                    patterns=[],
                    cluster=None,
                    capture_match=None,
                    max_bytes=268435456,
                    state_dir=str(state_dir),
                    overlay_paths=None,
                    env_file=None,
                )
        assert "already running" in ei.value.message.lower()
        assert ei.value.detail["run_id"] == fixed_rid

    def test_start_startup_error_cleans_pidfile(self, tmp_path):
        """spawn writes a startup-error envelope; core terminates + removes pidfile."""
        cfg_path = _write_config(tmp_path)
        state_dir = tmp_path / "state"

        terminated: list[int] = []

        def fake_spawn(argv, log_path, env=None):
            Path(log_path).parent.mkdir(parents=True, exist_ok=True)
            Path(log_path).write_text(
                json.dumps(
                    {
                        "ok": False,
                        "command": "kafka.listen.run",
                        "error": {
                            "type": "ConnectionError",
                            "message": "broker unreachable",
                            "detail": {},
                        },
                    }
                )
                + "\n"
            )
            return 12345

        def fake_terminate(pid, timeout):
            terminated.append(pid)
            return "SIGTERM"

        with patch(
            "agctl.commands.kafka_listen_commands.spawn_daemon", side_effect=fake_spawn
        ), patch(
            "agctl.commands.kafka_listen_commands.terminate", side_effect=fake_terminate
        ):
            with pytest.raises(ConfigError) as ei:
                _kafka_listen_start_core(
                    config_path=str(cfg_path),
                    topics=["orders.created"],
                    patterns=[],
                    cluster=None,
                    capture_match=None,
                    max_bytes=268435456,
                    state_dir=str(state_dir),
                    overlay_paths=None,
                    env_file=None,
                )
        assert "broker unreachable" in ei.value.message
        assert 12345 in terminated

    def test_start_readiness_timeout_cleans_pidfile(self, tmp_path):
        """spawn writes nothing; budget expires → ConfigError + cleanup."""
        cfg_path = _write_config(tmp_path)
        state_dir = tmp_path / "state"

        def fake_spawn(argv, log_path, env=None):
            Path(log_path).parent.mkdir(parents=True, exist_ok=True)
            return 12345  # write nothing

        with patch(
            "agctl.commands.kafka_listen_commands.spawn_daemon", side_effect=fake_spawn
        ), patch(
            "agctl.commands.kafka_listen_commands._START_BUDGET_SECONDS", 0.2
        ), patch(
            "agctl.commands.kafka_listen_commands.terminate", return_value="SIGTERM"
        ):
            with pytest.raises(ConfigError) as ei:
                _kafka_listen_start_core(
                    config_path=str(cfg_path),
                    topics=["orders.created"],
                    patterns=[],
                    cluster=None,
                    capture_match=None,
                    max_bytes=268435456,
                    state_dir=str(state_dir),
                    overlay_paths=None,
                    env_file=None,
                )
        assert "did not become ready" in ei.value.message.lower()

    def test_start_no_topics_raises_configerror(self, tmp_path):
        cfg_path = _write_config(tmp_path)
        with pytest.raises(ConfigError):
            _kafka_listen_start_core(
                config_path=str(cfg_path),
                topics=[],
                patterns=[],
                cluster=None,
                capture_match=None,
                max_bytes=268435456,
                state_dir=str(tmp_path / "state"),
                overlay_paths=None,
                env_file=None,
            )


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------


class TestStatus:
    def test_status_running_counts_capture_lines_and_bytes(self, tmp_path):
        """pidfile + run_dir with two <topic>.ndjson (N and M lines) → per-topic
        captured==N/M, bytes==file size, running:True."""
        state_dir = tmp_path / "state"
        run_id = "aa112233"
        rdir = state_dir / f"listen-{run_id}"
        rdir.mkdir(parents=True)
        logp = rdir / "events.log"
        logp.write_text(_started_line(run_id, ["alpha", "beta"]))

        # Two capture files: 3 lines / 5 lines.
        (rdir / "alpha.ndjson").write_text('{"v":1}\n{"v":2}\n{"v":3}\n')
        (rdir / "beta.ndjson").write_text('{"v":1}\n{"v":2}\n{"v":3}\n{"v":4}\n{"v":5}\n')

        pidfile = state_dir / f"listen-{run_id}.pid"
        pidfile.write_text(
            json.dumps(
                {
                    "pid": os.getpid(),
                    "run_id": run_id,
                    "topics": ["alpha", "beta"],
                    "group": f"agctl-listen-{run_id}",
                    "cluster": "default",
                    "started_at": _now_iso_z(),
                    "state_dir": str(state_dir),
                    "log_path": str(logp),
                }
            )
        )

        result = _kafka_listen_status_core(
            run_id=None, pid=None, state_dir=str(state_dir)
        )
        assert result["running"] is True
        assert result["pid"] == os.getpid()
        assert result["run_id"] == run_id
        assert isinstance(result["uptime_ms"], int) and result["uptime_ms"] >= 0

        topics = {t["topic"]: t for t in result["topics"]}
        assert topics["alpha"]["captured"] == 3
        assert topics["alpha"]["bytes"] == len('{"v":1}\n{"v":2}\n{"v":3}\n')
        assert topics["alpha"]["overflowed"] is False
        assert topics["beta"]["captured"] == 5
        assert topics["beta"]["overflowed"] is False

        # Pidfile still present (status never removes).
        assert pidfile.exists()

    def test_status_overflow_flag(self, tmp_path):
        """capture.overflow event for a topic flags overflowed:True."""
        state_dir = tmp_path / "state"
        run_id = "bb445566"
        rdir = state_dir / f"listen-{run_id}"
        rdir.mkdir(parents=True)
        logp = rdir / "events.log"
        logp.write_text(
            _started_line(run_id, ["alpha"])
            + json.dumps({"event": "capture.overflow", "topic": "alpha"}) + "\n"
        )
        (rdir / "alpha.ndjson").write_text('{"v":1}\n')

        pidfile = state_dir / f"listen-{run_id}.pid"
        pidfile.write_text(
            json.dumps(
                {
                    "pid": os.getpid(),
                    "run_id": run_id,
                    "topics": ["alpha"],
                    "group": f"agctl-listen-{run_id}",
                    "cluster": "default",
                    "started_at": _now_iso_z(),
                    "state_dir": str(state_dir),
                    "log_path": str(logp),
                }
            )
        )
        result = _kafka_listen_status_core(
            run_id=None, pid=None, state_dir=str(state_dir)
        )
        assert result["topics"][0]["overflowed"] is True
        assert result["topics"][0]["captured"] == 1

    def test_status_missing_capture_file_is_zero(self, tmp_path):
        """No <topic>.ndjson → captured 0, bytes 0."""
        state_dir = tmp_path / "state"
        run_id = "cc778899"
        rdir = state_dir / f"listen-{run_id}"
        rdir.mkdir(parents=True)
        logp = rdir / "events.log"
        logp.write_text(_started_line(run_id, ["alpha"]))
        # No alpha.ndjson written.
        pidfile = state_dir / f"listen-{run_id}.pid"
        pidfile.write_text(
            json.dumps(
                {
                    "pid": os.getpid(),
                    "run_id": run_id,
                    "topics": ["alpha"],
                    "group": f"agctl-listen-{run_id}",
                    "cluster": "default",
                    "started_at": _now_iso_z(),
                    "state_dir": str(state_dir),
                    "log_path": str(logp),
                }
            )
        )
        result = _kafka_listen_status_core(
            run_id=None, pid=None, state_dir=str(state_dir)
        )
        assert result["topics"][0]["captured"] == 0
        assert result["topics"][0]["bytes"] == 0

    def test_status_not_running(self, tmp_path):
        result = _kafka_listen_status_core(
            run_id=None, pid=None, state_dir=str(tmp_path / "empty")
        )
        assert result == {"running": False}


# ---------------------------------------------------------------------------
# stop
# ---------------------------------------------------------------------------


class TestStop:
    def _plant_listener(
        self,
        state_dir: Path,
        run_id: str,
        pid: int,
        events: str,
        expectations: list[dict] | None = None,
    ) -> Path:
        """Write a pidfile + run_dir with canned events.log (+ optional asserts.jsonl)."""
        from agctl.listen.daemon import run_dir as _run_dir

        rdir = _run_dir(state_dir, run_id)
        rdir.mkdir(parents=True, exist_ok=True)
        logp = rdir / "events.log"
        logp.write_text(events)
        pidfile = state_dir / f"listen-{run_id}.pid"
        pidfile.write_text(
            json.dumps(
                {
                    "pid": pid,
                    "run_id": run_id,
                    "topics": ["alpha"],
                    "group": f"agctl-listen-{run_id}",
                    "cluster": "default",
                    "started_at": _now_iso_z(),
                    "state_dir": str(state_dir),
                    "log_path": str(logp),
                }
            )
        )
        if expectations:
            with (rdir / "asserts.jsonl").open("w") as fh:
                for exp in expectations:
                    fh.write(json.dumps(exp) + "\n")
        return rdir

    def test_stop_clean_removes_rundir(self, tmp_path, sleeper_pids):
        state_dir = tmp_path / "state"
        state_dir.mkdir()
        pid = _spawn_sleeper()
        sleeper_pids.append(pid)
        events = _started_line("dead0001", ["alpha"]) + json.dumps(
            {"event": "summary", "captured": {"alpha": 2}}
        ) + "\n"
        rdir = self._plant_listener(state_dir, "dead0001", pid, events)

        result = _kafka_listen_stop_core(
            run_id=None, pid=None, all_=False, timeout=5.0, state_dir=str(state_dir)
        )
        assert result["stopped"] is True
        assert result["pid"] == pid
        assert result["signal"] == "SIGTERM"
        assert result["cleaned"] is True
        assert result["failures"] == []
        # run_dir + pidfile gone.
        assert not rdir.exists()
        assert not (state_dir / "listen-dead0001.pid").exists()

    def test_stop_kafka_error_raises_and_cleans(self, tmp_path, sleeper_pids):
        state_dir = tmp_path / "state"
        state_dir.mkdir()
        pid = _spawn_sleeper()
        sleeper_pids.append(pid)
        events = (
            _started_line("dead0002", ["alpha"])
            + json.dumps(
                {"event": "kafka.error", "topic": "alpha", "message": "consume failed"}
            )
            + "\n"
        )
        rdir = self._plant_listener(state_dir, "dead0002", pid, events)

        with pytest.raises(AssertionFailure) as ei:
            _kafka_listen_stop_core(
                run_id=None,
                pid=None,
                all_=False,
                timeout=5.0,
                state_dir=str(state_dir),
            )
        assert "fatal failure" in ei.value.message.lower()
        assert ei.value.detail["stopped"] is True
        assert len(ei.value.detail["failures"]) == 1
        # run_dir removed even on failure.
        assert not rdir.exists()

    def test_stop_overflow_on_asserted_topic_is_fatal(self, tmp_path, sleeper_pids):
        """capture.overflow on a topic with an attached expectation is fatal;
        overflow on an un-asserted topic is NOT fatal."""
        state_dir = tmp_path / "state"
        state_dir.mkdir()
        pid = _spawn_sleeper()
        sleeper_pids.append(pid)
        events = (
            _started_line("dead0003", ["alpha", "beta"])
            + json.dumps({"event": "capture.overflow", "topic": "alpha"})
            + "\n"
            + json.dumps({"event": "capture.overflow", "topic": "beta"})
            + "\n"
        )
        # Only alpha has an attached expectation → alpha's overflow is fatal, beta's is not.
        rdir = self._plant_listener(
            state_dir,
            "dead0003",
            pid,
            events,
            expectations=[{"id": "exp-1", "topic": "alpha", "modes": {}, "params": {}, "expect_count": 1}],
        )

        with pytest.raises(AssertionFailure) as ei:
            _kafka_listen_stop_core(
                run_id=None,
                pid=None,
                all_=False,
                timeout=5.0,
                state_dir=str(state_dir),
            )
        # Exactly one fatal failure (alpha's overflow); beta's overflow is not fatal.
        assert len(ei.value.detail["failures"]) == 1
        assert ei.value.detail["failures"][0]["topic"] == "alpha"
        assert not rdir.exists()

    def test_stop_overflow_without_expectation_is_not_fatal(self, tmp_path, sleeper_pids):
        """capture.overflow on a topic with NO attached expectation is not fatal."""
        state_dir = tmp_path / "state"
        state_dir.mkdir()
        pid = _spawn_sleeper()
        sleeper_pids.append(pid)
        events = (
            _started_line("dead0004", ["alpha"])
            + json.dumps({"event": "capture.overflow", "topic": "alpha"})
            + "\n"
        )
        # No asserts.jsonl → no asserted topics.
        rdir = self._plant_listener(state_dir, "dead0004", pid, events)

        result = _kafka_listen_stop_core(
            run_id=None, pid=None, all_=False, timeout=5.0, state_dir=str(state_dir)
        )
        assert result["stopped"] is True
        # failures list carries the overflow event for visibility, but it's not fatal.
        assert len(result["failures"]) == 0
        assert not rdir.exists()

    def test_stop_not_running(self, tmp_path):
        result = _kafka_listen_stop_core(
            run_id=None, pid=None, all_=False, timeout=5.0, state_dir=str(tmp_path / "empty")
        )
        assert result == {"stopped": False}

    def test_stop_all(self, tmp_path, sleeper_pids):
        state_dir = tmp_path / "state"
        state_dir.mkdir()
        p1 = _spawn_sleeper()
        p2 = _spawn_sleeper()
        sleeper_pids.extend([p1, p2])
        r1 = self._plant_listener(
            state_dir, "dead0101", p1, _started_line("dead0101", ["alpha"])
        )
        r2 = self._plant_listener(
            state_dir, "dead0202", p2, _started_line("dead0202", ["alpha"])
        )

        result = _kafka_listen_stop_core(
            run_id=None, pid=None, all_=True, timeout=5.0, state_dir=str(state_dir)
        )
        assert isinstance(result["stopped"], list)
        assert len(result["stopped"]) == 2
        assert all(v["stopped"] is True for v in result["stopped"])
        assert all(v["cleaned"] is True for v in result["stopped"])
        assert not r1.exists()
        assert not r2.exists()


# ---------------------------------------------------------------------------
# multiple-running selector
# ---------------------------------------------------------------------------


class TestMultipleRunning:
    def _plant(self, state_dir: Path, run_id: str) -> None:
        rdir = state_dir / f"listen-{run_id}"
        rdir.mkdir(parents=True, exist_ok=True)
        logp = rdir / "events.log"
        logp.write_text(_started_line(run_id, ["alpha"]))
        (state_dir / f"listen-{run_id}.pid").write_text(
            json.dumps(
                {
                    "pid": os.getpid(),
                    "run_id": run_id,
                    "topics": ["alpha"],
                    "group": f"agctl-listen-{run_id}",
                    "cluster": "default",
                    "started_at": _now_iso_z(),
                    "state_dir": str(state_dir),
                    "log_path": str(logp),
                }
            )
        )

    def test_status_multiple_running_no_selector_configerror(self, tmp_path):
        state_dir = tmp_path / "state"
        state_dir.mkdir()
        self._plant(state_dir, "aaaa1111")
        self._plant(state_dir, "bbbb2222")
        with pytest.raises(ConfigError) as ei:
            _kafka_listen_status_core(
                run_id=None, pid=None, state_dir=str(state_dir)
            )
        assert "multiple" in ei.value.message.lower()

    def test_stop_multiple_running_no_selector_configerror(self, tmp_path):
        state_dir = tmp_path / "state"
        state_dir.mkdir()
        self._plant(state_dir, "aaaa3333")
        self._plant(state_dir, "bbbb4444")
        with pytest.raises(ConfigError) as ei:
            _kafka_listen_stop_core(
                run_id=None, pid=None, all_=False, timeout=5.0, state_dir=str(state_dir)
            )
        assert "multiple" in ei.value.message.lower()
