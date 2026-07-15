"""Tests for agctl/listen/daemon.py — run_id-keyed lifecycle helpers.

Mirrors tests/unit/test_mock_daemon.py's style: real file I/O against tmp_path,
no mocks. Pure functions only (no Kafka, no network).
"""

import json
import os
from pathlib import Path

import pytest

from agctl.listen.daemon import (
    ExpectationSpec,
    ParsedEvents,
    RunningListener,
    append_expectation,
    asserts_path,
    capture_path,
    events_log_path,
    list_running_listeners,
    meta_path,
    new_run_id,
    parse_events_log,
    pidfile_path,
    read_expectations,
    read_meta,
    resolve_listener_target,
    run_dir,
    write_meta,
)
from agctl.daemon import write_pidfile
from agctl.errors import ConfigError

# listen/daemon.py reuses the POSIX-only is_alive (os.kill(pid, 0)) from
# agctl/daemon.py. The managed listen daemon surface is gated to POSIX by the
# commands that call these helpers, so skip the whole file on native Windows
# (mirrors tests/unit/test_mock_daemon.py).
pytestmark = pytest.mark.skipif(
    os.name == "nt",
    reason="listen/daemon.py is POSIX-only (managed daemon surface); gated on Windows",
)


class TestNewRunId:
    """Tests for new_run_id."""

    def test_new_run_id_is_eight_hex_chars(self):
        """new_run_id returns 8 lowercase hex chars (secrets.token_hex(4))."""
        rid = new_run_id()
        assert isinstance(rid, str)
        assert len(rid) == 8
        # Must be valid hex.
        int(rid, 16)

    def test_new_run_id_is_random(self):
        """Two calls produce distinct ids (overwhelmingly likely)."""
        ids = {new_run_id() for _ in range(16)}
        assert len(ids) == 16


class TestPathNaming:
    """Tests for path derivation (Shared Types contract)."""

    def test_pidfile_path(self, tmp_path):
        """pidfile_path → <state_dir>/listen-<run_id>.pid."""
        result = pidfile_path(tmp_path, "abcd1234")
        assert result == tmp_path / "listen-abcd1234.pid"
        assert result.name == "listen-abcd1234.pid"
        assert result.parent == tmp_path

    def test_run_dir(self, tmp_path):
        """run_dir → <state_dir>/listen-<run_id>/."""
        result = run_dir(tmp_path, "abcd1234")
        assert result == tmp_path / "listen-abcd1234"
        assert result.parent == tmp_path

    def test_capture_path(self, tmp_path):
        """capture_path → <run_dir>/<topic>.ndjson."""
        rd = run_dir(tmp_path, "abcd1234")
        result = capture_path(rd, "orders.created")
        assert result == rd / "orders.created.ndjson"
        assert result.name == "orders.created.ndjson"

    def test_events_log_path(self, tmp_path):
        """events_log_path → <run_dir>/events.log."""
        rd = run_dir(tmp_path, "abcd1234")
        result = events_log_path(rd)
        assert result == rd / "events.log"
        assert result.name == "events.log"

    def test_meta_path(self, tmp_path):
        """meta_path → <run_dir>/meta.json."""
        rd = run_dir(tmp_path, "abcd1234")
        assert meta_path(rd) == rd / "meta.json"

    def test_asserts_path(self, tmp_path):
        """asserts_path → <run_dir>/asserts.jsonl."""
        rd = run_dir(tmp_path, "abcd1234")
        assert asserts_path(rd) == rd / "asserts.jsonl"


class TestMetaRoundTrip:
    """Tests for write_meta / read_meta."""

    def test_write_and_read_meta(self, tmp_path):
        """write_meta then read_meta round-trips the dict."""
        rd = run_dir(tmp_path, "abcd1234")
        meta = {
            "run_id": "abcd1234",
            "topics": ["orders.created", "orders.updated"],
            "group": "agctl-listen-abcd1234",
            "cluster": "prod",
            "started_at": "2026-07-15T09:00:00Z",
        }
        write_meta(rd, meta)
        # meta.json was created inside the run dir.
        assert meta_path(rd).exists()
        assert read_meta(rd) == meta

    def test_read_meta_missing_returns_none(self, tmp_path):
        """read_meta on a run dir with no meta.json returns None."""
        rd = run_dir(tmp_path, "abcd1234")
        assert read_meta(rd) is None

    def test_read_meta_unparseable_returns_none(self, tmp_path):
        """read_meta on a corrupt meta.json returns None."""
        rd = run_dir(tmp_path, "abcd1234")
        rd.mkdir(parents=True)
        meta_path(rd).write_text("not valid json{{{")
        assert read_meta(rd) is None


class TestExpectationsRoundTrip:
    """Tests for append_expectation / read_expectations."""

    def test_append_and_read_two_specs_preserves_order(self, tmp_path):
        """append_expectation then read_expectations round-trips two specs, order kept."""
        rd = run_dir(tmp_path, "abcd1234")
        spec_a = ExpectationSpec(
            id="ord",
            topic="orders.created",
            modes={"pattern": "order-created"},
            params={"orderId": "ord-789"},
            expect_count=1,
        )
        spec_b = ExpectationSpec(
            id="evt",
            topic="events",
            modes={"contains": '"eventType":"SHIPPED"', "match": None, "path": None, "pattern": None},
            params={},
            expect_count=2,
        )

        append_expectation(rd, spec_a)
        append_expectation(rd, spec_b)

        result = read_expectations(rd)
        assert len(result) == 2
        assert result[0]["id"] == "ord"
        assert result[0]["topic"] == "orders.created"
        assert result[0]["modes"] == {"pattern": "order-created"}
        assert result[0]["params"] == {"orderId": "ord-789"}
        assert result[0]["expect_count"] == 1
        assert result[1]["id"] == "evt"
        assert result[1]["expect_count"] == 2
        # Order preserved.
        assert [r["id"] for r in result] == ["ord", "evt"]

    def test_read_expectations_missing_file_returns_empty(self, tmp_path):
        """read_expectations on a run dir with no asserts.jsonl returns []."""
        rd = run_dir(tmp_path, "abcd1234")
        assert read_expectations(rd) == []

    def test_read_expectations_skips_blank_and_unparseable(self, tmp_path):
        """read_expectations skips blank and unparseable lines, keeps the good ones."""
        rd = run_dir(tmp_path, "abcd1234")
        rd.mkdir(parents=True)
        asserts_path(rd).write_text(
            "\n".join(
                [
                    json.dumps({"id": "a", "topic": "t", "modes": {}, "params": {}, "expect_count": 1}),
                    "",
                    "not valid json{{{",
                    json.dumps({"id": "b", "topic": "t", "modes": {}, "params": {}, "expect_count": 1}),
                ]
            )
            + "\n"
        )
        result = read_expectations(rd)
        assert [r["id"] for r in result] == ["a", "b"]

    def test_append_expectation_creates_run_dir_parents(self, tmp_path):
        """append_expectation creates the run dir (and parents) if absent."""
        rd = run_dir(tmp_path, "abcd1234")
        assert not rd.exists()
        append_expectation(
            rd,
            ExpectationSpec(id="x", topic="t", modes={}, params={}, expect_count=1),
        )
        assert asserts_path(rd).exists()


class TestListRunningListeners:
    """Tests for list_running_listeners."""

    def _write_pidfile(self, state_dir: Path, run_id: str, pid: int, **overrides):
        """Helper to write a listen pidfile."""
        data = {
            "pid": pid,
            "run_id": run_id,
            "topics": ["orders.created"],
            "group": f"agctl-listen-{run_id}",
            "cluster": "prod",
            "started_at": "2026-07-15T09:00:00Z",
            "state_dir": str(state_dir),
            "log_path": str(run_dir(state_dir, run_id) / "events.log"),
        }
        data.update(overrides)
        write_pidfile(pidfile_path(state_dir, run_id), data)

    def test_list_returns_live_and_cleans_stale(self, tmp_path):
        """One live pidfile is returned; one stale (dead-pid) pidfile is removed."""
        self._write_pidfile(tmp_path, "live00001", os.getpid())
        stale_pidfile = pidfile_path(tmp_path, "dead00002")
        self._write_pidfile(tmp_path, "dead00002", 999_999)

        result = list_running_listeners(tmp_path)

        assert len(result) == 1
        listener = result[0]
        assert isinstance(listener, RunningListener)
        assert listener.pid == os.getpid()
        assert listener.run_id == "live00001"
        assert listener.group == "agctl-listen-live00001"
        assert listener.topics == ["orders.created"]
        assert listener.cluster == "prod"
        assert listener.pidfile_path == pidfile_path(tmp_path, "live00001")

        # Stale pidfile cleaned.
        assert not stale_pidfile.exists()

    def test_list_missing_dir_returns_empty(self, tmp_path):
        """Missing state_dir does not error; returns empty."""
        missing = tmp_path / "does-not-exist"
        assert list_running_listeners(missing) == []

    def test_list_empty_dir_returns_empty(self, tmp_path):
        """Empty state_dir returns empty list."""
        assert list_running_listeners(tmp_path) == []

    def test_list_skips_unparseable_pidfiles(self, tmp_path):
        """Unparseable pidfiles are skipped, valid ones returned."""
        self._write_pidfile(tmp_path, "live00001", os.getpid())
        pidfile_path(tmp_path, "bad000002").write_text("invalid json")
        result = list_running_listeners(tmp_path)
        assert len(result) == 1
        assert result[0].run_id == "live00001"


class TestResolveListenerTarget:
    """Tests for resolve_listener_target."""

    def _write_pidfile(self, state_dir: Path, run_id: str, pid: int):
        data = {
            "pid": pid,
            "run_id": run_id,
            "topics": ["orders.created"],
            "group": f"agctl-listen-{run_id}",
            "cluster": "prod",
            "started_at": "2026-07-15T09:00:00Z",
            "state_dir": str(state_dir),
            "log_path": str(run_dir(state_dir, run_id) / "events.log"),
        }
        write_pidfile(pidfile_path(state_dir, run_id), data)

    def test_singleton_one_running_returns_it(self, tmp_path):
        """No selector with exactly one running listener returns that listener."""
        self._write_pidfile(tmp_path, "run00001", os.getpid())
        result = resolve_listener_target(tmp_path, run_id=None, pid=None, all_=False)
        assert len(result) == 1
        assert result[0].run_id == "run00001"

    def test_no_selector_two_running_raises(self, tmp_path):
        """No selector with two running listeners raises ConfigError."""
        self._write_pidfile(tmp_path, "run00001", os.getpid())
        self._write_pidfile(tmp_path, "run00002", os.getpid())
        with pytest.raises(ConfigError) as exc_info:
            resolve_listener_target(tmp_path, run_id=None, pid=None, all_=False)
        assert "multiple" in str(exc_info.value).lower()

    def test_all_returns_all(self, tmp_path):
        """all_=True returns every running listener."""
        self._write_pidfile(tmp_path, "run00001", os.getpid())
        self._write_pidfile(tmp_path, "run00002", os.getpid())
        result = resolve_listener_target(tmp_path, run_id=None, pid=None, all_=True)
        assert {r.run_id for r in result} == {"run00001", "run00002"}

    def test_run_id_selector_matches(self, tmp_path):
        """run_id selector returns the matching listener."""
        self._write_pidfile(tmp_path, "run00001", os.getpid())
        self._write_pidfile(tmp_path, "run00002", os.getpid())
        result = resolve_listener_target(tmp_path, run_id="run00002", pid=None, all_=False)
        assert len(result) == 1
        assert result[0].run_id == "run00002"

    def test_run_id_selector_matches_nothing_raises(self, tmp_path):
        """run_id selector that matches nothing raises ConfigError."""
        self._write_pidfile(tmp_path, "run00001", os.getpid())
        with pytest.raises(ConfigError):
            resolve_listener_target(tmp_path, run_id="nope0000", pid=None, all_=False)

    def test_pid_selector_matches(self, tmp_path):
        """pid selector returns the matching listener."""
        self._write_pidfile(tmp_path, "run00001", os.getpid())
        result = resolve_listener_target(tmp_path, run_id=None, pid=os.getpid(), all_=False)
        assert len(result) == 1
        assert result[0].pid == os.getpid()

    def test_pid_selector_matches_nothing_raises(self, tmp_path):
        """pid selector that matches nothing raises ConfigError."""
        self._write_pidfile(tmp_path, "run00001", os.getpid())
        with pytest.raises(ConfigError):
            resolve_listener_target(tmp_path, run_id=None, pid=999_999, all_=False)

    def test_no_selector_zero_running_returns_empty(self, tmp_path):
        """No selector with zero running returns empty list."""
        assert resolve_listener_target(tmp_path, run_id=None, pid=None, all_=False) == []


class TestParseEventsLog:
    """Tests for parse_events_log."""

    def test_parse_happy_path(self, tmp_path):
        """parse_events_log populates started/overflow_topics/summary/startup_error."""
        log_file = tmp_path / "events.log"
        lines = [
            '{"event":"started","topics":["T"],"group":"agctl-listen-abcd1234","cluster":"prod"}',
            '{"event":"capture.overflow","topic":"T","bytes":1048576}',
            '{"event":"kafka.error","message":"delivery failed"}',
            '{"event":"summary","captured":17,"duration_ms":5000}',
            '{"ok":false,"command":"kafka.listen.run","error":{"type":"ConnectionError","message":"no broker"}}',
        ]
        log_file.write_text("\n".join(lines))

        parsed = parse_events_log(log_file)

        assert isinstance(parsed, ParsedEvents)
        assert parsed.started == {
            "event": "started",
            "topics": ["T"],
            "group": "agctl-listen-abcd1234",
            "cluster": "prod",
        }
        assert parsed.overflow_topics == ["T"]
        assert parsed.summary == {
            "event": "summary",
            "captured": 17,
            "duration_ms": 5000,
        }
        assert parsed.startup_error == {
            "ok": False,
            "command": "kafka.listen.run",
            "error": {"type": "ConnectionError", "message": "no broker"},
        }
        # kafka.error collected into errors.
        assert len(parsed.errors) == 1
        assert parsed.errors[0]["event"] == "kafka.error"

    def test_parse_missing_file_returns_empty(self, tmp_path):
        """Missing events.log returns an empty ParsedEvents."""
        parsed = parse_events_log(tmp_path / "does-not-exist.log")
        assert parsed == ParsedEvents(
            started=None,
            startup_error=None,
            summary=None,
            overflow_topics=[],
            errors=[],
        )

    def test_parse_skips_blank_and_unparseable(self, tmp_path):
        """Blank and unparseable lines are skipped."""
        log_file = tmp_path / "events.log"
        log_file.write_text(
            "\n".join(
                [
                    "",
                    "not json",
                    '{"event":"started","ok":true}',
                    "   ",
                ]
            )
        )
        parsed = parse_events_log(log_file)
        assert parsed.started == {"event": "started", "ok": True}
        assert parsed.overflow_topics == []
        assert parsed.errors == []
        assert parsed.startup_error is None
        assert parsed.summary is None

    def test_parse_multiple_overflow_topics_preserves_order(self, tmp_path):
        """Multiple capture.overflow lines append topics in order."""
        log_file = tmp_path / "events.log"
        log_file.write_text(
            "\n".join(
                [
                    '{"event":"capture.overflow","topic":"A"}',
                    '{"event":"capture.overflow","topic":"B"}',
                    '{"event":"capture.overflow","topic":"A"}',
                ]
            )
        )
        parsed = parse_events_log(log_file)
        # Each overflow line appends its topic (duplicates kept).
        assert parsed.overflow_topics == ["A", "B", "A"]
