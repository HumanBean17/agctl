"""Tests for LogBackend protocol and canonical DTOs."""

import threading
import time
from datetime import datetime

from agctl.clients.log_backend_protocol import (
    AwaitResult,
    CanonicalEntry,
    LogBackend,
    LogFilter,
    ScanResult,
    SchemaDescriptor,
)
from agctl.config.models import LogSource


def test_canonical_entry_required_fields():
    """CanonicalEntry constructs with required fields; defaults for optionals."""
    entry = CanonicalEntry(
        timestamp="2024-01-01T00:00:00Z",
        level="ERROR",
        logger="my.app",
        message="Something failed",
    )
    assert entry.timestamp == "2024-01-01T00:00:00Z"
    assert entry.level == "ERROR"
    assert entry.logger == "my.app"
    assert entry.message == "Something failed"
    assert entry.thread is None
    assert entry.service is None
    assert entry.stack_trace is None
    assert entry.tags is None
    assert entry.fields == {}


def test_log_filter_defaults_all_none():
    """LogFilter() has all filter fields None and params == {}."""
    filt = LogFilter()
    assert filt.level is None
    assert filt.logger_glob is None
    assert filt.message_substring is None
    assert filt.match_jq is None
    assert filt.params == {}


def test_scanresult_and_awaitresult_hold_fields():
    """ScanResult and AwaitResult hold their field values."""
    scan_result = ScanResult(
        entries=[], matched=0, scanned=5, truncated=False
    )
    assert scan_result.entries == []
    assert scan_result.matched == 0
    assert scan_result.scanned == 5
    assert scan_result.truncated is False

    await_result = AwaitResult(entry=None, scanned=5, elapsed_ms=12)
    assert await_result.entry is None
    assert await_result.scanned == 5
    assert await_result.elapsed_ms == 12


def test_log_backend_is_protocol():
    """LogBackend is a runtime_checkable Protocol; structural check works."""
    class _Fake:
        """Minimal implementation satisfying LogBackend protocol."""

        def validate_config(self) -> None:
            pass

        def scan(
            self,
            filt: LogFilter,
            *,
            since: datetime | None,
            until: datetime | None,
            limit: int,
            tail_lines: int,
        ) -> ScanResult:
            return ScanResult(entries=[], matched=0, scanned=0, truncated=False)

        def await_one(
            self,
            filt: LogFilter,
            *,
            since: datetime | None,
            timeout_s: float,
            poll_interval_ms: int,
        ) -> AwaitResult:
            return AwaitResult(entry=None, scanned=0, elapsed_ms=0)

        def follow(
            self, filt: LogFilter, *, stop_event: threading.Event, poll_interval_ms: int
        ):
            return
            yield  # pragma: no cover - make it a generator

        def sample_schema(
            self, *, sample_lines: int = 100
        ) -> SchemaDescriptor:
            return SchemaDescriptor(
                standard=[], conditional=[], observed=[]
            )

    fake = _Fake()
    assert isinstance(fake, LogBackend) is True
    assert isinstance(object(), LogBackend) is False


# ============================================================================
# Task 3: NdjsonFileBackend - normalizer tests
# ============================================================================


def test_normalize_maps_logstash_slots():
    """_normalize maps logstash fields to canonical, non-slots to fields."""
    # Import here to avoid ImportError if module doesn't exist yet
    from agctl.clients.log_backends.ndjson_file import NdjsonFileBackend

    source = LogSource(path="/tmp/test.log", format="logstash")
    backend = NdjsonFileBackend(source)

    raw = {
        "@timestamp": "2026-07-08T10:00:00Z",
        "level": "info",
        "logger_name": "c.Foo",
        "thread_name": "t1",
        "message": "hi",
        "service": "svc",
        "@version": "1",
        "level_value": 20000,
        "orderId": "ord-1",
    }

    entry = backend._normalize(raw)

    assert entry.timestamp == "2026-07-08T10:00:00Z"
    assert entry.level == "INFO"  # UPPER-normalized
    assert entry.logger == "c.Foo"
    assert entry.message == "hi"
    assert entry.thread == "t1"
    assert entry.service == "svc"
    assert entry.stack_trace is None
    assert entry.tags is None
    # Non-slot keys go to fields
    assert entry.fields == {
        "@version": "1",
        "level_value": 20000,
        "orderId": "ord-1",
    }


def test_normalize_stack_trace_and_tags():
    """_normalize populates stack_trace and tags slots, excludes from fields."""
    from agctl.clients.log_backends.ndjson_file import NdjsonFileBackend

    source = LogSource(path="/tmp/test.log", format="logstash")
    backend = NdjsonFileBackend(source)

    raw = {
        "@timestamp": "2026-07-08T10:00:00Z",
        "level": "ERROR",
        "logger_name": "c.Foo",
        "message": "failed",
        "stack_trace": "java.lang.RuntimeException: oops\n\tat Foo.java:42",
        "tags": ["hot", "critical"],
        "orderId": "ord-2",
    }

    entry = backend._normalize(raw)

    assert entry.stack_trace == "java.lang.RuntimeException: oops\n\tat Foo.java:42"
    assert entry.tags == ["hot", "critical"]
    # Tags and stack_trace are NOT in fields
    assert entry.fields == {"orderId": "ord-2"}


def test_normalize_missing_fields_are_none():
    """_normalize handles empty/missing input with defaults."""
    from agctl.clients.log_backends.ndjson_file import NdjsonFileBackend

    source = LogSource(path="/tmp/test.log", format="logstash")
    backend = NdjsonFileBackend(source)

    raw = {}

    entry = backend._normalize(raw)

    assert entry.timestamp is None
    assert entry.level == ""  # Missing level -> empty string after upper()
    assert entry.logger is None
    assert entry.message is None
    assert entry.thread is None
    assert entry.service is None
    assert entry.stack_trace is None
    assert entry.tags is None
    assert entry.fields == {}


# ============================================================================
# Task 3: NdjsonFileBackend - scan tests
# ============================================================================


def test_scan_missing_file_is_empty(tmp_path):
    """scan returns empty ScanResult for non-existent file (no exception)."""
    from agctl.clients.log_backends.ndjson_file import NdjsonFileBackend

    source = LogSource(path=str(tmp_path / "nope.log"), format="logstash")
    backend = NdjsonFileBackend(source)

    result = backend.scan(
        LogFilter(), since=None, until=None, limit=50, tail_lines=200
    )

    assert result.entries == []
    assert result.matched == 0
    assert result.scanned == 0
    assert result.truncated is False


def test_scan_reads_tail_and_filters_level(tmp_path):
    """scan reads all lines from tail and filters by level."""
    from agctl.clients.log_backends.ndjson_file import NdjsonFileBackend

    log_file = tmp_path / "test.log"
    lines = [
        '{"@timestamp":"2026-07-08T10:00:00Z","level":"ERROR","logger_name":"c.Foo","message":"err1"}',
        '{"@timestamp":"2026-07-08T10:00:01Z","level":"INFO","logger_name":"c.Foo","message":"info1"}',
        '{"@timestamp":"2026-07-08T10:00:02Z","level":"ERROR","logger_name":"c.Foo","message":"err2"}',
        '{"@timestamp":"2026-07-08T10:00:03Z","level":"INFO","logger_name":"c.Foo","message":"info2"}',
        '{"@timestamp":"2026-07-08T10:00:04Z","level":"INFO","logger_name":"c.Foo","message":"info3"}',
    ]
    log_file.write_text("\n".join(lines))

    source = LogSource(path=str(log_file), format="logstash")
    backend = NdjsonFileBackend(source)

    result = backend.scan(
        LogFilter(level="ERROR"), since=None, until=None, limit=50, tail_lines=200
    )

    assert result.matched == 2
    assert result.scanned == 5
    assert result.truncated is False
    assert len(result.entries) == 2
    assert all(e.level == "ERROR" for e in result.entries)
    assert result.entries[0].message == "err1"
    assert result.entries[1].message == "err2"


def test_scan_skips_non_json_lines(tmp_path, capsys):
    """scan skips non-JSON lines with stderr message (no exception)."""
    from agctl.clients.log_backends.ndjson_file import NdjsonFileBackend

    log_file = tmp_path / "test.log"
    lines = [
        '{"@timestamp":"2026-07-08T10:00:00Z","level":"INFO","logger_name":"c.Foo","message":"valid1"}',
        "this is not json",
        '{"@timestamp":"2026-07-08T10:00:01Z","level":"INFO","logger_name":"c.Foo","message":"valid2"}',
    ]
    log_file.write_text("\n".join(lines))

    source = LogSource(path=str(log_file), format="logstash")
    backend = NdjsonFileBackend(source)

    result = backend.scan(
        LogFilter(), since=None, until=None, limit=50, tail_lines=200
    )

    assert result.scanned == 2  # Only the 2 valid JSON lines
    assert result.matched == 2
    captured = capsys.readouterr()
    assert "skipping non-JSON log line" in captured.err


def test_scan_logger_glob_and_message(tmp_path):
    """scan filters by logger glob and message substring (AND logic)."""
    from agctl.clients.log_backends.ndjson_file import NdjsonFileBackend

    log_file = tmp_path / "test.log"
    lines = [
        '{"@timestamp":"2026-07-08T10:00:00Z","level":"INFO","logger_name":"com.myco.order.Svc","message":"Order persisted"}',
        '{"@timestamp":"2026-07-08T10:00:01Z","level":"INFO","logger_name":"com.myco.order.Svc","message":"Order failed"}',
        '{"@timestamp":"2026-07-08T10:00:02Z","level":"INFO","logger_name":"com.other.Svc","message":"Order persisted"}',
    ]
    log_file.write_text("\n".join(lines))

    source = LogSource(path=str(log_file), format="logstash")
    backend = NdjsonFileBackend(source)

    # Should match only the first line (both glob and message)
    result = backend.scan(
        LogFilter(logger_glob="com.myco.order.*", message_substring="persisted"),
        since=None,
        until=None,
        limit=50,
        tail_lines=200,
    )

    assert result.matched == 1
    assert result.scanned == 3
    assert result.entries[0].message == "Order persisted"
    assert result.entries[0].logger == "com.myco.order.Svc"

    # Different glob should not match
    result2 = backend.scan(
        LogFilter(logger_glob="com.other.*"),
        since=None,
        until=None,
        limit=50,
        tail_lines=200,
    )

    assert result2.matched == 1
    assert result2.entries[0].logger == "com.other.Svc"


def test_scan_match_jq_on_fields(tmp_path):
    """scan filters by jq predicate against canonical entry (fields accessible)."""
    from agctl.clients.log_backends.ndjson_file import NdjsonFileBackend

    log_file = tmp_path / "test.log"
    lines = [
        '{"@timestamp":"2026-07-08T10:00:00Z","level":"INFO","logger_name":"c.Foo","message":"msg","orderId":"ord-9"}',
        '{"@timestamp":"2026-07-08T10:00:01Z","level":"INFO","logger_name":"c.Foo","message":"msg","orderId":"ord-1"}',
    ]
    log_file.write_text("\n".join(lines))

    source = LogSource(path=str(log_file), format="logstash")
    backend = NdjsonFileBackend(source)

    # Match on fields.orderId
    result = backend.scan(
        LogFilter(match_jq='.fields.orderId == "ord-9"'),
        since=None,
        until=None,
        limit=50,
        tail_lines=200,
    )

    assert result.matched == 1
    assert result.entries[0].fields["orderId"] == "ord-9"

    # Different orderId should not match
    result2 = backend.scan(
        LogFilter(match_jq='.fields.orderId == "other"'),
        since=None,
        until=None,
        limit=50,
        tail_lines=200,
    )

    assert result2.matched == 0


def test_scan_window_since_until(tmp_path):
    """scan applies time window bounds via _parse_iso_datetime/_to_utc."""
    from agctl.clients.log_backends.ndjson_file import NdjsonFileBackend
    from datetime import timedelta, timezone

    log_file = tmp_path / "test.log"
    # Two timestamps 10 minutes apart
    lines = [
        '{"@timestamp":"2026-07-08T10:00:00Z","level":"INFO","logger_name":"c.Foo","message":"old"}',
        '{"@timestamp":"2026-07-08T10:10:00Z","level":"INFO","logger_name":"c.Foo","message":"new"}',
    ]
    log_file.write_text("\n".join(lines))

    source = LogSource(path=str(log_file), format="logstash")
    backend = NdjsonFileBackend(source)

    # Window bounds only the recent entry
    since = datetime(2026, 7, 8, 10, 5, 0, tzinfo=timezone.utc)
    until = datetime(2026, 7, 8, 10, 15, 0, tzinfo=timezone.utc)

    result = backend.scan(
        LogFilter(), since=since, until=until, limit=50, tail_lines=200
    )

    assert result.matched == 1
    assert result.scanned == 1
    assert result.entries[0].message == "new"


def test_scan_limit_truncates(tmp_path):
    """scan returns first limit matches with truncation flag."""
    from agctl.clients.log_backends.ndjson_file import NdjsonFileBackend

    log_file = tmp_path / "test.log"
    lines = [
        f'{{"@timestamp":"2026-07-08T10:00:0{i}Z","level":"INFO","logger_name":"c.Foo","message":"msg{i}"}}'
        for i in range(5)
    ]
    log_file.write_text("\n".join(lines))

    source = LogSource(path=str(log_file), format="logstash")
    backend = NdjsonFileBackend(source)

    result = backend.scan(
        LogFilter(), since=None, until=None, limit=2, tail_lines=200
    )

    assert result.matched == 5
    assert result.scanned == 5
    assert len(result.entries) == 2  # Only first 2
    assert result.truncated is True
    assert result.entries[0].message == "msg0"
    assert result.entries[1].message == "msg1"


def test_scan_tail_lines_bounds_read(tmp_path):
    """scan reads only the last tail_lines from the file."""
    from agctl.clients.log_backends.ndjson_file import NdjsonFileBackend

    log_file = tmp_path / "test.log"
    lines = [
        f'{{"@timestamp":"2026-07-08T10:00:0{i}Z","level":"INFO","logger_name":"c.Foo","message":"msg{i}"}}'
        for i in range(10)
    ]
    log_file.write_text("\n".join(lines))

    source = LogSource(path=str(log_file), format="logstash")
    backend = NdjsonFileBackend(source)

    result = backend.scan(
        LogFilter(), since=None, until=None, limit=50, tail_lines=3
    )

    # Only the last 3 lines should be considered
    assert result.scanned <= 3
    assert result.matched <= 3


def test_scan_tail_lines_long_lines(tmp_path):
    """scan handles long lines (300-800 bytes) via loop-growing read window."""
    import json
    from agctl.clients.log_backends.ndjson_file import NdjsonFileBackend

    log_file = tmp_path / "test.log"

    # Generate 5 NDJSON lines, each ~400 bytes (pad message + add fields)
    lines = []
    for i in range(5):
        # Create a proper long message (300 characters)
        long_message = "x" * 300
        # Build valid JSON object with multiple fields to reach ~400 bytes
        line_obj = {
            "@timestamp": f"2026-07-08T10:00:0{i}Z",
            "level": "INFO",
            "logger_name": f"com.example.service.Service{i}",
            "message": long_message,
            "field1": "value1",
            "field2": "value2",
            "field3": "value3",
            "orderId": f"ord-{i}",
            "userId": f"user-{i}",
            "requestId": f"req-{i}",
            "sessionId": f"sess-{i}",
        }
        line = json.dumps(line_obj)
        lines.append(line)

    log_file.write_text("\n".join(lines))

    source = LogSource(path=str(log_file), format="logstash")
    backend = NdjsonFileBackend(source)

    # Request last 3 lines - should correctly capture them despite long lines
    result = backend.scan(
        LogFilter(), since=None, until=None, limit=50, tail_lines=3
    )

    # Should scan at most 3 lines (the last 3)
    assert result.scanned == 3
    assert result.matched == 3

    # The last 3 lines (indices 2, 3, 4) should be in the results
    messages = [e.message for e in result.entries]
    assert all(len(msg) == 300 for msg in messages)  # All 300-char messages
    assert all(msg == "x" * 300 for msg in messages)  # All are the long message
    assert len(messages) == 3


# ============================================================================
# Task 4: NdjsonFileBackend - await_one tests
# ============================================================================


def test_await_one_shot_match(tmp_path):
    """await_one in one-shot mode (timeout_s=0) returns matching entry."""
    from agctl.clients.log_backends.ndjson_file import NdjsonFileBackend

    log_file = tmp_path / "test.log"
    lines = [
        '{"@timestamp":"2026-07-07T10:00:00Z","level":"INFO","logger_name":"c.Foo","message":"info"}',
        '{"@timestamp":"2026-07-07T10:00:01Z","level":"ERROR","logger_name":"c.Foo","message":"err1"}',
        '{"@timestamp":"2026-07-07T10:00:02Z","level":"ERROR","logger_name":"c.Foo","message":"err2"}',
    ]
    log_file.write_text("\n".join(lines))

    source = LogSource(path=str(log_file), format="logstash")
    backend = NdjsonFileBackend(source)

    result = backend.await_one(
        LogFilter(level="ERROR"), since=None, timeout_s=0, poll_interval_ms=100
    )

    assert result.entry is not None
    assert result.entry.level == "ERROR"
    assert result.scanned > 0
    assert result.elapsed_ms >= 0


def test_await_one_shot_no_match(tmp_path):
    """await_one in one-shot mode returns None when no match."""
    from agctl.clients.log_backends.ndjson_file import NdjsonFileBackend

    log_file = tmp_path / "test.log"
    lines = [
        '{"@timestamp":"2026-07-07T10:00:00Z","level":"INFO","logger_name":"c.Foo","message":"info"}',
        '{"@timestamp":"2026-07-07T10:00:01Z","level":"INFO","logger_name":"c.Foo","message":"info2"}',
    ]
    log_file.write_text("\n".join(lines))

    source = LogSource(path=str(log_file), format="logstash")
    backend = NdjsonFileBackend(source)

    result = backend.await_one(
        LogFilter(level="ERROR"), since=None, timeout_s=0, poll_interval_ms=100
    )

    assert result.entry is None
    assert result.scanned > 0
    assert result.elapsed_ms >= 0


def test_await_one_poll_finds_after_delay(tmp_path):
    """await_one polling mode finds entry that appears after initial read."""
    from agctl.clients.log_backends.ndjson_file import NdjsonFileBackend

    log_file = tmp_path / "test.log"

    # Start with empty file
    log_file.write_text("")

    source = LogSource(path=str(log_file), format="logstash")

    # Fake monotonic that advances: start=0.0 -> first check=0.05 -> sleep -> second check=0.15 (timeout at 0.2)
    monotonic_calls = [0.0, 0.05, 0.15]

    def fake_monotonic():
        return monotonic_calls.pop(0)

    # Fake sleep that appends a line after first poll
    sleep_calls = []

    def fake_sleep(seconds):
        sleep_calls.append(seconds)
        # After first sleep, append matching line
        if len(sleep_calls) == 1:
            existing = log_file.read_text()
            log_file.write_text(
                existing
                + '\n{"@timestamp":"2026-07-07T10:00:01Z","level":"ERROR","logger_name":"c.Foo","message":"err1"}'
            )

    # Inject fakes via __init__ kwargs
    backend = NdjsonFileBackend(source, monotonic=fake_monotonic, sleep=fake_sleep)

    result = backend.await_one(
        LogFilter(level="ERROR"), since=None, timeout_s=0.2, poll_interval_ms=100
    )

    # Should have found the entry after the second poll
    assert result.entry is not None
    assert result.entry.level == "ERROR"
    assert result.entry.message == "err1"
    assert result.scanned > 0
    assert result.elapsed_ms >= 50  # At least one sleep cycle
    assert len(sleep_calls) >= 1  # Slept at least once


def test_await_one_poll_times_out(tmp_path):
    """await_one polling mode times out when no match appears."""
    from agctl.clients.log_backends.ndjson_file import NdjsonFileBackend

    log_file = tmp_path / "test.log"
    lines = [
        '{"@timestamp":"2026-07-07T10:00:00Z","level":"INFO","logger_name":"c.Foo","message":"info"}',
    ]
    log_file.write_text("\n".join(lines))

    source = LogSource(path=str(log_file), format="logstash")

    # Use real time but short timeout
    backend = NdjsonFileBackend(source)

    result = backend.await_one(
        LogFilter(level="ERROR"), since=None, timeout_s=0.1, poll_interval_ms=50
    )

    assert result.entry is None
    assert result.scanned > 0
    assert result.elapsed_ms >= 100  # ~100ms elapsed


def test_await_one_cumulative_scanned(tmp_path):
    """await_one accumulates scanned count across poll iterations."""
    from agctl.clients.log_backends.ndjson_file import NdjsonFileBackend

    log_file = tmp_path / "test.log"

    # Start with non-matching lines
    lines = [
        '{"@timestamp":"2026-07-07T10:00:00Z","level":"INFO","logger_name":"c.Foo","message":"info1"}',
        '{"@timestamp":"2026-07-07T10:00:01Z","level":"INFO","logger_name":"c.Foo","message":"info2"}',
        '{"@timestamp":"2026-07-07T10:00:02Z","level":"INFO","logger_name":"c.Foo","message":"info3"}',
    ]
    log_file.write_text("\n".join(lines))

    source = LogSource(path=str(log_file), format="logstash")

    # Fake monotonic: start=0.0 -> first check=0.05 -> sleep -> second check=0.15 (timeout at 0.2)
    monotonic_calls = [0.0, 0.05, 0.15]

    def fake_monotonic():
        return monotonic_calls.pop(0)

    # Fake sleep that appends matching line after first sleep
    sleep_calls = []

    def fake_sleep(seconds):
        sleep_calls.append(seconds)
        if len(sleep_calls) == 1:
            # Append more non-matching + matching line
            existing = log_file.read_text()
            log_file.write_text(
                existing
                + '\n{"@timestamp":"2026-07-07T10:00:03Z","level":"INFO","logger_name":"c.Foo","message":"info4"}'
                + '\n{"@timestamp":"2026-07-07T10:00:04Z","level":"ERROR","logger_name":"c.Foo","message":"err1"}'
            )

    backend = NdjsonFileBackend(source, monotonic=fake_monotonic, sleep=fake_sleep)

    result = backend.await_one(
        LogFilter(level="ERROR"), since=None, timeout_s=0.2, poll_interval_ms=100
    )

    assert result.entry is not None
    assert result.entry.level == "ERROR"
    # First poll scanned 3, second poll scanned 2 = total 5
    assert result.scanned >= 5


# ============================================================================
# Task 4: NdjsonFileBackend - sample_schema tests
# ============================================================================


def test_sample_schema_missing_file_empty(tmp_path):
    """sample_schema returns empty lists when file is missing."""
    from agctl.clients.log_backends.ndjson_file import NdjsonFileBackend

    # Non-existent file
    source = LogSource(path=str(tmp_path / "nope.log"), format="logstash")
    backend = NdjsonFileBackend(source)

    result = backend.sample_schema(sample_lines=100)

    assert result.standard == []
    assert result.conditional == []
    assert result.observed == []


def test_sample_schema_enumerates(tmp_path):
    """sample_schema enumerates standard/conditional/observed fields correctly."""
    from agctl.clients.log_backends.ndjson_file import NdjsonFileBackend

    log_file = tmp_path / "test.log"
    lines = [
        # Line with standard fields + orderId + @version
        '{"@timestamp":"2026-07-07T10:00:00Z","level":"INFO","logger_name":"c.Foo","message":"msg1","orderId":"ord-1","@version":"1"}',
        # Line with standard fields + status + level_value + stack_trace
        '{"@timestamp":"2026-07-07T10:00:01Z","level":"ERROR","logger_name":"c.Foo","message":"msg2","status":"failed","level_value":40000,"stack_trace":"err"}',
        # Line with standard fields + tags
        '{"@timestamp":"2026-07-07T10:00:02Z","level":"INFO","logger_name":"c.Bar","message":"msg3","tags":["tag1"]}',
    ]
    log_file.write_text("\n".join(lines))

    source = LogSource(path=str(log_file), format="logstash")
    backend = NdjsonFileBackend(source)

    result = backend.sample_schema(sample_lines=100)

    # standard: from predefined set, non-None/non-empty, sorted
    # Should include timestamp, level, logger, message (always present if parsed)
    assert "timestamp" in result.standard
    assert "level" in result.standard
    assert "logger" in result.standard
    assert "message" in result.standard
    # thread, service not present in any entry -> not in standard

    # conditional: subset of stack_trace, tags seen non-None, sorted
    assert "stack_trace" in result.conditional
    assert "tags" in result.conditional

    # observed: union of all keys in fields, EXCLUDING @version and level_value
    assert "orderId" in result.observed
    assert "status" in result.observed
    # Well-known noise should be excluded
    assert "@version" not in result.observed
    assert "level_value" not in result.observed


# ============================================================================
# Task 5: NdjsonFileBackend - follow tests
# ============================================================================


def test_follow_yields_new_matches_then_stops(tmp_path):
    """follow yields new matching entries as file grows, stops when stop_event set."""
    import os
    import threading

    from agctl.clients.log_backends.ndjson_file import NdjsonFileBackend

    log_file = tmp_path / "test.log"
    # Start with one INFO line (not matched by ERROR filter) followed by ERROR line
    # IMPORTANT: End with newline so the last line is processed, not buffered
    initial_line = (
        '{"@timestamp":"2026-07-08T10:00:00Z","level":"INFO","logger_name":"c.Foo",'
        '"message":"info1"}'
    )
    error_line = (
        '\n{"@timestamp":"2026-07-08T10:00:01Z","level":"ERROR",'
        '"logger_name":"c.Foo","message":"err1"}\n'
    )
    full_content = initial_line + error_line
    log_file.write_text(full_content)

    source = LogSource(path=str(log_file), format="logstash")
    stop_event = threading.Event()

    # Fake stat that simulates file growth: starts at 0 (file empty, nothing read yet),
    # then grows to full size (simulating new data appearing)
    stat_call_count = [0]
    sizes = [0, len(full_content)]  # 0 → full size (growth detected)

    def fake_stat(path):
        stat_call_count[0] += 1
        idx = min(stat_call_count[0] - 1, len(sizes) - 1)
        return os.stat_result((33188, 12345, 123, 1, 1000, 1000, sizes[idx], 0, 0, 0))

    # Cooperative wait: sets stop_event after 2 calls to ensure bounded termination
    # Flow: stat(size=0, no growth from last_offset=0) → wait → stat(size=full, growth!) → yield ERROR → check stop_event (not set) → wait sets stop_event
    wait_call_count = [0]

    def cooperative_wait(event, timeout):
        wait_call_count[0] += 1
        # Set stop_event after 2 calls (allows yield to happen first, then termination)
        if wait_call_count[0] >= 2:
            event.set()
            return True  # Event was set during wait
        return False  # Continue looping

    # Inject fakes
    backend = NdjsonFileBackend(source, stat_fn=fake_stat, _wait=cooperative_wait)

    # Create generator
    gen = backend.follow(
        LogFilter(level="ERROR"), stop_event=stop_event, poll_interval_ms=10
    )

    # First iteration: should yield the ERROR entry
    # The generator will: stat(size=0, no growth) → wait → stat(size=full, growth!) → read → yield ERROR
    entry = next(gen, None)
    assert entry is not None, "Should yield ERROR entry"
    assert entry.level == "ERROR"
    assert entry.message == "err1"

    # After yielding, the generator waits and cooperative_wait sets stop_event
    # Second iteration: generator should exit (StopIteration)
    entry = next(gen, None)
    assert entry is None, "Generator should stop after stop_event is set"

    # Should have called stat at least once
    assert stat_call_count[0] >= 1


def test_follow_missing_file_waits(tmp_path):
    """follow waits for missing file, yields nothing, returns when stop_event set."""
    import threading

    from agctl.clients.log_backends.ndjson_file import NdjsonFileBackend

    # Non-existent file
    log_file = tmp_path / "does_not_exist.log"

    source = LogSource(path=str(log_file), format="logstash")
    stop_event = threading.Event()

    # Fake stat that always returns FileNotFoundError (file never appears)
    stat_call_count = [0]

    def fake_stat(path):
        stat_call_count[0] += 1
        raise FileNotFoundError(f"Mock missing: {path}")

    # Cooperative wait: sets stop_event after 2 calls to ensure bounded termination
    # The generator will: stat → FileNotFoundError → wait → stat → FileNotFoundError → wait sets stop_event → return
    wait_call_count = [0]

    def cooperative_wait(event, timeout):
        wait_call_count[0] += 1
        # Set stop_event after 2 calls (2 iterations → self-terminate)
        if wait_call_count[0] >= 2:
            event.set()
            return True  # Event was set during wait
        return False  # Continue looping

    backend = NdjsonFileBackend(source, stat_fn=fake_stat, _wait=cooperative_wait)

    # Create generator
    gen = backend.follow(LogFilter(), stop_event=stop_event, poll_interval_ms=10)

    # First iteration: file doesn't exist, should yield nothing
    # The cooperative wait ensures bounded iterations before setting stop_event
    entry = next(gen, None)
    assert entry is None, "Should yield nothing when file is missing"

    # Should have attempted stat at least once and waited at least once
    assert stat_call_count[0] >= 1
    assert wait_call_count[0] >= 1

    # Verify stop_event was set (cooperative wait did its job)
    assert stop_event.is_set(), "Cooperative wait should have set stop_event"

    # Second iteration: generator should have exited (StopIteration)
    entry = next(gen, None)
    assert entry is None, "Generator should stop after stop_event is set"


# --- LogClient tests (Task 6) -----------------------------------------------


def test_log_client_selects_file_backend(tmp_path):
    """LogClient selects NdjsonFileBackend for type='file' and validates config."""
    from agctl.clients.log_client import LogClient
    from agctl.clients.log_backends.ndjson_file import NdjsonFileBackend

    log_file = tmp_path / "test.log"
    log_file.write_text('{"@timestamp":"2024-01-01T00:00:00Z","level":"INFO","message":"test"}\n')

    source = LogSource(type="file", path=str(log_file), format="logstash")
    client = LogClient(source)

    # Should have selected the file backend
    assert isinstance(client._backend, NdjsonFileBackend)

    # validate_config should return None (no error)
    assert client.validate_config() is None


def test_log_client_unknown_type_raises():
    """LogClient raises ConfigError for unknown backend type."""
    from agctl.clients.log_client import LogClient
    from agctl.errors import ConfigError

    source = LogSource(type="victoria", path=None)
    try:
        LogClient(source)
        assert False, "Should have raised ConfigError"
    except ConfigError as e:
        # Message should mention unknown type
        assert "Unknown logs backend type" in str(e)
        assert "victoria" in str(e)
        # Detail should include the type
        assert e.detail.get("type") == "victoria"


def test_log_client_missing_path_raises():
    """LogClient raises ConfigError when file backend missing path."""
    from agctl.clients.log_client import LogClient
    from agctl.errors import ConfigError

    # File type requires path
    source = LogSource(type="file", path=None)
    try:
        LogClient(source)
        assert False, "Should have raised ConfigError"
    except ConfigError as e:
        # Error should mention path requirement
        assert "path" in str(e).lower()


def test_log_client_injected_backend_skips_lookup():
    """LogClient uses injected backend directly, skipping load_backends."""
    from agctl.clients.log_client import LogClient
    from agctl.clients.log_backend_protocol import (
        LogBackend,
        ScanResult,
        CanonicalEntry,
    )

    # Create a fake backend
    class FakeBackend(LogBackend):
        def __init__(self, source):
            self.source = source

        def validate_config(self):
            return None

        def scan(self, filt, *, since, until, limit, tail_lines):
            # Return a sentinel ScanResult
            return ScanResult(
                entries=[CanonicalEntry(
                    timestamp="2024-01-01T00:00:00Z",
                    level="INFO",
                    logger="test",
                    message="sentinel"
                )],
                matched=1,
                scanned=1,
                truncated=False
            )

        def await_one(self, filt, *, since, timeout_s, poll_interval_ms):
            raise NotImplementedError()

        def follow(self, filt, *, stop_event, poll_interval_ms):
            raise NotImplementedError()

        def sample_schema(self, *, sample_lines=100):
            raise NotImplementedError()

    fake = FakeBackend(None)
    source = LogSource(type="file", path="/tmp/test.log")

    # Inject the backend
    client = LogClient(source, backend=fake)

    # Should use the injected backend, not load_backends
    assert client._backend is fake

    # Methods should delegate to the fake backend
    result = client.scan(
        filt=LogFilter(),
        since=None,
        until=None,
        limit=10,
        tail_lines=100
    )
    assert result.matched == 1
    assert result.entries[0].message == "sentinel"


def test_load_backends_includes_file_and_skips_broken():
    """LogClient.load_backends() includes 'file' and skips broken entry points."""
    from agctl.clients.log_client import LogClient
    from agctl.clients.log_backends.ndjson_file import NdjsonFileBackend
    import importlib.metadata

    # Monkeypatch entry_points to return a broken entry point
    original_entry_points = importlib.metadata.entry_points

    def broken_entry_points():
        # Return a fake entry point that raises on load
        class BrokenEntryPoint:
            name = "broken_backend"
            def load(self):
                raise RuntimeError("Broken backend")

        # Mock the select/get interface
        class MockGroup:
            def __iter__(self):
                return iter([BrokenEntryPoint()])

        class MockEPS:
            def select(self, group=None):
                return MockGroup()

        return MockEPS()

    importlib.metadata.entry_points = broken_entry_points

    try:
        # load_backends should not crash, should skip broken
        backends = LogClient.load_backends()

        # Should include the built-in 'file' backend
        assert "file" in backends
        assert backends["file"] is NdjsonFileBackend

        # Should not include the broken backend
        assert "broken_backend" not in backends
    finally:
        # Restore original
        importlib.metadata.entry_points = original_entry_points


def test_validates_logs_sources_in_config():
    """validate_config() checks logs.sources and surfaces missing-path errors."""
    from agctl.config.models import (
        Config,
        DatabaseConfig,
        Defaults,
        KafkaConfig,
        LogSource,
        LogsConfig,
    )
    from agctl.config.validator import validate_config

    # Build a config with a malformed logs source (missing path)
    cfg = Config.model_validate(
        dict(
            version="1",
            services={},
            kafka=KafkaConfig(),
            database=DatabaseConfig(),
            templates={},
            defaults=Defaults(),
            logs=LogsConfig(
                sources={
                    "svc": LogSource(type="file", path=None, format="logstash")
                }
            ),
        )
    )

    errors, warnings = validate_config(cfg)

    # Should have an error for logs.sources.svc
    log_errors = [e for e in errors if e["path"] == "logs.sources.svc"]
    assert len(log_errors) == 1, f"Expected 1 error for logs.sources.svc, got {len(log_errors)}"
    assert "path" in log_errors[0]["message"].lower()


def test_validates_logs_sources_with_wellformed_source():
    """validate_config() passes for well-formed logs sources."""
    from agctl.config.models import (
        Config,
        DatabaseConfig,
        Defaults,
        KafkaConfig,
        LogSource,
        LogsConfig,
    )
    from agctl.config.validator import validate_config

    # Build a config with a well-formed logs source
    cfg = Config.model_validate(
        dict(
            version="1",
            services={},
            kafka=KafkaConfig(),
            database=DatabaseConfig(),
            templates={},
            defaults=Defaults(),
            logs=LogsConfig(
                sources={
                    "svc": LogSource(type="file", path="/tmp/x.log", format="logstash")
                }
            ),
        )
    )

    errors, warnings = validate_config(cfg)

    # Should have no error for logs.sources.svc
    log_errors = [e for e in errors if e["path"] == "logs.sources.svc"]
    assert len(log_errors) == 0, f"Expected no error for logs.sources.svc, got {log_errors}"
