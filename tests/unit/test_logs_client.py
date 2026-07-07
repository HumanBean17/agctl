"""Tests for LogBackend protocol and canonical DTOs."""

import threading
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
            self, filt: LogFilter, *, stop_event: threading.Event
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


def test_scan_skips_non_json_lines(tmp_path):
    """scan skips non-JSON lines with stderr message (no exception)."""
    from agctl.clients.log_backends.ndjson_file import NdjsonFileBackend
    import sys

    log_file = tmp_path / "test.log"
    lines = [
        '{"@timestamp":"2026-07-08T10:00:00Z","level":"INFO","logger_name":"c.Foo","message":"valid1"}',
        "this is not json",
        '{"@timestamp":"2026-07-08T10:00:01Z","level":"INFO","logger_name":"c.Foo","message":"valid2"}',
    ]
    log_file.write_text("\n".join(lines))

    source = LogSource(path=str(log_file), format="logstash")
    backend = NdjsonFileBackend(source)

    # Capture stderr to check for skip message
    import io
    old_stderr = sys.stderr
    sys.stderr = io.StringIO()

    try:
        result = backend.scan(
            LogFilter(), since=None, until=None, limit=50, tail_lines=200
        )
    finally:
        stderr_output = sys.stderr.getvalue()
        sys.stderr = old_stderr

    assert result.scanned == 2  # Only the 2 valid JSON lines
    assert result.matched == 2
    assert "skipping non-JSON log line" in stderr_output


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
