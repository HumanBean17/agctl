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
