"""NDJSON file backend for logstash-formatted logs (DESIGN §9.2)."""

import dataclasses
import fnmatch
import json
import sys
from datetime import datetime
from pathlib import Path

from agctl.assertions import _parse_iso_datetime, _to_utc, jq_bool
from agctl.clients.log_backend_protocol import (
    AwaitResult,
    CanonicalEntry,
    LogFilter,
    ScanResult,
    SchemaDescriptor,
)
from agctl.config.models import LogSource
from agctl.errors import ConfigError


_SLOT_SOURCE_SET = {
    "@timestamp",
    "level",
    "logger_name",
    "thread_name",
    "message",
    "service",
    "stack_trace",
    "tags",
}


class NdjsonFileBackend:
    """Log backend for local NDJSON files in logstash format.

    Consumes one NDJSON line per log entry, normalizes to CanonicalEntry,
    and applies client-side filters (level, logger glob, message substring,
    jq predicate).
    """

    def __init__(self, source: LogSource):
        """Store source config (no I/O at construction)."""
        self._path = source.path
        self._format = source.format

    def validate_config(self) -> None:
        """Raise ConfigError if path is None (file type requires path)."""
        if self._path is None:
            raise ConfigError(
                "logs source of type 'file' requires 'path'",
                {"type": "file"},
            )

    def _normalize(self, raw: dict) -> CanonicalEntry:
        """Map a parsed logstash JSON object to CanonicalEntry.

        Slot fields (timestamp, level, logger, thread, message, service,
        stack_trace, tags) are mapped directly. All other top-level keys
        (e.g. MDC, StructuredArguments, @version, level_value) go into
        the ``fields`` dict.
        """
        # Extract slot fields
        timestamp = raw.get("@timestamp")
        level = str(raw.get("level", "")).upper()
        logger = raw.get("logger_name")
        message = raw.get("message")
        thread = raw.get("thread_name")
        service = raw.get("service")
        stack_trace = raw.get("stack_trace")
        tags = raw.get("tags")

        # All other keys go to fields
        fields = {k: v for k, v in raw.items() if k not in _SLOT_SOURCE_SET}

        return CanonicalEntry(
            timestamp=timestamp,
            level=level,
            logger=logger,
            message=message,
            thread=thread,
            service=service,
            stack_trace=stack_trace,
            tags=tags,
            fields=fields,
        )

    def scan(
        self,
        filt: LogFilter,
        *,
        since: datetime | None,
        until: datetime | None,
        limit: int,
        tail_lines: int,
    ) -> ScanResult:
        """Scan last N lines of the file, applying window and filters.

        If the file does not exist, returns empty ScanResult (no error).
        Reads only the last ``tail_lines`` newline-terminated lines.
        Non-JSON lines are skipped (stderr message, no exception).
        Window bounds (since/until) are applied via _parse_iso_datetime.
        Filters are applied in AND order (level, logger glob, message substring,
        jq predicate). Returns up to ``limit`` matches with truncation flag.
        """
        if self._path is None:
            return ScanResult(entries=[], matched=0, scanned=0, truncated=False)
        path = Path(self._path)
        if not path.exists():
            return ScanResult(entries=[], matched=0, scanned=0, truncated=False)

        # Read last tail_lines from file (backward read without loading all)
        lines = self._tail_lines(path, tail_lines)

        matched = 0
        scanned = 0
        entries = []

        for line in lines:
            if not line.strip():
                continue

            try:
                raw = json.loads(line)
            except json.JSONDecodeError:
                print("agctl: skipping non-JSON log line", file=sys.stderr)
                continue

            entry = self._normalize(raw)

            # Window check
            if since is not None or until is not None:
                entry_ts = _parse_iso_datetime(entry.timestamp)
                if entry_ts is None:
                    # Unparseable timestamp: only keep if no bounds
                    continue
                else:
                    entry_ts_utc = _to_utc(entry_ts)
                    if since is not None and entry_ts_utc < since:
                        continue
                    if until is not None and entry_ts_utc > until:
                        continue

            scanned += 1

            # Apply filters (AND logic)
            if filt.level is not None:
                if entry.level != filt.level.upper():
                    continue

            if filt.logger_glob is not None:
                if not fnmatch.fnmatch(entry.logger or "", filt.logger_glob):
                    continue

            if filt.message_substring is not None:
                if filt.message_substring not in (entry.message or ""):
                    continue

            if filt.match_jq is not None:
                if not jq_bool(dataclasses.asdict(entry), filt.match_jq):
                    continue

            matched += 1
            if len(entries) < limit:
                entries.append(entry)

        return ScanResult(
            entries=entries,
            matched=matched,
            scanned=scanned,
            truncated=matched > limit,
        )

    def _tail_lines(self, path: Path, n: int) -> list[str]:
        """Read the last n lines from a file without loading it all.

        Uses loop-growing read window to handle long lines robustly.
        Starts with an estimate and doubles the window until either n lines
        are captured or the start of the file is reached. Discards partial
        leading fragment when seeking from a non-zero offset.
        Robust to files smaller than the estimate and final lines without
        trailing newline.
        """
        with path.open("rb") as f:
            # Get file size
            f.seek(0, 2)
            file_size = f.tell()

            # Start with initial estimate
            estimate = n * 100  # Conservative initial estimate

            while True:
                # Calculate seek offset
                seek_offset = max(0, file_size - estimate)
                f.seek(seek_offset)

                # Read and decode
                raw = f.read()
                decoded = raw.decode("utf-8", errors="replace")

                # If seeking from middle of file, discard partial leading fragment
                if seek_offset > 0:
                    first_newline_idx = decoded.find("\n")
                    if first_newline_idx != -1:
                        # Slice off everything before and including first newline
                        decoded = decoded[first_newline_idx + 1:]
                    else:
                        # No newline in read - entire content is partial fragment
                        # Grow window and retry
                        estimate *= 2
                        if estimate >= file_size:
                            estimate = file_size
                        continue

                # Split into lines
                all_lines = decoded.splitlines()

                # Check if we got enough lines
                if len(all_lines) >= n:
                    # We have enough (or more), return last n
                    return all_lines[-n:]

                # Not enough lines - check if we've reached the start
                if seek_offset == 0:
                    # At file start, return whatever we have
                    return all_lines

                # Not at start and not enough lines - grow the window and retry
                estimate *= 2
                if estimate >= file_size:
                    # Window would cover entire file, just read it all
                    estimate = file_size

    def await_one(
        self,
        filt: LogFilter,
        *,
        since: datetime | None,
        timeout_s: float,
        poll_interval_ms: int,
    ) -> AwaitResult:
        """Block until a matching entry appears or timeout (Task 4)."""
        raise NotImplementedError("await_one will be implemented in Task 4")

    def follow(self, filt: LogFilter, *, stop_event):
        """Stream matching entries indefinitely (Task 5)."""
        raise NotImplementedError("follow will be implemented in Task 5")

    def sample_schema(self, *, sample_lines: int = 100) -> SchemaDescriptor:
        """Infer field presence patterns from a sample (Task 4)."""
        raise NotImplementedError("sample_schema will be implemented in Task 4")
