"""NDJSON file backend for logstash-formatted logs (DESIGN §9.2)."""

import dataclasses
import fnmatch
import json
import os
import sys
import threading
import time
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

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

    def __init__(
        self,
        source: LogSource,
        *,
        monotonic: callable = time.monotonic,
        sleep: callable = time.sleep,
        stat_fn: callable = os.stat,
        _wait: Optional[callable] = None,
    ):
        """Store source config and injectable clock/sleep for testing.

        Args:
            source: LogSource configuration
            monotonic: Injectable monotonic clock (default: time.monotonic)
            sleep: Injectable sleep function (default: time.sleep)
            stat_fn: Injectable stat function (default: os.stat)
            _wait: Injectable wait function for stop_event (default: stop_event.wait)
        """
        self._path = source.path
        self._format = source.format
        self._monotonic = monotonic
        self._sleep = sleep
        self._stat_fn = stat_fn
        self._wait = _wait or (lambda ev, timeout: ev.wait(timeout))

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

    def _read_window(
        self,
        filt: LogFilter,
        since: datetime | None,
        until: datetime | None,
        limit: int,
        tail_lines: int,
    ) -> tuple[list[CanonicalEntry], int, int]:
        """Read and filter a time window, returning (entries, matched, scanned).

        Shared helper for scan and await_one. Reads up to tail_lines from file,
        applies time bounds and filters, returns up to limit matches.

        Returns:
            (entries, matched, scanned) tuple
        """
        if self._path is None:
            return [], 0, 0
        path = Path(self._path)
        if not path.exists():
            return [], 0, 0

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

        return entries, matched, scanned

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
        entries, matched, scanned = self._read_window(
            filt, since, until, limit, tail_lines
        )

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

    def _drain_complete_lines(
        self, buffer: bytes, new_bytes: bytes
    ) -> tuple[list[str], bytes]:
        """Combine buffer + new bytes, decode, split on newline.

        Returns ``(complete_lines, remaining_buffer)`` where ``remaining_buffer``
        holds the trailing partial-line fragment (no terminating newline yet) to
        be prepended on the next read. Shared by :meth:`follow` and poll-mode
        :meth:`await_one` so a line split across reads is never mis-counted.
        """
        combined = buffer + new_bytes
        decoded = combined.decode("utf-8", errors="replace")
        lines = decoded.split("\n")
        remaining = b""
        if not decoded.endswith("\n"):
            if lines:
                remaining = lines.pop().encode("utf-8", errors="replace")
        return lines, remaining

    def await_one(
        self,
        filt: LogFilter,
        *,
        since: datetime | None,
        timeout_s: float,
        poll_interval_ms: int,
        tail_lines: int,
    ) -> AwaitResult:
        """Block until a matching entry appears or timeout.

        One-shot mode (timeout_s <= 0): single read of the last ``tail_lines``
        (historical window); return first match or None.

        Poll mode (timeout_s > 0): **two-phase incremental** tail.
        Phase 1 reads the historical window (last ``tail_lines``) ONCE and
        counts each line exactly once. Phase 2 then tracks a high-water byte
        offset (seeded to the file's size after the historical read) so every
        subsequent iteration reads only NEW growth — physical lines are never
        re-counted across polls. Rollover / truncation (file shrank) resets the
        offset to 0.

        Args:
            filt: Filter criteria
            since: Time window start (None = unbounded)
            timeout_s: Timeout in seconds (<= 0 for one-shot, > 0 for poll mode)
            poll_interval_ms: Poll interval in milliseconds (poll mode only)
            tail_lines: Historical window (number of trailing lines) read once
                at the start of both one-shot and poll mode

        Returns:
            AwaitResult with first matching entry (or None), cumulative scanned
            count, and elapsed wall-clock time in milliseconds.
        """
        start_time = self._monotonic()
        scanned_total = 0
        until_now = datetime.now(timezone.utc)

        # Phase 1 (one-shot mode is just this phase): read the historical window
        # once and count each line exactly once.
        entries, _matched, scanned = self._read_window(
            filt=filt,
            since=since,
            until=until_now,
            limit=1,
            tail_lines=tail_lines,
        )
        scanned_total += scanned

        if timeout_s <= 0 or entries:
            elapsed_ms = int((self._monotonic() - start_time) * 1000)
            return AwaitResult(
                entry=entries[0] if entries else None,
                scanned=scanned_total,
                elapsed_ms=elapsed_ms,
            )

        # Phase 2 (poll mode): seed high-water offset to current file size, then
        # read only NEW growth each iteration (no re-count of historical bytes).
        deadline = start_time + timeout_s
        offset, _buffer = self._seed_offset()

        while True:
            new_entries, scanned, offset, _buffer = self._read_increment(
                filt=filt,
                since=since,
                offset=offset,
                buffer=_buffer,
            )
            scanned_total += scanned

            if new_entries:
                elapsed_ms = int((self._monotonic() - start_time) * 1000)
                return AwaitResult(
                    entry=new_entries[0],
                    scanned=scanned_total,
                    elapsed_ms=elapsed_ms,
                )

            now = self._monotonic()
            if now >= deadline:
                elapsed_ms = int((now - start_time) * 1000)
                return AwaitResult(
                    entry=None,
                    scanned=scanned_total,
                    elapsed_ms=elapsed_ms,
                )

            self._sleep(poll_interval_ms / 1000)

    def _seed_offset(self) -> tuple[int, bytes]:
        """Seed the poll-mode high-water offset to the file's current size.

        Returns ``(offset, buffer)``. If the file is missing, offset is 0
        (when it later appears, growth is read from the start). Buffer always
        starts empty.
        """
        if self._path is None:
            return 0, b""
        path = Path(self._path)
        try:
            return self._stat_fn(path).st_size, b""
        except (FileNotFoundError, OSError):
            return 0, b""

    def _read_increment(
        self,
        filt: LogFilter,
        since: datetime | None,
        offset: int,
        buffer: bytes,
    ) -> tuple[list[CanonicalEntry], int, int, bytes]:
        """Read NEW bytes since ``offset``, returning up to one match.

        Returns ``(entries, scanned, new_offset, new_buffer)``. Each physical
        line is counted in ``scanned`` exactly once. Rollover/truncation
        (size < offset) resets the offset to 0 and clears the buffer.

        No upper time bound is applied: bytes read here are, by construction,
        freshly appended (they did not exist when the offset was last
        advanced), so an upper ``until`` bound would only wrongly exclude
        them. The ``since`` lower bound is still honored.
        """
        entries: list[CanonicalEntry] = []
        scanned = 0
        if self._path is None:
            return entries, scanned, offset, buffer
        path = Path(self._path)

        try:
            current_size = self._stat_fn(path).st_size
        except (FileNotFoundError, OSError):
            return entries, scanned, offset, buffer

        # Rollover/truncation: file shrank -> reset offset to 0, clear buffer
        if current_size < offset:
            offset = 0
            buffer = b""

        if current_size <= offset:
            return entries, scanned, offset, buffer

        try:
            with path.open("rb") as f:
                f.seek(offset)
                new_bytes = f.read(current_size - offset)
            new_offset = current_size
        except (OSError, IOError):
            return entries, scanned, offset, buffer

        if not new_bytes:
            return entries, scanned, new_offset, buffer

        lines, remaining = self._drain_complete_lines(buffer, new_bytes)

        for line in lines:
            if not line.strip():
                continue
            try:
                raw = json.loads(line)
            except json.JSONDecodeError:
                print("agctl: skipping non-JSON log line", file=sys.stderr)
                continue

            entry = self._normalize(raw)

            # Window check: only the `since` lower bound. Fresh bytes are, by
            # construction, recent, so no upper `until` bound is applied (an
            # upper bound computed at poll start would wrongly exclude entries
            # appended later in the poll window).
            if since is not None:
                entry_ts = _parse_iso_datetime(entry.timestamp)
                if entry_ts is None:
                    continue
                entry_ts_utc = _to_utc(entry_ts)
                if entry_ts_utc < since:
                    continue

            scanned += 1

            # Apply filters (AND logic)
            if filt.level is not None and entry.level != filt.level.upper():
                continue

            if filt.logger_glob is not None and not fnmatch.fnmatch(
                entry.logger or "", filt.logger_glob
            ):
                continue

            if filt.message_substring is not None and (
                filt.message_substring not in (entry.message or "")
            ):
                continue

            if filt.match_jq is not None and not jq_bool(
                dataclasses.asdict(entry), filt.match_jq
            ):
                continue

            entries.append(entry)
            break  # limit == 1 for await_one

        return entries, scanned, new_offset, remaining

    def follow(
        self,
        filt: LogFilter,
        *,
        stop_event: threading.Event,
        poll_interval_ms: int,
    ) -> Iterator[CanonicalEntry]:
        """Stream matching entries indefinitely until stop_event is set.

        Polls the file for growth, yields new matching entries as they appear.
        On the first successful stat of an existing file, the read offset is
        seeded to that file's current size (EOF), so only growth AFTER connect
        is streamed — historical entries are not replayed (spec §6.4/§8.1:
        "new" entries only). If the file is missing, it waits and retries;
        when the file appears, the offset is seeded to its then-current size.
        Handles file truncation/rollover by resetting offset to 0.

        Args:
            filt: Filter criteria for entries
            stop_event: Threading event to signal graceful shutdown
            poll_interval_ms: Poll interval in milliseconds

        Yields:
            CanonicalEntry: New matching entries as they appear
        """
        if self._path is None:
            return

        path = Path(self._path)
        # None sentinel: not yet seeded. On first successful stat we seed to the
        # file's current size (EOF) so only post-connect growth is streamed.
        last_offset: int | None = None
        poll_interval = poll_interval_ms / 1000
        _buffer = b""  # Buffer for partial lines across iterations

        while True:
            # Check if we should stop
            if stop_event.is_set():
                return

            try:
                stat_result = self._stat_fn(path)
                current_size = stat_result.st_size
            except (FileNotFoundError, OSError):
                # File doesn't exist yet (service not started) - wait and retry
                if self._wait(stop_event, poll_interval):
                    return  # Event was set during wait
                continue

            # First successful stat: seed offset to current size (EOF) so we
            # stream only NEW growth after connect (no history replay).
            if last_offset is None:
                last_offset = current_size
                _buffer = b""
                if self._wait(stop_event, poll_interval):
                    return
                continue

            # Check for rollover/truncation (file shrank)
            if current_size < last_offset:
                last_offset = 0
                _buffer = b""  # Clear buffer on rollover

            # Check if file grew
            if current_size > last_offset:
                # Open file, seek to last offset, read new bytes
                try:
                    with path.open("rb") as f:
                        f.seek(last_offset)
                        new_bytes = f.read(current_size - last_offset)
                        last_offset = current_size

                        # No new data (shouldn't happen since size > offset, but be safe)
                        if not new_bytes:
                            if self._wait(stop_event, poll_interval):
                                return
                            continue

                        # Decode + split, carrying any partial line across reads.
                        lines, _buffer = self._drain_complete_lines(_buffer, new_bytes)

                        for line in lines:
                            if not line.strip():
                                continue

                            try:
                                raw = json.loads(line)
                            except json.JSONDecodeError:
                                print(
                                    "agctl: skipping non-JSON log line",
                                    file=sys.stderr,
                                )
                                continue

                            entry = self._normalize(raw)

                            # Apply filters (AND logic)
                            if filt.level is not None:
                                if entry.level != filt.level.upper():
                                    continue

                            if filt.logger_glob is not None:
                                if not fnmatch.fnmatch(
                                    entry.logger or "", filt.logger_glob
                                ):
                                    continue

                            if filt.message_substring is not None:
                                if filt.message_substring not in (
                                    entry.message or ""
                                ):
                                    continue

                            if filt.match_jq is not None:
                                if not jq_bool(dataclasses.asdict(entry), filt.match_jq):
                                    continue

                            # Yield the matching entry
                            yield entry

                            # Check stop_event immediately after yielding
                            if stop_event.is_set():
                                return

                except (OSError, IOError):
                    # File read error - wait and retry
                    if self._wait(stop_event, poll_interval):
                        return
                    continue

            # No new data - wait before next poll
            if self._wait(stop_event, poll_interval):
                return  # Event was set during wait

    def sample_schema(self, *, sample_lines: int = 100) -> SchemaDescriptor:
        """Infer field presence patterns from a sample of log entries.

        Args:
            sample_lines: Number of trailing lines to sample (default 100)

        Returns:
            SchemaDescriptor with:
            - standard: Fields from predefined set present in sample
            - conditional: Optional fields (stack_trace, tags) present in sample
            - observed: All keys from fields dict, excluding logstash noise
        """
        if self._path is None:
            return SchemaDescriptor(standard=[], conditional=[], observed=[])
        path = Path(self._path)
        if not path.exists():
            return SchemaDescriptor(standard=[], conditional=[], observed=[])

        # Read last sample_lines from file
        lines = self._tail_lines(path, sample_lines)

        # Standard slot field set (from brief)
        standard_slots = {
            "timestamp",
            "level",
            "logger",
            "message",
            "thread",
            "service",
        }

        # Conditional slot field set (from brief)
        conditional_slots = {"stack_trace", "tags"}

        # Track presence across entries
        standard_seen = set()
        conditional_seen = set()
        observed_keys = set()

        for line in lines:
            if not line.strip():
                continue

            try:
                raw = json.loads(line)
            except json.JSONDecodeError:
                # Skip non-JSON lines silently for schema inference
                continue

            entry = self._normalize(raw)

            # Check standard slots (non-None/non-empty)
            if entry.timestamp is not None and entry.timestamp != "":
                standard_seen.add("timestamp")
            if entry.level is not None and entry.level != "":
                standard_seen.add("level")
            if entry.logger is not None and entry.logger != "":
                standard_seen.add("logger")
            if entry.message is not None and entry.message != "":
                standard_seen.add("message")
            if entry.thread is not None and entry.thread != "":
                standard_seen.add("thread")
            if entry.service is not None and entry.service != "":
                standard_seen.add("service")

            # Check conditional slots (non-None)
            if entry.stack_trace is not None:
                conditional_seen.add("stack_trace")
            if entry.tags is not None:
                conditional_seen.add("tags")

            # Collect all observed keys from fields (excluding logstash noise)
            for key in entry.fields.keys():
                if key not in {"@version", "level_value"}:
                    observed_keys.add(key)

        return SchemaDescriptor(
            standard=sorted(standard_seen),
            conditional=sorted(conditional_seen),
            observed=sorted(observed_keys),
        )
