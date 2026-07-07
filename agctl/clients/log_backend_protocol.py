"""LogBackend protocol and canonical DTOs (DESIGN §9.2).

A minimal structural protocol describing the contract every log backend
must satisfy. Backends are registered as entry points (``agctl.logs_backends``)
and selected by the ``LogClient`` based on the source's ``type`` config field.
"""

from __future__ import annotations

import threading
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol, runtime_checkable


@dataclass
class CanonicalEntry:
    """Canonical log entry representation.

    All backends normalize their native format to this structure. Optional
    fields default to None; ``fields`` is an empty dict by default for
    extensibility.
    """

    timestamp: str
    level: str
    logger: str
    message: str
    thread: str | None = None
    service: str | None = None
    stack_trace: str | None = None
    tags: list[str] | None = None
    fields: dict = field(default_factory=dict)


@dataclass
class LogFilter:
    """Filter criteria for log queries.

    All filter fields are optional (default None). ``params`` carries
    backend-specific extensions (e.g. journal fields, extra jq predicates).
    """

    level: str | None = None
    logger_glob: str | None = None
    message_substring: str | None = None
    match_jq: str | None = None
    params: dict = field(default_factory=dict)


@dataclass
class ScanResult:
    """Result of a scan operation.

    ``entries`` holds the matched canonical entries. ``matched`` is the total
    count of matches (may exceed ``len(entries)`` if ``limit`` truncated).
    ``scanned`` is the total lines/events examined. ``truncated`` indicates
    whether more results exist beyond the returned set.
    """

    entries: list[CanonicalEntry]
    matched: int
    scanned: int
    truncated: bool


@dataclass
class AwaitResult:
    """Result of an await_one operation.

    ``entry`` is the first matching entry (or None if timeout). ``scanned``
    is the total lines/events examined. ``elapsed_ms`` is wall-clock time.
    """

    entry: CanonicalEntry | None
    scanned: int
    elapsed_ms: int


@dataclass
class SchemaDescriptor:
    """Schema descriptor from sample_schema.

    ``standard`` lists fields present in all sampled entries. ``conditional``
    lists fields present in some entries. ``observed`` lists all unique field
    names seen across the sample.
    """

    standard: list[str]
    conditional: list[str]
    observed: list[str]


@runtime_checkable
class LogBackend(Protocol):
    """Structural contract for a log backend.

    - :meth:`validate_config` validates the source config before use.
    - :meth:`scan` queries logs within a time window, returning paginated
      results.
    - :meth:`await_one` blocks until a matching entry appears or timeout.
    - :meth:`follow` streams matching entries indefinitely until stopped.
    - :meth:`sample_schema` inspects a sample of entries to infer field
      presence patterns.
    """

    def validate_config(self) -> None: ...

    def scan(
        self,
        filt: LogFilter,
        *,
        since: datetime | None,
        until: datetime | None,
        limit: int,
        tail_lines: int,
    ) -> ScanResult: ...

    def await_one(
        self,
        filt: LogFilter,
        *,
        since: datetime | None,
        timeout_s: float,
        poll_interval_ms: int,
    ) -> AwaitResult: ...

    def follow(
        self, filt: LogFilter, *, stop_event: threading.Event, poll_interval_ms: int
    ) -> Iterator[CanonicalEntry]: ...

    def sample_schema(self, *, sample_lines: int = 100) -> SchemaDescriptor: ...
