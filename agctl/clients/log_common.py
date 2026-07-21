"""Shared log normalization, filtering, and schema inference.

Pure helpers extracted from ``NdjsonFileBackend`` so that any log backend
(file, Loki, ...) can reuse the same slot-mapping, per-entry AND-filtering,
and field-presence schema inference without triplicating the logic.

DESIGN §9.2. This module is intentionally side-effect-free and depends only
on stdlib (``dataclasses``, ``fnmatch``) plus the canonical log DTOs. The
optional ``jq`` dependency is imported lazily inside :func:`entry_matches`
branch (d), so a missing ``jq`` extra is only surfaced when a ``match_jq``
filter is actually used (matching ``NdjsonFileBackend``'s historical behavior).
"""

from __future__ import annotations

import dataclasses
import fnmatch

from agctl.clients.log_backend_protocol import (
    CanonicalEntry,
    LogFilter,
    SchemaDescriptor,
)

# The logstash source keys consumed by ``normalize_dict``. Everything NOT in
# this set lands in ``CanonicalEntry.fields``. Moved verbatim from
# ``NdjsonFileBackend._SLOT_SOURCE_SET`` so backends stay byte-for-byte aligned.
SLOT_SOURCE_SET: frozenset[str] = frozenset(
    {
        "@timestamp",
        "level",
        "logger_name",
        "thread_name",
        "message",
        "service",
        "stack_trace",
        "tags",
    }
)

# Canonical (post-normalize) slot names tracked by ``infer_schema``.
_STANDARD_SLOTS: frozenset[str] = frozenset(
    {"timestamp", "level", "logger", "message", "thread", "service"}
)
_CONDITIONAL_SLOTS: frozenset[str] = frozenset({"stack_trace", "tags"})

# Logstash noise keys excluded from the ``observed`` schema list.
_SCHEMA_NOISE: frozenset[str] = frozenset({"@version", "level_value"})


def normalize_dict(
    raw: dict,
    *,
    service: str | None = None,
    ts_override: str | None = None,
) -> CanonicalEntry:
    """Map a parsed logstash JSON object to a :class:`CanonicalEntry`.

    Slot fields (timestamp, level, logger, thread, message, service,
    stack_trace, tags) are pulled from their logstash key names; ``level`` is
    upper-cased. All other top-level keys (MDC, StructuredArguments,
    ``@version``, ``level_value``, ...) go into ``fields``.

    Generalizes ``NdjsonFileBackend._normalize`` with two optional overrides:
      * ``ts_override`` -- if set, wins over ``raw["@timestamp"]`` (backends
        that carry their own timestamp source, e.g. a Loki streaming chunk
        timestamp, can inject it here).
      * ``service`` -- if set, wins over ``raw["service"]`` (backends that
        know the service out-of-band, e.g. from source config, can inject it).

    With neither override set, the mapping is byte-for-byte the current
    ``NdjsonFileBackend._normalize`` behavior.
    """
    timestamp = ts_override if ts_override is not None else raw.get("@timestamp")
    level = str(raw.get("level", "")).upper()
    logger = raw.get("logger_name")
    message = raw.get("message")
    thread = raw.get("thread_name")
    svc = service if service is not None else raw.get("service")
    stack_trace = raw.get("stack_trace")
    tags = raw.get("tags")

    fields = {k: v for k, v in raw.items() if k not in SLOT_SOURCE_SET}

    return CanonicalEntry(
        timestamp=timestamp,
        level=level,
        logger=logger,
        message=message,
        thread=thread,
        service=svc,
        stack_trace=stack_trace,
        tags=tags,
        fields=fields,
    )


def entry_matches(entry: CanonicalEntry, filt: LogFilter) -> bool:
    """Return True iff ``entry`` satisfies every active dimension of ``filt``.

    AND of (None dimensions are skipped):
      (a) ``filt.level``: case-insensitive -- ``filt.level.upper()`` equals
          ``entry.level`` (which is already upper-cased by ``normalize_dict``).
      (b) ``filt.logger_glob``: ``fnmatch`` glob over ``entry.logger or ""``.
      (c) ``filt.message_substring``: substring of ``entry.message or ""``.
      (d) ``filt.match_jq``: jq predicate over ``dataclasses.asdict(entry)``.
          ``jq_bool`` is imported lazily here so a missing ``jq`` extra is only
          hit when a ``match_jq`` filter is actually used.

    Extracts the filter block currently triplicated in
    ``NdjsonFileBackend._read_window`` / ``_read_increment`` / ``follow``.
    """
    if filt.level is not None:
        if entry.level != filt.level.upper():
            return False

    if filt.logger_glob is not None:
        if not fnmatch.fnmatch(entry.logger or "", filt.logger_glob):
            return False

    if filt.message_substring is not None:
        if filt.message_substring not in (entry.message or ""):
            return False

    if filt.match_jq is not None:
        # Lazy import: a missing jq extra is only surfaced when a match_jq
        # filter is actually used (mirrors NdjsonFileBackend's behavior, where
        # jq_bool is imported at module top but the jq *library* is itself
        # lazy-loaded inside agctl.assertions._jq).
        from agctl.assertions import jq_bool

        if not jq_bool(dataclasses.asdict(entry), filt.match_jq):
            return False

    return True


def infer_schema(entries: list[CanonicalEntry]) -> SchemaDescriptor:
    """Infer field-presence patterns across already-normalized ``entries``.

    Returns a :class:`SchemaDescriptor` whose:
      * ``standard`` is the sorted union of present non-empty standard slots
        (``timestamp``, ``level``, ``logger``, ``message``, ``thread``,
        ``service``);
      * ``conditional`` is the sorted union of present ``stack_trace`` /
        ``tags`` (presence == non-None);
      * ``observed`` is the sorted union of every ``entry.fields`` key,
        excluding the logstash noise keys ``@version`` and ``level_value``.

    Extracts the presence-tracking currently inside
    ``NdjsonFileBackend.sample_schema``. Operates on already-normalized
    entries so any backend can build a sample then call this once.
    """
    standard_seen: set[str] = set()
    conditional_seen: set[str] = set()
    observed_keys: set[str] = set()

    for entry in entries:
        for slot in _STANDARD_SLOTS:
            value = getattr(entry, slot)
            if value is not None and value != "":
                standard_seen.add(slot)

        if entry.stack_trace is not None:
            conditional_seen.add("stack_trace")
        if entry.tags is not None:
            conditional_seen.add("tags")

        for key in entry.fields.keys():
            if key not in _SCHEMA_NOISE:
                observed_keys.add(key)

    return SchemaDescriptor(
        standard=sorted(standard_seen),
        conditional=sorted(conditional_seen),
        observed=sorted(observed_keys),
    )
