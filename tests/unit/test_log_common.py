"""Unit tests for the shared ``agctl.clients.log_common`` module.

Covers the four public symbols extracted from ``NdjsonFileBackend``:
``SLOT_SOURCE_SET``, ``normalize_dict``, ``entry_matches``, ``infer_schema``.
"""

import pytest

from agctl.clients.log_backend_protocol import (
    CanonicalEntry,
    LogFilter,
    SchemaDescriptor,
)
from agctl.clients.log_common import (
    SLOT_SOURCE_SET,
    entry_matches,
    infer_schema,
    normalize_dict,
)


# --- SLOT_SOURCE_SET --------------------------------------------------------
def test_slot_source_set_is_frozen_logstash_keys():
    assert SLOT_SOURCE_SET == frozenset(
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


# --- normalize_dict ---------------------------------------------------------
def test_normalize_dict_maps_slots_and_collects_fields():
    raw = {
        "@timestamp": "2026-07-22T12:00:00Z",
        "level": "error",
        "logger_name": "c.F",
        "message": "hi",
        "extra": 1,
    }
    entry = normalize_dict(raw)

    assert entry.timestamp == "2026-07-22T12:00:00Z"
    # level is upper-cased
    assert entry.level == "ERROR"
    assert entry.logger == "c.F"
    assert entry.message == "hi"
    assert entry.thread is None
    assert entry.service is None
    assert entry.stack_trace is None
    assert entry.tags is None
    # unknown keys land in fields, slot keys excluded
    assert entry.fields == {"extra": 1}


def test_normalize_dict_overrides_win_over_raw():
    raw = {
        "@timestamp": "X",
        "level": "info",
        "logger_name": "c.F",
        "message": "hi",
    }
    entry = normalize_dict(raw, service="svc", ts_override="2026-07-22T12:00:00.123Z")

    # ts_override wins over raw["@timestamp"]
    assert entry.timestamp == "2026-07-22T12:00:00.123Z"
    # service param wins (raw had none)
    assert entry.service == "svc"
    # @timestamp / service excluded from fields
    assert "@timestamp" not in entry.fields
    assert "service" not in entry.fields


def test_normalize_dict_missing_level_yields_empty_string():
    # No "level" key -> str(raw.get("level","")).upper() == ""
    entry = normalize_dict({"@timestamp": "t", "message": "m"})
    assert entry.level == ""


def test_normalize_dict_byte_for_byte_without_overrides():
    # With no overrides, behavior matches NdjsonFileBackend._normalize exactly.
    raw = {
        "@timestamp": "2026-07-22T12:00:00Z",
        "level": "WARN",
        "logger_name": "lgr",
        "message": "boom",
        "thread_name": "main",
        "service": "svc",
        "stack_trace": "trace...",
        "tags": ["a", "b"],
        "@version": 1,
        "level_value": 10000,
        "mdc": {"k": "v"},
    }
    entry = normalize_dict(raw)

    assert entry.timestamp == "2026-07-22T12:00:00Z"
    assert entry.level == "WARN"
    assert entry.logger == "lgr"
    assert entry.message == "boom"
    assert entry.thread == "main"
    assert entry.service == "svc"
    assert entry.stack_trace == "trace..."
    assert entry.tags == ["a", "b"]
    # non-slot keys (incl. logstash noise) all go to fields
    assert entry.fields == {"@version": 1, "level_value": 10000, "mdc": {"k": "v"}}


# --- entry_matches: level --------------------------------------------------
def test_entry_matches_level_case_insensitive_on_filter():
    entry = CanonicalEntry(
        timestamp="t", level="ERROR", logger="l", message="m"
    )
    # filter lower-case, entry upper-case -> match
    assert entry_matches(entry, LogFilter(level="error")) is True


def test_entry_matches_level_mismatch_returns_false():
    entry = CanonicalEntry(
        timestamp="t", level="INFO", logger="l", message="m"
    )
    assert entry_matches(entry, LogFilter(level="error")) is False


# --- entry_matches: logger_glob + message_substring ------------------------
def test_entry_matches_logger_glob_match_and_miss():
    match = CanonicalEntry(
        timestamp="t", level="INFO", logger="com.example.OrderService", message="m"
    )
    miss = CanonicalEntry(
        timestamp="t", level="INFO", logger="org.other.X", message="m"
    )
    assert entry_matches(match, LogFilter(logger_glob="com.example.*")) is True
    assert entry_matches(miss, LogFilter(logger_glob="com.example.*")) is False


def test_entry_matches_message_substring_present_and_absent():
    hit = CanonicalEntry(timestamp="t", level="INFO", logger="l", message="request failed")
    miss = CanonicalEntry(timestamp="t", level="INFO", logger="l", message="ok")
    assert entry_matches(hit, LogFilter(message_substring="failed")) is True
    assert entry_matches(miss, LogFilter(message_substring="failed")) is False


def test_entry_matches_logger_glob_against_none_logger():
    # entry.logger is None -> fnmatch over "" (no crash)
    entry = CanonicalEntry(timestamp="t", level="INFO", logger=None, message="m")
    assert entry_matches(entry, LogFilter(logger_glob="com.*")) is False


# --- entry_matches: match_jq (guarded) -------------------------------------
def test_entry_matches_jq_predicate():
    pytest.importorskip("jq")
    err = CanonicalEntry(timestamp="t", level="ERROR", logger="l", message="m")
    info = CanonicalEntry(timestamp="t", level="INFO", logger="l", message="m")
    assert entry_matches(err, LogFilter(match_jq='.level == "ERROR"')) is True
    assert entry_matches(info, LogFilter(match_jq='.level == "ERROR"')) is False


# --- entry_matches: empty filter ------------------------------------------
def test_entry_matches_empty_filter_is_true_for_any_entry():
    entry = CanonicalEntry(
        timestamp="t", level="INFO", logger="l", message="whatever"
    )
    assert entry_matches(entry, LogFilter()) is True


def test_entry_matches_and_combination():
    # All four filter dimensions set; only an entry passing all of them matches.
    pytest.importorskip("jq")
    entry = CanonicalEntry(
        timestamp="t",
        level="ERROR",
        logger="com.example.Svc",
        message="request failed",
    )
    filt = LogFilter(
        level="error",
        logger_glob="com.example.*",
        message_substring="failed",
        match_jq='.level == "ERROR"',
    )
    assert entry_matches(entry, filt) is True
    # failing any one dimension flips to False
    assert entry_matches(entry, LogFilter(level="info", logger_glob="com.example.*")) is False


# --- infer_schema ----------------------------------------------------------
def test_infer_schema_standard_conditional_observed():
    with_stack = CanonicalEntry(
        timestamp="t",
        level="ERROR",
        logger="c.F",
        message="boom",
        thread="main",
        service="svc",
        stack_trace="trace...",
        tags=["t1"],
        fields={"@version": 1, "request_id": "abc", "level_value": 10000},
    )
    fields_only = CanonicalEntry(
        timestamp="t",
        level="INFO",
        logger="c.G",
        message="hi",
        fields={"duration_ms": 42, "request_id": "def"},
    )

    schema = infer_schema([with_stack, fields_only])

    assert isinstance(schema, SchemaDescriptor)
    # standard slots present (non-empty) across either entry
    assert schema.standard == ["level", "logger", "message", "service", "thread", "timestamp"]
    # conditional only when stack_trace / tags present
    assert schema.conditional == ["stack_trace", "tags"]
    # observed = union of fields keys, minus @version / level_value noise
    assert schema.observed == ["duration_ms", "request_id"]


def test_infer_schema_empty_input():
    schema = infer_schema([])
    assert schema == SchemaDescriptor(standard=[], conditional=[], observed=[])


def test_infer_schema_excludes_empty_standard_slots():
    # entry with empty level/empty message -> those slots NOT counted as present
    entry = CanonicalEntry(
        timestamp="t",
        level="",  # empty -> not present
        logger="l",
        message="",  # empty -> not present
    )
    schema = infer_schema([entry])
    assert schema.standard == ["logger", "timestamp"]
    assert schema.conditional == []
    assert schema.observed == []
