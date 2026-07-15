"""Tests for agctl/listen/capture_file.py — pure-function NDJSON reader.

Mirrors tests/unit/test_listen_daemon.py's style: real file I/O against
``tmp_path``, no mocks, no Kafka. The reader filters/counts/paginates a
topic's captured ``<topic>.ndjson`` file (one CapturedEnvelope per line) and
reuses ``kafka assert``'s predicate machinery via :func:`build_predicate`.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from agctl.errors import ConfigError
from agctl.listen.capture_file import (
    build_predicate,
    count_matching,
    first_matching,
    iter_messages,
    read_messages,
)


def _envelope(value: dict, *, key: str | None = None, offset: int = 0) -> dict:
    """Build a minimal CapturedEnvelope for tests (value is a dict here)."""
    return {
        "topic": "orders",
        "key": key,
        "value": value,
        "partition": 0,
        "offset": offset,
        "timestamp": None,
        "headers": {},
        "captured_at": "2026-07-15T00:00:00Z",
    }


# Two ORDER_CREATED envelopes and two with other event types.
ORDER_A = _envelope({"eventType": "ORDER_CREATED", "id": "a"}, offset=0)
ORDER_B = _envelope({"eventType": "ORDER_CREATED", "id": "b"}, offset=3)
OTHER_C = _envelope({"eventType": "ORDER_CANCELLED", "id": "c"}, offset=1)
OTHER_D = _envelope({"eventType": "PAYMENT_PROCESSED", "id": "d"}, offset=2)

ALL_ENVELOPES = [ORDER_A, ORDER_B, OTHER_C, OTHER_D]


def _write_capture(path: Path, envelopes: list[dict]) -> Path:
    """Write each envelope as one NDJSON line, returning the path."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for env in envelopes:
            fh.write(json.dumps(env))
            fh.write("\n")
    return path


def _write_messy_capture(path: Path) -> Path:
    """A capture with blank lines and unparseable lines mixed in."""
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        json.dumps(ORDER_A),
        "",
        "   \t  ",  # whitespace-only line
        json.dumps(OTHER_C),
        "{not valid json",  # unparseable
        json.dumps(ORDER_B),
        json.dumps(OTHER_D),
    ]
    with path.open("w", encoding="utf-8") as fh:
        for line in lines:
            fh.write(line + "\n")
    return path


@pytest.fixture
def capture_path(tmp_path: Path) -> Path:
    """The <topic>.ndjson capture path with the 4-envelope fixture written."""
    path = tmp_path / "orders.ndjson"
    _write_capture(path, ALL_ENVELOPES)
    return path


def _is_order_created(msg: dict) -> bool:
    return msg["value"]["eventType"] == "ORDER_CREATED"


class TestIterMessages:
    """iter_messages yields parsed envelopes, skipping blank/unparseable lines."""

    def test_yields_all_envelopes(self, capture_path: Path):
        """A clean file yields 4 envelopes in file order."""
        msgs = list(iter_messages(capture_path))
        assert len(msgs) == 4
        assert [m["offset"] for m in msgs] == [0, 3, 1, 2]

    def test_skips_blank_and_unparseable(self, tmp_path: Path):
        """Blank and unparseable lines are skipped silently."""
        path = tmp_path / "orders.ndjson"
        _write_messy_capture(path)
        msgs = list(iter_messages(path))
        # 4 real envelopes survive; blank/whitespace/garbage lines skipped.
        assert len(msgs) == 4
        assert [m["offset"] for m in msgs] == [0, 1, 3, 2]

    def test_missing_file_yields_nothing(self, tmp_path: Path):
        """A missing capture file is 'no messages yet' — yields nothing."""
        path = tmp_path / "does-not-exist.ndjson"
        assert list(iter_messages(path)) == []


class TestCountMatching:
    """count_matching counts ALL matches (no short-circuit) + scanned total."""

    def test_counts_matches_and_scanned(self, capture_path: Path):
        matched, scanned = count_matching(capture_path, _is_order_created)
        assert matched == 2
        assert scanned == 4

    def test_predicate_matches_none(self, capture_path: Path):
        def never(_msg: dict) -> bool:
            return False

        matched, scanned = count_matching(capture_path, never)
        assert matched == 0
        assert scanned == 4

    def test_predicate_matches_all(self, capture_path: Path):
        def always(_msg: dict) -> bool:
            return True

        matched, scanned = count_matching(capture_path, always)
        assert matched == 4
        assert scanned == 4

    def test_missing_file_returns_zero(self, tmp_path: Path):
        path = tmp_path / "does-not-exist.ndjson"
        matched, scanned = count_matching(path, _is_order_created)
        assert matched == 0
        assert scanned == 0


class TestFirstMatching:
    """first_matching stops at the first match and returns scanned count."""

    def test_returns_first_match(self, capture_path: Path):
        match, scanned = first_matching(capture_path, _is_order_created)
        assert match is not None
        assert match["offset"] == 0  # ORDER_A is first in file
        # Scanned count is the number of lines inspected including the match.
        assert scanned == 1

    def test_returns_none_when_no_match(self, capture_path: Path):
        def never(_msg: dict) -> bool:
            return False

        match, scanned = first_matching(capture_path, never)
        assert match is None
        assert scanned == 4

    def test_missing_file_returns_none(self, tmp_path: Path):
        path = tmp_path / "does-not-exist.ndjson"
        match, scanned = first_matching(path, _is_order_created)
        assert match is None
        assert scanned == 0


class TestReadMessages:
    """read_messages applies optional predicate then caps at limit."""

    def test_applies_predicate_and_truncates_at_limit(self, capture_path: Path):
        result = read_messages(
            capture_path, predicate=_is_order_created, limit=1
        )
        assert result["matched"] == 2
        assert result["truncated"] is True
        assert len(result["messages"]) == 1
        assert result["messages"][0]["offset"] == 0

    def test_no_truncation_when_under_limit(self, capture_path: Path):
        result = read_messages(
            capture_path, predicate=_is_order_created, limit=10
        )
        assert result["matched"] == 2
        assert result["truncated"] is False
        assert len(result["messages"]) == 2
        assert [m["offset"] for m in result["messages"]] == [0, 3]

    def test_no_predicate_returns_first_limit(self, capture_path: Path):
        """With predicate=None, read_messages returns up to `limit` messages."""
        result = read_messages(capture_path, predicate=None, limit=2)
        assert result["matched"] == 4
        assert result["truncated"] is True
        assert len(result["messages"]) == 2

    def test_missing_file_is_empty(self, tmp_path: Path):
        path = tmp_path / "does-not-exist.ndjson"
        result = read_messages(path, predicate=None, limit=10)
        assert result == {"matched": 0, "truncated": False, "messages": []}


class TestBuildPredicate:
    """build_predicate validates jq up front then delegates to assert machinery."""

    def test_match_predicate_true_for_matching_envelope(self):
        pred = build_predicate({"match": '.value.eventType == "ORDER_CREATED"'})
        assert pred(ORDER_A) is True
        assert pred(OTHER_C) is False

    def test_match_predicate_false_for_non_match(self):
        pred = build_predicate({"match": '.value.eventType == "NOPE"'})
        assert pred(ORDER_A) is False
        assert pred(OTHER_C) is False

    def test_invalid_match_raises_config_error(self):
        with pytest.raises(ConfigError):
            build_predicate({"match": ".value.eventType =="})  # truncated jq

    def test_invalid_path_raises_config_error(self):
        with pytest.raises(ConfigError):
            build_predicate({"path": ".value[["})  # malformed jq path

    def test_contains_json_is_parsed_into_needle(self):
        # contains is a JSON value; the predicate roots at msg["value"].
        pred = build_predicate({"contains": '{"eventType": "ORDER_CREATED"}'})
        assert pred(ORDER_A) is True
        assert pred(OTHER_C) is False

    def test_invalid_contains_json_raises_config_error(self):
        with pytest.raises(json.JSONDecodeError):
            build_predicate({"contains": "{not json"})

    def test_empty_spec_matches_everything(self):
        pred = build_predicate({})
        assert pred(ORDER_A) is True
        assert pred(OTHER_C) is True

    def test_filled_pattern_match_validated(self):
        # A pre-filled pattern jq expression is validated up front.
        pred = build_predicate(
            {"filled_pattern_match": '.value.eventType == "ORDER_CREATED"'}
        )
        assert pred(ORDER_A) is True
        assert pred(OTHER_C) is False

    def test_invalid_filled_pattern_match_raises_config_error(self):
        with pytest.raises(ConfigError):
            build_predicate({"filled_pattern_match": ".value[("})

    def test_predicate_swallows_per_message_exceptions(self, tmp_path: Path):
        """A predicate that raises is treated as a non-match (kafka assert parity).

        ``build_predicate`` delegates to ``jq_bool`` which already swallows
        jq errors; this pins the CONTRACT at the reader layer — a predicate
        that raises for non-jq reasons must not propagate.
        """

        def boom(_msg: dict) -> bool:
            raise RuntimeError("per-message explosion")

        path = tmp_path / "orders.ndjson"
        _write_capture(path, [ORDER_A, OTHER_C])
        matched, scanned = count_matching(path, boom)
        assert matched == 0
        assert scanned == 2
