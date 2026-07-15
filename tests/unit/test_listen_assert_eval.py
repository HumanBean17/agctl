"""Tests for agctl/listen/assert_eval.py — expectation evaluation over captures.

Mirrors tests/unit/test_listen_capture_file.py's style: real file I/O against
``tmp_path``, no mocks, no Kafka. The evaluator reads a run dir's attached
``asserts.jsonl``, resolves each spec's modes (named-pattern fill + explicit
merge), scans the matching ``<topic>.ndjson`` capture, and returns one
``ExpectationResult`` per spec with an at-least ``expect_count`` verdict.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from agctl.config.models import KafkaPattern
from agctl.errors import TemplateNotFound
from agctl.listen.assert_eval import evaluate_expectations, resolve_spec_modes


def _envelope(value: dict, *, topic: str = "topicA", offset: int = 0) -> dict:
    """Build a minimal CapturedEnvelope for tests."""
    return {
        "topic": topic,
        "key": None,
        "value": value,
        "partition": 0,
        "offset": offset,
        "timestamp": None,
        "headers": {},
        "captured_at": "2026-07-15T00:00:00Z",
    }


# topicA: one ORDER_CREATED envelope (matches the --contains spec).
ORDER_ENV = _envelope({"eventType": "ORDER_CREATED", "id": "a"}, topic="topicA")
# topicB: a non-matching envelope only (the --match spec finds nothing).
OTHER_ENV = _envelope({"eventType": "PAYMENT_PROCESSED", "id": "b"}, topic="topicB")


def _write_ndjson(path: Path, envelopes: list[dict]) -> Path:
    """Write each envelope as one NDJSON line, returning the path."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for env in envelopes:
            fh.write(json.dumps(env))
            fh.write("\n")
    return path


def _write_asserts(run_dir: Path, specs: list[dict]) -> Path:
    """Write each spec dict as one JSON line to asserts.jsonl."""
    run_dir.mkdir(parents=True, exist_ok=True)
    path = run_dir / "asserts.jsonl"
    with path.open("w", encoding="utf-8") as fh:
        for spec in specs:
            fh.write(json.dumps(spec))
            fh.write("\n")
    return path


def _spec(
    *,
    id: str,
    topic: str,
    modes: dict,
    params: dict | None = None,
    expect_count: int = 1,
) -> dict:
    """Build an ExpectationSpec dict as it appears in asserts.jsonl."""
    return {
        "id": id,
        "topic": topic,
        "modes": modes,
        "params": params or {},
        "expect_count": expect_count,
    }


class TestEvaluateExpectations:
    """evaluate_expectations returns one self-debugging result per spec."""

    def test_pass_and_fail_results(self, tmp_path: Path):
        """Spec A (--contains) passes; spec B (--match) fails with debug detail."""
        _write_asserts(
            tmp_path,
            [
                _spec(
                    id="exp-1",
                    topic="topicA",
                    modes={"contains": '{"eventType": "ORDER_CREATED"}'},
                    expect_count=1,
                ),
                _spec(
                    id="exp-2",
                    topic="topicB",
                    modes={"match": '.value.eventType == "ORDER_CREATED"'},
                    expect_count=1,
                ),
            ],
        )
        _write_ndjson(tmp_path / "topicA.ndjson", [ORDER_ENV])
        _write_ndjson(tmp_path / "topicB.ndjson", [OTHER_ENV])

        results = evaluate_expectations(tmp_path, patterns={})

        assert len(results) == 2

        # --- Spec A: passed, exactly one match. ---
        a = results[0]
        assert a["id"] == "exp-1"
        assert a["topic"] == "topicA"
        assert a["passed"] is True
        assert a["matched_count"] == 1
        assert a["expect_count"] == 1
        # modes list always present; contains roots at the message value.
        assert a["modes"] == [
            {"mode": "contains", "root": "message value", "needle": {"eventType": "ORDER_CREATED"}}
        ]
        # No failure detail on a passing result.
        assert a["detail"] == {}

        # --- Spec B: failed, no match — detail is self-debugging. ---
        b = results[1]
        assert b["id"] == "exp-2"
        assert b["topic"] == "topicB"
        assert b["passed"] is False
        assert b["matched_count"] == 0
        assert b["expect_count"] == 1
        # The match mode roots at the message envelope.
        assert b["modes"] == [
            {
                "mode": "match",
                "root": "message envelope",
                "expr": '.value.eventType == "ORDER_CREATED"',
            }
        ]
        # Failed results carry messages_scanned + the modes list.
        assert b["detail"]["messages_scanned"] == 1
        assert b["detail"]["modes"] == b["modes"]

    def test_at_least_semantics_matched_exceeds_expect(self, tmp_path: Path):
        """passed is matched_count >= expect_count (at-least, not exact)."""
        _write_asserts(
            tmp_path,
            [
                _spec(
                    id="exp-1",
                    topic="topicA",
                    modes={"contains": '{"eventType": "ORDER_CREATED"}'},
                    expect_count=1,
                )
            ],
        )
        # Two matching envelopes, expect_count=1 → still passes (at-least).
        _write_ndjson(
            tmp_path / "topicA.ndjson",
            [
                _envelope({"eventType": "ORDER_CREATED", "id": "a"}, topic="topicA"),
                _envelope({"eventType": "ORDER_CREATED", "id": "c"}, topic="topicA", offset=1),
            ],
        )

        results = evaluate_expectations(tmp_path, patterns={})
        assert len(results) == 1
        assert results[0]["passed"] is True
        assert results[0]["matched_count"] == 2
        assert results[0]["expect_count"] == 1

    def test_fail_when_expect_count_unmet(self, tmp_path: Path):
        """Matched but fewer than expect_count → failed with debug detail."""
        _write_asserts(
            tmp_path,
            [
                _spec(
                    id="exp-1",
                    topic="topicA",
                    modes={"contains": '{"eventType": "ORDER_CREATED"}'},
                    expect_count=3,
                )
            ],
        )
        _write_ndjson(tmp_path / "topicA.ndjson", [ORDER_ENV])  # only 1 match

        results = evaluate_expectations(tmp_path, patterns={})
        assert len(results) == 1
        r = results[0]
        assert r["passed"] is False
        assert r["matched_count"] == 1
        assert r["expect_count"] == 3
        assert r["detail"]["messages_scanned"] == 1

    def test_missing_capture_file_is_zero_matches(self, tmp_path: Path):
        """A topic with no <topic>.ndjson yet is zero matches / zero scanned."""
        _write_asserts(
            tmp_path,
            [
                _spec(
                    id="exp-1",
                    topic="topicA",
                    modes={"match": '.value.eventType == "ORDER_CREATED"'},
                    expect_count=1,
                )
            ],
        )
        # No topicA.ndjson written.

        results = evaluate_expectations(tmp_path, patterns={})
        assert results[0]["passed"] is False
        assert results[0]["matched_count"] == 0
        assert results[0]["detail"]["messages_scanned"] == 0

    def test_empty_asserts_yields_empty_results(self, tmp_path: Path):
        """A run dir with no asserts.jsonl returns an empty result list."""
        assert evaluate_expectations(tmp_path, patterns={}) == []

    def test_pattern_mode_debug_entry(self, tmp_path: Path):
        """A failed --pattern spec echoes the pattern name + filled expr."""
        _write_asserts(
            tmp_path,
            [
                _spec(
                    id="exp-1",
                    topic="topicA",
                    modes={"pattern": "order_created"},
                    params={"orderId": "42"},
                    expect_count=1,
                )
            ],
        )
        _write_ndjson(tmp_path / "topicA.ndjson", [OTHER_ENV])  # no match → fail

        patterns = {
            "order_created": KafkaPattern(
                topic="topicA",
                match='.value.eventType == "ORDER_CREATED" and .value.orderId == "{orderId}"',
            )
        }
        results = evaluate_expectations(tmp_path, patterns={**patterns})
        r = results[0]
        assert r["passed"] is False
        # Pattern mode roots at the message envelope and echoes name + filled expr.
        assert r["modes"] == [
            {
                "mode": "pattern",
                "root": "message envelope",
                "pattern": "order_created",
                "expr": '.value.eventType == "ORDER_CREATED" and .value.orderId == "42"',
            }
        ]
        assert r["detail"]["modes"] == r["modes"]


class TestResolveSpecModes:
    """resolve_spec_modes expands a spec into build_predicate's input dict."""

    def test_passes_through_explicit_modes(self):
        spec = _spec(
            id="exp-1",
            topic="topicA",
            modes={
                "contains": '{"a": 1}',
                "match": ".value.x",
                "path": ".value",
            },
        )
        resolved = resolve_spec_modes(spec, patterns={})
        assert resolved == {
            "contains": '{"a": 1}',
            "match": ".value.x",
            "path": ".value",
            "filled_pattern_match": None,
        }

    def test_fills_named_pattern_placeholder_from_params(self):
        spec = _spec(
            id="exp-1",
            topic="orders",
            modes={"pattern": "by_order"},
            params={"orderId": "42"},
        )
        patterns = {
            "by_order": KafkaPattern(
                topic="orders",
                match='.value.orderId == "{orderId}"',
            )
        }
        resolved = resolve_spec_modes(spec, patterns=patterns)
        assert resolved["filled_pattern_match"] == '.value.orderId == "42"'
        # Explicit contains/match/path are absent (None) — only the pattern fills.
        assert resolved["contains"] is None
        assert resolved["match"] is None
        assert resolved["path"] is None

    def test_explicit_modes_win_over_pattern_merge(self):
        """Explicit contains/match/path coexist with the filled pattern match."""
        spec = _spec(
            id="exp-1",
            topic="orders",
            modes={"pattern": "by_order", "match": ".value.flag == true"},
            params={"orderId": "7"},
        )
        patterns = {
            "by_order": KafkaPattern(topic="orders", match='.value.orderId == "{orderId}"')
        }
        resolved = resolve_spec_modes(spec, patterns=patterns)
        # Explicit match is preserved; pattern contributes filled_pattern_match.
        assert resolved["match"] == ".value.flag == true"
        assert resolved["filled_pattern_match"] == '.value.orderId == "7"'

    def test_pattern_without_match_yields_none(self):
        """A named pattern with no `match` leaves filled_pattern_match None."""
        spec = _spec(
            id="exp-1", topic="orders", modes={"pattern": "topic_only"}
        )
        patterns = {"topic_only": KafkaPattern(topic="orders")}
        resolved = resolve_spec_modes(spec, patterns=patterns)
        assert resolved["filled_pattern_match"] is None

    def test_unknown_pattern_raises_template_not_found(self):
        spec = _spec(
            id="exp-1", topic="orders", modes={"pattern": "no_such_pattern"}
        )
        with pytest.raises(TemplateNotFound) as exc_info:
            resolve_spec_modes(spec, patterns={})
        # The detail carries the config path for discovery (kafka assert parity).
        assert exc_info.value.detail["path"] == "kafka.patterns.no_such_pattern"

    def test_no_pattern_keeps_filled_pattern_match_none(self):
        spec = _spec(
            id="exp-1",
            topic="topicA",
            modes={"contains": '{"x": 1}'},
        )
        resolved = resolve_spec_modes(spec, patterns={})
        assert resolved["filled_pattern_match"] is None
