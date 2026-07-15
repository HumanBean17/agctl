"""Pure-function expectation evaluator for ``kafka listen assert``.

The companion to :mod:`agctl.listen.daemon` (lifecycle) and
:mod:`agctl.listen.capture_file` (payload reader): this module evaluates the
run's ATTACHED expectations (``asserts.jsonl``) against the per-topic captured
``<topic>.ndjson`` files and returns one self-debugging result per spec.

Each expectation is resolved into the same predicate machinery ``kafka assert``
uses (``--contains`` / ``--match`` / ``--path`` / ``--pattern``), then scanned
to exhaustion — :func:`count_matching` reads the WHOLE capture file. There is
deliberately NO wall-clock deadline anywhere: the scan is bounded by file size
only (a listener's capture is a finite on-disk artifact, not a live stream).

Functions:

- :func:`resolve_spec_modes` — expand one ExpectationSpec into
  ``{contains, match, path, filled_pattern_match}`` (named-pattern lookup +
  ``{placeholder}`` fill + explicit-mode merge).
- :func:`evaluate_expectations` — read ``asserts.jsonl``; for each spec resolve
  modes, build the predicate, count matches over its capture, and assemble an
  ``ExpectationResult`` (``passed = matched_count >= expect_count``).

The module is pure: it reads files via the Task-2/3 helpers and never touches a
Kafka client. A missing capture file is "no messages yet" → zero matches.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable

from ..config.models import KafkaPattern
from ..errors import TemplateNotFound
from ..resolution import fill_placeholders
from .capture_file import build_predicate, count_matching
from .daemon import capture_path, read_expectations

__all__ = ["resolve_spec_modes", "evaluate_expectations"]


def resolve_spec_modes(spec: dict[str, Any], patterns: dict[str, KafkaPattern]) -> dict[str, Any]:
    """Expand one ExpectationSpec into the mode dict :func:`build_predicate` expects.

    If ``spec["modes"]["pattern"]`` names a pattern, it is looked up in
    ``patterns`` (missing → :class:`TemplateNotFound` with the
    ``kafka.patterns.<name>`` config path, mirroring ``kafka assert``), its
    ``match`` is filled via :func:`fill_placeholders` against ``spec["params"]``
    into ``filled_pattern_match``, and the explicit ``contains``/``match``/
    ``path`` from ``spec["modes"]`` are carried through unchanged (explicit wins
    — they occupy different keys than ``filled_pattern_match``, so the two modes
    coexist and are AND-ed by the predicate, exactly as in ``kafka assert``).

    Args:
        spec: An ExpectationSpec dict (``{id, topic, modes, params, expect_count}``)
            as read from ``asserts.jsonl``.
        patterns: Mapping of pattern name → :class:`KafkaPattern`.

    Returns:
        ``{contains, match, path, filled_pattern_match}`` ready for
        :func:`build_predicate`. Unused modes are ``None``. A named pattern with
        no ``match`` yields ``filled_pattern_match=None``.

    Raises:
        TemplateNotFound: If ``spec["modes"]["pattern"]`` is not in ``patterns``.
    """
    modes = spec.get("modes") or {}
    params = spec.get("params") or {}

    pattern_name = modes.get("pattern")
    filled_pattern_match: str | None = None
    if pattern_name is not None:
        if pattern_name not in patterns:
            raise TemplateNotFound(
                f"Unknown kafka pattern: {pattern_name}",
                {"path": f"kafka.patterns.{pattern_name}"},
            )
        pat = patterns[pattern_name]
        # KafkaPattern (pydantic) exposes attribute access; a plain dict is also
        # accepted so tests/config payloads can pass either shape.
        pat_match = pat.match if hasattr(pat, "match") else pat.get("match")
        if pat_match is not None:
            filled_pattern_match = fill_placeholders(pat_match, params)

    return {
        "contains": modes.get("contains"),
        "match": modes.get("match"),
        "path": modes.get("path"),
        "filled_pattern_match": filled_pattern_match,
    }


def _build_modes_debug(
    resolved: dict[str, Any], pattern_name: str | None, needle: Any
) -> list[dict[str, Any]]:
    """Build the self-debugging per-mode list mirroring ``kafka assert``.

    Each active mode is echoed with its jq ``root`` so a no-match is diagnosable:
    ``contains``/``path`` root at the message value while ``match``/``pattern``
    root at the message envelope. ``path`` scopes the ``contains`` subset search,
    so it is folded into the ``contains`` entry (same root) — matching
    ``_kafka_assert_core``'s no-match detail exactly.

    Args:
        resolved: The mode dict from :func:`resolve_spec_modes`.
        pattern_name: The original ``--pattern`` name (for the pattern entry), or None.
        needle: The already-parsed ``--contains`` needle (for the contains entry).

    Returns:
        A list of ``{mode, root, ...}`` dicts, one per active mode.
    """
    match = resolved.get("match")
    path = resolved.get("path")
    filled = resolved.get("filled_pattern_match")

    modes: list[dict[str, Any]] = []
    if needle is not None:
        entry: dict[str, Any] = {"mode": "contains", "root": "message value", "needle": needle}
        if path is not None:
            entry["path"] = path
        modes.append(entry)
    if match is not None:
        modes.append({"mode": "match", "root": "message envelope", "expr": match})
    if pattern_name is not None:
        modes.append(
            {
                "mode": "pattern",
                "root": "message envelope",
                "pattern": pattern_name,
                "expr": filled,
            }
        )
    return modes


def evaluate_expectations(run_dir: Path, patterns: dict[str, KafkaPattern]) -> list[dict[str, Any]]:
    """Evaluate every attached expectation against its topic's captured file.

    Reads ``asserts.jsonl``; for each spec: resolves its modes, builds the
    predicate, and counts matches over ``<run_dir>/<topic>.ndjson``. The verdict
    is at-least semantics: ``passed = matched_count >= expect_count``.

    Every result carries a ``modes`` list (the active modes echoed for
    self-debugging). On a FAILED expectation, ``detail`` additionally includes
    ``messages_scanned`` (the number of envelopes inspected) and the ``modes``
    list — the same diagnostic intent as ``kafka assert``'s no-match detail.

    There is no wall-clock cutoff: the scan is bounded by file size only.

    Args:
        run_dir: The run directory (``<state_dir>/listen-<run_id>``).
        patterns: Mapping of pattern name → :class:`KafkaPattern`.

    Returns:
        One ``ExpectationResult`` dict per spec, in file order:
        ``{id, topic, passed, matched_count, expect_count, modes, detail}``.
        ``detail`` is ``{}`` on pass and ``{messages_scanned, modes}`` on fail.
    """
    results: list[dict[str, Any]] = []

    for spec in read_expectations(run_dir):
        resolved = resolve_spec_modes(spec, patterns)

        # Parse --contains into the needle ONCE so the predicate and the
        # self-debugging modes list share one source of truth (no double
        # json.loads — mirrors _kafka_assert_core). build_predicate treats a
        # non-string contains as an already-parsed value (passthrough).
        contains_raw = resolved.get("contains")
        needle: Any = None
        if contains_raw is not None:
            needle = (
                json.loads(contains_raw) if isinstance(contains_raw, str) else contains_raw
            )

        predicate = build_predicate({**resolved, "contains": needle})
        matched_count, scanned = count_matching(
            capture_path(run_dir, spec.get("topic", "")), predicate
        )

        expect_count = spec.get("expect_count", 0)
        passed = matched_count >= expect_count

        pattern_name = (spec.get("modes") or {}).get("pattern")
        modes = _build_modes_debug(resolved, pattern_name, needle)

        detail: dict[str, Any] = {}
        if not passed:
            detail = {"messages_scanned": scanned, "modes": modes}

        results.append(
            {
                "id": spec.get("id"),
                "topic": spec.get("topic"),
                "passed": passed,
                "matched_count": matched_count,
                "expect_count": expect_count,
                "modes": modes,
                "detail": detail,
            }
        )

    return results
